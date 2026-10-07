import os
import sys
import json
import base64
import tempfile
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from io import BytesIO
import schedule
import re

# Load a local .env (if present) so secrets never have to live in source.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import socket
import httplib2

# Optional: LlamaCloud SDK uses httpx under the hood; treat its transport errors as network errors too
try:
    import httpx
    _HTTPX_ERRORS = (httpx.TransportError,)
except ImportError:
    _HTTPX_ERRORS = ()

# === NEW LLAMA CLOUD SDK ===
try:
    from llama_cloud import LlamaCloud
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False

# === PDF TEXT EXTRACTION FOR VALIDATION ===
try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

# Custom SUCCESS log level
logging.SUCCESS = 25
logging.addLevelName(logging.SUCCESS, "SUCCESS")

def success(self, message, *args, **kwargs):
    if self.isEnabledFor(logging.SUCCESS):
        self._log(logging.SUCCESS, message, args, **kwargs)

logging.Logger.success = success

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Hardcoded configuration
CONFIG = {
    'gmail': {
        'sender': "noreply@hyperpure.com",
        'search_term': "Please find attached a copy of PO",
        'days_back': 2,
        'max_results': 100,
        'gdrive_folder_id': "1hjF05nxRUrK-bH67SVow1p140RVWs278"
    },
    'pdf': {
        'drive_folder_id': "1oRc5usVewCoAiq6uKpgY7G6xtAO_6Hz3",
        # "local" = free pdfplumber parser for the Hyperpure PO template (no API, no credits).
        # "llama" = LlamaCloud Extract v2 (needs LLAMA_API_KEY and credits).
        'extraction_engine': os.environ.get('EXTRACTION_ENGINE', "local").strip().lower(),
        # Secret is read from the environment (or a local .env). Never hardcode it here.
        'llama_api_key': os.environ.get('LLAMA_API_KEY', ""),
        # Saved Extract v2 configuration on LlamaCloud, created from EXTRACT_DATA_SCHEMA below.
        # Set LLAMA_EXTRACT_CONFIG_ID="" to fall back to sending the inline schema on every call.
        'llama_extract_config_id': os.environ.get('LLAMA_EXTRACT_CONFIG_ID', "cfg-ezk14ea1egxpsyy7p15yrzvd4pjq"),
        'spreadsheet_id': "1ZfKCYAaUJCSw2g-yTC9zxxYyZ4h5Ffyn2mUvA7aFufU",
        'sheet_range': "test",
        'days_back': 2,
        'max_files': 100
    },
    'log': {
        'sheet_range': "workflow_logs"
    },
    # Durable quarantine for PDFs that could not be extracted/appended. These are retried on
    # every run regardless of the days_back window, so a bad file is never silently dropped.
    'quarantine': {
        'sheet_range': "failed_extractions"
    }
}

TOKEN_FILE = 'token.json'
MAX_RETRIES = 3
RETRY_WAIT = 2

# --- Network-outage handling ---
# Errors that mean "the machine cannot reach the internet" (DNS failure, connection refused/reset, timeout).
# These are retried with a long backoff; if still failing, the whole run is aborted via NetworkDown so
# emails/PDFs are NOT marked as failed/processed and get picked up cleanly on the next scheduled run.
NETWORK_ERRORS = (
    socket.gaierror,          # [Errno 11001] getaddrinfo failed (Windows DNS failure)
    socket.timeout,
    ConnectionError,
    TimeoutError,
    httplib2.ServerNotFoundError,   # "Unable to find the server at gmail.googleapis.com"
) + _HTTPX_ERRORS
NET_RETRIES = 5      # attempts per API call before giving up
NET_BASE_WAIT = 10   # seconds; waits 10, 20, 30, 40 s between attempts (~100 s total)


class NetworkDown(RuntimeError):
    """Raised when an API call still fails with a network error after NET_RETRIES attempts."""
    pass

# Validation / retry settings specifically for LlamaCloud extraction completeness
EXTRACTION_MAX_RETRIES = 3   # how many times to re-run the extraction job on mismatch
EXTRACTION_RETRY_WAIT = 15   # seconds to wait between extraction retries
# Tolerance: allow ±1 SKU difference to absorb merged/blank footer rows in the PDF
EXTRACTION_TOLERANCE = 1

# Static, ordered column headers for the POS sheet — never changes regardless of extracted data
STATIC_POS_HEADERS = [
    "po_number", "po_date", "expected_delivery_date", "vendor_id", "account_number",
    "delivery_charge", "total_amount", "total_taxable_value_overall", "total_tax_amount_overall",
    "amount_chargeable_in_words",
    "buyer_name", "buyer_gstin", "buyer_state", "buyer_address",
    "supplier_name", "supplier_gstin", "supplier_phone", "supplier_address",
    "product_number", "product_name", "description", "hsn", "unit_of_measure",
    "quantity_ordered", "mrp", "price_per_unit", "margin_percentage", "gst_rate",
    "total_amount_item", "total_tax_amount_item",
    "source_file", "drive_file_id", "processed_date",
]

# ---------------------------------------------------------------------------
# Single source of truth for the LlamaCloud Extract v2 schema.
# The saved platform configuration (CONFIG['pdf']['llama_extract_config_id']) was created from
# exactly this schema. If you change anything here, recreate the saved config so the two agree.
# ---------------------------------------------------------------------------
EXTRACT_TARGET = "per_doc"
EXTRACT_TIER = "agentic"
EXTRACT_DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "purchase_order_number": {"type": "string"},
        "purchase_order_date": {"type": "string"},
        "expected_delivery_date": {"type": "string"},
        "vendor_id": {"type": "string"},
        "account_number": {"type": "string"},
        "delivery_charge": {"type": "string"},
        "total_amount": {"type": "string"},
        "total_taxable_value_overall": {"type": "string"},
        "total_tax_amount_overall": {"type": "string"},
        "amount_chargeable_in_words": {"type": "string"},
        "buyer": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "gstin": {"type": "string"},
                "state": {"type": "string"},
                "address": {"type": "string"},
            },
        },
        "supplier": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "gstin": {"type": "string"},
                "phone": {"type": "string"},
                "address": {"type": "string"},
            },
        },
        "line_items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "product_number": {"type": "string"},
                    "product_name": {"type": "string"},
                    "description": {"type": "string"},
                    "hsn": {"type": "string"},
                    "unit_of_measure": {"type": "string"},
                    "quantity_ordered": {"type": "string"},
                    "mrp": {"type": "string"},
                    "price_per_unit": {"type": "string"},
                    "margin_percentage": {"type": "string"},
                    "gst_rate": {"type": "string"},
                    "total_amount_item": {"type": "string"},
                    "total_tax_amount_item": {"type": "string"},
                },
            },
        },
    },
}


# ---------------------------------------------------------------------------
# Local (free) parser for Hyperpure purchase-order PDFs — EXTRACTION_ENGINE=local.
# Returns the same shape as a LlamaCloud extraction of EXTRACT_DATA_SCHEMA, so
# _process_extracted_data() consumes it unchanged.
# ---------------------------------------------------------------------------
PO_PRODUCT_NUM_RE = re.compile(r"^\d{6}$")
PO_NUM = r"-?[\d,]+(?:\.\d+)?"

# Line-item columns in the order Hyperpure prints them. Item rows are read positionally
# from their non-empty-span cells, because continuation pages use a different column grid.
ITEM_FIELDS = [
    "product_number", "product_name", "hsn", "mrp", "margin_percentage", "description",
    "quantity_ordered", "price_per_unit", "unit_of_measure", "gst_rate",
    "total_tax_amount_item", "total_amount_item",
]

HEADER_LABELS = {
    "Purchase Order Number": "purchase_order_number",
    "Purchase Order Date": "purchase_order_date",
    "Expected Delivery Date": "expected_delivery_date",
    "Account Number": "account_number",
    "Vendor Id": "vendor_id",
}
PARTY_LABELS = ("Bill From", "Bill To", "Shipped", "From :", "Address :", "GSTIN", "Phone", "State")


class POParseError(Exception):
    pass


def _flat(s):
    return " ".join((s or "").split())


def _join_lines(lines):
    """Join wrapped lines with spaces, except a word broken at a hyphen ('Haryana-' + '131103')."""
    out = ""
    for l in lines:
        if out and not (out.endswith("-") and not out.endswith(" -")):
            out += " "
        out += l
    return out


# A line-item row as it appears in the text layer:
# <product no> [name part] <HSN> <MRP> <margin> <desc> <qty> <price> <UoM> <GST> <tax> <total>
ITEM_LINE_RE = re.compile(
    rf"^(\d{{6}})\s*(.*?)\s*\b(\d{{8}})\s+({PO_NUM})\s+({PO_NUM})\s+(.+?)\s+({PO_NUM})\s+({PO_NUM})\s+(.+?)"
    rf"\s+({PO_NUM})%?\s+({PO_NUM})\s+({PO_NUM})$")


def _items_from_text(page_texts):
    """Line items read from the text layer, in document order. Used to recover rows that
    pdfplumber's table finder drops (a row cut by a page break has no bottom border)."""
    items = []
    for p, ptext in enumerate(page_texts):
        lines = ptext.split("\n")
        for j, line in enumerate(lines):
            m = ITEM_LINE_RE.match(line.strip())
            if not m:
                continue
            g = list(m.groups())
            name = g[1]
            # Row cut at the page break: the rest of the name is the next page's first line.
            if j + 1 < len(lines) and lines[j + 1].startswith("Page ") and p + 1 < len(page_texts):
                name = _join_lines([name, page_texts[p + 1].split("\n")[0].strip()])
            g[1] = name
            items.append(dict(zip(ITEM_FIELDS, g)))
    return items


def _field(cell_text, label):
    m = re.search(rf"{label}\s*:\s*(.*)", cell_text)
    return m.group(1).strip() if m else ""


def _buyer(cell):
    """'Bill To :' block: name on the line after the label, then address, State, Phone, GSTIN."""
    lines = [l.strip() for l in cell.split("\n") if l.strip()]
    lines = lines[1:] if lines and lines[0].startswith("Bill To") else lines
    name, addr = (lines[0] if lines else ""), []
    for l in lines[1:]:
        if l.startswith(("State", "Phone", "GSTIN")):
            break
        addr.append(l[len("Address :"):].strip() if l.startswith("Address :") else l)
    return {
        "name": name,
        "gstin": _field(cell, "GSTIN"),
        "state": _field(cell, "State"),
        "address": _join_lines([a for a in addr if a]),
    }


def _supplier(page, bbox, cell):
    """'Bill From :' block. The label sits mid-name on some layouts, so read words by position:
    bold words in the value column are the name, regular words above GSTIN are the address."""
    words = page.crop(bbox).extract_words(extra_attrs=["fontname"], keep_blank_chars=False)
    labels = [w for w in words if w["text"] in ("Bill", "Address", "GSTIN")]
    value_x = max((w["x1"] for w in words if w["text"] == ":" and w["x0"] < bbox[0] + 60), default=bbox[0] + 45) + 3
    gstin_top = min((w["top"] for w in labels if w["text"] == "GSTIN"), default=bbox[3])
    values = [w for w in words if w["x0"] >= value_x and w["top"] < gstin_top - 2]

    def lines(ws):
        rows = {}
        for w in sorted(ws, key=lambda w: (round(w["top"]), w["x0"])):
            rows.setdefault(round(w["top"]), []).append(w["text"])
        return [" ".join(r) for r in rows.values()]

    name = lines([w for w in values if "Bold" in w["fontname"]])
    addr = lines([w for w in values if "Bold" not in w["fontname"]])
    return {
        "name": " ".join(name),
        "gstin": _field(cell, "GSTIN"),
        "phone": _field(cell, "Phone"),
        "address": _join_lines(addr),
    }


def parse_po_pdf(pdf_bytes: bytes) -> dict:
    data = {k: "" for k in (
        "purchase_order_number", "purchase_order_date", "expected_delivery_date", "vendor_id",
        "account_number", "delivery_charge", "total_amount", "total_taxable_value_overall",
        "total_tax_amount_overall", "amount_chargeable_in_words")}
    data["buyer"], data["supplier"], data["line_items"] = {}, {}, []
    seen_header = False

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        page_texts = [p.extract_text() or "" for p in pdf.pages]
        text = "\n".join(page_texts)
        for page in pdf.pages:
            for tbl in page.find_tables():
                for row_obj, row in zip(tbl.rows, tbl.extract()):
                    cells = [c or "" for c in row]
                    first = cells[0]

                    if "Bill From" in first and not data["supplier"]:
                        data["supplier"] = _supplier(page, row_obj.cells[0], first)
                        continue
                    if first.lstrip().startswith("Bill To") and not data["buyer"]:
                        data["buyer"] = _buyer(first)
                        continue
                    for c in cells:
                        label, _, value = c.partition("\n")
                        if label.strip() in HEADER_LABELS:
                            data[HEADER_LABELS[label.strip()]] = _flat(value)

                    if _flat(first) == "Product Number":
                        seen_header = True
                        continue
                    if seen_header and PO_PRODUCT_NUM_RE.match(first.strip()):
                        vals = [_flat(c) for c in row if c is not None]
                        if len(vals) != len(ITEM_FIELDS):
                            raise POParseError(
                                f"line item {first.strip()} has {len(vals)} columns, expected {len(ITEM_FIELDS)}")
                        data["line_items"].append(dict(zip(ITEM_FIELDS, vals)))

    # Totals from the text layer: first "Total" line = items total (qty, amount),
    # last "Total" line = tax summary (taxable value, tax amount).
    totals = re.findall(rf"(?m)^Total ({PO_NUM}) ({PO_NUM})\s*$", text)
    if totals:
        data["total_amount"] = totals[0][1].replace(",", "")
        data["total_taxable_value_overall"] = totals[-1][0].replace(",", "")
        data["total_tax_amount_overall"] = totals[-1][1].replace(",", "")
    m = re.search(rf"(?m)^Delivery Charge[ \t]*({PO_NUM})?\s*$", text)
    if m and m.group(1):
        data["delivery_charge"] = m.group(1).replace(",", "")
    m = re.search(r"Amount Chargeable \(in words\):.*\n(.+)", text)
    if m:
        data["amount_chargeable_in_words"] = m.group(1).strip()

    # Recover rows the table finder missed, keeping document order.
    text_items = _items_from_text(page_texts)
    have = {i["product_number"] for i in data["line_items"]}
    missing = [i for i in text_items if i["product_number"] not in have]
    if missing:
        order = {i["product_number"]: n for n, i in enumerate(text_items)}
        data["line_items"] = sorted(data["line_items"] + missing, key=lambda i: order.get(i["product_number"], 0))

    if not data["purchase_order_number"] or not data["line_items"]:
        raise POParseError("could not find PO number or line items — unexpected PDF layout")

    # Self-check: line totals (+ delivery charge) must add up to the PO total.
    try:
        items_sum = sum(float(i["total_amount_item"].replace(",", "")) for i in data["line_items"])
        expected = float(data["total_amount"]) - float(data["delivery_charge"] or 0)
    except ValueError as e:
        raise POParseError(f"non-numeric total: {e}")
    if abs(items_sum - expected) > 0.05:
        raise POParseError(
            f"line items sum to {items_sum:.2f} but PO total is {expected:.2f} — a row may be missing")
    return data


class MilkbasketAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive.file']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
        self.logs = []
        self.current_stats = {
            'gmail': {'processed_emails': 0, 'total_attachments': 0, 'successful_uploads': 0, 'failed_uploads': 0},
            'pdf': {'total_pdfs': 0, 'processed_pdfs': 0, 'skipped_pdfs': 0, 'failed_pdfs': 0, 'rows_added': 0}
        }
    
    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {"timestamp": timestamp, "level": level.upper(), "message": message}
        self.logs.append(log_entry)
        if len(self.logs) > 100:
            self.logs = self.logs[-100:]
        level_map = {"INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR, "SUCCESS": logging.SUCCESS}
        logger.log(level_map.get(level.upper(), logging.INFO), message)
    
    def get_logs(self): return self.logs
    def clear_logs(self): self.logs = []
    def reset_stats(self):
        self.current_stats = {
            'gmail': {'processed_emails': 0, 'total_attachments': 0, 'successful_uploads': 0, 'failed_uploads': 0},
            'pdf': {'total_pdfs': 0, 'processed_pdfs': 0, 'skipped_pdfs': 0, 'failed_pdfs': 0, 'rows_added': 0}
        }
    def get_stats(self): return self.current_stats

    def authenticate(self):
        try:
            self.log("Starting authentication process...", "INFO")
            combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
            
            creds = None
            if os.path.exists(TOKEN_FILE):
                creds = Credentials.from_authorized_user_file(TOKEN_FILE, combined_scopes)
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    with open(TOKEN_FILE, 'w') as token:
                        token.write(creds.to_json())
            
            if not creds or not creds.valid:
                if os.environ.get('GITHUB_ACTIONS') == 'true':
                    # No browser on a CI runner: run_local_server() would hang until the job times out.
                    self.log(
                        "Google token is missing, expired or revoked. Run generate_token.py locally and "
                        "update the GOOGLE_TOKEN_JSON repository secret with the new token.json.",
                        "ERROR",
                    )
                    return False
                if not os.path.exists('credentials.json'):
                    self.log("credentials.json not found.", "ERROR")
                    return False
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', combined_scopes)
                creds = flow.run_local_server(port=0)
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                self.log("OAuth authentication successful!", "SUCCESS")
            
            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            return True
        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            return False

    def api_call(self, fn, what: str = "API call"):
        """
        Run a zero-arg callable (usually a lambda around `.execute()`), retrying only on
        network-level errors with linear backoff. Non-network exceptions (HttpError 4xx,
        KeyError, etc.) propagate immediately. Raises NetworkDown after NET_RETRIES failures.
        """
        for attempt in range(1, NET_RETRIES + 1):
            try:
                return fn()
            except NETWORK_ERRORS as e:
                if attempt == NET_RETRIES:
                    raise NetworkDown(f"{what}: network still down after {NET_RETRIES} attempts ({e})") from e
                wait = NET_BASE_WAIT * attempt
                self.log(f"Network error during {what} ({e}); retry {attempt}/{NET_RETRIES} in {wait}s", "WARNING")
                time.sleep(wait)

    def retry_wrapper(self, func, *args, **kwargs):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.log(f"Attempt {attempt} failed: {str(e)}", "WARNING")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_WAIT)
                else:
                    raise

    def search_emails(self, sender: str = "", search_term: str = "", days_back: int = 7, max_results: int = 50) -> List[Dict]:
        def _search():
            query_parts = ["has:attachment"]
            if sender:
                query_parts.append(f'from:"{sender}"')
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            query = " ".join(query_parts)
            self.log(f"Searching Gmail with query: {query}", "INFO")
            result = self.api_call(
                lambda: self.gmail_service.users().messages().list(userId='me', q=query, maxResults=max_results).execute(),
                "Gmail search")
            messages = result.get('messages', [])
            self.log(f"Gmail search returned {len(messages)} messages", "INFO")
            return messages
        try:
            return self.retry_wrapper(_search)
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Email search failed: {str(e)}", "ERROR")
            return []

    def get_existing_filenames(self, spreadsheet_id: str, sheet_range: str) -> set:
        try:
            result = self.api_call(lambda: self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=sheet_range, majorDimension="ROWS"
            ).execute(), "Sheets read (existing filenames)")
            values = result.get('values', [])
            if not values:
                return set()
            headers = values[0]
            if "source_file" not in headers:
                return set()
            id_index = headers.index("source_file")
            existing_ids = {row[id_index] for row in values[1:] if len(row) > id_index and row[id_index]}
            self.log(f"Found {len(existing_ids)} existing file names in sheet", "INFO")
            return existing_ids
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to get existing file names: {str(e)}", "ERROR")
            return set()

    def get_existing_file_ids(self, spreadsheet_id: str, sheet_range: str) -> set:
        try:
            result = self.api_call(lambda: self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=sheet_range, majorDimension="ROWS"
            ).execute(), "Sheets read (existing file IDs)")
            values = result.get('values', [])
            if not values:
                return set()
            headers = values[0]
            if "drive_file_id" not in headers:
                return set()
            id_index = headers.index("drive_file_id")
            existing_ids = {row[id_index] for row in values[1:] if len(row) > id_index and row[id_index]}
            self.log(f"Found {len(existing_ids)} existing file IDs in sheet", "INFO")
            return existing_ids
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to get existing file IDs: {str(e)}", "ERROR")
            return set()

    def process_gmail_workflow(self, config: dict, progress_callback=None, status_callback=None):
        try:
            if status_callback: status_callback("Starting Gmail workflow...")
            self.log("Starting Gmail workflow", "INFO")
            if progress_callback: progress_callback(10)

            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            self.current_stats['gmail']['processed_emails'] = len(emails)

            if progress_callback: progress_callback(25)
            if not emails:
                self.log("No emails found matching criteria", "WARNING")
                return {'success': True, 'processed': 0}

            if status_callback: status_callback(f"Found {len(emails)} emails. Processing attachments...")
            self.log(f"Found {len(emails)} emails matching criteria", "INFO")

            base_folder_name = "Gmail_Attachments"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'))

            if not base_folder_id:
                self.log("Failed to create base folder in Google Drive", "ERROR")
                return {'success': False, 'processed': 0}

            if progress_callback: progress_callback(50)

            processed_count = 0
            total_attachments = 0

            for i, email in enumerate(emails):
                try:
                    if status_callback: status_callback(f"Processing email {i+1}/{len(emails)}")
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]

                    inv_match = re.search(r'against Inv: (\S+)', subject)
                    if inv_match and '/' in inv_match.group(1):
                        self.log(f"Skipping email (contains '/'): {subject}", "INFO")
                        continue

                    self.log(f"Processing email: {subject}", "INFO")

                    message = self.api_call(
                        lambda: self.gmail_service.users().messages().get(userId='me', id=email['id'], format='full').execute(),
                        "Gmail message fetch")
                    attachment_count, failed_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], config, base_folder_id)

                    total_attachments += attachment_count
                    self.current_stats['gmail']['total_attachments'] += attachment_count + failed_count
                    self.current_stats['gmail']['successful_uploads'] += attachment_count
                    self.current_stats['gmail']['failed_uploads'] += failed_count

                    if attachment_count > 0:
                        processed_count += 1
                        self.log(f"Found {attachment_count} attachments", "SUCCESS")
                    
                    if progress_callback:
                        progress = 50 + (i + 1) / len(emails) * 45
                        progress_callback(int(progress))
                except NetworkDown as e:
                    # Abort the whole Gmail pass: remaining emails are NOT failures, they are simply
                    # unprocessed and will be picked up by the next scheduled run (days_back window).
                    self.log(f"Aborting Gmail workflow at email {i+1}/{len(emails)} — {e}", "ERROR")
                    raise
                except Exception as e:
                    # Email-level failure: attachments were never enumerated, so count it as one
                    # lost unit of work rather than pretending we know the attachment count.
                    self.log(f"Failed to process email: {str(e)}", "ERROR")
                    self.current_stats['gmail']['failed_uploads'] += 1

            if progress_callback: progress_callback(100)
            if status_callback: status_callback(f"Gmail workflow completed! Processed {total_attachments} attachments")
            self.log(f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails", "SUCCESS")
            return {'success': True, 'processed': total_attachments}
        except NetworkDown as e:
            self.log(f"Gmail workflow aborted (network down): {str(e)}", "ERROR")
            return {'success': False, 'processed': 0, 'network_down': True}
        except Exception as e:
            self.log(f"Gmail workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'processed': 0}

    def _get_email_details(self, message_id: str) -> Dict:
        try:
            message = self.api_call(
                lambda: self.gmail_service.users().messages().get(userId='me', id=message_id, format='metadata').execute(),
                "Gmail metadata fetch")
            headers = message['payload'].get('headers', [])
            return {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to get email details: {str(e)}", "ERROR")
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}

    @staticmethod
    def _q(value: str) -> str:
        """Escape a value for interpolation into a single-quoted Drive API query literal.
        Without this, a name containing an apostrophe (e.g. "Baker's PO.pdf") produces an
        invalid query, which the callers swallow — silently re-uploading duplicates."""
        return (value or "").replace("\\", r"\\").replace("'", r"\'")

    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        try:
            query = f"name='{self._q(folder_name)}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            existing = self.api_call(
                lambda: self.drive_service.files().list(q=query, fields='files(id, name)').execute(),
                "Drive folder lookup")
            if existing.get('files'):
                return existing['files'][0]['id']
            
            folder_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            folder = self.api_call(
                lambda: self.drive_service.files().create(body=folder_metadata, fields='id').execute(),
                "Drive folder create")
            return folder.get('id')
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to create folder {folder_name}: {str(e)}", "ERROR")
            return ""

    def _extract_attachments_from_email(self, message_id: str, payload: Dict, config: dict, base_folder_id: str) -> tuple:
        """Walk the MIME tree and upload every attachment.
        Returns (uploaded_count, failed_count) so the caller can report failures per
        attachment rather than per email."""
        processed_count = 0
        failed_count = 0
        if "parts" in payload:
            for part in payload["parts"]:
                up, failed = self._extract_attachments_from_email(message_id, part, config, base_folder_id)
                processed_count += up
                failed_count += failed
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            try:
                attachment_id = payload["body"].get("attachmentId")
                att = self.api_call(
                    lambda: self.gmail_service.users().messages().attachments().get(userId='me', messageId=message_id, id=attachment_id).execute(),
                    "Gmail attachment fetch")
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))

                search_folder_id = self._create_drive_folder(config.get('search_term', 'all-attachments'), base_folder_id)
                type_folder_id = self._create_drive_folder(self._classify_extension(filename), search_folder_id)

                clean_filename = self._sanitize_filename(filename)
                if not self._file_exists_in_folder(clean_filename, type_folder_id):
                    file_metadata = {'name': clean_filename, 'parents': [type_folder_id]}
                    media = MediaIoBaseUpload(BytesIO(file_data), mimetype='application/octet-stream', resumable=True)
                    self.api_call(
                        lambda: self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute(),
                        "Drive upload")
                    self.log(f"Uploaded: {clean_filename}", "INFO")
                    processed_count = 1
                else:
                    self.log(f"File already exists, skipping: {clean_filename}", "INFO")
            except NetworkDown:
                raise
            except Exception as e:
                self.log(f"Failed to process attachment {filename}: {str(e)}", "ERROR")
                failed_count = 1
        return processed_count, failed_count

    def _sanitize_filename(self, filename: str) -> str:
        cleaned = re.sub(r'[<>:"/\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                cleaned = f"{'.'.join(name_parts[:-1])[:95]}.{name_parts[-1]}"
            else:
                cleaned = cleaned[:100]
        return cleaned

    def _classify_extension(self, filename: str) -> str:
        if not filename or '.' not in filename:
            return "Other"
        ext = filename.split(".")[-1].lower()
        type_map = {
            "pdf": "PDFs", "doc": "Documents", "docx": "Documents", "txt": "Documents",
            "xls": "Spreadsheets", "xlsx": "Spreadsheets", "csv": "Spreadsheets",
            "jpg": "Images", "jpeg": "Images", "png": "Images", "gif": "Images",
            "ppt": "Presentations", "pptx": "Presentations",
            "zip": "Archives", "rar": "Archives", "7z": "Archives",
        }
        return type_map.get(ext, "Other")

    def _file_exists_in_folder(self, filename: str, folder_id: str) -> bool:
        try:
            query = f"name='{self._q(filename)}' and '{folder_id}' in parents and trashed=false"
            files = self.api_call(
                lambda: self.drive_service.files().list(q=query, fields='files(id, name)').execute(),
                "Drive duplicate check").get('files', [])
            return len(files) > 0
        except NetworkDown:
            # Never silently return False here: that would upload a duplicate on the next line.
            raise
        except Exception as e:
            self.log(f"Duplicate check failed for {filename} (treating as not present): {str(e)}", "WARNING")
            return False

    # ====================== UPDATED PDF WORKFLOW (FIXED) ======================
    def process_pdf_workflow(self, config: dict, progress_callback=None, status_callback=None):
        try:
            engine = config.get('extraction_engine', 'local')
            client = None
            if engine == 'local':
                if not PDFPLUMBER_AVAILABLE:
                    self.log("pdfplumber not installed. Run: pip install pdfplumber", "ERROR")
                    return {'success': False, 'processed': 0}
            elif engine == 'llama':
                if not LLAMA_AVAILABLE:
                    self.log("llama-cloud not installed. Run: pip install llama-cloud>=2.1", "ERROR")
                    return {'success': False, 'processed': 0}

                if not config.get('llama_api_key'):
                    self.log(
                        "LLAMA_API_KEY is not set. Export it (or put it in a local .env) before running; "
                        "the key must never be committed to source.",
                        "ERROR",
                    )
                    return {'success': False, 'processed': 0}
                client = LlamaCloud(api_key=config['llama_api_key'])
            else:
                self.log(f"Unknown EXTRACTION_ENGINE '{engine}' (use 'local' or 'llama')", "ERROR")
                return {'success': False, 'processed': 0}

            if status_callback: status_callback("Starting PDF processing workflow...")
            self.log(f"Starting PDF processing workflow (engine={engine})...", "INFO")
            if progress_callback: progress_callback(20)

            if progress_callback: progress_callback(40)

            # Always fetch existing file IDs and filenames to prevent any duplication
            existing_file_ids = self.get_existing_file_ids(config['spreadsheet_id'], config['sheet_range'])
            existing_file_names = self.get_existing_filenames(config['spreadsheet_id'], config['sheet_range'])
            pdf_files = self._list_drive_files(config['drive_folder_id'], config['days_back'])

            # Re-queue anything previously quarantined. Those files are older than the days_back
            # window by definition, so without this they would be dropped forever.
            quarantine_range = CONFIG['quarantine']['sheet_range']
            recent_ids = {f['id'] for f in pdf_files}
            requeued = [
                f for f in self._get_quarantined_files(config['spreadsheet_id'], quarantine_range)
                if f['id'] not in recent_ids and f['id'] not in existing_file_ids
            ]
            if requeued:
                self.log(
                    f"Re-queueing {len(requeued)} previously quarantined PDF(s): "
                    + ", ".join(f['name'] for f in requeued),
                    "INFO",
                )
                pdf_files = pdf_files + requeued

            self.current_stats['pdf']['total_pdfs'] = len(pdf_files)

            skipped = []
            files_to_process = []
            for f in pdf_files:
                if f['id'] in existing_file_ids:
                    self.log(f"Skipping (drive_file_id already in sheet): {f['name']}", "INFO")
                    skipped.append(f)
                elif f['name'] in existing_file_names:
                    self.log(f"Skipping (filename already in sheet): {f['name']}", "INFO")
                    skipped.append(f)
                else:
                    files_to_process.append(f)

            self.current_stats['pdf']['skipped_pdfs'] += len(skipped)
            files_to_process = files_to_process[:config.get('max_files', len(files_to_process))]

            if not files_to_process:
                self.log("No PDF files to process", "WARNING")
                return {'success': True, 'processed': 0}

            self.log(
                f"Processing {len(files_to_process)} PDFs via "
                + ("local parser" if engine == 'local' else "LlamaCloud Extract v2 (tier=agentic)"),
                "INFO",
            )

            processed_count = 0
            total_rows = 0
            sheet_name = config['sheet_range'].split('!')[0]
            # Track files that failed validation after all retries
            failed_extractions: List[Dict] = []

            for i, file in enumerate(files_to_process):
                try:
                    if status_callback: status_callback(f"Processing PDF {i+1}/{len(files_to_process)}: {file['name']}")
                    self.log(f"Processing: {file['name']}", "INFO")

                    # --- Download PDF bytes (kept in memory for reuse across retries) ---
                    pdf_data = self._download_from_drive(file['id'], file['name'])
                    if not pdf_data:
                        self.current_stats['pdf']['failed_pdfs'] += 1
                        continue

                    if engine == 'local':
                        # Deterministic, so no retry loop: parse_po_pdf() raises POParseError unless
                        # the line items add up to the PO total.
                        try:
                            extracted_data = parse_po_pdf(pdf_data)
                        except POParseError as e:
                            self.log(f"[{file['name']}] Local parse failed: {e}", "ERROR")
                            failed_extractions.append({
                                "file_name": file['name'],
                                "drive_file_id": file['id'],
                                "reason": f"local_parse_failed: {e}",
                                "expected_skus": "",
                                "extracted_skus": "",
                            })
                            self.current_stats['pdf']['failed_pdfs'] += 1
                            continue
                        validation_passed = True
                        expected_skus = extracted_skus = len(extracted_data['line_items'])
                        self.log(f"[{file['name']}] Parsed locally: {extracted_skus} line items", "INFO")
                    else:
                        # --- Extract with validation-aware retry loop ---
                        extracted_data, validation_passed, expected_skus, extracted_skus = (
                            self._extract_with_validation_retry(client, pdf_data, file['name'], config)
                        )

                    # --- Guard: never write partial/unvalidated data ---
                    if not validation_passed:
                        self.log(
                            f"[{file['name']}] Skipping Sheets append — validation did not pass.",
                            "ERROR",
                        )
                        failed_extractions.append({
                            "file_name": file['name'],
                            "drive_file_id": file['id'],
                            "reason": "extraction_count_mismatch",
                            "expected_skus": expected_skus,
                            "extracted_skus": extracted_skus,
                        })
                        self.current_stats['pdf']['failed_pdfs'] += 1
                        continue

                    if not extracted_data or not isinstance(extracted_data, dict):
                        self.log(f"No valid extracted data for {file['name']}", "ERROR")
                        failed_extractions.append({
                            "file_name": file['name'],
                            "drive_file_id": file['id'],
                            "reason": "no_extracted_data",
                            "expected_skus": expected_skus,
                            "extracted_skus": 0,
                        })
                        self.current_stats['pdf']['failed_pdfs'] += 1
                        continue

                    # --- Process and append (only reaches here when validated) ---
                    rows = self._process_extracted_data(extracted_data, file)
                    if rows:
                        # A failed append must never be reported as a processed file: the rows are
                        # not in the sheet, so the run is incomplete and the file needs another go.
                        if not self._save_to_sheets(config['spreadsheet_id'], sheet_name, rows):
                            self.log(
                                f"[{file['name']}] Sheets append failed — file quarantined, no rows written.",
                                "ERROR",
                            )
                            failed_extractions.append({
                                "file_name": file['name'],
                                "drive_file_id": file['id'],
                                "reason": "sheets_append_failed",
                                "expected_skus": expected_skus,
                                "extracted_skus": extracted_skus,
                            })
                            self.current_stats['pdf']['failed_pdfs'] += 1
                            continue

                        processed_count += 1
                        self.current_stats['pdf']['processed_pdfs'] += 1
                        self.current_stats['pdf']['rows_added'] += len(rows)
                        total_rows += len(rows)
                        self.log(f"Successfully processed {file['name']}: {len(rows)} rows", "SUCCESS")
                    else:
                        self.log(f"No rows extracted from {file['name']}", "WARNING")
                        failed_extractions.append({
                            "file_name": file['name'],
                            "drive_file_id": file['id'],
                            "reason": "no_line_items",
                            "expected_skus": expected_skus,
                            "extracted_skus": 0,
                        })
                        self.current_stats['pdf']['failed_pdfs'] += 1

                    if progress_callback:
                        progress = 40 + (i + 1) / len(files_to_process) * 55
                        progress_callback(int(progress))

                except NetworkDown as e:
                    self.log(f"Aborting PDF workflow at file {i+1}/{len(files_to_process)} — {e}", "ERROR")
                    raise
                except Exception as e:
                    self.log(f"Failed to process PDF {file['name']}: {str(e)}", "ERROR")
                    # Quarantine so the file is retried once it falls out of the days_back window
                    # (e.g. LlamaCloud 402 "credits exhausted" used to drop files silently).
                    failed_extractions.append({
                        "file_name": file['name'],
                        "drive_file_id": file['id'],
                        "reason": f"error: {str(e)[:200]}",
                        "expected_skus": "",
                        "extracted_skus": "",
                    })
                    self.current_stats['pdf']['failed_pdfs'] += 1

            # --- Summary of files that could not be validated, persisted for the next run ---
            if failed_extractions:
                self.log(
                    f"Extraction failures ({len(failed_extractions)} file(s)): "
                    + ", ".join(f['file_name'] for f in failed_extractions),
                    "ERROR",
                )
                self._record_quarantine(
                    config['spreadsheet_id'],
                    CONFIG['quarantine']['sheet_range'],
                    failed_extractions,
                )

            if progress_callback: progress_callback(100)
            if status_callback: status_callback(f"PDF workflow completed! Processed {processed_count} PDFs")
            self.log(f"PDF workflow completed! Processed {processed_count} PDFs, added {total_rows} rows", "SUCCESS")
            return {'success': True, 'processed': processed_count, 'rows_added': total_rows}

        except NetworkDown as e:
            self.log(f"PDF workflow aborted (network down): {str(e)}", "ERROR")
            return {'success': False, 'processed': 0, 'network_down': True}
        except Exception as e:
            self.log(f"PDF workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'processed': 0}

    def _list_drive_files(self, folder_id: str, days_back: int) -> List[Dict]:
        try:
            # Fix deprecation warning: use timezone-aware UTC
            start_datetime = datetime.now(timezone.utc) - timedelta(days=days_back - 1)
            start_str = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
            query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false and createdTime >= '{start_str}'"
            all_files = []
            page_token = None
            while True:
                results = self.api_call(lambda: self.drive_service.files().list(
                    q=query, fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    orderBy="createdTime desc", pageSize=1000, pageToken=page_token
                ).execute(), "Drive file list")
                all_files.extend(results.get('files', []))
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            return all_files
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to list files: {str(e)}", "ERROR")
            return []

    def _download_from_drive(self, file_id: str, file_name: str) -> bytes:
        try:
            return self.api_call(
                lambda: self.drive_service.files().get_media(fileId=file_id).execute(),
                f"Drive download ({file_name})")
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to download {file_name}: {str(e)}", "ERROR")
            return b""

    def _process_extracted_data(self, extracted_data: Dict, file_info: Dict) -> List[Dict]:
        rows = []
        # Handle both direct dict and nested "data" wrapper
        data = extracted_data.get("data", extracted_data)
        line_items = data.get("line_items") or data.get("items")
        if not line_items:
            self.log(f"Skipping (no line_items): {file_info['name']}", "WARNING")
            return rows

        # Header fields
        po_number = self._get_value(data, ["purchase_order_number", "po_number", "PO No"])
        po_date = self._get_value(data, ["purchase_order_date", "po_date"])
        expected_delivery_date = self._get_value(data, ["expected_delivery_date", "delivery_date"])
        vendor_id = self._get_value(data, ["vendor_id"])
        account_number = self._get_value(data, ["account_number"])
        delivery_charge = self._get_value(data, ["delivery_charge"])
        total_amount = self._get_value(data, ["total_amount"])
        total_taxable_value = self._get_value(data, ["total_taxable_value_overall"])
        total_tax_amount = self._get_value(data, ["total_tax_amount_overall"])
        amount_chargeable_in_words = self._get_value(data, ["amount_chargeable_in_words"])

        buyer = data.get("buyer", {}) or {}
        supplier = data.get("supplier", {}) or {}

        for item in line_items:
            row = {
                "po_number": po_number,
                "po_date": po_date,
                "expected_delivery_date": expected_delivery_date,
                "vendor_id": vendor_id,
                "account_number": account_number,
                "delivery_charge": delivery_charge,
                "total_amount": total_amount,
                "total_taxable_value_overall": total_taxable_value,
                "total_tax_amount_overall": total_tax_amount,
                "amount_chargeable_in_words": amount_chargeable_in_words,
                "buyer_name": buyer.get("name", ""),
                "buyer_gstin": buyer.get("gstin", ""),
                "buyer_state": buyer.get("state", ""),
                "buyer_address": buyer.get("address", ""),
                "supplier_name": supplier.get("name", ""),
                "supplier_gstin": supplier.get("gstin", ""),
                "supplier_phone": supplier.get("phone", ""),
                "supplier_address": supplier.get("address", ""),
                "product_number": item.get("product_number", ""),
                "product_name": item.get("product_name", ""),
                "description": item.get("description", ""),
                "hsn": item.get("hsn", ""),
                "unit_of_measure": item.get("unit_of_measure", ""),
                "quantity_ordered": item.get("quantity_ordered", ""),
                "mrp": item.get("mrp", ""),
                "price_per_unit": item.get("price_per_unit", ""),
                "margin_percentage": item.get("margin_percentage", ""),
                "gst_rate": item.get("gst_rate", ""),
                "total_amount_item": item.get("total_amount_item", ""),
                "total_tax_amount_item": item.get("total_tax_amount_item", ""),
                "source_file": file_info['name'],
                "drive_file_id": file_info['id'],
                "processed_date": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            rows.append(row)
        return rows

    def _get_value(self, data, possible_keys, default=""):
        for key in possible_keys:
            if key in data:
                return data[key]
        return default

    # ====================== EXTRACTION VALIDATION HELPERS ======================

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """
        Extract raw text from a PDF given its bytes.
        Uses pdfplumber (preferred). Falls back to an empty string on any error
        so that validation is skipped gracefully when text extraction is unavailable.
        """
        if not PDFPLUMBER_AVAILABLE:
            self.log("pdfplumber not available — SKU count validation will be skipped.", "WARNING")
            return ""
        try:
            from io import BytesIO
            text_parts = []
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    text_parts.append(page_text)
            full_text = "\n".join(text_parts)
            self.log(f"Extracted {len(full_text)} chars of raw PDF text for validation.", "INFO")
            return full_text
        except Exception as e:
            self.log(f"PDF text extraction failed (validation will be skipped): {e}", "WARNING")
            return ""

    def get_expected_sku_count(self, pdf_text: str, pdf_bytes: bytes = b"") -> int:
        """
        Count the number of distinct product line-items in a HyperPure PO.

        Two strategies are attempted in order; the first one that yields a result wins.

        Strategy 1 — pdfplumber table extraction (most reliable):
            Iterates every table cell across all pages and counts rows whose first
            cell is exactly a 6-digit integer (the HyperPure product number format).
            This is immune to header/footer/address/total noise because those rows
            never have a bare 6-digit integer in column 0.

        Strategy 2 — raw-text line pattern (fallback if table extraction returns nothing):
            Counts lines in the raw PDF text that begin with a 6-digit number AND
            also contain an 8-digit HSN code starting with "19" on the same line
            (e.g. "168338 19054000 55 0.3 ...").  The two-token requirement eliminates
            PIN codes (562162 562162), account numbers, and totals that are also
            6-digit-ish but never co-occur with an HSN on the same line.

        Returns 0 when neither strategy produces a result, which instructs the caller
        to skip validation for this file (non-blocking).
        """
        # --- Strategy 1: table-based (requires pdf_bytes) ---
        if pdf_bytes and PDFPLUMBER_AVAILABLE:
            try:
                from io import BytesIO as _BytesIO
                PRODUCT_NUM_RE = re.compile(r'^\d{6}$')
                table_count = 0
                with pdfplumber.open(_BytesIO(pdf_bytes)) as pdf:
                    for page in pdf.pages:
                        for table in page.extract_tables():
                            for row in table:
                                first_cell = (row[0] or "").strip()
                                if PRODUCT_NUM_RE.match(first_cell):
                                    table_count += 1
                if table_count >= 1:
                    self.log(f"SKU count (table extraction): {table_count}", "INFO")
                    return table_count
            except Exception as e:
                self.log(f"Table-based SKU count failed, falling back to text: {e}", "WARNING")

        # --- Strategy 2: raw-text line pattern (fallback) ---
        # Each HyperPure PO line-item produces a raw-text line that:
        #   • starts with the 6-digit product number, AND
        #   • contains the 8-digit HSN code (always starts with "19") on the same line.
        # No header, footer, address, PIN code, or total row matches both conditions.
        if pdf_text and pdf_text.strip():
            LINEITEM_RE = re.compile(r'(?m)^\s*\d{6}\b.*\b19\d{6}\b')
            matches = LINEITEM_RE.findall(pdf_text)
            if matches:
                self.log(f"SKU count (text line-pattern fallback): {len(matches)}", "INFO")
                return len(matches)

        self.log(
            "Could not determine expected SKU count — validation will be skipped for this file.",
            "WARNING",
        )
        return 0

    def validate_extraction(
        self,
        expected_count: int,
        extracted_items: list,
        file_name: str,
    ) -> bool:
        """
        Compare the expected SKU count (from raw PDF text) against the number of
        extracted line items.

        Returns True  → counts match within EXTRACTION_TOLERANCE; safe to append.
        Returns False → mismatch detected; caller must retry or skip.

        A return of True is also given when expected_count == 0 (i.e. we could not
        determine the expected count), so that validation is non-blocking in that case.
        """
        extracted_count = len(extracted_items) if extracted_items else 0

        if expected_count == 0:
            self.log(
                f"[{file_name}] SKU count validation skipped (expected count undetermined). "
                f"Extracted: {extracted_count} items.",
                "WARNING",
            )
            return True  # non-blocking: proceed with whatever was extracted

        diff = abs(expected_count - extracted_count)
        if diff <= EXTRACTION_TOLERANCE:
            self.log(
                f"[{file_name}] Validation PASSED — "
                f"Expected SKUs: {expected_count} | Extracted SKUs: {extracted_count}",
                "SUCCESS",
            )
            return True
        else:
            self.log(
                f"[{file_name}] Validation FAILED — "
                f"Expected SKUs: {expected_count} | Extracted SKUs: {extracted_count} | "
                f"Difference: {diff} (tolerance: {EXTRACTION_TOLERANCE})",
                "ERROR",
            )
            return False

    def _run_llama_extraction(self, client, file_bytes: bytes, file_name: str, config: dict) -> Optional[Dict]:
        """
        Upload the PDF to LlamaCloud and poll until the extraction job completes.
        Returns the raw extracted_data dict on success, or None on failure/timeout.
        This is extracted into its own method so the validation-retry loop can call it
        multiple times without duplicating the upload+poll logic.
        """
        import tempfile, os as _os

        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        extracted_data = None
        try:
            def _upload():
                with open(tmp_path, "rb") as f:   # re-open per attempt so a retry never sends a half-read file
                    return client.files.create(file=f, purpose="extract")
            file_obj = self.api_call(_upload, "LlamaCloud file upload")
            file_id = file_obj.id

            # Extract v2 has no "agent" concept (`agent_name` returns HTTP 422). Either a saved
            # configuration id (preferred — see EXTRACT_DATA_SCHEMA / LLAMA_EXTRACT_CONFIG_ID) or an
            # inline configuration is the complete v2 request.
            config_id = config.get('llama_extract_config_id') or ""
            if config_id:
                self.log(f"Using saved LlamaCloud extraction config {config_id}", "INFO")
                job = self.api_call(
                    lambda: client.extract.create(file_input=file_id, configuration_id=config_id),
                    "LlamaCloud extract create")
            else:
                self.log("No saved extraction config id — sending inline schema", "INFO")
                job = self.api_call(lambda: client.extract.create(
                    file_input=file_id,
                    configuration={
                        "extraction_target": EXTRACT_TARGET,
                        "tier": EXTRACT_TIER,
                        "data_schema": EXTRACT_DATA_SCHEMA,
                    },
                ), "LlamaCloud extract create")

            # Poll for completion
            max_wait = 300
            wait_interval = 10
            elapsed = 0
            while elapsed < max_wait:
                job = self.api_call(lambda: client.extract.get(job.id), "LlamaCloud extract poll")
                status = (job.status or "").upper()
                if status in ("SUCCESS", "COMPLETED"):
                    self.log(f"Extraction completed for {file_name} (status={job.status})", "SUCCESS")
                    extracted_data = (
                        getattr(job, 'extract_result', None)
                        or getattr(job, 'data', None)
                        or getattr(job, 'result', None)
                        or getattr(getattr(job, 'extraction', None), 'data', None)
                        or getattr(getattr(job, 'output', None), 'data', None)
                        or getattr(job, 'extracted_data', None)
                        or getattr(job, 'json_result', None)
                    )
                    if extracted_data is None:
                        self.log(f"WARNING: Could not find data in job. Full job: {vars(job)}", "WARNING")
                    break
                elif status in ("FAILED", "ERROR", "CANCELLED"):
                    self.log(f"Extraction failed for {file_name}: status={job.status}", "ERROR")
                    break
                self.log(f"Extraction status: {job.status} ({elapsed}s elapsed)", "INFO")
                time.sleep(wait_interval)
                elapsed += wait_interval
            else:
                self.log(f"Extraction timed out after {max_wait}s for {file_name}", "ERROR")

        finally:
            if _os.path.exists(tmp_path):
                _os.unlink(tmp_path)

        return extracted_data

    def _extract_with_validation_retry(
        self,
        client,
        pdf_bytes: bytes,
        file_name: str,
        config: dict,
    ) -> tuple[Optional[Dict], bool, int, int]:
        """
        Run LlamaCloud extraction with automatic retry when the extracted line-item
        count does not match the expected SKU count from the raw PDF text.

        Returns:
            (extracted_data, validation_passed, expected_count, extracted_count)
            - extracted_data : the best result dict obtained (may be None)
            - validation_passed : True if counts matched (or validation was skipped)
            - expected_count / extracted_count : recorded on the quarantine row so a
              failure can be diagnosed without re-running the extraction

        Callers must check validation_passed before writing to Sheets.
        """
        # Step 1 — get expected count from raw PDF text (done once, before any retries)
        pdf_text = self._extract_pdf_text(pdf_bytes)
        expected_count = self.get_expected_sku_count(pdf_text, pdf_bytes)

        if expected_count > 0:
            self.log(
                f"[{file_name}] Expected SKU count from PDF: {expected_count}",
                "INFO",
            )
        else:
            self.log(
                f"[{file_name}] Could not determine expected SKU count — "
                "extraction will proceed without count validation.",
                "WARNING",
            )

        last_extracted_data: Optional[Dict] = None
        last_extracted_count = 0

        for attempt in range(1, EXTRACTION_MAX_RETRIES + 1):
            if attempt > 1:
                self.log(
                    f"[{file_name}] Retry attempt {attempt - 1}/{EXTRACTION_MAX_RETRIES - 1} — "
                    f"waiting {EXTRACTION_RETRY_WAIT}s before re-extraction...",
                    "WARNING",
                )
                time.sleep(EXTRACTION_RETRY_WAIT)

            self.log(f"[{file_name}] Running extraction (attempt {attempt}/{EXTRACTION_MAX_RETRIES})...", "INFO")
            extracted_data = self._run_llama_extraction(client, pdf_bytes, file_name, config)

            if not extracted_data or not isinstance(extracted_data, dict):
                self.log(f"[{file_name}] Attempt {attempt}: extraction returned no data.", "WARNING")
                continue

            last_extracted_data = extracted_data

            # Step 2 — count extracted line items
            data = extracted_data.get("data", extracted_data)
            line_items = data.get("line_items") or data.get("items") or []

            last_extracted_count = len(line_items)

            # Step 3 — validate
            passed = self.validate_extraction(expected_count, line_items, file_name)
            if passed:
                return extracted_data, True, expected_count, last_extracted_count

            self.log(
                f"[{file_name}] Attempt {attempt}/{EXTRACTION_MAX_RETRIES} validation failed. "
                f"Expected: {expected_count} | Got: {len(line_items)}",
                "WARNING",
            )

        # All retries exhausted
        self.log(
            f"[{file_name}] All {EXTRACTION_MAX_RETRIES} extraction attempts failed validation. "
            "File will be added to failed list — NO data appended to Sheets.",
            "ERROR",
        )
        return last_extracted_data, False, expected_count, last_extracted_count

    def _save_to_sheets(self, spreadsheet_id: str, sheet_name: str, rows: List[Dict]) -> bool:
        """Append rows under the canonical header. Returns True only when the append
        actually succeeded — the caller relies on this to decide whether the file counts
        as processed."""
        try:
            if not rows: return False
            self._ensure_sheet_exists(spreadsheet_id, sheet_name)

            # STATIC_POS_HEADERS is the single source of truth — extracted data never changes this
            all_headers = STATIC_POS_HEADERS

            existing_headers = self._get_sheet_headers(spreadsheet_id, sheet_name)
            if existing_headers != all_headers:
                # Header is missing, empty, or has drifted — always restore the canonical header
                self._update_headers(spreadsheet_id, sheet_name, all_headers)
                if existing_headers:
                    self.log(
                        f"Header mismatch detected on sheet '{sheet_name}' — restored static headers. "
                        f"Was: {existing_headers[:5]}...",
                        "WARNING"
                    )
                else:
                    self.log(f"Sheet '{sheet_name}' was empty — wrote static headers.", "INFO")

            # Build data-only rows aligned to the fixed column order; missing keys become ""
            values = [[row.get(h, "") for h in all_headers] for row in rows]
            return self._append_to_google_sheet(spreadsheet_id, sheet_name, values)
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to save to sheets: {str(e)}", "ERROR")
            return False

    def _get_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> List[str]:
        try:
            last_col = self._col_to_letter(max(len(STATIC_POS_HEADERS), 26))
            result = self.api_call(lambda: self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1:{last_col}1", majorDimension="ROWS"
            ).execute(), "Sheets header read")
            return result.get('values', [[]])[0]
        except NetworkDown:
            raise
        except Exception:
            return []

    def _col_to_letter(self, n: int) -> str:
        """Convert 1-based column index to spreadsheet letters, e.g. 1→A, 26→Z, 27→AA."""
        result = ""
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def _update_headers(self, spreadsheet_id: str, sheet_name: str, headers: List[str]) -> bool:
        try:
            body = {'values': [headers]}
            last_col = self._col_to_letter(len(headers))
            self.api_call(lambda: self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:{last_col}1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute(), "Sheets header update")
            self.log(f"Updated headers with {len(headers)} columns", "INFO")
            return True
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to update headers: {str(e)}", "ERROR")
            return False

    def _get_sheet_data(self, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
        try:
            result = self.api_call(lambda: self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=sheet_name, majorDimension="ROWS"
            ).execute(), "Sheets data read")
            return result.get('values', [])
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to get sheet data: {str(e)}", "ERROR")
            return []

    def _append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> bool:
        for attempt in range(1, 4):
            try:
                body = {'values': values}
                self.api_call(lambda: self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id, range=range_name, valueInputOption='USER_ENTERED', body=body
                ).execute(), "Sheets append")
                return True
            except NetworkDown:
                raise
            except Exception as e:
                if attempt < 3:
                    time.sleep(2)
                else:
                    self.log(f"Failed to append after 3 attempts: {str(e)}", "ERROR")
                    return False
        return False

    def _ensure_sheet_exists(self, spreadsheet_id: str, sheet_name: str):
        try:
            metadata = self.api_call(lambda: self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute(),
                                     "Sheets metadata")
            existing = [s['properties']['title'] for s in metadata.get('sheets', [])]
            if sheet_name not in existing:
                body = {'requests': [{'addSheet': {'properties': {'title': sheet_name}}}]}
                self.api_call(lambda: self.sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute(),
                              "Sheets add tab")
                self.log(f"Created sheet tab: {sheet_name}", "INFO")
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to ensure sheet exists: {str(e)}", "WARNING")

    # ====================== QUARANTINE (durable failure list) ======================

    QUARANTINE_HEADERS = [
        "quarantined_at", "file_name", "drive_file_id", "reason", "expected_skus", "extracted_skus",
    ]

    def _record_quarantine(self, spreadsheet_id: str, sheet_range: str, entries: List[Dict]):
        """Persist files that could not be extracted or appended.

        Without this the failure list lived only in memory, so a file that failed all
        extraction attempts fell out of the days_back window and was never retried.
        """
        try:
            if not entries:
                return
            sheet_name = sheet_range.split('!')[0]
            self._ensure_sheet_exists(spreadsheet_id, sheet_name)

            existing_headers = self._get_sheet_headers(spreadsheet_id, sheet_name)
            if existing_headers != self.QUARANTINE_HEADERS:
                self._update_headers(spreadsheet_id, sheet_name, self.QUARANTINE_HEADERS)

            now = time.strftime("%Y-%m-%d %H:%M:%S")
            rows = [[
                now,
                e.get("file_name", ""),
                e.get("drive_file_id", ""),
                e.get("reason", ""),
                e.get("expected_skus", ""),
                e.get("extracted_skus", ""),
            ] for e in entries]
            if self._append_to_google_sheet(spreadsheet_id, sheet_name, rows):
                self.log(f"Quarantined {len(rows)} file(s) to '{sheet_name}' for retry next run", "WARNING")
            else:
                self.log(f"Could not write quarantine rows to '{sheet_name}'", "ERROR")
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to record quarantine: {str(e)}", "ERROR")

    def _get_quarantined_files(self, spreadsheet_id: str, sheet_range: str) -> List[Dict]:
        """Read the quarantine tab and return unique {id, name} entries to re-attempt."""
        try:
            sheet_name = sheet_range.split('!')[0]
            # Create the tab on first use. Without this the read below fails with
            # "Unable to parse range", which _get_sheet_data reports as an ERROR on every
            # run of a perfectly healthy install that has simply never quarantined anything.
            self._ensure_sheet_exists(spreadsheet_id, sheet_name)
            values = self._get_sheet_data(spreadsheet_id, sheet_name)
            if not values or len(values) < 2:
                return []
            headers = values[0]
            try:
                name_col = headers.index("file_name")
                id_col = headers.index("drive_file_id")
            except ValueError:
                self.log(f"Quarantine sheet '{sheet_name}' has unexpected headers — ignoring", "WARNING")
                return []

            seen = set()
            out = []
            for row in values[1:]:
                if len(row) <= max(name_col, id_col):
                    continue
                file_id = (row[id_col] or "").strip()
                file_name = (row[name_col] or "").strip()
                if not file_id or file_id in seen:
                    continue
                seen.add(file_id)
                out.append({"id": file_id, "name": file_name})
            return out
        except NetworkDown:
            raise
        except Exception as e:
            self.log(f"Failed to read quarantine list: {str(e)}", "WARNING")
            return []

    def _log_workflow_to_sheet(self, spreadsheet_id: str, sheet_range: str, start_time: datetime, end_time: datetime, stats: Dict):
        try:
            sheet_name = sheet_range.split('!')[0] if '!' in sheet_range else sheet_range
            self._ensure_sheet_exists(spreadsheet_id, sheet_name)
            headers = [
                "Start Time", "End Time",
                "Gmail Processed Emails", "Gmail Total Attachments", "Gmail Successful Uploads", "Gmail Failed Uploads",
                "PDF Total PDFs", "PDF Processed PDFs", "PDF Skipped PDFs", "PDF Failed PDFs", "PDF Rows Added"
            ]
            log_row = [
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                stats['gmail']['processed_emails'],
                stats['gmail']['total_attachments'],
                stats['gmail']['successful_uploads'],
                stats['gmail']['failed_uploads'],
                stats['pdf']['total_pdfs'],
                stats['pdf']['processed_pdfs'],
                stats['pdf']['skipped_pdfs'],
                stats['pdf']['failed_pdfs'],
                stats['pdf']['rows_added']
            ]
            values = self._get_sheet_data(spreadsheet_id, sheet_name)
            if not values or values[0] != headers:
                self._append_to_google_sheet(spreadsheet_id, sheet_range, [headers])
            self._append_to_google_sheet(spreadsheet_id, sheet_range, [log_row])
            self.log("Workflow logged to sheet", "SUCCESS")
        except Exception as e:
            self.log(f"Failed to log workflow: {str(e)}", "ERROR")


def run_combined_workflow(automation):
    start_time = datetime.now()
    automation.log("Starting combined workflow...")
    automation.reset_stats()
    
    gmail_config = CONFIG['gmail']
    pdf_config = CONFIG['pdf']
    
    def update_progress(value): automation.log(f"Progress: {value}%")
    def update_status(message): automation.log(message)
    
    gmail_result = automation.process_gmail_workflow(gmail_config, progress_callback=update_progress, status_callback=update_status)
    
    if not gmail_result.get('success'):
        automation.log("Gmail workflow failed", "ERROR")
        end_time = datetime.now()
        automation._log_workflow_to_sheet(pdf_config['spreadsheet_id'], CONFIG['log']['sheet_range'], start_time, end_time, automation.current_stats)
        return {"status": "failed"}
    
    pdf_result = automation.process_pdf_workflow(pdf_config, progress_callback=update_progress, status_callback=update_status)
    
    end_time = datetime.now()
    if not pdf_result.get('success'):
        automation.log("PDF workflow failed — run NOT complete; unprocessed PDFs will be retried next run", "ERROR")
        automation._log_workflow_to_sheet(pdf_config['spreadsheet_id'], CONFIG['log']['sheet_range'], start_time, end_time, automation.current_stats)
        return {"status": "failed", "gmail_attachments": gmail_result.get('processed', 0)}

    automation.log(f"Combined workflow completed! Gmail: {gmail_result.get('processed',0)} attachments | PDF: {pdf_result.get('processed',0)} PDFs, {pdf_result.get('rows_added',0)} rows", "SUCCESS")
    automation._log_workflow_to_sheet(pdf_config['spreadsheet_id'], CONFIG['log']['sheet_range'], start_time, end_time, automation.current_stats)
    
    return {
        "status": "success",
        "gmail_attachments": gmail_result.get('processed', 0),
        "pdf_pdfs": pdf_result.get('processed', 0),
        "pdf_skipped": automation.current_stats['pdf']['skipped_pdfs'],
        "pdf_rows": pdf_result.get('rows_added', 0)
    }


def main():
    automation = MilkbasketAutomation()
    if not automation.authenticate():
        logger.error("Authentication failed. Exiting.")
        sys.exit(1)

    result = run_combined_workflow(automation)
    logger.info(f"Workflow result: {result}")

    # `python app.py --once` (used by the GitHub Actions schedule): single run, and a non-zero
    # exit code on failure so the Actions run shows red.
    if "--once" in sys.argv:
        sys.exit(0 if result.get("status") == "success" else 1)

    schedule.every(3).hours.do(run_combined_workflow, automation)
    logger.info("Scheduler started. Running every 3 hours.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    main()