# HyperPO

Pulls Hyperpure purchase-order emails from Gmail, saves the PDF/XLSX attachments to Google Drive,
parses each PO PDF and appends one row per line item to the Google Sheet.

## How it runs

GitHub Actions (`.github/workflows/hyperpo.yml`) runs `python app.py --once` every 3 hours.
It can also be started by hand from the **Actions** tab → **HyperPO sync** → **Run workflow**.
Nobody needs to keep a machine running.

PDFs are parsed locally with `pdfplumber` (`EXTRACTION_ENGINE=local`) — no LlamaCloud key or credits.
Each PO is only written if its line items add up to the PO total; anything that fails is recorded in the
`failed_extractions` tab and retried on every run.

## Setup / maintenance

The only secret is the Google OAuth token, stored as the repository secret `GOOGLE_TOKEN_JSON`
(the full contents of `token.json`).

If a run fails with *"Google token is missing, expired or revoked"*:

1. Locally, with `credentials.json` present: `python generate_token.py` (opens a browser to sign in),
   which writes `token_encoded.txt`; or run `python app.py` once to produce `token.json`.
2. Update the secret: `gh secret set GOOGLE_TOKEN_JSON < token.json`

Never commit `token.json`, `credentials.json` or `.env` — they are in `.gitignore`.

## Running locally

```
pip install -r requirements.txt
python app.py --once      # single run
python app.py             # run now, then every 3 hours
```
