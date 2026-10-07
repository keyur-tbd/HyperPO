#!/usr/bin/env python3
"""
Simple script to generate token and save both credentials and token as encoded text
"""
import base64
import json
from google_auth_oauthlib.flow import InstalledAppFlow

# Scopes — must match the combined scopes app.py requests, or app.py will silently
# operate with a token whose grants differ from what it expects.
# app.py needs read-only Gmail, drive.file (only files this app creates), and Sheets.
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]

# Read the original credentials file
with open('credentials.json', 'r') as f:
    credentials_data = f.read()

# Encode credentials
credentials_encoded = base64.b64encode(credentials_data.encode('utf-8')).decode('utf-8')

# Save encoded credentials
with open('credentials_encoded.txt', 'w') as f:
    f.write(credentials_encoded)

print("✅ Credentials saved as credentials_encoded.txt")

# Generate token
flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
creds = flow.run_local_server(port=0)

# Convert token to JSON
token_json = creds.to_json()

# Encode token
token_encoded = base64.b64encode(token_json.encode('utf-8')).decode('utf-8')

# Save encoded token
with open('token_encoded.txt', 'w') as f:
    f.write(token_encoded)

print("✅ Token saved as token_encoded.txt")
print(f"Scopes: {creds.scopes}")

# Verify the token carries exactly the scopes app.py expects
granted = set(creds.scopes or [])
missing = set(SCOPES) - granted
if missing:
    print("❌ ERROR: token is missing required scope(s): " + ", ".join(sorted(missing)))
else:
    print("✅ All scopes required by app.py are present")

print("\n📦 Files generated:")
print("  - credentials_encoded.txt (your OAuth credentials)")
print("  - token_encoded.txt (your access token)")
print("\n⚠️  These files contain secrets. They are covered by .gitignore — keep them out of source control.")