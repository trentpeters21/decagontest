#!/usr/bin/env python3
"""
Voice Conversations → Google Sheet Appender

Runs the warehouse SQL query via Satori CLI, then appends ONLY new
conversations to a specific Google Sheet tab.

Environment variables used for Google auth (first found wins):
- GOOGLE_SERVICE_ACCOUNT_JSON: Inline JSON credentials
- GOOGLE_APPLICATION_CREDENTIALS: Path to service account JSON file
- Otherwise, falls back to gspread.service_account() which looks for
  service_account.json in the working directory.
"""

import os
import json
import subprocess
from typing import List, Dict, Optional

import requests
from dotenv import load_dotenv

# Third-party for Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials
except Exception:
    gspread = None  # Will error later with actionable message
    ServiceAccountCredentials = None


# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv('.env')

# Sheet config (provided by user)
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID', '1JHZ1eqoQBIHbNqRvx902x10wOgqhL_N4QLnEp834x1k')
GOOGLE_SHEET_TAB = os.getenv('GOOGLE_SHEET_TAB', 'AI_Voice_QA')

# Optional: service account to share sheet with (for info/logging)
SERVICE_ACCOUNT_ID = os.getenv('GOOGLE_SERVICE_ACCOUNT_ID', 'ai-experiences@decagon-461902.iam.gserviceaccount.com')

# Warehouse query inputs
SQL_QUERY_FILE = 'voice_conversations_gsheet_query.sql'


# ── Warehouse query helpers ────────────────────────────────────────────────────
def load_sql_query() -> Optional[str]:
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.join(base_dir, SQL_QUERY_FILE)
        with open(sql_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        print(f"SQL query file {SQL_QUERY_FILE} not found")
        return None


def run_satori_query(query: str) -> Optional[str]:
    """Run query via Satori CLI and return stdout string, or None on error."""
    try:
        print("Executing query via Satori CLI…")
        cmd = [
            'satori', 'run', 'psql',
            '--no-launch-browser',
            'Redshift - Prod',
            'pantheon',
            # Force unaligned, pipe-separated, tuples only (no headers/footers)
            '-A', '-F', '|', '-t',
            '-c', query,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"Satori CLI error: {result.stderr}")
            return None
        return result.stdout
    except subprocess.TimeoutExpired:
        print("Query timed out after 120 seconds")
        return None
    except Exception as e:
        print(f"Error running Satori query: {e}")
        return None


def parse_psql_results(output: str) -> List[Dict[str, str]]:
    """Parse psql tabular output into list of conversation dicts."""
    lines = (output or '').strip().split('\n')
    conversations: List[Dict[str, str]] = []
    if not lines:
        return conversations

    # With -A -F '|' -t, each data row is a single line with 9 fields.
    data_lines = [l for l in lines if l.strip()]

    for line in data_lines:
        parts = [part.strip() for part in line.split('|')]
        # Expect 9 columns per the new query
        if len(parts) >= 9:
            conv = {
                'decagon_conversation_link': parts[0],
                'five9_conversation_link': parts[1],
                'zendesk_ticket_link': parts[2],
                'routing_department': parts[3],
                'skill': parts[4],
                'abandoned': parts[5],
                'created_at_est': parts[6],
                'created_at_utc': parts[7],
                'five9_session_id': parts[8],
            }
            conversations.append(conv)
    if not conversations:
        # Emit a small snippet for debugging
        preview = '\n'.join(lines[:10])
        print("No rows parsed. First lines of output:\n" + preview)
    return conversations


# ── Google Sheets helpers ──────────────────────────────────────────────────────
SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]


def get_gspread_client():
    if gspread is None:
        raise RuntimeError(
            "gspread/google-auth not installed. Please add gspread and google-auth to requirements and install."
        )

    # 1) Inline JSON via env var
    inline_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
    if inline_json:
        try:
            info = json.loads(inline_json)
            creds = ServiceAccountCredentials.from_service_account_info(info, scopes=SCOPE)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"Failed to use GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    # 2) Path via GOOGLE_APPLICATION_CREDENTIALS
    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        try:
            creds = ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPE)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"Failed to use GOOGLE_APPLICATION_CREDENTIALS file: {e}")

    # 3) Default: gspread.service_account() looks for service_account.json in CWD
    try:
        return gspread.service_account()
    except Exception as e:
        raise RuntimeError(
            "No Google credentials found. Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS, "
            "or place service_account.json in working directory."
        ) from e


def open_or_create_worksheet(client, sheet_id: str, tab_name: str):
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=20)
    return ws


HEADERS = [
    'decagon_conversation_link',
    'five9_conversation_link',
    'zendesk_ticket_link',
    'routing_department',
    'skill',
    'abandoned',
    'created_at_est',
    'created_at_utc',
    'five9_session_id',
]


def ensure_headers(ws) -> None:
    values = ws.get_all_values()
    if not values:
        ws.append_row(HEADERS, value_input_option='RAW')
        return
    first = values[0]
    if [h.strip() for h in first] != HEADERS:
        # If headers incorrect, insert at top (do not overwrite user data)
        ws.insert_row(HEADERS, index=1)


def get_existing_ids(ws) -> set:
    values = ws.get_all_values()
    if not values:
        return set()
    # Determine where headers are (assume first row after ensure_headers)
    data_rows = values[1:] if values else []
    existing_ids = {row[0] for row in data_rows if row and len(row) > 0 and row[0]}
    return existing_ids


def append_new_conversations(ws, conversations: List[Dict[str, str]]) -> int:
    existing_ids = get_existing_ids(ws)
    to_append = []
    for c in conversations:
        # Use first column (decagon_conversation_link) as unique key
        cid = c.get('decagon_conversation_link')
        if not cid or cid in existing_ids:
            continue
        row = [c.get(h, '') for h in HEADERS]
        to_append.append(row)

    if not to_append:
        return 0

    # Batch append in chunks to avoid quotas
    CHUNK = 200
    appended = 0
    for i in range(0, len(to_append), CHUNK):
        chunk = to_append[i:i+CHUNK]
        ws.append_rows(chunk, value_input_option='RAW')
        appended += len(chunk)
    return appended


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=== Voice Conversations → Google Sheet (new only) ===")
    print(f"Sheet ID: {GOOGLE_SHEET_ID}")
    print(f"Tab: {GOOGLE_SHEET_TAB}")
    print(f"Service Account (share sheet with this): {SERVICE_ACCOUNT_ID}")

    query = load_sql_query()
    if not query:
        return

    stdout = run_satori_query(query)
    if not stdout:
        print("No results from warehouse")
        return

    conversations = parse_psql_results(stdout)
    print(f"Parsed {len(conversations)} conversations from warehouse output")
    if not conversations:
        print("No conversations parsed")
        return

    try:
        client = get_gspread_client()
        ws = open_or_create_worksheet(client, GOOGLE_SHEET_ID, GOOGLE_SHEET_TAB)
        ensure_headers(ws)
        appended = append_new_conversations(ws, conversations)
        print(f"Appended {appended} new conversations to Google Sheet")
    except Exception as e:
        print(f"Google Sheets error: {e}")
        return

    print("Done!")


if __name__ == "__main__":
    main()


