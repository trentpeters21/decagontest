import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv('.env.local')

API_KEY = os.getenv('DECAGON_API_KEY')
BASE_URL = os.getenv('DECAGON_BASE_URL', 'https://api.decagon.ai')
DECAGON_WEB_URL = os.getenv('DECAGON_WEB_URL', 'https://decagon.ai')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
ENDPOINT = f"{BASE_URL}/conversation/export"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

ET_TZ = ZoneInfo("America/New_York")

# ── Time helpers ───────────────────────────────────────────────────────────────
def parse_iso_utc(ts: str | None) -> datetime | None:
    """Parse ISO8601 (handles 'Z') → aware UTC datetime."""
    if not ts:
        return None
    try:
        ts_norm = ts.replace('Z', '+00:00')
        dt = datetime.fromisoformat(ts_norm)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None

def best_timestamp_utc(conv: dict) -> datetime | None:
    """
    Choose the best timestamp field to evaluate recency.
    Prefer created_at, but fall back to started_at / first_message_at / updated_at.
    """
    for key in ("created_at", "started_at", "first_message_at", "updated_at"):
        dt = parse_iso_utc(conv.get(key))
        if dt:
            return dt
    return None

def format_et(utc_ts: str | None) -> str:
    """Format a UTC ISO timestamp into ET (DST-aware)."""
    if not utc_ts:
        return ''
    dt_utc = parse_iso_utc(utc_ts)
    if not dt_utc:
        return str(utc_ts)
    return dt_utc.astimezone(ET_TZ).strftime('%B %d, %Y %I:%M %p')

# ── Fetch ──────────────────────────────────────────────────────────────────────
def fetch_voice_conversations(days_back: int = 30, max_pages: int | None = None) -> list[dict]:
    """
    Fetch conversations filtered server-side to the voice flow and client-side to the date window.
    """
    if not API_KEY:
        raise RuntimeError("DECAGON_API_KEY is not set")

    cutoff_utc = datetime.now(timezone.utc) - timedelta(days=days_back)
    all_items: list[dict] = []
    seen_ids: set[str] = set()
    cursor = None
    page = 0

    session = requests.Session()

    while True:
        params = {
            # exact flow filter for voice (as requested)
            "user_filters": json.dumps({"flow_filter": ["wealthsimple_voice"]}),
            "limit": 100,
        }
        if cursor:
            params["cursor"] = cursor

        # simple retries for transient errors
        for attempt in range(3):
            try:
                resp = session.get(ENDPOINT, headers=HEADERS, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))

        page += 1
        conversations = data.get("conversations", []) or []

        kept_this_page = 0
        for conv in conversations:
            conv_dt = best_timestamp_utc(conv)
            if not conv_dt or conv_dt < cutoff_utc:
                continue
            conv_id = conv.get("conversation_id") or conv.get("id")
            # if ID truly missing, skip to avoid weird dedup collisions
            if not conv_id:
                continue
            if conv_id in seen_ids:
                continue
            seen_ids.add(conv_id)
            all_items.append(conv)
            kept_this_page += 1

        print(f"[page {page}] fetched {len(conversations)}; kept {kept_this_page}; total so far {len(all_items)}")

        cursor = data.get("next_page_cursor") or data.get("nextCursor") or data.get("next")
        if not cursor:
            break
        if max_pages and page >= max_pages:
            print(f"Stopping early at max_pages={max_pages}")
            break

    return all_items

# ── Slack ──────────────────────────────────────────────────────────────────────
def send_to_slack(conversation: dict) -> bool:
    if not SLACK_WEBHOOK_URL:
        return False

    conv_id = conversation.get('conversation_id') or conversation.get('id', 'N/A')
    created_at_et = format_et(conversation.get('created_at'))
    deflected = str(not conversation.get('undeflected', True))
    csat = conversation.get('csat', 'N/A')
    summary = conversation.get('summary') or '(no summary)'
    conversation_url = f"{DECAGON_WEB_URL}/conversation/{conv_id}"

    payload = {
        "text": ":microphone: New Voice Conversation",
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": ":microphone: New Voice Conversation"}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Date:* {created_at_et}"},
                    {"type": "mrkdwn", "text": f"*Deflected:* {deflected}"},
                    {"type": "mrkdwn", "text": f"*CSAT:* {csat}"},
                    {"type": "mrkdwn", "text": f"*ID:* `{conv_id}`"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Summary:* {summary}"}},
            {
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Conversation"},
                    "url": conversation_url,
                    "style": "primary",
                }],
            },
        ],
    }

    try:
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)
        return 200 <= r.status_code < 300
    except Exception as e:
        print(f"Slack error: {e}")
        return False

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=== Voice Conversations Export ===")
    print("Flow filter: wealthsimple_voice")
    print("Fetching last 30 days (UTC window)…")

    try:
        conversations = fetch_voice_conversations(days_back=30)
    except Exception as e:
        print(f"Fetch failed: {e}")
        return

    total = len(conversations)
    print(f"Found {total} voice conversations in the last 30 days")

    # Save JSON
    out_path = 'voice_conversations_api.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(conversations, f, ensure_ascii=False, indent=2)
    print(f"Saved to {out_path}")

    # Optional Slack fanout
    if SLACK_WEBHOOK_URL:
        print("Sending conversations to Slack…")
        sent = 0
        for conv in conversations:
            if send_to_slack(conv):
                sent += 1
        print(f"Sent {sent}/{total} to Slack")
    else:
        print("No Slack webhook configured; skipping Slack")

    print("Done.")

if __name__ == "__main__":
    main()