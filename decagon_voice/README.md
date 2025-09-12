# Voice Conversations Data Warehouse Integration

This setup queries voice conversations directly from your data warehouse using Satori CLI and sends them to Workato.

## Files

- `voice_conversations_warehouse_clean.py` - Main script
- `voice_conversations_query.sql` - SQL query (easily editable)
- `.env` - Configuration file
- `voice_conversations_warehouse.json` - Output data
- `last_warehouse_run.json` - Tracks last run for delta updates

## Setup

1. **Configure environment variables** in `.env`:
   ```bash
   SATORI_DATASTORE=Redshift - Prod
   SATORI_DATABASE=pantheon
   WORKATO_WEBHOOK_URL=https://webhooks.workato.com/webhooks/rest/YOUR/WEBHOOK/URL
   ```

2. **Run the script**:
   ```bash
   export PATH="/opt/homebrew/opt/postgresql@14/bin:$PATH"
   python3 voice_conversations_warehouse_clean.py
   ```

## Customizing

- **Modify the query**: Edit `voice_conversations_query.sql`
- **Change date range**: Edit the `days_back` parameter in the script
- **Adjust Workato payload**: Modify the `send_to_workato_webhook()` function

## Features

- Direct database access (no API rate limits)
- Delta runs (only fetches new conversations)
- Modular SQL queries
- Workato webhook integration

## 🔗 Workato Integration

The script sends raw conversation data to your Workato webhook in this format:
```json
{
  "conversation_id": "string",
  "conversation_url": "string", 
  "csat": "string",
  "deflected": "string",
  "summary": "string",
  "created_at": "string",
  "tags": "string",
  "metadata": "string"
}
```
