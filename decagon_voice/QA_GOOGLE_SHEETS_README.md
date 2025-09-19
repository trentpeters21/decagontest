# Voice Conversations QA with Google Sheets Integration

This system queries voice conversation data, compares it against previously processed conversations, and adds only new conversations to a Google Sheet.

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set up Google Service Account Credentials

#### Option A: Using the setup script (recommended)
```bash
python setup_google_credentials.py
```

#### Option B: Manual setup
1. Download the service account JSON key for `ai-experiences@decagon-461902.iam.gserviceaccount.com`
2. Save it as `decagon-voice-service-account.json` in this directory
3. Set the environment variable:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/decagon-voice-service-account.json
   ```

### 3. Verify Google Sheet Access
Make sure the service account has edit access to:
- **Sheet ID**: `1JHZ1eqoQBIHbNqRvx902x10wOgqhL_N4QLnEp834x1k`
- **Tab**: `Old`

## Usage

### Run the QA Process
```bash
python voice_conversations_qa_google.py
```

## How It Works

1. **Query Data**: Executes the SQL query from `voice_conversations_qa_query.sql` using Satori CLI
2. **Convert to DataFrame**: Converts the results to a pandas DataFrame for easier manipulation
3. **Check Against Existing**: Compares new conversation IDs against:
   - Previously processed conversations (stored in `processed_conversations.json`)
   - Existing conversations in the Google Sheet
4. **Add New Conversations**: Only adds conversations with new `conversation_id` values to the Google Sheet

## Files Created/Modified

- `processed_conversations.json`: Tracks conversation IDs that have been processed
- Google Sheet: Receives new conversation data
- Logs: Console output shows progress and results

## Data Flow

```
Database Query → DataFrame → ID Comparison → Google Sheets (new conversations only)
```

## Troubleshooting

### Google Sheets Access Issues
- Verify service account has edit permissions on the sheet
- Check that the sheet ID and tab name are correct
- Ensure credentials file is valid JSON

### Satori CLI Issues
- Make sure Satori CLI is installed and authenticated
- Verify database connection and query syntax

### Missing Dependencies
```bash
pip install --upgrade -r requirements.txt
```

## Configuration

The system uses these configuration values:
- **Google Sheet ID**: `1JHZ1eqoQBIHbNqRvx902x10wOgqhL_N4QLnEp834x1k`
- **Sheet Tab**: `Old`
- **Service Account**: `ai-experiences@decagon-461902.iam.gserviceaccount.com`
- **Tracking File**: `processed_conversations.json`
