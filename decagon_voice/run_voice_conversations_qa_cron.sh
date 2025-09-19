#!/bin/bash
cd /Users/trent.peters/Desktop/decagon_voice/decagon_voice

# Ensure PATH includes common locations when run by cron
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# Absolute log path
LOG_FILE="/Users/trent.peters/Desktop/decagon_voice/decagon_voice/voice_conversations_qa_cron.log"

# Timestamped logging helper
log_with_timestamp() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

log_with_timestamp "Starting voice conversations QA fetch..."

# Run the QA script
python3 voice_conversations_qa.py >> "$LOG_FILE" 2>&1

# Exit status logging
if [ $? -eq 0 ]; then
    log_with_timestamp "QA fetch completed successfully"
else
    log_with_timestamp "QA fetch failed with exit code $?"
fi

log_with_timestamp "Cron job finished"
