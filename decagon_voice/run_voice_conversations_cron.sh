#!/bin/bash
# Cron job script for voice conversations warehouse fetcher
# Runs at 1pm and 1am EST daily

# Set the working directory to the script location
cd /Users/trent.peters/Desktop/decagon_voice/decagon_voice

# Set environment variables
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# Log file with timestamp
LOG_FILE="/Users/trent.peters/Desktop/decagon_voice/decagon_voice/voice_conversations_cron.log"

# Function to log with timestamp
log_with_timestamp() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

log_with_timestamp "Starting voice conversations warehouse fetch..."

# Run the script
python3 voice_conversations_warehouse_clean.py >> "$LOG_FILE" 2>&1

# Check exit status
if [ $? -eq 0 ]; then
    log_with_timestamp "Voice conversations fetch completed successfully"
else
    log_with_timestamp "Voice conversations fetch failed with exit code $?"
fi

log_with_timestamp "Cron job finished"
