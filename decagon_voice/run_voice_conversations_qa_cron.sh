#!/bin/bash
cd /Users/trent.peters/Desktop/decagon_voice/decagon_voice
LOG_FILE="voice_conversations_qa_cron.log"
echo "--- $(date) ---" >> "$LOG_FILE"
python3 voice_conversations_qa.py >> "$LOG_FILE" 2>&1
