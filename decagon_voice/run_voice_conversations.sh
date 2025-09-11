#!/bin/bash

# Voice Conversations Daily Runner
# This script runs the voice conversations fetcher daily at 1pm EST

# Change to the script directory
cd /Users/trent.peters/Desktop/decagon_voice/decagon_voice

# Activate VPN (you may need to adjust this command based on your VPN client)
# Uncomment the line below if you need to start VPN automatically
# open -a "Your VPN App Name"

# Wait a moment for VPN to connect (adjust as needed)
# sleep 30

# Run the Python script
python3 voice_conversations_warehouse_clean.py

# Log the result
echo "$(date): Voice conversations script completed" >> /Users/trent.peters/Desktop/decagon_voice/decagon_voice/voice_conversations.log
