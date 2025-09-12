#!/usr/bin/env python3
"""
Voice Conversations API Fetcher
Fetches voice conversations from Decagon API and sends to Workato
"""

import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv('warehouse.env')

# Configuration
DECAGON_API_KEY = os.getenv('DECAGON_API_KEY')
DECAGON_BASE_URL = os.getenv('DECAGON_BASE_URL', 'https://api.decagon.ai')
DECAGON_WEB_URL = os.getenv('DECAGON_WEB_URL', 'https://decagon.ai')
WORKATO_WEBHOOK_URL = os.getenv('WORKATO_WEBHOOK_URL')

# File paths
LAST_RUN_FILE = 'last_api_run.json'

def get_last_run_timestamp():
    """Get the timestamp of the last successful run"""
    if os.path.exists(LAST_RUN_FILE):
        with open(LAST_RUN_FILE, 'r') as f:
            data = json.load(f)
            return data.get('last_run_timestamp')
    return None

def save_last_run_timestamp():
    """Save the current timestamp as the last successful run"""
    timestamp = int(datetime.now().timestamp())
    with open(LAST_RUN_FILE, 'w') as f:
        json.dump({'last_run_timestamp': timestamp}, f)

def get_voice_conversations_from_api():
    """Fetch voice conversations from Decagon API"""
    try:
        if not DECAGON_API_KEY:
            print("Error: DECAGON_API_KEY not found in environment variables")
            return []
        
        print("Fetching conversations from Decagon API...")
        
        # Calculate timestamp for last 24 hours
        now = datetime.now()
        yesterday = now - timedelta(hours=24)
        since_timestamp = int(yesterday.timestamp())
        
        # API endpoint for conversations
        url = f"{DECAGON_BASE_URL}/conversations"
        
        headers = {
            'Authorization': f'Bearer {DECAGON_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'since': since_timestamp,
            'flow_type': 'VOICE',
            'limit': 1000
        }
        
        print(f"Requesting conversations since: {yesterday.strftime('%Y-%m-%d %H:%M:%S')}")
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"API request failed with status {response.status_code}: {response.text}")
            return []
        
        data = response.json()
        conversations = data.get('conversations', [])
        
        # Filter for voice conversations with summaries
        voice_conversations = []
        for conv in conversations:
            if (conv.get('flow_type') == 'VOICE' and 
                conv.get('summary') and 
                conv.get('summary').strip()):
                
                # Format the conversation data
                formatted_conv = {
                    'conversation_id': conv.get('conversation_id', ''),
                    'conversation_url': f"{DECAGON_WEB_URL}/conversations/{conv.get('conversation_id', '')}",
                    'csat': conv.get('csat', ''),
                    'deflected': str(conv.get('deflected', False)),
                    'summary': conv.get('summary', ''),
                    'created_at_utc': conv.get('created_at', ''),
                    'created_at_est': format_timestamp_est(conv.get('created_at', '')),
                    'tags': json.dumps(conv.get('tags', [])),
                    'metadata': json.dumps(conv.get('metadata', {}))
                }
                voice_conversations.append(formatted_conv)
        
        print(f"Found {len(voice_conversations)} voice conversations")
        return voice_conversations
        
    except Exception as e:
        print(f"Error fetching from API: {e}")
        return []

def format_timestamp_est(utc_timestamp):
    """Format UTC timestamp to EST string"""
    try:
        if not utc_timestamp:
            return ''
        
        # Parse the timestamp (assuming it's in ISO format or Unix timestamp)
        if isinstance(utc_timestamp, (int, float)):
            dt = datetime.fromtimestamp(utc_timestamp)
        else:
            # Try parsing ISO format
            dt = datetime.fromisoformat(utc_timestamp.replace('Z', '+00:00'))
        
        # Convert to EST (UTC-5 or UTC-4 depending on DST)
        # For simplicity, using UTC-5 (EST)
        est_dt = dt - timedelta(hours=5)
        
        # Format as "September 10, 2025 5:01 pm"
        return est_dt.strftime('%B %d, %Y %I:%M %p').lower()
        
    except Exception as e:
        print(f"Error formatting timestamp: {e}")
        return str(utc_timestamp)

def send_to_workato_webhook(conversation, webhook_url):
    """Send conversation data to Workato webhook"""
    try:
        payload = {
            "conversation_id": conversation.get("conversation_id"),
            "conversation_url": conversation.get("conversation_url"),
            "csat": conversation.get("csat"),
            "deflected": conversation.get("deflected"),
            "summary": conversation.get("summary"),
            "created_at_utc": conversation.get("created_at_utc"),
            "created_at_est": conversation.get("created_at_est"),
            "tags": conversation.get("tags"),
            "metadata": conversation.get("metadata")
        }
        
        response = requests.post(webhook_url, json=payload, timeout=30)
        return response.status_code == 200
    except Exception as e:
        print(f"Error sending to Workato: {e}")
        return False

def save_conversations_to_json(conversations, filename="voice_conversations_api.json"):
    """Save conversations to a JSON file"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(conversations, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving to JSON: {e}")
        return False

def main():
    """Main function to fetch voice conversations and send to Workato"""
    print("=== Voice Conversations API Fetcher ===")
    print("Using Decagon API to fetch conversations")
    
    # Fetch conversations from API
    conversations = get_voice_conversations_from_api()
    
    if not conversations:
        print("No voice conversations found")
        return
    
    print(f"Found {len(conversations)} voice conversations")
    
    # Save to JSON file
    if save_conversations_to_json(conversations):
        print(f"Successfully saved {len(conversations)} conversations to voice_conversations_api.json")
    
    # Send to Workato
    if not WORKATO_WEBHOOK_URL or WORKATO_WEBHOOK_URL == 'your-workato-webhook-url-here':
        print("No Workato webhook URL configured - skipping Workato notification")
    else:
        print("Sending conversations to Workato...")
        sent_count = 0
        for conversation in conversations:
            if send_to_workato_webhook(conversation, WORKATO_WEBHOOK_URL):
                sent_count += 1
            else:
                print(f"Failed to send conversation {conversation.get('conversation_id', 'unknown')}")
        
        print(f"Successfully sent {sent_count} conversations to Workato")
    
    # Save last run timestamp
    save_last_run_timestamp()
    print("Done!")

if __name__ == "__main__":
    main()
