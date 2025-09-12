import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env.local')

# Configuration
API_KEY = os.getenv('DECAGON_API_KEY')
BASE_URL = os.getenv('DECAGON_BASE_URL', 'https://api.decagon.ai')
DECAGON_WEB_URL = os.getenv('DECAGON_WEB_URL', 'https://decagon.ai')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
ENDPOINT = f"{BASE_URL}/conversation/export"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

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

def export_voice_conversations():
    """Export voice conversations from Decagon API"""
    if not API_KEY:
        print("Error: DECAGON_API_KEY not found in environment variables")
        return []

    # Calculate 30 days ago
    from datetime import datetime, timedelta
    thirty_days_ago = datetime.now() - timedelta(days=30)

    all_conversations = []
    cursor = None

    while True:
        # Set up parameters with flow_filter for voice conversations
        params = {
            "user_filters": json.dumps({
                "flow_filter": ["wealthsimple_voice"]
            }),
            "limit": 100  # API max limit
        }
        
        # Add cursor if we have one (for pagination)
        if cursor:
            params["cursor"] = cursor
            
        try:
            # Make the API request
            response = requests.get(ENDPOINT, headers=HEADERS, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            data = response.json()
            
            # Extract conversations from this page
            conversations = data.get("conversations", [])

            # Filter for conversations from last 30 days (client-side filtering)
            recent_conversations = []
            for conv in conversations:
                created_at = conv.get('created_at', '')
                if created_at:
                    try:
                        conv_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        conv_dt_naive = conv_dt.replace(tzinfo=None)
                        if conv_dt_naive >= thirty_days_ago:
                            recent_conversations.append(conv)
                    except:
                        # If date parsing fails, skip this conversation
                        continue
            
            all_conversations.extend(recent_conversations)
            
            print(f"Retrieved {len(conversations)} conversations from this page")
            print(f"Filtered to {len(recent_conversations)} conversations from last 30 days")
            print(f"Total recent conversations so far: {len(all_conversations)}")
            
            # Check if there are more pages
            cursor = data.get("next_page_cursor")
            if not cursor:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            break
    
    return all_conversations

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

def send_to_slack(conversation, webhook_url):
    """Send conversation data to Slack"""
    try:
        # Format the message for Slack
        conversation_id = conversation.get('conversation_id', 'N/A')
        created_at_est = conversation.get('created_at_est', 'N/A')
        deflected = conversation.get('deflected', 'N/A')
        csat = conversation.get('csat', 'N/A')
        summary = conversation.get('summary', 'N/A')
        conversation_url = f"{DECAGON_WEB_URL}/conversation/{conversation_id}"
        
        # Create Slack message
        slack_message = {
            "text": f"🎤 New Voice Conversation",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🎤 New Voice Conversation"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Date:* {created_at_est}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Deflected:* {deflected}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*CSAT:* {csat}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Conversation ID:* `{conversation_id}`"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Summary:* {summary}"
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "View Conversation"
                            },
                            "url": conversation_url,
                            "style": "primary"
                        }
                    ]
                }
            ]
        }
        
        response = requests.post(webhook_url, json=slack_message, timeout=30)
        return response.status_code == 200
    except Exception as e:
        print(f"Error sending to Slack: {e}")
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

def format_conversation_for_workato(conversation):
    """Format conversation data for Workato webhook"""
    return {
        'conversation_id': conversation.get('conversation_id', ''),
        'conversation_url': conversation.get('conversation_url', ''),
        'csat': conversation.get('csat', ''),
        'deflected': str(not conversation.get('undeflected', True)),  # Convert undeflected to deflected
        'summary': conversation.get('summary', ''),
        'created_at_utc': conversation.get('created_at', ''),
        'created_at_est': format_timestamp_est(conversation.get('created_at', '')),
        'tags': json.dumps(conversation.get('tags', [])),
        'metadata': json.dumps(conversation.get('metadata', {}))
    }

def main():
    """Main function to fetch voice conversations from last 30 days and send to Slack"""
    print("=== Voice Conversations API Fetcher ===")
    print("Using Decagon API to fetch voice conversations from last 30 days")
    
    # Export voice conversations
    voice_conversations = export_voice_conversations()
    
    if not voice_conversations:
        print("No voice conversations found")
        return
    
    print(f"\nSuccessfully retrieved {len(voice_conversations)} voice conversations")
    
    # Save to file
    save_conversations_to_json(voice_conversations, "voice_conversations_api.json")
    
    # Print sample conversation info
    if voice_conversations:
        sample = voice_conversations[0]
        print(f"\nSample conversation:")
        print(f"  ID: {sample.get('conversation_id')}")
        print(f"  Flow Type: {sample.get('flow_type')}")
        print(f"  Created: {sample.get('created_at')}")
        print(f"  Messages: {len(sample.get('messages', []))}")
    
    # Send to Slack
    if not SLACK_WEBHOOK_URL or SLACK_WEBHOOK_URL == 'your-slack-webhook-url-here':
        print("No Slack webhook URL configured - skipping Slack notification")
    else:
        print("Sending conversations to Slack...")
        sent_count = 0
        for conversation in voice_conversations:
            formatted_conv = format_conversation_for_workato(conversation)
            if send_to_slack(formatted_conv, SLACK_WEBHOOK_URL):
                sent_count += 1
            else:
                print(f"Failed to send conversation {conversation.get('conversation_id', 'unknown')}")

        print(f"Successfully sent {sent_count} conversations to Slack")
    
    # Save last run timestamp
    save_last_run_timestamp()
    print("Done!")

if __name__ == "__main__":
    main()
