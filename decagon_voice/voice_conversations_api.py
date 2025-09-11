#!/usr/bin/env python3
"""
Voice Conversations API Fetcher
Uses Decagon API directly with flow_filter to get voice conversations
"""

import subprocess
import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv('warehouse.env')

# Configuration
SATORI_DATASTORE = os.getenv('SATORI_DATASTORE', 'Redshift - Prod')
SATORI_DATABASE = os.getenv('SATORI_DATABASE', 'pantheon')
WORKATO_WEBHOOK_URL = os.getenv('WORKATO_WEBHOOK_URL')

# File paths
LAST_RUN_FILE = 'last_warehouse_run.json'

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

def run_decagon_api_query():
    """Execute Decagon API query with flow_filter fallback to flow_id for voice conversations"""
    try:
        api_key = os.getenv('DECAGON_API_KEY')
        base_url = os.getenv('DECAGON_BASE_URL', 'https://api.decagon.ai')
        if not api_key:
            print("Error: DECAGON_API_KEY not found in environment variables")
            return None

        url = f"{base_url}/conversation/export"
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }

        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        base_payload = {
            "start_date": start_date.isoformat() + "Z",
            "end_date": end_date.isoformat() + "Z"
        }

        def request(payload):
            print("Executing Decagon API query...")
            print(f"URL: {url}")
            print(f"Payload: {json.dumps(payload, indent=2)}")
            resp = requests.get(url, headers=headers, params=payload, timeout=300)
            print(f"Response status code: {resp.status_code}")
            return resp

        # 1) Try with flow_filter using provided key
        payload_flow_filter = dict(base_payload)
        payload_flow_filter.update({"flow_filter": ["wealthsimple_voice"]})
        response = request(payload_flow_filter)
        if response.status_code == 200:
            try:
                data = response.json()
                conversations = data if isinstance(data, list) else data.get('conversations') or data.get('data') or []
                has_voice = any(c.get('flow_type') == 'VOICE' for c in conversations)
                if has_voice:
                    return response.text
            except Exception:
                pass
        else:
            print(f"API request failed with status {response.status_code}")
            print(f"Response: {response.text}")

        # 2) Fallback: try flow_id with same value
        payload_flow_id = dict(base_payload)
        payload_flow_id.update({"flow_id": ["wealthsimple_voice"]})
        response = request(payload_flow_id)
        if response.status_code != 200:
            print(f"API request failed with status {response.status_code}")
            print(f"Response: {response.text}")
            return None
        return response.text

    except requests.exceptions.Timeout:
        print("API request timed out after 5 minutes")
        return None
    except Exception as e:
        print(f"Error running Decagon API query: {e}")
        return None

def parse_api_results(output):
    """Parse the JSON output from the API"""
    try:
        # Try to parse as JSON
        data = json.loads(output)
        
        # Check if it's a list of conversations or has a conversations field
        if isinstance(data, list):
            conversations = data
        elif isinstance(data, dict) and 'conversations' in data:
            conversations = data['conversations']
        elif isinstance(data, dict) and 'data' in data:
            conversations = data['data']
        else:
            print(f"Unexpected API response format: {type(data)}")
            return []
        
        print(f"Found {len(conversations)} conversations from API")
        
        # Process conversations to match expected format
        processed_conversations = []
        for conv in conversations:
            processed_conv = {
                'conversation_id': conv.get('conversation_id', ''),
                'conversation_url': conv.get('conversation_url', ''),
                'csat': conv.get('csat', ''),
                'deflected': conv.get('deflected', ''),
                'summary': conv.get('summary', ''),
                'created_at': conv.get('created_at', ''),
                'tags': conv.get('tags', ''),
                'metadata': conv.get('metadata', ''),
                'flow_type': conv.get('flow_type', '')  # Include flow_type for verification
            }
            processed_conversations.append(processed_conv)
        
        return processed_conversations
        
    except json.JSONDecodeError as e:
        print(f"Error parsing API response as JSON: {e}")
        print(f"Raw output: {output[:500]}")
        return []
    except Exception as e:
        print(f"Error processing API results: {e}")
        return []

def get_voice_conversations_from_api():
    """Fetch voice conversations using Decagon API with flow_filter"""
    print("Fetching voice conversations using Decagon API with flow_filter...")
    
    # Execute API query
    result = run_decagon_api_query()
    if not result:
        return []
    
    # Parse results
    return parse_api_results(result)

def send_to_workato_webhook(conversation, webhook_url):
    """Send conversation data to Workato webhook"""
    try:
        # Format created_at to a clean date and timestamp
        created_at_str = conversation.get("created_at")
        formatted_created_at = created_at_str
        
        if created_at_str:
            try:
                # Parse the ISO 8601 string into a datetime object
                dt_object = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                # Format it to "September 09, 2025 at 05:10 PM"
                formatted_created_at = dt_object.strftime("%B %d, %Y at %I:%M %p")
            except (ValueError, AttributeError) as e:
                print(f"Warning: Could not parse created_at '{created_at_str}': {e}")
                # Keep original if parsing fails
                formatted_created_at = created_at_str
        
        payload = {
            "conversation_id": conversation.get("conversation_id"),
            "conversation_url": conversation.get("conversation_url"),
            "csat": conversation.get("csat"),
            "deflected": conversation.get("deflected"),
            "summary": conversation.get("summary"),
            "created_at": formatted_created_at,
            "tags": conversation.get("tags"),
            "metadata": conversation.get("metadata"),
            "flow_type": conversation.get("flow_type")  # Include flow_type for verification
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
    """Main function to fetch voice conversations using API and send to Workato"""
    print("=== Voice Conversations API Fetcher ===")
    print(f"Using datastore: {SATORI_DATASTORE}")
    print("Using flow_filter: ['wealthsimple_voice']")
    
    # Fetch conversations from API
    conversations = get_voice_conversations_from_api()
    
    if not conversations:
        print("No voice conversations found")
        return
    
    print(f"Found {len(conversations)} voice conversations")
    
    # Show flow_type distribution for verification
    flow_types = {}
    for conv in conversations:
        flow_type = conv.get('flow_type', 'unknown')
        flow_types[flow_type] = flow_types.get(flow_type, 0) + 1
    
    print("Flow type distribution:")
    for flow_type, count in flow_types.items():
        print(f"  {flow_type}: {count}")
    
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
