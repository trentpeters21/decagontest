#!/usr/bin/env python3
"""
Voice Conversations Data Warehouse Fetcher
Reads SQL query from file and executes it via Satori CLI
"""

import os
import json
import requests
import subprocess
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')

# Configuration
WORKATO_WEBHOOK_URL = os.getenv('WORKATO_WEBHOOK_URL', 'https://webhooks.workato.com/webhooks/rest/7215f6d4-811d-43ea-a85e-91278de0f5f1/decagon_voice_conversations')

# File paths
SQL_QUERY_FILE = 'voice_conversations_query.sql'
LAST_RUN_FILE = 'last_warehouse_run.json'

def load_sql_query():
    """Load the SQL query from file"""
    try:
        with open(SQL_QUERY_FILE, 'r') as f:
            return f.read()
    except FileNotFoundError:
        print(f"SQL query file {SQL_QUERY_FILE} not found")
        return None

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

def run_satori_query(query):
    """Execute a SQL query using Satori CLI"""
    try:
        print("Executing query via Satori CLI...")
        
        # Use Satori CLI to execute the query
        cmd = [
            'satori', 'run', 'psql', 
            '--no-launch-browser',
            'Redshift - Prod', 
            'pantheon', 
            '-c', query
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            print(f"Satori CLI error: {result.stderr}")
            return None
            
        print("Query executed successfully")
        return parse_psql_results(result.stdout)
        
    except subprocess.TimeoutExpired:
        print("Query timed out after 60 seconds")
        return None
    except Exception as e:
        print(f"Error running Satori query: {e}")
        return None

def parse_psql_results(output):
    """Parse the tabular output from psql"""
    lines = output.strip().split('\n')
    conversations = []
    
    if len(lines) < 3:
        return conversations
    
    # Find data rows (skip header and separator)
    data_lines = []
    for line in lines:
        if (line.strip() and 
            not line.strip().startswith('(') and 
            'rows)' not in line and 
            'conversation_id' not in line and 
            '---' not in line):
            data_lines.append(line)
    
    print(f"Found {len(data_lines)} conversations")
    
    for line in data_lines:
        # Split by | and clean up
        parts = [part.strip() for part in line.split('|')]
        
        if len(parts) >= 9:  # We expect 9 columns now
            conversation = {
                'conversation_id': parts[0] if len(parts) > 0 else '',
                'conversation_url': parts[1] if len(parts) > 1 else '',
                'csat': parts[2] if len(parts) > 2 else '',
                'deflected': parts[3] if len(parts) > 3 else '',
                'summary': parts[4] if len(parts) > 4 else '',
                'created_at_utc': parts[5] if len(parts) > 5 else '',
                'created_at_est': parts[6] if len(parts) > 6 else '',
                'tags': parts[7] if len(parts) > 7 else '',
                'metadata': parts[8] if len(parts) > 8 else ''
            }
            conversations.append(conversation)
    
    return conversations

def get_voice_conversations_from_warehouse():
    """Fetch voice conversations from the last 24 hours (defined in SQL)."""
    # Load SQL query (already constrained to last 24 hours)
    query = load_sql_query()
    if not query:
        return []
    print("Fetching conversations from the last 12 hours (per SQL filter)")

    # Execute query using Satori CLI
    conversations = run_satori_query(query)
    if not conversations:
        return []
    
    return conversations

def send_to_workato_webhook(conversation, webhook_url, max_retries=3):
    """Send conversation data to Workato webhook with retry logic"""
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
    
    for attempt in range(max_retries):
        try:
            print(f"DEBUG: Sending to {webhook_url}")
            print(f"DEBUG: Payload keys: {list(payload.keys())}")
            print(f"DEBUG: conversation_id: {payload.get('conversation_id')}")
            response = requests.post(webhook_url, json=payload, timeout=30)
            print(f"DEBUG: Response status: {response.status_code}")
            print(f"DEBUG: Response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                return True
            elif response.status_code == 429:
                print(f"RATE LIMITED: HTTP 429 - {response.text}")
                return "RATE_LIMITED"
            elif response.status_code == 404:
                if attempt < max_retries - 1:
                    print(f"Recipe stopped (404), retrying in 2 seconds... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(2)
                    continue
                else:
                    print(f"Recipe still stopped after {max_retries} attempts: HTTP 404")
                    return False
            else:
                print(f"HTTP {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Error sending to Workato (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(1)
                continue
            else:
                print(f"Error sending to Workato after {max_retries} attempts: {e}")
                return False
    
    return False


def save_conversations_to_json(conversations, filename="voice_conversations_warehouse.json"):
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
    print("=== Voice Conversations Data Warehouse Fetcher ===")
    print("Using Satori CLI to connect to Redshift - Prod database")
    
    # Fetch conversations from data warehouse
    conversations = get_voice_conversations_from_warehouse()
    
    if not conversations:
        print("No voice conversations found")
        return
    
    print(f"Found {len(conversations)} voice conversations")
    
    # Save to JSON file
    if save_conversations_to_json(conversations):
        print(f"Successfully saved {len(conversations)} conversations to voice_conversations_warehouse.json")
    
    # Send to Workato with preflight and throttling
    if not WORKATO_WEBHOOK_URL or WORKATO_WEBHOOK_URL == 'your-workato-webhook-url-here':
        print("No Workato webhook URL configured - skipping Workato notification")
    else:
        print(f"Sending {len(conversations)} conversations to Workato (rate-limited)…")
        sent_count = 0
        failed_count = 0
        
        # Rate limiting: 6000 requests per 5 minutes = 20 requests per second max
        # Use conservative 10 requests per second to stay well under limit
        requests_per_second = 10
        delay_between_requests = 1.0 / requests_per_second
        
        consecutive_failures = 0
        max_consecutive_failures = 5  # Stop if 5 consecutive conversations fail
        
        for idx, conversation in enumerate(conversations, start=1):
            result = send_to_workato_webhook(conversation, WORKATO_WEBHOOK_URL)
            
            if result == "RATE_LIMITED":
                print(f"Hit rate limit at conversation {idx}. Stopping to avoid more failures.")
                print(f"Rate limit resets in 5 minutes. Remaining conversations will be sent in next run.")
                break
            elif result:
                sent_count += 1
                consecutive_failures = 0  # Reset counter on success
            else:
                failed_count += 1
                consecutive_failures += 1
                print(f"Failed to send conversation {conversation.get('conversation_id', 'unknown')}")
                
                # Stop if too many consecutive failures (recipe likely stopped)
                if consecutive_failures >= max_consecutive_failures:
                    print(f"Stopping after {consecutive_failures} consecutive failures. Recipe appears to be stopped.")
                    print("Please start the Workato recipe and try again.")
                    break

            # Progress indicator every 10 conversations
            if idx % 10 == 0:
                print(f"Progress: {idx}/{len(conversations)} sent, {sent_count} successful, {failed_count} failed")

            # Rate limiting delay
            time.sleep(delay_between_requests)

        print(f"Successfully sent {sent_count}/{len(conversations)} conversations to Workato")
    
    # Save last run timestamp
    save_last_run_timestamp()
    print("Done!")

if __name__ == "__main__":
    main()
