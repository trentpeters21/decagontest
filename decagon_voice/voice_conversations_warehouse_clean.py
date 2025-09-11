#!/usr/bin/env python3
"""
Voice Conversations Data Warehouse Fetcher
Reads SQL query from file and executes it via Satori CLI
"""

import os
import json
import requests
import psycopg2
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv('warehouse.env')

# Configuration
WORKATO_WEBHOOK_URL = os.getenv('WORKATO_WEBHOOK_URL')

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

def run_ssh_tunnel_query(query):
    """Execute a SQL query using SSH tunnel to Redshift"""
    try:
        print("Connecting to SSH tunnel...")
        
        # Redshift connection credentials
        creds = {
            'dbname': 'pantheon',
            'user': 'wealthsimple',
            'password': os.getenv('REDSHIFT_PASSWORD'),
            'host': '127.0.0.1',  # localhost because we're using SSH tunnel
            'port': 5534
        }
        
        # Establish SSH tunnel
        with SSHTunnelForwarder(
            ('mgmt.prod.iad.w10e.com', 22),
            ssh_username=os.getenv('SSH_USERNAME'),
            ssh_private_key=os.getenv('SSH_PRIVATE_KEY_PATH'),
            remote_bind_address=('wsprod-pantheon-redshift.c9qtpydth0xz.us-east-1.redshift.amazonaws.com', 5439),
            local_bind_address=('localhost', 5534)
        ) as ssh_tunnel:
            print('SSH tunnel connected')
            
            print('Connecting to Redshift...')
            with psycopg2.connect(**creds) as conn:
                print("Connected to Redshift successfully!")
                
                print("Executing query...")
                with conn.cursor() as cur:
                    cur.execute(query)
                    results = cur.fetchall()
                    
                    # Convert results to list of dictionaries
                    conversations = []
                    for row in results:
                        conversation = {
                            'conversation_id': row[0] if len(row) > 0 else '',
                            'conversation_url': row[1] if len(row) > 1 else '',
                            'csat': row[2] if len(row) > 2 else '',
                            'deflected': row[3] if len(row) > 3 else '',
                            'summary': row[4] if len(row) > 4 else '',
                            'created_at_utc': str(row[5]) if len(row) > 5 else '',
                            'created_at_est': row[6] if len(row) > 6 else '',
                            'tags': row[7] if len(row) > 7 else '',
                            'metadata': row[8] if len(row) > 8 else ''
                        }
                        conversations.append(conversation)
                    
                    print(f"Found {len(conversations)} conversations")
                    return conversations
                    
    except Exception as e:
        print(f"Error running SSH tunnel query: {e}")
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
    """Fetch voice conversations from the last 12 hours (defined in SQL)."""
    # Load SQL query (already constrained to last 12 hours)
    query = load_sql_query()
    if not query:
        return []
    print("Fetching conversations from the last 12 hours (per SQL filter)")

    # Execute query using SSH tunnel
    conversations = run_ssh_tunnel_query(query)
    if not conversations:
        return []
    
    return conversations

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
    print("Using SSH tunnel to connect to Redshift database")
    
    # Fetch conversations from data warehouse
    conversations = get_voice_conversations_from_warehouse()
    
    if not conversations:
        print("No voice conversations found")
        return
    
    print(f"Found {len(conversations)} voice conversations")
    
    # Save to JSON file
    if save_conversations_to_json(conversations):
        print(f"Successfully saved {len(conversations)} conversations to voice_conversations_warehouse.json")
    
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
