#!/usr/bin/env python3
"""
Test version of QA flow - shows what would be added without writing to Google Sheets
"""

import os
import json
import pandas as pd
import subprocess
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')

# File paths
SQL_QUERY_FILE = 'voice_conversations_qa_query.sql'
PROCESSED_CONVERSATIONS_FILE = 'processed_conversations.json'

def load_sql_query():
    """Load the SQL query from file"""
    try:
        with open(SQL_QUERY_FILE, 'r') as f:
            return f.read()
    except FileNotFoundError:
        print(f"SQL query file {SQL_QUERY_FILE} not found")
        return None

def run_satori_query(query):
    """Execute a SQL query using Satori CLI"""
    try:
        print("Executing query via Satori CLI...")

        cmd = [
            'satori', 'run', 'psql',
            '--no-launch-browser',
            'Redshift - Prod',
            'pantheon',
            '-c', query
        ]

        # Up to 3 automatic retries (total 4 attempts including the first)
        max_retries = 3
        backoff_seconds = 2

        for attempt_index in range(0, max_retries + 1):
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)

                # Non-zero return code -> retryable
                if result.returncode != 0:
                    print(f"Satori CLI error (attempt {attempt_index + 1}/{max_retries + 1}): {result.stderr.strip()}")
                else:
                    # Sometimes stdout can be empty on transient failures
                    if result.stdout and result.stdout.strip():
                        print("Query executed successfully")
                        return parse_psql_results(result.stdout)
                    else:
                        print(f"Empty response from Satori (attempt {attempt_index + 1}/{max_retries + 1})")

            except subprocess.TimeoutExpired:
                print(f"Query timed out (attempt {attempt_index + 1}/{max_retries + 1})")
            except Exception as e:
                print(f"Error running Satori query (attempt {attempt_index + 1}/{max_retries + 1}): {e}")

            # If we're here, we will retry unless we've exhausted attempts
            if attempt_index < max_retries:
                sleep_seconds = backoff_seconds * (2 ** attempt_index)
                print(f"Retrying Satori query in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)

        # All attempts failed
        print("Satori query failed after retries")
        return None

    except Exception as e:
        # Catch-all just in case
        print(f"Unexpected error preparing Satori command: {e}")
        return None

def parse_psql_results(output):
    """Parse the tabular output from psql and return as list of dictionaries"""
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
            'decagon_conversation_link' not in line and 
            '---' not in line):
            data_lines.append(line)
    
    print(f"Found {len(data_lines)} conversations")
    
    for line in data_lines:
        # Split by | and clean up
        parts = [part.strip() for part in line.split('|')]
        
        # Expected 12 columns:
        # decagon_link, zendesk_link, five9_call_id, decagon_routing,
        # actual_skill, expected_skill, is_match, abandoned,
        # chat_created_ts_est, chat_created_ts, is_deflected, conversation_id
        if len(parts) >= 12:
            conversation = {
                'decagon_link': parts[0] if len(parts) > 0 else '',
                'zendesk_link': parts[1] if len(parts) > 1 else '',
                'five9_call_id': parts[2] if len(parts) > 2 else '',
                'decagon_routing': parts[3] if len(parts) > 3 else '',
                'actual_skill': parts[4] if len(parts) > 4 else '',
                'expected_skill': parts[5] if len(parts) > 5 else '',
                'is_match': parts[6] if len(parts) > 6 else '',
                'abandoned': parts[7] if len(parts) > 7 else '',
                'chat_created_ts_est': parts[8] if len(parts) > 8 else '',
                'chat_created_ts': parts[9] if len(parts) > 9 else '',
                'is_deflected': parts[10] if len(parts) > 10 else '',
                'conversation_id': parts[11] if len(parts) > 11 else ''
            }
            conversations.append(conversation)
    
    return conversations

def get_processed_conversation_ids():
    """Load the list of conversation IDs that have already been processed"""
    if os.path.exists(PROCESSED_CONVERSATIONS_FILE):
        try:
            with open(PROCESSED_CONVERSATIONS_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get('processed_conversation_ids', []))
        except Exception as e:
            print(f"Error loading processed conversations: {e}")
            return set()
    return set()

def main():
    """Main function implementing the QA flow (test version)"""
    print("=== Voice Conversations QA Flow Test ===")
    print("(This version shows what would be added without writing to Google Sheets)")
    
    # Step 1: Query the data
    print("\n1. Querying data from database...")
    query = load_sql_query()
    if not query:
        print("Failed to load SQL query")
        return
    
    conversations = run_satori_query(query)
    if not conversations:
        print("No conversations found or query failed")
        return
    
    print(f"Retrieved {len(conversations)} conversations from database")
    
    # Step 2: Convert to dataframe
    print("\n2. Converting data to DataFrame...")
    df = pd.DataFrame(conversations)
    print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns")
    
    # Step 3: Check against previously processed conversations
    print("\n3. Checking against previously processed conversations...")
    
    # Get conversation IDs from local tracking file
    processed_ids = get_processed_conversation_ids()
    print(f"Found {len(processed_ids)} previously processed conversation IDs")
    
    # Find new conversations
    current_conversation_ids = set(df['conversation_id'].tolist())
    new_conversation_ids = current_conversation_ids - processed_ids
    
    print(f"Found {len(new_conversation_ids)} new conversation IDs")
    
    if not new_conversation_ids:
        print("No new conversations to process")
        return
    
    # Filter dataframe to only new conversations
    new_conversations_df = df[df['conversation_id'].isin(new_conversation_ids)]
    new_conversations = new_conversations_df.to_dict('records')
    
    # Step 4: Show what would be added (instead of actually adding)
    print(f"\n4. NEW CONVERSATIONS THAT WOULD BE ADDED TO GOOGLE SHEET:")
    print(f"Total new conversations: {len(new_conversations)}")
    
    # Show sample of new conversations
    print(f"\nSample of new conversations (first 5):")
    for i, conv in enumerate(new_conversations[:5], 1):
        print(f"\n{i}. Conversation ID: {conv['conversation_id']}")
        print(f"   Created: {conv['chat_created_ts_est']}")
        print(f"   Routing: {conv['decagon_routing']}")
        print(f"   Match: {conv['is_match']}")
        print(f"   Abandoned: {conv['abandoned']}")
    
    if len(new_conversations) > 5:
        print(f"\n... and {len(new_conversations) - 5} more conversations")
    
    # Save new conversations to a test file
    test_output_file = f"new_conversations_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(test_output_file, 'w') as f:
        json.dump(new_conversations, f, indent=2)
    
    print(f"\n=== TEST COMPLETE ===")
    print(f"New conversations saved to: {test_output_file}")
    print(f"Once you set up Google credentials, these {len(new_conversations)} conversations")
    print(f"will be added to your Google Sheet.")

if __name__ == "__main__":
    main()
