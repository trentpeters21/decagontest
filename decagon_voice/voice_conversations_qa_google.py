#!/usr/bin/env python3
"""
Voice Conversations QA with Google Sheets Integration
Queries data, converts to dataframe, checks against existing conversations,
and adds new conversations to Google Sheets
"""

import os
import json
import pandas as pd
import subprocess
import time
from datetime import datetime
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv('.env')

# Configuration
GOOGLE_SHEET_ID = '1JHZ1eqoQBIHbNqRvx902x10wOgqhL_N4QLnEp834x1k'
GOOGLE_SHEET_TAB = 'AI_Voice_QA'
GOOGLE_SERVICE_ACCOUNT = 'ai-experiences@decagon-461902.iam.gserviceaccount.com'

# File paths
SQL_QUERY_FILE = 'voice_conversations_qa_query.sql'

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

def get_existing_conversation_ids_from_sheet(client):
    """Get existing conversation IDs from the Google Sheet"""
    try:
        # Open the spreadsheet
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB)
        
        # Get all values from the sheet
        all_values = sheet.get_all_values()
        
        if not all_values:
            print("Sheet is empty, no existing conversation IDs found")
            return set()
        
        # Find the conversation_id column (should be the last column based on our query)
        headers = all_values[0]
        try:
            conversation_id_col_index = headers.index('conversation_id')
        except ValueError:
            # If conversation_id column not found, assume it's the last column
            conversation_id_col_index = len(headers) - 1
            print(f"conversation_id column not found in headers, using column {conversation_id_col_index}")
        
        # Extract conversation IDs (skip header row)
        existing_ids = set()
        for row in all_values[1:]:
            if len(row) > conversation_id_col_index and row[conversation_id_col_index].strip():
                existing_ids.add(row[conversation_id_col_index].strip())
        
        print(f"Found {len(existing_ids)} existing conversation IDs in Google Sheet")
        return existing_ids
        
    except Exception as e:
        print(f"Error reading existing conversation IDs from sheet: {e}")
        return set()

def get_google_sheets_client():
    """Initialize Google Sheets client using service account"""
    try:
        # Define the scopes
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Create credentials from service account info
        # You'll need to download the service account JSON key file
        # and set the GOOGLE_APPLICATION_CREDENTIALS environment variable
        # or place the JSON file in the project directory
        
        # Try to get credentials from environment variable first
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_path and os.path.exists(creds_path):
            creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        else:
            # Try to find secrets.json in current directory
            if os.path.exists('secrets.json'):
                creds = Credentials.from_service_account_file('secrets.json', scopes=scopes)
            else:
                print("No Google service account credentials found.")
                print("Please either:")
                print("1. Set GOOGLE_APPLICATION_CREDENTIALS environment variable to your service account JSON file")
                print("2. Place your service account JSON file as 'secrets.json' in the project directory")
                return None
        
        # Create the client
        client = gspread.authorize(creds)
        return client
        
    except Exception as e:
        print(f"Error initializing Google Sheets client: {e}")
        return None

def get_existing_conversation_ids_from_sheet(client):
    """Get existing conversation IDs from the Google Sheet"""
    try:
        # Open the spreadsheet
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB)
        
        # Get all values from the sheet
        all_values = sheet.get_all_values()
        
        if not all_values:
            print("Sheet is empty, no existing conversation IDs found")
            return set()
        
        # Find the conversation_id column (should be the last column based on our query)
        headers = all_values[0]
        try:
            conversation_id_col_index = headers.index('conversation_id')
        except ValueError:
            # If conversation_id column not found, assume it's the last column
            conversation_id_col_index = len(headers) - 1
            print(f"conversation_id column not found in headers, using column {conversation_id_col_index}")
        
        # Extract conversation IDs (skip header row)
        existing_ids = set()
        for row in all_values[1:]:
            if len(row) > conversation_id_col_index and row[conversation_id_col_index].strip():
                existing_ids.add(row[conversation_id_col_index].strip())
        
        print(f"Found {len(existing_ids)} existing conversation IDs in Google Sheet")
        return existing_ids
        
    except Exception as e:
        print(f"Error reading existing conversation IDs from sheet: {e}")
        return set()

def add_new_conversations_to_sheet(client, new_conversations):
    """Add new conversations to the Google Sheet"""
    try:
        # Open the spreadsheet
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB)
        
        # Prepare data for insertion
        # Convert conversations to list of lists in the correct order
        rows_to_add = []
        for conv in new_conversations:
            row = [
                conv.get('decagon_link', ''),
                conv.get('zendesk_link', ''),
                conv.get('five9_call_id', ''),
                conv.get('decagon_routing', ''),
                conv.get('actual_skill', ''),
                conv.get('expected_skill', ''),
                conv.get('is_match', ''),
                conv.get('abandoned', ''),
                conv.get('chat_created_ts_est', ''),
                conv.get('chat_created_ts', ''),
                conv.get('is_deflected', ''),
                conv.get('conversation_id', '')
            ]
            rows_to_add.append(row)
        
        if rows_to_add:
            # Add rows to the sheet
            sheet.append_rows(rows_to_add)
            print(f"Successfully added {len(rows_to_add)} new conversations to Google Sheet")
            return True
        else:
            print("No new conversations to add")
            return True
            
    except Exception as e:
        print(f"Error adding conversations to sheet: {e}")
        return False

def main():
    """Main function implementing the QA flow"""
    print("=== Voice Conversations QA with Google Sheets Integration ===")
    
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
    
    # Step 3: Check against existing conversations in Google Sheet
    print("\n3. Checking against existing conversations in Google Sheet...")
    
    # Get Google Sheets client
    client = get_google_sheets_client()
    if not client:
        print("Failed to initialize Google Sheets client")
        return
    
    # Get existing conversation IDs from Google Sheet
    existing_ids = get_existing_conversation_ids_from_sheet(client)
    print(f"Found {len(existing_ids)} existing conversation IDs in Google Sheet")
    
    # Find new conversations
    current_conversation_ids = set(df['conversation_id'].tolist())
    new_conversation_ids = current_conversation_ids - existing_ids
    
    print(f"Found {len(new_conversation_ids)} new conversation IDs")
    
    if not new_conversation_ids:
        print("No new conversations to process")
        return
    
    # Filter dataframe to only new conversations
    new_conversations_df = df[df['conversation_id'].isin(new_conversation_ids)]
    new_conversations = new_conversations_df.to_dict('records')
    
    # Step 4: Add new conversations to Google Sheet
    print(f"\n4. Adding {len(new_conversations)} new conversations to Google Sheet...")
    
    success = add_new_conversations_to_sheet(client, new_conversations)
    
    if success:
        print(f"Successfully processed {len(new_conversations)} new conversations")
        print(f"Total conversations now in Google Sheet: {len(existing_ids) + len(new_conversations)}")
    else:
        print("Failed to add conversations to Google Sheet")
    
    print("\n=== QA Process Complete ===")

if __name__ == "__main__":
    main()
