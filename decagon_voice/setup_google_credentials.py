#!/usr/bin/env python3
"""
Setup script for Google Sheets credentials
This script helps you download and configure the service account credentials
"""

import os
import json
import subprocess
import sys

def download_service_account_key():
    """Download the service account key using gcloud CLI"""
    service_account_email = "ai-experiences@decagon-461902.iam.gserviceaccount.com"
    key_file = "decagon-voice-service-account.json"
    
    print(f"Downloading service account key for {service_account_email}...")
    
    try:
        # Use gcloud to create and download the service account key
        cmd = [
            'gcloud', 'iam', 'service-accounts', 'keys', 'create', key_file,
            '--iam-account', service_account_email,
            '--project', 'decagon-461902'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"Successfully downloaded service account key to {key_file}")
            return key_file
        else:
            print(f"Error downloading service account key: {result.stderr}")
            return None
            
    except FileNotFoundError:
        print("gcloud CLI not found. Please install Google Cloud SDK first.")
        print("Visit: https://cloud.google.com/sdk/docs/install")
        return None
    except Exception as e:
        print(f"Error downloading service account key: {e}")
        return None

def setup_environment_variable(key_file):
    """Set up the GOOGLE_APPLICATION_CREDENTIALS environment variable"""
    abs_path = os.path.abspath(key_file)
    
    print(f"\nSetting up environment variable...")
    print(f"GOOGLE_APPLICATION_CREDENTIALS={abs_path}")
    
    # Create a .env file
    env_content = f"GOOGLE_APPLICATION_CREDENTIALS={abs_path}\n"
    
    with open('.env', 'a') as f:
        f.write(env_content)
    
    print("Added GOOGLE_APPLICATION_CREDENTIALS to .env file")
    
    # Also set it for the current session
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = abs_path
    print("Set environment variable for current session")

def verify_credentials(key_file):
    """Verify that the credentials file is valid"""
    try:
        with open(key_file, 'r') as f:
            creds = json.load(f)
        
        required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email', 'client_id']
        
        for field in required_fields:
            if field not in creds:
                print(f"Warning: Missing required field '{field}' in credentials file")
                return False
        
        print(f"Credentials file is valid for project: {creds['project_id']}")
        print(f"Service account email: {creds['client_email']}")
        return True
        
    except Exception as e:
        print(f"Error verifying credentials: {e}")
        return False

def main():
    """Main setup function"""
    print("=== Google Sheets Credentials Setup ===")
    print("This script will help you set up Google service account credentials")
    print("for the Decagon Voice QA system.\n")
    
    # Check if credentials already exist
    existing_files = [f for f in os.listdir('.') if f.endswith('.json') and 'service' in f.lower()]
    
    if existing_files:
        print(f"Found existing service account file: {existing_files[0]}")
        choice = input("Use existing file? (y/n): ").lower().strip()
        if choice == 'y':
            key_file = existing_files[0]
        else:
            key_file = download_service_account_key()
    else:
        key_file = download_service_account_key()
    
    if not key_file:
        print("Failed to obtain service account credentials")
        return
    
    # Verify the credentials
    if not verify_credentials(key_file):
        print("Credentials verification failed")
        return
    
    # Set up environment variable
    setup_environment_variable(key_file)
    
    print("\n=== Setup Complete ===")
    print("You can now run the voice_conversations_qa_google.py script")
    print("Make sure to install dependencies first:")
    print("pip install -r requirements.txt")

if __name__ == "__main__":
    main()
