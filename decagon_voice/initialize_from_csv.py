#!/usr/bin/env python3
"""
Initialize processed conversations tracking from CSV file
Reads conversation IDs from CSV and creates the tracking file
"""

import csv
import json
import os
from datetime import datetime

def initialize_from_csv(csv_file_path, output_file='processed_conversations.json'):
    """Initialize processed conversations from CSV file"""
    
    if not os.path.exists(csv_file_path):
        print(f"CSV file not found: {csv_file_path}")
        return False
    
    conversation_ids = set()
    
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            # Try to detect if there's a header
            sample = csvfile.read(1024)
            csvfile.seek(0)
            
            # Check if first line looks like a header
            first_line = csvfile.readline().strip()
            csvfile.seek(0)
            
            # If first line contains 'conversation_id' or similar, skip it
            has_header = 'conversation_id' in first_line.lower() or 'id' in first_line.lower()
            
            reader = csv.reader(csvfile)
            
            if has_header:
                next(reader)  # Skip header row
                print("Detected header row, skipping...")
            
            for row_num, row in enumerate(reader, start=2 if has_header else 1):
                if row and len(row) > 0:
                    conversation_id = row[0].strip()
                    if conversation_id and conversation_id != 'conversation_id':
                        conversation_ids.add(conversation_id)
                else:
                    print(f"Warning: Empty row {row_num}")
        
        print(f"Loaded {len(conversation_ids)} conversation IDs from CSV")
        
        # Create the tracking data structure
        tracking_data = {
            'processed_conversation_ids': list(conversation_ids),
            'last_updated': datetime.now().isoformat(),
            'source': f'Initialized from CSV: {csv_file_path}',
            'total_count': len(conversation_ids)
        }
        
        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(tracking_data, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully created {output_file}")
        print(f"Total conversation IDs tracked: {len(conversation_ids)}")
        
        # Show first few IDs as verification
        sample_ids = list(conversation_ids)[:5]
        print(f"Sample IDs: {sample_ids}")
        
        return True
        
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        return False

def main():
    """Main function"""
    print("=== Initialize Processed Conversations from CSV ===")
    
    # Look for CSV files in current directory
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in current directory")
        print("Please place your CSV file in this directory and run again")
        return
    
    print(f"Found CSV files: {csv_files}")
    
    if len(csv_files) == 1:
        csv_file = csv_files[0]
        print(f"Using: {csv_file}")
    else:
        print("Multiple CSV files found. Please specify which one to use:")
        for i, file in enumerate(csv_files, 1):
            print(f"{i}. {file}")
        
        try:
            choice = int(input("Enter number: ")) - 1
            csv_file = csv_files[choice]
        except (ValueError, IndexError):
            print("Invalid choice")
            return
    
    # Initialize from CSV
    success = initialize_from_csv(csv_file)
    
    if success:
        print("\n=== Initialization Complete ===")
        print("You can now run the QA system:")
        print("python voice_conversations_qa_google.py")
    else:
        print("\n=== Initialization Failed ===")
        print("Please check your CSV file format and try again")

if __name__ == "__main__":
    main()
