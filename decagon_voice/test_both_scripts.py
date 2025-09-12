#!/usr/bin/env python3
"""
Test script to run both warehouse and API approaches
"""

import subprocess
import sys

def run_script(script_name, description):
    """Run a script and return the result"""
    print(f"\n{'='*50}")
    print(f"Testing: {description}")
    print(f"Script: {script_name}")
    print(f"{'='*50}")
    
    try:
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=True, text=True, timeout=120)
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        print(f"Exit code: {result.returncode}")
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("Script timed out after 120 seconds")
        return False
    except Exception as e:
        print(f"Error running script: {e}")
        return False

def main():
    """Test both scripts"""
    print("Testing both voice conversation scripts...")
    
    # Test warehouse script (Satori CLI)
    warehouse_success = run_script("voice_conversations_warehouse_clean.py", 
                                 "Data Warehouse (Satori CLI)")
    
    # Test API script
    api_success = run_script("voice_conversations_api.py", 
                           "Decagon API")
    
    print(f"\n{'='*50}")
    print("SUMMARY:")
    print(f"Warehouse script: {'✅ SUCCESS' if warehouse_success else '❌ FAILED'}")
    print(f"API script: {'✅ SUCCESS' if api_success else '❌ FAILED'}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
