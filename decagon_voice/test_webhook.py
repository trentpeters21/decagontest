#!/usr/bin/env python3
"""
Test script to send a single test payload to Workato webhook
"""

import requests
import json

# Your webhook URL
WEBHOOK_URL = "https://webhooks.workato.com/webhooks/rest/7215f6d4-811d-43ea-a85e-91278de0f5f1/decagon_voice_conversations"

# Test payload
test_payload = {
    "conversation_id": "test-12345-67890",
    "conversation_url": "https://decagon.ai/admin/conversations#test-12345-67890",
    "csat": "5",
    "deflected": "False",
    "summary": "Test conversation for webhook validation",
    "created_at_utc": "2025-01-15T10:30:00Z",
    "created_at_est": "January 15, 2025 5:30 am",
    "tags": "test,webhook",
    "metadata": "{\"test\": true, \"source\": \"manual_test\"}"
}

def test_webhook():
    print("=== Testing Workato Webhook ===")
    print(f"URL: {WEBHOOK_URL}")
    print(f"Payload: {json.dumps(test_payload, indent=2)}")
    
    try:
        response = requests.post(WEBHOOK_URL, json=test_payload, timeout=30)
        print(f"\nResponse Status: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print(f"Response Body: {response.text}")
        
        if response.status_code == 200:
            print("\n✅ SUCCESS: Webhook test passed!")
        else:
            print(f"\n❌ FAILED: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")

if __name__ == "__main__":
    test_webhook()
