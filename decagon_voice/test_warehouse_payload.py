#!/usr/bin/env python3
"""
Test script using warehouse payload format (no sensitive data)
"""

import requests
import json

# Your webhook URL
WEBHOOK_URL = "https://webhooks.workato.com/webhooks/rest/7215f6d4-811d-43ea-a85e-91278de0f5f1/decagon_voice_conversations"

# Test payload using warehouse format but with test data only
warehouse_payload = {
    "conversation_id": "test-warehouse-format",
    "conversation_url": "https://decagon.ai/admin/conversations#test-warehouse-format",
    "csat": "",  # Empty like warehouse data
    "deflected": "False",
    "summary": "Test conversation using warehouse payload format",
    "created_at_utc": "2025-09-12 20:47:55.021698+00",
    "created_at_est": "September 12, 2025 4:47 pm",
    "tags": "[{\"name\":\"Test Category\",\"level\":0},{\"name\":\"Test Subcategory\",\"level\":1}]",  # Complex JSON like warehouse
    "metadata": "{\"To\":\"+1234567890\",\"From\":\"+1234567890\",\"Called\":\"+1234567890\",\"Caller\":\"+1234567890\",\"CallSid\":\"TEST_CALL_SID\",\"FromCity\":\"TestCity\",\"language\":\"english\",\"CallToken\":\"TEST_TOKEN\",\"CallerZip\":\"\",\"user_tier\":\"core\",\"AccountSid\":\"TEST_ACCOUNT_SID\",\"CallerCity\":\"TestCity\",\"undeflected\":true,\"CallerCountry\":\"US\",\"call_duration\":60.0,\"department_id\":0,\"department_name\":\"Test Department\",\"interrupt_count\":0,\"_decagon_channel\":\"voice\",\"five9_session_id\":\"TEST_FIVE9_SESSION\",\"zendesk_ticket_id\":\"12345\",\"recording_blob_url\":\"test-recordings/test-warehouse-format/test-recording\",\"zendesk_ticket_url\":\"https://test.zendesk.com/tickets/12345\",\"sentiment_evaluation\":\"neutral\",\"_decagon_git_commit_sha\":\"test_commit_sha\"}"
}

def test_warehouse_payload():
    print("=== Testing Workato Webhook with Warehouse Payload Format ===")
    print(f"URL: {WEBHOOK_URL}")
    print(f"Payload: {json.dumps(warehouse_payload, indent=2)}")
    
    try:
        response = requests.post(WEBHOOK_URL, json=warehouse_payload, timeout=30)
        print(f"\nResponse Status: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print(f"Response Body: {response.text}")
        
        if response.status_code == 200:
            print("\n✅ SUCCESS: Warehouse payload format works!")
        else:
            print(f"\n❌ FAILED: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")

if __name__ == "__main__":
    test_warehouse_payload()
