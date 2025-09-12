# Voice Conversations from Decagon API

## Summary
Found **17 voice conversations from the last 30 days** from the Decagon API using the `wealthsimple_voice` flow filter.

The API returned 100 total conversations, and after client-side filtering for the last 30 days, 17 conversations matched the criteria.

## Recent Conversations (Last 30 Days)
**17 conversations found from August 19-20, 2025**

### Sample Recent Conversations (August 19-20, 2025)

**Conversation 1:**
- **ID:** `a64eddb6-2a7c-4341-bb40-853c44f898a7`
- **Created:** 2025-08-19T02:08:05.890247+00:00
- **User:** +16504956914
- **Messages:** 1
- **Summary:** N/A

**Conversation 2:**
- **ID:** `0034e0db-290d-4daa-bb2e-0e9fa0663ee3`
- **Created:** 2025-08-20T08:09:24.852884+00:00
- **User:** +16504956914
- **Summary:** The customer reached out to support seeking human assistance for an unspecified problem
- **Messages:** 3

**Conversation 3:**
- **ID:** `e1530ed3-1a3e-48e9-85cc-c2c8e246c62c`
- **Created:** 2025-08-20T08:11:03.212064+00:00
- **User:** +16504956914
- **Summary:** Customer was frustrated with having to explain their issue to an AI assistant and repeatedly request human help
- **Messages:** 7
- **Undeflected:** True

**Conversation 4:**
- **ID:** `71c781c0-c604-4a30-8113-6ebc2d77cfcd`
- **Created:** 2025-08-20T08:13:18.729117+00:00
- **User:** +16504956914
- **Summary:** The customer inquired about the 2024 AirPods referral promotion and wanted to know more details
- **Messages:** 9
- **Undeflected:** True

**Conversation 5:**
- **ID:** `243d28ea-f31a-4c07-9052-89be75fd8ae8`
- **Created:** 2025-08-20T13:32:15.712697+00:00
- **User:** +19054655413
- **Summary:** Customer was frustrated trying to get information about an AirPods promotional offer
- **Messages:** 9
- **Undeflected:** True

## Historical Data
The API also contains older voice conversations dating back to March 2025, but the 30-day filter focuses on recent activity.

## Data Structure

Each conversation contains the following fields:
- `conversation_id`: Unique identifier
- `conversation_url`: Link to view the conversation in Decagon admin
- `flow_type`: Always "VOICE" for these conversations
- `created_at`: ISO timestamp of when the conversation occurred
- `user_id`: Phone number or user identifier
- `summary`: AI-generated summary of the conversation
- `messages`: Array of individual messages in the conversation
- `csat`: Customer satisfaction score (if available)
- `undeflected`: Boolean indicating if the conversation was deflected
- `tags`: Array of tags associated with the conversation
- `metadata`: Additional metadata about the conversation

## Full JSON Data

The complete JSON data is available in `voice_conversations_api.json` containing all 100 conversations with full message details and metadata.

## API Endpoint Used

- **Endpoint:** `https://api.decagon.ai/conversation/export`
- **Filter:** `wealthsimple_voice` flow filter
- **Authentication:** Bearer token
- **Pagination:** Cursor-based pagination

## Generated on
September 11, 2025
