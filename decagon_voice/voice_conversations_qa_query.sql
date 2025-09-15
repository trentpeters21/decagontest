WITH dec_calls AS (
  SELECT 
  conversation_id
, JSON_EXTRACT_PATH_TEXT(metadata, 'five9_session_id') AS "five9_session_id"
, JSON_EXTRACT_PATH_TEXT(metadata, 'zendesk_ticket_id')::bigint AS "zendesk_ticket_id"
, REGEXP_REPLACE(JSON_EXTRACT_PATH_TEXT(metadata, 'department_name'), '\\s*\\[.*\\]$', '') AS "routing_department"
, JSON_EXTRACT_PATH_TEXT(metadata, 'zendesk_ticket_url') AS "zendesk_ticket_url"
, created_at
, conversation_url
, summary
  FROM decagon_chat.conversation 
  WHERE flow_type = 'VOICE'
)

, five9 AS (
SELECT 
  five9.*
, zd.zendesk_ticket_id
FROM five9.call_log five9
JOIN client_experience_backroom.map_phone_five9_zendesk zd
  ON zd.five9_call_id = five9.call_id 
JOIN dec_calls
  ON dec_calls.zendesk_ticket_id = zd.zendesk_ticket_id

)

SELECT
--'<a href="' || 'https://decagon.ai/admin/conversations#' || conversation_id || '">' || conversation_id || '<\a>' AS decagon_conversation_link
   'https://decagon.ai/admin/conversations#' || conversation_id AS decagon_conversation_link
--,   '<a href="' || 'https://decagon.ai/admin/conversations#' || call_id || '">' || call_id || '<\a>' AS five9_conversation_link
,   'https://decagon.ai/admin/conversations#' || call_id AS five9_conversation_link
--,   '<a href="' || 'https://wealthsimple.zendesk.com/agent/tickets/' || zendesk_ticket_id || '">' || zendesk_ticket_id || '<\a>' AS zendesk_ticket_link
,   'https://wealthsimple.zendesk.com/agent/tickets/' || dec_calls.zendesk_ticket_id AS zendesk_ticket_link
,   routing_department
,   skill
,   CASE WHEN abandoned = 1 THEN 'Yes' ELSE 'No' END AS abandoned
,   TO_CHAR(convert_timezone('UTC'::varchar,'US/Eastern'::varchar, created_at::timestamp), 'YYYY-MM-DD FMHH12:MI pm') AS created_at_est
,   created_at AS created_at_utc
,   conversation_id
FROM dec_calls
LEFT JOIN five9 
  ON dec_calls.zendesk_ticket_id = five9.zendesk_ticket_id
WHERE 
  dec_calls.created_at >= DATEADD(hour, -12, GETDATE())
  AND summary IS NOT NULL
  AND summary != ''
ORDER BY created_at_est ASC