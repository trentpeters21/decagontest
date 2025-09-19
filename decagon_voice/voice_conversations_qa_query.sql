SELECT
  'https://decagon.ai/admin/conversations#' || conversation_id AS decagon_link,
  'https://wealthsimple.zendesk.com/agent/tickets/' || zendesk_ticket_id AS zendesk_link,
  five9_call_id,
  decagon_routing AS decagon_routing,
  actual_skill AS actual_skill,
  expected_skill AS expected_skill,
  is_match AS is_match,
  CASE WHEN abandoned = 1 THEN 'Yes' ELSE 'No' END AS abandoned,
  chat_created_ts_est AS chat_created_ts_est,
  chat_created_ts AS chat_created_ts,
  is_deflected AS is_deflected,
  conversation_id 
FROM (
  
WITH voice_ai AS (
    SELECT
      conversation_id,
      user_id,
      created_at AS chat_created_ts,
      TO_CHAR(convert_timezone('UTC'::varchar,'US/Eastern'::varchar, created_at::timestamp), 'YYYY-MM-DD FMHH12:MI pm') AS chat_created_ts_est,
      CAST(created_at AS DATE) AS chat_created_date,
      JSON_EXTRACT_PATH_TEXT(metadata, 'From') AS from_number,
      JSON_EXTRACT_PATH_TEXT(metadata, 'five9_session_id') AS five9_session_id,
      JSON_EXTRACT_PATH_TEXT(metadata, 'zendesk_ticket_id') AS zendesk_ticket_id,
      JSON_EXTRACT_PATH_TEXT(metadata, 'department_name') AS decagon_routing,
      destination,
      CASE WHEN undeflected THEN 0 ELSE 1 END AS is_deflected
    FROM decagon_chat.conversation
    WHERE
      created_at >= '2025-09-11' AND flow_type = 'VOICE'
  )
  

, voice_calls_new AS (
    SELECT
      cs.call_id AS five9_call_id,
      cl.ani, /* ,cl.call_type */
      cs.called_party AS skill,
      cl.skill AS call_skill, /* ,cs.call_segment_id */
      cl.timestamp_millisecond AS call_created_ts,
      CAST(cl.timestamp_millisecond AS DATE) AS call_created_date,
      cl.abandoned
    /* , row_number() over (partition by cs.call_id order by cs.timestamp_millisecond asc) */
    FROM five9.call_segment AS cs
    JOIN five9.call_log AS cl
      ON cs.call_id = cl.call_id
    WHERE
      1 = 1
      AND cs.timestamp_millisecond >= '2025-09-11'
      AND cs.queue_wait_time > 0
      AND cl.call_type = '3rd party transfer'
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY cs.call_id ORDER BY cs.timestamp_millisecond ASC) = 1
  )

, merged AS (
    SELECT
      ABS(DATEDIFF(MILLISECOND, CAST(chat_created_ts AS TIMESTAMP), call_created_ts)) AS t_delta,
      ROW_NUMBER() OVER (PARTITION BY conversation_id ORDER BY t_delta ASC) AS rnk,
      *
    FROM voice_ai
    LEFT JOIN voice_calls_new
      ON voice_ai.from_number = voice_calls_new.ani AND chat_created_date = call_created_date
  )
  SELECT
    conversation_id,
    five9_call_id,
    zendesk_ticket_id,
    call_created_ts,
    chat_created_ts_est,
    chat_created_ts,
    user_id,
    destination,
    decagon_routing,
    skill AS actual_skill,
    map.decagon_dept_name,
    map.five9_skill AS expected_skill,
    is_deflected,
    abandoned,
    CASE
      WHEN (
        (
          merged.skill = map.five9_skill
        ) OR merged.decagon_routing IS NULL
      )
      THEN 1
      ELSE 0
    END AS is_match
  FROM merged
  LEFT JOIN preset_uploads.cxo_decagon_five9_skill_map AS map
    ON map.decagon_dept_name = merged.decagon_routing
  WHERE
    rnk = 1
) AS virtual_table