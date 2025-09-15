SELECT 
    conversation_id,
    conversation_url,
    csat,
    CASE
        WHEN undeflected = True THEN 'False'
        WHEN undeflected = False THEN 'True'
        ELSE NULL
    END AS deflected,
    summary,
    created_at AS created_at_utc,
    TO_CHAR(convert_timezone('UTC'::varchar,'US/Eastern'::varchar, created_at::timestamp), 'FMMonth DD, YYYY FMHH12:MI pm') AS created_at_est,
    tags,
    metadata
FROM decagon_chat.conversation
        WHERE
            flow_type = 'VOICE'
            AND summary IS NOT NULL
            AND summary != ''
            AND created_at >= DATEADD(hour, -12, GETDATE())
ORDER BY created_at DESC
LIMIT 1000;