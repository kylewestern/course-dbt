SELECT 
    session_id,
    MIN(created_at) AS session_started_at,
    MAX(created_at) AS session_ended_at
FROM {{ ref('stg_events') }}
GROUP BY 1