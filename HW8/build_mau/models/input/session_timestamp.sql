WITH session_data AS (
    SELECT sessionId, ts
    FROM {{ source('raw_data', 'session_timestamp') }}
    WHERE sessionId IS NOT NULL
)
SELECT *
FROM session_data
