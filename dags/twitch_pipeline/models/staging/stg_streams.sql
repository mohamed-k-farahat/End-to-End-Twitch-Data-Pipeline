-- models/staging/stg_streams.sql
-- Flattens the raw JSON array, adds the snapshot timestamp,
-- and de-duplicates the data.

WITH source AS (
    -- 1. Get the raw array AND the time it was loaded
    SELECT 
        RAW_JSON,
        LOADED_AT AS snapshot_timestamp -- This is the key
    FROM {{ source('api_data', 'RAW_STREAMS') }}
),

flattened AS (
    -- 2. "Explode" the array, keeping the snapshot time with each stream
    SELECT 
        value AS stream_data,
        snapshot_timestamp
    FROM 
        source,
        LATERAL FLATTEN(input => source.RAW_JSON)
),

parsed AS (
    -- 3. Parse the JSON for each stream
    SELECT 
        snapshot_timestamp, -- We need this for the incremental model
        stream_data:id::STRING AS stream_id,
        stream_data:user_id::STRING AS user_id,
        stream_data:user_login::STRING AS user_login,
        stream_data:user_name::STRING AS user_name,
        stream_data:game_id::STRING AS game_id,
        stream_data:game_name::STRING AS game_name,
        stream_data:type::STRING AS stream_type,
        stream_data:title::STRING AS stream_title,
        stream_data:viewer_count::INT AS viewer_count,
        stream_data:started_at::TIMESTAMP_NTZ AS started_at_timestamp,
        stream_data:language::STRING AS language,
        stream_data:is_mature::BOOLEAN AS is_mature
    FROM 
        flattened
),

deduplicated AS (
    -- 4. Assign a row number to each stream, with "1" being the most recent
    --    (This prevents duplicates if the same file is loaded twice)
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY stream_id, snapshot_timestamp ORDER BY snapshot_timestamp DESC) AS rn
    FROM
        parsed
)

-- 5. Select only the most recent record for each stream snapshot
--    (We explicitly list columns instead of using 'EXCEPT')
SELECT 
    snapshot_timestamp,
    stream_id,
    user_id,
    user_login,
    user_name,
    game_id,
    game_name,
    stream_type,
    stream_title,
    viewer_count,
    started_at_timestamp,
    language,
    is_mature
FROM deduplicated
WHERE rn = 1

