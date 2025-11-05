{{ config(
    materialized = 'incremental',
    unique_key = 'stream_snapshot_id',
    incremental_strategy = 'merge'
) }}

WITH stg_streams AS (
    SELECT * FROM {{ ref('stg_streams') }}
),

latest_staging_snapshot AS (
    SELECT MAX(snapshot_timestamp) AS max_snapshot_timestamp
    FROM stg_streams
)

{% if is_incremental() %}
-- Get the newest snapshot already loaded into this fact table
, latest_fact_snapshot AS (
    SELECT MAX(snapshot_timestamp) AS max_snapshot_timestamp
    FROM {{ this }}
)
{% endif %}

SELECT
    MD5(
        COALESCE(s.stream_id::STRING, '') ||
        COALESCE(s.snapshot_timestamp::STRING, '')
    ) AS stream_snapshot_id,

    s.stream_id,
    s.user_id,
    s.game_id,
    s.viewer_count,
    s.stream_title,
    s.language,

    s.started_at_timestamp,
    s.snapshot_timestamp,
    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM stg_streams s
JOIN latest_staging_snapshot l
    ON s.snapshot_timestamp = l.max_snapshot_timestamp

{% if is_incremental() %}
WHERE s.snapshot_timestamp >
      (SELECT max_snapshot_timestamp FROM latest_fact_snapshot)
   OR (SELECT max_snapshot_timestamp FROM latest_fact_snapshot) IS NULL
{% endif %}
