-- models/staging/stg_games.sql
WITH source AS (
    SELECT 
        RAW_JSON,
        LOADED_AT
    FROM {{ source('api_data', 'RAW_GAMES') }}
),
flattened AS (
    SELECT 
        value AS game_data,
        LOADED_AT
    FROM 
        source,
        LATERAL FLATTEN(input => source.RAW_JSON)
),
parsed AS (
    SELECT 
        game_data:id::STRING AS game_id,
        game_data:name::STRING AS game_name,
        REPLACE(
            REPLACE(game_data:box_art_url::STRING, '{width}', '288'), 
            '{height}', '384'
        ) AS box_art_url,
        LOADED_AT
    FROM 
        flattened
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY game_id ORDER BY loaded_at DESC) AS rn
    FROM
        parsed
)
-- Fixed the 'EXCEPT' syntax error
SELECT
    game_id,
    game_name,
    box_art_url,
    LOADED_AT
FROM deduplicated
WHERE rn = 1