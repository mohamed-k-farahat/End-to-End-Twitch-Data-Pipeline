-- -- models/staging/stg_users.sql
WITH source AS (
    SELECT 
        RAW_JSON,
        LOADED_AT
    FROM {{ source('api_data', 'RAW_USERS') }}
),
flattened AS (
    SELECT 
        value AS user_data,
        LOADED_AT
    FROM 
        source,
        LATERAL FLATTEN(input => source.RAW_JSON)
),
parsed AS (
    SELECT 
        user_data:id::STRING AS user_id,
        user_data:login::STRING AS user_login,
        user_data:display_name::STRING AS display_name,
        user_data:type::STRING AS user_type,
        user_data:description::STRING AS description,
        user_data:profile_image_url::STRING AS profile_image_url,
        user_data:created_at::TIMESTAMP_NTZ AS created_at,
        LOADED_AT
    FROM 
        flattened
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY loaded_at DESC) AS rn
    FROM
        parsed
)
-- Fixed the 'EXCEPT' syntax error
SELECT
    user_id,
    user_login,
    display_name,
    user_type,
    description,
    profile_image_url,
    created_at,
    LOADED_AT
FROM deduplicated
WHERE rn = 1