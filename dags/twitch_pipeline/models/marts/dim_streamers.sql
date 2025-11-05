-- models/marts/dim_streamers.sql
-- This model creates a physical table of all unique streamers.
-- The config is set in dbt_project.yml to be a "table".

SELECT
    user_id,
    user_login,
    display_name,    -- This was the typo, it's now correct.
    description,
    profile_image_url,
    created_at,
    {{ get_current_timestamp() }} AS loaded_at -- Uses our macro
FROM
    {{ ref('stg_users') }}