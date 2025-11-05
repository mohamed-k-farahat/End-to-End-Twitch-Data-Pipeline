-- models/marts/dim_games.sql
-- This model creates a physical table of all unique games.
-- The config is set in dbt_project.yml to be a "table".

SELECT
    game_id,
    game_name,
    box_art_url,
    {{ get_current_timestamp() }} AS loaded_at -- Uses our macro
FROM
    {{ ref('stg_games') }}