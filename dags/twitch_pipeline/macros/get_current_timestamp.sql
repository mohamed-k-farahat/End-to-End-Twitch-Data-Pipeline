-- macros/get_current_timestamp.sql

{% macro get_current_timestamp() %}
    CURRENT_TIMESTAMP()
{% endmacro %}