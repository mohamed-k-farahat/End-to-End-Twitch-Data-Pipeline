FROM astrocrpublic.azurecr.io/runtime:3.1-3


RUN python -m venv dbt-venv && source dbt-venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate