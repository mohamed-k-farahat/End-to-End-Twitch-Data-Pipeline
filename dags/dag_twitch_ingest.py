from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os

SNOWPIPE_REFRESH_SQL = """
ALTER PIPE pipe_streams REFRESH;
ALTER PIPE pipe_users REFRESH;
ALTER PIPE pipe_games REFRESH;
"""

@dag(
    dag_id='twitch_ingestion_pipeline',
    start_date=datetime(2025, 11, 1),
    schedule='*/15 * * * *',
    catchup=False,
    tags=['twitch', 'ingestion']
)
def twitch_ingestion_dag():

    @task
    def extract_and_load_task():
        try:
            from twitch_extraction import fetch_and_load_to_blob
            fetch_and_load_to_blob()
        except ImportError:
            print("ERROR: Could not import twitch_extraction.py")
            print(f"Current working directory: {os.getcwd()}")
            raise

    refresh_snowpipes = SQLExecuteQueryOperator(
        task_id='refresh_snowpipes',
        conn_id='snowflake_pipe_conn',
        sql=SNOWPIPE_REFRESH_SQL
    )

    extract_and_load_task() >> refresh_snowpipes

twitch_ingestion_dag()
