from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.sensors.external_task import ExternalTaskSensor
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os

# --- 1. Define Your dbt Project Path ---
# This tells cosmos where to find your dbt project
# It MUST be at: dags/twitch_pipeline
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/twitch_pipeline"

# --- 2. Define Your dbt Profile ---
# This tells cosmos how to connect to Snowflake,
# using the Airflow Connection 'snowflake_dbt_conn'
SNOWFLAKE_PROFILE_CONFIG = ProfileConfig(
    profile_name="twitch_pipeline", # This must match the 'profile' in dbt_project.yml
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dbt_conn",
        profile_args={
            "database": "ANALYTICS",
            "schema": "PUBLIC",
            "role": "ACCOUNTADMIN",
            "warehouse": "COMPUTE_WH"
        },
    ),
)

# --- 3. Create a Standard Airflow DAG ---
# We are no longer using the "magic" DbtDag
@dag(
    dag_id="dbt_transform_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule='*/15 * * * *',  # Match the ingestion schedule
    catchup=False,
    tags=['twitch', 'dbt', 'transform'],
    default_args={"retries": 2},
)
def dbt_transform_dag():
    """
    This DAG uses DbtTaskGroup to run the dbt build command.
    This moves the dbt project parsing from Airflow's "parse time"
    to the DAG's "run time", fixing the DagBag timeout error.
    """
    
    # --- 4. Define the dbt Task Group ---
    DbtTaskGroup(
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=SNOWFLAKE_PROFILE_CONFIG,
        
        # This tells cosmos to run "dbt build"
        render_config=RenderConfig(
            select=['path:models/'] 
        ),
        
        operator_args={
            "install_deps": True, 
        },
    )

# --- 5. Instantiate the DAG ---
# This is a standard Airflow practice to make the DAG discoverable
dbt_transform_dag()
