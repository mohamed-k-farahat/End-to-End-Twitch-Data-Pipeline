# 🎮 End-to-End Twitch Data Pipeline

<div align="center">

![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.0+-red.svg)
![dbt](https://img.shields.io/badge/dbt-1.0+-orange.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8.svg)
![Azure](https://img.shields.io/badge/Azure-Blob%20Storage-0078D4.svg)

*A production-ready data pipeline that extracts real-time streaming data from Twitch, orchestrates it through Azure and Snowflake, and transforms it into analytics-ready datasets.*

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Pipeline Flow](#-pipeline-flow)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Setup](#-setup)
- [Configuration](#-configuration)
- [Running the Pipeline](#-running-the-pipeline)
- [Data Models](#-data-models)
- [Monitoring](#-monitoring)
- [Troubleshooting](#-troubleshooting)

---

## 🎯 Overview

This project implements a **modern data engineering pipeline** that:

- 📡 **Extracts** live streaming data from the Twitch API every 15 minutes
- 💾 **Loads** raw JSON files into Azure Blob Storage for data lake storage
- 🔄 **Ingests** data into Snowflake using Snowpipes for near real-time loading
- 🔨 **Transforms** raw data into dimensional models using dbt
- 📊 **Produces** analytics-ready fact and dimension tables

The pipeline is orchestrated using **Apache Airflow** on Astronomer, providing robust scheduling, monitoring, and error handling capabilities.

---

## 🏗️ Architecture

```
┌─────────────────┐
│   Twitch API    │  ← Live streaming data
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Apache Airflow  │  ← Orchestration (Every 15 min)
│  (Astronomer)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Azure Blob      │  ← Raw JSON storage (Data Lake)
│   Storage       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Snowpipe      │  ← Auto-ingest on file arrival
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Snowflake     │  ← Data Warehouse
│  (Raw Tables)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   dbt Models    │  ← Transformation (Hourly)
│ (Staging/Marts) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Analytics     │  ← BI Tools / Data Science
│     Layer       │
└─────────────────┘
```

---

## ✨ Features

### 🚀 Data Ingestion
- **Real-time extraction** from Twitch API (streams, users, games)
- **Batch processing** of 100+ concurrent streams
- **Automatic retries** and error handling
- **Timestamped snapshots** for historical tracking

### 🔄 Data Transformation
- **Incremental models** for efficient processing
- **Dimensional modeling** (star schema)
- **Data quality tests** with dbt
- **Idempotent transformations** using merge strategies

### 📊 Analytics Ready
- **Fact tables** for stream snapshots with temporal tracking
- **Dimension tables** for streamers and games
- **Pre-aggregated metrics** for dashboard consumption
- **Type-2 slowly changing dimensions** support

### 🛠️ DevOps & Monitoring
- **Containerized** deployment with Docker
- **Version controlled** infrastructure
- **Airflow UI** for pipeline monitoring
- **dbt docs** for data lineage and documentation

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow (Astronomer) | Workflow scheduling and management |
| **Data Lake** | Azure Blob Storage | Raw data storage |
| **Data Warehouse** | Snowflake | Analytics database |
| **Transformation** | dbt (Data Build Tool) | SQL-based transformations |
| **Language** | Python 3.9+ | API extraction and scripting |
| **Infrastructure** | Docker | Containerization |
| **Version Control** | Git | Source code management |

### Key Python Libraries
- `apache-airflow-providers-snowflake` - Snowflake integration
- `apache-airflow-providers-microsoft-azure` - Azure integration
- `astronomer-cosmos` - dbt + Airflow integration
- `azure-storage-blob` - Azure Blob Storage SDK
- `dbt-snowflake` - dbt Snowflake adapter
- `requests` - HTTP API calls

---

## 🔄 Pipeline Flow

### **DAG 1: Twitch Ingestion Pipeline** (`twitch_ingestion_pipeline`)
**Schedule:** Every 15 minutes  
**Tags:** `twitch`, `ingestion`

```
┌──────────────────────────┐
│  extract_and_load_task   │ → Fetch from Twitch API
│                          │   & Upload to Azure Blob
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│   refresh_snowpipes      │ → Trigger Snowflake ingestion
└──────────────────────────┘
```

**What it does:**
1. Authenticates with Twitch OAuth
2. Fetches top 100 live streams
3. Batch retrieves user and game details
4. Uploads JSON files to Azure Blob Storage (`streams/`, `users/`, `games/`)
5. Refreshes Snowflake pipes to ingest new data

### **DAG 2: dbt Transform Pipeline** (`dbt_transform_pipeline`)
**Schedule:** Hourly  
**Tags:** `twitch`, `dbt`, `transform`

```
┌──────────────────────────┐
│   dbt_build_group        │ → Run all dbt models
│   ├── stg_streams        │   (staging views)
│   ├── stg_users          │
│   ├── stg_games          │
│   ├── dim_streamers      │   (dimension tables)
│   ├── dim_games          │
│   └── fact_stream_       │   (fact tables)
│       snapshots          │
└──────────────────────────┘
```

**What it does:**
1. Runs dbt staging models (views in `STAGING` schema)
2. Builds dimension tables (streamers, games)
3. Creates fact table with incremental stream snapshots
4. Runs data quality tests

---

## 📁 Project Structure

```
Twitch_PipeLine_dbt_dag/
├── dags/
│   ├── dag_twitch_ingest.py          # Extraction & loading DAG
│   ├── dag_dbt_transform.py          # dbt transformation DAG
│   ├── twitch_extraction.py          # Twitch API logic
│   └── twitch_pipeline/              # dbt project
│       ├── dbt_project.yml           # dbt configuration
│       ├── models/
│       │   ├── staging/              # Staging views
│       │   │   ├── stg_streams.sql
│       │   │   ├── stg_users.sql
│       │   │   └── stg_games.sql
│       │   └── marts/                # Analytics tables
│       │       ├── dim_streamers.sql
│       │       ├── dim_games.sql
│       │       └── fact_stream_snapshots.sql
│       ├── macros/
│       │   └── get_current_timestamp.sql
│       └── tests/
├── requirements.txt                   # Python dependencies
├── Dockerfile                         # Astronomer Runtime image
├── airflow_settings.yaml              # Local connections & variables
└── README.md                          # This file
```

---

## 📦 Prerequisites

Before you begin, ensure you have:

- ✅ **Docker Desktop** installed and running
- ✅ **Astronomer CLI** (`astro`) installed: [Installation Guide](https://docs.astronomer.io/astro/cli/install-cli)
- ✅ **Twitch Developer Account** with API credentials ([Get here](https://dev.twitch.tv/))
- ✅ **Azure Storage Account** with Blob Storage enabled
- ✅ **Snowflake Account** with appropriate permissions
- ✅ **Git** for version control

### Required Accounts & Credentials

| Service | What You Need |
|---------|---------------|
| **Twitch** | Client ID, Client Secret |
| **Azure** | Storage Account Connection String |
| **Snowflake** | Account URL, Username, Password, Warehouse, Database |

---

## 🚀 Setup

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/mohamed-k-farahat/End-to-End-Twitch-Data-Pipeline.git
cd Twitch_PipeLine_dbt_dag
```

### 2️⃣ Install Astronomer CLI

```bash
# Windows (PowerShell)
winget install -e --id Astronomer.Astro

# macOS
brew install astro

# Linux
curl -sSL install.astronomer.io | sudo bash -s
```

### 3️⃣ Set Up Snowflake

Run the following SQL in your Snowflake worksheet:

```sql
-- Create database and schema
CREATE DATABASE ANALYTICS;
CREATE SCHEMA ANALYTICS.PUBLIC;
CREATE SCHEMA ANALYTICS.STAGING;

-- Create raw tables
CREATE TABLE ANALYTICS.PUBLIC.RAW_STREAMS (
    RAW_JSON VARIANT,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE ANALYTICS.PUBLIC.RAW_USERS (
    RAW_JSON VARIANT,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE ANALYTICS.PUBLIC.RAW_GAMES (
    RAW_JSON VARIANT,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create storage integration with Azure
CREATE STORAGE INTEGRATION azure_twitch_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your-tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://<storage-account>.blob.core.windows.net/twitch-raw-data/');

-- Create stages
CREATE STAGE azure_streams_stage
  STORAGE_INTEGRATION = azure_twitch_integration
  URL = 'azure://<storage-account>.blob.core.windows.net/twitch-raw-data/streams/';

CREATE STAGE azure_users_stage
  STORAGE_INTEGRATION = azure_twitch_integration
  URL = 'azure://<storage-account>.blob.core.windows.net/twitch-raw-data/users/';

CREATE STAGE azure_games_stage
  STORAGE_INTEGRATION = azure_twitch_integration
  URL = 'azure://<storage-account>.blob.core.windows.net/twitch-raw-data/games/';

-- Create Snowpipes
CREATE PIPE pipe_streams
  AUTO_INGEST = TRUE
  AS
  COPY INTO RAW_STREAMS(RAW_JSON, LOADED_AT)
  FROM (
    SELECT $1, CURRENT_TIMESTAMP()
    FROM @azure_streams_stage
  )
  FILE_FORMAT = (TYPE = 'JSON');

CREATE PIPE pipe_users
  AUTO_INGEST = TRUE
  AS
  COPY INTO RAW_USERS(RAW_JSON, LOADED_AT)
  FROM (
    SELECT $1, CURRENT_TIMESTAMP()
    FROM @azure_users_stage
  )
  FILE_FORMAT = (TYPE = 'JSON');

CREATE PIPE pipe_games
  AUTO_INGEST = TRUE
  AS
  COPY INTO RAW_GAMES(RAW_JSON, LOADED_AT)
  FROM (
    SELECT $1, CURRENT_TIMESTAMP()
    FROM @azure_games_stage
  )
  FILE_FORMAT = (TYPE = 'JSON');
```

### 4️⃣ Configure Azure Event Grid (Optional for Auto-Ingest)

For automatic Snowpipe triggering, set up Azure Event Grid to notify Snowflake when new blobs are created. Follow [Snowflake's Azure Event Grid guide](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-azure).

---

## ⚙️ Configuration

### Edit `airflow_settings.yaml`

```yaml
airflow:
  connections:
    - conn_id: azure_blob_conn
      conn_type: generic
      extra: |
        {
          "connection_string": "DefaultEndpointsProtocol=https;AccountName=<your-account>;AccountKey=<your-key>;EndpointSuffix=core.windows.net"
        }
    
    - conn_id: snowflake_pipe_conn
      conn_type: snowflake
      login: <your-username>
      password: <your-password>
      schema: PUBLIC
      extra: |
        {
          "account": "<your-account>.snowflakecomputing.com",
          "warehouse": "COMPUTE_WH",
          "database": "ANALYTICS",
          "region": "<your-region>",
          "role": "ACCOUNTADMIN"
        }
    
    - conn_id: snowflake_dbt_conn
      conn_type: snowflake
      login: <your-username>
      password: <your-password>
      schema: PUBLIC
      extra: |
        {
          "account": "<your-account>.snowflakecomputing.com",
          "warehouse": "COMPUTE_WH",
          "database": "ANALYTICS",
          "region": "<your-region>",
          "role": "ACCOUNTADMIN"
        }

  variables:
    - variable_name: TWITCH_CLIENT_ID
      variable_value: <your-twitch-client-id>
    
    - variable_name: TWITCH_CLIENT_SECRET
      variable_value: <your-twitch-client-secret>
```

---

## 🏃 Running the Pipeline

### Start Airflow Locally

```bash
# Start all Airflow services
astro dev start
```

This will spin up **5 Docker containers**:
- 🗄️ **Postgres** - Metadata database
- 📅 **Scheduler** - Task scheduling
- 🔄 **Triggerer** - Deferred tasks
- 🖥️ **Webserver** - UI at http://localhost:8080
- ⚙️ **DAG Processor** - DAG parsing

**Default Credentials:**  
Username: `admin`  
Password: `admin`

### Access the Airflow UI

1. Open your browser to **http://localhost:8080**
2. Log in with `admin` / `admin`
3. Enable the DAGs:
   - `twitch_ingestion_pipeline`
   - `dbt_transform_pipeline`

### Trigger the Pipeline Manually

```bash
# Trigger ingestion DAG
astro dev run dags trigger twitch_ingestion_pipeline

# Trigger transformation DAG
astro dev run dags trigger dbt_transform_pipeline
```

### View Logs

```bash
# Follow scheduler logs
astro dev logs -f scheduler

# Follow webserver logs
astro dev logs -f webserver
```

### Stop the Environment

```bash
astro dev stop
```

---

## 📊 Data Models

### Staging Layer (`ANALYTICS.STAGING`)

| Model | Type | Description |
|-------|------|-------------|
| `stg_streams` | View | Flattened stream data with snapshot timestamps |
| `stg_users` | View | Parsed user/streamer details |
| `stg_games` | View | Game metadata |

### Marts Layer (`ANALYTICS.PUBLIC`)

| Model | Type | Strategy | Description |
|-------|------|----------|-------------|
| `dim_streamers` | Table | - | Dimension table for streamers with SCD Type-1 |
| `dim_games` | Table | - | Dimension table for games |
| `fact_stream_snapshots` | Table | Incremental (Merge) | Fact table with stream metrics per snapshot |

### Sample Queries

```sql
-- Top 10 streamers by total viewers
SELECT 
    ds.streamer_name,
    SUM(fss.viewer_count) AS total_viewers,
    COUNT(*) AS snapshot_count
FROM ANALYTICS.PUBLIC.FACT_STREAM_SNAPSHOTS fss
JOIN ANALYTICS.PUBLIC.DIM_STREAMERS ds ON fss.streamer_id = ds.streamer_id
GROUP BY ds.streamer_name
ORDER BY total_viewers DESC
LIMIT 10;

-- Most popular games
SELECT 
    dg.game_name,
    COUNT(DISTINCT fss.stream_id) AS unique_streams,
    AVG(fss.viewer_count) AS avg_viewers
FROM ANALYTICS.PUBLIC.FACT_STREAM_SNAPSHOTS fss
JOIN ANALYTICS.PUBLIC.DIM_GAMES dg ON fss.game_id = dg.game_id
GROUP BY dg.game_name
ORDER BY unique_streams DESC;
```

---

## 📈 Monitoring

### Airflow UI
- **DAG Runs:** Track success/failure rates
- **Task Duration:** Identify bottlenecks
- **Logs:** Debug errors in real-time
- **Gantt Chart:** Visualize task dependencies

### dbt Docs

Generate and view dbt documentation:

```bash
# SSH into the scheduler container
astro dev bash -s scheduler

# Navigate to dbt project
cd dags/twitch_pipeline

# Generate docs
dbt docs generate --profiles-dir .

# Serve docs (if dbt docs serve is available)
dbt docs serve --profiles-dir .
```

### Snowflake Monitoring

```sql
-- Check Snowpipe status
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START=>DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    PIPE_NAME=>'ANALYTICS.PUBLIC.PIPE_STREAMS'
));

-- Check copy history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'ANALYTICS.PUBLIC.RAW_STREAMS',
    START_TIME=>DATEADD('hour', -24, CURRENT_TIMESTAMP())
));
```

---

## 🔧 Troubleshooting

### Common Issues

#### **DAG not appearing in Airflow UI**
```bash
# Check for parsing errors
astro dev run dags list

# Validate Python syntax
python dags/dag_twitch_ingest.py
```

#### **Snowflake connection failed**
- Verify account identifier format: `<account>.<region>.snowflakecomputing.com`
- Check username, password, and role permissions
- Ensure warehouse is running

#### **Azure Blob upload failed**
- Validate connection string in `airflow_settings.yaml`
- Check container name: `twitch-raw-data`
- Verify Azure Storage access keys

#### **dbt models failing**
```bash
# Test connection
astro dev bash -s scheduler
cd dags/twitch_pipeline
dbt debug --profiles-dir .

# Run specific model
dbt run --select stg_streams --profiles-dir .
```

#### **Port already in use**
```bash
# Change Airflow port in docker-compose.override.yml
astro config set webserver.port 8081
```

### Logs Location

```bash
# View all logs
astro dev logs

# Scheduler logs only
astro dev logs -f scheduler

# Follow logs for specific task
# (In Airflow UI: DAG > Task > Log)
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Mohamed K. Farahat**

- GitHub: [@mohamed-k-farahat](https://github.com/mohamed-k-farahat)
- Repository: [End-to-End-Twitch-Data-Pipeline](https://github.com/mohamed-k-farahat/End-to-End-Twitch-Data-Pipeline)

---

## 🙏 Acknowledgments

- [Astronomer](https://www.astronomer.io/) for the amazing Airflow platform
- [dbt Labs](https://www.getdbt.com/) for the dbt framework
- [Twitch](https://dev.twitch.tv/) for providing the API
- [Snowflake](https://www.snowflake.com/) for the data warehouse
- [Microsoft Azure](https://azure.microsoft.com/) for cloud storage

---

<div align="center">

**⭐ Star this repo if you find it helpful!**

Made with ❤️ and ☕ by Mohamed Farahat

</div>
