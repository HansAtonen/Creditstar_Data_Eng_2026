# Creditstar Data Engineering - Real-Time Feature Pipeline

A pure streaming pipeline that processes Change Data Capture (CDC) events from PostgreSQL via Kafka to compute and store real-time client features in MinIO.

## Architecture Overview

- **PostgreSQL**: Source database with loan and payment data
- **Kafka + Debezium**: CDC pipeline for streaming database changes
- **Python Pipeline**: Real-time feature processing
- **MinIO**: Feature storage with Hive-style partitioning

---

## Setup Instructions

### Prerequisites

Before starting, ensure you have the following installed:
- **Docker & Docker Compose** - [Install Docker](https://docs.docker.com/get-docker/)
- **Python 3.8+** - [Install Python](https://www.python.org/downloads/)
- **PowerShell** - Windows users (included with Windows)

---

### Step 1: Create and Activate Virtual Environment

```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment (Windows)
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

---

### Step 2: Start Docker Services

```powershell
docker compose up -d
```

This will start:
- PostgreSQL database (`de_postgres`)
- Kafka broker
- Kafka Connect (Debezium)
- MinIO object storage

**Verify services are running:**
```powershell
docker compose ps
```

---

### Step 3: Configure Kafka Connector (Connect PostgreSQL to Kafka)

Once all services are running, register the PostgreSQL Debezium connector:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -Body '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "admin",
      "database.password": "admin",
      "database.dbname": "credit_db",
      "topic.prefix": "credit",
      "plugin.name": "pgoutput",
      "table.include.list": "public.loan,public.payment",
      "slot.name": "debezium_slot",
      "publication.name": "debezium_pub"
    }
  }'
```

This connector will:
- Monitor the `loan` and `payment` tables in PostgreSQL
- Stream all INSERT, UPDATE, and DELETE operations to Kafka
- Create topics: `credit.public.loan` and `credit.public.payment`

---

### Step 4: Create MinIO Bucket for Features

Open MinIO console: **http://localhost:9001**

- **Username:** `admin`
- **Password:** `admin123`

Create a new bucket named `features`. This is where all computed client features will be stored.

---

### Step 5: Start the Python Pipeline

Navigate to the pipeline directory and run:

```powershell
cd "path\to\Creditstar\Data Engineering"
python pipeline.py
```

The pipeline will:
- Connect to Kafka on `localhost:9092`
- Listen for CDC events on loan and payment topics
- Compute real-time features for each client
- Upload feature files to MinIO immediately upon calculation

**Expected output:**
```
🚀 Pure Streaming Pipeline Started.
📥 Monitoring Kafka for CDC events... (Direct I/O)
✅ Real-time Feature Upload: features/client_id=X/YYYYMMDD_HHMMSS_XXXXXX.parquet
```

---

### Step 6: Restore Database Dump

Once the pipeline is running, populate the PostgreSQL database with test data:

#### 6a. Copy and Restore the Dump

Open a new terminal
```powershell
# Copy dump file to Docker container
docker cp de_test_task_db de_postgres:/tmp/de_test_task_db

# Restore the database from dump
docker exec de_postgres pg_restore -U admin -d credit_db --no-owner /tmp/de_test_task_db

# Verify tables were restored
docker exec de_postgres psql -U admin -d credit_db -c "\dt"
```

#### 6b. Monitor Database Population Progress

Run this command to watch records being inserted:

```powershell
docker exec -it de_postgres psql -U admin -d credit_db -c "SELECT relname, n_live_tup FROM pg_stat_user_tables;"
```

The Debezium connector will automatically capture all insert operations and stream them to Kafka, triggering the pipeline to compute and store features for each client.

---

## Computed Features

The pipeline computes the following features for each client:

1. **paid_loans_count** - Total number of paid loans
2. **days_since_last_late_payment** - Days elapsed since last late payment (NULL if no late payments)
3. **profit_in_last_90_days_rate** - Interest earned / total loan amount in last 90 days

---

## Output Storage

Features are stored in MinIO with Hive-style partitioning:

```
features/
├── client_id=1/
│   ├── 20260508_120000_123456.parquet
│   ├── 20260508_120030_654321.parquet
│   └── ...
├── client_id=2/
│   ├── 20260508_120015_789012.parquet
│   └── ...
```

Each parquet file contains:
- `client_id`
- `computed_at` (ISO timestamp)
- All computed feature columns

---

## Design Decisions & Tradeoffs

### Current Architecture: Pure Real-Time Streaming

**Why This Approach?**

The current implementation uses local Docker with Kafka CDC as a cost-effective, real-time solution suitable for:
- **Development/Testing**: Low infrastructure overhead
- **Simplicity**: Open-source tools, minimal configuration
- **Near Real-Time Features**: CDC ensures immediate processing of database changes
- **Budget Constraints**: No cloud provider fees

### Tradeoffs Considered

#### Advantages
- **Cost**: Docker + open-source tools = minimal expense
- **Implementation**: Minimal configuration needed to get data moving
- **Real-Time Ready**: Unlike batch processing, CDC captures every change the moment it happens, which is critical for time-sensitive features like "late payment" alerts.

#### Some Disadvantages
- **File Proliferation**: One parquet file per client per event causes storage bloat
  - Example: 10,000 clients with 100 updates/day = 1M files/month
  - Data scientists struggle with queryability and performance
- **No Deduplication**: Multiple late-payment records create redundant feature files
- **Scalability**: Single-machine Docker cannot handle production volume or dump load.
- **Reliability**: No High Availability (HA) - pipeline failure = data loss risk
- **Query Performance**: Data scientists must merge dozens of files per client

### Some ideas to Improve With More Budget/Time

**Delta Lake / Apache Iceberg**
   - Replace parquet with Delta Lake format
   - Benefits:
     - Time travel (query features as of specific timestamp)
     - Automatic schema evolution
     - Deduplication on writes
     - Data scientists query single latest snapshot instead of multiple files

**Data Quality & Monitoring**
   - Add validation rules: `days_since_late_payment >= 0`, `profit_rate between 0-1`
   - Alert on pipeline lag or failed uploads
   - Log every feature computation with metadata (timestamp, record count, latency)


**Cloud Migration** (Optional)
   - **Azure**: Cosmos DB + Event Hubs + Databricks
   - Benefits: Managed High availability, Autoscaling
