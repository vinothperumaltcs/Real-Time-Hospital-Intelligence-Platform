#  Real-Time Hospital Intelligence Platform

A real-time data engineering pipeline built on Azure that monitors hospital patient flow and capacity across a multi-hospital network. The system streams live patient admission and discharge events through a **Medallion Architecture (Bronze → Silver → Gold)** on Azure Databricks and Delta Lake, and serves analytics-ready data via Azure Synapse SQL Pool and Power BI.

---

##  Architecture

![Real-Time Hospital Intelligence Platform Architecture](architecture.png)

The pipeline is orchestrated end-to-end using **Azure Data Factory (ADF)**. Notably, the Gold layer transformation is triggered conditionally by ADF — it runs only when the incoming silver record count exceeds 1, ensuring efficient processing and avoiding unnecessary compute.

---

##  Key Features

- **Real-time streaming** via Azure Event Hubs with Kafka-compatible protocol
- **Medallion Architecture** implemented on Delta Lake (Bronze, Silver, Gold)
- **Schema evolution** support in the Silver layer using Delta Lake's `mergeSchema`
- **SCD Type 2 (Slowly Changing Dimension)** for patient dimension history tracking in the Gold layer
- **Star Schema** with Fact and Dimension tables optimized for analytical queries
- **ADF-triggered Gold layer** — conditional execution based on record count thresholds
- **Data quality enforcement** — invalid ages and future timestamps are detected and corrected in Silver
- **Synapse SQL Pool** external tables over Delta Lake for serverless querying


---

##  Repository Structure

```
azurepulse/
│
├── databricks-notebooks/
│   ├── 01_bronze_rawdata.py              # Bronze: raw ingestion from Event Hubs to Delta
│   ├── 02_silver_cleandata.py            # Silver: schema parsing, cleaning, validation
│   └── 03_gold_transform.py             # Gold: SCD Type 2 dims + fact table construction
│
├── simulator/
│   └── patient_flow_generator.py        # Kafka producer: synthetic patient event generator
│
├── sqlpool-quries/
│   └── SQL_pool_quries.sql              # Synapse external table DDL + sample queries
│
└── README.md
```

---

##  Pipeline Walkthrough

### 1. Data Simulator — `patient_flow_generator.py`

A Kafka producer that continuously sends synthetic patient events to Azure Event Hubs. Each event includes:

| Field | Description |
|---|---|
| `patient_id` | UUID |
| `gender` | Male / Female |
| `age` | 1–100 (with 5% dirty data: 101–150) |
| `department` | Emergency, ICU, Surgery, Cardiology, etc. |
| `admission_time` | UTC timestamp (with 5% future timestamps injected) |
| `discharge_time` | Derived from admission + random hours |
| `bed_id` | 1–500 |
| `hospital_id` | 1–7 (simulating a 7-hospital network) |

Dirty data is intentionally injected to validate downstream cleansing logic.

---

### 2. Bronze Layer — `01_bronze_rawdata.py`

Reads the raw Kafka stream from Event Hubs and writes it as-is to Delta Lake on ADLS Gen2.

- **Input:** Azure Event Hub (Kafka endpoint)
- **Output:** Delta table at the Bronze path
- **Mode:** Streaming (`readStream` / `writeStream`) with checkpoint
- **Transformation:** Casts raw Kafka bytes to JSON string (`raw_json`), no parsing

---

### 3. Silver Layer — `02_silver_cleandata.py`

Reads from the Bronze Delta stream, parses JSON, applies a defined schema, and enforces data quality rules.

**Schema:**
```
patient_id: String, gender: String, age: Integer,
department: String, admission_time: Timestamp,
discharge_time: Timestamp, bed_id: Integer, hospital_id: Integer
```

**Data Quality Rules:**
- `admission_time` — future timestamps are replaced with `current_timestamp()`
- `age` — values > 100 are replaced with a random integer between 1–90
- Missing columns are added as `null` (schema evolution support)
- Written with `mergeSchema = true` to accommodate upstream schema changes

---

### 4. Gold Layer — `03_gold_transform.py`

>  **Triggered by ADF** — this notebook runs only when the Silver record count exceeds 1.

Transforms Silver data into a dimensional model with full SCD Type 2 support.

#### `dim_patient` — SCD Type 2

Tracks historical changes to patient attributes (`gender`, `age`).

| Column | Description |
|---|---|
| `surrogate_key` | Auto-generated integer PK |
| `patient_id` | Natural key |
| `gender` / `age` | Tracked attributes |
| `effective_from` | When the record became active |
| `effective_to` | When the record was superseded (NULL = current) |
| `is_current` | Boolean flag for latest record |

Change detection uses SHA-256 hash comparison. Changed records are expired (`is_current = false`, `effective_to` stamped) and new versions are inserted.

#### `dim_department`

Static dimension mapping `department` + `hospital_id` to a surrogate key. Overwritten on each run (no SCD needed).

#### `fact_patient_flow`

Central fact table joining Silver events with dimension surrogate keys.

| Column | Description |
|---|---|
| `fact_id` | Monotonically increasing ID |
| `patient_sk` | FK to `dim_patient` |
| `department_sk` | FK to `dim_department` |
| `admission_time` / `discharge_time` | Event timestamps |
| `admission_date` | Date partition key |
| `length_of_stay_hours` | Computed: `(discharge - admission) / 3600` |
| `is_currently_admitted` | True if `discharge_time > current_timestamp()` |
| `event_ingestion_time` | Pipeline audit timestamp |

---

### 5. Synapse SQL Pool — `SQL_pool_quries.sql`

External tables are created over the Gold Delta Lake path using Managed Identity for secure access. No data movement required — Synapse reads directly from ADLS Gen2 parquet files.

```sql
-- Example
SELECT * FROM dbo.fact_patient_flow;
```

Tables exposed:
- `dbo.dim_patient`
- `dbo.dim_department`
- `dbo.fact_patient_flow`

---

##  Prerequisites

| Component | Service |
|---|---|
| Streaming | Azure Event Hubs (Kafka-compatible) |
| Compute | Azure Databricks (with Delta Lake) |
| Storage | Azure Data Lake Storage Gen2 |
| Orchestration | Azure Data Factory |
| Warehouse | Azure Synapse Analytics (Dedicated SQL Pool) |
| Security | Azure Key Vault, Azure Active Directory |
| Visualization | Power BI |
| Local Simulator | Python 3.x, `kafka-python` package |

---

##  Configuration

All notebooks use placeholder tokens that must be replaced before deployment:

| Placeholder | Description |
|---|---|
| `<<Namespace_hostname>>` | Event Hubs namespace (e.g. `myns.servicebus.windows.net`) |
| `<<Eventhub_Name>>` | Event Hub name |
| `<<Connection_string>>` | Event Hub connection string (primary key) |
| `<<Storageaccount_name>>` | ADLS Gen2 storage account name |
| `<<Storage_Account_access_key>>` | Storage account access key |
| `<<container>>` | ADLS container name |
| `<<path>>` | Delta table path within the container |

> **Recommended:** Store all secrets in **Azure Key Vault** and reference them via Databricks Secret Scopes instead of hardcoding credentials.

---

##  Running the Pipeline

**1. Start the simulator:**
```bash
pip install kafka-python
python simulator/patient_flow_generator.py
```

**2. Run Databricks notebooks in order:**
```
01_bronze_rawdata.py   →  02_silver_cleandata.py  →  03_gold_transform.py
```
Or deploy via **ADF pipeline** where the Gold notebook is triggered conditionally.

**3. Create Synapse external tables:**
```sql
-- Run SQL_pool_quries.sql in your Synapse dedicated SQL pool
```

**4. Connect Power BI** to the Synapse SQL Pool endpoint and build dashboards.

---

##  Sample Analytical Queries

```sql
-- Current bed occupancy by department
SELECT d.department, COUNT(*) AS current_patients
FROM dbo.fact_patient_flow f
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
WHERE f.is_currently_admitted = 1
GROUP BY d.department;

-- Average length of stay per department
SELECT d.department, AVG(f.length_of_stay_hours) AS avg_los_hours
FROM dbo.fact_patient_flow f
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
GROUP BY d.department
ORDER BY avg_los_hours DESC;

-- Patient age distribution
SELECT p.age, p.gender, COUNT(*) AS admissions
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
WHERE p.is_current = 1
GROUP BY p.age, p.gender;
```

---

##  Security Notes

- Use **Azure Managed Identity** for Synapse-to-ADLS access (already configured in the SQL scripts).
- Rotate storage account keys and Event Hub connection strings regularly.
- For production, replace all hardcoded keys with **Databricks Secret Scope** backed by Azure Key Vault.

---

