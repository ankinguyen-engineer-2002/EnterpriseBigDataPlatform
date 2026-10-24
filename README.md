# Enterprise Commerce Data Platform

Production-shaped, end-to-end **Enterprise Data Engineering Platform** for e-commerce operations and real-time customer activity.

- **Scope:** 100% Data Platform Engineering (Ingestion, CDC, Streaming, Lakehouse, Warehouse, Orchestration, Governance, Observability, and Cloud IaC).
- **Scale Target:** `>= 100 GB` offline batch volume across core and extended commerce tables; `200–500 MB/s` sustained throughput (`0.5–1.0 GB/s` peak burst) with full Day-2 maintenance controls (compaction, state TTL, DLQ isolation, and backlog catch-up).
- **Canonical Docs:**
  - **Living Step-by-Step Build & Learning Guide:** [docs/runbooks/STEP_BY_STEP_BUILD_GUIDE.md](docs/runbooks/STEP_BY_STEP_BUILD_GUIDE.md)
  - Architecture & Design Decisions: [docs/architecture/PROJECT_SOLUTION.md](docs/architecture/PROJECT_SOLUTION.md)
  - Data Source & Simulation Strategy: [docs/research/DATA_SOURCE_AND_SIMULATION.md](docs/research/DATA_SOURCE_AND_SIMULATION.md)
  - 7-Stage Execution Checklist (100/100 DE Rubric): [docs/roadmap/IMPLEMENTATION_CHECKLIST.md](docs/roadmap/IMPLEMENTATION_CHECKLIST.md)

## End-to-End Architecture Flow

```mermaid
flowchart TB
    subgraph L1["1. SOURCE & LEGACY LAYER"]
        FASTAPI["FastAPI Order Service\n(Pydantic Validation)"]
        DATAGEN["Vectorized Data Generator\n(PyArrow/DuckDB: >=100GB Batch + Stream)"]
        PG[("PostgreSQL 16 OLTP\n(wal_level=logical + Indexes)")]
        LEGACY["Legacy Monolithic Report\n(legacy_daily_revenue_report SQL)"]
        FASTAPI --> PG
        DATAGEN --> PG
        PG --> LEGACY
    end

    subgraph L2["2. INGESTION & TRANSPORT LAYER"]
        CDC["Debezium CDC Connector"]
        CONTRACT["Schema & Data Contracts\n(Inject: Skew 80%, Late 5-10m, Dup 1.5-2%, Schema Evol)"]
        KAFKA[("Apache Kafka (KRaft)\nPartitioned Topics")]
        DLQ[("Dead-Letter Queue (DLQ)\nPoison Messages Isolation")]
        PG -->|WAL Stream| CDC --> CONTRACT
        DATAGEN -->|Clickstream & Callbacks| CONTRACT
        CONTRACT -->|Valid| KAFKA
        CONTRACT -->|Invalid| DLQ
    end

    subgraph L3["3. DISTRIBUTED PROCESSING & MEDALLION LAKEHOUSE (MinIO S3 + Delta Lake)"]
        RAW[("MinIO: s3://raw\n(Immutable Landing)")]
        FLINK["Apache Flink (Streaming)\n• Watermarks & Side Outputs (Late 5-10m)\n• KeyedProcessFunction + StateTTL (1.5% Dup)\n• Event-Time Tumbling/Sliding Windows"]
        BRONZE[("Delta Lake: Bronze (raw_)\nAppend-only History")]
        SPARK["Apache Spark (Batch Engine)\n• Skew Optimization (AQE / Key Salting)\n• Schema Evolution Merge (failure_reason)\n• Offline Dedup (2%) + OPTIMIZE & Z-ORDER"]
        SILVER[("Delta Lake: Silver (stg_)\nCleaned & Conformed")]

        DATAGEN -->|Bulk Parquet| RAW --> SPARK
        KAFKA --> FLINK --> BRONZE
        KAFKA --> SPARK
        BRONZE --> SPARK --> SILVER
    end

    subgraph L4["4. MODELING, FINTECH GOVERNANCE & WAP GATE"]
        DBT["dbt Core (Advanced)\n• staging / intermediate / marts\n• SCD Type 2 (dbt snapshot: dim_customers, dim_products)\n• Jinja PII Masking (SHA-256 email/phone/card)\n• Feature Table (feat_customer_order_metrics_90d)\n• Slim CI (state:modified+ via manifest.json)"]
        GOLD[("Delta Lake: Gold\n(dim_, fact_, obt_, feat_)")]
        WAP{"WAP Zero-Copy & Migration\nReconciliation Gate\n(DuckDB / PyArrow)"}
        ROLLBACK["Auto Rollback\n(Delta RESTORE TABLE)"]

        SILVER --> DBT --> GOLD
        GOLD --> WAP
        LEGACY -.->|Parallel Run Compare| WAP
        WAP -->|Fail: Delta Discrepancy| ROLLBACK --> GOLD
    end

    subgraph L5["5. OLAP SERVING WAREHOUSE"]
        CH[("ClickHouse OLAP\n• MergeTree / ReplacingMergeTree\n• Primary/Sorting Keys\n• Data Skipping Indexes & Materialized Views")]
        WAP -->|Pass: Zero Discrepancy| CH
    end

    subgraph L6["6. CONTROL, GOVERNANCE & INFRA/GITOPS PLANE"]
        AIRFLOW["Apache Airflow 3\n• DAGs: DP1, DP2, DP3 (Ingest + Validate)\n• Custom OOP Plugins: WAPReconciliationOperator,\n  ClickHousePublishHook (pytest tested)"]
        DATAHUB["DataHub Catalog\n• End-to-End Lineage\n• PII Classification Tags\n• Quality Assertions"]
        OBS["Observability & Security\n• Prometheus + Grafana + Loki\n• HashiCorp Vault + NGINX"]
        INFRA["IaC & GitOps Runtime\n• Multi-stage Dockerfile (>60% reduction)\n• Docker Compose (Local OrbStack ARM64)\n• K3s Helm/ArgoCD + Terraform/Ansible\n  (OCI us-phoenix-1 Always Free & Burst)"]
    end

    AIRFLOW -.->|Orchestrates| SPARK
    AIRFLOW -.->|Orchestrates| DBT
    AIRFLOW -.->|Triggers| WAP
    DATAHUB -.->|Tracks Lineage & PII| GOLD
    DATAHUB -.->|Tracks Lineage| CH
```

## Enterprise Data Ecosystem (`Seed-and-Amplify` Unified Engine)

Rather than stitching disconnected public CSVs with mismatched foreign keys, the platform owns a vectorized generator (`commerce-datagen`) seeded with real commerce dictionaries (**Google `theLook` + Brazilian `Olist` Marketplace + `H&M` Product Catalog + `Open CDP` Behavioral Funnel**), producing **13 source tables/streams (165 columns)** with 100% referential integrity and feeding **14 curated Gold tables**:

- **3 Ingestion Channels:**
  1. **Transactional OLTP & CDC (`FastAPI` → `PostgreSQL 16 WAL` → `Debezium` → `Kafka`):** Live `INSERT/UPDATE` orders, payments, inventory mutations, and `dlq_poison_events` contract isolation.
  2. **High-Rate Behavioral & Webhook Stream (`Datagen` → `Kafka` → `Apache Flink`):** `clickstream` (injecting **5–10 min mobile late arrivals**) and `payment_callbacks` (injecting **`~1.5%` gateway duplicates**).
  3. **High-Throughput Historical Bulk (`PyArrow/DuckDB` → `MinIO s3://raw` + `Postgres COPY` → `Apache Spark`):** Generates `>= 100 GB` across 9 relational tables (`customers`, `distribution_centers`, `sellers`, `products`, `inventory_items`, `orders`, `order_items`, `payments`, `order_reviews`) with **80% Ho Chi Minh City / 75% Electronics Skew**, mid-dataset **`payments.failure_reason` Schema Evolution**, and **`~2%` `order_items` Offline Duplicates**.
- **14 Target Gold Tables (`dbt` + `Delta/Iceberg UniForm` + `ClickHouse`):**
  - **5 Dimensions (`dim_*`):** `dim_customers` (SCD Type 2 + `SHA-256` PII masking), `dim_products` (SCD Type 2), `dim_sellers`, `dim_distribution_centers`, `dim_date`.
  - **6 Facts (`fact_*`):** `fact_orders`, `fact_order_items`, `fact_payments`, `fact_inventory_movements`, `fact_clickstream_5m`, `fact_order_reviews`.
  - **1 One Big Table (`obt_*`):** `obt_commerce_sales_wide`.
  - **2 Feature & Label Tables:** `feat_customer_order_metrics_90d` (`event_timestamp`, `created_at`) and `label_customer_reorder_7d` (`will_order_in_7d`).
- **2-Phase Execution & Testing Strategy:**
  - **Phase A (Incremental Stage Build & Clean):** Test each newly built stage with lightweight `ci`/`dev` fixtures (`50 MB – 1 GB` in seconds), then wipe temporary volumes (`make clean`) to keep local SSD consumption at `0 GB` during development.
  - **Phase B (Full End-to-End Zero-to-Gold Campaign):** Once all 7 stages are wired together, trigger a clean-slate end-to-end run from `Source` → `CDC/Kafka` → `Flink/Spark` → `Bronze/Silver/Gold` → `WAP Gate` → `ClickHouse` → `Airflow/DataHub/Grafana`, measuring throughput (`MB/s`), latency (`ms`), and wall-clock execution time at every hop.

## 7-Stage Practical Roadmap

| Stage | Focus | Core Stack | DE Rubric Points |
| :--- | :--- | :--- | :--- |
| **Stage 1** | Container Foundation, Storage & Dockerfile Optimization | Docker Compose, PostgreSQL 16, MinIO, Multi-stage Dockerfile, Makefile | **5 pts** |
| **Stage 2** | Commerce Data Generator (`>=100 GB` + Stream) & CDC Ingestion | PyArrow/DuckDB Generator, FastAPI, Debezium CDC, Kafka KRaft, DLQ | **12 pts** |
| **Stage 3** | Real-Time Stream Processing | Apache Flink (Watermarks, Late Arrivals, State Dedup, Windowing) | **10 pts** |
| **Stage 4** | Batch Processing & Delta Lakehouse Optimization | Apache Spark (Skew, Schema Evolution, Dedup) + Delta Lake (`OPTIMIZE`, `Z-ORDER`) | **16 pts** |
| **Stage 5** | Dimensional Modeling, Slim CI & OLAP Serving Warehouse | dbt Core (`staging/intermediate/marts`, SCD Type 2, `state:modified+` Slim CI, PII Masking, `feat_` SQL table) + ClickHouse Indexing | **13 pts** |
| **Stage 6** | Orchestration, Custom Plugins & End-to-End Data Governance | Apache Airflow (DP1, DP2, DP3 + Custom OOP Operators/Hooks) + DataHub (Lineage, PII Classification & Assertions) | **24 pts** |
| **Stage 7** | Enterprise Ops, Migration Reconciliation, IaC & 2 Novel Ideas | WAP Zero-Copy & Legacy DW Reconciliation Gate, Prometheus/Grafana/Loki, Vault, NGINX, K3s GitOps (Helm/ArgoCD) & OCI Terraform/Ansible (`us-phoenix-1`) | **20 pts** |

## Quick Start (Stage 1)

```bash
make setup   # Create .env from .env.example if missing
make up      # Start PostgreSQL 16 + MinIO and auto-initialize lakehouse buckets
make ps      # Verify container health status
make test    # Run persistence and connectivity checks
```
