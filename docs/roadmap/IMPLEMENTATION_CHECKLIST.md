# Implementation Checklist — Enterprise Commerce Data Platform

This is the execution checklist for the **Enterprise Commerce Data Platform** (100% Pure Data Engineering scope, 100/100 DE Rubric points).

## Status Legend
- `[ ]` Not started
- `[~]` In progress
- `[x]` Completed and verified

---

## Stage 0 — Host Cleanup & Workspace Readiness (Completed)
- [x] Validate Apple M1 Max host (`10 cores`, `64 GiB RAM`, OrbStack runtime).
- [x] Reclaim `41 GiB` SSD by removing legacy `~/.colima` and unused caches (`183 GiB` free SSD verified).
- [x] Quarantine duplicate coursework archive (`edai-k11-cw*`) into `quarantine/manifest.json`.
- [ ] Initialize Git repository (`git init -b main`) and create initial commit.

---

## Stage 1 — Container Foundation, Core Storage & Dockerfile Optimization (5 Rubric Points)
- [ ] **1.1 — Local Compose Foundation (`PostgreSQL 16` + `MinIO`):**
  - Create `.env.example`, `Makefile` (`setup`, `up`, `down`, `ps`, `logs`, `test`), and `docker-compose.yml`.
  - Configure named persistent volumes, custom bridge network, resource limits, and strict `healthcheck` gates.
  - Auto-initialize MinIO lakehouse buckets (`raw`, `bronze`, `silver`, `gold`, `checkpoints`).
  - Verify data durability across container restart and recreation.
- [ ] **1.2 — Dockerfile Optimization (`R3–R4`: 5 pts):**
  - Build baseline single-stage `Dockerfile` vs optimized **Multi-stage `Dockerfile`** (`uv`/wheel builder, slim runtime, non-root user).
  - Record before/after `docker images` size comparison (`>60%` size reduction) in `docs/`.

---

## Stage 2 — Commerce Data Generator (`>= 100 GB` + Stream) & CDC Ingestion (12 Rubric Points)
- [ ] **2.1 — Enterprise Commerce OLTP Schema & FastAPI Service:**
  - Create core and extended commerce tables in PostgreSQL (`customers`, `products`, `orders`, `order_items`, `payments`, plus fulfillment/catalog tables) with primary/foreign keys and indexes.
  - Build FastAPI source service with Pydantic validation and health checks.
- [ ] **2.2 — Offline Data Generator (`R5–R8`: 8 pts):**
  - Build vectorized `commerce-datagen` (`PyArrow` / `DuckDB`) with `dev` (`1–2 GB`) and `rubric_100gb` (`>= 100 GB` rolling chunked Parquet) profiles.
  - Inject **Skew** (`80%` `shipping_city = 'Ho Chi Minh City'`, `75%` `category = 'electronics'`), **Schema Evolution** (mid-dataset `payments.failure_reason`), and **Offline Duplicates** (`~2%` `order_items` with shifted `created_ts`).
  - Land generated raw files into MinIO/PostgreSQL ready for Bronze ingestion.
- [ ] **2.3 — Online Streaming Generator, Debezium CDC & Kafka (`R9–R10`: 4 pts):**
  - Deploy Kafka (KRaft) + Debezium PostgreSQL CDC connector + Dead-Letter Queue (`dlq`).
  - Stream `clickstream` (injecting **5–10 min Late Arrivals**) and `payment_callbacks` (injecting **`~1.5%` Duplicates**).

---

## Stage 3 — Real-Time Stream Processing with Apache Flink (10 Rubric Points)
- [ ] **3.1 — Flink Baseline Job (`R16`: 2 pts):** Run unoptimized streaming job and capture Flink UI baseline metrics.
- [ ] **3.2 — Late Arrivals Handling (`R17`: 3 pts):** Implement bounded-out-of-orderness watermarks, allowed lateness, and side outputs for 5–10 min delayed mobile `clickstream`.
- [ ] **3.3 — Streaming Deduplication (`R18`: 3 pts):** Implement idempotent `KeyedProcessFunction` state deduplication with strict `StateTtlConfig` for `1.5%` duplicate `payment_callbacks`.
- [ ] **3.4 — Window Processing (`R19`: 2 pts):** Implement tumbling/sliding event-time windows and sink curated streaming aggregates to MinIO/Delta Lake.

---

## Stage 4 — Batch Processing with Apache Spark & Delta Lakehouse Optimization (16 Rubric Points)
- [ ] **4.1 — Spark Baseline vs Optimized Jobs (`R11–R15`: 13 pts):**
  - Implement Bronze (`raw_`) → Silver (`stg_`) Spark jobs on Delta Lake.
  - Capture Spark UI evidence before and after optimizing **Skew** (AQE / Key Salting), **Schema Evolution** (`failure_reason` merge), and **Offline Deduplication** (`2%` `order_items`).
- [ ] **4.2 — Lakehouse Storage Optimization (`R20`: 3 pts):**
  - Apply partitioning, `ZSTD` compression, small-file compaction (`OPTIMIZE`), and `Z-ORDER BY`.
  - Benchmark file counts, storage footprint, and query read latency before vs after optimization.

---

## Stage 5 — Dimensional Modeling (`dbt`) & OLAP Warehouse (`ClickHouse`) (13 Rubric Points)
- [ ] **5.1 — Gold Dimensional & Feature-Ready Models (`R34–R38`: 10 pts):**
  - Build `dbt` models adhering to naming conventions (`raw_`, `stg_`, `dim_`, `fact_`, `obt_`, `feat_`).
  - Implement **SCD Type 2** dimensions (`dim_customers`, `dim_products` with `valid_from_ts`, `valid_to_ts`, `is_current`).
  - Build SQL feature-ready summary table (`feat_customer_order_metrics_90d` with `event_timestamp` and `created_at`).
  - Export ERD diagram across zones via DBeaver.
- [ ] **5.2 — ClickHouse Warehouse Optimization (`R21`: 3 pts):**
  - Load Gold tables into ClickHouse (`MergeTree` / `ReplacingMergeTree`).
  - Benchmark analytical queries before and after configuring Primary/Sorting Keys, Data Skipping Indexes, and Materialized Views.

---

## Stage 6 — Pipeline Orchestration (`Airflow`) & Data Governance (`DataHub`) (24 Rubric Points)
- [ ] **6.1 — Airflow 3 Core Pipelines (`R22–R27`: 12 pts):**
  - Centralize all connections and variables in Airflow.
  - Implement **DP1** (`Raw → Bronze`), **DP2** (`Bronze → Silver → Gold`), and **DP3** (`Compute Offline Feature Table`), each with explicit **Ingest Stage** and **Validate Stage**.
- [ ] **6.2 — DataHub Lineage & Validation (`R28–R33`: 12 pts):**
  - Integrate DataHub with Airflow, Delta Lake, dbt, and ClickHouse.
  - Capture UI evidence showing end-to-end table lineage and validation assertion results for DP1, DP2, and DP3.

---

## Stage 7 — Enterprise Operations, Testing, Cloud IaC & 2 Novel Ideas (20 Rubric Points)
- [ ] **7.1 — Novel Idea #1 (`R39`: 10 pts):** **Write-Audit-Publish (WAP) Zero-Copy Quality Gate** using DuckDB/PyArrow + Delta Lake Time Travel to block invalid data before ClickHouse publication.
- [ ] **7.2 — Novel Idea #2 (`R40`: 10 pts):** **Sustained Throughput & Time-Compressed Soak Benchmark (`200–500 MB/s` sustained, `0.5–1.0 GB/s` peak)** with automated compaction/TTL maintenance, Prometheus/Grafana/Loki observability, HashiCorp Vault, NGINX Gateway, and Terraform/Ansible cloud deployment.
