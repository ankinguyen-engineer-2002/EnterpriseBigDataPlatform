# MultiCloud-Commerce-Lakehouse: Real-Time & Batch Data Platform — From Local Dev to Oracle Cloud Production & Google Cloud Migration (Implementation Checklist)

This is the 7-stage execution checklist for **`MultiCloud-Commerce-Lakehouse`** (100% Pure Data Engineering scope, 100/100 DE Rubric points).

## Status Legend
- `[ ]` Not started
- `[~]` In progress
- `[x]` Completed and verified

---

## Stage 0 — Host Cleanup, Workspace & Cloud Tenancy Readiness (Completed)
- [x] Validate Apple M1 Max host (`10 cores`, `64 GiB RAM`, OrbStack Ubuntu 24.04 ARM64 runtime).
- [x] Reclaim `41 GiB` SSD by removing legacy `~/.colima` and unused caches (`183 GiB` free SSD verified).
- [x] Quarantine duplicate coursework archive (`edai-k11-cw*`) into `quarantine/manifest.json`.
- [x] Initialize Git repository (`git init -b main`) and push initial commit to GitHub.
- [x] Register & verify **Oracle Cloud Infrastructure (`Tenancy: CloudBigDataPlatform`, `us-phoenix-1`)** (`41 OCPUs / 277 GB RAM` `A1.Flex` in `PHX-AD-1`, `2 × E2.1.Micro` in `PHX-AD-2`, `200 GB` Free Block Storage), configure `oci-cloud` MCP + `oci-cli` + `terraform`, and scaffold `infra/terraform/oci-always-free/` with `ZRAM zstd` & `Smart Auto-Yield Keepalive`.

---

## Stage 1 — Container Foundation, Core Storage & Dockerfile Optimization (5 Rubric Points)
- [x] **1.1 — Local Compose Foundation (`PostgreSQL 16` + `MinIO`):**
  - Create `.env.example`, `Makefile` (`setup`, `up`, `down`, `ps`, `logs`, `test`), and `docker-compose.yml`.
  - Configure named persistent volumes, custom bridge network, resource limits, and strict `healthcheck` gates.
  - Auto-initialize MinIO lakehouse buckets (`raw`, `bronze`, `silver`, `gold`, `checkpoints`).
  - Verify data durability across container restart and recreation.
- [x] **1.2 — Dockerfile Optimization (`R3–R4`: 5 pts):**
  - Build baseline single-stage `Dockerfile` vs optimized **Multi-stage `Dockerfile`** (`python:3.11-slim-bookworm` builder + runtime, non-root user `edpuser` `UID 10001`).
  - Record before/after `docker images` size comparison (`[Verified]`: `edp-app:baseline` `2.32 GB` disk / `637 MB` content $\rightarrow$ `edp-app:optimized` `674 MB` disk / `159 MB` content = **`-71.0%` disk / `-75.0%` content reduction**) in `docs/`.

---

## Stage 2 — Commerce Data Generator (`>= 100 GB` + Stream) & CDC Ingestion (12 Rubric Points)
- [ ] **2.1 — Enterprise Commerce OLTP Schema (9 Tables / 123 Columns) & FastAPI Service:**
  - Create full 9-table OLTP & Marketplace relational schema in PostgreSQL 16 (`customers`, `distribution_centers`, `sellers`, `products`, `inventory_items`, `orders`, `order_items`, `payments`, `order_reviews`) + `legacy_daily_revenue_report` SQL view/table with primary/foreign keys and B-Tree/BRIN indexes.
  - Build FastAPI source & API collector service with Pydantic schema contract validation, pagination, and health checks.
- [ ] **2.2 — Offline Data Generator (`R5–R8`: 8 pts):**
  - Build vectorized `commerce-datagen` (`PyArrow` / `DuckDB`) with `ci` (`50 MB`), `dev` (`1–2 GB`), `rubric_100gb` (`>= 100 GB` rolling chunked Parquet), `stream`, and `stress` profiles seeded from real `theLook` / `H&M` catalog descriptions.
  - Inject **Skew** (`80%` `orders.shipping_city = 'Ho Chi Minh City'`, `75%` `products.category = 'electronics'`), **Schema Evolution** (mid-dataset `payments.failure_reason` 15th column), **Offline Duplicates** (`~2%` `order_items` with shifted `created_ts`), and **SCD Type 2** mutations.
  - Land generated raw files into MinIO/PostgreSQL ready for Bronze ingestion.
- [ ] **2.3 — Online Streaming Generator, Debezium CDC & Kafka (`R9–R10`: 4 pts):**
  - Deploy Kafka (KRaft) + Debezium PostgreSQL CDC connector (`pgoutput` WAL) + Dead-Letter Queue (`dlq_poison_events`).
  - Stream `clickstream` (14 columns, injecting **5–10 min Late Arrivals** & out-of-order events) and `payment_callbacks` (11 columns, injecting **`~1.5%` Duplicates**).

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
- [ ] **4.2 — Lakehouse Storage Optimization & Banking Iceberg Interoperability (`R20`: 3 pts):**
  - Apply partitioning, `ZSTD` compression, small-file compaction (`OPTIMIZE`), and `Z-ORDER BY`.
  - Enable **Delta UniForm (`delta.universalFormat.enabledFormats = 'iceberg'`)** on curated MinIO tables to emit dual `_delta_log/` + `Iceberg metadata/*.json` trees (bridging Spark/Delta coursework requirements with Vietnam Banking `Iceberg + MinIO` standards at zero extra Parquet storage cost).
  - Benchmark file counts, storage footprint, and query read latency before vs after optimization.

---

## Stage 5 — Dimensional Modeling (`dbt`), Slim CI & OLAP Warehouse (`ClickHouse`) (13 Rubric Points)
- [ ] **5.1 — Gold Dimensional (14 Tables), Feature-Ready Models & dbt Slim CI (`R34–R38`: 10 pts):**
  - Build `dbt` models structured into `staging/`, `intermediate/`, and `marts/` adhering to naming conventions (`raw_`, `stg_`, `dim_`, `fact_`, `obt_`, `feat_`, `label_`):
    - **5 Dimensions (`dim_*`):** `dim_customers` (SCD Type 2 + PII hashed), `dim_products` (SCD Type 2), `dim_sellers`, `dim_distribution_centers`, `dim_date`.
    - **6 Facts (`fact_*`):** `fact_orders`, `fact_order_items`, `fact_payments`, `fact_inventory_movements`, `fact_clickstream_5m`, `fact_order_reviews`.
    - **1 One Big Table (`obt_*`):** `obt_commerce_sales_wide`.
    - **2 Feature & Label Tables:** `feat_customer_order_metrics_90d` (`event_timestamp`, `created_at`) and `label_customer_reorder_7d` (`will_order_in_7d`).
  - Implement **SCD Type 2** dimensions (`dim_customers`, `dim_products` with `valid_from_ts`, `valid_to_ts`, `is_current`) via `dbt snapshot` and custom Jinja macros for **PII Masking** (`SHA-256` hashing on `email`, `phone`, `card_last4` for non-privileged roles).
  - Configure **dbt Slim CI** (`dbt build --select state:modified+ --defer --state ./prod-run-artifacts`) in `Makefile` and CI workflow using persisted `manifest.json`.
  - Export ERD diagram across zones via DBeaver.
- [ ] **5.2 — ClickHouse Warehouse Optimization (`R21`: 3 pts):**
  - Load Gold tables into ClickHouse (`MergeTree` / `ReplacingMergeTree`).
  - Benchmark analytical queries before and after configuring Primary/Sorting Keys, Data Skipping Indexes, and Materialized Views.

---

## Stage 6 — Pipeline Orchestration (`Airflow`) & Data Governance (`DataHub`) (24 Rubric Points)
- [ ] **6.1 — Airflow 3 Core Pipelines & Custom OOP Plugins (`R22–R27`: 12 pts):**
  - Centralize all connections and variables in Airflow.
  - Develop production-grade Python custom plugins (`WAPReconciliationOperator` and `ClickHousePublishHook` with `pytest` unit tests and type hints).
  - Implement **DP1** (`Raw → Bronze`), **DP2** (`Bronze → Silver → Gold`), and **DP3** (`Compute Offline Feature Table`), each with explicit **Ingest Stage** and **Validate Stage**.
- [ ] **6.2 — DataHub Lineage, PII Governance & Validation (`R28–R33`: 12 pts):**
  - Integrate DataHub with Airflow, Delta Lake, dbt, and ClickHouse.
  - Register `PII` / `Sensitive` classification tags and access-audit metadata on customer/payment attributes.
  - Capture UI evidence showing end-to-end table lineage and validation assertion results for DP1, DP2, and DP3.

---

## Stage 7 — Enterprise Operations, Migration Reconciliation, Multi-Cloud IaC & 2 Novel Ideas (20 Rubric Points)
- [ ] **7.1 — Novel Idea #1 (`R39`: 10 pts):** **Write-Audit-Publish (WAP) Zero-Copy Quality Gate & Legacy DW Migration Reconciliation** using DuckDB/PyArrow + Delta Lake Time Travel to run parallel reconciliation (`row_count`, `sum_gross_amount`, `distinct_customers`) against a simulated Legacy SQL monolithic report (`legacy_daily_revenue_report`), blocking invalid cutovers and auto-rolling back via `RESTORE TABLE` before ClickHouse publication.
- [ ] **7.2 — Novel Idea #2 (`R40`: 10 pts):** **Sustained Throughput & Time-Compressed Soak Benchmark (`200–500 MB/s` sustained, `0.5–1.0 GB/s` peak) + Multi-Cloud IaC Progression (`From Local Dev to Oracle Cloud Production & Google Cloud Migration`):**
  - Promote the local M1 Max platform to **Oracle Cloud Production (`CloudBigDataPlatform`, `us-phoenix-1`)** using production Infrastructure-as-Code (`infra/terraform/oci-always-free/` & `oci-hyperscale-burst/`, `Ansible` OS/kernel/K3s bootstrap, `Helm` + `ArgoCD` declarative GitOps).
  - Deploy edge `NGINX` Gateway, `HashiCorp Vault`, and `Prometheus/Grafana/Loki` observability across `PHX-AD-1` (`VM.Standard.A1.Flex` ARM64) and `PHX-AD-2` (`2 × VM.Standard.E2.1.Micro`), run the high-rate soak/burst benchmark with automated compaction/TTL maintenance, and keep the Always Free core cluster running permanently at `$0/month`.
  - Execute the **Google Cloud Migration Drill (`Oracle Cloud → Google Cloud`):** Provision an ephemeral Google Cloud target environment via Terraform, replicate curated Gold Lakehouse tables (`dim_`, `fact_`, `obt_`, `feat_`, `Delta/Iceberg UniForm`) from Oracle Cloud to Google Cloud, verify zero discrepancy (`Delta = 0`) via the WAP Reconciliation Gate, and `terraform destroy` the ephemeral Google Cloud test nodes.
