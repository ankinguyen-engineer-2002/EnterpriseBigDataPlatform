# Commerce Data Foundation Platform

**Status:** Accepted project direction  
**Scope:** Data Engineering and analytics platform first; ML and LLM are future consumers  
**Primary domain:** Commerce operations and customer activity  
**Decision owner:** Project owner  
**Change rule:** Update this document when an architectural decision changes. Do not maintain a competing architecture in another file.

## Table of contents

1. [Purpose](#purpose)
2. [Problem and outcomes](#problem-and-outcomes)
3. [Design principles](#design-principles)
4. [Target architecture](#target-architecture)
5. [Source ingestion](#source-ingestion)
6. [Storage and data products](#storage-and-data-products)
7. [Processing and orchestration](#processing-and-orchestration)
8. [Governance, quality, and observability](#governance-quality-and-observability)
9. [Infrastructure, Kubernetes, and Terraform](#infrastructure-kubernetes-and-terraform)
10. [Scale and capacity model](#scale-and-capacity-model)
11. [Production test strategy](#production-test-strategy)
12. [Delivery roadmap](#delivery-roadmap)
13. [Explicit non-goals](#explicit-non-goals)
14. [Future ML and LLM extensions](#future-ml-and-llm-extensions)
15. [Change log](#change-log)

## Purpose

This project is a real engineering exercise for an internal commerce data platform, not a toy benchmark. It should provide reliable, replayable, governed data products for analytics and operations while preserving clean extension points for feature engineering, ML, and LLM applications.

The platform is intentionally designed as a **single-region, single-domain, scale-out-ready reference implementation**. It will use a small local or remote topology while keeping the interfaces, data contracts, deployment boundaries, and operational practices needed for larger environments.

The project will not claim that a Mac or one VPS is processing production-scale terabytes. It will prove scale readiness through capacity models, controlled load profiles, replay tests, and measured bottlenecks.

## Problem and outcomes

The platform addresses a common enterprise problem: business data arrives from transactional databases, APIs, files, and event streams with different delivery guarantees and quality issues. Downstream analytics need consistent, timely, traceable data without repeatedly loading or damaging source systems.

The first product is a trusted commerce data foundation containing:

- Orders, order items, payments, customers, products, inventory, and delivery events.
- Near-real-time operational events such as clickstream and payment callbacks.
- Historical Bronze, Silver, and Gold datasets.
- Business-ready dimensional models and analytical marts.
- Replayable ingestion, idempotent processing, backfill, and data-quality gates.
- Catalog, ownership, lineage, freshness, and operational monitoring.

The project uses a three-source realism strategy. Public datasets and deterministic synthetic generators provide repeatable load and failure experiments. A permitted, anonymized company data extract may be used for batch schema, history, and backfill practice, but it must never leave an approved boundary or enter Git, images, logs, or public storage. A historical database is valuable even when it is not a stream: CDC, change replay, event-time simulation, and source adapters can turn controlled changes into streaming test input without pretending that the original database is an event platform.

## Design principles

1. **Use the smallest useful set of technologies.** One tool owns each responsibility. Do not add a second query engine, table format, quality framework, or deployment system without a measured need.
2. **Separate data, control, and infrastructure planes.** Pipelines move data; Airflow coordinates them; Terraform and Kubernetes provide runtime infrastructure.
3. **Object storage is the durable analytical source.** Kafka is a replay buffer and transport layer, not the long-term archive.
4. **Contracts precede processing.** Every source has a schema, compatibility policy, ownership, and failure path.
5. **Batch and streaming share a data model.** Flink handles event-time workloads; Spark handles batch, compaction, and backfill. They converge in the lakehouse.
6. **Make every operation replayable and idempotent.** Retries and reprocessing must not create incorrect business results.
7. **Measure before claiming scale.** Throughput, lag, freshness, recovery time, storage growth, and cost are recorded for every load profile.
8. **Build for learning without hiding the mechanics.** Local Compose makes process, network, storage, and failure behavior visible before Kubernetes abstracts them.
9. **Keep future consumers decoupled.** ML and LLM systems consume governed data products; they do not redefine the source pipeline.

## Target architecture

```mermaid
flowchart LR
    subgraph Sources
        OLTP[(PostgreSQL / MySQL)]
        API[REST and SaaS APIs]
        FILES[SFTP and object files]
        EVENTS[Web, mobile, and payment events]
    end

    subgraph Ingestion[Ingestion platform]
        ADAPTER[Source adapters]
        CDC[Debezium CDC]
        CONTRACT[Schema and data contracts]
        KAFKA[(Kafka)]
        DLQ[(Dead-letter queues)]
        RAW[Immutable raw landing]
    end

    subgraph Processing[Processing plane]
        FLINK[Flink streaming]
        SPARK[Spark batch and backfill]
    end

    subgraph Lakehouse[Lakehouse]
        BRONZE[(Bronze)]
        SILVER[(Silver)]
        GOLD[(Gold)]
    end

    subgraph Serving[Analytics serving]
        DBT[dbt models and tests]
        OLAP[(ClickHouse or warehouse)]
        BI[BI and analytical consumers]
        DATAAPI[Data access API]
    end

    subgraph Control[Control plane]
        AIRFLOW[Airflow]
        DATAHUB[DataHub]
        QUALITY[Quality gates]
        OBS[Prometheus, Grafana, and OpenTelemetry]
    end

    OLTP --> CDC
    API --> ADAPTER
    FILES --> ADAPTER
    EVENTS --> ADAPTER
    CDC --> CONTRACT
    ADAPTER --> CONTRACT
    CONTRACT --> KAFKA
    CONTRACT --> RAW
    CONTRACT --> DLQ
    KAFKA --> FLINK --> BRONZE
    RAW --> SPARK
    BRONZE --> SPARK --> SILVER
    SILVER --> DBT --> GOLD
    GOLD --> OLAP
    OLAP --> BI
    OLAP --> DATAAPI
    AIRFLOW -. submits and validates .-> SPARK
    AIRFLOW -. manages .-> FLINK
    AIRFLOW -. runs .-> QUALITY
    DBT -. publishes lineage .-> DATAHUB
    KAFKA -. emits metrics .-> OBS
    SPARK -. emits metrics .-> OBS
    FLINK -. emits metrics .-> OBS
```

The main path uses one lakehouse table format. The coursework-aligned first implementation uses Delta Lake; the table access layer remains isolated so a future Iceberg or Hudi migration is possible without rewriting source adapters and business contracts. The main path must not run multiple table formats simultaneously.

### Storage, table format, and query engine are different layers

These technologies are complementary rather than interchangeable:

| Layer | Examples | Responsibility |
|---|---|---|
| Object storage | MinIO, Amazon S3, Google Cloud Storage | Stores objects and files such as JSON, Parquet, checkpoints, and metadata |
| Table format | Delta Lake, Apache Iceberg, Apache Hudi, Apache Paimon | Adds table metadata, transaction logs, schema evolution, snapshots, merges, and time travel on object storage |
| Compute engine | Apache Spark, Apache Flink | Performs batch, streaming, transformation, aggregation, and maintenance work |
| Query engine | Trino, Spark SQL, DuckDB, ClickHouse | Executes SQL or analytical queries against tables and other sources |
| Catalog and governance | DataHub, Hive Metastore, Nessie, Glue Catalog | Registers tables, schemas, locations, ownership, and lineage |

Trino is therefore not a lakehouse format. It is a distributed SQL query engine that can query lakehouse tables through connectors. Depending on connector and version, Trino can query Delta Lake, Iceberg, Hudi, Parquet, relational databases, and other systems. A Trino catalog points to a table format and its catalog or storage location.

Delta Lake, Iceberg, and Hudi occupy the same architectural layer and are alternatives for the main table path:

- **Delta Lake:** strong Spark integration and a good first choice for this Spark-first coursework project.
- **Apache Iceberg:** strong multi-engine and multi-cloud interoperability, partition evolution, and broad support across Spark, Flink, Trino, and cloud platforms.
- **Apache Hudi:** strong incremental processing, upserts, and CDC-oriented workloads; it was created at Uber and is used in public Grab data-platform designs.
- **Apache Paimon:** another streaming-oriented table format that can be considered later, but it is outside the first project path.

The project will use Delta Lake in the first implementation to align with the course. Iceberg and Hudi belong in isolated comparison experiments or a later migration ADR, never as simultaneous table formats in the same production path.

All three formats can use the same object-storage backends. For example, separate table prefixes in one MinIO bucket could look like:

```text
s3://lake/commerce_delta/orders/
├── data/*.parquet
└── _delta_log/*

s3://lake/commerce_iceberg/orders/
├── data/*.parquet
└── metadata/*.json

s3://lake/commerce_hudi/orders/
├── *.parquet
└── .hoodie/*
```

The storage backend is still MinIO. The format-specific files provide table semantics. A catalog such as a filesystem/path catalog, Hive Metastore, REST catalog, Nessie, or a cloud catalog registers the table for engines such as Spark, Flink, and Trino. These are separate choices: storage backend, table format, catalog, and compute/query engine should not be conflated.

### What MinIO provides and does not provide

MinIO provides the object-storage layer: S3-compatible APIs, buckets, object reads and writes, policies, versioning, lifecycle controls, encryption options, notifications, and distributed storage features when deployed with the required nodes and disks. Its S3-compatible interface is useful because the same application and table paths can later move to Amazon S3 or Google Cloud Storage with limited changes.

MinIO does not provide table transactions, SQL, Spark or Flink processing, orchestration, data contracts, data quality, catalog lineage, or business semantics. Those capabilities come from Delta Lake, Iceberg or Hudi; Spark and Flink; Airflow; contracts and tests; DataHub; and serving engines such as ClickHouse or Trino.

A single MinIO container on a laptop is a development object store. A production object store requires an appropriate distributed deployment, disks, network, replication or erasure coding, backup, lifecycle policy, monitoring, and recovery tests. MinIO is a strong storage foundation, not the entire Data Platform.

### Trino and ClickHouse

Trino and ClickHouse sit near the same analytics-serving layer, but they solve different problems:

| System | Type | Best fit |
|---|---|---|
| Trino | Distributed federated SQL query engine | Querying Delta/Iceberg/Hudi, object storage, and multiple databases without copying every dataset into one store |
| ClickHouse | Columnar analytical database | Low-latency, high-concurrency analytics on curated data that has been loaded or materialized into ClickHouse |

Trino is primarily a compute and query layer; it does not own the canonical data. ClickHouse is a serving database with its own storage, ingestion, indexes, partitions, and materialized views. They can coexist, with Trino used for lakehouse exploration and federation and ClickHouse used for hot business dashboards or APIs.

The lean first path uses one primary serving engine. ClickHouse is preferred when the project needs a fast analytical serving surface. Trino is added later when federated SQL or direct lakehouse querying is a demonstrated requirement. The project must not deploy both merely because both are available.

## Source ingestion

### Source realism and controlled data use

The platform must practice three different properties rather than search for one magical dataset:

| Property | Suitable source | What it proves |
|---|---|---|
| Real business shape | Authorized company snapshot, theLook-shaped Postgres, or Olist | Messy schemas, keys, nulls, corrections, SCD, payments, delivery, and domain rules |
| Reproducible scale | Seeded synthetic generator and deterministic Parquet/event writer | Exact volume, skew, event size, burst rate, late data, poison records, and repeatable benchmarks |
| Streaming behavior | PostgreSQL CDC, outbox/change-log replay, API polling simulator, and event producer | Ordering, offsets, partitions, retries, consumer lag, replay, checkpointing, and event-time behavior |

The company database may contain valuable 30-year history for batch profiling, schema evolution, SCD/backfill, reconciliation, and performance tests. It does not become a stream merely because it is large. Use only data that the owner has explicitly authorized, remove or tokenize personal and confidential fields, keep it in an isolated local environment, document retention, and never upload it to a public repository or unapproved cloud. Create a sanitized snapshot and a reproducible change-log/replay harness instead of connecting the platform directly to a production database.

The project will maintain a small public or synthetic reference dataset for CI and a separate local-only dataset profile for larger experiments. A replayable change log generated from the historical snapshot will exercise Kafka and Flink; the original database remains an OLTP/batch source.

### How large volume is practiced without storing petabytes

Company history and public datasets prove realism of shape. They rarely prove daily ingest rate. Thirty years of records in an OLTP database is accumulated state, not a PB/day stream. A 10 TB historical snapshot averaged over 30 years is roughly 1 GB/day of historical accumulation. PB/day is an ingress rate of about 11.6 GB/s, which is a different problem.

The project therefore practices volume in three complementary ways:

1. **Shape from real data.** An authorized, anonymized company snapshot, a theLook-shaped Postgres seed, or Olist supplies schemas, keys, nulls, corrections, SCD cases, and business rules. NYC Taxi is not a shape source for Gold; it is an isolated file fixture.
2. **Rate from a generator.** A seeded producer emits events that match those contracts at a controlled events/sec and bytes/sec. The generator can clone, mutate, skew, delay, duplicate, and poison records without requiring a source that already produces PB/day.
3. **Scale from measurement plus a model.** The lab measures the maximum sustainable rate of one partition, one broker, one Spark/Flink worker, and one object-store prefix. The architecture model multiplies those measured units to 10 TB/day, 100 TB/day, and PB/day.

A PB/day campaign must not try to persist 1 PB locally. Use these lab methods instead:

| Method | What is stored | What is proven |
|---|---|---|
| Stats-only load | Counters, lag, CPU, disk, network | Maximum ingest or processing rate |
| Short burst | Minutes of data, then delete | Peak multiplier and backpressure |
| Time compression | One day of history replayed in one hour | Event-time, watermarks, late data, and window correctness under accelerated clocks |
| Partition saturation | Data for one or a few partitions | Per-partition throughput, hot keys, and consumer lag |
| Schema-faithful fan-out | Sampled real records cloned with new IDs | Volume using real field distributions |
| Component isolation | One layer at a time | Kafka, object storage, Spark, Flink, and serving bottlenecks separately |
| Full path at small/medium | End-to-end Bronze to Gold | Correctness, idempotency, quality gates, and recovery |

The generator must be contract-driven. It reads the same event schema used by CDC and API adapters and can mix:

- Replayed company-derived change logs.
- Public dataset rows mapped to commerce events.
- Synthetic records with controlled size, skew, late arrival, duplicates, and invalid payloads.

Every scale report must state whether the run persisted data, sampled data, or discarded payloads after measurement. A PB/day number without event size, peak multiplier, replication, partition count, and the measured unit it was calibrated from is not an architecture claim.

### Data source catalog

The project uses one commerce domain and several source *types*. It does not mix taxi, reviews, ads, and retail into one canonical Gold model. Public datasets are adapters into that domain or isolated scale fixtures. Evidence, sizes, licenses, and rejected corpora are recorded in [DATA_SOURCE_AND_SIMULATION.md](../research/DATA_SOURCE_AND_SIMULATION.md).

Shape sources are an authorized company snapshot, theLook-shaped Postgres, and Olist. NYC Taxi is an isolated file fixture, not a shape source for Gold.

#### Canonical OLTP and CDC source

The primary source system is a PostgreSQL commerce schema aligned with theLook eCommerce: `users`, `products`, `orders`, `order_items`, `inventory_items`, `events`, `distribution_centers`, plus payments if required by the product. theLook is a Looker/Google synthetic clothing store published as `bigquery-public-data.thelook_ecommerce`. Typical analyses sit around 100k users/orders; public extracts are on the order of 170 MB. That is enough for schema practice and far too small as a lakehouse volume source.

The operational pattern is:

```text
Seed snapshot
  → PostgreSQL
  → owned contract-driven generator writes inserts/updates/deletes
  → Debezium reads WAL
  → Kafka topics per table
```

This is the main source path. Factor House's theLook eComm CDC/datagen is a pattern and reference for live OLTP activity (purchases, updates, cancellations, returns, ghost sessions, configurable QPS). It is not a runtime dependency. Own the generator in this repository.

An authorized company snapshot later maps onto the same contracts: different rows, same tables, same ingestion rules.

#### Additional source types in the same domain

| Type | Concrete source | Role |
|---|---|---|
| OLTP CDC | PostgreSQL + theLook-shaped in-repo generator | Orders, customers, products, inventory, payments |
| Clickstream/events | `events` table plus a web/mobile producer | Page view, cart, purchase callbacks |
| REST/SaaS API | FastAPI order/catalog service in front of PostgreSQL | Pagination, rate limits, tokens, partial responses |
| Files | Daily CSV/Parquet dumps and SFTP-style arrivals | Manifests, checksums, late/corrupt/duplicate files |
| Messy commerce adapter | Olist (99,441 orders, 9 CSVs, ~45 MB compressed / 120–130 MB raw) | Multi-item orders, split sellers, installments, delivery SLA, reviews, geo, SCD/backfill. Not a second Gold domain |
| Optional company snapshot | Authorized anonymized extract only | Real nulls, corrections, long history, SCD/backfill |

Company data is an optional adapter behind the same contracts. It is never the only source, never committed to Git, and never connected live to production. History is state; a changelog replay is the stream.

#### Public bulk fixtures for lake and serving scale

These datasets are too useful to ignore, but they are *fixtures*, not a second business domain and not Gold:

| Dataset | Typical size | Best isolated use |
|---|---|---|
| Open CDP / REES46 store events | ~206k users, ~412M events, ~52 GB uncompressed; Kaggle ~4.6 GB compressed | Clickstream volume, sessionization, late events |
| NYC TLC trip Parquet | Monthly vendor files with schema drift. Official 2024 yellow: 41,169,638 trips | Spark/Delta file ingestion, partitioning, compaction. Taxi, not commerce |
| Amazon Reviews 2023 | 571.54M reviews, 54.51M users, 48.19M items; hundreds of GB on disk | Nested documents, schema evolution, file/API-style landing |
| GA4 obfuscated merchandise events | Small BigQuery public sample | Event schema practice, not volume |
| TPC-DS via `dsdgen` | SF1 ≈ 1 GB, SF100 ≈ 100 GB, SF1000 ≈ 1 TB, SF10000 ≈ 10 TB | ClickHouse/dbt warehouse performance, skew, star/snowflake queries |
| ClickHouse serving labs | ClickBench ~100M hits; GitHub events 3.1B / 75 GB download | Serving-engine stress only |

NYC Taxi remains the best public *file* corpus because monthly Parquet files actually drift. TPC-DS remains the best *generated warehouse* corpus because scale factor is a knob. Open CDP is the best public commerce-like clickstream volume. None of them replace the theLook-shaped OLTP/CDC path.

#### Simulation stack

Do not make the platform depend on a random GitHub generator. Own a small in-repo generator with the same contracts as production adapters. External tools are references and optional load harnesses:

| Tool | Use |
|---|---|
| In-repo commerce generator | Canonical. Seeded, contract-driven, writes PostgreSQL and/or Kafka, can clone/skew/delay/duplicate/poison |
| Factor House theLook simulator | Pattern/reference only for live OLTP activity that Debezium can capture. Not a runtime dependency |
| Materialize `datagen` | Schema-driven fake data with joinable keys |
| AWS Flink DataGen example | High-rate parallel generation; published ~64k records/sec per small cluster |
| `kafka-data-gen` / Khaos-style harness | Events/sec, message size, hot partitions, consumer lag, broker failure |
| `dsdgen` | Warehouse-scale analytical tables |

The generator has profiles:

```text
ci             small, deterministic, checked into tests (~50–100 MB)
dev            interactive daily coding profile (~1–2 GB, <10s runtime, <8 GB total disk across zones)
rubric_100gb   >=100 GB chunked Parquet generation (250–500 MB chunks, <6 GiB RAM) with rolling stage cleanup
stream         CDC or direct Kafka at a target events/sec with late arrivals, duplicates, and drift
stress         stats-only or short-burst (--max-seconds 10 --max-gb 2 hard caps); 0 GB permanent disk footprint
```

#### Unified Single-Source Engine (Seed-and-Amplify across DE, ML, and LLM/RAG)

Rather than stitching disconnected datasets across modules, the canonical implementation uses **one unified in-repo data engine (`commerce-datagen`)** that satisfies all three coursework layers (Data Platform, ML Platform, and LLM/Agent Platform) and scores the full **18/100 Rubric points** dedicated to `Implement Data Generator` (12 pts) and `Improve the Data Generator` (6 pts):

1. **Real Catalog & Policy Seed (`~10–50 MB`):** Seeds `products` from real e-commerce catalog descriptions (`theLook` / `H&M articles.csv` `detail_desc`) paired with structured Markdown `FAQ & Shipping/Return Policy` documents. This ensures `product_id`, `customer_id`, and `order_id` link seamlessly across OLTP tables, Feast offline/online features, and the Module 2 RAG vector store without generating meaningless random strings.
2. **Vectorized Bulk Amplification (`PyArrow` / `DuckDB` / `PySpark`):** Generates `customers`, `products`, `orders`, `order_items`, `payments`, and daily `label` snapshots (`will_order_in_7d`) in `250–500 MB` streaming chunks so the Apple M1 Max (`10 cores, 64 GiB RAM, 214 GiB available SSD`) writes `>= 100 GB` in 3–6 minutes using `< 6 GiB RAM`.
3. **Config-Driven Rubric Problem Injection (`generator_config.yaml`):**
   - **Data Skew (Offline):** `80%` of `orders.shipping_city = 'Ho Chi Minh City'`; `75%` of `products.category = 'electronics'`.
   - **Schema Evolution (Offline):** Mid-dataset offset adds nullable string column `failure_reason` to `payments`.
   - **Offline Duplicates:** `~2%` of `order_items` duplicated with shifted `created_ts`.
   - **SCD Type 2 History:** Controlled price/status/segment mutations producing `valid_from_ts`, `valid_to_ts`, and `is_current`.
   - **Streaming Late Arrivals & Duplicates:** `clickstream` mobile events delayed 5–10 minutes; `~1.5%` of `payment_callbacks` duplicated; configurable **Data Drift** distribution shifts for ML retrain triggers.
4. **Rolling Stage Cleanup (Protecting the `214 GiB` Local SSD):** During the `rubric_100gb` campaign on the Mac, the pipeline executes stage-by-stage: generate `100 GB` Raw → run Spark DP1 (Raw → Bronze with `ZSTD` dictionary/RLE compression reducing high-skew data to `~25–35 GB`) → capture Spark UI/DataHub evidence and purge temporary `100 GB` Raw before running DP2/DP3 (Bronze → Silver → Gold → Feast). Peak local disk usage never exceeds `~130 GiB`.



### Transactional databases

PostgreSQL and MySQL sources use Debezium CDC from WAL or binlog. Initial snapshots are checkpointed. Change events retain source keys, operation type, source timestamp, transaction metadata, schema version, and ingestion timestamp.

The source database remains the operational system of record. The lakehouse receives an immutable analytical copy and does not run heavy analytical queries against production OLTP systems.

### REST and SaaS APIs

Downstream jobs must not call external APIs directly. Each API has a connector that owns:

- Authentication and token refresh.
- Cursor or checkpoint persistence.
- Incremental extraction using `updated_since`, cursors, webhooks, or vendor change feeds.
- Pagination, rate limiting, timeout, retry, and backoff.
- Raw response landing with request metadata and checksums.
- Schema validation and version mapping.
- Idempotent record keys and ingestion run IDs.
- Quarantine and replay for malformed responses.

Raw API responses are retained before normalization. This protects the source from uncontrolled fan-out, makes extraction replayable, and preserves evidence when a vendor changes its response schema.

### Files and object sources

File ingestion uses manifests, checksums, arrival timestamps, source identifiers, schema validation, and duplicate-file detection. Partial, corrupt, or incompatible files are quarantined instead of silently entering Silver data.

### Events

Web, mobile, and payment events enter through a collector or producer and then Kafka. Event contracts define required fields, event ID, event time, producer version, and compatibility rules. Invalid events are routed to a dead-letter queue with enough metadata for diagnosis and replay.

## Storage and data products

The storage design is append-first and replayable:

- **Raw landing:** immutable source payloads, CDC envelopes, API responses, and file manifests.
- **Bronze:** source-shaped tables with ingestion metadata.
- **Silver:** typed, deduplicated, conformed entities and events.
- **Gold:** business models and data products.

Gold naming follows the coursework convention:

- `dim_` for dimensions.
- `fact_` for events and transactions.
- `obt_` for intentionally denormalized analytical tables.
- `feat_` for point-in-time feature-ready tables.
- `raw_` and `stg_` for lower zones.

Dimensions support SCD Type 2 where history matters. Fact tables document grain and keys. Feature-ready tables include `event_timestamp` and `created_at` even before ML is implemented.

MinIO and Delta Lake have different responsibilities:

- **MinIO** is the S3-compatible object storage layer. It stores objects such as JSON, Parquet files, checkpoints, manifests, and Delta transaction-log files.
- **Delta Lake** is the table format and transaction layer on top of that storage. It provides table schemas, transaction logs, controlled schema evolution, ACID-style commits, time travel, and reliable merges.
- **Spark/Flink** are compute engines that read and write the stored data.
- **DataHub or a catalog** provides discovery, ownership, and lineage.

For example, a MinIO bucket may contain `s3://lake/bronze/orders/` with Parquet files and a `_delta_log/` directory. The bucket is not itself a Delta table; Delta gives the files table semantics.

Object storage uses partitioning, compaction, lifecycle policies, versioning, and retention. Partition keys must reflect query and update patterns; partitions must not be created for every high-cardinality identifier.

ClickHouse or a cloud warehouse is a serving projection of Gold data. It is not the canonical historical store.

## Processing and orchestration

### Flink

Flink handles streaming concerns that require event time and state:

- Watermarks and bounded out-of-orderness.
- Windowed aggregations.
- Deduplication with state TTL.
- Late-event side outputs.
- Checkpoints and recovery.
- Backpressure and consumer-lag monitoring.

Airflow deploys, monitors, or validates the long-running Flink job. It does not schedule each event.

### Spark

Spark handles batch and recovery-oriented work:

- Bronze-to-Silver transformations.
- Schema evolution and controlled merges.
- Skew handling and partition tuning.
- Deduplication and reconciliation.
- Compaction and table maintenance.
- Backfill and historical reprocessing.

Business models are owned by dbt where SQL models provide better reviewability and lineage. Spark remains responsible for large-scale physical processing and maintenance.

### Airflow

The first pipelines are:

1. Source or raw landing to Bronze.
2. Bronze to Silver and Gold.
3. Offline feature-ready table computation.

Connections, variables, retries, schedules, validation, backfill parameters, and alert routing live in Airflow configuration rather than being hard-coded into jobs.

## Governance, quality, and observability

### Contracts and quality

Quality is split into two gates:

- **Technical gate:** schema, required fields, types, uniqueness, arrival time, file integrity, and allowed evolution.
- **Business gate:** referential integrity, order totals, payment reconciliation, non-negative quantities, currency consistency, freshness, and anomaly thresholds.

Contracts, dbt tests, SQL checks, and focused Python checks are the initial implementation. A larger validation framework is added only when the rules cannot be maintained clearly with these primitives.

Failed quality gates prevent invalid Gold data from replacing the last known-good output. Failed records remain available for diagnosis and replay.

### Catalog and lineage

DataHub records datasets, owners, descriptions, classifications, freshness, validation results, and lineage from source to Airflow, dbt models, and serving tables. Business definitions live alongside model schemas so the catalog remains versioned with code.

### Observability

The first dashboard tracks:

- Source arrival and freshness.
- Kafka consumer lag and partition health.
- Flink checkpoint duration, state size, and backpressure.
- Spark duration, shuffle, spills, and failed tasks.
- File counts, table size, small-file growth, and compaction.
- Quality pass/fail counts.
- Airflow task duration and failure rate.
- Serving query latency and error rate.

Logs and traces are added with OpenTelemetry as the platform grows. A dashboard is not a substitute for the native Spark, Flink, Airflow, or DataHub interfaces; it is the operational summary.

## Infrastructure, Kubernetes, and Terraform

```text
Terraform
  ├── Network, IAM, firewall, DNS
  ├── Object storage and container registry
  ├── VM or managed Kubernetes cluster
  └── Node pools, storage classes, and cloud policies

Ansible
  └── Ubuntu and self-managed K3s bootstrap

Helm and operators
  ├── Kafka/Strimzi
  ├── Flink operator
  ├── Spark jobs or operator
  ├── Airflow
  └── Monitoring and ingress

CI/CD
  └── Test, build, scan, publish, and deploy images

Airflow
  └── Execute data pipelines and maintenance jobs
```

Terraform owns infrastructure and persistent platform boundaries. Helm or operators own Kubernetes applications. CI/CD owns application release. Airflow owns data execution. Terraform must not manage individual task runs or data rows.

The deployment progression is:

1. Docker Compose profiles on the Mac for fast development.
2. Ubuntu VPS with Docker Compose or single-node K3s.
3. Multi-node K3s for Kubernetes and operator learning.
4. GKE or EKS provisioned by Terraform.
5. Cloud object storage, managed Kafka, and managed or multi-zone compute when the capacity model requires them.

A single-node K3s deployment is production-shaped, not high availability. High availability requires multiple nodes, failure domains, replicated storage, backups, and tested recovery.

## Scale and capacity model

The platform scales horizontally through Kafka partitions, connector workers, Flink parallelism, Spark executors, Kubernetes nodes, and serving replicas. Storage grows independently of compute through object storage.

The project uses logical scale profiles:

| Profile | Purpose |
|---|---|
| Small | Fast development and debugging |
| Medium | Coursework-scale data and optimization evidence |
| Large | Logical 1 TB replay and capacity modeling |
| Stress | Logical 5–10 TB/day workload model without storing all data locally |

Every profile records throughput, peak resource usage, lag, freshness, shuffle, storage growth, recovery time, and estimated cost. A scale claim is valid only when the bottleneck and scaling action are documented.

At several TB/day, the expected migration is from local MinIO/Kafka and self-managed compute to distributed cloud object storage, multi-broker or managed Kafka, and elastic Kubernetes or cloud data processing. The source contracts, table layout, and pipeline interfaces remain stable.

### Scale claims and hyperscale modeling

The physical local implementation targets correctness and measured scale up to the limits of the Mac and the temporary cloud lab. It does not claim that a laptop processes production petabytes. The project records separate tiers:

| Tier | Meaning | Evidence required |
|---|---|---|
| Local measured | Data that actually runs on the Mac | Throughput, lag, freshness, resource usage, storage growth, and recovery measurements |
| Cloud measured | Data that actually runs on one or more temporary cloud VMs | Node count, partitioning, bottleneck, cost, failure and recovery measurements |
| Logical scale model | A capacity design for 10 TB/day, 100 TB/day, or PB/day | Explicit event size, peak multiplier, partition/broker count, storage/egress model, and scaling assumptions |

For reference, 1 PB/day is about 11.6 GB/s of average raw ingress before replication, indexing, compaction, and peak traffic. A replication factor of three makes the transport/storage write budget roughly 35 GB/s before overhead. That is a hyperscale design exercise involving many brokers, independent storage, multiple failure domains, and serious network and cost planning; it is not a credible personal-laptop acceptance criterion. The project may include a PB/day architecture and capacity model as a future design review, while the executable implementation proves smaller profiles and the horizontal scaling mechanisms that lead toward them.

### Architecture evidence pack

A scale claim is not accepted because a diagram contains a large number. Each important architecture claim must have an evidence pack that another engineer can review and reproduce:

1. **Context and requirements:** source types, data products, consumers, freshness, throughput, retention, security, SLO, RPO, and RTO.
2. **Logical and physical architecture:** trust boundaries, data/control/infrastructure planes, deployment topology, failure domains, ownership, and data lineage.
3. **Data architecture:** source contracts, keys, event-time semantics, schema compatibility, raw/Bronze/Silver/Gold rules, partitioning, retention, quality gates, and PII classification.
4. **Capacity model:** event size distribution, average and peak events/sec, bytes/day, replication factor, partition/broker count, worker parallelism, storage growth, network budget, and cost assumptions.
5. **Failure model:** dependency failure matrix, blast radius, retry/DLQ behavior, checkpoint/replay path, backup/restore path, and degraded-mode behavior.
6. **Verification evidence:** load-test results, lag/freshness graphs, resource profiles, recovery-time measurements, schema-evolution tests, and reconciliation results.
7. **Decision record:** alternatives considered, why a tool was chosen, known limits, migration trigger, and the next scaling action when a bottleneck appears.

For a PB/day design that cannot be run locally, the evidence is a transparent capacity and failure model calibrated against smaller measured runs. The model must show how a measured 100 GB/day or 1 TB/day run maps to additional partitions, brokers, workers, storage, network, and failure domains. This proves architecture reasoning without making a false claim that the personal lab physically processed PB/day.

## Production test strategy

The coursework provides deliberate cases for skew, schema evolution, duplicates, late arrivals, windows, and storage optimization. The platform extends them with:

- API pagination, rate limits, token expiry, and partial responses.
- Invalid and breaking schema changes.
- Poison messages and DLQ replay.
- Duplicate after acknowledgement loss.
- Consumer restart and checkpoint restore.
- Backfill repeated for the same time range.
- Partial writes and sink retry.
- Corrupt or duplicate files.
- Source outage and object-store outage.
- Kafka lag, hot partitions, and burst traffic.
- Business reconciliation failures.
- Freshness and volume anomalies.
- Disk, memory, and resource-limit failures.
- Deployment rollback and configuration drift.
- Backup and restore.
- PII leakage and secret exposure in logs.

Each source and pipeline must have unit, contract, integration, and end-to-end tests. Failure tests must prove that the system fails visibly, preserves replayability, and avoids publishing incorrect Gold data.

## Delivery roadmap

### Phase 1 — Linux, infrastructure, and engineering foundation

Linux, Python, SQL, PostgreSQL, FastAPI, Makefile, Docker, repository design, unit tests, and documentation.

#### Infrastructure-first execution model

The project is built from infrastructure to data products. A phase is complete only when its services have configuration, health checks, metrics, backups where state is involved, a recovery procedure, and a tested deployment path. A small vertical slice is used as a production gate for the next layer; it is not the final demo or a substitute for the complete platform.

The environments are deliberately separated:

- **Local development:** macOS with OrbStack as the primary container and Linux runtime, used for fast iteration on Apple Silicon. Colima remains a valid fallback and reproducibility option, but both runtimes must not run the same stack at the same time.
- **Linux learning environment:** a separate Ubuntu ARM64 VM or remote Ubuntu host, used when the task is to learn the operating system itself: SSH, systemd, users, disks, mounts, firewall, kernel settings, Ansible, and K3s. OrbStack's managed Linux VM is convenient for containers but is not treated as the complete Ubuntu operations lab.
- **Remote staging:** Ubuntu VPS or cloud VM/K3s, used to exercise real Linux networking, storage, deployment, and recovery.
- **Scale environment:** a Terraform-managed multi-node or managed Kubernetes environment created only for capacity tests and later cloud migration.

OrbStack may be used throughout local development, integration testing, failure testing, and release rehearsal. It is not the production target. The deployable unit is the container image, configuration, schema, migration, Helm/Compose manifest, and operational runbook; those artifacts must run on a standard Ubuntu host, K3s, or managed Kubernetes without depending on OrbStack-specific behavior. Production requires Linux hosts, durable and replicated storage, independent failure domains, backups, monitoring, controlled networking, access management, and a tested recovery process. A laptop runtime cannot provide those guarantees.

#### Environment progression and cost strategy

The project uses a staged environment model. The stages are compatible deployment targets, not four systems that must run continuously at the same time.

```text
Local development
  macOS + OrbStack + Docker Compose
        ↓
Remote Linux lab
  Ubuntu VPS + K3s (single node, self-managed, not HA)
        ↓
Advanced infrastructure lab
  Three cloud VMs + K3s HA (self-managed multi-node cluster)
        ↓
Cloud production evolution
  GKE or EKS (managed Kubernetes control plane)
```

The local environment is the daily development and data-volume environment. It runs the complete functional path with small-to-large synthetic profiles, including ingestion, CDC, Kafka, raw landing, Delta Lake, processing, dbt, serving, quality, replay, and failure tests. The Mac's 64 GB RAM and an external NVMe disk are used for large local data campaigns; cloud storage is not rented merely to generate bulk test data.

The remote lab is introduced after the local foundation is reliable. A small Ubuntu VPS is used to learn and verify Terraform, SSH, Ansible, firewall rules, persistent disks, container deployment, K3s, CI/CD, backup, restore, rollback, and remote operations. A single VPS with K3s is production-shaped staging, not high availability: losing that VPS loses the cluster unless recovery has been tested.

The three-VM K3s environment is an advanced, temporary lab. It is created with Terraform only for campaigns that require node failure, quorum, scheduling, rolling deployment, or multi-node storage experiments, then destroyed after measurements are captured. Three K3s server/control-plane nodes are required for an embedded-etcd HA control plane; replicas on one VM do not provide HA.

Google Cloud Compute Engine with self-managed Ubuntu and K3s/Kubernetes is the primary cloud-evolution target for this project. This keeps the operating model visible: Terraform creates VMs and networks, Ansible configures Ubuntu, and the project operates the cluster and stateful services. Multiple cloud VMs provide a more scalable failure domain than a single VPS while preserving the mechanics that a managed service would hide.

Managed services such as GKE, Cloud SQL, managed Kafka, GCS, BigQuery, Dataproc, and Dataflow are optional comparison or migration experiments, not dependencies of the main implementation. They may be introduced later when a measured reliability, scale, or operating-cost requirement justifies them. AWS EC2/EKS/S3 and Azure VM/AKS/ADLS remain valid comparison targets when a course or job requires them; they are not deployed in parallel without a concrete reason.

The cost policy is local-first, campaign-based, and strictly `$0` out-of-pocket:

1. **Local-first on Apple M1 Max (`64 GiB RAM`, `214 GiB` available SSD):** Keep OrbStack and Docker Compose as the primary daily environment (`dev` profile `1–2 GB`), using rolling stage cleanup for the `rubric_100gb` run and `retention.ms=10000` + `lz4` compression for local high-rate burst tests (`0.6–1.2 GB/s` equivalent to `50–100 TB/day`).
2. **Primary Cloud Target — GCP Free Trial (`$300` Credit / 90 days) with `$0` Out-of-Pocket Guardrails:**
   - Register with a dedicated personal Gmail and a standard Vietnam Visa/Mastercard Debit card (keep `~50,000 VND` only for the initial `$1` verification hold, then zero out or lock international online limit).
   - Remain in the **Free Trial** tier (do not upgrade to Pay-As-You-Go unless extra vCPU quota is explicitly required) so automatic card charging is impossible.
   - Skip Oracle Cloud Free Tier (`VM.Standard.A1.Flex`) due to chronic regional `Out of Host Capacity` errors, 30-day credit expiration, and 4-OCPU/24-GB caps that are weaker than the local M1 Max.
3. **Strict Corporate Cloud Isolation:** Never provision VMs, networks, or coursework resources on the corporate Azure subscription (`NAric@ashleyfurniture.com` / `DATAWAREHOUSE PROD` `8e606065-9e63-4319-ba39-0ff434c72b39`, `CostCenter 5720`), which has `0` ARM control-plane permissions and is strictly reserved for company data-plane work.
4. **Ephemeral Hyperscale Burst Raid (Novel Idea #1 — `1/5 to 1/3 PB/day` / `2.3–3.8 GB/s`):**
   - Use Terraform (`terraform/hyperscale-burst/`) to spin up an ephemeral cluster of GCP Spot VMs + Local NVMe SSD (or `3–4 × n2-standard-8` within the default 24–32 vCPU trial quota) running Kafka KRaft / Redpanda + Spark/Flink for 15–20 minutes (`~$1.50–$3.00` deducted from the `$300` trial credit).
   - **Mandatory Cloud Safety Gates:** Place all VMs in a **single GCP Zone** communicating strictly over **Internal Private IP** (`$0.00/GB` network egress), configure `Cloud Billing Budget Alerts` at `$10/$30/$50`, embed a hard OS timer (`sudo shutdown -h +45`) in the Terraform `startup_script`, and run `terraform destroy` immediately after capturing Grafana/Spark/Flink evidence.
5. Never make a temporary or free-tier cloud resource the only copy of source code, data seeds, or evidence packs.

The deployable artifacts must remain portable across all stages: multi-architecture container images, Compose files, Helm charts, Terraform modules, Ansible roles, migrations, contracts, and runbooks. Moving from OrbStack to a VPS, from a VPS to K3s HA, or from K3s to GKE/EKS changes the infrastructure target; it must not require rewriting the source contracts or business transformations.

#### Infrastructure-first build order

1. Define the initial capacity target, SLOs, RPO/RTO, data retention, security boundaries, and failure domains.
2. Provision the Linux target with Terraform: network, firewall, DNS, IAM or service accounts, servers, disks, registry, and remote state.
3. Configure Ubuntu with Ansible: non-root access, SSH hardening, time synchronization, patching, disk mounts, kernel settings, container runtime, and node labels.
4. Bootstrap K3s or the selected Kubernetes distribution. Add ingress, storage classes, resource quotas, namespaces, metrics, and backup procedures.
5. Install the platform operators and shared services: Kafka/Strimzi, object storage, PostgreSQL, registry access, monitoring, and certificate management. Stateful services receive persistent storage and tested restore procedures.
6. Establish CI/CD for infrastructure plans, container builds, image scanning, Helm releases, and rollback. No application is deployed through manual shell commands after this gate.
7. Deploy the source and ingestion platform: API, generators, Debezium, contracts, Kafka topics, DLQs, API/file adapters, and raw landing.
8. Add the lakehouse and processing layer: Delta tables, Spark, Flink, compaction, checkpointing, schema evolution, backfill, and replay.
9. Add analytics products: dbt models, SCD Type 2 dimensions, facts, marts, ClickHouse serving, business quality, and data SLAs.
10. Add governance and operating controls: Airflow, DataHub, ownership, lineage, runbooks, alert routing, incident drills, and capacity dashboards.
11. Run scale and failure campaigns at increasing profiles, then migrate the same Terraform and application boundaries to cloud object storage, managed Kubernetes, and managed Kafka where the measured workload requires them.

#### Phase 1 execution plan: the first production gate

The first gate is deliberately small but infrastructure-complete. One order must travel through a real source system into a durable raw landing area on the provisioned platform. This proves the Linux, deployment, storage, and ingestion foundations before distributed processing is added.

**Week 1 — Linux and infrastructure foundation**

1. Create the repository layout, Makefile, Python environment, pre-commit checks, and CI test entry point.
2. Provision or select the Ubuntu target and document its network, disk, resource, and access boundaries.
3. Apply Terraform for the initial infrastructure and configure the host with Ansible.
4. Install the container runtime or K3s, ingress, storage, metrics, and backup hooks.
5. Establish a repeatable deployment and rollback path from CI/CD.

**Week 2 — Source system and durable ingestion gate**

1. Run PostgreSQL on the provisioned Linux platform and define the initial commerce schema: `customers`, `products`, `orders`, `order_items`, and `payments`.
2. Build a small FastAPI order service with Pydantic validation, database constraints, health checks, and API tests.
3. Build a deterministic generator with a seed. The small profile calls the API; the bulk profile uses efficient database loading for historical data.
4. Add Debezium CDC and Kafka in the deployed platform.
5. Define a versioned event contract containing `event_id`, `event_type`, `event_version`, `event_time`, `source`, `trace_id`, and payload metadata.
6. Validate events before downstream processing and route invalid events to a dead-letter topic.
7. Consume the event stream into an immutable MinIO raw landing area with ingestion metadata and a manifest.
8. Implement replay and idempotency tests: restarting the consumer or replaying a range must not create duplicate raw records.

**Phase 1 exit criteria**

- The infrastructure can be recreated from Terraform and Ansible without undocumented manual steps.
- CI/CD can deploy and roll back the platform and source services.
- `make setup` starts the local equivalent of the Linux-based source and ingestion stack.
- `make test` runs unit, contract, and integration tests.
- Creating an order produces a traceable CDC event in Kafka.
- An invalid event is rejected with a useful DLQ reason.
- The event is durably present in MinIO with source and ingestion metadata.
- Consumer restart and replay produce deterministic results.
- PostgreSQL, Kafka, and MinIO have health checks, metrics, backup or replay procedures, and documented recovery steps.
- The schema, contract, architecture diagram, failure cases, deployment steps, and runbook are documented.

Spark, Flink, Airflow, DataHub, ClickHouse, and Kubernetes are intentionally deferred until this slice is reliable. The next phase adds the lakehouse and processing engines on top of this stable source and ingestion boundary.

### Phase 2 — Source and ingestion platform

Debezium CDC, API adapters, file ingestion, Kafka, contracts, raw landing, DLQ, checkpoints, and replay.

### Phase 3 — Lakehouse and processing

MinIO, Delta Lake, Bronze/Silver/Gold, Spark, Flink, deduplication, skew, schema evolution, late data, and windows.

### Phase 4 — Analytics data products

dbt, dimensions, facts, SCD Type 2, business marts, ClickHouse, data dictionary, and quality gates.

### Phase 5 — Orchestration and operations

Airflow, DataHub, Prometheus, Grafana, CI/CD, runbooks, backfill, recovery, and capacity dashboards.

### Phase 6 — Infrastructure automation

Terraform, Ansible, Ubuntu VPS, K3s, Helm, operators, persistent storage, ingress, and release rollback.

### Phase 7 — Cloud migration

GCS or S3, GKE or EKS, managed Kafka, cloud IAM, cloud registry, lifecycle policies, and cost controls.

### Phase 8 — Future consumers

Feast, Kubeflow, MLflow, KServe, RAG, MCP tools, and agents.

## Explicit non-goals

- Recreating the complete internal platform of Grab, Uber, or another hyperscale company.
- Claiming that one laptop or VPS processes production-scale terabytes.
- Running multiple table formats, warehouses, orchestration systems, or observability stacks without a measured reason.
- Building ML, LLM, or agent services before the data foundation is replayable and governed.
- Treating a single-node Kubernetes cluster as highly available.
- Using direct API calls from analytical jobs without a durable, checkpointed ingestion layer.

## Future ML and LLM extensions

Once Gold and feature-ready tables are reliable:

```text
Gold and feature-ready tables
        ↓
Feast → Kubeflow → MLflow → KServe
        ↓
FastAPI and ML inference
```

For LLM workloads:

```text
Product, FAQ, and policy data
        ↓
RAG ingestion → embeddings → governed retrieval
        ↓
MCP tools → agents → gateway
```

These extensions consume the platform. They do not change the source, contract, replay, quality, and governance foundations.

## Change log

| Date | Change |
|---|---|
| 2026-10-03 | Accepted the lean, production-shaped Commerce Data Foundation Platform direction. |
| 2026-10-08 | Accepted data-source and simulation research: one commerce domain, owned generator, public datasets as fixtures/adapters, PB/day as a capacity model. |
| 2026-10-16 | Locked Unified Single-Source Data Engine (`Seed-and-Amplify` across DE/ML/RAG), rolling stage cleanup for `>=100 GB` rubric verification on Apple M1 Max (`214 GiB` available SSD), strict corporate Azure isolation, `$0` GCP Free Trial (`$300` / 90 days) debit-card guardrails, and Ephemeral Spot NVMe Hyperscale Raid (`2.3–3.8 GB/s`, `1/5–1/3 PB/day` equivalent) as Novel Idea #1. |
