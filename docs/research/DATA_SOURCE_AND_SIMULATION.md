# MultiCloud-Commerce-Lakehouse: Real-Time & Batch Data Platform — From Local Dev to Oracle Cloud Production & Google Cloud Migration (Data Source & Simulation Research)

## Purpose

This document records where data for **`MultiCloud-Commerce-Lakehouse`** comes from, how those sources were found, and how volume, messiness, and streaming behavior are simulated without pretending that a laptop processed petabytes. It is research evidence for the accepted catalog in [PROJECT_SOLUTION.md](../architecture/PROJECT_SOLUTION.md). It does not change the architecture, domain, or technology path.

The question this file answers is not "which public dump is biggest." The question is: how does one person prove a production-shaped data platform when real company streams are unavailable, public commerce snapshots are small, and PB/day is a capacity model rather than a personal-lab SLA.

## Decision lock

Do not reopen these choices here:

- One commerce domain. Public datasets are fixtures or adapters, not a second Gold model.
- Canonical operational path: TheLook-shaped PostgreSQL, owned contract-driven generator, Debezium CDC, Kafka, MinIO + Delta Lake, Spark/Flink, dbt, ClickHouse first.
- Own the generator in this repository. External generators are references, not dependencies.
- Company data is optional, authorized, anonymized, local-only, and never connected live to production.
- PB/day is modeled from measured units. It is not a laptop acceptance criterion.
- Implementation still starts at Phase 0/1 infrastructure. This file does not authorize Compose coding.

## The problem

There is no single magical dataset that is simultaneously:

- commercially shaped like a real order/inventory/payment system,
- messy enough to force SCD, corrections, late files, and schema drift,
- large enough to saturate Kafka, object storage, and compute,
- and already a live stream with inserts, updates, and deletes.

Public "ecommerce" dumps are usually static snapshots. A 30-year company database is accumulated state, not a stream. Taxi, ads, reviews, and GitHub events can stress files and serving engines, but they are the wrong canonical business model. Hyperscale PB/day claims from Grab, Uber, or similar platforms describe many brokers, independent storage, and multiple failure domains. Copying the slogan onto a Mac is not architecture.

The workable method is to practice three properties separately, then join them behind one contract.

## Three properties to practice

| Property | Source | What it proves | What it does not prove |
|---|---|---|---|
| Shape | TheLook-shaped Postgres, Olist, optional authorized company snapshot | Schemas, keys, nulls, corrections, SCD, payments, delivery, domain rules | Daily ingest rate |
| Rate | Seeded in-repo generator | Events/sec, bytes/sec, skew, burst, poison, duplicates, late data | Business realism by itself |
| Streaming | Debezium CDC, changelog replay, API collector, event producer | Ordering, offsets, lag, replay, checkpoints, event-time, DLQ | That the original OLTP database was already an event platform |

Company history and public snapshots feed shape. The generator feeds rate. CDC and collectors feed streaming. Gold stays one commerce model.

```text
Shape: TheLook / Olist / authorized snapshot
Rate:  in-repo generator (clone, skew, delay, duplicate, poison)
Stream: Debezium WAL + API/file collectors + changelog replay
        ↓
Same contracts → Kafka → raw MinIO → Delta Bronze/Silver/Gold
```

## Canonical source path

The accepted operational source is a PostgreSQL commerce schema aligned with theLook eCommerce.

theLook is a Looker/Google synthetic clothing store published as [`bigquery-public-data.thelook_ecommerce`](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce). It is regenerated on a daily cadence and is intended for product discovery, testing, and evaluation, not as a production volume feed. Typical analyses work at roughly 100k-scale users and orders; the `events` table is larger than the relational core. A public Kaggle extract of the dataset is on the order of 170 MB, which is enough for schema practice and far too small as a lakehouse volume source.

Canonical tables and streams across the **Unified Single-Source Engine (`commerce-datagen` — 13 Source Feeds / 165 Columns → 14 Gold Tables)**:

| # | Table / Stream | Cols | Role & Domain Shape (`theLook` + `Olist` + `H&M` + `Open CDP`) |
| :---: | :--- | :---: | :--- |
| **1** | `customers` | 18 | Customers, traffic source, PII (`email`, `phone`) & SCD Type 2 history |
| **2** | `distribution_centers` | 8 | Supply chain / warehouse fulfillment network (`theLook`) |
| **3** | `sellers` | 9 | Multi-merchant marketplace fulfillment (`Olist`) |
| **4** | `products` | 17 | Catalog (`H&M` / `theLook` real `detail_desc`), **75% `electronics` Skew (`R6`)**, SCD Type 2 |
| **5** | `inventory_items` | 9 | Stock status, batch lot & COGS unit cost (`theLook`) |
| **6** | `orders` | 20 | Order header, **80% `Ho Chi Minh City` Skew (`R6`)** & delivery SLA timestamps (`Olist`) |
| **7** | `order_items` | 17 | Line items, COGS margin & **~2% Offline Duplicates (`R8`)** with shifted `created_ts` |
| **8** | `payments` | 15 | Payment attempts, installments, PII `card_last4` & mid-dataset **Schema Evolution `failure_reason` (`R7`)** |
| **9** | `order_reviews` | 10 | Post-purchase ratings (`1..5`), review text & sentiment (`Olist`) |
| **10** | `clickstream` | 14 | Behavioral web/app funnel (`Open CDP`) with **5–10 min Mobile Late Arrivals (`R9`, `R17`)** |
| **11** | `payment_callbacks` | 11 | Gateway webhooks with **~1.5% Streaming Duplicates (`R10`, `R18`)** |
| **12** | `dlq_poison_events` | 9 | Dead-Letter Queue isolating contract/schema violations |
| **13** | `legacy_daily_revenue_report` | 8 | Monolithic SQL report for Stage 7 **WAP Zero-Copy Migration Reconciliation (`R39`)** |

The production-shaped lab pattern is:

```text
Seed snapshot
  → PostgreSQL
  → contract-driven generator writes inserts/updates/deletes
  → Debezium reads WAL (pgoutput, logical decoding)
  → Kafka topics per table
  → immutable raw landing on MinIO
```

This is the same pattern Factor House published as [theLook eComm CDC](https://github.com/factorhouse/examples/tree/main/projects/thelook-ecomm-cdc) and [theLook datagen](https://github.com/factorhouse/examples/tree/main/datagen/thelook-ecomm): seed users/products/distribution centers, then continuously create purchases, user updates, cancellations, returns, and anonymous "ghost" sessions so CDC actually sees change. Use that as a pattern. Do not vendor the project to their generator, licenses, or Compose stack.

An authorized company snapshot later maps onto the same contracts. Different rows, same tables, same ingestion rules.

## Where to search

Useful places, in the order this project should look:

| Place | What it is good for | Limit |
|---|---|---|
| [Google Cloud Marketplace / BigQuery public data](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset) | theLook, GA4 merchandise sample, SQL-shaped public tables | Snapshots, not live CDC |
| [Kaggle](https://www.kaggle.com/datasets) | Olist, Open CDP/REES46 multi-category store events, compact retail event dumps | License, size, and domain vary; many are static CSVs |
| [Hugging Face](https://huggingface.co/datasets) | Amazon Reviews 2023 and other large document corpora | Nested JSON, not OLTP |
| [NYC TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Monthly Parquet, schema drift, file ingestion | Taxi domain; isolated fixture only |
| [ClickHouse example datasets](https://clickhouse.com/docs/getting-started/example-datasets) | Serving-engine stress (hits, GitHub events, Amazon reviews) | Not the canonical commerce model |
| [TPC-DS `dsdgen`](https://www.tpc.org/tpcds/) | Warehouse scale factor as a knob | Decision-support schema, not CDC |
| GitHub (`factorhouse/examples`, Debezium examples, dbt-labs/jaffle-shop, student lakehouse repos) | Generator and CDC patterns | Capstone quality unless proven otherwise |
| AWS Open Data / Azure Open Datasets / data.gov | Large file corpora | Rarely commerce OLTP |
| Alibaba Tianchi, SNAP, UCI | Recsys behavior logs | Hashed IDs, wrong grain for this Gold model |
| Company systems | Real nulls, 30-year history, local business rules | Only with authorization; history is not a stream |

Do not search for "the biggest dataset" and pour it into Gold. Search for a source that proves one of the three properties, then keep it behind an adapter.

## Dataset catalog

Roles used below:

- **Canonical** — shapes the PostgreSQL source and Gold model.
- **Shape adapter** — messy real commerce used to test mapping, quality, and SCD. Not a second domain.
- **Volume fixture** — isolated lake/file/serving stress. Never mixed into canonical Gold.
- **API/file practice** — tiny sources used only to build collectors.
- **Reject** — useful elsewhere, wrong here.

### Canonical

| Dataset | Size and grain | Role | Why it is canonical | Why it is not a volume source |
|---|---|---|---|---|
| theLook eCommerce | Seven related tables; synthetic clothing store; daily refresh in BigQuery; typical user/order analyses around 100k scale; events larger; public extracts ~170 MB | Canonical OLTP/CDC schema | Realistic customers, catalog, orders, line items, inventory, web events, logistics | Continuously regenerated snapshot, not a production firehose |
| In-repo generator on that schema | Controlled events/sec and bytes/sec | Canonical rate and mutation source | Owns contracts, seeds, poison, skew, and replay | Must be written; public dumps will not do this |

Closest published implementation of the live path, used as reference only:

- [factorhouse/examples `datagen/thelook-ecomm`](https://github.com/factorhouse/examples/tree/main/datagen/thelook-ecomm)
- [factorhouse/examples `projects/thelook-ecomm-cdc`](https://github.com/factorhouse/examples/tree/main/projects/thelook-ecomm-cdc)

Closest student lakehouse repos using the same domain, not verified production:

- [pyoska11/Data-lakehouse](https://github.com/pyoska11/Data-lakehouse)
- [lechihoang/Thelook-lakehouse](https://github.com/lechihoang/Thelook-lakehouse)

### Shape adapters

| Dataset | Size | Best use | Do not use for |
|---|---|---|---|
| [Olist Brazilian e-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) | 99,441 orders (2016–2018), nine CSVs, about 45 MB compressed / 120–130 MB raw. Tables: customers, orders, order_items, payments, reviews, products, sellers, geolocation (~1M rows), category translation | Messy marketplace reality: multi-item orders, split sellers, installments, delivery SLA, reviews, geo, SCD/backfill practice | Volume, CDC, or a second Gold domain |
| Authorized company snapshot | Whatever the owner permits after anonymization | Real nulls, corrections, 30-year history, local product rules | Git, cloud upload, live prod CDC, or "this is already a stream" |

Olist is the best public *messy commerce* adapter. It is still tiny. Map selected facts into the commerce contract or keep it as an isolated quality fixture. Do not rebuild Gold around Brazilian zip codes and Game-of-Thrones-anonymized seller names.

### Volume fixtures (isolated)

These prove file, object-store, and processing mechanics. They must not become canonical facts.

| Dataset | Typical published size | Best isolated use | Domain warning |
|---|---|---|---|
| [Open CDP / REES46 multi-category store events](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) | NVIDIA primer: ~206k users, ~412M events, ~52 GB uncompressed; Kaggle listing ~4.6 GB compressed; some HoF writeups used ~285M-event subsets. Oct 2019–Apr 2020. Event types: `view`, `cart`, `remove_from_cart`, `purchase` | Clickstream volume, sessionization, late events, funnel | Events only; no durable OLTP catalog/orders model |
| [NYC TLC trip Parquet](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Monthly vendor files with schema drift. Official 2024 yellow taxi volume: 41,169,638 trips. HVFHV monthly files are larger (commonly a few hundred MB each). Combined historical dumps in student repos are often tens of GB | Spark/Delta file ingestion, partitioning, compaction, evolving Parquet schemas | Taxi, not commerce. Best public *file* corpus, not the business model |
| [Amazon Reviews 2023](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023) (McAuley Lab / Hou et al.) | 571.54M reviews, 54.51M users, 48.19M items, 33 categories. On-disk size is hundreds of GB depending on JSONL vs Parquet and how many categories are stored | Nested documents, schema evolution, file/API-style landing | Reviews, not orders. ClickHouse also publishes an older ~150M-review, ~49 GB Parquet slice for serving labs |
| TPC-DS via [`dsdgen`](https://www.tpc.org/tpcds/) | Scale factor is a knob: SF1 ≈ 1 GB, SF100 ≈ 100 GB, SF1000 ≈ 1 TB, SF10000 ≈ 10 TB | ClickHouse/dbt warehouse performance, skew, star/snowflake queries | Decision-support schema. Generate it; do not scrape commerce into TPC-DS |
| [GA4 obfuscated merchandise](https://console.cloud.google.com/marketplace/product/bigquery-public-data/ga4-obfuscated-sample-ecommerce) | Small BigQuery public sample | Event schema practice | Not volume |

NYC Taxi remains the best public file corpus because monthly Parquet files actually drift and are large enough to stress Spark. TPC-DS remains the best generated warehouse corpus because scale is a parameter. Open CDP is the best public *commerce-like clickstream* volume. None of them replace theLook-shaped OLTP/CDC.

### Serving and query fixtures

Use these only to exercise ClickHouse, compaction, and query shape. They are not source-of-truth commerce data.

| Dataset | Published size | Use |
|---|---|---|
| ClickBench `hits` | About 100 million rows; compressed distributions commonly in the 10–20 GB range, TSV much larger | Browser-hit serving benchmark |
| ClickHouse GitHub events | 3.1 billion records from 2011 to 2020-12-06; 75 GB download; up to 200 GB on disk with lz4 | Large MergeTree lab |
| ClickHouse Amazon reviews slice | >150M reviews, 49 GB snappy Parquet; ~70 GB original / ~30 GB in ClickHouse in their walkthrough | Document-style serving |
| WikiStat / Criteo TB click logs | Far larger; ads or web stats | Reject for this domain; too big and the wrong grain |

### Useful but not canonical

| Dataset | Why people reach for it | Decision |
|---|---|---|
| Instacart ~3M orders / 200k users | Grocery basket analysis | Shape-adjacent only; license and domain are not the platform model |
| Taobao / Tianchi ~100M behaviors | Recsys | Hashed IDs, recsys grain |
| Retail Rocket ~2.76M events | Compact funnel | Too small and incomplete as OLTP |
| Yoochoose ~33M clicks | Session recsys | Not orders |
| dbt Jaffle Shop / `jafgen` | Teaching marts | Too small; keep as dbt tutorial, not source |
| DummyJSON / FakeStore API | Tiny REST | API-adapter practice only |
| Stripe test webhooks | Payment callbacks | Webhook-adapter practice only |
| Magento/Shopify sample, AdventureWorks, Northwind, Chinook | Teaching databases | Too small and the wrong operating model |

## Simulation stack

Own a small in-repo generator with the same contracts as production adapters. Do not make CI or Compose depend on a random GitHub project.

| Tool | Use in this project |
|---|---|
| In-repo commerce generator | Canonical. Seeded, contract-driven, writes PostgreSQL and/or Kafka, can clone/skew/delay/duplicate/poison |
| Factor House theLook simulator | Reference for live OLTP activity: purchases, address updates, status changes, ghost sessions, configurable QPS |
| Materialize `datagen` | Schema-driven fake data with joinable keys |
| AWS Flink DataGen example | High-rate parallel generation; published on the order of ~64k records/sec on a small cluster |
| `kafka-data-gen` / Khaos-style harness | Events/sec, message size, hot partitions, consumer lag, broker failure |
| `dsdgen` | Warehouse-scale analytical tables |
| Faker / SDV / Gretel / Benerator / Mockaroo | Shape and PII-safe fakes, not rate |
| Confluent datagen | Optional Kafka load harness |

Generator profiles, already accepted in the architecture:

```text
ci        small, deterministic, checked into tests
dev       interactive, seconds-to-minutes
batch     historical snapshot load into PostgreSQL/MinIO
stream    CDC or direct Kafka at a target events/sec
stress    stats-only or short-burst; do not persist petabytes
```

The generator must:

1. Seed from a fixture so CI is deterministic.
2. Speak the same Avro/JSON contracts as CDC and API adapters.
3. Write into PostgreSQL so Debezium sees real WAL, and optionally write Kafka directly for isolation tests.
4. Mix three inputs: replayed company-derived changelogs, public rows mapped to commerce events, and synthetic records.
5. Emit controlled failure modes: clone, skew, delay, duplicate, poison, schema drift, and burst.

Factor House's simulator is the best public demonstration of (3) and (4) for theLook. Copy the idea, not the dependency.

## How large volume is practiced without storing petabytes

Public snapshots and a 30-year OLTP database prove shape. They rarely prove daily ingest rate.

A 10 TB historical snapshot averaged over 30 years is roughly 1 GB/day of accumulation:

```text
10 TB / (30 × 365 days) ≈ 0.9 GB/day
```

PB/day is an ingress *rate*, about 11.6 GB/s of raw data before replication, indexing, compaction, and peak traffic. Replication factor 3 makes the write budget about 35 GB/s before overhead. That is a hyperscale design exercise.

Practice volume in three complementary ways:

1. **Shape from real data.** Authorized snapshot, theLook, or Olist.
2. **Rate from a generator.** Controlled events/sec and bytes/sec against those contracts.
3. **Scale from measurement plus a model.** Measure one partition, one broker, one Spark/Flink worker, and one object-store prefix. Multiply those measured units to 10 TB/day, 100 TB/day, and PB/day.

Lab methods that do not require storing 1 PB:

| Method | What is stored | What is proven |
|---|---|---|
| Stats-only load | Counters, lag, CPU, disk, network | Maximum ingest or processing rate |
| Short burst | Minutes of data, then delete | Peak multiplier and backpressure |
| Time compression | One day of history replayed in one hour | Event-time, watermarks, late data, windows |
| Partition saturation | Data for one or a few partitions | Per-partition throughput, hot keys, consumer lag |
| Schema-faithful fan-out | Sampled real records cloned with new IDs | Volume using real field distributions |
| Component isolation | One layer at a time | Kafka, object storage, Spark, Flink, serving bottlenecks |
| Full path at small/medium | End-to-end Bronze to Gold | Correctness, idempotency, quality, recovery |

Every scale report must state event size, peak multiplier, replication, partition count, whether payloads were persisted or discarded, and the measured unit it was calibrated from. A PB/day number without those fields is not an architecture claim.

### Architecture evidence pack

For any logical 10 TB/day, 100 TB/day, or PB/day design, publish:

1. Requirements: event size, retained days, peak multiplier, RPO/RTO, consumers.
2. Logical topology: sources, contracts, Kafka, lakehouse, serving.
3. Physical topology: brokers, workers, disks, network, failure domains.
4. Capacity model: measured unit × count of units, plus headroom.
5. Failure model: broker loss, hot partition, schema break, source outage, disk full.
6. Verification: the actual small/medium run used as calibration.
7. Decision records: what would change at the next scale gate.

This is how the project can be interview-grade without lying about laptop throughput.

## Company data policy

A 30-year company database can be extremely useful and is still the wrong thing to treat as a stream.

Rules:

- Use it only with explicit authorization from the data owner.
- Prefer a one-time anonymized snapshot over any live connection.
- Remove or tokenize personal and confidential fields before it touches the lab.
- Keep it on an isolated local volume. Never commit it to Git, bake it into images, print it in logs, or upload it to unapproved cloud.
- Document retention and destruction.
- Do not CDC production.

How to turn history into streaming practice:

```text
Authorized snapshot
  → anonymize / tokenize
  → profile nulls, duplicates, corrections, SCD cases
  → synthesize a changelog (inserts, status updates, delayed corrections)
  → replay with event-time into PostgreSQL or Kafka
```

History is state. The changelog is the stream. Ten terabytes of 30-year OLTP is not evidence of PB/day, and it is not automatically better than theLook plus a generator. It is better for *shape*: real product names, ugly statuses, backfills, and business exceptions.

If authorization never comes, the platform still has a complete source strategy: theLook-shaped Postgres, Olist as a messy adapter, Open CDP/NYC/Amazon/TPC-DS as isolated fixtures, and the owned generator for rate.

## Why jobs must not call APIs directly

Downstream Spark, Flink, dbt, or BI jobs must not paginate vendor APIs. Each API has a collector that owns:

- Authentication and token refresh.
- Cursor or checkpoint persistence.
- Incremental extraction (`updated_since`, cursors, webhooks, vendor change feeds).
- Pagination, rate limiting, timeout, retry, and backoff.
- Raw response landing with request metadata and checksums.
- Schema validation and version mapping.
- Idempotent record keys and ingestion run IDs.
- Quarantine and replay for malformed responses.

Reasons, all observed in real platforms:

| Direct API-from-job failure | What the collector fixes |
|---|---|
| Rate limits and vendor bans | One controlled extractor, backoff, budgets |
| Auth expiry mid-job | Token refresh isolated from compute |
| Pagination and partial pages | Checkpointed cursors |
| Schema drift | Raw landing plus contract tests |
| No replay | Immutable raw objects and run IDs |
| No DLQ | Quarantine with a reason |
| Fan-out from every retrying job | One writer, many downstream readers |

Tiny practice sources for the collector itself: DummyJSON, FakeStore, and Stripe test webhooks. They are not data volume.

## Closest existing work

The Hall of Fame scan in `/Users/MAC/Documents/Automation-Get-Slide/analysis/hall_of_fame/` and public GitHub both confirm the same lesson: people already assemble pieces of this platform, almost never the whole production-shaped path with evidence.

| Work | Why it matters | Limit |
|---|---|---|
| Factor House theLook datagen + Debezium CDC | Best live-simulator pattern for this exact schema | Example, not a platform; optional commercial UI |
| `lechihoang/Thelook-lakehouse`, `pyoska11/Data-lakehouse` | Closest student lakehouse on theLook | Capstone, not verified production |
| `bmd1905/Customer-Purchase-Prediction-ML-System` | Open CDP volume + CDC + Kafka + MinIO | ML-heavy, many Compose files |
| `trannhatnguyen2/NYC_Taxi_Data_Pipeline` | File/batch + stream DE shape | Taxi domain |
| `longphamkhac/Data-Processing-System-for-Amazon-Reviews` | GKE + Kafka + Flink/Spark on a large document corpus | Reviews, not commerce OLTP |
| `khanhhk/Data-Systems-for-Toxic-Comment-Classification` | Delta + Debezium + quality gates | NLP domain, tool soup |
| Grab / Uber / VNG public talks | Lean CDC → Kafka → lakehouse → serving; Hudi originated at Uber | Hyperscale ops and PB claims do not transfer to a laptop. This project stays Delta on MinIO |

No researched repository removed the need for an owned generator, a company-data policy, and a capacity model. The gap this project should beat is not "more tools." It is contracts, idempotency, replay, quality gates, recovery, and measured scale.

## Decisions for this project (Updated 2026-10-16)

1. **Unified Single-Source Engine (`Seed-and-Amplify`):** A single owned generator (`commerce-datagen`) seeded with real product descriptions (`theLook` / `H&M articles.csv` `detail_desc`) and Markdown `FAQ & Shipping/Return Policy` documents serves all three coursework layers (DE Lakehouse, Feast/Kubeflow ML `will_order_in_7d`, and RAG/MCP Agent) with 100% consistent `customer_id`, `product_id`, and `order_id` keys.
2. **Rubric-Exact Problem Injection (18/100 Rubric Points):** Controlled via `generator_config.yaml` across 5 profiles (`ci`, `dev`, `rubric_100gb`, `stream`, `stress`) to inject:
   - Offline: `80%` `shipping_city = 'Ho Chi Minh City'` & `75%` `category = 'electronics'` (Skew), mid-dataset `payments.failure_reason` (Schema Evolution), `~2%` `order_items` duplicates (Offline Duplicates), and SCD Type 2 history (`valid_from_ts`, `valid_to_ts`, `is_current`).
   - Streaming & ML: 5–10 min delayed `clickstream` (Late Arrivals), `~1.5%` duplicate `payment_callbacks` (Streaming Duplicates), configurable **Data Drift**, and daily `label` table (`will_order_in_7d`).
3. **Local SSD & RAM Protection on Apple M1 Max (`10 cores, 64 GiB RAM, 214 GiB available SSD`):**
   - Daily coding runs at `PROFILE=dev` (`1–2 GB`, `<10s`).
   - Rubric `>=100 GB` verification runs in `250–500 MB` streaming PyArrow/DuckDB/Spark chunks (`<6 GiB RAM`, 3–6 min) with **Rolling Stage Cleanup** (Raw `100 GB` → Bronze `ZSTD` compressed `~25–35 GB` → delete temporary Raw `100 GB` before Silver/Gold).
   - Local high-rate burst (`0.6–1.2 GB/s`, `50–100 TB/day` equivalent) uses hard circuit breakers (`--max-seconds 10 --max-gb 2`) and Kafka `retention.ms=10000` + `lz4` compression (`0 GB` permanent disk growth).
4. **Permanent Always Free Cloud & Ephemeral Hyperscale Cloud Raid (`200–500 MB/s` sustained, `0.5–1.0 GB/s` peak) on Oracle Cloud (`us-phoenix-1` — 100% Self-Managed on Raw VMs, Zero Managed Services):**
   - Provisioned via Terraform (`oracle/oci` provider) on **Oracle Cloud Infrastructure (`Tenancy: CloudBigDataPlatform`, Home Region: `us-phoenix-1`)**, verified live via `oci-cloud` MCP (`41 OCPUs / 277 GB RAM` `VM.Standard.A1.Flex` in `UHSM:PHX-AD-1`, `2 × VM.Standard.E2.1.Micro` in `UHSM:PHX-AD-2`, `200 GB` `20-VPU` Raw Block Storage).
   - **Strict Zero-Managed-Service Rule:** Only raw IaaS primitives (`VCN`, `Subnet`, `Security List`, raw Ubuntu 24.04 VMs, and raw `20-VPU` Block Volumes) are used. Every platform service (`K3s`, `MinIO`, `PostgreSQL 16`, `Kafka KRaft`, `Debezium`, `Flink`, `Spark`, `ClickHouse`, `Airflow 3`, `DataHub`, `Vault`, `NGINX`, `Prometheus/Grafana/Loki`, `ArgoCD`) is 100% self-hosted on raw Linux VMs — no managed PaaS/SaaS (`Autonomous DB`, `OKE`, `OCI Object Storage`, etc.).
   - Strictly isolated from corporate Azure (`NAric@ashleyfurniture.com` / `DATAWAREHOUSE PROD` `8e606065-9e63-4319-ba39-0ff434c72b39`) and kept in the `Free Tier account` state so out-of-pocket card charges are `$0.00`.
   - Permanent staging runs `VM.Standard.A1.Flex` (`ARM64` matching local M1 Max 1:1) + `2 × E2.1.Micro` edge nodes with `ZRAM zstd` and `Smart Auto-Yield Keepalive`; ephemeral multi-node K3s HA & soak benchmarks use the `$300` Free Trial credit pool inside the private VCN subnet (`10.20.0.0/16`, `$0/GB` internal traffic) followed by `terraform destroy` on burst nodes.
5. **External public dumps (`Olist`, `Open CDP`, `NYC TLC`, `Amazon Reviews 2023`, `TPCx-BB`):** Retained in documentation as optional reference benchmarks only; none are required to complete the canonical Gold, ML, or RAG path.

These decisions are now part of the accepted catalog. Change them in `PROJECT_SOLUTION.md`, not by quietly adding another dataset to Compose.

## What not to do

- Do not hunt for a public PB/day commerce stream. It is not sitting on Kaggle.
- Do not CDC the company production database or touch the corporate Azure subscription (`DATAWAREHOUSE PROD`).
- Do not commit company extracts, API keys, or raw PII.
- Do not depend on Factor House, Confluent datagen, or a student repo at runtime.
- Do not put NYC Taxi trips into commerce facts.
- Do not treat 30 years of OLTP as proof of daily ingest.
- Do not persist 4 copies of `100 GB` simultaneously on the `214 GiB` local SSD without rolling stage cleanup.
- Do not choose congested Asia-Pacific regions (`Singapore`/`Tokyo`) for Oracle Cloud Free Tier; use `us-phoenix-1` (`CloudBigDataPlatform`) where `41 OCPUs / 277 GB RAM` ARM64 capacity is verified available.

## Evidence limits

Numbers in this file come from dataset landing pages, vendor docs, official TLC reporting, Hugging Face dataset cards, ClickHouse documentation, NVIDIA/Kaggle listings, GCP Compute Engine / Spot / Local SSD documentation, and public GitHub READMEs, reviewed through 2026-10-16. Treat sizes as order-of-magnitude planning figures and re-measure before any benchmark that will be quoted.

## Implementation implication

The source strategy is now locked around the **Unified Single-Source Engine (`commerce-datagen`)**. The next *build* step remains Phase 1: repository layout, Compose, PostgreSQL, MinIO.

When Phase 2 starts, implement in this order:

1. Commerce schema + Real Catalog & Policy Seed (`ci` profile ~50 MB, linking `products`, `customers`, `orders`, and RAG markdown policies).
2. Owned vectorized generator (`commerce-datagen`) with `dev` (`1–2 GB`), `rubric_100gb` (`>=100 GB` chunked + rolling stage cleanup), `stream`, and `stress` (`--max-seconds 10 --max-gb 2`) profiles driven by `generator_config.yaml`.
3. FastAPI source service, Debezium CDC, and Kafka topics (`lz4` compressed, DLQ enabled).
4. Spark batch optimization (Skew, Schema Evolution, Dedup, Compaction) and Flink streaming optimization (Watermarks/Late Arrivals, Idempotent Dedup, Windowing).
