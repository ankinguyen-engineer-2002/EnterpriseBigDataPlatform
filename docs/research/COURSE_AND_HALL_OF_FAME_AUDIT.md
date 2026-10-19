# Course and Hall-of-Fame Audit

## Purpose

This document records what the scanned course material and public Hall-of-Fame repositories teach us about building the Commerce Data Foundation Platform. It distinguishes reusable engineering patterns from claims that still require execution evidence.

## Evidence and limits

The scan in `/Users/MAC/Documents/Automation-Get-Slide/analysis/hall_of_fame/` was generated on 2026-10-06. It inventoried 48 public entries and successfully read metadata and selected repository content for 47 of them. The scanner did not clone repositories, run code, run containers, execute CI jobs, deploy infrastructure, or verify a live service. Its architecture labels are heuristics based on README prose, tree paths, and manifest names.

The course material in `/Users/MAC/Documents/Automation-Get-Slide/output/` covers Linux, Python, databases, Web APIs, validation and testing, containers, ingestion, storage, Spark, Flink, orchestration, and consumption. Slides are useful for concepts and expected scope; they are not operational evidence for any external project.

Therefore, a repository containing `terraform/`, `k8s/`, `helm/`, `prometheus/`, or a screenshot of GKE is evidence that an author documented or attempted that layer. It is not evidence of high availability, a tested disaster recovery process, a production SLO, or sustained scale.

## What the scan confirms

### The recurring production-shaped pattern

The Hall of Fame scan found Docker/container signals in 44 of 48 entries, Kubernetes in 31, Helm in 32, Terraform in 29, Jenkins in 32, Prometheus in 32, and Grafana in 33. These numbers show a strong course and community pattern:

```text
Source code
  → container image
  → registry
  → CI/CD
  → Kubernetes or a cloud VM
  → monitoring and an exposed service
```

Among the data-engineering entries, the repeated data path is:

```text
OLTP/files/APIs
  → Debezium or an ingestion connector
  → Kafka for streaming
  → MinIO/object storage
  → Delta Lake/Parquet
  → Spark and/or Flink
  → dbt and a warehouse/OLAP engine
  → BI or ML consumers
```

The lessons support the same separation of concerns:

- Linux provides process, filesystem, networking, users, permissions, Bash, cron, and Make fundamentals.
- Containers provide portable images, runtimes, namespaces, cgroups, volumes, networks, registries, and Compose.
- Ingestion distinguishes structured, semi-structured, and unstructured sources, batch files, CDC, Kafka partitions, retention, and replay.
- Storage distinguishes file, block, and object storage; data lake, lakehouse, warehouse, dimensional modeling, and Delta Lake.
- Spark addresses distributed batch execution, partitions, shuffles, caching, skew, and memory.
- Flink addresses event time, watermarks, windows, late data, state, checkpoints, and backpressure.
- Airflow coordinates dependencies, retries, backfills, and quality gates; it is not a warehouse, queue, or processing engine.
- Consumption covers facts, dimensions, SCD, OLAP engines, ClickHouse/Pinot, and feature-ready data.

This validates the project's existing layer boundaries.

## Reference patterns worth borrowing

| Reference | Useful pattern | Use in this project | What not to assume |
|---|---|---|---|
| Customer Purchase Prediction ML System | Separate Compose files, Makefile, lockfile, CDC, Kafka, Flink, Spark, MinIO, feature/serving and observability boundaries | Use profiles and explicit start/stop targets; keep the data foundation independent from future ML | Many Compose files do not prove that the full stack is reliable or affordable to run together |
| NYC Taxi Data Pipeline | Clear local batch/stream split, Airflow, dbt, Delta, MinIO, Debezium, validation, warehouse and query layer | Use as the first local end-to-end reference and repository shape | Manual uploads, loose credentials, unpinned images, or local-only instructions are not production controls |
| Amazon Reviews on GKE | Terraform/Ansible, Helm, Strimzi, Flink and Spark operators, Schema Registry, Kafka-to-object-storage sink, Airflow | Borrow the later cloud/Kubernetes operating model and schema-compatibility boundary | A README saying “deployed on GKE” does not verify HA, security, recovery, or scale |
| Toxic Comment Data Systems | Delta on MinIO, Debezium/Kafka/Flink, Spark, dbt, Great Expectations, Trino/Hive, Airflow, DVC/MLflow, monitoring | Borrow explicit quality, lineage, batch/stream convergence, and replay concepts | Running every framework simultaneously can obscure ownership and create an expensive local stack |
| AWS Recommender / GKE MLOps projects | Local Compose or Kind plus Terraform, Helm, CI/CD and cloud deployment | Borrow environment promotion and infrastructure-as-code boundaries | Kubeflow, Feast, Ray, Jenkins, and multiple serving systems are not required for the DE foundation |

## Decisions for the Commerce Data Foundation Platform

### Product level

The target is an **enterprise-shaped personal platform**:

- It must be buildable and understandable by one person on a Mac.
- It must run a complete functional path locally and in a remote lab.
- It must expose the contracts, deployment boundaries, failure modes, recovery procedures, and capacity model expected at work.
- It must not claim that one laptop or one VM is a highly available enterprise production system.

This is more valuable than making a small demo look large with many untested services.

### Main technology path

The main path remains self-managed and course-aligned:

```text
PostgreSQL/API/file/event sources
  → source adapters and contracts
  → Debezium + Kafka
  → immutable raw landing on MinIO
  → Delta Lake Bronze/Silver/Gold
  → Spark batch/backfill + Flink streaming
  → dbt business models
  → ClickHouse serving
  → BI/API/future ML consumers
```

Airflow, DataHub, Prometheus/Grafana, CI/CD, Terraform, Ansible, and K3s are control and operations layers added at the gates defined in the implementation checklist. Trino, Iceberg, Hudi, managed Kafka, BigQuery, GKE, and other alternatives remain isolated comparison experiments until a measured requirement justifies them.

### Environment path

The scanned projects reinforce this promotion path:

```text
OrbStack + Docker Compose
  → one Ubuntu cloud VM + self-managed services
  → three cloud VMs + K3s HA lab
  → optional GKE/EKS/AKS comparison
```

Each environment is complete and has one canonical storage path. The project must not create an accidental hybrid where Kafka is local, processing is remote, and storage is split between both without a deliberate networking, security, cost, and recovery design.

## What qualifies as “production-shaped” here

The project earns that description only when it can demonstrate the following, not merely list the tools:

- Versioned source and event contracts with compatibility policy.
- Immutable raw landing and replayable ingestion.
- Idempotent consumers, deterministic backfills, and duplicate handling.
- Explicit DLQ/quarantine paths and actionable failure reasons.
- Data-quality checks that block publication of invalid Gold data.
- Batch and streaming convergence in the same governed data model.
- Health checks, metrics, logs, traces where useful, and freshness/volume alerts.
- CI tests, image build, registry promotion, deployment, health verification, and rollback.
- Terraform/Ansible recreation without undocumented manual steps.
- Persistent storage, backup, restore, retention, secrets, PII-safe logging, and access boundaries.
- SLO, RPO, RTO, capacity profiles, bottleneck evidence, and a recovery runbook.
- Failure campaigns for source outage, Kafka lag, poison messages, late data, disk pressure, node loss, partial writes, and schema changes.

## What to build first

The scan does not change the current starting point. The first implementation gate remains:

1. Complete Ubuntu and container fundamentals.
2. Create the repository Compose foundation and Makefile.
3. Run PostgreSQL with a persistent volume and verify restart/recovery.
4. Add MinIO and an immutable raw landing manifest.
5. Add a small commerce API and seeded generator.
6. Add CDC and Kafka only after the source and storage boundaries are understood.

The repositories are references for design review, not templates to copy wholesale. The desired outcome is a lean platform with strong evidence at each layer.

## Bottom line

The scanned work is highly useful for selecting the project's shape and for seeing how course projects expose local, cloud, CI/CD, Kubernetes, and observability boundaries. It is mostly evidence of **production-shaped personal or capstone systems**, not independently verified enterprise production. Our project should therefore use the same open-source core and promotion model, then exceed those examples in the areas that are usually hidden: contracts, idempotency, quality gates, recovery, security, SLOs, and measured failure/scale tests.
