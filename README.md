# GitStream

Real-time GitHub public events pipeline — from API polling to Delta Lake, with live SQL dashboards.

![CI](https://github.com/HongJunseong/gitstream/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda_Cloud-red)
![Spark](https://img.shields.io/badge/Spark-Structured_Streaming-orange)
![Delta Lake](https://img.shields.io/badge/Storage-Delta_Lake-brightgreen)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow_2.9-lightblue)

---

## Motivation
Built to explore end-to-end streaming architecture using modern data engineering tools —
specifically how Kafka, Spark Structured Streaming, and Delta Lake work together
in a production-like environment.

---

## Overview

GitStream continuously polls the [GitHub Public Events API](https://docs.github.com/en/rest/activity/events), streams events through Redpanda (Kafka-compatible), processes them with Spark Structured Streaming on Databricks Serverless, and stores the results in Delta Lake tables for real-time analytics.

```
GitHub API
    │  poll every 30s (ETag dedup)
    ▼
┌─────────────────┐
│  Poller         │  Python · Docker
│  (github_poller)│  SASL_SSL + SCRAM-SHA-256
└────────┬────────┘
         │ produce (gzip, idempotent)
         ▼
┌─────────────────┐
│  Redpanda Cloud │  Kafka-compatible · Serverless
│  github.raw.    │  3 partitions
│  events         │
└────────┬────────┘
         │ readStream
         ▼
┌─────────────────────────────────────┐
│  Databricks Serverless              │
│  Spark Structured Streaming         │
│                                     │
│  parse → validate → write           │
│  ├── events_v1        (raw append)  │
│  └── repo_activity_5m (5m window)   │
└────────┬────────────────────────────┘
         │ Delta Lake (Unity Catalog Volumes)
         ▼
┌─────────────────┐     ┌──────────────────┐
│  SQL Dashboard  │     │  Airflow DAG     │
│  4 live charts  │     │  health monitor  │
└─────────────────┘     └──────────────────┘
```
---

## Dashboard
(Example) Real-time GitHub event activity monitored across 4 panels, refreshed every 10 minutes.

<img width="1616" height="783" alt="Databricks Dashboard" src="https://github.com/user-attachments/assets/1623abc8-1707-470c-8aae-d07ec08ee340" />

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, GitHub REST API v2022-11-28 |
| Message Broker | Redpanda Cloud (Kafka-compatible, SASL_SSL + SCRAM-SHA-256) |
| Stream Processing | Apache Spark Structured Streaming (Databricks Serverless) |
| Storage | Delta Lake on Unity Catalog Volumes |
| Orchestration & Monitoring | Apache Airflow 2.9 (Docker Compose) |
| Visualization | Databricks Dashboard (Lakeview) |
| CI/CD | GitHub Actions (ruff + pytest / Docker build + EC2 & Databricks deploy) |
| Container | Docker, Docker Compose |

---

## Key Features

- **Zero message loss** — idempotent Kafka producer (`enable.idempotence=true`, `acks=all`)
- **Efficient deduplication** — ETag conditional requests (HTTP 304) + `last_event_id` filter eliminate redundant API calls and duplicate events
- **Late data handling** — 10-minute watermark on `created_at` absorbs out-of-order events before closing 5-minute tumbling windows
- **Graceful shutdown** — SIGTERM/SIGINT handler flushes the Kafka producer before exit
- **Exponential backoff** — full-jitter backoff on GitHub API rate limits (403/429) and network errors

---

## Delta Tables

### `events_v1` — raw validated events
| Column | Type | Description |
|---|---|---|
| `event_id` | STRING | GitHub event ID |
| `event_type` | STRING | PushEvent / PullRequestEvent / etc. |
| `actor_login` | STRING | GitHub username |
| `repo_name` | STRING | owner/repo |
| `created_at` | TIMESTAMP | Event creation time |
| `raw_payload` | STRING | Full JSON payload |
| `kafka_ingest_ts` | TIMESTAMP | Kafka ingestion time |

### `repo_activity_5m` — 5-minute tumbling window aggregation
| Column | Type | Description |
|---|---|---|
| `window_start` | TIMESTAMP | Window start |
| `window_end` | TIMESTAMP | Window end |
| `repo_name` | STRING | owner/repo |
| `event_count` | LONG | Events in window |
| `event_types` | ARRAY[STRING] | Distinct event types |

---

## Technical Challenges & Solutions

### 1. Databricks Serverless — trigger limitation
Databricks Serverless compute does not support `processingTime` triggers for Structured Streaming.

```python
# ❌ Fails on Serverless: INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED
.trigger(processingTime="30 seconds")

# ✅ Solution: availableNow — processes all backlog then exits;
#    Continuous Job mode restarts immediately, achieving near-realtime behavior
.trigger(availableNow=True)
```

### 2. Shaded Kafka connector on Databricks
Databricks bundles a shaded version of the Kafka client. Using the standard class path causes a `LoginModule not found` exception.

```python
# ❌ Standard path fails
"org.apache.kafka.common.security.scram.ScramLoginModule required ..."

# ✅ Must use shaded prefix
"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required ..."
```

### 3. ETag-based deduplication
GitHub's API supports conditional requests via `If-None-Match`. When no new events exist, the server returns `304 Not Modified` — saving both API quota and parse overhead. Combined with `last_event_id` tracking, duplicate events across paginated responses are eliminated.

```python
if page == 1 and self.state.etag:
    headers["If-None-Match"] = self.state.etag   # → 304 if unchanged
```

### 4. Watermark + append-mode window aggregation
Using `outputMode("append")` with a watermark ensures only fully-closed windows are written to Delta, avoiding repeated updates and making downstream reads stable.

```python
events_validated
    .withWatermark("created_at", "10 minutes")   # tolerate 10min late arrivals
    .groupBy(F.window("created_at", "5 minutes"), F.col("repo_name"))
    .agg(...)
    .writeStream
    .outputMode("append")   # emit only after window is closed
```

---

## Project Structure

```
gitstream/
├── poller/
│   ├── github_poller.py        # Main polling loop
│   ├── utils/
│   │   ├── dedup.py            # ETag + last_event_id state manager
│   │   └── backoff.py          # Truncated exponential backoff with jitter
│   └── Dockerfile
├── databricks/
│   ├── notebooks/
│   │   └── github_events_streaming.py   # Spark Structured Streaming job
│   ├── job_settings.example.yml         # Databricks Asset Bundle template
│   └── job_settings.example.json        # Databricks Job JSON template
├── dags/
│   └── github_events_dag.py    # Airflow pipeline health monitor
├── airflow/
│   └── Dockerfile              # Airflow + confluent-kafka
├── config/
│   └── kafka_config.py         # Shared Kafka config constants
├── tests/
│   ├── test_dedup.py           # EventState unit tests
│   └── test_backoff.py         # Backoff utility unit tests
├── .github/
│   └── workflows/ci.yml        # GitHub Actions: lint + test
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── .env.example
```

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- [Redpanda Cloud](https://cloud.redpanda.com) account (free Serverless tier)
- [Databricks](https://databricks.com) workspace
- GitHub Personal Access Token

### 1. Configure environment

```bash
cp .env.example .env
# Fill in GITHUB_TOKEN, KAFKA_* credentials
```

### 2. Start the poller and Airflow

```bash
docker compose up -d
```

Airflow UI → http://localhost:8080 → enable `github_events_pipeline` DAG

### 3. Deploy Databricks streaming job

```bash
cp databricks/job_settings.example.yml databricks/job_settings.yml
# Fill in notebook path, Redpanda credentials, Volume paths

databricks bundle deploy   # requires Databricks CLI v2
```

Or paste `job_settings.yml` into Databricks UI → Workflows → Edit as YAML.

### 4. Verify data

```sql
-- In a Databricks notebook
SELECT event_type, COUNT(*) as cnt
FROM delta.`/Volumes/workspace/default/github_events/delta/events_v1`
GROUP BY event_type
ORDER BY cnt DESC
```

---

## Airflow Monitoring

The `github_events_pipeline` DAG runs every 10 minutes and checks:

1. **`check_kafka_topic`** — Verifies the Redpanda topic exists and has partitions
2. **`check_topic_offsets`** — Asserts total message count > 0 (poller is alive)
3. **`check_databricks_job`** — Confirms the Databricks Streaming Job is `RUNNING`
4. **`log_pipeline_status`** — Logs final OK timestamp

---

## CI/CD

GitHub Actions runs on every push and pull request to `main`:

### CI
```
lint-and-test
├── ruff check .       (code quality)
└── pytest tests/ -v   (11 unit tests)
```

### CD
Triggered automatically when CI passes on `main`:

```
build-and-push
└── Build Docker image → push to GitHub Container Registry (GHCR)

deploy-ec2
└── SSH into EC2 → docker compose pull poller → docker compose up -d poller

deploy-databricks
└── databricks bundle deploy → update Databricks streaming job settings
```
