"""
DAG: github_events_pipeline
────────────────────────────
Runs every 10 minutes to:
  1. Verify the Kafka topic exists and is healthy.
  2. Check consumer-group lag (stub — wire up to Burrow / Prometheus in prod).
  3. Log pipeline status.

The poller container runs independently; this DAG provides observability
and is the entry-point for wiring downstream transformations (e.g. Spark,
dbt, ClickHouse ingestion).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry":   False,
}


def _check_kafka_topic(**context) -> None:
    """Assert the raw-events topic exists and push its partition count to XCom."""
    from confluent_kafka.admin import AdminClient

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic     = os.getenv("KAFKA_TOPIC", "github.raw.events")

    admin  = AdminClient({"bootstrap.servers": bootstrap})
    topics = admin.list_topics(timeout=15)

    if topic not in topics.topics:
        raise RuntimeError(f"Topic '{topic}' not found — is the Kafka broker running?")

    n_partitions = len(topics.topics[topic].partitions)
    context["ti"].xcom_push(key="partition_count", value=n_partitions)
    print(f"[OK] Topic '{topic}' found with {n_partitions} partition(s).")


def _check_consumer_lag(**_context) -> None:
    """
    Stub: log consumer-group lag.

    Production options:
      - Poll `kafka-consumer-groups.sh --describe` via BashOperator
      - Query Burrow REST API
      - Read from a Prometheus / Grafana datasource via the HTTP provider
    """
    print(
        "Consumer lag check: integrate with Burrow or kafka-consumer-groups "
        "for production monitoring."
    )


with DAG(
    dag_id           = "github_events_pipeline",
    description      = "Kafka pipeline health check and observability for GitHub Events",
    schedule         = timedelta(minutes=10),
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    default_args     = default_args,
    max_active_runs  = 1,
    tags             = ["github", "kafka", "events"],
) as dag:

    check_topic = PythonOperator(
        task_id         = "check_kafka_topic",
        python_callable = _check_kafka_topic,
    )

    check_lag = PythonOperator(
        task_id         = "check_consumer_lag",
        python_callable = _check_consumer_lag,
    )

    log_status = BashOperator(
        task_id      = "log_pipeline_status",
        bash_command = 'echo "Pipeline OK at $(date -u +%Y-%m-%dT%H:%M:%SZ)"',
    )

    check_topic >> check_lag >> log_status
