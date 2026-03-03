"""
DAG: github_events_pipeline
────────────────────────────
Runs every 10 minutes to:
  1. Verify the Kafka topic exists and has partitions.
  2. Check total message count in the topic (low/high watermark offsets).
  3. Verify the Databricks Streaming Job is actively running.
  4. Log pipeline status.
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

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    topic     = os.getenv("KAFKA_TOPIC", "github.raw.events")
    username  = os.getenv("KAFKA_SASL_USERNAME", "")
    password  = os.getenv("KAFKA_SASL_PASSWORD", "")
    protocol  = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    mechanism = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")

    conf = {"bootstrap.servers": bootstrap}
    if username and password:
        conf.update({
            "security.protocol": protocol,
            "sasl.mechanisms":   mechanism,
            "sasl.username":     username,
            "sasl.password":     password,
        })

    admin  = AdminClient(conf)
    topics = admin.list_topics(timeout=15)

    if topic not in topics.topics:
        raise RuntimeError(f"Topic '{topic}' not found — is the Kafka broker running?")

    n_partitions = len(topics.topics[topic].partitions)
    context["ti"].xcom_push(key="partition_count", value=n_partitions)
    print(f"[OK] Topic '{topic}' found with {n_partitions} partition(s).")


def _check_topic_offsets(**context) -> None:
    """Check total message count in the topic via watermark offsets (high - low)."""
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    topic     = os.getenv("KAFKA_TOPIC", "github.raw.events")
    username  = os.getenv("KAFKA_SASL_USERNAME", "")
    password  = os.getenv("KAFKA_SASL_PASSWORD", "")
    protocol  = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    mechanism = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")

    base_conf = {"bootstrap.servers": bootstrap}
    if username and password:
        base_conf.update({
            "security.protocol": protocol,
            "sasl.mechanisms":   mechanism,
            "sasl.username":     username,
            "sasl.password":     password,
        })

    # Get partition list
    admin    = AdminClient(base_conf)
    metadata = admin.list_topics(topic=topic, timeout=15)
    partitions = [
        TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions.keys()
    ]

    # Get watermark offsets
    consumer = Consumer({**base_conf, "group.id": "airflow-monitor"})
    total_messages = 0
    for tp in partitions:
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        total_messages += max(0, high - low)
    consumer.close()

    context["ti"].xcom_push(key="total_messages", value=total_messages)
    print(f"[OK] Topic '{topic}' has {total_messages:,} messages across {len(partitions)} partition(s).")

    if total_messages == 0:
        raise RuntimeError(f"Topic '{topic}' has 0 messages — poller may not be running!")


def _check_databricks_job(**context) -> None:
    """Check the Databricks Streaming Job is actively running via REST API."""
    import requests

    host     = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    token    = os.getenv("DATABRICKS_TOKEN", "")
    job_name = os.getenv("DATABRICKS_JOB_NAME", "github-events-streaming")

    if not host or not token:
        print("DATABRICKS_HOST or DATABRICKS_TOKEN not set — skipping check.")
        return

    headers = {"Authorization": f"Bearer {token}"}

    # Find job by name
    resp = requests.get(
        f"{host}/api/2.1/jobs/list",
        headers=headers,
        params={"name": job_name},
        timeout=15,
    )
    resp.raise_for_status()
    jobs = resp.json().get("jobs", [])

    if not jobs:
        raise RuntimeError(f"Databricks Job '{job_name}' not found!")

    job_id = jobs[0]["job_id"]

    # Get latest active run
    resp = requests.get(
        f"{host}/api/2.1/jobs/runs/list",
        headers=headers,
        params={"job_id": job_id, "limit": 1, "active_only": True},
        timeout=15,
    )
    resp.raise_for_status()
    runs = resp.json().get("runs", [])

    if not runs:
        raise RuntimeError(f"No active runs for Job '{job_name}' — is the Job paused?")

    life_cycle = runs[0]["state"].get("life_cycle_state", "UNKNOWN")
    context["ti"].xcom_push(key="job_state", value=life_cycle)
    print(f"[OK] Databricks Job '{job_name}' — state: {life_cycle}")

    if life_cycle not in ("RUNNING", "PENDING"):
        raise RuntimeError(f"Job '{job_name}' is not running: {life_cycle}")


with DAG(
    dag_id           = "github_events_pipeline",
    description      = "Kafka + Databricks pipeline health check for GitHub Events",
    schedule         = timedelta(minutes=10),
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    default_args     = default_args,
    max_active_runs  = 1,
    tags             = ["github", "kafka", "databricks", "events"],
) as dag:

    check_topic = PythonOperator(
        task_id         = "check_kafka_topic",
        python_callable = _check_kafka_topic,
    )

    check_offsets = PythonOperator(
        task_id         = "check_topic_offsets",
        python_callable = _check_topic_offsets,
    )

    check_databricks = PythonOperator(
        task_id         = "check_databricks_job",
        python_callable = _check_databricks_job,
    )

    log_status = BashOperator(
        task_id      = "log_pipeline_status",
        bash_command = 'echo "Pipeline OK at $(date -u +%Y-%m-%dT%H:%M:%SZ)"',
    )

    check_topic >> check_offsets >> check_databricks >> log_status
