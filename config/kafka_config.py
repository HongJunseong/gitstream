"""
Shared Kafka configuration constants.

Import these dicts in any producer/consumer in the project so that
Kafka settings are defined in one place.
"""

import os

# ── Producer ───────────────────────────────────────────────────────────────── #

PRODUCER_CONFIG: dict = {
    "bootstrap.servers":  os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "acks":               "all",
    "enable.idempotence": True,
    "retries":            5,
    "linger.ms":         200,
    "compression.type":  "gzip",
}

# ── Consumer ───────────────────────────────────────────────────────────────── #

CONSUMER_CONFIG: dict = {
    "bootstrap.servers":  os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id":           os.getenv("KAFKA_CONSUMER_GROUP", "github-events-consumer"),
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": False,   # commit manually for at-least-once delivery
}

# ── Topics ─────────────────────────────────────────────────────────────────── #

TOPIC_RAW_EVENTS: str = os.getenv("KAFKA_TOPIC", "github.raw.events")
