"""
GitHub Public Events Poller
────────────────────────────
Polls the GitHub /events REST endpoint and produces JSON records to Kafka.

Design decisions:
  - ETag      : send If-None-Match header → GitHub returns 304 when nothing changed.
  - last_event_id : skip already-seen events when content did change (multi-page safe).
  - X-Poll-Interval : honour the server-recommended polling interval.
  - Rate-limit (403/429) : wait until X-RateLimit-Reset before retrying.
  - Exponential backoff : network/5xx errors get jittered retries.
  - Graceful shutdown : SIGTERM/SIGINT flush Kafka producer before exit.
"""

import json
import logging
import os
import signal
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests
from confluent_kafka import Producer

from utils.backoff import calc_backoff
from utils.dedup import EventState

# ── Logging ────────────────────────────────────────────────────────────────── #

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("github_poller")

# ── Configuration (all from environment) ───────────────────────────────────── #

GITHUB_TOKEN: str          = os.getenv("GITHUB_TOKEN", "")
GITHUB_API_BASE: str       = "https://api.github.com"
KAFKA_BOOTSTRAP_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str           = os.getenv("KAFKA_TOPIC", "github.raw.events")
KAFKA_SASL_USERNAME: str   = os.getenv("KAFKA_SASL_USERNAME", "")   # Redpanda username
KAFKA_SASL_PASSWORD: str   = os.getenv("KAFKA_SASL_PASSWORD", "")   # Redpanda password
KAFKA_SECURITY_PROTOCOL    = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISM       = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
POLL_INTERVAL: int         = int(os.getenv("POLL_INTERVAL", "60"))
MAX_RETRIES: int           = int(os.getenv("MAX_RETRIES", "5"))
MAX_PAGES: int             = int(os.getenv("MAX_PAGES", "3"))   # 3 × 100 = 300 events max
PER_PAGE: int              = 100


# ── Poller ─────────────────────────────────────────────────────────────────── #

class GithubPoller:
    def __init__(self) -> None:
        self.state    = EventState()
        self.producer = self._build_producer()
        self.session  = self._build_session()
        self._alive   = True

        # Graceful shutdown
        signal.signal(signal.SIGTERM, self._on_shutdown)
        signal.signal(signal.SIGINT,  self._on_shutdown)

    # ── Lifecycle ──────────────────────────────────────────────────────────── #

    def _on_shutdown(self, signum, _frame) -> None:
        logger.info("Received signal %s — shutting down …", signum)
        self._alive = False

    def run(self) -> None:
        logger.info(
            "Poller started  topic=%s  bootstrap=%s",
            KAFKA_TOPIC,
            KAFKA_BOOTSTRAP_SERVERS,
        )
        while self._alive:
            try:
                wait = self._poll_once()
            except Exception:
                logger.exception("Unexpected error in poll cycle.")
                wait = POLL_INTERVAL

            logger.info("Next poll in %ds", wait)
            self._interruptible_sleep(wait)

        logger.info("Flushing Kafka producer …")
        self.producer.flush(timeout=10)
        logger.info("Poller stopped.")

    def _interruptible_sleep(self, seconds: int) -> None:
        """Sleep in 1s ticks so SIGTERM is handled promptly."""
        for _ in range(seconds):
            if not self._alive:
                break
            time.sleep(1)

    # ── Core poll cycle ────────────────────────────────────────────────────── #

    def _poll_once(self) -> int:
        """Fetch new events, produce to Kafka. Returns recommended next interval."""
        all_new: List[Dict] = []
        next_interval = POLL_INTERVAL

        for page in range(1, MAX_PAGES + 1):
            events, headers = self._fetch_page(page)
            next_interval = int(headers.get("X-Poll-Interval", next_interval))

            # 304 Not Modified → nothing to do
            if events is None:
                return next_interval

            # Persist ETag only from the first page
            if page == 1 and (etag := headers.get("ETag")):
                self.state.etag = etag

            new = self.state.filter_new_events(events)
            all_new.extend(new)

            # If this page contained already-seen events, no need to go deeper
            if len(new) < len(events):
                break

        if all_new:
            # events are newest-first; all_new[0] is the most recent
            self.state.last_event_id = str(all_new[0]["id"])
            logger.info("Producing %d new event(s) to Kafka.", len(all_new))
            self._produce(all_new)
        else:
            logger.info("No new events.")

        return next_interval

    # ── GitHub HTTP ────────────────────────────────────────────────────────── #

    def _fetch_page(self, page: int) -> Tuple[Optional[List[Dict]], Dict]:
        """
        Fetch one page of /events.

        Returns (events, response_headers) or (None, headers) on 304/error.
        Handles rate-limits and retries with backoff.
        """
        url     = f"{GITHUB_API_BASE}/events"
        params  = {"per_page": PER_PAGE, "page": page}
        headers: Dict[str, str] = {}

        # Attach ETag only on the first page to enable 304 short-circuit
        if page == 1 and self.state.etag:
            headers["If-None-Match"] = self.state.etag

        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(
                    url, params=params, headers=headers, timeout=10
                )
                resp_headers = dict(resp.headers)

                if resp.status_code == 304:
                    logger.debug("304 Not Modified (page %d).", page)
                    return None, resp_headers

                # Rate limit exhausted
                if resp.status_code in (403, 429):
                    self._handle_rate_limit(resp_headers)
                    continue

                resp.raise_for_status()
                return resp.json(), resp_headers

            except requests.RequestException as exc:
                if attempt == MAX_RETRIES - 1:
                    logger.error("All %d retries exhausted: %s", MAX_RETRIES, exc)
                    return None, {}
                delay = calc_backoff(attempt)
                logger.warning(
                    "Request error (attempt %d/%d): %s — retry in %.2fs",
                    attempt + 1, MAX_RETRIES, exc, delay,
                )
                time.sleep(delay)

        return None, {}

    @staticmethod
    def _handle_rate_limit(headers: Dict) -> None:
        """Block until GitHub's rate-limit window resets."""
        reset_ts = int(headers.get("X-RateLimit-Reset", time.time() + 60))
        wait     = max(reset_ts - int(time.time()), 1)
        logger.warning("Rate limit hit. Waiting %ds until reset.", wait)
        time.sleep(wait)

    # ── Kafka ──────────────────────────────────────────────────────────────── #

    def _produce(self, events: List[Dict]) -> None:
        for event in events:
            self.producer.produce(
                topic    = KAFKA_TOPIC,
                key      = str(event["id"]).encode(),
                value    = json.dumps(event, ensure_ascii=False).encode(),
                callback = self._on_delivery,
            )
        # Block until all messages are acknowledged
        self.producer.flush()

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            logger.error("Kafka delivery error: %s", err)
        else:
            logger.debug(
                "Delivered → topic=%s partition=%d offset=%d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    # ── Builders ───────────────────────────────────────────────────────────── #

    @staticmethod
    def _build_producer() -> Producer:
        conf: dict = {
            "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
            "acks":               "all",
            "enable.idempotence": True,
            "retries":            5,
            "linger.ms":         200,
            "compression.type":  "gzip",
        }
        # Redpanda SASL 인증 — USERNAME 이 설정된 경우에만 활성화
        # Redpanda Cloud: SASL_SSL + SCRAM-SHA-256
        if KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD:
            conf.update(
                {
                    "security.protocol": KAFKA_SECURITY_PROTOCOL,
                    "sasl.mechanisms":   KAFKA_SASL_MECHANISM,
                    "sasl.username":     KAFKA_SASL_USERNAME,
                    "sasl.password":     KAFKA_SASL_PASSWORD,
                }
            )
        return Producer(conf)

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Accept":               "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                **({"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}),
            }
        )
        return session


# ── Entry point ────────────────────────────────────────────────────────────── #

if __name__ == "__main__":
    GithubPoller().run()
