"""
GitHub Events DLT Pipeline
===========================
Kafka → Bronze → Silver (events_v1) → Gold (repo_activity_5m)

레이어 전략:
  Bronze  github_raw_kafka   : Kafka 원본 바이트 → string 보존
  Silver  events_v1          : JSON 파싱 + 스키마 정규화 + 품질 검증
  Gold    repo_activity_5m   : 5분 텀블링 윈도우 집계 (repo 별 이벤트 수)

────────────────────────────────────────────────────────────────────────────────
Databricks DLT 파이프라인 설정에서 아래 Configuration 키를 반드시 지정하세요.
  pipeline.kafka.bootstrap_servers  예) broker1:9092,broker2:9092
  pipeline.kafka.topic              예) github.raw.events
  pipeline.kafka.starting_offsets   예) latest  (최초 실행 시 earliest 권장)
────────────────────────────────────────────────────────────────────────────────
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ── Pipeline 파라미터 ────────────────────────────────────────────────────────── #
# DLT 파이프라인 UI → "Configuration" 탭에서 설정하거나 REST API/Terraform으로 주입.
# spark.conf.get(key, default) 형식으로 읽는다.

KAFKA_BOOTSTRAP_SERVERS: str = spark.conf.get(  # noqa: F821 (spark는 DLT 런타임 제공)
    "pipeline.kafka.bootstrap_servers",
    "localhost:9092",       # ← 로컬 테스트용 기본값; 운영 시 반드시 override
)
KAFKA_TOPIC: str = spark.conf.get(
    "pipeline.kafka.topic",
    "github.raw.events",    # ← Kafka 토픽명
)
KAFKA_STARTING_OFFSETS: str = spark.conf.get(
    "pipeline.kafka.starting_offsets",
    "latest",               # ← 최초 풀 수집이 필요하면 "earliest"로 변경
)

# ── 이벤트 타입 화이트리스트 ─────────────────────────────────────────────────── #
# Expectation 문자열에서 SQL IN 절로 그대로 삽입된다.
_ALLOWED_EVENT_TYPES = (
    "PushEvent",
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "IssuesEvent",
)

# ── 파싱 스키마 ──────────────────────────────────────────────────────────────── #
# GitHub /events 응답 중 우리가 추출할 필드만 선언한다.
# 나머지 필드는 raw_payload(원본 JSON)에 보존된다.
_GITHUB_EVENT_SCHEMA = StructType(
    [
        StructField("id",   StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField(
            "actor",
            StructType([StructField("login", StringType(), nullable=True)]),
            nullable=True,
        ),
        StructField(
            "repo",
            StructType([StructField("name", StringType(), nullable=True)]),
            nullable=True,
        ),
        # GitHub API는 UTC ISO-8601 문자열로 반환 ("2024-01-15T12:34:56Z")
        StructField("created_at", StringType(), nullable=True),
    ]
)


# ════════════════════════════════════════════════════════════════════════════════
# BRONZE — github_raw_kafka
# Kafka 원본 메시지를 Delta에 그대로 착지(landing)시킨다.
# 스키마 변경·재처리를 대비해 원본 JSON을 항상 보존한다.
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="github_raw_kafka",
    comment="Bronze: Kafka github.raw.events 원본 메시지. 재처리 기준점.",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
    },
)
def github_raw_kafka():
    return (
        spark.readStream.format("kafka")  # noqa: F821
        # ── Kafka 연결 설정 ────────────────────────────────────────────────── #
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        # ── 처리량 / 안정성 튜닝 ──────────────────────────────────────────── #
        .option("maxOffsetsPerTrigger", 50_000)   # 마이크로배치당 최대 오프셋 수
        .option("failOnDataLoss", "false")         # Kafka 로그 컴팩션 허용
        .load()
        .select(
            # Kafka value 바이트를 UTF-8 문자열로 변환
            F.col("value").cast("string").alias("raw_json"),
            # Kafka 메타데이터 — 디버깅·재처리 추적용
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ingest_ts"),
        )
    )


# ════════════════════════════════════════════════════════════════════════════════
# SILVER — events_v1
# Bronze의 raw JSON을 파싱하고 정규화된 스키마로 변환한다.
# Expectations로 품질이 보장된 행만 Delta에 저장된다.
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="events_v1",
    comment="Silver: 정규화된 GitHub 이벤트. 품질 검증 통과 행만 저장.",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
# ── Expectations (데이터 품질 규칙) ──────────────────────────────────────────── #
# expect_or_drop: 위반 행을 조용히 제거하고 DLT 메트릭에 집계한다.
# expect_or_fail: 위반 시 파이프라인 전체를 중단한다. (강결합 보장이 필요할 때 사용)
@dlt.expect_or_drop("event_id_not_null",  "event_id IS NOT NULL")
@dlt.expect_or_drop("created_at_not_null", "created_at IS NOT NULL")
@dlt.expect_or_drop(
    "valid_event_type",
    # Python tuple repr → SQL IN ('a','b','c') 형식으로 자동 변환
    f"event_type IN {_ALLOWED_EVENT_TYPES!r}",
)
def events_v1():
    """
    컬럼 매핑:
      event_id    ← .id
      event_type  ← .type
      actor_login ← .actor.login
      repo_name   ← .repo.name
      created_at  ← .created_at  (ISO-8601 → TimestampType)
      raw_payload ← 원본 JSON 문자열 (스키마 진화 대비 보존)
    """
    return (
        dlt.read_stream("github_raw_kafka")   # Bronze 스트리밍 읽기
        .select(
            F.from_json("raw_json", _GITHUB_EVENT_SCHEMA).alias("e"),
            F.col("raw_json").alias("raw_payload"),
            F.col("kafka_ingest_ts"),
        )
        .select(
            F.col("e.id").alias("event_id"),
            F.col("e.type").alias("event_type"),
            F.col("e.actor.login").alias("actor_login"),
            F.col("e.repo.name").alias("repo_name"),
            # GitHub API 타임스탬프 형식: "2024-01-15T12:34:56Z" (항상 UTC)
            F.to_timestamp("e.created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("created_at"),
            F.col("raw_payload"),
            F.col("kafka_ingest_ts"),
        )
    )


# ════════════════════════════════════════════════════════════════════════════════
# GOLD — repo_activity_5m
# events_v1을 5분 텀블링 윈도우로 집계한다.
# 이벤트 시간(created_at) 기준이며 워터마크로 지연 도착 데이터를 처리한다.
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="repo_activity_5m",
    comment=(
        "Gold: repo 별 5분 텀블링 윈도우 이벤트 집계. "
        "워터마크 10분 — 10분 이상 지연 도착한 이벤트는 드롭된다."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def repo_activity_5m():
    """
    출력 컬럼:
      window_start  ← 5분 버킷 시작 시각 (이벤트 시간 기준)
      window_end    ← 5분 버킷 종료 시각
      repo_name     ← 저장소 전체 이름 (예: "torvalds/linux")
      event_count   ← 버킷 내 이벤트 수
      event_types   ← 버킷 내 등장한 이벤트 타입 목록 (중복 제거 배열)

    워터마크 동작:
      created_at 기준으로 워터마크 10분을 설정한다.
      윈도우 종료 후 10분이 지난 시점에 해당 윈도우 상태가 확정·방출된다.
      10분 이상 늦게 도착한 이벤트는 집계에서 제외된다.
    """
    return (
        dlt.read_stream("events_v1")
        # 워터마크: 지연 도착 허용 범위 및 상태 정리 기준
        .withWatermark("created_at", "10 minutes")
        .groupBy(
            F.window("created_at", "5 minutes"),  # 5분 텀블링 윈도우
            F.col("repo_name"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.collect_set("event_type").alias("event_types"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("repo_name"),
            F.col("event_count"),
            F.col("event_types"),
        )
    )
