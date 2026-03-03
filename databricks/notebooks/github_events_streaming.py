"""
GitHub Events Structured Streaming
====================================
DLT 없이 순수 Structured Streaming + Delta Lake 로 동일한 파이프라인 구현.
Databricks Standard 티어에서 동작한다.

실행 방법:
  Databricks Job (Continuous) 로 이 노트북을 등록하면
  DLT 와 동일하게 항상 실행 상태를 유지한다.

DLT 와의 기능 대응:
  @dlt.table          →  writeStream.format("delta")
  @dlt.view           →  중간 DataFrame (저장 없음, 메모리 변환)
  @dlt.expect_or_drop →  .filter() 로 수동 품질 검증
  DLT pipeline config →  dbutils.widgets (Job 파라미터로 주입)
  DLT checkpoint      →  .option("checkpointLocation", ...)

────────────────────────────────────────────────────────────────────────────────
Databricks Job 파라미터 (job_settings.json 참고):
  kafka_bootstrap_servers  예) broker1:9092,broker2:9092
  kafka_topic              예) github.raw.events
  kafka_starting_offsets   예) latest
  checkpoint_base          예) dbfs:/checkpoints/github-events
  delta_base               예) dbfs:/delta/github-events
────────────────────────────────────────────────────────────────────────────────
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ── 파라미터 (Databricks Widgets → Job 파라미터로 주입) ──────────────────────── #
# Job 설정에서 base_parameters 로 넘겨주면 아래 widget 이 값을 받는다.
# 노트북 단독 실행 시에는 기본값이 사용된다.

dbutils.widgets.text("kafka_bootstrap_servers", "seed-xxxx.any.us-east-1.mpx.prd.cloud.redpanda.com:9092")
dbutils.widgets.text("kafka_topic",             "github.raw.events")
dbutils.widgets.text("kafka_starting_offsets",  "latest")
dbutils.widgets.text("kafka_sasl_username",     "")   # Redpanda username
dbutils.widgets.text("kafka_sasl_password",     "")   # Redpanda password
dbutils.widgets.text("checkpoint_base",         "dbfs:/checkpoints/github-events")
dbutils.widgets.text("delta_base",              "dbfs:/delta/github-events")

KAFKA_BOOTSTRAP    = dbutils.widgets.get("kafka_bootstrap_servers")
KAFKA_TOPIC        = dbutils.widgets.get("kafka_topic")
KAFKA_OFFSETS      = dbutils.widgets.get("kafka_starting_offsets")
KAFKA_SASL_USER    = dbutils.widgets.get("kafka_sasl_username")
KAFKA_SASL_PASS    = dbutils.widgets.get("kafka_sasl_password")
CHECKPOINT_BASE    = dbutils.widgets.get("checkpoint_base")
DELTA_BASE         = dbutils.widgets.get("delta_base")

# 경로 상수
PATH_EVENTS_V1        = f"{DELTA_BASE}/events_v1"
PATH_REPO_ACTIVITY    = f"{DELTA_BASE}/repo_activity_5m"
CKPT_EVENTS_V1        = f"{CHECKPOINT_BASE}/events_v1"
CKPT_REPO_ACTIVITY    = f"{CHECKPOINT_BASE}/repo_activity_5m"

# ── 이벤트 타입 화이트리스트 ─────────────────────────────────────────────────── #
ALLOWED_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "IssuesEvent",
]

# ── 파싱 스키마 ──────────────────────────────────────────────────────────────── #
GITHUB_EVENT_SCHEMA = StructType([
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
    StructField("created_at", StringType(), nullable=True),
])


# ════════════════════════════════════════════════════════════════════════════════
# STEP 1 — Kafka 스트리밍 읽기
#
# DLT 의 github_raw_kafka(@dlt.table) 대신 DataFrame 으로 직접 읽는다.
# checkpointLocation 이 Kafka offset 상태를 관리한다 → DLT checkpoint 역할.
# ════════════════════════════════════════════════════════════════════════════════

_kafka_opts = {
    "kafka.bootstrap.servers":  KAFKA_BOOTSTRAP,
    "subscribe":                KAFKA_TOPIC,
    "startingOffsets":          KAFKA_OFFSETS,
    "maxOffsetsPerTrigger":     "50000",
    "failOnDataLoss":           "false",
}
# Redpanda Cloud SASL 인증 — SCRAM-SHA-256 (username 이 있을 때만 활성화)
if KAFKA_SASL_USER:
    _kafka_opts.update({
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism":    "SCRAM-SHA-256",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{KAFKA_SASL_USER}" password="{KAFKA_SASL_PASS}";'
        ),
    })

raw_stream = (
    spark.readStream.format("kafka")
    .options(**_kafka_opts)
    .load()
    .select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ingest_ts"),
    )
)


# ════════════════════════════════════════════════════════════════════════════════
# STEP 2 — 파싱 + 정규화  (메모리 변환, Delta 저장 없음)
#
# DLT 의 @dlt.view 와 동일한 역할.
# DataFrame 변환이라 별도 저장 없이 다음 단계로 바로 전달된다.
# ════════════════════════════════════════════════════════════════════════════════

events_parsed = (
    raw_stream
    .select(
        F.from_json("raw_json", GITHUB_EVENT_SCHEMA).alias("e"),
        F.col("raw_json").alias("raw_payload"),
        F.col("kafka_ingest_ts"),
    )
    .select(
        F.col("e.id").alias("event_id"),
        F.col("e.type").alias("event_type"),
        F.col("e.actor.login").alias("actor_login"),
        F.col("e.repo.name").alias("repo_name"),
        F.to_timestamp("e.created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("created_at"),
        F.col("raw_payload"),
        F.col("kafka_ingest_ts"),
    )
)


# ════════════════════════════════════════════════════════════════════════════════
# STEP 3 — 품질 검증  (DLT @dlt.expect_or_drop 대체)
#
# DLT expectations 는 메트릭 UI 를 제공하지만 유료.
# 여기서는 .filter() 로 동일한 드롭 동작을 구현한다.
# 드롭된 행 수는 별도 모니터링이 필요하다면 foreachBatch + 카운터로 추가 가능.
# ════════════════════════════════════════════════════════════════════════════════

events_validated = (
    events_parsed
    .filter(F.col("event_id").isNotNull())                      # event_id null 금지
    .filter(F.col("created_at").isNotNull())                    # created_at null 금지
    .filter(F.col("event_type").isin(ALLOWED_EVENT_TYPES))      # 타입 화이트리스트
)


# ════════════════════════════════════════════════════════════════════════════════
# STEP 4 — events_v1 Delta 테이블로 쓰기
#
# trigger(availableNow) : Serverless 클러스터 전용. 현재 Kafka에 쌓인 데이터를
#                         모두 처리하고 완료. Job(Continuous)이 즉시 재시작해
#                         processingTime 과 유사한 near-realtime 동작을 만든다.
# checkpointLocation    : Kafka offset + 스트리밍 상태 저장 (재시작 시 이어서 처리)
# ════════════════════════════════════════════════════════════════════════════════

query_events_v1 = (
    events_validated
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", CKPT_EVENTS_V1)
    .option("mergeSchema", "true")          # 스키마 진화 허용
    .start(PATH_EVENTS_V1)
)

print(f"[events_v1] 스트리밍 시작  →  {PATH_EVENTS_V1}")


# ════════════════════════════════════════════════════════════════════════════════
# STEP 5 — repo_activity_5m  5분 텀블링 윈도우 집계
#
# withWatermark: 지연 도착 허용 범위 + 상태 정리 기준 (메모리 누수 방지)
# outputMode("append"): watermark 기준으로 확정된 윈도우만 append
#                       → 다운스트림 쿼리가 안정적으로 읽을 수 있다
# ════════════════════════════════════════════════════════════════════════════════

repo_activity_stream = (
    events_validated
    .withWatermark("created_at", "10 minutes")
    .groupBy(
        F.window("created_at", "5 minutes"),
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

query_repo_activity = (
    repo_activity_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", CKPT_REPO_ACTIVITY)
    .start(PATH_REPO_ACTIVITY)
)

print(f"[repo_activity_5m] 스트리밍 시작  →  {PATH_REPO_ACTIVITY}")


# ════════════════════════════════════════════════════════════════════════════════
# STEP 6 — 두 스트림 동시 실행 유지
#
# awaitAnyTermination(): 어느 한 쪽 스트림이 종료되거나 오류가 날 때까지 블로킹.
# Databricks Job (Continuous) 이 이 상태를 감지해 자동 재시작한다.
# ════════════════════════════════════════════════════════════════════════════════

print("두 스트리밍 쿼리 실행 중 — 종료 신호 대기 …")
spark.streams.awaitAnyTermination()
