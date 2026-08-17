"""
Kafka → ScyllaDB consumer.

Config-driven: reads <jobs_repo>/scylla-sink-targets.json (written by the backend's
scylla_sink_config.py whenever a pipeline with a ScyllaDB sink node is run) to learn which
Kafka topic(s) feed which ScyllaDB table(s), and how source fields map onto sink columns.
Reloads that file every CONFIG_RELOAD_INTERVAL_S seconds, so new/changed pipelines are
picked up without restarting this process — no topic/table is hardcoded here.

Batches up to BATCH_SIZE records per target table, or flushes after FLUSH_INTERVAL_S
seconds, whichever comes first. Uses execute_concurrent_with_args for parallelism.

Every 60 seconds a summary is logged:
  - Records read from Kafka / written to ScyllaDB / errors
  - Kafka consumer lag (ms between message creation and processing)
  - ScyllaDB batch flush latency

Logs are written to logs/scylla-consumer.log and rotated at 10 MB
(10 backups = 100 MB max on disk). Set LOG_DIR / LOG_MAX_BYTES /
LOG_BACKUP_COUNT env vars to override.

ClickHouse note: ClickHouse writes are handled inside ClickHouse itself (a Kafka-engine
table + materialized view, set up by the backend's clickhouse_sink.py) — not this process.
"""
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy
from confluent_kafka import Consumer, KafkaError, TIMESTAMP_CREATE_TIME

from log_setup import configure_logging
from metrics import MetricsReporter

log = configure_logging("scylla-consumer")

CONFIG_PATH = os.getenv("CONFIG_PATH", str(Path(__file__).parent.parent / "config.yaml"))

with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

kfk_cfg = cfg["kafka"]
sc_cfg  = cfg["scylladb"]

BOOTSTRAP   = kfk_cfg["bootstrap_servers"]
CONTACT_PTS = sc_cfg["contact_points"]
PORT        = sc_cfg.get("port", 9042)

TARGETS_FILENAME     = "scylla-sink-targets.json"
BATCH_SIZE            = 500   # flush a table's batch when this many records accumulate
FLUSH_INTERVAL_S      = 1.0   # also flush after this many seconds with pending records
SUMMARY_INTERVAL_S    = 60    # how often to emit the 1-minute activity summary
CONFIG_RELOAD_INTERVAL_S = 30 # how often to re-read scylla-sink-targets.json


def _resolve_jobs_repo() -> Path:
    """Mirrors backend/flink_job_generator.py's resolve_jobs_repo(): env JOBS_REPO_PATH ->
    config.yaml jobs_repo.path -> project root. Duplicated rather than imported since this
    process ships independently of backend/ (separate requirements.txt, separate container)."""
    env_path = os.environ.get("JOBS_REPO_PATH")
    if env_path:
        p = Path(env_path)
        return p if p.is_absolute() else Path(__file__).parent.parent / p

    repo_cfg = cfg.get("jobs_repo") or {}
    cfg_path = repo_cfg.get("path")
    if cfg_path:
        p = Path(cfg_path)
        return p if p.is_absolute() else Path(__file__).parent.parent / p

    return Path(__file__).parent.parent


TARGETS_PATH = _resolve_jobs_repo() / TARGETS_FILENAME


@dataclass
class Target:
    table_key: str       # "keyspace.table"
    columns: list[str]   # sink column order, matches the prepared statement's bind order
    field_map: dict       # sink_column -> source_field name, or None
    stmt: object = None   # prepared INSERT, built lazily once the session exists
    batch: list = field(default_factory=list)
    last_flush: float = 0.0


def connect_scylla():
    while True:
        try:
            cluster = Cluster(
                CONTACT_PTS,
                port=PORT,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
                protocol_version=4,
            )
            session = cluster.connect()
            log.info("Connected to ScyllaDB at %s", CONTACT_PTS)
            return session
        except Exception as exc:
            log.warning("ScyllaDB not ready: %s — retrying in 5 s", exc)
            time.sleep(5)


def load_targets_config() -> dict:
    if not TARGETS_PATH.exists():
        return {}
    try:
        return json.loads(TARGETS_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Failed to read %s: %s", TARGETS_PATH, exc)
        return {}


def build_topic_routes(raw_targets: dict, session) -> dict[str, list[Target]]:
    """topic -> list of Targets fed by that topic, with prepared statements ready."""
    topic_routes: dict[str, list[Target]] = {}
    for table_key, target_cfg in raw_targets.items():
        for topic, topic_cfg in (target_cfg.get("topics") or {}).items():
            field_map = topic_cfg.get("field_map") or {}
            columns = list(field_map.keys())
            if not columns:
                continue
            placeholders = ", ".join("?" for _ in columns)
            stmt = session.prepare(
                f"INSERT INTO {table_key} ({', '.join(columns)}) VALUES ({placeholders})"
            )
            target = Target(table_key=table_key, columns=columns, field_map=field_map, stmt=stmt)
            topic_routes.setdefault(topic, []).append(target)
    return topic_routes


def _to_row(target: Target, rec: dict) -> tuple:
    return tuple(rec.get(target.field_map[col]) if target.field_map[col] else None for col in target.columns)


def flush_batch(session, target: Target, metrics: MetricsReporter) -> None:
    if not target.batch:
        return
    try:
        with metrics.latency("scylla_flush").measure():
            execute_concurrent_with_args(session, target.stmt, target.batch, concurrency=50)
        n = len(target.batch)
        metrics.count("scylla_written").inc(n)
        metrics.count(f"scylla_written:{target.table_key}").inc(n)
    except Exception as exc:
        log.warning("Batch upsert failed for %s (%d rows): %s", target.table_key, len(target.batch), exc)
        metrics.count("scylla_errors").inc()
    target.batch.clear()
    target.last_flush = time.monotonic()


def run():
    metrics = MetricsReporter(log, interval_s=SUMMARY_INTERVAL_S)
    session = connect_scylla()

    consumer = Consumer({
        "bootstrap.servers":  BOOTSTRAP,
        "group.id":           "scylla-consumer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    log.info(
        "Starting — watching %s for sink targets, reload every %ds | summary every %ds | "
        "logs → logs/scylla-consumer.log",
        TARGETS_PATH, CONFIG_RELOAD_INTERVAL_S, SUMMARY_INTERVAL_S,
    )

    topic_routes: dict[str, list[Target]] = {}
    subscribed_topics: set[str] = set()
    last_reload = 0.0

    try:
        while True:
            now = time.monotonic()

            if now - last_reload >= CONFIG_RELOAD_INTERVAL_S:
                raw_targets = load_targets_config()
                topic_routes = build_topic_routes(raw_targets, session)
                last_reload = now

                new_topics = set(topic_routes.keys())
                if new_topics != subscribed_topics:
                    if new_topics:
                        consumer.subscribe(list(new_topics))
                        log.info("Subscribed to topics: %s", ", ".join(sorted(new_topics)))
                    else:
                        consumer.unsubscribe()
                        log.info("No sink targets configured yet — not subscribed to anything")
                    subscribed_topics = new_topics

            for targets in topic_routes.values():
                for target in targets:
                    if target.batch and now - target.last_flush >= FLUSH_INTERVAL_S:
                        flush_batch(session, target, metrics)

            metrics.tick()

            if not subscribed_topics:
                time.sleep(0.5)
                continue

            msg = consumer.poll(timeout=0.2)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                    metrics.count("kafka_errors").inc()
                continue

            ts_type, ts_ms = msg.timestamp()
            if ts_type == TIMESTAMP_CREATE_TIME:
                lag_s = max(0.0, time.time() - ts_ms / 1000)
                metrics.latency("kafka_lag").record(lag_s)

            try:
                rec = json.loads(msg.value())
            except Exception as exc:
                log.warning("Failed to parse record: %s", exc)
                metrics.count("parse_errors").inc()
                continue
            metrics.count("kafka_read").inc()

            for target in topic_routes.get(msg.topic(), []):
                target.batch.append(_to_row(target, rec))
                if len(target.batch) >= BATCH_SIZE:
                    flush_batch(session, target, metrics)

    finally:
        for targets in topic_routes.values():
            for target in targets:
                flush_batch(session, target, metrics)
        consumer.close()


if __name__ == "__main__":
    run()
