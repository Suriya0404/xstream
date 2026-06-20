"""
Kafka → ScyllaDB consumer.

Reads enriched trade records from finnhub-enriched and upserts them into
xstream.finnhub_merged (keyed by symbol — last-write-wins in ScyllaDB).

Batches up to BATCH_SIZE records or flushes after FLUSH_INTERVAL_S seconds,
whichever comes first. Uses execute_concurrent_with_args for parallelism.

Every 60 seconds a summary is logged:
  - Records read from Kafka / written to ScyllaDB / errors
  - Kafka consumer lag (ms between message creation and processing)
  - ScyllaDB batch flush latency

Logs are written to logs/scylla-consumer.log and rotated at 10 MB
(10 backups = 100 MB max on disk). Set LOG_DIR / LOG_MAX_BYTES /
LOG_BACKUP_COUNT env vars to override.

ClickHouse note: ClickHouse writes are handled by the Flink SQL Gateway
(not this process). Check the Flink Job Manager UI at :8082 for those metrics.
"""
import json
import os
import time
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
fh_cfg  = cfg["finnhub"]

TOPIC       = fh_cfg["topics"]["enriched"]
BOOTSTRAP   = kfk_cfg["bootstrap_servers"]
CONTACT_PTS = sc_cfg["contact_points"]
PORT        = sc_cfg.get("port", 9042)
KEYSPACE    = sc_cfg.get("keyspace", "xstream")

BATCH_SIZE         = 500   # flush when this many records accumulate
FLUSH_INTERVAL_S   = 1.0   # also flush after this many seconds with pending records
SUMMARY_INTERVAL_S = 60    # how often to emit the 1-minute activity summary


def connect_scylla():
    while True:
        try:
            cluster = Cluster(
                CONTACT_PTS,
                port=PORT,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
                protocol_version=4,
            )
            session = cluster.connect(KEYSPACE)
            log.info("Connected to ScyllaDB keyspace=%s", KEYSPACE)
            return session
        except Exception as exc:
            log.warning("ScyllaDB not ready: %s — retrying in 5 s", exc)
            time.sleep(5)


def prepare_stmt(session):
    return session.prepare("""
        INSERT INTO finnhub_merged
            (symbol, price, volume, quote_current, quote_high, quote_low, pct_change, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)


def _to_row(rec: dict) -> tuple:
    return (
        rec.get("symbol", ""),
        float(rec.get("price") or 0),
        float(rec.get("volume") or 0),
        float(rec.get("quote_current") or 0),
        float(rec.get("quote_high") or 0),
        float(rec.get("quote_low") or 0),
        float(rec.get("pct_change") or 0),
        int(rec.get("timestamp") or 0),
    )


def flush_batch(
    session,
    stmt,
    batch: list[tuple],
    total: int,
    metrics: MetricsReporter,
) -> int:
    if not batch:
        return total
    try:
        with metrics.latency("scylla_flush").measure():
            execute_concurrent_with_args(session, stmt, batch, concurrency=50)
        n = len(batch)
        total += n
        metrics.count("scylla_written").inc(n)
        if total % 500 == 0 or n >= BATCH_SIZE:
            log.info("Upserted %d records total (batch=%d)", total, n)
    except Exception as exc:
        log.warning("Batch upsert failed (%d rows): %s", len(batch), exc)
        metrics.count("scylla_errors").inc()
    return total


def run():
    metrics = MetricsReporter(log, interval_s=SUMMARY_INTERVAL_S)

    session = connect_scylla()
    stmt    = prepare_stmt(session)

    consumer = Consumer({
        "bootstrap.servers":  BOOTSTRAP,
        "group.id":           "scylla-consumer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])
    log.info(
        "Subscribed to %s — writing to %s.finnhub_merged  "
        "| summary every %ds | logs → logs/scylla-consumer.log",
        TOPIC, KEYSPACE, SUMMARY_INTERVAL_S,
    )

    batch: list[tuple] = []
    last_flush = time.monotonic()
    total = 0

    try:
        while True:
            # Time-based flush (before poll so we always flush on schedule)
            if time.monotonic() - last_flush >= FLUSH_INTERVAL_S:
                total = flush_batch(session, stmt, batch, total, metrics)
                batch = []
                last_flush = time.monotonic()

            # Emit 1-minute summary when due
            metrics.tick()

            msg = consumer.poll(timeout=0.2)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                    metrics.count("kafka_errors").inc()
                continue

            # Track consumer lag: how old is this message at the time we process it?
            ts_type, ts_ms = msg.timestamp()
            if ts_type == TIMESTAMP_CREATE_TIME:
                lag_s = max(0.0, time.time() - ts_ms / 1000)
                metrics.latency("kafka_lag").record(lag_s)

            try:
                rec = json.loads(msg.value())
                batch.append(_to_row(rec))
                metrics.count("kafka_read").inc()
            except Exception as exc:
                log.warning("Failed to parse record: %s", exc)
                metrics.count("parse_errors").inc()
                continue

            if len(batch) >= BATCH_SIZE:
                total = flush_batch(session, stmt, batch, total, metrics)
                batch = []
                last_flush = time.monotonic()

    finally:
        flush_batch(session, stmt, batch, total, metrics)
        consumer.close()


if __name__ == "__main__":
    run()
