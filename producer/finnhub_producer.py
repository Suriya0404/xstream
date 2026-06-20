"""
Finnhub real-time producer.

Merges two Finnhub endpoints:
  1. WebSocket  wss://ws.finnhub.io  → real-time trade ticks  → finnhub-trades
  2. REST       /api/v1/quote        → OHLCV snapshot (polled) → finnhub-quotes

Enriched record (trade + latest cached quote) → finnhub-enriched

Every 60 seconds a summary is logged:
  - Trades received from WebSocket, quotes fetched via REST, enriched records published
  - REST quote fetch latency (min/avg/max)
  - WebSocket message processing latency

Logs are written to logs/finnhub-producer.log and rotated at 10 MB
(10 backups = 100 MB max on disk). Set LOG_DIR / LOG_MAX_BYTES /
LOG_BACKUP_COUNT env vars to override.
"""

import asyncio
import json
import os
import time
from pathlib import Path

import requests
import websockets
import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from log_setup import configure_logging
from metrics import MetricsReporter

log = configure_logging("finnhub-producer")

# ── Config ────────────────────────────────────────────────────────────────────

CONFIG_PATH = os.getenv("CONFIG_PATH", str(Path(__file__).parent.parent / "config.yaml"))

with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

fh_cfg  = cfg["finnhub"]
kfk_cfg = cfg["kafka"]

API_KEY        = fh_cfg["api_key"]
SYMBOLS        = fh_cfg["symbols"]
TOPIC_TRADES   = fh_cfg["topics"]["trades"]
TOPIC_QUOTES   = fh_cfg["topics"]["quotes"]
TOPIC_ENRICHED = fh_cfg["topics"]["enriched"]
POLL_INTERVAL  = fh_cfg.get("quote_poll_interval_s", 10)

SUMMARY_INTERVAL_S = 60

WS_URL    = f"wss://ws.finnhub.io?token={API_KEY}"
REST_BASE = "https://finnhub.io/api/v1"

# ── Topic bootstrap ───────────────────────────────────────────────────────────

def ensure_topics():
    """Create Finnhub topics if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": kfk_cfg["bootstrap_servers"]})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    wanted = [TOPIC_TRADES, TOPIC_QUOTES, TOPIC_ENRICHED]
    to_create = [
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in wanted if t not in existing
    ]
    if not to_create:
        log.info("Topics already exist: %s", wanted)
        return
    results = admin.create_topics(to_create)
    for topic, fut in results.items():
        try:
            fut.result()
            log.info("Created topic: %s", topic)
        except Exception as exc:
            log.warning("Could not create topic %s: %s", topic, exc)


# ── Kafka producer ────────────────────────────────────────────────────────────

producer = Producer({
    "bootstrap.servers": kfk_cfg["bootstrap_servers"],
    "linger.ms": 5,
    "compression.type": "lz4",
})

def _delivery_report(err, msg):
    if err:
        log.warning("Delivery failed %s: %s", msg.topic(), err)

def publish(topic: str, key: str, payload: dict):
    producer.produce(
        topic,
        key=key.encode(),
        value=json.dumps(payload).encode(),
        callback=_delivery_report,
    )
    producer.poll(0)


# ── Quote cache (symbol → latest quote snapshot) ──────────────────────────────

quote_cache: dict[str, dict] = {}


def fetch_quote(symbol: str, metrics: MetricsReporter) -> dict | None:
    try:
        with metrics.latency("quote_rest_fetch").measure():
            r = requests.get(
                f"{REST_BASE}/quote",
                params={"symbol": symbol, "token": API_KEY},
                timeout=5,
            )
            r.raise_for_status()
            data = r.json()

        if data.get("c") is None:
            return None

        metrics.count("quotes_fetched").inc()
        return {
            "symbol":     symbol,
            "current":    data["c"],
            "high":       data["h"],
            "low":        data["l"],
            "open":       data["o"],
            "prev_close": data["pc"],
            "timestamp":  data["t"],
            "fetched_at": int(time.time()),
        }
    except Exception as exc:
        log.warning("Quote fetch failed for %s: %s", symbol, exc)
        metrics.count("quote_errors").inc()
        return None


async def quote_poller(metrics: MetricsReporter):
    """Periodically fetches REST quotes and updates the cache."""
    while True:
        for sym in SYMBOLS:
            q = fetch_quote(sym, metrics)
            if q:
                quote_cache[sym] = q
                publish(TOPIC_QUOTES, sym, q)
                log.info("QUOTE  %-22s  price=%.4f", sym, q["current"])
        producer.flush()
        await asyncio.sleep(POLL_INTERVAL)


# ── WebSocket trade listener ──────────────────────────────────────────────────

async def trade_listener(metrics: MetricsReporter):
    """Subscribes to Finnhub WebSocket and publishes trade ticks."""
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                for sym in SYMBOLS:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
                    log.info("Subscribed to %s", sym)

                async for raw in ws:
                    t_msg_start = time.perf_counter()

                    msg = json.loads(raw)
                    if msg.get("type") != "trade":
                        continue

                    for tick in msg.get("data", []):
                        sym   = tick.get("s", "")
                        price = tick.get("p", 0.0)
                        vol   = tick.get("v", 0.0)
                        ts    = tick.get("t", 0)   # milliseconds epoch

                        trade_record = {
                            "symbol":    sym,
                            "price":     price,
                            "volume":    vol,
                            "timestamp": ts,
                        }
                        publish(TOPIC_TRADES, sym, trade_record)
                        metrics.count("ws_trades").inc()

                        # Enrich with latest cached quote
                        q = quote_cache.get(sym)
                        if q:
                            enriched = {
                                **trade_record,
                                "quote_current":    q["current"],
                                "quote_high":       q["high"],
                                "quote_low":        q["low"],
                                "quote_open":       q["open"],
                                "quote_prev_close": q["prev_close"],
                                "pct_change": round(
                                    (price - q["prev_close"]) / q["prev_close"] * 100, 4
                                ) if q["prev_close"] else None,
                            }
                            publish(TOPIC_ENRICHED, sym, enriched)
                            metrics.count("enriched_published").inc()

                        log.info("TRADE  %-22s  price=%.4f  vol=%.2f", sym, price, vol)

                    producer.poll(0)
                    metrics.latency("ws_msg_processing").record(
                        time.perf_counter() - t_msg_start
                    )

        except Exception as exc:
            log.error("WebSocket error: %s — reconnecting in 5 s", exc)
            metrics.count("ws_reconnects").inc()
            await asyncio.sleep(5)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    metrics = MetricsReporter(log, interval_s=SUMMARY_INTERVAL_S)

    log.info("Starting Finnhub producer")
    log.info(
        "Summary every %ds | logs → logs/finnhub-producer.log",
        SUMMARY_INTERVAL_S,
    )
    ensure_topics()
    log.info("Symbols : %s", SYMBOLS)
    log.info(
        "Topics  : trades=%s  quotes=%s  enriched=%s",
        TOPIC_TRADES, TOPIC_QUOTES, TOPIC_ENRICHED,
    )
    log.info("Kafka   : %s", kfk_cfg["bootstrap_servers"])

    log.info("Seeding quote cache…")
    for sym in SYMBOLS:
        q = fetch_quote(sym, metrics)
        if q:
            quote_cache[sym] = q
            log.info("  %s → %.4f", sym, q["current"])

    await asyncio.gather(
        trade_listener(metrics),
        quote_poller(metrics),
        metrics.run(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
        producer.flush()
