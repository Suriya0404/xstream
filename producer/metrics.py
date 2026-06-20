"""
Lightweight in-process metrics: named counters + latency buckets.

Emits a 1-minute activity summary to any standard logger.

Synchronous usage (call metrics.tick() from your main loop):
    m = MetricsReporter(log, interval_s=60)
    m.count("kafka_read").inc(len(batch))
    with m.latency("scylla_flush").measure():
        execute_batch(...)
    m.tick()

Async usage (launch metrics.run() as a background task):
    asyncio.create_task(m.run())
"""
import asyncio
import threading
import time
from contextlib import contextmanager
from typing import Generator


class _LatencyBucket:
    """Thread-safe accumulator of elapsed-time samples (stored in seconds)."""

    __slots__ = ("_lock", "_samples")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._samples: list[float] = []

    def record(self, seconds: float) -> None:
        with self._lock:
            self._samples.append(seconds)

    @contextmanager
    def measure(self) -> Generator[None, None, None]:
        """Context manager: records wall time of the enclosed block."""
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.record(time.perf_counter() - t0)

    def flush(self) -> dict | None:
        """Return stats (in ms) and reset. Returns None when no samples."""
        with self._lock:
            s, self._samples = self._samples, []
        if not s:
            return None
        return {
            "n":      len(s),
            "min_ms": round(min(s) * 1000, 1),
            "avg_ms": round(sum(s) / len(s) * 1000, 1),
            "max_ms": round(max(s) * 1000, 1),
        }


class _Counter:
    """Thread-safe integer counter that resets on flush."""

    __slots__ = ("_lock", "_v")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._v = 0

    def inc(self, n: int = 1) -> None:
        with self._lock:
            self._v += n

    def flush(self) -> int:
        with self._lock:
            v, self._v = self._v, 0
            return v


class MetricsReporter:
    """
    Collects named counters and latency buckets; emits a one-line summary
    with per-metric latency details every `interval_s` seconds.

    Summary looks like:

        ── 1-min summary ──  kafka_read=1,234  scylla_written=1,230  kafka_errors=0
            kafka_lag:    avg=18.3ms  min=1.2ms  max=340.0ms  (n=1234)
            scylla_flush: avg=42.1ms  min=11.0ms  max=198.5ms  (n=3)
    """

    def __init__(self, logger, interval_s: int = 60) -> None:
        self._log = logger
        self._interval = interval_s
        self._last = time.monotonic()
        self._counters: dict[str, _Counter] = {}
        self._latencies: dict[str, _LatencyBucket] = {}

    def count(self, name: str) -> _Counter:
        if name not in self._counters:
            self._counters[name] = _Counter()
        return self._counters[name]

    def latency(self, name: str) -> _LatencyBucket:
        if name not in self._latencies:
            self._latencies[name] = _LatencyBucket()
        return self._latencies[name]

    def tick(self) -> None:
        """Emit summary if interval has elapsed. Call from a synchronous loop."""
        if time.monotonic() - self._last >= self._interval:
            self._emit()
            self._last = time.monotonic()

    async def run(self) -> None:
        """Async background task: emit summary every interval_s seconds."""
        while True:
            await asyncio.sleep(self._interval)
            self._emit()

    def _emit(self) -> None:
        counts = {k: v.flush() for k, v in self._counters.items()}
        count_str = "  ".join(
            f"{k}={v:,}" for k, v in counts.items()
        ) or "(no activity)"

        self._log.info("── 1-min summary ──  %s", count_str)

        for name, bucket in self._latencies.items():
            stats = bucket.flush()
            if stats:
                self._log.info(
                    "    %-20s avg=%sms  min=%sms  max=%sms  (n=%d)",
                    name + ":",
                    stats["avg_ms"], stats["min_ms"], stats["max_ms"], stats["n"],
                )
