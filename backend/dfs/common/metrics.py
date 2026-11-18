"""Métricas simples em memória, expostas em JSON no endpoint /metrics (observabilidade)."""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque


class OpStats:
    def __init__(self, window: int = 2000):
        self.count = 0
        self.errors = 0
        self.total_ms = 0.0
        self.bytes = 0
        self.samples: deque[float] = deque(maxlen=window)

    def add(self, ms: float, nbytes: int = 0, error: bool = False) -> None:
        self.count += 1
        self.total_ms += ms
        self.bytes += nbytes
        if error:
            self.errors += 1
        self.samples.append(ms)

    def summary(self) -> dict:
        s = sorted(self.samples)

        def pct(p: float) -> float | None:
            if not s:
                return None
            return round(s[min(len(s) - 1, int(p * len(s)))], 3)

        return {
            "count": self.count,
            "errors": self.errors,
            "bytes": self.bytes,
            "avg_ms": round(self.total_ms / self.count, 3) if self.count else None,
            "p50_ms": pct(0.50),
            "p95_ms": pct(0.95),
            "max_ms": round(s[-1], 3) if s else None,
        }


class Metrics:
    def __init__(self):
        self.started = time.time()
        self._ops: dict[str, OpStats] = defaultdict(OpStats)
        self.counters: dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()

    def observe(self, op: str, ms: float, nbytes: int = 0, error: bool = False) -> None:
        with self._lock:
            self._ops[op].add(ms, nbytes, error)

    def inc(self, name: str, value: float = 1) -> None:
        with self._lock:
            self.counters[name] += value

    def snapshot(self) -> dict:
        with self._lock:
            uptime = time.time() - self.started
            ops = {}
            for k, v in self._ops.items():
                summ = v.summary()
                summ["throughput_MBps_avg"] = round(v.bytes / uptime / 1e6, 4) if uptime else None
                ops[k] = summ
            return {
                "uptime_s": round(uptime, 1),
                "operations": ops,
                "counters": dict(self.counters),
            }


class Timer:
    """with Timer() as t: ...; t.ms"""

    def __enter__(self):
        self._t = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.ms = (time.perf_counter() - self._t) * 1000
        return False
