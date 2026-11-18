"""Logs estruturados (RF12).

Cada evento vira UMA linha JSON com, no mínimo:
  ts (ISO-8601 UTC), level, component, instance, event
e campos opcionais como path, chunk_id, node_id, disk_id, bytes, duration_ms...

Linhas JSON são fáceis de filtrar com `jq`/pandas na Parte 2
(ex.: tempo entre `node_dead` e `recovery_completed`).
"""
from __future__ import annotations

import json
import os
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Callable


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class EventLogger:
    def __init__(self, component: str, instance: str | None = None, file: str | None = None):
        self.component = component
        self.instance = instance or component
        self.sink: Callable[[dict], None] | None = None
        self._lock = threading.Lock()
        self._fh = None
        if file:
            os.makedirs(os.path.dirname(os.path.abspath(file)), exist_ok=True)
            self._fh = open(file, "a", encoding="utf-8")

    def emit(self, level: str, event: str, **fields: Any) -> dict:
        rec = {
            "ts": utc_iso(),
            "level": level,
            "component": self.component,
            "instance": self.instance,
            "event": event,
        }
        rec.update({k: v for k, v in fields.items() if v is not None})
        line = json.dumps(rec, default=str, ensure_ascii=False)
        with self._lock:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
            if self._fh:
                self._fh.write(line + "\n")
                self._fh.flush()
        if self.sink:
            try:
                self.sink(rec)
            except Exception:
                pass
        return rec

    def info(self, event: str, **fields: Any) -> dict:
        return self.emit("INFO", event, **fields)

    def warning(self, event: str, **fields: Any) -> dict:
        return self.emit("WARN", event, **fields)

    def error(self, event: str, **fields: Any) -> dict:
        return self.emit("ERROR", event, **fields)
