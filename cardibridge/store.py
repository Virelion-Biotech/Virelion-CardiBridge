from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .contracts import BridgeEnvelope


class EventStore:
    """Small durable audit/idempotency store; replaceable by a distributed backend."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.db = sqlite3.connect(str(path))
        self.db.execute("CREATE TABLE IF NOT EXISTS events (key TEXT PRIMARY KEY, message_id TEXT NOT NULL, payload TEXT NOT NULL)")
        self.db.commit()

    def seen(self, key: str) -> bool:
        return self.db.execute("SELECT 1 FROM events WHERE key = ?", (key,)).fetchone() is not None

    def append(self, envelope: BridgeEnvelope) -> bool:
        if self.seen(envelope.idempotency_key):
            return False
        self.db.execute(
            "INSERT INTO events(key, message_id, payload) VALUES (?, ?, ?)",
            (envelope.idempotency_key, envelope.message_id, json.dumps(envelope.model_dump(mode="json"))),
        )
        self.db.commit()
        return True

    def get(self, key: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT payload FROM events WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None
