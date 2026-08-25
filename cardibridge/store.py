from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .contracts import BridgeEnvelope
from .protocol import content_hash, topic_for


class EventStore:
    """Durable inbox/outbox/audit log with replayable ordered events."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self._lock = threading.RLock()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE NOT NULL, message_id TEXT UNIQUE NOT NULL, topic TEXT NOT NULL, payload TEXT NOT NULL, digest TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL)")
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_events_topic_seq ON events(topic, seq)")
        self.db.commit()

    def seen(self, key: str) -> bool:
        return self.db.execute("SELECT 1 FROM events WHERE key=?", (key,)).fetchone() is not None

    def append(self, envelope: BridgeEnvelope, status: str = "accepted") -> bool:
        with self._lock, self.db:
            if self.seen(envelope.idempotency_key):
                return False
            raw = envelope.model_dump(mode="json")
            self.db.execute("INSERT INTO events(key,message_id,topic,payload,digest,status,created_at) VALUES(?,?,?,?,?,?,?)", (envelope.idempotency_key, envelope.message_id, topic_for(envelope), json.dumps(raw, sort_keys=True), content_hash(raw), status, datetime.now(timezone.utc).isoformat()))
            return True

    def mark(self, message_id: str, status: str) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE events SET status=? WHERE message_id=?", (status, message_id))

    def get(self, key: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT payload FROM events WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def get_envelope(self, message_id: str) -> BridgeEnvelope | None:
        row = self.db.execute("SELECT payload FROM events WHERE message_id=?", (message_id,)).fetchone()
        return BridgeEnvelope.model_validate_json(row[0]) if row else None

    def replay(self, topic: str | None = None, after: int = 0) -> Iterable[tuple[int, BridgeEnvelope]]:
        sql = "SELECT seq,payload FROM events WHERE seq>?"
        params: tuple[Any, ...] = (after,)
        if topic:
            sql += " AND topic=?"
            params += (topic,)
        sql += " ORDER BY seq ASC"
        for seq, payload in self.db.execute(sql, params):
            yield seq, BridgeEnvelope.model_validate_json(payload)

    def close(self) -> None:
        self.db.close()
