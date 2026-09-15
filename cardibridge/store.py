from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contracts import BridgeEnvelope, LineageEvent
from .protocol import content_hash, topic_for
from .reliability import DeliveryAttempt


class EventStore:
    """SQLite-backed inbox/outbox, delivery history, lineage and replay store."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self._lock = threading.RLock()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                message_id TEXT UNIQUE NOT NULL,
                topic TEXT NOT NULL,
                payload TEXT NOT NULL,
                digest TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            )"""
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_events_topic_seq ON events(topic, seq)")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS delivery_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                success INTEGER NOT NULL,
                error TEXT,
                attempted_at TEXT NOT NULL,
                next_retry_at TEXT
            )"""
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_attempts_message ON delivery_attempts(message_id, attempt)")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS lineage_events (
                event_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                digest TEXT NOT NULL,
                created_at TEXT NOT NULL
            )"""
        )
        self.db.commit()

    def seen(self, key: str) -> bool:
        return self.db.execute("SELECT 1 FROM events WHERE key=?", (key,)).fetchone() is not None

    def append(self, envelope: BridgeEnvelope, status: str = "accepted") -> bool:
        with self._lock, self.db:
            if self.seen(envelope.idempotency_key):
                return False
            raw = envelope.model_dump(mode="json")
            self.db.execute(
                "INSERT INTO events(key,message_id,topic,payload,digest,status,created_at) VALUES(?,?,?,?,?,?,?)",
                (
                    envelope.idempotency_key,
                    envelope.message_id,
                    topic_for(envelope),
                    json.dumps(raw, sort_keys=True),
                    content_hash(raw),
                    status,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            return True

    def mark(self, message_id: str, status: str) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE events SET status=? WHERE message_id=?", (status, message_id))

    def status(self, message_id: str) -> str | None:
        row = self.db.execute("SELECT status FROM events WHERE message_id=?", (message_id,)).fetchone()
        return row[0] if row else None

    def list_events(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = "SELECT seq,message_id,topic,status,digest,created_at FROM events"
        params: tuple[Any, ...] = ()
        if status is not None:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY seq DESC LIMIT ?"
        params += (max(1, limit),)
        rows = self.db.execute(sql, params).fetchall()
        return [
            {"seq": seq, "message_id": message_id, "topic": topic, "status": state, "digest": digest, "created_at": created}
            for seq, message_id, topic, state, digest, created in rows
        ]

    def get(self, key: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT payload FROM events WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def get_envelope(self, message_id: str) -> BridgeEnvelope | None:
        row = self.db.execute("SELECT payload FROM events WHERE message_id=?", (message_id,)).fetchone()
        return BridgeEnvelope.model_validate_json(row[0]) if row else None

    def pending_outbox(self, limit: int = 100) -> list[BridgeEnvelope]:
        rows = self.db.execute(
            "SELECT payload FROM events WHERE status IN ('outbox','retry') ORDER BY seq ASC LIMIT ?",
            (max(1, limit),),
        ).fetchall()
        return [BridgeEnvelope.model_validate_json(row[0]) for row in rows]

    def record_attempt(self, attempt: DeliveryAttempt) -> None:
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO delivery_attempts(message_id,attempt,success,error,attempted_at,next_retry_at) VALUES(?,?,?,?,?,?)",
                (
                    attempt.message_id,
                    attempt.attempt,
                    int(attempt.success),
                    attempt.error,
                    attempt.attempted_at.isoformat(),
                    attempt.next_retry_at.isoformat() if attempt.next_retry_at else None,
                ),
            )

    def attempts(self, message_id: str) -> list[DeliveryAttempt]:
        rows = self.db.execute(
            "SELECT message_id,attempt,success,error,attempted_at,next_retry_at FROM delivery_attempts WHERE message_id=? ORDER BY attempt ASC",
            (message_id,),
        ).fetchall()
        return [
            DeliveryAttempt(
                message_id=row[0],
                attempt=row[1],
                success=bool(row[2]),
                error=row[3],
                attempted_at=datetime.fromisoformat(row[4]),
                next_retry_at=datetime.fromisoformat(row[5]) if row[5] else None,
            )
            for row in rows
        ]

    def append_lineage(self, event: LineageEvent) -> bool:
        raw = event.model_dump(mode="json")
        with self._lock, self.db:
            exists = self.db.execute("SELECT 1 FROM lineage_events WHERE event_id=?", (event.event_id,)).fetchone()
            if exists:
                return False
            self.db.execute(
                "INSERT INTO lineage_events(event_id,run_id,event_type,payload,digest,created_at) VALUES(?,?,?,?,?,?)",
                (
                    event.event_id,
                    event.run_id,
                    event.event_type,
                    json.dumps(raw, sort_keys=True),
                    content_hash(raw),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            return True

    def lineage(self, run_id: str | None = None) -> Iterable[LineageEvent]:
        if run_id:
            rows = self.db.execute(
                "SELECT payload FROM lineage_events WHERE run_id=? ORDER BY created_at ASC", (run_id,)
            )
        else:
            rows = self.db.execute("SELECT payload FROM lineage_events ORDER BY created_at ASC")
        for (payload,) in rows:
            yield LineageEvent.model_validate_json(payload)

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
