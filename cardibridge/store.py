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
        self.db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_attempts_message_attempt "
            "ON delivery_attempts(message_id, attempt)"
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
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_lineage_run_time ON lineage_events(run_id, created_at)")
        self.db.commit()

    def seen(self, key: str) -> bool:
        return self.status_by_key(key) is not None

    def status_by_key(self, key: str) -> str | None:
        with self._lock:
            row = self.db.execute("SELECT status FROM events WHERE key=?", (key,)).fetchone()
            return row[0] if row else None

    def append(self, envelope: BridgeEnvelope, status: str = "accepted") -> bool:
        raw = envelope.model_dump(mode="json")
        serialized = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        digest = content_hash(raw)
        with self._lock, self.db:
            try:
                self.db.execute(
                    "INSERT INTO events(key,message_id,topic,payload,digest,status,created_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        envelope.idempotency_key,
                        envelope.message_id,
                        topic_for(envelope),
                        serialized,
                        digest,
                        status,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                existing = self.db.execute(
                    "SELECT digest FROM events WHERE key=?", (envelope.idempotency_key,)
                ).fetchone()
                if existing and existing[0] == digest:
                    return False
                if existing:
                    raise ValueError(
                        "idempotency_key is already associated with a different envelope"
                    ) from exc
                raise ValueError("message_id is already associated with another idempotency key") from exc
            return True

    def claim(self, key: str, allowed_statuses: Iterable[str]) -> bool:
        statuses = tuple(allowed_statuses)
        if not statuses:
            raise ValueError("allowed_statuses must not be empty")
        placeholders = ",".join("?" for _ in statuses)
        with self._lock, self.db:
            cursor = self.db.execute(
                f"UPDATE events SET status=? WHERE key=? AND status IN ({placeholders})",
                ("processing", key, *statuses),
            )
            return cursor.rowcount == 1

    def mark(self, message_id: str, status: str) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE events SET status=? WHERE message_id=?", (status, message_id))

    def status(self, message_id: str) -> str | None:
        with self._lock:
            row = self.db.execute("SELECT status FROM events WHERE message_id=?", (message_id,)).fetchone()
            return row[0] if row else None

    def list_events(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if limit < 1:
            return []
        sql = "SELECT seq,message_id,topic,status,digest,created_at FROM events"
        params: tuple[Any, ...] = ()
        if status is not None:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY seq DESC LIMIT ?"
        params += (limit,)
        with self._lock:
            rows = self.db.execute(sql, params).fetchall()
        return [
            {"seq": seq, "message_id": mid, "topic": topic, "status": state, "digest": digest, "created_at": created}
            for seq, mid, topic, state, digest, created in rows
        ]

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute("SELECT payload FROM events WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def get_envelope(self, message_id: str) -> BridgeEnvelope | None:
        with self._lock:
            row = self.db.execute("SELECT payload FROM events WHERE message_id=?", (message_id,)).fetchone()
        return BridgeEnvelope.model_validate_json(row[0]) if row else None

    def pending_outbox(self, limit: int = 100) -> list[BridgeEnvelope]:
        if limit < 1:
            return []
        with self._lock:
            rows = self.db.execute(
                "SELECT payload FROM events WHERE status IN ('outbox','retry') ORDER BY seq ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [BridgeEnvelope.model_validate_json(row[0]) for row in rows]

    def record_attempt(self, attempt: DeliveryAttempt) -> None:
        with self._lock, self.db:
            try:
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
            except sqlite3.IntegrityError as exc:
                raise ValueError(
                    f"delivery attempt {attempt.attempt} already recorded for {attempt.message_id}"
                ) from exc

    def attempts(self, message_id: str) -> list[DeliveryAttempt]:
        with self._lock:
            rows = self.db.execute(
                "SELECT message_id,attempt,success,error,attempted_at,next_retry_at "
                "FROM delivery_attempts WHERE message_id=? ORDER BY attempt ASC",
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
        serialized = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        with self._lock, self.db:
            try:
                self.db.execute(
                    "INSERT INTO lineage_events(event_id,run_id,event_type,payload,digest,created_at) VALUES(?,?,?,?,?,?)",
                    (
                        event.event_id,
                        event.run_id,
                        event.event_type,
                        serialized,
                        content_hash(raw),
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
            except sqlite3.IntegrityError:
                return False
            return True

    def lineage(self, run_id: str | None = None) -> Iterable[LineageEvent]:
        with self._lock:
            sql = "SELECT payload FROM lineage_events"
            params: tuple[Any, ...] = ()
            if run_id:
                sql += " WHERE run_id=?"
                params = (run_id,)
            rows = self.db.execute(sql + " ORDER BY created_at ASC, event_id ASC", params).fetchall()
        return (LineageEvent.model_validate_json(payload) for (payload,) in rows)

    def replay(
        self, topic: str | None = None, after: int = 0, limit: int | None = None
    ) -> Iterable[tuple[int, BridgeEnvelope]]:
        if after < 0:
            raise ValueError("after must be >= 0")
        sql = "SELECT seq,payload FROM events WHERE seq>?"
        params: tuple[Any, ...] = (after,)
        if topic:
            sql += " AND topic=?"
            params += (topic,)
        sql += " ORDER BY seq ASC"
        if limit is not None:
            if limit < 1:
                return iter(())
            sql += " LIMIT ?"
            params += (limit,)
        with self._lock:
            rows = self.db.execute(sql, params).fetchall()
        return ((seq, BridgeEnvelope.model_validate_json(payload)) for seq, payload in rows)

    def close(self) -> None:
        with self._lock:
            self.db.close()
