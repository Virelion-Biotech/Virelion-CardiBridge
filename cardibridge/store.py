from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contracts import BridgeEnvelope, LineageEvent
from .deadletter import DeadLetter
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
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_attempts_message ON delivery_attempts(message_id, attempt)"
        )
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
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_lineage_run_time ON lineage_events(run_id, created_at)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS consumer_results (key TEXT PRIMARY KEY, payload TEXT NOT NULL)"
        )
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(events)")}
        for name, kind in (("lease_owner", "TEXT"), ("lease_until", "REAL")):
            if name not in columns:
                try:
                    self.db.execute(f"ALTER TABLE events ADD COLUMN {name} {kind}")
                    if name == "lease_until":
                        self.db.execute("UPDATE events SET lease_until=0 WHERE status='processing'")
                except sqlite3.OperationalError:
                    if name not in {r[1] for r in self.db.execute("PRAGMA table_info(events)")}:
                        raise
        result_columns = {row[1] for row in self.db.execute("PRAGMA table_info(consumer_results)")}
        if "digest" not in result_columns:
            try:
                self.db.execute("ALTER TABLE consumer_results ADD COLUMN digest TEXT")
            except sqlite3.OperationalError:
                if "digest" not in {
                    r[1] for r in self.db.execute("PRAGMA table_info(consumer_results)")
                }:
                    raise
            for key, payload in self.db.execute(
                "SELECT key,payload FROM consumer_results"
            ).fetchall():
                self.db.execute(
                    "UPDATE consumer_results SET digest=? WHERE key=?",
                    (content_hash(json.loads(payload)), key),
                )
        self.db.commit()

    def complete(self, key: str, result: Any, *, owner: str | None = None) -> None:
        """Atomically complete delivery and retain JSON consumer output for retries."""
        try:
            payload = json.dumps(result, allow_nan=False)
        except (TypeError, ValueError):
            payload = None  # Non-JSON handlers keep the legacy duplicate receipt.
        with self._lock, self.db:
            row = self.db.execute("SELECT lease_owner FROM events WHERE key=?", (key,)).fetchone()
            if row is None or (row[0] is not None and row[0] != owner):
                raise ValueError("consumer completion does not own the delivery claim")
            self.db.execute("DELETE FROM consumer_results WHERE key=?", (key,))
            if payload is not None:
                self.db.execute(
                    "INSERT OR REPLACE INTO consumer_results(key,payload,digest) VALUES(?,?,?)",
                    (key, payload, content_hash(result)),
                )
            self.db.execute(
                "UPDATE events SET status='processed',lease_owner=NULL,lease_until=NULL WHERE key=?",
                (key,),
            )

    def duplicate_receipt(self, key: str) -> dict[str, Any]:
        with self._lock:
            event = self.db.execute("SELECT message_id FROM events WHERE key=?", (key,)).fetchone()
            result = self.db.execute(
                "SELECT payload,digest FROM consumer_results WHERE key=?", (key,)
            ).fetchone()
        receipt = {"status": "duplicate", "message_id": event[0] if event else None}
        if result:
            value = json.loads(result[0])
            if content_hash(value) != result[1]:
                raise ValueError("stored consumer result digest mismatch")
            receipt["result"] = value
        return receipt

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
                raise ValueError(
                    "message_id is already associated with another idempotency key"
                ) from exc
            return True

    def claim(
        self,
        key: str,
        allowed_statuses: Iterable[str],
        *,
        owner: str | None = None,
        lease_seconds: float = 30.0,
        processing_status: str = "processing",
    ) -> bool:
        statuses = tuple(allowed_statuses)
        if not statuses or not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("nonempty statuses and positive finite lease are required")
        placeholders = ",".join("?" for _ in statuses)
        now = time.time()
        with self._lock, self.db:
            cursor = self.db.execute(
                f"UPDATE events SET status=?,lease_owner=?,lease_until=? WHERE key=? "
                f"AND (status IN ({placeholders}) OR (status=? AND lease_until<=?))",
                (
                    processing_status,
                    owner,
                    now + lease_seconds,
                    key,
                    *statuses,
                    processing_status,
                    now,
                ),
            )
            return cursor.rowcount == 1

    @contextmanager
    def lease(self, key: str, owner: str, seconds: float = 30.0) -> Iterator[None]:
        """Renew a claimed lease during live work; crashed workers expire automatically."""
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("lease duration must be positive and finite")
        stopped = threading.Event()

        def renew() -> None:
            while not stopped.wait(seconds / 3):
                with self._lock, self.db:
                    self.db.execute(
                        "UPDATE events SET lease_until=? WHERE key=? AND lease_owner=?",
                        (time.time() + seconds, key, owner),
                    )

        worker = threading.Thread(target=renew, daemon=True)
        worker.start()
        try:
            yield
        finally:
            stopped.set()
            worker.join()

    def mark(self, message_id: str, status: str, *, owner: str | None = None) -> None:
        with self._lock, self.db:
            cursor = self.db.execute(
                "UPDATE events SET status=?,lease_owner=NULL,lease_until=NULL WHERE message_id=? "
                "AND (lease_owner IS NULL OR lease_owner=?)",
                (status, message_id, owner),
            )
            if cursor.rowcount != 1:
                raise ValueError("status transition does not own the delivery claim")

    def status(self, message_id: str) -> str | None:
        with self._lock:
            row = self.db.execute(
                "SELECT status FROM events WHERE message_id=?", (message_id,)
            ).fetchone()
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
            {
                "seq": seq,
                "message_id": mid,
                "topic": topic,
                "status": state,
                "digest": digest,
                "created_at": created,
            }
            for seq, mid, topic, state, digest, created in rows
        ]

    @staticmethod
    def _verified(payload: str, digest: str) -> dict[str, Any]:
        from .codec import _reject_constant, _unique_object

        value: Any = json.loads(
            payload, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
        if not isinstance(value, dict) or content_hash(value) != digest:
            raise ValueError("stored payload digest mismatch")
        return value

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT payload,digest FROM events WHERE key=?", (key,)
            ).fetchone()
        return self._verified(row[0], row[1]) if row else None

    def get_envelope(self, message_id: str) -> BridgeEnvelope | None:
        with self._lock:
            row = self.db.execute(
                "SELECT payload,digest FROM events WHERE message_id=?", (message_id,)
            ).fetchone()
        return BridgeEnvelope.model_validate(self._verified(row[0], row[1])) if row else None

    def pending_outbox(self, limit: int = 100) -> list[BridgeEnvelope]:
        if limit < 1:
            return []
        with self._lock:
            rows = self.db.execute(
                "SELECT payload,digest FROM events WHERE status IN ('outbox','retry') "
                "OR (status='publishing' AND lease_until<=?) ORDER BY seq ASC LIMIT ?",
                (time.time(), limit),
            ).fetchall()
        return [BridgeEnvelope.model_validate(self._verified(row[0], row[1])) for row in rows]

    def record_attempt(self, attempt: DeliveryAttempt, *, owner: str | None = None) -> None:
        with self._lock, self.db:
            if owner is not None:
                row = self.db.execute(
                    "SELECT lease_owner FROM events WHERE message_id=?", (attempt.message_id,)
                ).fetchone()
                if row is None or row[0] != owner:
                    raise ValueError("attempt does not own the delivery claim")
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
            except sqlite3.IntegrityError as exc:
                existing = self.db.execute(
                    "SELECT digest FROM lineage_events WHERE event_id=?", (event.event_id,)
                ).fetchone()
                if existing and existing[0] == content_hash(raw):
                    return False
                raise ValueError("lineage event_id is associated with different content") from exc
            return True

    def lineage(self, run_id: str | None = None) -> Iterable[LineageEvent]:
        with self._lock:
            sql = "SELECT payload,digest FROM lineage_events"
            params: tuple[Any, ...] = ()
            if run_id:
                sql += " WHERE run_id=?"
                params = (run_id,)
            rows = self.db.execute(
                sql + " ORDER BY created_at ASC, event_id ASC", params
            ).fetchall()
        return (
            LineageEvent.model_validate(self._verified(payload, digest)) for payload, digest in rows
        )

    def replay(
        self, topic: str | None = None, after: int = 0, limit: int | None = None
    ) -> Iterable[tuple[int, BridgeEnvelope]]:
        if after < 0:
            raise ValueError("after must be >= 0")
        sql = "SELECT seq,payload,digest FROM events WHERE seq>?"
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
        return (
            (seq, BridgeEnvelope.model_validate(self._verified(payload, digest)))
            for seq, payload, digest in rows
        )

    def dead_letters(self, limit: int = 100) -> list[DeadLetter]:
        """Recover terminal delivery diagnostics from the durable outbox after restart."""
        result = []
        for event in self.list_events(status="dead_letter", limit=limit):
            envelope = self.get_envelope(event["message_id"])
            if envelope is not None:
                attempts = tuple(self.attempts(envelope.message_id))
                reason = attempts[-1].error if attempts else "delivery failed"
                result.append(
                    DeadLetter(
                        envelope=envelope, reason=reason or "delivery failed", attempts=attempts
                    )
                )
        return result

    def close(self) -> None:
        with self._lock:
            self.db.close()
