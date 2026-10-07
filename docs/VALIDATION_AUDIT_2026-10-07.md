# CardiBridge CPU validation audit — 2026-10-07

## Outcome and scope

Version 0.3.1 repairs reproduced protocol, persistence, delivery, and gateway defects. CardiBridge is an interoperability library; it contains no cardiac solver, training pipeline, or clinical prediction algorithm. CPU cloud execution can validate its software contracts and delivery behavior. It cannot establish the scientific validity of results supplied by other HeartTwin services or certify a perfect product.

Baseline: main `d17d30b057e7744cbd6fe23afb805db081e66f40`, version 0.3.0. The 51 original tests, Ruff, and strict mypy passed. Sixteen new regression cases failed against that implementation before repairs. The final local suite has 103 passing tests, including 52 additions; measured statement coverage is 87.44% (CI requires at least 80%).

## Reproduced defects and repairs

| Failure | Repair and evidence |
| --- | --- |
| Callback failure permanently marked an in-memory message as delivered | Retain identities and messages only after successful callbacks; retry, concurrent duplicate, queue cleanup, and recursive callback tests |
| In-memory routers silently accepted different envelopes sharing one key | Bind successful/in-flight keys to full envelope digests and reject identity conflicts |
| Cancelled asynchronous handlers retained processing claims | Clean up on `BaseException`; cancellation followed by retry succeeds |
| Concurrent durable publishers could both send and record attempt 1 | SQLite compare-and-set claims, renewable ownership, and owner-checked attempt recording; concurrent publication test |
| Crashed durable consumers remained stuck forever | Expiring leases, live-worker renewal, fenced completion, process-exit recovery, and legacy-database migration tests |
| Persisted retry schedules were ignored on resume | Re-read attempt history after acquiring ownership, honor stored deadlines, preserve attempt numbering, and recover expired outbox claims |
| Stored payload and lineage digests were never checked | Verify digest on get, replay, outbox and lineage reads; consumer result digests added; corruption fails closed |
| Duplicate lineage IDs masked changed event contents | Permit identical duplicates, reject identity/content conflicts |
| Shallow migration copies changed source payloads | Deep-copy migration inputs and same-version results; validate versions and registered destination schema; a registered transform alone does not imply target-schema support |
| Unsupported envelope versions could be dispatched | Validate detached envelopes and exact registered versions before routing and encoding |
| NaN retry settings and large exponents bypassed bounds or overflowed | Finite configuration validation and saturating exponential arithmetic, including subnormal delay inputs |
| Nested numeric/string keys collapsed during Pydantic serialization | Validate model keys in Python form before encoding, hashing, signing, storing or routing; five boundary regressions |
| Ambiguous or non-finite JSON and non-ASCII signatures caused inconsistent behavior | Reject duplicate keys, non-finite values and ambiguous canonical inputs; verification returns false on malformed signatures |
| An explicitly supplied empty DLQ was discarded | Preserve the caller's instance; recover terminal records from SQLite after restart |
| Gateway leaked replay under configured authentication and mishandled errors | Protect replay, normalize validation failures, reject unknown contracts and conflicting identities with stable HTTP errors |
| HTTP publisher accepted arbitrary 2xx bodies and unfinished duplicates | Validate message identity and completion acknowledgement, preserve completed duplicate status; live TCP gateway integration |
| AsyncAPI addresses/references diverged from wire traffic | Export typed envelopes on versioned topics with resolvable schema references and channel message references |
| Built-in registry omitted benchmark contracts | Use the canonical seven-contract registry |
| NATS buffered sends and subscription resources had no explicit cleanup | Flush compatible clients and unsubscribe in `finally`; NATS/Kafka client-double round-trip and error tests |

## Verification

- `python -m ruff check cardibridge tests ci`: pass.
- `python -m mypy cardibridge`: pass for all 31 source modules.
- `python -m pytest --cov=cardibridge --cov-report=term --cov-fail-under=80`: 103 tests pass, 87.44% statement coverage.
- `python -m build`: sdist and wheel build successfully; SPDX license metadata retained.
- `python ci/validate_wheel.py`: clean virtual environment, imports outside source checkout, codec round trip, SQLite restart, durable publication, FastAPI gateway, CLI and dependency consistency pass.
- Official `@asyncapi/parser` 3.6.3: document accepted, zero errors and warnings. Pydantic-derived envelope schemas also validate actual envelope JSON through JSON Schema and reject invalid payloads.
- Runtime dependency vulnerability audit: no known vulnerabilities in the checked pinned installed dependency set. The separate CI security job audits its resolved development/server environment. An audit is a point-in-time check against known advisories.
- CI configuration: Ubuntu and Windows, Python 3.10–3.14, test/lint/type/build/clean-wheel checks; separate security and official AsyncAPI validation jobs. Remote execution status is available in the repository's Actions history for the audit commit.

Heartbeat renewal is tested with an observed SQLite expiry update and a controlled clock, so ownership assertions do not depend on runner scheduling.

Reproduction cases live in `tests/test_delivery_regressions.py`, `tests/test_recovery_surfaces.py`, and `tests/test_transport_adapters.py`. Existing adversarial, multiservice, benchmark-contract, end-to-end, provenance, and scrub suites remain enabled.

## Recovery and operational boundaries

Claims last 30 seconds and renew every 10 seconds while a cooperating worker is active. A stopped or crashed worker can be retried after expiration. Completion and attempt writes require the same owner, so reclaimed work cannot be completed by its previous owner. An external effect followed by a crash before persistence can still be repeated: consumers must be idempotent. Duplicate receipts alone do not promise completion. The `/v1` gateway returns a retryable error for an unfinished durable duplicate.

Stop all 0.3.0 workers before upgrading an existing database. Columns are added automatically; abandoned legacy `processing` records are eligible for recovery. Historical consumer results receive digests at upgrade time, which cannot retrospectively authenticate their earlier contents. New and existing rows are subsequently checked when read. `pending_outbox()` exposes eligible deliveries; applications must drive its publication loop. `dead_letters()` recovers persisted terminal diagnostics; the separate queue remains bounded and in-process.

SQLite WAL is used on a local filesystem, not a distributed filesystem. Cross-machine failover, network partition fencing, sustained throughput, backup/restore procedures and broker-specific deployments need additional environment-specific validation. Lease expiration uses wall-clock time and assumes synchronized clocks. Process stalls exceeding a lease can repeat external effects even while stale database writes are fenced.

NATS and Kafka tests exercise compatible client APIs, not deployed brokers. NATS core publish/flush is not a JetStream durable receipt. Kafka client lifecycle and offsets are owned by the caller. No broker failover or load certification is claimed. The legacy HTTP adapter is unauthenticated; production deployments should use the `/v1` gateway with credentials and deployment-managed transport security.

Digest checks reveal inconsistent stored contents; they do not protect against an actor able to replace both contents and digests. HMAC signatures require trusted externally managed secrets; HTTP gateway authentication does not automatically verify envelope signatures. Scientific meaning, artifact byte identity, disease phenotype correctness, benchmark data quality and clinical performance remain outside CardiBridge's software-contract validation.
