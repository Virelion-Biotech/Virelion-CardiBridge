# Changelog

All notable changes to Virelion-CardiBridge are documented here.

## [0.3.0] - 2026-09-15

### Added

- Durable SQLite-backed inbox/outbox persistence with replay and delivery-attempt history.
- At-least-once delivery semantics with mandatory idempotency handling.
- Bounded exponential retry policy with deterministic jitter.
- Dead-letter lifecycle for exhausted deliveries.
- Broker-neutral HTTP, callback, NATS-client, and Kafka-client transport adapters.
- Contract registry fingerprints and AsyncAPI-compatible catalog export.
- Explicit compatibility negotiation and registered migrations.
- Provenance chain, lineage events, artifact references, and execution context primitives.
- Optional FastAPI gateway with health/readiness, validation, catalog, encoding, routing, and replay surfaces.
- Strict mypy, Ruff, dependency consistency, package build, installation, coverage, and vulnerability gates in CI.

### Changed

- Canonical JSON now has a single bytes-returning boundary used consistently by the wire codec and hashing functions.
- Package version metadata is exposed as `cardibridge.__version__` and aligned to `0.3.0`.
- Transport subscription interfaces are explicitly typed as async iterators.
- README examples now match the actual durable adapter API.

### Validation

- Supported Python matrix: 3.10, 3.11, and 3.12.
- Ruff and strict mypy pass in CI.
- Test coverage is enforced at a minimum of 60%.
- Built wheel installation and runtime version assertion are exercised in CI.
- `pip check` and `pip-audit` are required release gates.

[0.3.0]: https://github.com/Virelion-Biotech/Virelion-CardiBridge/releases/tag/v0.3.0
