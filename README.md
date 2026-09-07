# Virelion-CardiBridge

CardiBridge is a typed interoperability and protocol library for exchanging structured computational messages. It defines versioned message contracts, validation, routing, persistence boundaries, and compatibility rules between components.

## What it contains

- Strict Pydantic message contracts.
- Versioned contract registry and schema fingerprints.
- Explicit migrations and compatibility checks.
- Canonical JSON and SHA-256 payload identity.
- HMAC signing and authorization primitives.
- SQLite inbox/outbox/audit log.
- Idempotent routing and replay.
- Synchronous and asynchronous transport abstractions.
- Persist-before-publish boundary for durable adapters.
- Conformance tests and contract catalog export.
- Delivery, validation, failure, and latency metrics.

CardiBridge does not implement domain-specific cardiac algorithms.

## Protocol rules

1. Validate before dispatch.
2. Persist before external publish where the durable adapter is used.
3. Require an idempotency key.
4. Treat schema evolution as an explicit migration.
5. Carry trace and provenance metadata with messages.
6. Use deterministic serialization for hashing/signing.
7. Keep transport implementations separate from scientific contracts.

## Installation

```bash
pip install -e '.[test]'
```

## Usage

```python
from cardibridge.builtin import default_registry
from cardibridge.production import ProductionRouter

registry = default_registry()
router = ProductionRouter(registry)
router.register("example.message", "consumer", lambda envelope: {"accepted": True})
```

## Inputs and outputs

**Inputs:** versioned message envelopes, contract definitions, routing rules, idempotency keys, optional authorization/signing metadata, and transport configuration.

**Outputs:** validated/canonicalized messages, routing results, persisted inbox/outbox/audit records, compatibility results, delivery metrics, and contract catalogs.

## Validation

The repository includes contract conformance and compatibility tests. Messages are validated before dispatch, schema fingerprints are checked, and explicit migrations are required for incompatible schema evolution.

Protocol compliance does not establish scientific correctness or validity of the payload's domain content.

## Limitations

CardiBridge validates and transports structured messages; it does not validate the scientific meaning of domain-specific data. Authorization and signing primitives require appropriate key storage, rotation, and trust management in deployment. Transport reliability depends on the selected adapter and deployment environment.

## License

GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later). See `LICENSE`.
