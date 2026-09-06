# Virelion-CardiBridge

CardiBridge is the typed interoperability and protocol library for Virelion services. It defines versioned message contracts, validation, routing, persistence boundaries, and compatibility rules between components.

## Scope

- strict Pydantic message contracts;
- versioned contract registry and schema fingerprints;
- explicit migrations and compatibility checks;
- canonical JSON and SHA-256 payload identity;
- HMAC signing primitives and authorization primitives;
- SQLite inbox/outbox/audit log;
- idempotent routing and replay;
- synchronous and asynchronous transport abstractions;
- persist-before-publish integration boundary;
- conformance tests and contract catalog export;
- delivery, validation, failure, and latency metrics.

CardiBridge does not implement domain-specific cardiac algorithms.

## Protocol rules

1. Validate before dispatch.
2. Persist before external publish where the durable adapter is used.
3. Require an idempotency key.
4. Treat schema evolution as an explicit migration.
5. Carry trace and provenance metadata with messages.
6. Use deterministic serialization for hashing/signing.
7. Keep transport implementations separate from scientific contracts.

## Contract families

| Contract | Producer | Consumer | Purpose |
|---|---|---|---|
| `agent.challenge` | CardiAgent | CardiVex | challenge/task definition |
| `vex.observation` | CardiVex | downstream/evaluation | structured observations |
| `eval.request` | model/service | CardiEval | evaluation request |
| `eval.result` | CardiEval | downstream | evaluation result |

## Installation

```bash
pip install -e '.[test]'
```

## Python

```python
from cardibridge.builtin import default_registry
from cardibridge.production import ProductionRouter

registry = default_registry()
router = ProductionRouter(registry)
router.register("agent.challenge", "CardiVex", lambda envelope: {"accepted": True})
```

## Integration

CardiBridge is the protocol boundary used by HeartTwin and sibling Virelion services. It should remain independent of model implementation details so services can be upgraded without changing domain algorithms.

## Limitations

A valid protocol message demonstrates contract compliance, not scientific validity. Authorization/signing primitives require appropriate key and trust management in deployment.

## License

GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later). See `LICENSE`.

## Citation

Cite the repository release and the contract/schema version used.
