# CardiBridge Protocol

CardiBridge is Virelion's **interoperability and protocol layer**. It is the typed, versioned, validated boundary between CardiAgent, CardiVex, CardiEval, CardiTrace and future Virelion services.

## Architecture

```text
CardiAgent ── agent.challenge ──┐
                                │
CardiVex   ── vex.observation ──┼──> CardiBridge ──> routing / validation / persistence / provenance
                                │
CardiEval  ── eval.request ─────┘                         │
                                                         ▼
                                                   eval.result
```

## Production capabilities

- Typed Pydantic contracts with strict fields.
- Versioned contract registry and deterministic schema fingerprints.
- Explicit migration/compatibility gates; no silent scientific payload coercion.
- Canonical JSON and SHA-256 content addressing.
- HMAC signing primitives and principal/scope authorization primitives.
- Durable SQLite inbox/outbox/audit event log with WAL, idempotency and replay.
- Deterministic in-process router plus a production router with persistence and metrics.
- Async transport abstraction and reference in-memory transport.
- Durable persist-before-publish adapter for external brokers.
- Conformance test primitives for Agent/Vex/Eval contract compatibility.
- Machine-readable contract catalog export suitable for API/schema registries.
- Observable delivery, duplicate, validation, failure and latency metrics.

## Protocol invariants

1. **Validate before dispatch.** Unknown or malformed contracts never reach a consumer.
2. **Persist before external publish.** The durable adapter implements an application-level outbox boundary.
3. **Idempotency is mandatory.** Every envelope carries an idempotency key.
4. **Schema evolution is explicit.** Version changes require a registered migration.
5. **Provenance travels with the message.** Trace IDs, parent spans, source and provenance metadata remain part of the contract.
6. **Canonical bytes are reproducible.** Hashing/signing uses deterministic JSON serialization.
7. **Transport is replaceable.** Kafka, NATS, HTTP or another broker can implement the transport protocol without changing scientific contracts.

## Quick start

```python
from cardibridge.builtin import default_registry
from cardibridge.production import ProductionRouter

registry = default_registry()
router = ProductionRouter(registry)

router.register("agent.challenge", "CardiVex", lambda envelope: {"accepted": True})
```

## Contract families

| Contract | Producer | Consumer | Purpose |
|---|---|---|---|
| `agent.challenge` | CardiAgent | CardiVex | Challenge population and task definition |
| `vex.observation` | CardiVex | downstream/evaluation | Structured observations and evidence |
| `eval.request` | model/service | CardiEval | Evaluation request and predictions |
| `eval.result` | CardiEval | downstream | Metrics, uncertainty and reproducibility |

CardiBridge deliberately does **not** own domain-specific algorithms. Its job is to make those algorithms composable, auditable, replayable and version-safe.
