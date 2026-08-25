# Virelion CardiBridge

**Contract-first interoperability and provenance fabric for the Virelion cardiac AI ecosystem.**

CardiBridge is the formal boundary between **CardiAgent → CardiVex → CardiEval**, while remaining extensible to CardiAtlas, CardiLearn, CardiSim, CardiTrace and CardiStudio. It is deliberately transport-agnostic: services exchange typed, versioned envelopes rather than sharing fragile internal Python objects.

## What this provides

- Strict Pydantic contracts with forward-compatible versioning.
- Canonical message envelopes, trace IDs, provenance and idempotency keys.
- Runtime contract registry, validation reports and schema fingerprints.
- Deterministic routing with duplicate-delivery protection.
- Machine-readable JSON Schema generated from every contract.
- CLI validation and schema inspection.
- A foundation for HTTP, queues, event buses and gRPC adapters without coupling the core.
- Audit-friendly metadata suitable for CardiTrace.
- Explicit separation of observations, generated challenges, predictions and evaluations.

## Core flow

```text
CardiAgent
   │ agent.challenge
   ▼
CardiBridge ───────────────► CardiVex
   │                            │
   │ vex.observation            │ prediction / evidence
   ▼                            ▼
CardiBridge ───────────────► CardiEval
   │                            │
   └──────── provenance ────────┘
```

## Design principles

1. **Contracts before transport.** HTTP, Kafka, NATS, RabbitMQ or gRPC are adapters, not the source of truth.
2. **Fail closed.** Unknown contracts, malformed payloads and missing routes are rejected.
3. **Reproducibility first.** Every message can carry trace and provenance information.
4. **Idempotent by design.** Consumers can safely receive the same envelope more than once.
5. **No hidden coupling.** Downstream services depend on stable schemas, not implementation details.
6. **Scientific integrity.** CardiBridge transports evidence and uncertainty; it does not silently manufacture them.

## Quick start

```bash
pip install -e '.[dev]'
pytest
cardibridge schema agent.challenge
cardibridge validate agent.challenge @examples/agent_challenge.json
```

## Repository status

This repository is intentionally being developed as infrastructure rather than a demo. The next expansion layers are transport adapters, persistent event/audit storage, schema migration tooling, cryptographic message signing, OpenAPI generation, async execution, dead-letter handling, distributed idempotency, and conformance tests against every Virelion service.

License: AGPL-3.0-or-later.
