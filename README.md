# Virelion-CardiBridge

CardiBridge is the contract-first interoperability layer for the Virelion computational cardiac stack. It standardizes typed messages, schema evolution, provenance, artifact references, execution context, routing, delivery semantics, and transport adapters without embedding cardiac algorithms or a specific broker.

## What it contains

- Strict Pydantic message contracts with semantic versions.
- Artifact references, workflow/task execution context, trace context, and lineage facets.
- Runtime contract registry with validation and SHA-256 schema fingerprints.
- Machine-readable contract catalog and AsyncAPI 3-compatible export.
- Explicit compatibility negotiation and deterministic migrations.
- Canonical JSON encoding, content identity, HMAC signing, and authorization primitives.
- SQLite inbox/outbox/audit persistence with replay and delivery-attempt history.
- Mandatory idempotency and at-least-once delivery semantics.
- Bounded exponential retries with deterministic jitter and dead-letter handling.
- In-memory, HTTP, callback, NATS-client, and Kafka-client transport adapters.
- Circuit breaker, health/readiness, batching, conformance, and observability primitives.
- Optional FastAPI gateway that exposes contract, validation, catalog, and routing endpoints.

CardiBridge deliberately does **not** implement domain-specific cardiac algorithms or act as a workflow engine. Workflow orchestration belongs in a higher layer and can propagate its identity through `ExecutionContext`.

## Architecture

```text
CardiAgent / CardiVex / CardiEval / CardiLearn / HeartTwin
                         |
                         v
                  +--------------+
                  | CardiBridge   |
                  |--------------|
                  | Contracts     |
                  | Validation    |
                  | Compatibility |
                  | Routing       |
                  | Idempotency   |
                  | Provenance    |
                  | Delivery      |
                  +------+-------+
                         |
          +--------------+--------------+
          |              |              |
         HTTP           NATS          Kafka
          |              |              |
          +--------------+--------------+
                         |
                    external infra
```

## Installation

For development:

```bash
pip install -e '.[dev]'
```

For the optional HTTP gateway:

```bash
pip install -e '.[server]'
```

## Contract validation

```bash
cardibridge schema agent.challenge
cardibridge asyncapi --output asyncapi.json
cardibridge validate agent.challenge '{"challenge_type":"mi","population":[{"sample":"S1"}],"intended_task":"classify","trace":{"source":"CardiAgent"}}'
```

## Durable delivery

```python
from cardibridge import DurableTransportAdapter, EventStore, InMemoryTransport, RetryPolicy

store = EventStore("cardibridge.db")
transport = DurableTransportAdapter(
    store,
    InMemoryTransport(),
    RetryPolicy(max_attempts=5),
)
receipt = await transport.publish(envelope)
```

The durable adapter persists before external publication, records every attempt, and moves exhausted deliveries to its dead-letter queue. Consumers should remain idempotent because the protocol is intentionally at-least-once.

## Interoperability

The core contract model is independent of FastAPI, Kafka, NATS, RabbitMQ, Kubernetes, or cloud SDKs. Adapters translate between a selected transport and the same `BridgeEnvelope`. This keeps scientific contracts stable while infrastructure can evolve independently.

## Provenance

Use `ArtifactRef` for datasets, models, feature tables, simulation outputs, and other potentially large artifacts. Use `ExecutionContext` for workflow/task identity and `LineageEvent` for run/job/input/output relationships. `ProvenanceChain` provides tamper-evident commitments and `EventStore` provides local persistence and replay.

## Validation and limitations

CI runs Ruff, mypy, pytest, package builds, dependency consistency checks, and vulnerability auditing across supported Python versions. Protocol conformance does not establish scientific correctness. Scientific validity, model performance, data quality, and domain-specific safety remain the responsibility of the consuming service.

## License

GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later). See `LICENSE`.
