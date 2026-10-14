# Virelion-CardiBridge

CardiBridge is the contract-first interoperability layer for the Virelion computational cardiac stack. It standardizes typed messages, schema evolution, provenance, artifact references, execution context, routing, delivery semantics, and transport adapters without embedding cardiac algorithms or a specific broker.


<!-- VALIDATION_PRODUCT_STATUS:BEGIN -->
## Validation and product completion status

**Reviewed 2026-10-14; implementation base `fc990ace415c`.** **Status: Contract/durable-local transport checks; broker deployment qualification incomplete.** This is an evidence-backed scope assessment, not a percentage-complete estimate or a declaration that the product is finished.

| Area | Current evidence / remaining gate |
|---|---|
| Software / operational checks | Automated test sources and CI definitions are present; this documentation audit does not report a new algorithm/data-validation run. |
| Scientific or domain evidence | Local HTTP and SQLite delivery have scoped integration evidence. NATS/Kafka client-double checks do not establish real broker failover, ACLs, persistence or throughput; transport correctness does not validate the payload science. |
| Remaining for the declared product scope | Run real NATS/Kafka broker failure/replay/conformance tests; qualify authentication/authorization, key rotation and delivery observability; test clock/lease and crash-idempotency behavior at deployment scale; preserve explicit consumer scientific validation responsibilities. |

Evidence and limits: [repository report](docs/VALIDATION_AUDIT_2026-10-07.md). The [organization status index](https://github.com/Virelion-Biotech/Virelion-HeartTwin/blob/main/docs/STACK_VALIDATION_STATUS.md) separates software, numerical, data-readiness and empirical qualification. Code/schema execution or a completed download is not a substitute for endpoint-specific validation.
<!-- VALIDATION_PRODUCT_STATUS:END -->

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

The durable adapter persists before external publication, records delivery attempts, and moves exhausted deliveries to its dead-letter queue. SQLite claims prevent simultaneous publication by cooperating workers and renew every 10 seconds during live work. An interrupted worker's claim becomes eligible for recovery after its 30-second lease expires. Consumers must remain idempotent: an external side effect can succeed immediately before a crash prevents its receipt from being recorded.

After restarting, call `pending_outbox()` and publish its envelopes through the durable adapter. Stored retry deadlines are honored. `store.dead_letters()` recovers exhausted delivery diagnostics even when the in-process dead-letter queue was lost. A duplicate receipt acknowledges an existing record; inspect its durable status to distinguish completed and in-flight work.

Upgrading a 0.3.0 database adds lease and consumer-result digest columns automatically. Stop all old workers before upgrading; old `processing` records become eligible for recovery. The existing `EventStore` API remains compatible. Use SQLite WAL on a local filesystem shared by processes on one machine, with synchronized wall clocks.

## Interoperability

The core contract model is independent of FastAPI, Kafka, NATS, RabbitMQ, Kubernetes, or cloud SDKs. Adapters translate between a selected transport and the same `BridgeEnvelope`. This keeps scientific contracts stable while infrastructure can evolve independently.

## Provenance

Use `ArtifactRef` for datasets, models, feature tables, simulation outputs, and other potentially large artifacts. Use `ExecutionContext` for workflow/task identity and `LineageEvent` for run/job/input/output relationships. `ProvenanceChain` provides tamper-evident commitments and `EventStore` provides local persistence and replay.

## Validation and limitations

CI runs Ruff, mypy, pytest (minimum 80% coverage), package builds, clean installed-wheel smoke tests, dependency consistency checks, the official AsyncAPI parser, and vulnerability auditing. The test matrix covers Ubuntu and Windows with Python 3.10 through 3.14. Protocol conformance does not establish scientific correctness. Scientific validity, model performance, data quality, and domain-specific safety remain the responsibility of the consuming service.

The live HTTP gateway and HTTP publisher are integration-tested over a local TCP connection. NATS and Kafka adapters are tested with compatible client doubles; broker failover, ACLs, persistence, and deployment throughput require a broker-specific integration environment. Core NATS publication and flush do not supply JetStream durable acknowledgement. Kafka client startup, shutdown, and offsets remain the caller's responsibility. The legacy HTTP adapter is unauthenticated; use the `/v1` gateway and configure `CARDIBRIDGE_GATEWAY_KEY` for protected message, encode, and replay operations.

Stored payloads, lineage, and consumer results are checked against their digests when read. These checks detect inconsistent data, not an attacker who can rewrite both payloads and digest columns. HMAC authentication requires application-managed trusted keys; gateway credentials are independent of envelope signatures. Contract registry versions are enforced before routing; explicitly migrate to a registered target schema before dispatching.

See [the CPU validation audit](docs/VALIDATION_AUDIT_2026-10-07.md) for evidence, repair details, and scope.

## License

GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later). See `LICENSE`.
