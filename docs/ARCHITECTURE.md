# CardiBridge Architecture

CardiBridge is the interoperability boundary between Virelion computational services. It owns protocol contracts, validation, identity, routing, delivery semantics, provenance and observability. It does not own scientific algorithms or workflow orchestration.

## Contract graph

| Contract | Producer | Consumer | Role |
|---|---|---|---|
| `agent.challenge` | CardiAgent | CardiVex | Challenge population + task definition |
| `vex.observation` | CardiVex | CardiEval / CardiTrace | Detected challenge, phenotype and evidence |
| `eval.request` | orchestrator | CardiEval | Predictions, ground truth, metrics and split |
| `eval.result` | CardiEval | CardiTrace / orchestrator | Metrics, uncertainty and reproducibility |

## Core layers

```text
Canonical contracts
      |
      v
BridgeEnvelope -> validation -> routing -> delivery -> audit/provenance
      |                               |
      |                               +--> retry / circuit breaker / DLQ
      |
      +--> artifact refs / execution context / lineage facets

Transport adapters remain outside the contract model:
HTTP | callback | NATS client | Kafka producer | future adapters
```

## Envelope invariants

Every cross-service message has a unique message ID, explicit producer/consumer, idempotency key, trace context, schema message type and timestamp. Artifact references and execution context may accompany the envelope without embedding artifact bytes. Payloads are validated before dispatch. Duplicate idempotency keys are not executed twice.

## Contract-first interoperability

Pydantic models are the runtime representation. `ContractRegistry` provides immutable version registration, validation and schema fingerprints. `export_catalog()` emits a machine-readable registry and `export_asyncapi()` emits an AsyncAPI 3-compatible description without making the core depend on AsyncAPI tooling.

Schema evolution is explicit: additive changes should remain compatible; breaking changes require a new contract version or an explicitly registered migration. Silent field dropping, reinterpretation or scientific-value coercion is prohibited.

## Provenance and lineage

`ArtifactRef` represents a stable scientific artifact reference by ID, URI and optional digest. `ExecutionContext` propagates workflow/task identity and attempts. `LineageEvent` models run/job/input/output relationships and extensible facets. `ProvenanceChain` stores tamper-evident hash commitments, while `EventStore` can persist lineage records for later audit.

## Delivery semantics

CardiBridge uses **at-least-once delivery with mandatory idempotency**. `DurableTransportAdapter` persists an outbox record before external publication, records delivery attempts, supports bounded exponential retry with deterministic jitter, and moves exhausted messages to a dead-letter queue. The SQLite implementation is a local reference backend; distributed deployments should retain these semantics with a transactional datastore or durable broker.

## Transport independence

The core package does not import FastAPI, Kafka, NATS, RabbitMQ or cloud SDKs. `HttpTransport`, `NatsTransport` and `KafkaTransport` are thin adapters around injected clients or standard-library HTTP. This keeps infrastructure replaceable and prevents transport-specific concepts from contaminating scientific contracts.

## Operational separation

CardiBridge is not a workflow engine. Workflow orchestration should live in a higher layer using the `ExecutionContext` and envelope protocol. This keeps the bridge small enough to embed in CardiAgent, CardiVex, CardiEval, CardiTrace and HeartTwin components without forcing a single execution engine on the ecosystem.

## Security boundary

HMAC-SHA256 signing and authorization primitives are reference mechanisms. Production deployments should use managed key storage, service authentication, authorization, TLS and replay protection. A signature establishes message integrity; it does not establish identity by itself.
