# CardiBridge production protocol

CardiBridge is the interoperability boundary between Virelion services. Scientific payloads are validated before dispatch and schema evolution is explicit.

## Wire contract

External transports SHOULD carry a frame with:

- `protocol`: `cardibridge`
- `wire_version`: integer wire version
- `content_type`: canonical JSON media type
- `payload`: the typed `BridgeEnvelope` representation
- `content_digest`: SHA-256 of canonical payload bytes
- optional `key_id` and `signature`

The canonical payload excludes the signature when computing the envelope digest.

## Delivery semantics

The protocol uses **at-least-once delivery with mandatory idempotency**. Consumers must safely handle duplicates. Durable publishers persist an outbox record before attempting external publication. Every delivery attempt is recorded with success/failure and the next retry time.

`RetryPolicy` provides bounded exponential backoff. When jitter is enabled, the jitter factor is deterministic for a given idempotency key and attempt, which keeps retry planning reproducible in tests and audit records.

## Failure handling

Transient failures are retried according to bounded backoff. Repeated failures enter the dead-letter lifecycle with the original envelope and recorded attempts. Circuit breakers remain available to prevent persistent downstream failure from causing unbounded cascading load. Replay preserves original message identity, idempotency and provenance metadata.

## Transport adapters

The contract core is broker-neutral. The repository includes:

- `InMemoryTransport` for deterministic testing.
- `HttpTransport` using the standard library.
- `CallbackTransport` for dependency-injected client SDKs.
- `NatsTransport` and `KafkaTransport` as thin broker-client adapters that do not add those libraries to core dependencies.

Production applications can therefore choose NATS, Kafka, HTTP or another provider without changing scientific contracts.

## Compatibility

A contract version is immutable. Producers and consumers negotiate a mutually supported version or use an explicit registered migration. Silent field dropping, reinterpretation, or scientific-value coercion is prohibited. Contract fingerprints provide an additional deployment-time identity check.

## Provenance and artifacts

Large scientific data should be represented by `ArtifactRef` rather than embedded in messages. `ExecutionContext` propagates workflow/task identity. `LineageEvent` records run/job/input/output relationships and extensible facets. SQLite `EventStore` persistence is a reference implementation for local audit and replay; distributed deployments should provide equivalent transactional semantics.

## Operational endpoints

The optional HTTP application exposes contract validation and catalog surfaces, including `/health`, `/contracts`, `/asyncapi`, `/validate/{contract}`, `/envelope/validate`, and `/route`.

## Security boundary

Deployments SHOULD terminate TLS at the gateway or broker boundary, authenticate service principals, authorize contract/topic access, and rotate signing keys. HMAC is provided as a reference mechanism; production deployments may replace it with asymmetric signatures while retaining the same payload commitment model.
