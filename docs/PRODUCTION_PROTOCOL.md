# CardiBridge production protocol

CardiBridge is the interoperability boundary between Virelion services. Scientific payloads are validated before dispatch and schema evolution is explicit.

## Wire contract

External transports SHOULD carry a frame with:

- `protocol`: `cardibridge`
- `wire_version`: integer wire version
- `content_type`: canonical JSON media type
- `payload`: the typed BridgeEnvelope representation
- `content_digest`: SHA-256 of canonical payload bytes
- optional `key_id` and `signature`

## Delivery semantics

The protocol uses **at-least-once delivery with mandatory idempotency**. Consumers must safely handle duplicate envelopes. Durable publishers persist an outbox record before attempting external publication.

## Security

Deployments SHOULD terminate TLS at the gateway or broker boundary, authenticate service principals, authorize contract/topic access, and rotate signing keys. HMAC is provided as a reference mechanism; production deployments may replace it with asymmetric signatures while retaining the same payload commitment model.

## Failure handling

Transient failures are retried according to bounded exponential backoff. Repeated failures enter the dead-letter lifecycle. Circuit breakers prevent persistent downstream failure from causing unbounded cascading load. Replay must preserve the original envelope identity and provenance.

## Compatibility

A contract version is immutable. Producers and consumers negotiate a mutually supported version or use an explicit registered migration. Silent field dropping, reinterpretation, or scientific-value coercion is prohibited.

## Operational endpoints

The optional HTTP gateway exposes `/health`, `/ready`, `/v1/contracts`, `/v1/validate`, and `/v1/encode`. FastAPI remains optional so the protocol core stays transport-independent.
