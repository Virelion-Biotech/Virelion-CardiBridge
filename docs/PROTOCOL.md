# CardiBridge Protocol v1

CardiBridge is the canonical message boundary for Virelion services. Domain services own scientific computation; CardiBridge owns interoperability, validation, delivery semantics, provenance and replay.

## Wire contract

Every message is a `BridgeEnvelope` containing:

- `message_id`: globally unique message identifier
- `message_type`: registered contract name
- `producer` / `consumer`: protocol actors
- `idempotency_key`: required duplicate-suppression key
- `payload`: validated contract payload
- `trace`: provenance and distributed-tracing context
- `timestamp`: UTC creation time
- `signature`: optional integrity/authentication material

## Delivery invariants

1. Validate the contract before invoking a consumer.
2. Persist before external publication when using the durable adapter.
3. Treat duplicate idempotency keys as already accepted.
4. Never silently migrate a scientific payload.
5. Retry only under an explicit policy.
6. Move exhausted deliveries to a dead-letter boundary.
7. Preserve trace and provenance through every hop.
8. Canonical serialization must be deterministic before hashing/signing.

## Contract lifecycle

`draft -> experimental -> stable -> deprecated -> retired`

Only registered migrations may cross schema versions. A migration must be deterministic, tested and reversible at the policy level when reversibility is scientifically required.

## Canonical topics

Topics are derived from the contract name and protocol namespace. Example:

`virelion.cardibridge.agent.challenge`

Transport adapters may map this logical topic to Kafka, NATS, RabbitMQ, HTTP or another infrastructure without changing the contract.

## Failure model

Failures are classified as validation, authorization, delivery, handler or compatibility failures. Retryable failures use `RetryPolicy`; terminal or exhausted failures become `DeadLetter` records. Operators can inspect and replay messages without modifying the original envelope.

## Scientific integrity

CardiBridge must not infer missing labels, fabricate uncertainty, alter ground truth, or silently normalize incompatible scientific semantics. If a conversion can change scientific meaning, it belongs in an explicit, versioned migration owned by the domain contract.
