# CardiBridge Architecture

## Contract graph

| Contract | Producer | Consumer | Role |
|---|---|---|---|
| `agent.challenge` | CardiAgent | CardiVex | Challenge population + task definition |
| `vex.observation` | CardiVex | CardiEval / CardiTrace | Detected challenge, phenotype and evidence |
| `eval.request` | orchestrator | CardiEval | Predictions, ground truth, metrics and split |
| `eval.result` | CardiEval | CardiTrace / orchestrator | Metrics, uncertainty and reproducibility |

## Envelope invariants

Every cross-service message has a unique message ID, explicit producer/consumer, idempotency key, trace context, schema message type and timestamp. Payloads are validated before dispatch. Duplicate idempotency keys are not executed twice.

## Compatibility strategy

Contracts use semantic versions. Additive fields should normally be introduced without breaking consumers. Breaking changes require a new major contract version or an explicit migration. `MigrationRegistry` provides a deterministic migration hook rather than implicit coercion.

## Security

`security.py` implements HMAC-SHA256 signing and constant-time verification. Production deployments should place key management outside the repository, rotate keys, and bind authorization to producer/consumer identity. Signatures protect integrity; they do not replace authentication, authorization, TLS, or replay controls.

## Persistence

`EventStore` provides a minimal SQLite audit/idempotency backend. Distributed deployments should replace it with a transactional datastore or durable event infrastructure while retaining the same semantics.

## Transport independence

The core package does not depend on FastAPI, Kafka, NATS, RabbitMQ or cloud services. This prevents transport choices from contaminating scientific contracts. `http.py` is an optional HTTP adapter; future adapters should translate transport envelopes into the same `BridgeEnvelope`.
