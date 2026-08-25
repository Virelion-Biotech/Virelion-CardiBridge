from __future__ import annotations

import hashlib
import hmac
import json

from .contracts import BridgeEnvelope


def canonical_envelope(envelope: BridgeEnvelope) -> bytes:
    data = envelope.model_dump(mode="json", exclude={"signature"})
    return json.dumps(data, sort_keys=True, separators=(",", ":")).encode()


def sign_envelope(envelope: BridgeEnvelope, secret: bytes) -> str:
    return hmac.new(secret, canonical_envelope(envelope), hashlib.sha256).hexdigest()


def verify_envelope(envelope: BridgeEnvelope, secret: bytes) -> bool:
    if not envelope.signature:
        return False
    expected = sign_envelope(envelope, secret)
    return hmac.compare_digest(expected, envelope.signature)
