from __future__ import annotations

import hashlib
import hmac

from .contracts import BridgeEnvelope
from .protocol import canonical_json


def canonical_envelope(envelope: BridgeEnvelope) -> bytes:
    """Return the same canonical representation used by protocol digests."""
    return canonical_json(envelope.model_dump(mode="json", exclude={"signature"}))


def sign_envelope(envelope: BridgeEnvelope, secret: bytes) -> str:
    if not secret:
        raise ValueError("signing secret must not be empty")
    return hmac.new(secret, canonical_envelope(envelope), hashlib.sha256).hexdigest()


def verify_envelope(envelope: BridgeEnvelope, secret: bytes) -> bool:
    if not secret or not envelope.signature:
        return False
    expected = sign_envelope(envelope, secret)
    return hmac.compare_digest(expected, envelope.signature)
