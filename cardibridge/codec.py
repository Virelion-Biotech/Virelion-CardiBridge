from __future__ import annotations

import base64
import json
from typing import Any

from .contracts import BridgeEnvelope
from .protocol import canonical_json


def _reject_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant is not permitted: {value}")


class EnvelopeCodec:
    """Canonical wire codec with strict JSON and optional base64 framing."""

    @staticmethod
    def encode(envelope: BridgeEnvelope) -> bytes:
        return canonical_json(envelope.model_dump(mode="json"))

    @staticmethod
    def decode(data: bytes | str) -> BridgeEnvelope:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="strict")
        value: Any = json.loads(
            data,
            parse_constant=_reject_constant,
        )
        if not isinstance(value, dict):
            raise TypeError("envelope wire representation must be a JSON object")
        return BridgeEnvelope.model_validate(value)

    @classmethod
    def encode_base64(cls, envelope: BridgeEnvelope) -> str:
        return base64.b64encode(cls.encode(envelope)).decode("ascii")

    @classmethod
    def decode_base64(cls, value: str) -> BridgeEnvelope:
        if not value:
            raise ValueError("base64 envelope must not be empty")
        return cls.decode(base64.b64decode(value, validate=True))
