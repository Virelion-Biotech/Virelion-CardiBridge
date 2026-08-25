from __future__ import annotations

import base64
import json
from typing import Any

from .contracts import BridgeEnvelope
from .protocol import canonical_json


class EnvelopeCodec:
    """Canonical wire codec with strict JSON and optional base64 framing."""

    @staticmethod
    def encode(envelope: BridgeEnvelope) -> bytes:
        return canonical_json(envelope.model_dump(mode="json")).encode("utf-8")

    @staticmethod
    def decode(data: bytes | str) -> BridgeEnvelope:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        value: Any = json.loads(data)
        if not isinstance(value, dict):
            raise ValueError("envelope wire representation must be a JSON object")
        return BridgeEnvelope.model_validate(value)

    @classmethod
    def encode_base64(cls, envelope: BridgeEnvelope) -> str:
        return base64.b64encode(cls.encode(envelope)).decode("ascii")

    @classmethod
    def decode_base64(cls, value: str) -> BridgeEnvelope:
        return cls.decode(base64.b64decode(value, validate=True))
