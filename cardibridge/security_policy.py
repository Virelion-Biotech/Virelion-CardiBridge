from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Principal:
    subject: str
    roles: frozenset[str] = frozenset()
    scopes: frozenset[str] = frozenset()

    def allows(self, scope: str) -> bool:
        return "*" in self.scopes or scope in self.scopes


class Authorizer:
    """Minimal policy engine; applications can replace it with OAuth/JWT/mTLS policy."""

    def __init__(self) -> None:
        self._policies: dict[tuple[str, str], set[str]] = {}

    def allow(self, principal: str, action: str, resources: Iterable[str]) -> None:
        self._policies.setdefault((principal, action), set()).update(resources)

    def check(self, principal: Principal, action: str, resource: str) -> bool:
        if "admin" in principal.roles or principal.allows(action):
            return True
        return resource in self._policies.get((principal.subject, action), set())


def sign_bytes(payload: bytes, secret: bytes) -> str:
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()


def verify_bytes(payload: bytes, signature: str, secret: bytes) -> bool:
    expected = sign_bytes(payload, secret)
    return hmac.compare_digest(expected, signature)
