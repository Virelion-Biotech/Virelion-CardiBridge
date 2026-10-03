from __future__ import annotations

import hashlib
import hmac
from collections.abc import Iterable
from dataclasses import dataclass, field
from threading import RLock


def _nonempty(value: str, label: str) -> str:
    if not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value


@dataclass(frozen=True)
class Principal:
    subject: str
    roles: frozenset[str] = field(default_factory=frozenset)
    scopes: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        _nonempty(self.subject, "subject")
        if any(not role.strip() for role in self.roles):
            raise ValueError("roles must not contain empty values")
        if any(not scope.strip() for scope in self.scopes):
            raise ValueError("scopes must not contain empty values")

    def allows(self, scope: str) -> bool:
        _nonempty(scope, "scope")
        return "*" in self.scopes or scope in self.scopes


class Authorizer:
    """Minimal policy engine; applications can replace it with OAuth/JWT/mTLS policy."""

    def __init__(self) -> None:
        self._policies: dict[tuple[str, str], set[str]] = {}
        self._lock = RLock()

    def allow(self, principal: str, action: str, resources: Iterable[str]) -> None:
        _nonempty(principal, "principal")
        _nonempty(action, "action")
        resource_set = {_nonempty(resource, "resource") for resource in resources}
        with self._lock:
            self._policies.setdefault((principal, action), set()).update(resource_set)

    def check(self, principal: Principal, action: str, resource: str) -> bool:
        _nonempty(action, "action")
        _nonempty(resource, "resource")
        with self._lock:
            if "admin" in principal.roles or principal.allows(action):
                return True
            return resource in self._policies.get((principal.subject, action), set())


def sign_bytes(payload: bytes, secret: bytes) -> str:
    if not secret:
        raise ValueError("signing secret must not be empty")
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()


def verify_bytes(payload: bytes, signature: str, secret: bytes) -> bool:
    if not signature or not secret:
        return False
    expected = sign_bytes(payload, secret)
    return hmac.compare_digest(expected, signature)
