from __future__ import annotations

from typing import Any

from .registry import ContractRegistry


def export_catalog(registry: ContractRegistry) -> dict[str, Any]:
    """Return the stable machine-readable contract catalog."""
    contracts: dict[str, Any] = {}
    for name in registry.names():
        contracts[name] = {
            "version": registry.version(name),
            "fingerprint": registry.fingerprint(name),
            "schema": registry.model(name).model_json_schema(),
        }
    return {
        "protocol": "virelion-cardibridge",
        "version": "1.0.0",
        "contracts": contracts,
    }


def export_asyncapi(registry: ContractRegistry) -> dict[str, Any]:
    """Build a dependency-free AsyncAPI-compatible contract description."""
    messages: dict[str, Any] = {}
    channels: dict[str, Any] = {}
    for name in registry.names():
        message_name = name.replace(".", "_")
        messages[message_name] = {
            "name": message_name,
            "title": name,
            "contentType": "application/json",
            "payload": registry.model(name).model_json_schema(),
            "x-cardibridge-version": registry.version(name),
            "x-cardibridge-fingerprint": registry.fingerprint(name),
        }
        channels[name] = {
            "address": name,
            "messages": {message_name: {"$ref": f"#/components/messages/{message_name}"}},
        }
    return {
        "asyncapi": "3.0.0",
        "info": {
            "title": "Virelion CardiBridge Protocol",
            "version": "1.0.0",
            "description": "Contract-first interoperability surface for Virelion computational services.",
        },
        "channels": channels,
        "components": {"messages": messages},
    }
