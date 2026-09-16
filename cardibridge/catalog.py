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
    """Build a valid AsyncAPI 3.0 document from registered contracts."""
    messages: dict[str, Any] = {}
    channels: dict[str, Any] = {}
    operations: dict[str, Any] = {}

    for name in registry.names():
        message_name = name.replace(".", "_")
        channel_name = message_name
        messages[message_name] = {
            "name": message_name,
            "title": name,
            "contentType": "application/json",
            "payload": registry.model(name).model_json_schema(),
            "x-cardibridge-version": registry.version(name),
            "x-cardibridge-fingerprint": registry.fingerprint(name),
        }
        channels[channel_name] = {
            "address": name,
            "messages": {message_name: {"$ref": f"#/components/messages/{message_name}"}},
        }
        for action in ("send", "receive"):
            operations[f"{action}_{message_name}"] = {
                "action": action,
                "channel": {"$ref": f"#/channels/{channel_name}"},
                "messages": [{"$ref": f"#/components/messages/{message_name}"}],
            }

    return {
        "asyncapi": "3.0.0",
        "info": {
            "title": "Virelion CardiBridge Protocol",
            "version": "1.0.0",
            "description": "Contract-first interoperability surface for Virelion computational services.",
        },
        "channels": channels,
        "operations": operations,
        "components": {"messages": messages},
    }
