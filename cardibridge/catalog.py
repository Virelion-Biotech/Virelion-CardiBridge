from __future__ import annotations

from typing import Any

from .contracts import BridgeEnvelope
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
    schemas: dict[str, Any] = {}
    messages: dict[str, Any] = {}
    channels: dict[str, Any] = {}
    operations: dict[str, Any] = {}

    for name in registry.names():
        message_name = name.replace(".", "_")
        channel_name = message_name
        schema = BridgeEnvelope.model_json_schema()
        contract_schema = registry.model(name).model_json_schema()
        definitions = schema.pop("$defs", {})
        definitions.update(contract_schema.pop("$defs", {}))
        schema["properties"]["payload"] = contract_schema

        def relocate(value: Any, prefix: str = message_name) -> Any:
            if isinstance(value, list):
                return [relocate(item) for item in value]
            if isinstance(value, dict):
                return {
                    key: (
                        f"#/components/schemas/{prefix}_{item.split('/')[-1]}"
                        if key == "$ref" and item.startswith("#/$defs/")
                        else relocate(item)
                    )
                    for key, item in value.items()
                }
            return value

        for definition, value in definitions.items():
            schemas[f"{message_name}_{definition}"] = relocate(value)
        messages[message_name] = {
            "name": message_name,
            "title": name,
            "contentType": "application/json",
            "payload": relocate(schema),
            "x-cardibridge-version": registry.version(name),
            "x-cardibridge-fingerprint": registry.fingerprint(name),
        }
        channels[channel_name] = {
            "address": f"virelion.{name}.v{registry.version(name).split('.')[0]}",
            "messages": {message_name: {"$ref": f"#/components/messages/{message_name}"}},
        }
        for action in ("send", "receive"):
            operations[f"{action}_{message_name}"] = {
                "action": action,
                "channel": {"$ref": f"#/channels/{channel_name}"},
                "messages": [{"$ref": f"#/channels/{channel_name}/messages/{message_name}"}],
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
        "components": {"messages": messages, "schemas": schemas},
    }
