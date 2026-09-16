from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .catalog import export_asyncapi
from .defaults import default_registry
from .registry import ContractRegistry


def build_registry() -> ContractRegistry:
    return default_registry()


def _load_json(value: str) -> object:
    if value.startswith("@"):
        path = Path(value[1:])
        raw = path.read_text(encoding="utf-8")
    else:
        raw = value
    return json.loads(raw)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cardibridge")
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate")
    validate.add_argument("contract")
    validate.add_argument("payload", help="JSON object or @file.json")

    schema = sub.add_parser("schema")
    schema.add_argument("contract")

    asyncapi = sub.add_parser("asyncapi")
    asyncapi.add_argument("--output", help="Write the AsyncAPI-compatible document to a JSON file")

    args = parser.parse_args(argv)
    registry = build_registry()

    try:
        if args.command == "schema":
            print(json.dumps(registry.model(args.contract).model_json_schema(), indent=2))
            return 0

        if args.command == "asyncapi":
            document = export_asyncapi(registry)
            encoded = json.dumps(document, indent=2) + "\n"
            if args.output:
                Path(args.output).write_text(encoded, encoding="utf-8")
            else:
                print(encoded, end="")
            return 0

        payload = _load_json(args.payload)
        if not isinstance(payload, dict):
            print(json.dumps({"valid": False, "error": "payload must be a JSON object"}))
            return 2
        report = registry.validate(args.contract, payload)
        print(report.model_dump_json(indent=2))
        return 0 if report.valid else 2
    except FileNotFoundError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}))
        return 2
    except json.JSONDecodeError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}))
        return 2
    except KeyError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}))
        return 2
    except OSError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
