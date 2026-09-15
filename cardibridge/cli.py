import argparse
import json
import sys
from pathlib import Path

from .registry import ContractRegistry
from .schemas import SCHEMAS, export_asyncapi


def build_registry() -> ContractRegistry:
    registry = ContractRegistry()
    for name, model in SCHEMAS.items():
        registry.register(name, model)
    return registry


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
    if args.command == "schema":
        print(json.dumps(registry.model(args.contract).model_json_schema(), indent=2))
        return 0
    if args.command == "asyncapi":
        document = export_asyncapi(registry)
        encoded = json.dumps(document, indent=2)
        if args.output:
            Path(args.output).write_text(encoded + "\n", encoding="utf-8")
        else:
            print(encoded)
        return 0
    raw = Path(args.payload[1:]).read_text(encoding="utf-8") if args.payload.startswith("@") else args.payload
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}))
        return 2
    report = registry.validate(args.contract, payload)
    print(report.model_dump_json(indent=2))
    return 0 if report.valid else 2


if __name__ == "__main__":
    sys.exit(main())
