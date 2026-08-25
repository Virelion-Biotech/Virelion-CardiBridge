import argparse
import json
import sys

from .contracts import TraceContext
from .registry import ContractRegistry
from .schemas import SCHEMAS


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
    args = parser.parse_args(argv)
    registry = build_registry()
    if args.command == "schema":
        print(json.dumps(registry.model(args.contract).model_json_schema(), indent=2))
        return 0
    raw = open(args.payload[1:], encoding="utf-8").read() if args.payload.startswith("@") else args.payload
    report = registry.validate(args.contract, json.loads(raw))
    print(report.model_dump_json(indent=2))
    return 0 if report.valid else 2


if __name__ == "__main__":
    sys.exit(main())
