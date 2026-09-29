"""Scaffold inspection and validation commands. Run from the repository root."""

import argparse
import json
import sys
from pathlib import Path

from pydantic import ValidationError

from .common import load_summary
from .config import load_config
from .models import EventEnvelope


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    config_parser = sub.add_parser("validate-config")
    config_parser.add_argument("path", nargs="?", default="config/poc.json")
    sample_parser = sub.add_parser("validate-samples")
    sample_parser.add_argument("path", nargs="?", default="samples")
    generate_parser = sub.add_parser("generate")
    generate_parser.add_argument("stream", choices=["facility", "passenger", "airport_operations", "aircraft"])
    generate_parser.add_argument("--config", default="config/poc.json")
    args = parser.parse_args(argv)
    try:
        if args.command == "validate-config":
            config = load_config(args.path)
            print(json.dumps(load_summary(config), indent=2))
        elif args.command == "validate-samples":
            paths = sorted(Path(args.path).glob("*.json"))
            if not paths:
                raise ValueError(f"No JSON samples found in {args.path}")
            for path in paths:
                EventEnvelope.model_validate_json(path.read_text(encoding="utf-8"))
            print(f"Validated {len(paths)} sample events")
        else:
            load_config(args.config)
            raise NotImplementedError(f"{args.stream} generator is a scaffold; simulation is not implemented")
    except (OSError, ValueError, ValidationError, NotImplementedError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0
