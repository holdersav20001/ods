"""Validate all Avro schema files under ``schemas/``."""
from __future__ import annotations

import json
from pathlib import Path

from fastavro import parse_schema

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    for path in sorted((ROOT / "schemas").rglob("*.avsc")):
        parse_schema(json.loads(path.read_text(encoding="utf-8")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
