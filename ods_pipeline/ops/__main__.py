"""Entry point: ``python -m ods_pipeline.ops``.

Dispatches to subcommand modules. Argparse-only — no external deps.
"""
from __future__ import annotations

import argparse
import sys

from ods_pipeline.ops import dlq as dlq_cmd
from ods_pipeline.ops import runs as runs_cmd


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m ods_pipeline.ops")
    sub = parser.add_subparsers(dest="cmd", required=True)
    dlq_cmd.register(sub.add_parser("dlq", help="DLQ inspection + replay"))
    runs_cmd.register(sub.add_parser("runs", help="replay / rerun runs"))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "dlq":
        return dlq_cmd.dispatch(args)
    if args.cmd == "runs":
        return runs_cmd.dispatch(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
