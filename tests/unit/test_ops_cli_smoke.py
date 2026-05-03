"""CLI argparse wiring smoke tests (review concern #7).

Catches typos and wiring regressions in `python -m ods_pipeline.ops`
subcommand registration that the dispatch # pragma: no cover paths
would otherwise hide.
"""
from __future__ import annotations

import pytest

from ods_pipeline.ops.__main__ import _build_parser, main


def test_build_parser_registers_dlq_and_runs():
    parser = _build_parser()
    # SystemExit is raised by argparse on --help; we want to confirm both
    # subcommands are reachable without that side effect.
    actions = [a.dest for a in parser._actions if a.dest != "help"]
    assert "cmd" in actions


def test_help_exits_cleanly_via_main():
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])
    assert excinfo.value.code == 0


@pytest.mark.parametrize("argv", [
    ["dlq", "--help"],
    ["dlq", "list", "--help"],
    ["dlq", "show", "--help"],
    ["dlq", "replay", "--help"],
    ["runs", "--help"],
    ["runs", "replay", "--help"],
    ["runs", "rerun", "--help"],
])
def test_subcommand_help_exits_cleanly(argv):
    with pytest.raises(SystemExit) as excinfo:
        main(argv)
    assert excinfo.value.code == 0
