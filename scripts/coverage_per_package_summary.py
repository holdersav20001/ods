"""Print a per-package coverage summary from coverage.xml.

The unit-test gate is a single combined ``--cov-fail-under`` (so CI is
green day one), but it hides hot-spots: ``glue/jobs`` and ``services``
are basically uncovered while ``ods_pipeline`` is in the 70s. This
helper parses ``coverage.xml`` (Cobertura format produced by
``coverage xml``) and prints a one-line-per-package breakdown so devs
can see where the rot lives without scrolling the full term-missing
table.

Usage::

    python scripts/coverage_per_package_summary.py [coverage.xml]

The script never exits non-zero — it is informational only. The actual
gate stays on ``pytest --cov-fail-under``.
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# Ordered: most-mature to least-mature. Match by path prefix using
# forward slashes — coverage.xml normalises separators on all platforms.
PACKAGES: list[tuple[str, str]] = [
    ("ods_pipeline", "ods_pipeline/"),
    ("airflow/dags/common", "airflow/dags/common/"),
    ("glue/jobs", "glue/jobs/"),
    ("services", "services/"),
]


def main(path: str = "coverage.xml") -> int:
    xml_path = Path(path)
    if not xml_path.exists():
        print(f"coverage_per_package_summary: {xml_path} not found, skipping")
        return 0

    tree = ET.parse(xml_path)
    root = tree.getroot()

    # When ``coverage`` is run with multiple ``--cov`` source roots
    # (e.g. ``ods_pipeline`` + ``glue/jobs`` + ``services`` +
    # ``airflow/dags/common``), Cobertura emits one ``<source>`` element
    # per root and ``<class>`` ``filename`` attributes are relative to
    # *some* source — Cobertura does not say which. We disambiguate by
    # probing each source root on disk for the basename.
    sources: list[Path] = []
    for s in root.iter("source"):
        if s.text:
            sources.append(Path(s.text.strip()))

    buckets: dict[str, dict[str, int]] = {
        name: {"stmts": 0, "missed": 0} for name, _ in PACKAGES
    }
    other = {"stmts": 0, "missed": 0}

    def resolve_to_package(filename: str) -> dict[str, int]:
        rel = filename.replace("\\", "/")
        # Probe each source root: the first one where the file exists
        # on disk wins. Convert that absolute path back to a project-
        # relative path so we can match against the PACKAGES prefixes.
        for src in sources:
            candidate = src / rel
            if candidate.exists():
                # Build the prefix this source maps to (relative to
                # the project root, which is the source root's
                # repo-relative location).
                src_str = str(src).replace("\\", "/")
                # The repo prefix is the path component starting at
                # ods_pipeline / glue/jobs / services / airflow/dags/common.
                for name, _ in PACKAGES:
                    if src_str.endswith("/" + name):
                        return buckets[name]
        return other

    for cls in root.iter("class"):
        filename = cls.get("filename") or ""
        target = resolve_to_package(filename)
        for line in cls.iter("line"):
            target["stmts"] += 1
            if line.get("hits") == "0":
                target["missed"] += 1

    print("=" * 64)
    print("per-package coverage summary")
    print("=" * 64)
    print(f"{'package':<24} {'stmts':>8} {'missed':>8} {'cover':>8}")
    print("-" * 64)
    total_stmts = 0
    total_missed = 0
    for name, _ in PACKAGES:
        b = buckets[name]
        stmts = b["stmts"]
        missed = b["missed"]
        total_stmts += stmts
        total_missed += missed
        if stmts == 0:
            pct = "n/a"
        else:
            pct = f"{(1 - missed / stmts) * 100:.1f}%"
        print(f"{name:<24} {stmts:>8} {missed:>8} {pct:>8}")
    print("-" * 64)
    if total_stmts:
        total_pct = f"{(1 - total_missed / total_stmts) * 100:.1f}%"
    else:
        total_pct = "n/a"
    print(f"{'TOTAL':<24} {total_stmts:>8} {total_missed:>8} {total_pct:>8}")
    print("=" * 64)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "coverage.xml"))
