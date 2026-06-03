#!/usr/bin/env python
"""Apply cp migrations over a TCP psycopg connection (port 5440).

Mirrors db/apply.sh but uses the project's psycopg client instead of
`docker exec ... psql`, for environments where `docker exec` is unavailable
(e.g. a host kernel that rejects the runtime's seccomp filter). Each migration
runs in its own transaction; a failure aborts immediately (ON_ERROR_STOP
semantics).

Usage:
    python -m db.apply              # apply all migrations/[0-9]*.sql in order
    python -m db.apply --drop       # DROP SCHEMA cp/ods CASCADE first, then apply
"""
import glob
import os
import sys

from control.db import connect

HERE = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS = os.path.join(HERE, "migrations")


def main(argv):
    drop = "--drop" in argv
    conn = connect()
    conn.autocommit = True
    if drop:
        print(">> DROP SCHEMA cp/ods CASCADE")
        conn.execute("DROP SCHEMA IF EXISTS cp CASCADE")
        conn.execute("DROP SCHEMA IF EXISTS ods CASCADE")
    files = sorted(glob.glob(os.path.join(MIGRATIONS, "[0-9]*.sql")))
    for f in files:
        print(f">> applying {os.path.basename(f)}")
        with open(f, "r", encoding="utf-8") as fh:
            sql = fh.read()
        try:
            conn.execute(sql)
        except Exception as exc:  # noqa: BLE001 — surface and stop, like ON_ERROR_STOP
            print(f"!! FAILED in {os.path.basename(f)}: {exc}", file=sys.stderr)
            conn.close()
            return 1
    conn.close()
    print(">> migrations applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
