#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob   # zero migrations -> glob expands to nothing, loop is skipped
for f in "$(dirname "$0")"/migrations/[0-9]*.sql; do
  echo ">> applying $f"
  docker exec -i avivaods-postgres-1 psql -U ods -d ods_cp -v ON_ERROR_STOP=1 -f - < "$f"
done
echo ">> migrations applied"
