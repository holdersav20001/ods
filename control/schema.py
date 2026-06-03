"""Schema-validation contract helpers (area 3).

The DB (cp.schema_contract + cp.get_schema_contract) STORES the contract; the
VALIDATION LOGIC is pure Python here so the DLQ workflow (step B) can call it
without a round-trip per row. Keep it simple: a required column that is missing
or NULL on a non-nullable column makes the row BAD with a reason string.
"""

# Columns of cp.schema_contract, in table order — used to map a SELECT * row
# (RETURNS cp.schema_contract) to a dict.
_CONTRACT_COLUMNS = (
    "schema_contract_id", "domain", "dataset", "layer", "schema_version",
    "required_columns", "nullable_columns", "business_key",
    "replacement_scope", "replacement_key_template",
    "effective_from", "effective_to", "created_at",
)


def get_contract(conn, *, domain, dataset, layer, schema_version=None):
    """Return the matching schema contract as a dict, or None if none exists.

    With ``schema_version`` omitted, returns the latest version for
    (domain, dataset, layer). Mirrors cp.get_schema_contract.
    """
    row = conn.execute(
        "SELECT (c).* FROM cp.get_schema_contract(%s,%s,%s,%s) AS c",
        [domain, dataset, layer, schema_version],
    ).fetchone()
    # A composite-returning function yields one row of NULLs when nothing matched;
    # detect that via the PK column being NULL.
    if row is None or row[0] is None:
        return None
    return dict(zip(_CONTRACT_COLUMNS, row))


def validate_rows(rows, contract):
    """Split ``rows`` into (good, bad_with_reasons) against ``contract``.

    A row is BAD if a required column is absent, or a required NON-nullable
    column is present-but-NULL. ``bad`` items are ``(row, reason)`` tuples.
    A column is nullable iff it appears in the contract's nullable_columns.
    Pure Python: no DB access.
    """
    required = list(contract.get("required_columns") or [])
    nullable = set(contract.get("nullable_columns") or [])

    good = []
    bad = []
    for row in rows:
        reason = None
        for col in required:
            if col not in row:
                reason = f"required column '{col}' missing"
                break
            if row[col] is None and col not in nullable:
                reason = f"non-nullable column '{col}' is null"
                break
        if reason is None:
            good.append(row)
        else:
            bad.append((row, reason))
    return good, bad
