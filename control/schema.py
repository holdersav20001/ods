"""Schema-validation contract helpers (area 3).

The DB (cp.schema_contract + cp.get_schema_contract) STORES the contract; the
VALIDATION LOGIC is pure Python here so the DLQ workflow (step B) can call it
without a round-trip per row. Required-column/null checks are always enforced;
optional ``validation_rules`` add type/range/enum/pattern checks, and
``business_key`` catches duplicate keys inside the batch.
"""
import datetime as dt
import re
from decimal import Decimal

# Columns of cp.schema_contract, in table order — used to map a SELECT * row
# (RETURNS cp.schema_contract) to a dict.
_CONTRACT_COLUMNS = (
    "schema_contract_id", "domain", "dataset", "layer", "schema_version",
    "required_columns", "nullable_columns", "business_key",
    "replacement_scope", "replacement_key_template",
    "effective_from", "effective_to", "created_at", "validation_rules",
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


def _is_number(value):
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _validate_type(value, expected_type):
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "number":
        return _is_number(value)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "date":
        if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
            return True
        if not isinstance(value, str):
            return False
        try:
            dt.date.fromisoformat(value)
        except ValueError:
            return False
        return True
    return True


def _validate_column_rule(col, value, rule):
    expected_type = rule.get("type")
    if expected_type and not _validate_type(value, expected_type):
        return f"column '{col}' must be {expected_type}"

    if "allowed" in rule and value not in set(rule.get("allowed") or []):
        return f"column '{col}' must be one of {rule['allowed']}"

    if _is_number(value):
        if "min" in rule and value < rule["min"]:
            return f"column '{col}' must be >= {rule['min']}"
        if "max" in rule and value > rule["max"]:
            return f"column '{col}' must be <= {rule['max']}"

    if isinstance(value, str) and "pattern" in rule:
        if re.fullmatch(rule["pattern"], value) is None:
            return f"column '{col}' does not match pattern"

    return None


def validate_rows(rows, contract):
    """Split ``rows`` into (good, bad_with_reasons) against ``contract``.

    A row is BAD if a required column is absent, a required NON-nullable column
    is present-but-NULL, a validation_rules entry rejects its value, or the batch
    contains a duplicate business_key. ``bad`` items are ``(row, reason)``
    tuples. A column is nullable iff it appears in nullable_columns. Pure Python:
    no DB access.

    F7: a contract with NO required_columns is a MISCONFIGURATION — with no
    required columns there is nothing to validate against, so EVERY row
    (including an empty ``{}``) would silently pass. That is unsafe, so we raise
    ``ValueError`` rather than accept-everything. A row that is empty or omits a
    required column is therefore correctly marked BAD by the per-column loop
    below (an empty ``{}`` fails the first required column's presence check).
    """
    required = list(contract.get("required_columns") or [])
    nullable = set(contract.get("nullable_columns") or [])
    business_key = list(contract.get("business_key") or [])
    rules = contract.get("validation_rules") or {}
    column_rules = rules.get("columns") or {}

    if not required:
        raise ValueError("schema contract has no required_columns")

    validated = []
    key_counts = {}
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
            for col, rule in column_rules.items():
                if col not in row or row[col] is None:
                    continue
                reason = _validate_column_rule(col, row[col], rule)
                if reason is not None:
                    break

        key = None
        if reason is None and business_key:
            if all(col in row and row[col] is not None for col in business_key):
                key = tuple(row[col] for col in business_key)
                key_counts[key] = key_counts.get(key, 0) + 1

        validated.append((row, reason, key))

    good = []
    bad = []
    for row, reason, key in validated:
        if reason is None and key is not None and key_counts.get(key, 0) > 1:
            reason = f"duplicate business key {dict(zip(business_key, key))}"

        if reason is None:
            good.append(row)
        else:
            bad.append((row, reason))
    return good, bad
