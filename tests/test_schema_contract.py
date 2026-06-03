"""control/schema.py: get_contract (over cp.get_schema_contract) and the pure
Python validate_rows (required-missing/nullable-violation/valid).

Spec: docs/specs/2026-06-03-working-platform-completion-plan.md
      "3. Add Schema Validation Contracts" (lines ~358-424).
"""
import json

from control import schema


def _seed(conn, *, version="claim.v1", required, nullable):
    conn.execute(
        "INSERT INTO cp.schema_contract (domain,dataset,layer,schema_version,"
        "required_columns,nullable_columns,business_key,replacement_key_template) "
        "VALUES ('insurance','claim','silver',%s,%s,%s,%s,'{policy_id}:{claim_id}')",
        (version, json.dumps(required), json.dumps(nullable),
         json.dumps(["policy_id", "claim_id"])),
    )


# ---- get_contract -------------------------------------------------------------

def test_get_contract_returns_dict(conn):
    _seed(conn, required=["policy_id", "claim_id"], nullable=["notes"])
    c = schema.get_contract(conn, domain="insurance", dataset="claim",
                            layer="silver")
    assert c is not None
    assert c["schema_version"] == "claim.v1"
    assert c["required_columns"] == ["policy_id", "claim_id"]
    assert c["nullable_columns"] == ["notes"]
    assert c["business_key"] == ["policy_id", "claim_id"]
    assert c["replacement_key_template"] == "{policy_id}:{claim_id}"


def test_get_contract_latest_when_version_omitted(conn):
    _seed(conn, version="claim.v1", required=["policy_id"], nullable=[])
    _seed(conn, version="claim.v2", required=["policy_id", "claim_id"], nullable=[])
    c = schema.get_contract(conn, domain="insurance", dataset="claim",
                            layer="silver")
    assert c["schema_version"] == "claim.v2"
    exact = schema.get_contract(conn, domain="insurance", dataset="claim",
                                layer="silver", schema_version="claim.v1")
    assert exact["schema_version"] == "claim.v1"


def test_get_contract_none_when_missing(conn):
    assert schema.get_contract(conn, domain="nope", dataset="nope",
                               layer="silver") is None


# ---- F4: latest-version sort is correct for multi-part versions --------------

def test_get_schema_contract_latest_multipart_versions(conn):
    """F4 (migration 030): the OLD 'latest' sort stripped+concatenated digits
    (regexp_replace ... '\\D' ... ''), so 'claim.v1.10'->110 wrongly beat
    'claim.v2'->2 and 'claim.v1.0'->10 tied 'claim.v10'->10. The fix orders by
    effective_from DESC NULLS LAST, created_at DESC, so the most-recently-
    registered (= last inserted; created_at is clock_timestamp(), strictly
    increasing even within one INSERT) wins. Register v1.0, v1.10, v2 in an order
    where the digit-concat bug WOULD pick the wrong one, and assert the fix picks
    the genuinely-latest registered."""
    dom, ds, layer = "insurance", "claim_f4", "silver"
    # Insert in registration order; v2 registered LAST is the current contract.
    # Under the OLD bug v1.10 (digit-concat 110) would have won over v2 (2).
    conn.execute(
        "INSERT INTO cp.schema_contract (domain,dataset,layer,schema_version) "
        "VALUES (%s,%s,%s,'claim.v1.0'),(%s,%s,%s,'claim.v1.10'),"
        "(%s,%s,%s,'claim.v2')",
        (dom, ds, layer, dom, ds, layer, dom, ds, layer),
    )
    latest = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s) c",
        (dom, ds, layer),
    ).fetchone()[0]
    # The genuinely-latest registered is claim.v2; the digit-concat bug would
    # have returned claim.v1.10 (110 > 2). Assert the bug's failure case is fixed.
    assert latest == "claim.v2", f"latest picked {latest}, expected claim.v2"
    assert latest != "claim.v1.10", "digit-concatenation bug is still present"

    # effective_from dominates created_at: a contract marked effective later wins
    # even if registered earlier.
    dom2, ds2 = "insurance", "claim_f4b"
    conn.execute(
        "INSERT INTO cp.schema_contract "
        "(domain,dataset,layer,schema_version,effective_from) "
        "VALUES (%s,%s,%s,'claim.v9','2026-01-01'),"
        "(%s,%s,%s,'claim.v3','2026-06-01')",
        (dom2, ds2, layer, dom2, ds2, layer),
    )
    eff = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s) c",
        (dom2, ds2, layer),
    ).fetchone()[0]
    assert eff == "claim.v3", f"effective_from-latest picked {eff}, expected claim.v3"

    # exact-version path is UNCHANGED (still honours the request verbatim).
    exact = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s,'claim.v1.0') c",
        (dom, ds, layer),
    ).fetchone()[0]
    assert exact == "claim.v1.0"


# ---- validate_rows (pure Python) ---------------------------------------------

CONTRACT = {
    "required_columns": ["policy_id", "claim_id", "claim_amount"],
    "nullable_columns": ["claim_amount"],   # claim_amount may be null
}


def test_validate_rows_valid_passes():
    rows = [{"policy_id": "P1", "claim_id": "C1", "claim_amount": 10}]
    good, bad = schema.validate_rows(rows, CONTRACT)
    assert good == rows
    assert bad == []


def test_validate_rows_required_missing_is_bad():
    rows = [{"policy_id": "P1", "claim_amount": 10}]   # claim_id absent
    good, bad = schema.validate_rows(rows, CONTRACT)
    assert good == []
    assert len(bad) == 1
    row, reason = bad[0]
    assert row is rows[0]
    assert "claim_id" in reason and "missing" in reason


def test_validate_rows_nonnullable_null_is_bad():
    rows = [{"policy_id": None, "claim_id": "C1", "claim_amount": 10}]
    good, bad = schema.validate_rows(rows, CONTRACT)
    assert good == []
    assert len(bad) == 1
    _, reason = bad[0]
    assert "policy_id" in reason and "null" in reason


def test_validate_rows_nullable_null_is_good():
    # claim_amount is in nullable_columns, so null is allowed
    rows = [{"policy_id": "P1", "claim_id": "C1", "claim_amount": None}]
    good, bad = schema.validate_rows(rows, CONTRACT)
    assert good == rows
    assert bad == []


def test_validate_rows_splits_mixed_batch():
    rows = [
        {"policy_id": "P1", "claim_id": "C1", "claim_amount": 10},  # good
        {"policy_id": "P2", "claim_id": "C2"},                      # missing amount
        {"policy_id": None, "claim_id": "C3", "claim_amount": 5},   # null pk
    ]
    good, bad = schema.validate_rows(rows, CONTRACT)
    assert good == [rows[0]]
    assert [r for r, _ in bad] == [rows[1], rows[2]]


# ---- F7: empty-required contract is a misconfiguration -----------------------

def test_validate_rows_empty_required_contract_raises():
    """F7: a contract with NO required_columns can validate nothing — every row
    (even {}) would silently pass. That is unsafe, so validate_rows raises."""
    import pytest
    for bad_contract in ({}, {"required_columns": []},
                         {"required_columns": [], "nullable_columns": ["x"]}):
        with pytest.raises(ValueError, match="no required_columns"):
            schema.validate_rows([{"anything": 1}], bad_contract)


def test_validate_rows_empty_row_is_bad():
    """F7: an empty {} row (and a row missing all contract columns) is BAD —
    it fails the first required column's presence check."""
    good, bad = schema.validate_rows([{}], CONTRACT)
    assert good == []
    assert len(bad) == 1
    _, reason = bad[0]
    assert "missing" in reason
