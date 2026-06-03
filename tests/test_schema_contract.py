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


# ---- Re-audit #1/#2: latest is version-aware + effective-aware --------------

def test_get_schema_contract_latest_multipart_versions(conn):
    """Re-audit #1/#2 (migration 032): 'latest' must be the highest NUMERIC
    version VECTOR (int[]), INDEPENDENT of insert order, and must EXCLUDE
    future-dated contracts.

    The 030 "fix" ordered by effective_from DESC NULLS LAST, created_at DESC. That
    is (#2) NOT version-aware — with NULL effective_from it falls back to created_at
    (insert order), returning the LAST-inserted version — and (#1) NOT
    effective-aware — a future effective_from sorts FIRST and is returned as latest.

    PROOF this catches the 030 bug: each block inserts in an order where the 030
    body would return the WRONG row; the 032 version vector + future-exclusion
    returns the right one, so each assertion FAILS against the 030 ordering."""
    dom, ds, layer = "insurance", "claim_f4", "silver"
    # Non-monotonic insert (created_at order != version order). NULL effective_from
    # => 030 falls back to created_at DESC and returns the last-inserted (v1.2).
    # The 032 version vector ranks v1.10 {1,10} > v1.0 {1,0} and v2 {2} > both.
    conn.execute(
        "INSERT INTO cp.schema_contract (domain,dataset,layer,schema_version) "
        "VALUES (%s,%s,%s,'claim.v2'),(%s,%s,%s,'claim.v1.10'),"
        "(%s,%s,%s,'claim.v1.0'),(%s,%s,%s,'claim.v1.2')",
        (dom, ds, layer, dom, ds, layer, dom, ds, layer, dom, ds, layer),
    )
    latest = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s) c",
        (dom, ds, layer),
    ).fetchone()[0]
    # Genuine numeric latest is claim.v2 ({2} beats every {1,*}). The 030 body
    # (created_at DESC) would have returned claim.v1.2 (last inserted).
    assert latest == "claim.v2", f"latest picked {latest}, expected claim.v2"
    assert latest != "claim.v1.2", "030 created_at-only ordering is still present"

    # v1.10 > v1.2 numerically (a text/digit-concat order would mis-rank): isolate
    # the two so v2 does not dominate, insert v1.10 first so created_at disagrees.
    dom1, ds1 = "insurance", "claim_f4_mp"
    conn.execute(
        "INSERT INTO cp.schema_contract (domain,dataset,layer,schema_version) "
        "VALUES (%s,%s,%s,'claim.v1.10'),(%s,%s,%s,'claim.v1.2')",
        (dom1, ds1, layer, dom1, ds1, layer),
    )
    mp = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s) c",
        (dom1, ds1, layer),
    ).fetchone()[0]
    assert mp == "claim.v1.10", f"multipart latest picked {mp}, expected claim.v1.10"

    # #1 FUTURE-dated exclusion: v2 effective 2099 is NOT the latest (not yet in
    # force); v1 effective 2020 is. Under the 030 body v2 (effective_from DESC)
    # sorts first and IS returned -> this asserts the opposite.
    dom2, ds2 = "insurance", "claim_f4_future"
    conn.execute(
        "INSERT INTO cp.schema_contract "
        "(domain,dataset,layer,schema_version,effective_from) "
        "VALUES (%s,%s,%s,'claim.v1','2020-01-01'),"
        "(%s,%s,%s,'claim.v2','2099-01-01')",
        (dom2, ds2, layer, dom2, ds2, layer),
    )
    fut = conn.execute(
        "SELECT (c).schema_version FROM cp.get_schema_contract(%s,%s,%s) c",
        (dom2, ds2, layer),
    ).fetchone()[0]
    assert fut == "claim.v1", f"future-dated v2 leaked into latest: {fut}"
    # but exact-version still returns the future-dated contract on request.
    fut_exact = schema.get_contract(conn, domain=dom2, dataset=ds2, layer=layer,
                                    schema_version="claim.v2")
    assert fut_exact is not None and fut_exact["schema_version"] == "claim.v2"

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
