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
