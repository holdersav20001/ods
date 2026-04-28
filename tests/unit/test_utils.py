# tests/unit/test_utils.py
import hashlib
from datetime import date
import pytest

# These imports will FAIL until utils.py is created — that's expected
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../glue/jobs'))

from utils import (
    extract_business_date,
    generate_message_key,
    write_job_log,
    load_dataset_config,
    set_file_state,
    get_file_state,
)


# --- extract_business_date ---

def test_extract_business_date_standard():
    result = extract_business_date("policies_20260417.csv", r"policies_(\d{8})\.csv")
    assert result == date(2026, 4, 17)

def test_extract_business_date_december():
    result = extract_business_date("policies_20261201.csv", r"policies_(\d{8})\.csv")
    assert result == date(2026, 12, 1)

def test_extract_business_date_no_match_raises():
    with pytest.raises(ValueError, match="Cannot extract business_date"):
        extract_business_date("unknown_file.csv", r"policies_(\d{8})\.csv")


# --- generate_message_key ---

def test_generate_message_key_is_deterministic():
    row = {"policy_id": "POL-001", "premium_amount": 1200.0}
    assert generate_message_key(["policy_id"], row) == generate_message_key(["policy_id"], row)

def test_generate_message_key_is_sha256_hex():
    key = generate_message_key(["policy_id"], {"policy_id": "POL-001"})
    assert len(key) == 64
    assert all(c in "0123456789abcdef" for c in key)

def test_generate_message_key_changes_with_value():
    assert (generate_message_key(["policy_id"], {"policy_id": "POL-001"}) !=
            generate_message_key(["policy_id"], {"policy_id": "POL-002"}))

def test_generate_message_key_multi_field_order_stable():
    row = {"policy_id": "POL-001", "start_date": "2026-01-01"}
    k1 = generate_message_key(["policy_id", "start_date"], row)
    k2 = generate_message_key(["start_date", "policy_id"], row)
    assert k1 == k2  # sorted field order — stable


# --- load_dataset_config (requires postgres) ---

def _pg_kwargs():
    import os
    return dict(
        host=os.environ.get("TEST_PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_PG_PORT", "5440")),
        dbname=os.environ.get("TEST_PG_DB", "ods_dev"),
        user=os.environ.get("TEST_PG_USER", "ods"),
        password=os.environ.get("TEST_PG_PASSWORD", "ods"),
    )


@pytest.fixture(scope="module")
def pg():
    import psycopg2
    conn = psycopg2.connect(**_pg_kwargs())
    yield conn
    conn.close()


def postgres_available():
    try:
        import psycopg2
        psycopg2.connect(**_pg_kwargs()).close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not postgres_available(), reason="Postgres not running")
def test_load_dataset_config_returns_policies(pg):
    config = load_dataset_config(pg, "insurance", "policies")
    assert config["dataset"] == "policies"
    assert config["target_topic"] == "ods.insurance.policies"
    assert config["key_fields"] == ["policy_id"]
    assert "hard_blocks" in config["dq_rules"]

@pytest.mark.skipif(not postgres_available(), reason="Postgres not running")
def test_load_dataset_config_missing_raises(pg):
    with pytest.raises(ValueError, match="No active config"):
        load_dataset_config(pg, "insurance", "nonexistent")

@pytest.mark.skipif(not postgres_available(), reason="Postgres not running")
def test_set_and_get_file_state(pg):
    import uuid
    run_id = str(uuid.uuid4())
    s3_path = f"s3://ods-raw-local/test/{run_id}/test.csv"
    set_file_state(pg, s3_path, run_id, "new")
    assert get_file_state(pg, s3_path) == "new"
    set_file_state(pg, s3_path, run_id, "completed", record_count=100)
    assert get_file_state(pg, s3_path) == "completed"
