"""Unit contracts for `python -m ods_pipeline.ops dlq` (T9 / 8.2)."""
from __future__ import annotations

import io
import json
from unittest.mock import MagicMock

import pytest

from ods_pipeline.ops.dlq import _DlqOps, _split_s3_uri


def _fake_s3_with_keys(keys):
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    s3.get_paginator.return_value = paginator
    return s3


# ---------- list ----------

def test_list_groups_by_domain_dataset_stage_run():
    s3 = _fake_s3_with_keys([
        "insurance/policies/canonicalize/date=2026-05-02/run_id=r-1/0.json",
        "insurance/policies/canonicalize/date=2026-05-02/run_id=r-1/1.json",
        "insurance/policies/canonicalize/date=2026-05-02/run_id=r-2/0.json",
    ])
    ops = _DlqOps(s3=s3, pg_conn=MagicMock(), bucket="ods-dlq-test")

    lines = list(ops.list())

    assert any("total: 3 record(s)" in l for l in lines)
    assert any("insurance/policies/canonicalize/r-1\t2" in l for l in lines)
    assert any("insurance/policies/canonicalize/r-2\t1" in l for l in lines)


def test_list_empty_returns_friendly_message():
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": []}]
    s3.get_paginator.return_value = paginator
    ops = _DlqOps(s3=s3, pg_conn=MagicMock(), bucket="ods-dlq-test")
    assert list(ops.list()) == ["(no DLQ records found)"]


def test_list_filters_by_domain_dataset():
    s3 = _fake_s3_with_keys([
        "insurance/policies/canonicalize/date=x/run_id=r-1/0.json",
    ])
    ops = _DlqOps(s3=s3, pg_conn=MagicMock(), bucket="ods-dlq-test")
    list(ops.list(domain="insurance", dataset="policies"))
    s3.get_paginator.return_value.paginate.assert_called_with(
        Bucket="ods-dlq-test", Prefix="insurance/policies/"
    )


# ---------- show ----------

def test_show_returns_envelope_dict():
    body = {"_ods_run_id": "r-1", "payload": {"x": 1}}
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": io.BytesIO(json.dumps(body).encode("utf-8")),
    }
    ops = _DlqOps(s3=s3, pg_conn=MagicMock(), bucket="ods-dlq-test")

    result = ops.show("s3://ods-dlq-test/insurance/policies/canonicalize/x/0.json")

    assert result == body
    s3.get_object.assert_called_once_with(
        Bucket="ods-dlq-test",
        Key="insurance/policies/canonicalize/x/0.json",
    )


# ---------- replay ----------

def test_replay_dry_run_does_not_touch_kafka_or_pg():
    body = {
        "_ods_run_id": "r-orig",
        "_ods_domain": "insurance",
        "_ods_dataset": "policies",
        "payload": {"foo": 1},
    }
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": io.BytesIO(json.dumps(body).encode())}
    pg = MagicMock()
    factory = MagicMock()
    ops = _DlqOps(s3=s3, pg_conn=pg, bucket="ods-dlq-test",
                  producer_factory=factory)

    result = ops.replay("s3://ods-dlq-test/insurance/policies/canonicalize/x/0.json",
                        target_topic="ods.insurance.policies.canonical",
                        dry_run=True)

    assert result["dry_run"] is True
    assert result["original_run_id"] == "r-orig"
    assert result["status"] == "dry-run"
    assert "replay_run_id" in result
    factory.assert_not_called()
    pg.cursor.assert_not_called()


def test_replay_invalid_uri_raises():
    ops = _DlqOps(s3=MagicMock(), pg_conn=MagicMock(), bucket="ods-dlq-test")
    with pytest.raises(ValueError, match="not an s3 URI"):
        ops.replay("not-a-uri", target_topic="x", dry_run=True)


# ---------- helpers ----------

def test_split_s3_uri_parses_bucket_and_key():
    assert _split_s3_uri("s3://b/a/b/c.json") == ("b", "a/b/c.json")


def test_split_s3_uri_rejects_non_s3():
    with pytest.raises(ValueError):
        _split_s3_uri("https://example.com/x")


def test_split_s3_uri_rejects_missing_key():
    with pytest.raises(ValueError):
        _split_s3_uri("s3://only-bucket")
