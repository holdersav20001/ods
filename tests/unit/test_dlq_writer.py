"""Unit contracts for ods_pipeline.dlq.DlqWriter (B7).

The writer must:
- PUT envelope JSON to the canonical S3 key derived from envelope fields
- Retry transient errors with bounded exp backoff (1, 2, 4, 8, 16s default)
- Raise the last error if all retries exhausted
- NOT write to the ledger (caller's job)
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from ods_pipeline.dlq import DlqWriter, envelope


def _envelope(**overrides):
    base = dict(
        payload={"x": 1},
        run_id="r-1",
        domain="insurance",
        dataset="policies",
        source_application="sftp",
        error_type="schema_violation",
        error_message="missing field foo",
        stage="canonicalize",
        file_id="f-1",
        business_date="2026-05-02",
    )
    base.update(overrides)
    return envelope(**base)


def _writer(s3=None, *, sleeps=None, backoff=(0.0, 0.0, 0.0)):
    s3 = s3 or MagicMock()
    sleep_log: list[float] = [] if sleeps is None else sleeps
    return DlqWriter(
        s3,
        env="test",
        backoff=backoff,
        sleep=sleep_log.append,
    ), s3, sleep_log


def test_write_puts_to_canonical_s3_key():
    w, s3, _ = _writer()
    s3.put_object.return_value = {}
    env = _envelope()

    uri = w.write(env, stage="canonicalize", attempt=0)

    assert uri == (
        "s3://ods-dlq-test/insurance/policies/canonicalize/"
        "date=2026-05-02/run_id=r-1/0.json"
    )
    s3.put_object.assert_called_once()
    kwargs = s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "ods-dlq-test"
    assert kwargs["Key"] == "insurance/policies/canonicalize/date=2026-05-02/run_id=r-1/0.json"
    assert kwargs["ContentType"] == "application/json"
    body = json.loads(kwargs["Body"].decode("utf-8"))
    assert body["_ods_run_id"] == "r-1"
    assert body["payload"] == {"x": 1}


def test_write_uses_unknown_date_when_business_date_missing():
    w, s3, _ = _writer()
    s3.put_object.return_value = {}
    env = _envelope(business_date=None)

    w.write(env, stage="canonicalize", attempt=3)

    key = s3.put_object.call_args.kwargs["Key"]
    assert "date=unknown/" in key
    assert key.endswith("/3.json")


def test_write_retries_with_exp_backoff_then_succeeds():
    s3 = MagicMock()
    s3.put_object.side_effect = [RuntimeError("boom"), RuntimeError("boom"), {}]
    sleeps: list[float] = []
    w = DlqWriter(s3, env="test", backoff=(1.0, 2.0, 4.0, 8.0, 16.0), sleep=sleeps.append)

    uri = w.write(_envelope(), stage="canonicalize", attempt=0)

    assert uri.startswith("s3://ods-dlq-test/")
    assert s3.put_object.call_count == 3
    assert sleeps == [1.0, 2.0]


def test_write_raises_last_error_when_all_retries_exhausted():
    s3 = MagicMock()
    err = RuntimeError("permanent")
    s3.put_object.side_effect = [RuntimeError("transient")] * 5 + [err]
    sleeps: list[float] = []
    w = DlqWriter(s3, env="test", backoff=(0.0, 0.0, 0.0, 0.0, 0.0), sleep=sleeps.append)

    with pytest.raises(RuntimeError, match="permanent"):
        w.write(_envelope(), stage="canonicalize", attempt=0)

    assert s3.put_object.call_count == 6  # 1 initial + 5 retries
    assert sleeps == [0.0, 0.0, 0.0, 0.0, 0.0]


def test_write_default_backoff_is_1_2_4_8_16():
    w = DlqWriter(MagicMock(), env="test")
    assert w._backoff == (1.0, 2.0, 4.0, 8.0, 16.0)


def test_write_does_not_touch_ledger():
    """Side-effect isolation: only S3, no DB or other I/O.

    The writer takes only an s3_client; it has no conn/cursor parameter.
    Documented contract: caller writes the failure to run_log.
    """
    import inspect

    sig = inspect.signature(DlqWriter.__init__)
    params = set(sig.parameters)
    assert "conn" not in params
    assert "cursor" not in params
    sig_write = inspect.signature(DlqWriter.write)
    write_params = set(sig_write.parameters)
    assert "conn" not in write_params
    assert "cursor" not in write_params
