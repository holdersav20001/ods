"""Dual-sink parity recon check (T11 / A2).

Asserts check_dual_sink_parity returns the right status (ok / pending /
failed) given seeded current+history sink row counts, and writes a
matching reconciliation_log row.
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from ods_pipeline import reconciliation


def _mock_conn(current: int, history: int):
    cur = MagicMock()
    cur.fetchone.side_effect = [(current,), (history,), None]
    cur_cm = MagicMock()
    cur_cm.__enter__ = MagicMock(return_value=cur)
    cur_cm.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur_cm
    return conn, cur


def _run():
    return str(uuid.uuid4())


def test_parity_ok_when_counts_match():
    conn, _ = _mock_conn(current=10, history=10)
    result = reconciliation.check_dual_sink_parity(
        conn, run_id=_run(), domain="insurance", dataset="policies",
        business_date="2026-05-02",
    )
    assert result["status"] == "ok"
    assert result["delta"] == 0


def test_parity_failed_when_counts_diverge():
    conn, _ = _mock_conn(current=10, history=8)
    result = reconciliation.check_dual_sink_parity(
        conn, run_id=_run(), domain="insurance", dataset="policies",
        business_date="2026-05-02",
    )
    assert result["status"] == "failed"
    assert result["delta"] == -2
    assert "delta=-2" in result["detail"]


def test_parity_pending_when_one_sink_empty():
    """History sink is lagging — current has rows, history has none."""
    conn, _ = _mock_conn(current=10, history=0)
    result = reconciliation.check_dual_sink_parity(
        conn, run_id=_run(), domain="insurance", dataset="policies",
        business_date="2026-05-02",
    )
    assert result["status"] == "pending"
    assert "consumer lag" in result["detail"]


def test_parity_pending_when_both_empty():
    conn, _ = _mock_conn(current=0, history=0)
    result = reconciliation.check_dual_sink_parity(
        conn, run_id=_run(), domain="insurance", dataset="policies",
        business_date="2026-05-02",
    )
    assert result["status"] == "pending"


def test_parity_skipped_for_unregistered_dataset():
    conn = MagicMock()
    result = reconciliation.check_dual_sink_parity(
        conn, run_id=_run(), domain="insurance", dataset="risk",
        business_date="2026-05-02",
    )
    assert result["status"] == "skipped"
    conn.cursor.assert_not_called()


def test_parity_uses_pairs_override():
    """Custom pairs registry — call site can supply non-default tables."""
    conn, _ = _mock_conn(current=5, history=5)
    pairs = {"foo": ("schema.foo_current", "schema.foo_history", "_ods_run_id")}
    result = reconciliation.check_dual_sink_parity(
        conn, run_id=_run(), domain="x", dataset="foo",
        business_date="2026-05-02", pairs=pairs,
    )
    assert result["status"] == "ok"


def test_parity_rejects_unsafe_pairs_override():
    conn = MagicMock()
    pairs = {"foo": ("schema.foo_current; DROP TABLE x", "schema.foo_history", "_ods_run_id")}
    with pytest.raises(ValueError, match="table reference"):
        reconciliation.check_dual_sink_parity(
            conn, run_id=_run(), domain="x", dataset="foo",
            business_date="2026-05-02", pairs=pairs,
        )
    conn.cursor.assert_not_called()


def test_parity_writes_recon_log_row():
    """The check ends in a reconciliation_log INSERT."""
    conn, cur = _mock_conn(current=3, history=3)
    reconciliation.check_dual_sink_parity(
        conn, run_id=_run(), domain="insurance", dataset="policies",
        business_date="2026-05-02",
    )
    # 3 calls: count current, count history, INSERT recon row.
    assert cur.execute.call_count == 3
    insert_sql = cur.execute.call_args_list[2][0][0]
    assert "INSERT INTO pipeline.reconciliation_log" in insert_sql
