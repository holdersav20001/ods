"""Replay/rerun CLI (T15 / 8.6)."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from ods_pipeline.ops.runs import _RunsOps, _dag_for


def _mock_pg(rows):
    cur = MagicMock()
    cur.fetchone.return_value = rows[0] if rows else None
    cur.fetchall.return_value = rows
    cur_cm = MagicMock()
    cur_cm.__enter__ = MagicMock(return_value=cur)
    cur_cm.__exit__ = MagicMock(return_value=False)
    pg = MagicMock()
    pg.cursor.return_value = cur_cm
    return pg, cur


_RUN_TUPLE = ("r-orig", "file", "insurance", "policies", "2026-05-02",
              "f-1", "ods.insurance.policies")


def test_rerun_dry_run_returns_plan_no_side_effects():
    pg, cur = _mock_pg([_RUN_TUPLE])
    airflow = MagicMock()
    ops = _RunsOps(pg_conn=pg, airflow=airflow)

    result = ops.rerun("r-orig", dry_run=True)

    assert result["original_run_id"] == "r-orig"
    assert result["pipeline_type"] == "file"
    assert result["dry_run"] is True
    assert result["status"] == "dry-run"
    assert "replay_request_id" in result
    airflow.trigger_dag.assert_not_called()


def test_rerun_unknown_run_raises():
    pg, _ = _mock_pg([])
    ops = _RunsOps(pg_conn=pg, airflow=MagicMock())
    with pytest.raises(LookupError, match="not found"):
        ops.rerun("missing-run-id")


def test_replay_file_dry_run_for_each_run():
    rows = [_RUN_TUPLE, ("r-orig-2", "file", "insurance", "policies",
                         "2026-05-01", "f-1", "ods.insurance.policies")]
    pg, _ = _mock_pg(rows)
    ops = _RunsOps(pg_conn=pg, airflow=MagicMock())

    result = ops.replay_file("f-1", dry_run=True)

    assert result["file_id"] == "f-1"
    assert result["replay"]["dry_run"] is True
    assert result["replay"]["original_run_id"] == "r-orig"
    assert "replay_request_id" in result["replay"]


def test_replay_file_no_runs_raises():
    pg, _ = _mock_pg([])
    ops = _RunsOps(pg_conn=pg, airflow=MagicMock())
    with pytest.raises(LookupError, match="no runs found"):
        ops.replay_file("nope")


def test_dag_for_maps_pipeline_type():
    assert _dag_for("file") == "dag_ingest"
    assert _dag_for("message_api") == "dag_event_api"
    assert _dag_for("dlq_replay") == "dag_dlq_replay"
    assert _dag_for("unknown") == "dag_ingest"  # default


def test_rerun_real_call_triggers_airflow_with_correct_conf():
    """Non-dry-run path: airflow.trigger_dag is called with new_run_id +
    file_id + dataset metadata. Postgres calls are mocked at the cursor
    level so we don't actually start a run; we just verify the trigger
    payload shape."""
    pg, _ = _mock_pg([_RUN_TUPLE])
    airflow = MagicMock()
    ops = _RunsOps(pg_conn=pg, airflow=airflow)

    result = ops.rerun("r-orig", dry_run=False)

    airflow.trigger_dag.assert_called_once()
    kwargs = airflow.trigger_dag.call_args.kwargs
    assert kwargs["dag_id"] == "dag_ingest"
    conf = kwargs["conf"]
    assert conf["file_id"] == "f-1"
    assert conf["domain"] == "insurance"
    assert conf["dataset"] == "policies"
    assert conf["business_date"] == "2026-05-02"
    assert conf["replay_request_id"] == result["replay_request_id"]
    assert conf["replay_of_run_id"] == "r-orig"
    assert result["status"] == "triggered"
    assert result["dag_id"] == "dag_ingest"
