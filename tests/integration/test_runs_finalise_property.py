"""Hypothesis-driven property tests on ``runs.finalise``.

The finalise contract (docstring on :func:`ods_pipeline.runs.finalise`):

  1. If ``record_count_target > 0``, at least one ``lineage_edge``
     row must exist with ``consumer_run_id = run_id``.
  2. No non-terminal ``run_stage_log`` rows may exist for ``run_id``.

Violations → run is marked ``failed`` with an explanatory
``error_summary`` and ``LineageInvariantError`` is raised.

These properties drive both branches with random shapes so any
off-by-one or constraint relaxation surfaces as a failing test
instead of silent state drift.
"""
from __future__ import annotations

import uuid

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

import ods_pipeline
from ods_pipeline.runs import LineageInvariantError


DOMAIN = "insurance"
DATASET = "runs_finalise_property"


@pytest.fixture
def cleanup(pg_conn):
    yield
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.run_stage_log "
            "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
            "                  WHERE domain=%s AND dataset=%s)",
            (DOMAIN, DATASET),
        )
        cur.execute(
            "DELETE FROM pipeline.lineage_edge "
            "WHERE consumer_run_id IN (SELECT run_id FROM pipeline.run_log "
            "                        WHERE domain=%s AND dataset=%s)",
            (DOMAIN, DATASET),
        )
        cur.execute(
            "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
    pg_conn.commit()


def _seed_run(pg_conn, *, published: int, edges: int, open_stages: int) -> str:
    run_id = str(uuid.uuid4())
    ods_pipeline.runs.start(
        pg_conn,
        run_id=run_id,
        pipeline_type="ingestion",
        domain=DOMAIN,
        dataset=DATASET,
        business_date="2026-05-06",
    )
    if published > 0:
        ods_pipeline.runs.update(
            pg_conn, run_id, record_count_target=published,
        )
    for _ in range(edges):
        ods_pipeline.lineage.write_edge(
            pg_conn,
            consumer_run_id=run_id,
            source_file_id=None,
            upstream_run_id=run_id,  # self-edge ok for the invariant — only count matters
            edge_type="curated_to_kafka",
            record_count=published or 1,
        )
    for stage_idx in range(open_stages):
        # Each open stage uses a distinct stage name so the partial
        # unique index on (run_id, stage, attempt_number) doesn't
        # silently dedupe duplicate inserts.
        ods_pipeline.stages.start(
            pg_conn, run_id=run_id, stage=f"raw_read",
            attempt_number=stage_idx + 1,
        )
    return run_id


# ---------------------------------------------------------------------------
# Property: invariants
# ---------------------------------------------------------------------------


def _wipe(pg_conn):
    pg_conn.rollback()
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.run_stage_log "
            "WHERE run_id IN (SELECT run_id FROM pipeline.run_log "
            "                  WHERE domain=%s AND dataset=%s)",
            (DOMAIN, DATASET),
        )
        cur.execute(
            "DELETE FROM pipeline.lineage_edge "
            "WHERE consumer_run_id IN (SELECT run_id FROM pipeline.run_log "
            "                        WHERE domain=%s AND dataset=%s)",
            (DOMAIN, DATASET),
        )
        cur.execute(
            "DELETE FROM pipeline.run_log WHERE domain=%s AND dataset=%s",
            (DOMAIN, DATASET),
        )
    pg_conn.commit()


@given(
    published=st.integers(min_value=0, max_value=10),
    edges=st.integers(min_value=0, max_value=5),
    open_stages=st.integers(min_value=0, max_value=3),
)
@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_finalise_invariant_holds_for_arbitrary_shapes(
    pg_conn, cleanup, published, edges, open_stages,
):
    """For any (published, edges, open_stages):

    finalise raises LineageInvariantError iff
        (published > 0 and edges == 0) OR open_stages > 0
    Otherwise it returns and the run stays succeeded.
    """
    # Hypothesis re-runs this body N times against the same fixture; wipe
    # state between examples so accumulated rows from earlier examples
    # don't pollute the current invariant check.
    _wipe(pg_conn)
    run_id = _seed_run(
        pg_conn,
        published=published,
        edges=edges,
        open_stages=open_stages,
    )
    expects_violation = (published > 0 and edges == 0) or open_stages > 0

    if expects_violation:
        with pytest.raises(LineageInvariantError) as excinfo:
            ods_pipeline.runs.finalise(pg_conn, run_id)
        # Diagnostic must mention either the lineage gap or the open stage.
        msg = str(excinfo.value)
        if published > 0 and edges == 0:
            assert "no lineage_edge rows" in msg or "orphaned" in msg
        if open_stages > 0:
            assert "non-terminal stages remain" in msg

        # Run should be marked failed.
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM pipeline.run_log WHERE run_id=%s",
                (run_id,),
            )
            assert cur.fetchone()[0] == "failed"
    else:
        # Should succeed without raising. (finalise itself does not flip
        # status to succeeded — the caller does that — but the call
        # must complete without LineageInvariantError.)
        ods_pipeline.runs.finalise(pg_conn, run_id)


# ---------------------------------------------------------------------------
# Pinned worked examples — fixtures of well-known shapes
# ---------------------------------------------------------------------------


def test_finalise_passes_for_zero_published_zero_edges(pg_conn, cleanup):
    run_id = _seed_run(pg_conn, published=0, edges=0, open_stages=0)
    ods_pipeline.runs.finalise(pg_conn, run_id)


def test_finalise_passes_for_published_with_matching_edge(pg_conn, cleanup):
    run_id = _seed_run(pg_conn, published=5, edges=1, open_stages=0)
    ods_pipeline.runs.finalise(pg_conn, run_id)


def test_finalise_raises_orphan_when_published_without_edges(pg_conn, cleanup):
    run_id = _seed_run(pg_conn, published=5, edges=0, open_stages=0)
    with pytest.raises(LineageInvariantError, match="orphaned"):
        ods_pipeline.runs.finalise(pg_conn, run_id)


def test_finalise_raises_when_stage_left_open(pg_conn, cleanup):
    run_id = _seed_run(pg_conn, published=0, edges=0, open_stages=1)
    with pytest.raises(LineageInvariantError, match="non-terminal stages"):
        ods_pipeline.runs.finalise(pg_conn, run_id)


def test_finalise_reports_both_violations_when_both_present(pg_conn, cleanup):
    run_id = _seed_run(pg_conn, published=3, edges=0, open_stages=2)
    with pytest.raises(LineageInvariantError) as excinfo:
        ods_pipeline.runs.finalise(pg_conn, run_id)
    msg = str(excinfo.value)
    assert "orphaned" in msg or "no lineage_edge" in msg
    assert "non-terminal stages remain" in msg
