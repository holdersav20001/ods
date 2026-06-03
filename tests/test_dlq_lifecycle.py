"""DLQ lifecycle (migration 023): failed_payload + quarantine_output_link_id +
status, the cp.resolve_dlq resolution path, and provenance visibility of the
first-class quarantine output.

Spec: docs/specs/2026-06-03-working-platform-completion-plan.md
      "DLQ Implementation For This Repository" (lines ~198-277).
"""
import json
from uuid import uuid4

import pytest
import psycopg

from control import dlq

BD = "2026-05-29"


def _run(conn, pipeline_type="ingestion", wf=None):
    wf = wf or str(uuid4())
    run_id = conn.execute(
        "SELECT cp.start_run(%s,%s,'insurance','claim',%s,'manual')",
        (wf, pipeline_type, BD),
    ).fetchone()[0]
    return run_id, wf


def _raw_file(conn):
    return conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'insurance','claim')",
        (f"s3://raw/{uuid4()}.csv", uuid4().hex, BD),
    ).fetchone()[0]


# ---- quarantine now preserves payload + captures the output link --------------

def test_quarantine_stores_failed_payload_link_and_open_status(conn):
    """cp.quarantine (023) persists the actual rejected row in failed_payload,
    captures the quarantine output_link id it creates, and sets status='open'."""
    run_id, _ = _run(conn, "canonicalize")
    payload = {"policy_id": "P1", "claim_id": "C9", "claim_amount": -5}
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','claim_amount must be >= 0',%s,%s,%s,%s)",
        (run_id, json.dumps({"src": "raw"}),
         "s3://dlq/insurance/claim/2026-05-29/errors.json", 1,
         json.dumps(payload)),
    ).fetchone()[0]
    row = conn.execute(
        "SELECT status, failed_payload, quarantine_output_link_id, reason "
        "FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()
    assert row[0] == "open"
    assert row[1] == payload                     # exact rejected row preserved
    assert row[2] is not None                    # quarantine_output_link_id captured
    assert row[3] == "claim_amount must be >= 0"
    # the captured id IS the quarantine output_link for this run
    link = conn.execute(
        "SELECT lineage_link_id FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'", (run_id,)
    ).fetchone()[0]
    assert str(row[2]) == str(link)


def test_quarantine_payload_defaults_null_for_existing_callers(conn):
    """The 6-arg form (no p_failed_payload) still works; failed_payload is NULL."""
    run_id, _ = _run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'curate','bad rows',%s,%s,%s)",
        (run_id, json.dumps({"src": "x"}), "s3://dlq/p.json", 3),
    ).fetchone()[0]
    row = conn.execute(
        "SELECT status, failed_payload, quarantine_output_link_id "
        "FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()
    assert row[0] == "open"
    assert row[1] is None
    assert row[2] is not None


# ---- quarantine output is first-class + provenance-visible --------------------

def test_quarantine_output_is_first_class_and_good_traces_to_raw(conn):
    """The quarantine output is a first-class output_link visible in
    cp.v_provenance (edge_type='quarantine'); the good output of the SAME run
    traces to the raw file, so good + quarantine are sibling provenance nodes of
    the run that consumed the raw file (spec lines 219-244, 331)."""
    run_id, _ = _run(conn)
    fid = _raw_file(conn)
    # good raw_to_curated output anchored to the raw file
    good = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "s3://silver/claim",
                             "content_hash": "good1", "version": 1,
                             "schema_version": "claim.v1"}), 3,
         json.dumps([{"edge_type": "raw_to_curated", "source_file_id": str(fid),
                      "source_ref": {}, "record_count": 3}])),
    ).fetchone()[0]
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','claim_amount must be >= 0',%s,%s,%s,%s)",
        (run_id, json.dumps({"raw_file_id": str(fid)}),
         "s3://dlq/insurance/claim/2026-05-29/errors.json", 1,
         json.dumps({"policy_id": "P1", "claim_amount": -5})),
    ).fetchone()[0]
    qlink = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()[0]

    # quarantine output is visible in provenance (first-class provenance node)
    qprov = conn.execute(
        "SELECT count(*) FROM cp.v_provenance "
        "WHERE lineage_link_id=%s AND edge_type='quarantine'", (qlink,)
    ).fetchone()[0]
    assert qprov == 1

    # good output traces to the raw file via provenance
    good_to_raw = conn.execute(
        "SELECT count(*) FROM cp.v_provenance "
        "WHERE lineage_link_id=%s AND source_file_id=%s", (good, str(fid))
    ).fetchone()[0]
    assert good_to_raw == 1

    # good + quarantine are siblings of the run that traces to the raw file
    sib = conn.execute(
        "SELECT count(DISTINCT edge_type) FROM cp.v_provenance "
        "WHERE consumer_run_id=%s", (run_id,)
    ).fetchone()[0]
    assert sib == 2  # raw_to_curated + quarantine


# ---- recon consistency: input rows = good rows + dlq rows ---------------------

def test_quarantine_recon_input_equals_good_plus_dlq(conn):
    """Spec lines 304-308: reconciliation accounts for good + DLQ. With 4 raw
    rows, 3 good + 1 dlq, the quarantine record_count + good record_count == raw."""
    run_id, _ = _run(conn)
    fid = _raw_file(conn)
    conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"path": "s3://silver/c", "content_hash": "g",
                             "version": 1}), 3,
         json.dumps([{"edge_type": "raw_to_curated", "source_file_id": str(fid),
                      "source_ref": {}, "record_count": 3}])),
    )
    conn.execute(
        "SELECT cp.quarantine(%s,'validate','bad',%s,%s,%s,%s)",
        (run_id, json.dumps({}), "s3://dlq/e.json", 1, json.dumps({"x": 1})),
    )
    good = conn.execute(
        "SELECT coalesce(sum(record_count),0) FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='raw_to_curated'", (run_id,)
    ).fetchone()[0]
    dlq_n = conn.execute(
        "SELECT coalesce(sum(record_count),0) FROM cp.dlq WHERE run_id=%s", (run_id,)
    ).fetchone()[0]
    assert good + dlq_n == 4


# ---- resolve_dlq round-trip ---------------------------------------------------

def test_resolve_dlq_sets_status_and_refs_preserving_history(conn):
    """cp.resolve_dlq flips status + resolution refs WITHOUT touching
    failed_payload/reason (spec line 246, 271-272)."""
    run_id, _ = _run(conn)
    payload = {"policy_id": "P1", "claim_amount": -5}
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','claim_amount must be >= 0',%s,%s,%s,%s)",
        (run_id, json.dumps({}), "s3://dlq/e.json", 1, json.dumps(payload)),
    ).fetchone()[0]
    # a replay run + corrected output to point resolution at
    replay_run, _ = _run(conn, "replay")
    fid = _raw_file(conn)
    corrected = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (replay_run, json.dumps({"path": "s3://silver/fix", "content_hash": "fx",
                                 "version": 1}), 1,
         json.dumps([{"edge_type": "raw_to_curated", "source_file_id": str(fid),
                      "source_ref": {}, "record_count": 1}])),
    ).fetchone()[0]

    conn.execute(
        "SELECT cp.resolve_dlq(%s,'resolved',%s,%s)",
        (dlq_id, replay_run, corrected),
    )
    row = conn.execute(
        "SELECT status, failed_payload, reason, resolved_by_run_id, "
        "resolved_by_output_link_id, replayed_at FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)
    ).fetchone()
    assert row[0] == "resolved"
    assert row[1] == payload                      # payload untouched
    assert row[2] == "claim_amount must be >= 0"  # reason untouched
    assert str(row[3]) == str(replay_run)
    assert str(row[4]) == str(corrected)
    assert row[5] is not None                     # replayed_at stamped


def test_resolve_dlq_rejects_bad_status(conn):
    """The dlq_status_enum CHECK blocks a status outside the lifecycle set."""
    run_id, _ = _run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','bad',%s,%s,%s)",
        (run_id, json.dumps({}), "s3://dlq/e.json", 1),
    ).fetchone()[0]
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute("SELECT cp.resolve_dlq(%s,'nonsense')", (dlq_id,))


def test_resolve_dlq_unknown_id_raises(conn):
    with pytest.raises(psycopg.errors.RaiseException, match="no dlq row"):
        conn.execute("SELECT cp.resolve_dlq(%s,'resolved')", (str(uuid4()),))


# ---- intermediate lifecycle transitions --------------------------------------

def test_resolve_dlq_under_review_then_resolved(conn):
    run_id, _ = _run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','bad',%s,%s,%s,%s)",
        (run_id, json.dumps({}), "s3://dlq/e.json", 1, json.dumps({"x": 1})),
    ).fetchone()[0]
    conn.execute("SELECT cp.resolve_dlq(%s,'under_review')", (dlq_id,))
    assert conn.execute(
        "SELECT status, replayed_at FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone() == ("under_review", None)
    conn.execute("SELECT cp.resolve_dlq(%s,'resolved')", (dlq_id,))
    row = conn.execute(
        "SELECT status, replayed_at FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()
    assert row[0] == "resolved" and row[1] is not None


# ---- Python wrappers ----------------------------------------------------------

def test_python_quarantine_and_resolve_wrappers(conn):
    run_id, _ = _run(conn)
    payload = {"policy_id": "P1", "claim_amount": -5}
    dlq_id = dlq.quarantine(
        conn, run_id=run_id, stage="validate", reason="bad",
        source_ref={"src": "x"}, payload_ref="s3://dlq/e.json",
        record_count=1, failed_payload=payload, commit=False,
    )
    assert conn.execute(
        "SELECT failed_payload, status FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone() == (payload, "open")
    dlq.resolve(conn, dlq_id=dlq_id, status="rejected", commit=False)
    assert conn.execute(
        "SELECT status FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()[0] == "rejected"
