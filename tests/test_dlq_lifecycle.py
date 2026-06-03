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


# ---- F2: quarantine output traces to the RAW file -----------------------------

def test_quarantine_output_traces_to_raw_via_source_file_id(conn):
    """F2 (migration 030): when cp.quarantine is given p_source_file_id it stamps
    the quarantine EDGE's source_file_id, so the quarantine output_link appears in
    cp.v_provenance WITH that raw file id (no longer NULL / dead-ending), and
    trace_row.sql + dashboard_output_trace reach the raw S3 path from the
    quarantine link. source_ref still carries the raw id too."""
    run_id, _ = _run(conn)
    fid = _raw_file(conn)
    raw_path = conn.execute(
        "SELECT s3_raw_path FROM cp.file_catalogue WHERE file_id=%s", (fid,)
    ).fetchone()[0]

    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','claim_amount must be >= 0',%s,%s,%s,%s,%s)",
        (run_id, json.dumps({"raw_file_id": str(fid)}),
         "s3://dlq/insurance/claim/2026-05-29/errors.json", 1,
         json.dumps({"policy_id": "P1", "claim_amount": -5}), str(fid)),
    ).fetchone()[0]
    qlink = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()[0]

    # the quarantine link is in provenance AND now anchors to the raw file id
    anchored = conn.execute(
        "SELECT count(*) FROM cp.v_provenance "
        "WHERE lineage_link_id=%s AND edge_type='quarantine' AND source_file_id=%s",
        (qlink, str(fid)),
    ).fetchone()[0]
    assert anchored == 1, "quarantine link did not anchor to the raw source_file_id"

    # source_ref STILL carries the raw id (kept, not replaced)
    sref = conn.execute(
        "SELECT source_ref->>'raw_file_id' FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s AND edge_type='quarantine'", (qlink,)
    ).fetchone()[0]
    assert sref == str(fid)

    # trace_row.sql from the quarantine link reaches the raw file + raw path
    sql = open("control/queries/trace_row.sql").read()
    rows = conn.execute(sql, {"link_id": qlink}).fetchall()
    cols = [d.name for d in conn.execute(sql, {"link_id": qlink}).description]
    sfi = cols.index("source_file_id")
    rsp = cols.index("raw_s3_path")
    assert any(str(r[sfi]) == str(fid) and r[rsp] == raw_path for r in rows), \
        "trace_row.sql did not reach the raw file from the quarantine link"

    # dashboard_output_trace(quarantine_link) returns the raw path
    dash = conn.execute(
        "SELECT count(*) FROM cp.dashboard_output_trace(%s) "
        "WHERE source_file_id=%s AND raw_s3_path=%s", (qlink, str(fid), raw_path)
    ).fetchone()[0]
    assert dash == 1


def test_quarantine_without_source_file_id_still_works(conn):
    """F2: p_source_file_id is OPTIONAL — omitting it preserves the pre-030
    behaviour (a NULL source_file_id on the quarantine edge, exempt by the 012
    edge_must_anchor CHECK). The 7-arg form still works."""
    run_id, _ = _run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'validate','bad',%s,%s,%s,%s)",
        (run_id, json.dumps({}), "s3://dlq/e.json", 1, json.dumps({"x": 1})),
    ).fetchone()[0]
    qlink = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()[0]
    sfi = conn.execute(
        "SELECT source_file_id FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (qlink,)
    ).fetchone()[0]
    assert sfi is None


def test_python_quarantine_passes_source_file_id(conn):
    """F2: control.dlq.quarantine accepts source_file_id and stamps the edge."""
    run_id, _ = _run(conn)
    fid = _raw_file(conn)
    dlq_id = dlq.quarantine(
        conn, run_id=run_id, stage="validate", reason="bad",
        source_ref={"raw_file_id": str(fid)}, payload_ref="s3://dlq/e.json",
        record_count=1, failed_payload={"x": 1}, source_file_id=str(fid),
        commit=False,
    )
    qlink = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone()[0]
    assert str(conn.execute(
        "SELECT source_file_id FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (qlink,)
    ).fetchone()[0]) == str(fid)


# ---- F6: reconcile_workflow accounting + deterministic terminal run -----------

def _full_workflow(conn, wf, raw=4, dlq_n=0, dlq_status="open"):
    """Build a single-dataset (orders/sales) workflow: an ingestion run with a
    raw_to_curated link (raw rows), and a sink run with canonical_to_sink rows in
    ods.orders. Optionally quarantine dlq_n rows on the ingestion run. Returns
    (ingest_run, sink_run)."""
    fid = conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid4()}.csv", uuid4().hex, BD),
    ).fetchone()[0]
    ir = conn.execute(
        "SELECT cp.start_run(%s,'ingestion','sales','orders',%s,'manual',%s)",
        (wf, BD, fid),
    ).fetchone()[0]
    good = raw - dlq_n
    raw_link = conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (ir, json.dumps({"path": "s3://silver/o", "content_hash": uuid4().hex,
                         "version": 1}), raw,
         json.dumps([{"edge_type": "raw_to_curated", "source_file_id": str(fid),
                      "source_ref": {}, "record_count": raw}])),
    ).fetchone()[0]
    if dlq_n:
        dlq.quarantine(conn, run_id=ir, stage="validate", reason="bad",
                       source_ref={"raw_file_id": str(fid)},
                       payload_ref="s3://dlq/o.json", record_count=dlq_n,
                       failed_payload={"x": 1}, source_file_id=str(fid),
                       commit=False)
    cr = conn.execute(
        "SELECT cp.start_run(%s,'canonicalize','sales','orders',%s,'manual')",
        (wf, BD),
    ).fetchone()[0]
    canon = conn.execute(
        "SELECT cp.write_lineage_link(%s,'curated_to_canonical',%s,%s,%s)",
        (cr, json.dumps({"path": "s3://gold/o", "content_hash": uuid4().hex,
                         "version": 1}), good,
         json.dumps([{"edge_type": "curated_to_canonical",
                      "upstream_run_id": str(ir),
                      "upstream_lineage_link_id": str(raw_link),
                      "source_ref": {}, "record_count": good}])),
    ).fetchone()[0]
    sr = conn.execute(
        "SELECT cp.start_run(%s,'sink','sales','orders',%s,'manual')",
        (wf, BD),
    ).fetchone()[0]
    conn.execute(
        "SELECT cp.write_link_then_rows(%s,'canonical_to_sink',%s,%s,%s,%s,'postgres')",
        (sr, json.dumps({"path": "ods.orders", "content_hash": uuid4().hex,
                         "version": 1}), good,
         json.dumps([{"edge_type": "canonical_to_sink", "upstream_run_id": str(cr),
                      "upstream_lineage_link_id": str(canon), "source_ref": {},
                      "record_count": good}]),
         json.dumps([{"order_id": i} for i in range(good)])),
    )
    return ir, sr


def test_reconcile_workflow_healthy_is_ok(conn):
    """F6: a balanced workflow (4 raw -> 4 sink, no dlq) reconciles ok."""
    wf = str(uuid4())
    _full_workflow(conn, wf, raw=4, dlq_n=0)
    conn.execute("SELECT cp.reconcile_workflow(%s)", (wf,))
    row = conn.execute(
        "SELECT status, discrepancy, metrics FROM cp.reconciliation_log "
        "WHERE check_type='workflow' AND metrics->>'workflow_run_id'=%s", (wf,)
    ).fetchone()
    assert row[0] == "ok" and row[1] == 0
    assert row[2]["raw_in"] == 4 and row[2]["sink_out"] == 4 and row[2]["dlq_out"] == 0


def test_reconcile_workflow_no_dlq_double_count_after_replay(conn):
    """F6 (b): 4 raw, 3 sink + 1 quarantined reconciles ok WHILE the dlq row is
    open (3 + 1 == 4). After the dlq row is replayed/resolved its loss is
    recovered, so it must NO LONGER be summed into dlq_out (else 3 + 1 == 4 would
    become a phantom over-count once the replay also lands rows). Proven by:
    open -> dlq_out=1; resolved -> dlq_out=0."""
    wf = str(uuid4())
    ir, _ = _full_workflow(conn, wf, raw=4, dlq_n=1)
    # while open: 3 sink + 1 open dlq == 4 raw, ok
    conn.execute("SELECT cp.reconcile_workflow(%s)", (wf,))
    m1 = conn.execute(
        "SELECT status, metrics FROM cp.reconciliation_log "
        "WHERE check_type='workflow' AND metrics->>'workflow_run_id'=%s "
        "ORDER BY recon_id DESC LIMIT 1", (wf,)
    ).fetchone()
    assert m1[1]["dlq_out"] == 1 and m1[0] == "ok"

    # replay/resolve the dlq row: its loss is recovered, no longer un-accounted.
    dlq_id = conn.execute(
        "SELECT dlq_id FROM cp.dlq WHERE run_id=%s", (ir,)
    ).fetchone()[0]
    conn.execute("SELECT cp.resolve_dlq(%s,'resolved',%s)", (dlq_id, ir))
    conn.execute("SELECT cp.reconcile_workflow(%s)", (wf,))
    m2 = conn.execute(
        "SELECT metrics FROM cp.reconciliation_log "
        "WHERE check_type='workflow' AND metrics->>'workflow_run_id'=%s "
        "ORDER BY recon_id DESC LIMIT 1", (wf,)
    ).fetchone()
    assert m2[0]["dlq_out"] == 0, "resolved dlq row was still double-counted"


def test_reconcile_workflow_terminal_run_chosen_by_seq(conn):
    """F6 (a): the terminal run is chosen by finished_at DESC NULLS LAST, seq DESC
    (the 028 deterministic tiebreak). The sink run (highest seq, finalised last)
    must be the reconciliation_log.run_id."""
    wf = str(uuid4())
    _ir, sr = _full_workflow(conn, wf, raw=2, dlq_n=0)
    # finalise the sink run last so it is unambiguously terminal (sets finished_at)
    conn.execute("SELECT cp.patch_run(%s,%s)", (sr, json.dumps({"status": "succeeded"})))
    conn.execute("SELECT cp.reconcile_workflow(%s)", (wf,))
    term = conn.execute(
        "SELECT run_id FROM cp.reconciliation_log "
        "WHERE check_type='workflow' AND metrics->>'workflow_run_id'=%s "
        "ORDER BY recon_id DESC LIMIT 1", (wf,)
    ).fetchone()[0]
    # the chosen terminal run is the one with the max seq for this workflow
    max_seq_run = conn.execute(
        "SELECT run_id FROM cp.run_log WHERE workflow_run_id=%s "
        "ORDER BY finished_at DESC NULLS LAST, seq DESC LIMIT 1", (wf,)
    ).fetchone()[0]
    assert str(term) == str(max_seq_run)


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
    # under_review is a non-terminal state -> lenient, no ref required.
    conn.execute("SELECT cp.resolve_dlq(%s,'under_review')", (dlq_id,))
    assert conn.execute(
        "SELECT status, replayed_at FROM cp.dlq WHERE dlq_id=%s", (dlq_id,)
    ).fetchone() == ("under_review", None)
    # P2c (migration 029): a TERMINAL 'resolved' must be traceable to a run/output;
    # supply the resolving run id so the closed DLQ is traceable.
    conn.execute("SELECT cp.resolve_dlq(%s,'resolved',%s)", (dlq_id, run_id))
    row = conn.execute(
        "SELECT status, replayed_at, resolved_by_run_id FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)
    ).fetchone()
    assert row[0] == "resolved" and row[1] is not None
    assert str(row[2]) == str(run_id)


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
