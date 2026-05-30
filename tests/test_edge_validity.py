"""P8 edge-validity + target_ref contract (Codex P1b, P2).

Defect 1 (P1b): malformed edges create lineage leaves that trace to nowhere.
  - raw_to_curated edge must name a file (source_file_id NOT NULL).
  - every provenance edge must anchor to a file OR an upstream output link,
    except the recognised annotation/non-trace types
    (quarantine, orchestrates, replay).
  - write_lineage_link must RAISE if an edge's edge_type differs from the link's
    edge_type, UNLESS it is the allowed annotation ('replay').

Defect 2 (P2): target_ref is convention; make it a contract.
  - DB CHECK: non-empty path AND content_hash AND a present 'version' key.
  - quarantine must comply (now carries version).
  - Python wrappers validate target_ref shape before the DB call.

These probes are written TDD-first: they fail against 001-011 and pass once
012_edge_validity.sql + the Python guards land.
"""
import json
import uuid

import psycopg
import pytest

from control import lineage
from control.db import connect

BD = "2026-05-30"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _start_run(conn, *, status="running"):
    """A minimal real run we can hang lineage links off of."""
    wfid = str(uuid.uuid4())
    run_id = conn.execute(
        "INSERT INTO cp.run_log "
        "(workflow_run_id, pipeline_type, domain, dataset, business_date, "
        " trigger_type, status) "
        "VALUES (%s,'ingestion','sales','orders',%s,'manual',%s) RETURNING run_id",
        (wfid, BD, status),
    ).fetchone()[0]
    return run_id


def _register_file(conn):
    return conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid.uuid4()}.csv", uuid.uuid4().hex, BD),
    ).fetchone()[0]


def _good_target(tag="x"):
    return {"path": f"s3://curated/{tag}.parquet",
            "content_hash": f"hash-{tag}", "version": 1}


# --------------------------------------------------------------------------- #
# DEFECT 1 — edge anchoring + edge_type smuggling
# --------------------------------------------------------------------------- #
def test_raw_to_curated_edge_without_source_file_is_rejected(conn):
    """A raw_to_curated edge with NULL source_file_id is a dangling leaf — the
    raw_edge_requires_source_file CHECK must reject it (CheckViolation)."""
    run_id = _start_run(conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            lineage.write_link(
                conn,
                consumer_run_id=run_id,
                edge_type="raw_to_curated",
                target_ref=_good_target("raw_no_file"),
                record_count=1,
                edges=[{
                    # NO source_file_id — the smuggling/dangling hole.
                    "edge_type": "raw_to_curated",
                    "source_ref": {"path": "s3://raw/x.csv"},
                    "record_count": 1,
                }],
                commit=False,
            )


def test_edge_type_smuggling_under_mismatched_link_raises(conn):
    """The exact P1b probe that PASSED before: a raw_to_curated edge smuggled
    under a curated_to_canonical link. write_lineage_link must now fail-closed
    (RAISE) because the edge edge_type differs from the link edge_type and is
    not the allowed 'replay' annotation."""
    run_id = _start_run(conn)
    file_id = _register_file(conn)
    with pytest.raises(psycopg.errors.RaiseException):
        with conn.transaction():
            lineage.write_link(
                conn,
                consumer_run_id=run_id,
                edge_type="curated_to_canonical",
                target_ref=_good_target("smuggle"),
                record_count=1,
                edges=[{
                    "source_file_id": str(file_id),
                    "edge_type": "raw_to_curated",   # SMUGGLED — differs from link
                    "source_ref": {"path": "s3://raw/x.csv"},
                    "record_count": 1,
                }],
                commit=False,
            )


def test_provenance_edge_with_no_anchor_is_rejected(conn):
    """A curated_to_canonical edge with NEITHER source_file_id NOR
    upstream_lineage_link_id has nothing to trace through — edge_must_anchor (or
    the 009 run-edge CHECK) must reject it (CheckViolation)."""
    run_id = _start_run(conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            lineage.write_link(
                conn,
                consumer_run_id=run_id,
                edge_type="curated_to_canonical",
                target_ref=_good_target("no_anchor"),
                record_count=1,
                edges=[{
                    # neither file nor upstream link — anchorless.
                    "upstream_run_id": str(_start_run(conn)),
                    "edge_type": "curated_to_canonical",
                    "source_ref": {"note": "anchorless"},
                    "record_count": 1,
                }],
                commit=False,
            )


def test_quarantine_edge_without_anchor_still_allowed_and_has_version(conn):
    """A quarantine edge legitimately carries a DLQ payload ref, not a
    file/upstream anchor — the annotation exemption must STILL allow it, and the
    link's target_ref now carries a version (P2)."""
    run_id = _start_run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'curate','bad rows',%s,%s,%s)",
        (run_id, json.dumps({"src": "x"}), "s3://dlq/payload.json", 3),
    ).fetchone()[0]
    assert dlq_id is not None
    tref = conn.execute(
        "SELECT target_ref FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'",
        (run_id,),
    ).fetchone()[0]
    assert tref.get("version") is not None, "quarantine link missing version (P2)"
    assert tref.get("path"), "quarantine link missing path"
    assert tref.get("content_hash"), "quarantine link missing content_hash"


def test_quarantine_with_null_payload_ref_still_has_nonempty_path(conn):
    """If payload_ref is NULL, quarantine must fall back to a non-empty path
    (e.g. 'dlq:'||dlq_id) so target_ref_contract holds."""
    run_id = _start_run(conn)
    dlq_id = conn.execute(
        "SELECT cp.quarantine(%s,'curate','bad rows',%s,NULL,%s)",
        (run_id, json.dumps({"src": "x"}), 3),
    ).fetchone()[0]
    tref = conn.execute(
        "SELECT target_ref FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'",
        (run_id,),
    ).fetchone()[0]
    assert tref.get("path"), "quarantine with NULL payload_ref produced empty path"
    assert tref.get("version") is not None


# --------------------------------------------------------------------------- #
# DEFECT 2 — target_ref contract (DB CHECK)
# --------------------------------------------------------------------------- #
def test_target_ref_missing_version_rejected_by_db(conn):
    """target_ref_contract requires a 'version' key. Bypass the Python guard by
    calling the SQL function directly to prove the DB itself enforces it."""
    run_id = _start_run(conn)
    file_id = _register_file(conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
                (run_id,
                 json.dumps({"path": "s3://c/x.parquet", "content_hash": "h"}),
                 1,
                 json.dumps([{"source_file_id": str(file_id),
                              "edge_type": "raw_to_curated",
                              "record_count": 1}])),
            )


def test_target_ref_empty_content_hash_rejected_by_db(conn):
    """path present but content_hash empty must be rejected (the old F4 CHECK
    allowed this; target_ref_contract closes it)."""
    run_id = _start_run(conn)
    file_id = _register_file(conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
                (run_id,
                 json.dumps({"path": "s3://c/x.parquet",
                             "content_hash": "", "version": 1}),
                 1,
                 json.dumps([{"source_file_id": str(file_id),
                              "edge_type": "raw_to_curated",
                              "record_count": 1}])),
            )


# --------------------------------------------------------------------------- #
# DEFECT 2 — Python wrapper validation (defense in depth, clear errors)
# --------------------------------------------------------------------------- #
def test_python_wrapper_rejects_missing_version(conn):
    run_id = _start_run(conn)
    with pytest.raises(ValueError, match="version"):
        lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref={"path": "s3://c/x.parquet", "content_hash": "h"},
            record_count=1,
            edges=[{"source_file_id": str(_register_file(conn)),
                    "edge_type": "raw_to_curated", "record_count": 1}],
            commit=False,
        )


def test_python_wrapper_rejects_empty_content_hash(conn):
    run_id = _start_run(conn)
    with pytest.raises(ValueError, match="content_hash"):
        lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref={"path": "s3://c/x.parquet", "content_hash": "",
                        "version": 1},
            record_count=1,
            edges=[{"source_file_id": str(_register_file(conn)),
                    "edge_type": "raw_to_curated", "record_count": 1}],
            commit=False,
        )


def test_python_wrapper_rejects_empty_path(conn):
    run_id = _start_run(conn)
    with pytest.raises(ValueError, match="path"):
        lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref={"path": "", "content_hash": "h", "version": 1},
            record_count=1,
            edges=[{"source_file_id": str(_register_file(conn)),
                    "edge_type": "raw_to_curated", "record_count": 1}],
            commit=False,
        )


def test_python_wrapper_rejects_non_dict_target_ref(conn):
    run_id = _start_run(conn)
    with pytest.raises(ValueError):
        lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="raw_to_curated",
            target_ref="not-a-dict",
            record_count=1,
            edges=[{"source_file_id": str(_register_file(conn)),
                    "edge_type": "raw_to_curated", "record_count": 1}],
            commit=False,
        )


def test_write_link_then_rows_validates_target_ref(conn):
    run_id = _start_run(conn)
    with pytest.raises(ValueError, match="version"):
        lineage.write_link_then_rows(
            conn, consumer_run_id=run_id, edge_type="canonical_to_sink",
            target_ref={"path": "s3://sink/x", "content_hash": "h"},
            record_count=1,
            edges=[{"upstream_run_id": str(run_id),
                    "edge_type": "canonical_to_sink", "record_count": 1}],
            rows=[],
            sink_type="postgres",
            commit=False,
        )


# --------------------------------------------------------------------------- #
# replay annotation edge well-formedness (the allowed 'replay' exemption)
# --------------------------------------------------------------------------- #
def test_replay_annotation_edge_allowed_alongside_provenance_edge(conn):
    """A curated_to_canonical link may carry a 'replay' annotation edge ALONGSIDE
    its real provenance edge. 'replay' is the only edge_type allowed to differ
    from the link's edge_type (smuggling allowlist), and the replay edge is
    exempt from edge_must_anchor. This is the exact shape the replay composer
    writes — it must be accepted. (The full end-to-end replay trace is covered
    by tests/test_sink_dlq_replay.py::test_replay_traces_to_raw.)"""
    up_run = _start_run(conn)
    file_id = _register_file(conn)
    # an upstream raw_to_curated output link to anchor the curated_to_canonical
    # provenance edge against.
    up_link = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref=_good_target("rep_up"), record_count=4,
        edges=[{"source_file_id": str(file_id),
                "edge_type": "raw_to_curated", "record_count": 4}],
        commit=False,
    )
    canon_run = _start_run(conn)
    original_run = _start_run(conn)
    link_id = lineage.write_link(
        conn, consumer_run_id=canon_run, edge_type="curated_to_canonical",
        target_ref=_good_target("rep_canon"), record_count=4,
        edges=[
            {"upstream_run_id": str(up_run),
             "upstream_lineage_link_id": up_link,
             "edge_type": "curated_to_canonical", "record_count": 4},
            {"upstream_run_id": str(original_run),
             "edge_type": "replay",   # allowed annotation mismatch + exempt anchor
             "source_ref": {"note": "replay of original run"},
             "record_count": 4},
        ],
        commit=False,
    )
    replay_edges = conn.execute(
        "SELECT source_file_id, upstream_lineage_link_id FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s AND edge_type='replay'",
        (link_id,),
    ).fetchall()
    assert len(replay_edges) == 1
    assert replay_edges[0][0] is None and replay_edges[0][1] is None
