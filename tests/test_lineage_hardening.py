"""C4 — lineage-link hardening tests (migration 009).

These prove the two defects the hardening pass fixes:

  C2 (output identity): one run can write TWO canonical_to_sink links with the
      SAME content_hash but DIFFERENT path/sink_type -> two distinct links. This
      test would FAIL on the pre-009 key (consumer_run_id, edge_type,
      content_hash), which collapsed them to one.

  C3 (downstream specificity): a downstream edge that names ONE output of a
      multi-output upstream run (via upstream_lineage_link_id) walks
      cp.v_provenance to THAT output's ancestors, NOT the run's other output.

  C1 (multi-output independence): two outputs of one run trace independently to
      their own inputs.

Plus a constraint test: the CHECK rejects a run-to-run edge with NULL
upstream_lineage_link_id.

All use the rolled-back `conn` fixture (commit=False) for isolation.
"""
import datetime
import json
import pathlib
import uuid

import pytest
import psycopg

from control import lineage, runs
from harness import composers

BD = datetime.date(2026, 5, 29)

TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
             / "control" / "queries" / "trace_row.sql").read_text()


def _file(record_count=12, dataset="orders"):
    md5 = "md5-" + uuid.uuid4().hex
    return {
        "s3_raw_path": f"s3://raw/sales/{dataset}/{md5}.csv",
        "file_md5": md5,
        "business_date": BD,
        "domain": "sales",
        "dataset": dataset,
        "record_count": record_count,
    }


# --------------------------------------------------------------------------- #
# C2 — same content_hash, different path/sink_type -> TWO distinct links.
#      (The test that FAILED before 009.)
# --------------------------------------------------------------------------- #
def test_same_hash_different_target_yields_two_links(conn):
    # One canonical run that two sinks consume.
    up_run = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="canonicalization",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)
    up_link = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="curated_to_canonical",
        target_ref={"path": "s3://canonical/orders", "content_hash": "CANON",
                    "version": 1},
        record_count=5,
        edges=[{"upstream_run_id": up_run, "upstream_lineage_link_id": None,
                "source_file_id": None, "edge_type": "raw_to_curated",
                "record_count": 5}],
        commit=False)

    sink_run = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="sink",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)

    HASH = "orders-2026-05-29-canonical"  # SAME canonical bytes for both sinks
    pg_link = lineage.write_link(
        conn, consumer_run_id=sink_run, edge_type="canonical_to_sink",
        target_ref={"path": "postgres://orders", "content_hash": HASH, "version": 1},
        record_count=5, sink_type="postgres",
        edges=[{"upstream_lineage_link_id": up_link, "edge_type": "canonical_to_sink",
                "record_count": 5}],
        commit=False)
    kafka_link = lineage.write_link(
        conn, consumer_run_id=sink_run, edge_type="canonical_to_sink",
        target_ref={"path": "kafka://orders", "content_hash": HASH, "version": 1},
        record_count=5, sink_type="kafka",
        edges=[{"upstream_lineage_link_id": up_link, "edge_type": "canonical_to_sink",
                "record_count": 5}],
        commit=False)

    assert pg_link != kafka_link, (
        "same content_hash + same consumer_run_id collapsed two sinks to ONE "
        "link — the pre-009 dedup bug")

    rows = conn.execute(
        "SELECT sink_type, target_ref->>'content_hash' FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='canonical_to_sink' "
        "ORDER BY sink_type", (sink_run,)).fetchall()
    print("\n[C2] same-hash two-links — sink_run", sink_run)
    for st, h in rows:
        print("    sink_type", st, "content_hash", h)
    assert {r[0] for r in rows} == {"postgres", "kafka"}
    assert {r[1] for r in rows} == {HASH}, "both links must share the SAME hash"


# --------------------------------------------------------------------------- #
# C3 — downstream specificity: an edge naming ONE output of a multi-output
#      upstream run walks to THAT output's ancestors, NOT the other output.
# --------------------------------------------------------------------------- #
def test_downstream_edge_specificity(conn):
    # Build a multi-output upstream run: ONE run, TWO raw_to_curated outputs,
    # each with its OWN source file (its own ancestor).
    up_run = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="ingestion",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)
    file_a = runs.register_file(
        conn, s3_raw_path="s3://raw/A.csv", file_md5="md5-A-" + uuid.uuid4().hex,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    file_b = runs.register_file(
        conn, s3_raw_path="s3://raw/B.csv", file_md5="md5-B-" + uuid.uuid4().hex,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    out_a = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/A", "content_hash": "OUT-A", "version": 1},
        record_count=3,
        edges=[{"source_file_id": file_a, "edge_type": "raw_to_curated",
                "source_ref": {"path": "s3://raw/A.csv"}, "record_count": 3}],
        commit=False)
    out_b = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/B", "content_hash": "OUT-B", "version": 1},
        record_count=3,
        edges=[{"source_file_id": file_b, "edge_type": "raw_to_curated",
                "source_ref": {"path": "s3://raw/B.csv"}, "record_count": 3}],
        commit=False)
    assert out_a != out_b

    # Downstream run consumes ONLY output A (names out_a via upstream_lineage_link_id).
    down_run = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="canonicalization",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)
    down_link = lineage.write_link(
        conn, consumer_run_id=down_run, edge_type="curated_to_canonical",
        target_ref={"path": "s3://canonical/A", "content_hash": "CANON-A", "version": 1},
        record_count=3,
        edges=[{"upstream_run_id": up_run, "upstream_lineage_link_id": out_a,
                "edge_type": "curated_to_canonical",
                "source_ref": {"note": "consumes output A only"}, "record_count": 3}],
        commit=False)

    # v_provenance from the downstream link reaches output A's ancestors (file_a)
    # but NOT output B's (file_b). Walk the closure starting at down_link via the
    # link->link adjacency (upstream_lineage_link_id).
    walk = conn.execute(
        """
        WITH RECURSIVE chain AS (
            SELECT lineage_link_id, source_file_id
            FROM cp.v_provenance WHERE lineage_link_id=%s
          UNION
            SELECT p.lineage_link_id, p.source_file_id
            FROM chain c
            JOIN cp.lineage_edge e ON e.lineage_link_id = c.lineage_link_id
            JOIN cp.v_provenance p ON p.lineage_link_id = e.upstream_lineage_link_id
        )
        SELECT DISTINCT source_file_id FROM chain WHERE source_file_id IS NOT NULL
        """, (down_link,)).fetchall()
    reached = {str(r[0]) for r in walk}
    print("\n[C3] downstream specificity — down_link", down_link)
    print("    reached source files:", reached)
    print("    file_a:", file_a, "file_b:", file_b)

    assert file_a in reached, "downstream walk did NOT reach the named output's ancestor"
    assert file_b not in reached, (
        "downstream walk over-claimed the OTHER output of the multi-output run "
        "(defect 2: link->link walk must not pull sibling outputs)")


# --------------------------------------------------------------------------- #
# C1 — multi-output independence: two outputs of one run trace independently.
# --------------------------------------------------------------------------- #
def test_multi_output_independence(conn):
    up_run = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="ingestion",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)
    file_a = runs.register_file(
        conn, s3_raw_path="s3://raw/IA.csv", file_md5="md5-IA-" + uuid.uuid4().hex,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    file_b = runs.register_file(
        conn, s3_raw_path="s3://raw/IB.csv", file_md5="md5-IB-" + uuid.uuid4().hex,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    out_a = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/IA", "content_hash": "IND-A", "version": 1},
        record_count=2,
        edges=[{"source_file_id": file_a, "edge_type": "raw_to_curated",
                "record_count": 2}],
        commit=False)
    out_b = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/IB", "content_hash": "IND-B", "version": 1},
        record_count=2,
        edges=[{"source_file_id": file_b, "edge_type": "raw_to_curated",
                "record_count": 2}],
        commit=False)

    def _files_for(link):
        return {str(r[0]) for r in conn.execute(
            "SELECT DISTINCT source_file_id FROM cp.v_provenance "
            "WHERE lineage_link_id=%s AND source_file_id IS NOT NULL",
            (link,)).fetchall()}

    assert _files_for(out_a) == {file_a}
    assert _files_for(out_b) == {file_b}


# --------------------------------------------------------------------------- #
# CHECK — run-to-run edge with NULL upstream_lineage_link_id is REJECTED.
# --------------------------------------------------------------------------- #
def test_run_edge_requires_upstream_link(conn):
    run_id = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="canonicalization",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)
    with pytest.raises(psycopg.errors.CheckViolation):
        # curated_to_canonical edge with NO upstream_lineage_link_id -> CHECK fails.
        lineage.write_link(
            conn, consumer_run_id=run_id, edge_type="curated_to_canonical",
            target_ref={"path": "s3://c/x", "content_hash": "X", "version": 1},
            record_count=1,
            edges=[{"upstream_run_id": run_id, "edge_type": "curated_to_canonical",
                    "record_count": 1}],  # upstream_lineage_link_id missing -> NULL
            commit=False)


# --------------------------------------------------------------------------- #
# Harness-backed fan-out: two sinks now share the SAME hash (crutch removed),
# disambiguated by sink_type/path -> two links, equal counts.
# --------------------------------------------------------------------------- #
def test_harness_fanout_same_hash_two_links(conn):
    f = _file(15)
    res = composers.run_to_fanout_sinks(
        conn, file=f, sink_types=("postgres", "kafka"), commit=False)
    wfid = res["workflow_run_id"]
    links = conn.execute(
        "SELECT l.sink_type, l.record_count, l.target_ref->>'content_hash' "
        "FROM cp.lineage_link l JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
        "WHERE l.edge_type='canonical_to_sink' AND r.workflow_run_id=%s "
        "ORDER BY l.sink_type", (wfid,)).fetchall()
    assert {r[0] for r in links} == {"postgres", "kafka"}
    assert all(r[1] == f["record_count"] for r in links)
    assert len({r[2] for r in links}) == 1, (
        "fan-out links must now share the SAME canonical content_hash "
        "(sink_type no longer baked into the hash)")
    print("\n[C2-harness] fan-out same hash:", {r[2] for r in links})


# --------------------------------------------------------------------------- #
# trace_row.sql alignment: the debug query now walks link->link too, so given a
# multi-output upstream it must NOT pull the sibling output (matches v_provenance).
# --------------------------------------------------------------------------- #
def test_trace_row_sql_no_sibling_overclaim(conn):
    # Same shape as C3: one upstream run, TWO raw_to_curated outputs (own files);
    # a downstream curated_to_canonical link names ONLY output A.
    up_run = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="ingestion",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)
    file_a = runs.register_file(
        conn, s3_raw_path="s3://raw/TA.csv", file_md5="md5-TA-" + uuid.uuid4().hex,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    file_b = runs.register_file(
        conn, s3_raw_path="s3://raw/TB.csv", file_md5="md5-TB-" + uuid.uuid4().hex,
        business_date=BD, domain="sales", dataset="orders", commit=False)
    out_a = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/TA", "content_hash": "TOUT-A", "version": 1},
        record_count=3,
        edges=[{"source_file_id": file_a, "edge_type": "raw_to_curated",
                "source_ref": {"path": "s3://raw/TA.csv"}, "record_count": 3}],
        commit=False)
    out_b = lineage.write_link(
        conn, consumer_run_id=up_run, edge_type="raw_to_curated",
        target_ref={"path": "s3://curated/TB", "content_hash": "TOUT-B", "version": 1},
        record_count=3,
        edges=[{"source_file_id": file_b, "edge_type": "raw_to_curated",
                "source_ref": {"path": "s3://raw/TB.csv"}, "record_count": 3}],
        commit=False)
    assert out_a != out_b

    down_run = runs.start(
        conn, workflow_run_id=str(uuid.uuid4()), pipeline_type="canonicalization",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)
    down_link = lineage.write_link(
        conn, consumer_run_id=down_run, edge_type="curated_to_canonical",
        target_ref={"path": "s3://canonical/TA", "content_hash": "TCANON-A", "version": 1},
        record_count=3,
        edges=[{"upstream_run_id": up_run, "upstream_lineage_link_id": out_a,
                "edge_type": "curated_to_canonical",
                "source_ref": {"note": "consumes output A only"}, "record_count": 3}],
        commit=False)

    chain = conn.execute(TRACE_SQL, {"link_id": down_link}).fetchall()
    edge_types = [c[1] for c in chain]
    raw_paths = [c[5] for c in chain if c[5] is not None]

    print("\n[trace_row link->link] down_link", down_link)
    for c in chain:
        print("    hop", c[0], c[1], "consumer=", c[2], "upstream=", c[3], "raw=", c[5])

    # The chain reaches output A's raw file, NOT output B's sibling.
    assert "curated_to_canonical" in edge_types
    assert "raw_to_curated" in edge_types
    assert "s3://raw/TA.csv" in raw_paths, "trace did NOT reach the named output's raw file"
    assert "s3://raw/TB.csv" not in raw_paths, (
        "trace_row.sql over-claimed the sibling output of the multi-output "
        "upstream run (run->run walk regression — must follow upstream_lineage_link_id)")
