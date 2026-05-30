"""Audit A3 — empirical probes for the spec-vs-itself contradictions.

Findings doc: docs/reviews/2026-05-30-audit-a3.md

These probes do NOT assert "correct" behaviour — they DEMONSTRATE the
contradictions the audit found, so the contradiction is reproducible and the
later fix has a failing/locking test to flip. They use the shared `conn`
rollback fixture (every write is commit=False so nothing leaks), namespace
`audit_a3_*`, and clean up in finally. They NEVER drop or recreate schema.
"""
import uuid

import pytest

from control import runs, lineage


def _new_run(conn, pipeline_type):
    """A committed-in-transaction run row to hang links off (commit=False so the
    rollback fixture discards it)."""
    return runs.start(
        conn,
        workflow_run_id=f"audit_a3_{uuid.uuid4()}",
        pipeline_type=pipeline_type,
        domain="audit_a3_domain",
        dataset="audit_a3_dataset",
        business_date="2026-05-30",
        trigger_type="manual",
        commit=False,
    )


def test_audit_a3_run_output_link_fanout_raises_then_disambiguates(conn):
    """C-1 (HIGH) — FIXED (F1 / migration 010): a run mints K=2 canonical_to_sink
    links (fan-out, which the spec MANDATES). The old
    cp.run_output_link(run, 'canonical_to_sink') returned exactly ONE (newest),
    collapsing the run's outputs back to per-run grain — the precise 'never per
    run' failure C1/decision-#6 exist to forbid, relocated to the DISCOVERY
    layer. Now discovery RAISES on ambiguity and the two outputs are
    individually addressable by target path.

    RED-was: discovered one link, the sibling unreachable. GREEN-now: ambiguous
    discovery RAISES; each fan-out link is addressable by its path."""
    import psycopg
    try:
        # An upstream curated link so the run-to-run CHECK on canonical_to_sink
        # (upstream_lineage_link_id NOT NULL) is satisfiable.
        up_run = _new_run(conn, "canonicalization")
        up_file = conn.execute(
            "SELECT cp.register_file(%s,%s,%s,'audit_a3_dom','audit_a3_ds')",
            (f"s3://raw/{uuid.uuid4()}.csv", uuid.uuid4().hex,
             "2026-05-30")).fetchone()[0]
        up_link = lineage.write_link(
            conn,
            consumer_run_id=up_run,
            edge_type="raw_to_curated",
            target_ref={"path": "s3://audit_a3/curated", "content_hash": "h0",
                        "version": "1"},
            record_count=10,
            edges=[{"source_file_id": str(up_file), "input_slot": 0,
                    "edge_type": "raw_to_curated", "record_count": 10}],
            commit=False,
        )

        sink_run = _new_run(conn, "sink")
        # Fan-out: SAME canonical bytes (same content_hash) to TWO sinks.
        # Post-009 these mint TWO distinct links (output identity in the key).
        link_pg = lineage.write_link(
            conn,
            consumer_run_id=sink_run,
            edge_type="canonical_to_sink",
            target_ref={"path": "postgres://ods/orders", "content_hash": "hX",
                        "version": "1"},
            record_count=10,
            sink_type="postgres",
            edges=[{"upstream_lineage_link_id": up_link, "input_slot": 0,
                    "edge_type": "canonical_to_sink", "record_count": 10}],
            commit=False,
        )
        link_kafka = lineage.write_link(
            conn,
            consumer_run_id=sink_run,
            edge_type="canonical_to_sink",
            target_ref={"path": "kafka://ods.orders", "content_hash": "hX",
                        "version": "1"},
            record_count=10,
            sink_type="kafka",
            edges=[{"upstream_lineage_link_id": up_link, "input_slot": 0,
                    "edge_type": "canonical_to_sink", "record_count": 10}],
            commit=False,
        )

        # The run genuinely has TWO canonical_to_sink links (C2/009 fan-out OK).
        n_links = conn.execute(
            "SELECT count(*) FROM cp.lineage_link "
            "WHERE consumer_run_id=%s AND edge_type='canonical_to_sink'",
            [sink_run],
        ).fetchone()[0]
        assert n_links == 2, n_links
        assert link_pg != link_kafka

        # Discovery with NO target_path is now AMBIGUOUS -> RAISES (no random pick).
        conn.execute("SAVEPOINT a3_ambig")
        with pytest.raises(psycopg.errors.RaiseException, match="ambiguous"):
            runs.run_output_link(
                conn, run_id=sink_run, edge_type="canonical_to_sink")
        conn.execute("ROLLBACK TO SAVEPOINT a3_ambig")

        # BOTH fan-out outputs are individually addressable by their target path.
        got_pg = runs.run_output_link(
            conn, run_id=sink_run, edge_type="canonical_to_sink",
            target_path="postgres://ods/orders")
        got_kafka = runs.run_output_link(
            conn, run_id=sink_run, edge_type="canonical_to_sink",
            target_path="kafka://ods.orders")
        assert got_pg == link_pg
        assert got_kafka == link_kafka
        assert {got_pg, got_kafka} == {link_pg, link_kafka}, (
            "both fan-out outputs must be addressable via discovery (C1/C3)")
    finally:
        conn.rollback()


def test_audit_a3_register_file_per_dataset_distinct(conn):
    """C-3 (MEDIUM) — FIXED (F6 / migration 010): register_file dedupped on
    (file_md5, business_date) ONLY, so the SAME md5 + business_date under a
    DIFFERENT (domain, dataset) returned the FIRST file_id (wrong dataset's
    file). DECISION = per-dataset files: the key is now
    (file_md5, business_date, domain, dataset). Same content in two datasets is
    TWO distinct file_ids; same content+date+dataset is still idempotent.

    RED-was: first == second (cross-dataset collision). GREEN-now: two distinct
    file_ids cross-dataset; one idempotent file_id within a dataset."""
    try:
        md5 = f"audit_a3_{uuid.uuid4().hex}"
        first = runs.register_file(
            conn, s3_raw_path="s3://audit_a3/a.csv", file_md5=md5,
            business_date="2026-05-30", domain="dom_a", dataset="ds_a",
            commit=False)
        second = runs.register_file(
            conn, s3_raw_path="s3://audit_a3/b.csv", file_md5=md5,
            business_date="2026-05-30", domain="dom_b", dataset="ds_b",
            commit=False)
        # Per-dataset grain: different (domain, dataset) -> distinct file_ids.
        assert first != second, (
            "register_file collided cross-dataset — same md5+date in two datasets "
            "must mint TWO distinct file_ids (F6)")
        rows = {
            conn.execute("SELECT domain, dataset FROM cp.file_catalogue "
                         "WHERE file_id=%s", [fid]).fetchone()
            for fid in (first, second)
        }
        assert rows == {("dom_a", "ds_a"), ("dom_b", "ds_b")}, rows
        # And within ONE dataset it is still idempotent (same id).
        again = runs.register_file(
            conn, s3_raw_path="s3://audit_a3/a2.csv", file_md5=md5,
            business_date="2026-05-30", domain="dom_a", dataset="ds_a",
            commit=False)
        assert again == first, "same md5+date+dataset must be idempotent"
    finally:
        conn.rollback()
