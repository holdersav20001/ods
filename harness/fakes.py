"""Fake (Spark-free) pipeline stages for the ODS control-plane harness.

A fake stage exercises the FULL control-plane lifecycle (file registration, run
log, stage scope, lineage link+edges, reconciliation) without doing any real
data transformation. It is the substrate the lineage / recon GATEs run against.

LIFECYCLE CONTRACT (enforced by reviewers):
  * The composer owns the connection AND the workflow_run_id. A fake stage NEVER
    mints a workflow_run_id and NEVER opens its own connection — both arrive as
    arguments.
  * Lineage rows are written ONLY through control.lineage.write_link — never by
    hand-building cp.lineage_edge / cp.lineage_link rows.
  * No raw INSERT SQL; everything goes through the Phase-2 client wrappers.
"""
from control import dlq, lineage, recon, runs, stages


def fake_ingest(conn, *, workflow_run_id, file, commit=True) -> dict:
    """The ingest hop: raw file -> curated parquet (raw_to_curated).

    `file` is a dict: {s3_raw_path, file_md5, business_date, domain, dataset,
    record_count}. Returns the ids produced: {run_id, file_id, link_id}.

    This is a FAKE stage: all rows pass (record_in == record_out), the curated
    content_hash is faked from the raw md5, and recon is balanced by definition.
    The single lineage edge anchors the curated link to the registered RAW file
    (source_file_id) — that is the legitimate raw anchor, NOT a discovered
    upstream run (the ingest hop has no upstream run).
    """
    n = file["record_count"]

    file_id = runs.register_file(
        conn,
        s3_raw_path=file["s3_raw_path"],
        file_md5=file["file_md5"],
        business_date=file["business_date"],
        domain=file["domain"],
        dataset=file["dataset"],
        commit=commit,
    )

    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="ingestion",
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        trigger_type="manual",
        file_id=file_id,
        commit=commit,
    )

    with stages.stage_scope(conn, run_id, "ingest", commit=commit) as st:
        st.record_in = n
        st.record_out = n  # fake: every row passes through

    link_id = lineage.write_link(
        conn,
        consumer_run_id=run_id,
        edge_type="raw_to_curated",
        target_ref={
            "path": f"s3://curated/{file['file_md5']}.parquet",
            "content_hash": file["file_md5"],  # fake curated hash
            "version": 1,
        },
        record_count=n,
        edges=[{
            "source_file_id": file_id,  # raw anchor (the registered RAW file)
            "edge_type": "raw_to_curated",
            "source_ref": {"path": file["s3_raw_path"]},
            "record_count": n,
        }],
        commit=commit,
    )

    recon.write_check(
        conn,
        run_id=run_id,
        check_type="ingest",
        source_count=n,
        accounted_count=n,  # balanced
        commit=commit,
    )

    runs.finalise(conn, run_id, status="succeeded", record_count_out=n,
                  commit=commit)

    return {"run_id": run_id, "file_id": file_id, "link_id": link_id}


def fake_canonicalize(conn, *, workflow_run_id, domain, dataset, business_date,
                      record_count, transform_version="v1", commit=True) -> dict:
    """The canonicalize hop: curated parquet -> canonical parquet.

    DISCOVERY HOP. Its defining feature: it does NOT accept an upstream run id.
    It DISCOVERS its ingest upstream by calling control.runs.latest_succeeded_run
    for the (domain, dataset, business_date, pipeline_type='ingestion') slice.
    The discovered run id becomes the lineage edge's upstream_run_id, so the
    canonical link's provenance spans runs back to the ingest run (and through
    it, to the raw file). There is NO source_file_id on this hop's edge — the
    raw anchor is the ingest edge's job; this hop anchors to a RUN, not a file.

    Returns {run_id, upstream_run_id, link_id}.
    """
    n = record_count

    # 1. DISCOVER the upstream ingest run (never trust a passed-in id).
    upstream_run_id = runs.latest_succeeded_run(
        conn,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        pipeline_type="ingestion",
    )
    if upstream_run_id is None:
        raise ValueError(
            "no succeeded ingestion run to canonicalize for "
            f"({domain}/{dataset}/{business_date})"
        )

    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="canonicalization",
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        trigger_type="manual",
        commit=commit,
    )

    with stages.stage_scope(conn, run_id, "canonicalize", commit=commit) as st:
        st.record_in = n
        st.record_out = n  # fake: every row passes through

    link_id = lineage.write_link(
        conn,
        consumer_run_id=run_id,
        edge_type="curated_to_canonical",
        target_ref={
            "path": f"s3://canonical/{dataset}/{business_date}.parquet",
            "content_hash": f"{dataset}-{business_date}-{transform_version}",
            "version": 1,
        },
        record_count=n,
        edges=[{
            "upstream_run_id": upstream_run_id,  # the DISCOVERED ingest run
            "edge_type": "curated_to_canonical",
            "source_ref": {"note": "discovered ingest run"},
            "record_count": n,
        }],
        transform_version=transform_version,
        commit=commit,
    )

    recon.write_check(
        conn,
        run_id=run_id,
        check_type="canonicalize",
        source_count=n,
        accounted_count=n,  # balanced
        commit=commit,
    )

    runs.finalise(conn, run_id, status="succeeded", record_count_out=n,
                  commit=commit)

    return {"run_id": run_id, "upstream_run_id": upstream_run_id,
            "link_id": link_id}


def fake_merge(conn, *, workflow_run_id, domain, dataset, business_date,
               slot_counts, transform_version="v1", commit=True) -> dict:
    """The merge hop: N upstream ingest runs -> ONE canonical link (merge_to_canonical).

    1:N DISCOVERY HOP. Like fake_canonicalize it accepts NO upstream run id; it
    DISCOVERS *all* succeeded ingestion runs for the slice via
    control.runs.succeeded_runs. The only synthetic input is `slot_counts` — a
    list of per-slot row counts, zipped BY POSITION to the discovered upstreams.

    Produces ONE merge_to_canonical link with N edges (one per discovered
    upstream), each carrying input_slot=i and upstream_run_id=upstreams[i]. The
    link record_count and recon source/accounted counts are sum(slot_counts);
    recon.metrics.per_slot records the per-slot breakdown.

    Returns {run_id, link_id, upstream_run_ids}.
    """
    # 1. DISCOVER all upstream ingest runs (never trust passed-in ids).
    upstreams = runs.succeeded_runs(
        conn,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        pipeline_type="ingestion",
    )
    if len(upstreams) < 2:
        raise ValueError(
            "merge needs >=2 succeeded ingestion runs, discovered "
            f"{len(upstreams)} for ({domain}/{dataset}/{business_date})"
        )
    if len(upstreams) != len(slot_counts):
        raise ValueError(
            f"slot_counts has {len(slot_counts)} entries but discovery found "
            f"{len(upstreams)} upstream runs — one count per discovered upstream "
            "is required"
        )

    total = sum(slot_counts)

    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="merge",
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        trigger_type="manual",
        commit=commit,
    )

    with stages.stage_scope(conn, run_id, "merge", commit=commit) as st:
        st.record_in = total
        st.record_out = total  # fake: union of all slots, no dedup

    edges = [
        {
            "upstream_run_id": up,          # the DISCOVERED ingest run for this slot
            "input_slot": i,
            "edge_type": "merge_to_canonical",
            "source_ref": {"slot": i},
            "record_count": cnt,
        }
        for i, (up, cnt) in enumerate(zip(upstreams, slot_counts))
    ]

    link_id = lineage.write_link(
        conn,
        consumer_run_id=run_id,
        edge_type="merge_to_canonical",
        target_ref={
            "path": f"s3://canonical/{dataset}/{business_date}-merged.parquet",
            "content_hash": f"{dataset}-{business_date}-merged-{transform_version}",
            "version": 1,
        },
        record_count=total,
        edges=edges,
        transform_version=transform_version,
        commit=commit,
    )

    recon.write_check(
        conn,
        run_id=run_id,
        check_type="merge",
        source_count=total,
        accounted_count=total,  # balanced: union of all slots accounted for
        metrics={"per_slot": {str(i): c for i, c in enumerate(slot_counts)}},
        commit=commit,
    )

    runs.finalise(conn, run_id, status="succeeded", record_count_out=total,
                  commit=commit)

    return {"run_id": run_id, "link_id": link_id,
            "upstream_run_ids": upstreams}


def fake_sink(conn, *, workflow_run_id, domain, dataset, business_date,
              record_count, sink_type="postgres",
              upstream_pipeline_type="canonicalization", commit=True) -> dict:
    """The sink hop: canonical/merge parquet -> target rows (canonical_to_sink).

    TERMINAL HOP and the ONLY hop that writes target rows — POSTGRES WRITE IS
    LAST. Like the canonicalize/merge hops it is a DISCOVERY hop: it accepts NO
    upstream run id, it DISCOVERS its upstream canonical (or merge) run via
    control.runs.latest_succeeded_run for (domain, dataset, business_date,
    pipeline_type=upstream_pipeline_type). The discovered run becomes the
    canonical_to_sink edge's upstream_run_id, so the sink link's provenance spans
    back through the canonical run to the raw file.

    The target rows are written through control.lineage.write_link_then_rows —
    the sanctioned primitive that writes the link+edges FIRST, THEN inserts the
    rows into ods.<dataset> in the SAME transaction, stamping
    _ods_lineage_link_id + _ods_workflow_run_id. The FK on _ods_lineage_link_id
    guarantees no target row can exist without its committed link.

    `sink_type` ('postgres', 'kafka', ...) is stamped on the link (the
    sink_type_iff_sink CHECK requires it non-null for canonical_to_sink). Calling
    fake_sink twice for the same slice with different sink_type fans the same
    canonical out to two sinks (two canonical_to_sink links).

    Returns {run_id, link_id}.
    """
    n = record_count

    # 1. DISCOVER the upstream canonical/merge run (never trust a passed-in id).
    upstream = runs.latest_succeeded_run(
        conn,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        pipeline_type=upstream_pipeline_type,
    )
    if upstream is None:
        raise ValueError(
            "no succeeded "
            f"{upstream_pipeline_type} run to sink for "
            f"({domain}/{dataset}/{business_date})"
        )

    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="sink",
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        trigger_type="manual",
        commit=commit,
    )

    with stages.stage_scope(conn, run_id, "sink", commit=commit) as st:
        st.record_in = n
        st.record_out = n  # fake: every row written

    rows = [{"k": i} for i in range(n)]
    edges = [{
        "upstream_run_id": upstream,  # the DISCOVERED canonical/merge run
        "edge_type": "canonical_to_sink",
        "source_ref": {"note": "discovered canonical/merge run"},
        "record_count": n,
    }]

    # POSTGRES WRITE LAST: link+edges written, THEN rows, in one transaction.
    link_id = lineage.write_link_then_rows(
        conn,
        consumer_run_id=run_id,
        edge_type="canonical_to_sink",
        target_ref={
            "path": f"{sink_type}://{dataset}",
            "content_hash": f"{dataset}-{business_date}-{sink_type}",
            "version": 1,
        },
        record_count=n,
        edges=edges,
        rows=rows,
        sink_type=sink_type,
        commit=commit,
    )

    recon.write_check(
        conn,
        run_id=run_id,
        check_type="sink",
        source_count=n,
        accounted_count=n,  # balanced: every canonical row written to the sink
        commit=commit,
    )

    runs.finalise(conn, run_id, status="succeeded", record_count_out=n,
                  commit=commit)

    return {"run_id": run_id, "link_id": link_id}


def fake_fail(conn, *, workflow_run_id, domain, dataset, business_date,
              good_count, bad_count, commit=True) -> dict:
    """A stage that partially fails: good_count rows pass, bad_count are
    quarantined to the DLQ.

    Proves DLQ rows appear in the lineage graph and in recon. The DLQ write
    goes through control.dlq.quarantine, which inserts the dlq row AND a
    'quarantine' lineage link+edge atomically (so the quarantined rows are
    reachable in cp.v_provenance). Recon is written as source=good+bad,
    accounted=good+bad (good rows + dlq rows together account for the source),
    so the balanced case has discrepancy 0; a row lost without being DLQ'd would
    breach.

    Returns {run_id, dlq_id, link_id} where link_id is the quarantine link.
    """
    source = good_count + bad_count

    run_id = runs.start(
        conn,
        workflow_run_id=workflow_run_id,
        pipeline_type="canonicalization",
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        trigger_type="manual",
        commit=commit,
    )

    with stages.stage_scope(conn, run_id, "canonicalize", commit=commit) as st:
        st.record_in = source
        st.record_out = good_count  # bad rows did NOT pass

    dlq_id = dlq.quarantine(
        conn,
        run_id=run_id,
        stage="canonicalize",
        reason="fake validation failure",
        source_ref={"note": "synthetic bad rows"},
        payload_ref=f"s3://dlq/{dataset}/{business_date}.json",
        record_count=bad_count,
        commit=commit,
    )

    # The quarantine() function wrote a 'quarantine' link discriminated by the
    # dlq_id; surface its link_id so tests can assert it is in the graph.
    link_id = str(conn.execute(
        "SELECT lineage_link_id FROM cp.lineage_link "
        "WHERE consumer_run_id=%s AND edge_type='quarantine'",
        (run_id,)).fetchone()[0])

    # Recon: good rows + DLQ'd rows together account for the source. Balanced.
    recon.write_check(
        conn,
        run_id=run_id,
        check_type="canonicalize",
        source_count=source,
        accounted_count=good_count + bad_count,
        metrics={"good": good_count, "dlq": bad_count},
        commit=commit,
    )

    runs.finalise(conn, run_id, status="succeeded", record_count_out=good_count,
                  commit=commit)

    return {"run_id": run_id, "dlq_id": dlq_id, "link_id": link_id}
