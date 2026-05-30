"""Workflow composers: orchestrate fake stages under ONE workflow_run_id.

The composer is the single owner of the connection and of the workflow_run_id.
It mints exactly one workflow_run_id (the only sanctioned mint outside
dlq.replay) and threads it to every stage it drives. As later phases (P3b-d)
add canonicalize / merge / sink hops, they are appended here under the SAME
workflow_run_id.
"""
import uuid

from control import dlq, lineage, recon, runs, stages

from . import fakes


def run_single_file(conn, *, file, commit=True) -> dict:
    """Drive a single raw file through the ingest -> canonicalize pipeline.

    Mints ONE workflow_run_id and threads it to BOTH hops. The canonicalize hop
    discovers its upstream ingest run via latest_succeeded_run (it is NOT handed
    an upstream id here — the composer passes only the slice key + counts).

    Returns the nested per-hop results PLUS the ingest hop's top-level keys
    (run_id/file_id/link_id) for backward compatibility with the GATE-A tests:
        {workflow_run_id, ingest: {...}, canonicalize: {...},
         run_id, file_id, link_id}
    where the top-level run_id/file_id/link_id are the INGEST hop's (the A-tests
    assert against the curated link reaching the raw file).
    """
    workflow_run_id = str(uuid.uuid4())
    ingest = fakes.fake_ingest(
        conn, workflow_run_id=workflow_run_id, file=file, commit=commit)
    canonicalize = fakes.fake_canonicalize(
        conn,
        workflow_run_id=workflow_run_id,
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        record_count=file["record_count"],
        commit=commit,
    )
    return {
        "workflow_run_id": workflow_run_id,
        "ingest": ingest,
        "canonicalize": canonicalize,
        **ingest,  # backward-compat: ingest's run_id/file_id/link_id at top level
    }


def run_multi_file(conn, *, files, commit=True) -> dict:
    """Drive N raw files (same slice, different file_md5) through ingest, then
    MERGE the N ingest runs into ONE canonical link (merge_to_canonical).

    Mints ONE workflow_run_id and threads it to every ingest AND the merge run.
    The merge hop DISCOVERS its N upstream ingest runs (via succeeded_runs) AND
    derives each slot's count from THAT upstream's own record_count_out — it is
    handed no upstream id and no per-slot count list. The previous code passed a
    FILE-order `slot_counts` list which fake_merge then zipped against
    newest-first discovery, binding each slot's count to the WRONG upstream
    (audit F2). The counts now come from the upstreams themselves, so the merge
    composer no longer threads any positional count.

    `files` is a list of file dicts (same domain/dataset/business_date,
    different file_md5). Returns {workflow_run_id, ingests:[...], merge:{...}}.
    """
    workflow_run_id = str(uuid.uuid4())

    ingests = [
        fakes.fake_ingest(
            conn, workflow_run_id=workflow_run_id, file=f, commit=commit)
        for f in files
    ]

    merge = fakes.fake_merge(
        conn,
        workflow_run_id=workflow_run_id,
        domain=files[0]["domain"],
        dataset=files[0]["dataset"],
        business_date=files[0]["business_date"],
        commit=commit,
    )

    return {
        "workflow_run_id": workflow_run_id,
        "ingests": ingests,
        "merge": merge,
    }


def run_to_sink(conn, *, file, sink_type="postgres", commit=True) -> dict:
    """Drive a single file ingest -> canonicalize -> sink (the terminal hop).

    Mints ONE workflow_run_id and threads it to all three hops. The sink hop
    DISCOVERS its canonical upstream (it is handed no upstream id) and writes the
    target rows LAST via write_link_then_rows.

    Returns {workflow_run_id, ingest, canonicalize, sink}.
    """
    workflow_run_id = str(uuid.uuid4())
    ingest = fakes.fake_ingest(
        conn, workflow_run_id=workflow_run_id, file=file, commit=commit)
    canonicalize = fakes.fake_canonicalize(
        conn,
        workflow_run_id=workflow_run_id,
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        record_count=file["record_count"],
        commit=commit,
    )
    sink = fakes.fake_sink(
        conn,
        workflow_run_id=workflow_run_id,
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        record_count=file["record_count"],
        sink_type=sink_type,
        commit=commit,
    )
    return {
        "workflow_run_id": workflow_run_id,
        "ingest": ingest,
        "canonicalize": canonicalize,
        "sink": sink,
    }


def run_to_fanout_sinks(conn, *, file, sink_types=("postgres", "kafka"),
                        commit=True) -> dict:
    """Fan-out: ingest -> canonicalize, then sink the SAME canonical to TWO
    sinks under ONE workflow_run_id (a fake_sink call per sink_type).

    Each fake_sink discovers the same canonical upstream and produces its own
    canonical_to_sink link carrying its sink_type. Every sink link's
    record_count equals the canonical parent's record_count.

    Returns {workflow_run_id, ingest, canonicalize, sinks: {sink_type: {...}}}.
    """
    workflow_run_id = str(uuid.uuid4())
    ingest = fakes.fake_ingest(
        conn, workflow_run_id=workflow_run_id, file=file, commit=commit)
    canonicalize = fakes.fake_canonicalize(
        conn,
        workflow_run_id=workflow_run_id,
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        record_count=file["record_count"],
        commit=commit,
    )
    sinks = {
        st: fakes.fake_sink(
            conn,
            workflow_run_id=workflow_run_id,
            domain=file["domain"],
            dataset=file["dataset"],
            business_date=file["business_date"],
            record_count=file["record_count"],
            sink_type=st,
            commit=commit,
        )
        for st in sink_types
    }
    return {
        "workflow_run_id": workflow_run_id,
        "ingest": ingest,
        "canonicalize": canonicalize,
        "sinks": sinks,
    }


def restart_ingest(conn, *, workflow_run_id, file, commit=True) -> dict:
    """MODEL an Airflow clear-task RESTART of the ingest stage (P10-A).

    A clear-task re-runs a stage FROM THAT TASK FORWARD under the SAME
    workflow_run_id (NOT a new chain, NOT a replay). This helper re-drives the
    ingest hop under a workflow_run_id the caller already minted (e.g. from a
    prior run_single_file / run_to_sink), modelling the restart that was never
    modelled before — which is why the run-grain double-count bug shipped.

    It simply calls fake_ingest again under the SAME workflow_run_id and file.
    With migration 013, cp.start_run is idempotent on the run-identity key, so
    the restart REUSES the original ingest run_id (one run per (wfid, slice,
    file)); register_file and write_lineage_link dedup on their own keys. The
    returned dict therefore carries the SAME run_id/file_id/link_id as the
    original ingest under that workflow_run_id (proof of idempotent restart).

    `file` is the same file dict the original ingest used (same bytes => same
    restart, per decision #5's boundary rule). Returns fake_ingest's
    {run_id, file_id, link_id}.
    """
    return fakes.fake_ingest(
        conn, workflow_run_id=workflow_run_id, file=file, commit=commit)


def replay_single_file(conn, *, original_run_id, file, commit=True) -> dict:
    """REPLAY (refeed) the full chain for a corrected file under a NEW execution.

    This is the X5 requirement: a replayed target row MUST trace to raw via
    cp.v_provenance through its OWN re-written chain — it must NOT dead-end at
    the original (failed) run.

    What it does:
      * Mints a NEW workflow_run_id via control.dlq.replay (the sanctioned mint
        of a fresh execution). dlq.replay also starts the FIRST run of the new
        execution as a 'replay' run (trigger_type='replay',
        replay_of_run_id=original_run_id). That first run IS the re-ingest run,
        so the replay markers live on the re-driven chain itself.
      * Re-drives the FULL normal provenance chain under the new workflow_run_id:
        re-ingest (raw_to_curated, anchored to a freshly registered raw file) ->
        re-canonicalize (curated_to_canonical, discovers the replay ingest run)
        -> re-sink (canonical_to_sink, write_link_then_rows LAST). This chain
        stands ALONE: every hop traces back to raw independently of the original.
      * PLUS a 'replay' annotation edge on the replay canonical link, with
        upstream_run_id=original_run_id, linking the new chain to the original
        run for audit. 'replay' is is_provenance=true so it appears in the walk,
        but the chain does not depend on it to reach raw.

    `file` is the corrected file dict (same slice key as the original). Returns
    {workflow_run_id, ingest, canonicalize, sink, replay_run_id} where
    replay_run_id is the re-ingest run (the one carrying the replay markers).
    """
    # 1. Mint the new execution + its first (replay) run via the sanctioned path.
    new_workflow_run_id, replay_run_id = dlq.replay(
        conn,
        original_run_id=original_run_id,
        pipeline_type="ingestion",
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        commit=commit,
    )

    # 2. Re-INGEST under the replay run (raw_to_curated, anchored to raw). This
    #    is the same shape as fake_ingest but uses the already-started replay run
    #    (so the replay markers stay on the re-driven chain), not a new run.
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
    # Stamp the freshly-registered raw file onto the replay run so the run/file
    # lifecycle mirrors a normal ingest.
    runs.patch(conn, replay_run_id, record_count_in=n, commit=commit)
    with stages.stage_scope(conn, replay_run_id, "ingest", commit=commit) as st:
        st.record_in = n
        st.record_out = n
    ingest_link_id = lineage.write_link(
        conn,
        consumer_run_id=replay_run_id,
        edge_type="raw_to_curated",
        target_ref={
            "path": f"s3://curated/{file['file_md5']}.parquet",
            "content_hash": file["file_md5"],
            "version": 1,
        },
        record_count=n,
        edges=[{
            "source_file_id": file_id,  # raw anchor for the replayed chain
            "edge_type": "raw_to_curated",
            "source_ref": {"path": file["s3_raw_path"]},
            "record_count": n,
        }],
        commit=commit,
    )
    recon.write_check(
        conn, run_id=replay_run_id, check_type="ingest",
        source_count=n, accounted_count=n, commit=commit)
    runs.finalise(conn, replay_run_id, status="succeeded",
                  record_count_out=n, commit=commit)
    ingest = {"run_id": replay_run_id, "file_id": file_id,
              "link_id": ingest_link_id}

    # 3. Re-CANONICALIZE: discovers the replay ingest run (latest_succeeded_run
    #    now returns it), produces curated_to_canonical PLUS a 'replay'
    #    annotation edge to the ORIGINAL run.
    upstream_run_id = runs.latest_succeeded_run(
        conn, domain=file["domain"], dataset=file["dataset"],
        business_date=file["business_date"], pipeline_type="ingestion")
    if upstream_run_id is None:
        raise ValueError("replay: no succeeded ingest run discovered")
    # Name the EXACT upstream output (the replay ingest run's raw_to_curated
    # link) on the curated_to_canonical edge — same wiring as the normal chain.
    up_link = runs.run_output_link(
        conn, run_id=upstream_run_id, edge_type="raw_to_curated")
    canon_run_id = runs.start(
        conn,
        workflow_run_id=new_workflow_run_id,
        pipeline_type="canonicalization",
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        trigger_type="replay",
        replay_of_run_id=original_run_id,
        commit=commit,
    )
    with stages.stage_scope(conn, canon_run_id, "canonicalize", commit=commit) as st:
        st.record_in = n
        st.record_out = n
    canon_link_id = lineage.write_link(
        conn,
        consumer_run_id=canon_run_id,
        edge_type="curated_to_canonical",
        target_ref={
            "path": f"s3://canonical/{file['dataset']}/"
                    f"{file['business_date']}-replay.parquet",
            "content_hash": f"{file['dataset']}-{file['business_date']}-replay",
            "version": 1,
        },
        record_count=n,
        edges=[
            {
                "upstream_run_id": upstream_run_id,  # DISCOVERED replay ingest run
                "upstream_lineage_link_id": up_link,  # the EXACT upstream output
                "edge_type": "curated_to_canonical",
                "source_ref": {"note": "discovered replay ingest run"},
                "record_count": n,
            },
            {
                # ANNOTATION: link the new chain to the ORIGINAL run. is_provenance
                # but the chain reaches raw via the curated_to_canonical edge above
                # regardless of this one.
                "upstream_run_id": original_run_id,
                "edge_type": "replay",
                "source_ref": {"note": "replay of original run"},
                "record_count": n,
            },
        ],
        transform_version="replay",
        commit=commit,
    )
    recon.write_check(
        conn, run_id=canon_run_id, check_type="canonicalize",
        source_count=n, accounted_count=n, commit=commit)
    runs.finalise(conn, canon_run_id, status="succeeded",
                  record_count_out=n, commit=commit)
    canonicalize = {"run_id": canon_run_id, "upstream_run_id": upstream_run_id,
                    "link_id": canon_link_id}

    # 4. Re-SINK under the new execution (canonical_to_sink, rows LAST). Discovers
    #    the replay canonical run.
    sink = fakes.fake_sink(
        conn,
        workflow_run_id=new_workflow_run_id,
        domain=file["domain"],
        dataset=file["dataset"],
        business_date=file["business_date"],
        record_count=n,
        sink_type="postgres",
        commit=commit,
    )

    return {
        "workflow_run_id": new_workflow_run_id,
        "replay_run_id": replay_run_id,
        "ingest": ingest,
        "canonicalize": canonicalize,
        "sink": sink,
    }
