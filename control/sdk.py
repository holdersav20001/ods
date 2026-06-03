"""Developer SDK-style context managers (spec area 5).

A THIN ergonomic layer over the existing control wrappers — NOT a service. It
does not duplicate their logic, it calls them; it does not hide ids
(``run.run_id``, ``stage.stage_log_id``, the returned ``output_link_id`` are all
accessible); and it honours the ``commit`` flag by passing it straight through
(the SDK never forces a commit).

Usage::

    from control.sdk import task

    with task(conn, workflow_run_id=..., pipeline_type=..., domain=...,
              dataset=..., business_date=..., trigger_type=..., commit=False) as run:
        with run.stage("validate_schema") as stage:
            stage.finish(record_count_in=N, record_count_out=M)
        output = run.write_output(
            edge_type="curated_to_canonical", target_ref={...}, record_count=M,
            inputs=[raw_input])            # -> output_link_id; also run.last_output_link_id

On clean exit the run is finalised ``succeeded``; on exception it is marked
``failed`` (with the error recorded) and the exception is RE-RAISED. Stage
failure is recorded by ``stages.stage_scope`` itself; this layer adds only the
``.finish(...)`` convenience that pre-sets the handle's counts/metrics before
stage_scope writes them on exit.
"""
from contextlib import contextmanager

from . import lineage, recon, runs, stages


class _StageHandle:
    """Wraps the live ``stages.stage_scope`` handle. Exposes ``.stage_log_id``
    and the settable ``.record_in/.record_out/.metrics`` (read/written straight
    through to the underlying handle, so stage_scope finalises them on exit),
    plus a ``.finish(...)`` convenience that simply sets those values."""

    def __init__(self, scope_handle):
        self._scope = scope_handle

    @property
    def stage_log_id(self):
        return self._scope.stage_log_id

    @property
    def record_in(self):
        return self._scope.record_in

    @record_in.setter
    def record_in(self, value):
        self._scope.record_in = value

    @property
    def record_out(self):
        return self._scope.record_out

    @record_out.setter
    def record_out(self, value):
        self._scope.record_out = value

    @property
    def metrics(self):
        return self._scope.metrics

    @metrics.setter
    def metrics(self, value):
        self._scope.metrics = value

    def finish(self, *, record_count_in=None, record_count_out=None, metrics=None):
        """Set the stage's counts/metrics; stage_scope writes them on clean exit.

        A convenience only — equivalent to setting ``record_in/record_out/metrics``
        on the handle. Only non-None arguments are applied, so it is safe to call
        for just the value(s) you have."""
        if record_count_in is not None:
            self._scope.record_in = record_count_in
        if record_count_out is not None:
            self._scope.record_out = record_count_out
        if metrics is not None:
            self._scope.metrics = metrics


class _Run:
    """In-flight run handle. Holds the underlying ``conn``/``run_id`` and the
    ``commit`` flag, and exposes the convenience methods. Ids stay visible:
    ``run_id``, ``last_output_link_id``, ``output_link_ids``."""

    def __init__(self, conn, run_id, *, commit):
        self.conn = conn
        self.run_id = run_id
        self._commit = commit
        self.last_output_link_id = None
        self.output_link_ids = []

    @contextmanager
    def stage(self, name, attempt=1):
        """Open a stage via ``stages.stage_scope`` and yield an ergonomic handle.

        On a clean exit the stage is finished ``succeeded`` with whatever
        counts/metrics were set (directly or via ``.finish(...)``); on an
        exception it is finished ``failed`` and the exception propagates — both
        are stage_scope's existing behaviour, unchanged."""
        with stages.stage_scope(self.conn, self.run_id, name, attempt,
                                commit=self._commit) as scope_handle:
            yield _StageHandle(scope_handle)

    def write_output(self, *, edge_type, target_ref, record_count, inputs,
                     sink_type=None, transform_version=None, rows=None,
                     producer_stage_log_id=None):
        """Record one produced output (and its input edges); returns the
        ``output_link_id`` and also stores it on ``last_output_link_id`` /
        appends to ``output_link_ids``.

        Delegates to ``lineage.write_output_then_rows`` when ``rows`` are given
        (stamping the target rows), else ``lineage.write_output_link``. The
        ``commit`` flag is passed straight through.

        ``producer_stage_log_id`` is accepted for call-site readability (which
        stage produced this output) but is not yet recorded by the underlying
        lineage contract; it is intentionally not threaded into a fabricated
        column."""
        if rows is not None:
            output_link_id = lineage.write_output_then_rows(
                self.conn, consumer_run_id=self.run_id, edge_type=edge_type,
                target_ref=target_ref, record_count=record_count, inputs=inputs,
                rows=rows, sink_type=sink_type,
                transform_version=transform_version, commit=self._commit)
        else:
            output_link_id = lineage.write_output_link(
                self.conn, consumer_run_id=self.run_id, edge_type=edge_type,
                target_ref=target_ref, record_count=record_count, inputs=inputs,
                sink_type=sink_type, transform_version=transform_version,
                commit=self._commit)
        self.last_output_link_id = output_link_id
        self.output_link_ids.append(output_link_id)
        return output_link_id

    def reconcile_output(self, output_link_id, source_count):
        """Optional thin passthrough to ``recon.reconcile_sink_link`` for a
        per-output graph-derived sink reconciliation. Honours ``commit``."""
        recon.reconcile_sink_link(
            self.conn, lineage_link_id=output_link_id, source_count=source_count,
            commit=self._commit)


@contextmanager
def task(conn, *, workflow_run_id, pipeline_type, domain, dataset, business_date,
         trigger_type, file_id=None, replay_of_run_id=None, orchestrator=None,
         commit=True):
    """Run-lifecycle context manager: start on enter, finalise on exit.

    On enter calls ``runs.start(...)`` (passing through file_id/replay_of_run_id/
    orchestrator/commit) and yields a ``_Run`` exposing ``.run_id``, ``.stage()``,
    ``.write_output()`` and ``.reconcile_output()``. On a clean exit the run is
    finalised ``succeeded``; on an exception the run is patched ``failed`` with
    the error string recorded and the exception is RE-RAISED (so the caller still
    sees it). The ``commit`` flag is honoured throughout."""
    run_id = runs.start(
        conn, workflow_run_id=workflow_run_id, pipeline_type=pipeline_type,
        domain=domain, dataset=dataset, business_date=business_date,
        trigger_type=trigger_type, file_id=file_id,
        replay_of_run_id=replay_of_run_id, orchestrator=orchestrator,
        commit=commit)
    run = _Run(conn, run_id, commit=commit)
    try:
        yield run
    except BaseException as exc:
        # finalise has no error channel; patch carries the failure reason. A
        # stage that already failed leaves the run-level error here as the
        # task-level cause — the stage row records its own failure.
        runs.patch(conn, run_id, status="failed", error=str(exc), commit=commit)
        raise
    else:
        runs.finalise(conn, run_id, status="succeeded", commit=commit)
