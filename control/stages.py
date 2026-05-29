"""Stage-lifecycle context manager over cp.start_stage / cp.finish_stage.

Usage:
    with stage_scope(conn, run_id, "curate") as st:
        st.record_in = 10
        st.record_out = 9
        st.metrics = {"dropped": 1}

On clean exit the stage is finished 'succeeded'; if an exception propagates it
is finished 'failed' and re-raised.
"""
from contextlib import contextmanager

from psycopg.types.json import Jsonb


class _Stage:
    """Mutable handle for an in-flight stage; the caller sets counts/metrics."""

    def __init__(self, stage_log_id):
        self.stage_log_id = stage_log_id
        self.record_in = None
        self.record_out = None
        self.metrics = None


@contextmanager
def stage_scope(conn, run_id, stage, attempt=1, *, commit=True):
    stage_log_id = conn.execute(
        "SELECT cp.start_stage(%s,%s,%s)", [run_id, stage, attempt]
    ).fetchone()[0]
    st = _Stage(stage_log_id)
    try:
        yield st
    except BaseException:
        conn.execute(
            "SELECT cp.finish_stage(%s,%s,%s,%s,%s)",
            [stage_log_id, "failed", st.record_in, st.record_out,
             Jsonb(st.metrics) if st.metrics is not None else None],
        )
        if commit:
            conn.commit()
        raise
    else:
        conn.execute(
            "SELECT cp.finish_stage(%s,%s,%s,%s,%s)",
            [stage_log_id, "succeeded", st.record_in, st.record_out,
             Jsonb(st.metrics) if st.metrics is not None else None],
        )
        if commit:
            conn.commit()
