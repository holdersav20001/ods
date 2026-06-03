"""P4 item 2 — reconciliation NEGATIVES (the regression matrix).

cp.write_reconciliation_check computes discrepancy = source - accounted and
status: ok (0) / breach (>0, rows vanished) / double_count (<0, over-accounted).
These drive the check directly (and via fake_fail with imbalanced counts) and
assert the reconciliation_log row, then print the evidence table.

Run `pytest tests/test_recon.py -v -s` to capture the printed status table.
"""
import datetime
import uuid

import pytest

from control import recon, runs
from harness import fakes

BD = datetime.date(2025, 8, 12)


def _start(conn):
    wfid = str(uuid.uuid4())
    return runs.start(
        conn, workflow_run_id=wfid, pipeline_type="canonicalization",
        domain="sales", dataset="orders", business_date=BD,
        trigger_type="manual", commit=False)


def _recon_row(conn, run_id):
    return conn.execute(
        "SELECT source_count, accounted_count, discrepancy, status "
        "FROM cp.reconciliation_log WHERE run_id=%s", (run_id,)).fetchone()


# --------------------------------------------------------------------------- #
# Direct drive via recon.write_check — the three balance regimes.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "good,dlq,source,exp_disc,exp_status",
    [
        (17, 3, 20, 0, "ok"),           # good+dlq == source  -> balanced
        (15, 3, 20, 2, "breach"),       # good+dlq <  source  -> rows vanished
        (18, 5, 20, -3, "double_count"),# good+dlq >  source  -> double count
    ],
)
def test_recon_balance_regimes(conn, good, dlq, source, exp_disc, exp_status):
    """SCOPE (A3 audit 2026-06-03): this pins the ARITHMETIC CONTRACT of
    recon.write_check only — given a caller-supplied (source, accounted) pair it
    computes discrepancy=source-accounted and the ok/breach/double_count status.
    It does NOT detect real row loss: the test hands the function both numbers, so
    it is balanced/imbalanced purely by what the parametrize row chose. Real
    row-loss detection (where accounted is DB-DERIVED, not caller-supplied) lives
    in test_graph_recon.py and test_dlq_lifecycle.py::reconcile_workflow tests —
    those DELETE real rows and assert the breach. Keep these as the contract guard
    for write_check's status math."""
    run_id = _start(conn)
    recon.write_check(
        conn, run_id=run_id, check_type="canonicalize",
        source_count=source, accounted_count=good + dlq,
        metrics={"good": good, "dlq": dlq}, commit=False)
    src, acc, disc, status = _recon_row(conn, run_id)
    assert src == source
    assert acc == good + dlq
    assert disc == exp_disc
    assert status == exp_status
    print(f"\n[RECON] good={good:>2} dlq={dlq:>2} source={source:>2} "
          f"accounted={good + dlq:>2} -> discrepancy={disc:>3} status={status}")


# --------------------------------------------------------------------------- #
# Drive via fake_fail (a real partial-failure stage): balanced quarantine.
# --------------------------------------------------------------------------- #
def test_recon_balanced_via_fake_fail(conn):
    wfid = str(uuid.uuid4())
    res = fakes.fake_fail(
        conn, workflow_run_id=wfid, domain="sales", dataset="orders",
        business_date=BD, good_count=17, bad_count=3, commit=False)
    src, acc, disc, status = _recon_row(conn, res["run_id"])
    assert (src, acc, disc, status) == (20, 20, 0, "ok")
    print(f"\n[RECON via fake_fail] good=17 dlq=3 source=20 "
          f"accounted={acc} -> discrepancy={disc} status={status}")


# --------------------------------------------------------------------------- #
# Drive via fake_fail with an IMBALANCED accounted: simulate vanished rows by
# under-accounting -> breach. (We write the recon directly for the imbalanced
# case since fake_fail is balanced by construction.)
# --------------------------------------------------------------------------- #
def test_recon_breach_rows_vanished(conn):
    run_id = _start(conn)
    # 20 source, only 15 good accounted, NO dlq -> 5 rows vanished silently.
    recon.write_check(
        conn, run_id=run_id, check_type="canonicalize",
        source_count=20, accounted_count=15, commit=False)
    src, acc, disc, status = _recon_row(conn, run_id)
    assert disc == 5 and status == "breach"
    print(f"\n[RECON breach] source=20 accounted=15 -> "
          f"discrepancy={disc} status={status} (rows vanished)")


def test_recon_double_count(conn):
    run_id = _start(conn)
    recon.write_check(
        conn, run_id=run_id, check_type="canonicalize",
        source_count=20, accounted_count=23, commit=False)
    src, acc, disc, status = _recon_row(conn, run_id)
    assert disc == -3 and status == "double_count"
    print(f"\n[RECON double_count] source=20 accounted=23 -> "
          f"discrepancy={disc} status={status} (over-accounted)")
