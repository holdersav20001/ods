"""Replay/refeed REPLACEMENT POLICY — explicit scopes (spec area 4).

Spec: docs/specs/2026-06-03-working-platform-completion-plan.md
      "4. Formalize Replay/Refeed Policy" (Required Policies / Current Behavior
      To Preserve / Required Tests) and
      docs/reference/refeed-replacement-policy.md.

These tests formalize the supported ``replacement_scope`` values end-to-end:

    business_key   replace only specific business keys (changed-only) — PRESERVE
    slice          replace the WHOLE (domain,dataset,business_date) slice
    append_only    add a new active row WITHOUT superseding any prior (supersede=False)
    file           replace rows derived from a specific original file
    manual_approval  DOCUMENTED future option (needs a 'P' status; not built — xfail)

Every test drives recon-ok, succeeded sink outputs via harness.composers.run_to_sink
(ingest -> canonicalize -> sink; the sink run reaches 'succeeded' and writes the
graph-derived sink reconciliation that cp.activate_target_visibility gates on),
then activates visibility with an explicit scope/key. ALL work uses commit=False;
the function-scoped ``conn`` fixture rolls back, so no committed state leaks and
queries are naturally scoped to THIS test's freshly-minted ids.
"""
import datetime
import uuid

import psycopg
import pytest

from control import runs, visibility
from harness import composers

DOMAIN = "sales"
DATASET = "orders"
SINK_TYPE = "postgres"
TARGET = "ods.orders"
BD = datetime.date(2026, 5, 29)

# Distinct base date per sink: harness.fakes.fake_sink ALREADY auto-activates a
# slice-scope visibility row for THIS sink's own (domain,dataset,business_date).
# To keep each test's explicit activations independent of those internal slice
# activations, every _sink() drives on its OWN unique business_date, so the
# internal slice rows never collide with each other. Our explicit activations
# then key on the SHARED logical slice date BD (passed straight to activate —
# the visibility row's business_date is whatever we pass, independent of the
# producing run's own date; activation only gates on link<->run + recon).
_SINK_DATE_BASE = datetime.date(2099, 1, 1)
_sink_counter = [0]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _file(*, record_count, business_date):
    """A fresh raw-file descriptor (unique path + md5 each call)."""
    return {
        "s3_raw_path": f"s3://raw/{uuid.uuid4()}.csv",
        "file_md5": uuid.uuid4().hex,
        "business_date": business_date,
        "domain": DOMAIN,
        "dataset": DATASET,
        "record_count": record_count,
    }


def _sink(conn, *, record_count=3):
    """Drive a full ingest->canonicalize->sink under commit=False on a UNIQUE
    business_date (so fake_sink's internal slice activation is isolated).

    Returns (sink_link_id, sink_run_id, file_id). The sink run is 'succeeded'
    and carries a graph-derived sink reconciliation marked 'ok', so it is
    eligible for activation.
    """
    _sink_counter[0] += 1
    sink_date = _SINK_DATE_BASE + datetime.timedelta(days=_sink_counter[0])
    res = composers.run_to_sink(
        conn, file=_file(record_count=record_count, business_date=sink_date),
        commit=False)
    sink = res["sink"]
    file_id = res["ingest"]["file_id"]
    return sink["link_id"], sink["run_id"], file_id


def _wfid(conn, run_id):
    return conn.execute(
        "SELECT workflow_run_id FROM cp.run_log WHERE run_id=%s", (run_id,)
    ).fetchone()[0]


def _active_rows(conn, *, link_ids):
    """Active (Y) visibility rows whose producing link is one of ``link_ids``
    (i.e. produced by THIS test) AND that an explicit activation in this test
    created — i.e. NOT the internal slice-scope row fake_sink auto-activates on
    each sink's own unique business_date. We scope to business_date=BD (the
    shared logical slice all explicit activations use) so the per-sink internal
    slice rows (on the 2099-* dates) are excluded. Robust to committed demo data.

    Returns (visibility_id:str, replacement_scope, replacement_key, link_id)."""
    rows = conn.execute(
        "SELECT visibility_id, replacement_scope, replacement_key, lineage_link_id "
        "FROM ods.target_visibility "
        "WHERE status='Y' AND business_date=%s AND lineage_link_id = ANY(%s)",
        (BD, list(link_ids),)).fetchall()
    return [(str(r[0]), r[1], r[2], str(r[3])) for r in rows]


def _status(conn, vid):
    return conn.execute(
        "SELECT status FROM ods.target_visibility WHERE visibility_id=%s",
        (vid,)).fetchone()[0]


def _activate(conn, link_id, run_id, file_id, *, scope, key,
              business_date=BD, supersede=True):
    return visibility.activate(
        conn,
        domain=DOMAIN, dataset=DATASET, business_date=business_date,
        sink_type=SINK_TYPE, target_name=TARGET,
        file_id=file_id, output_link_id=link_id,
        producer_run_id=run_id, workflow_run_id=_wfid(conn, run_id),
        replacement_scope=scope, replacement_key=key,
        supersede=supersede, commit=False)


# --------------------------------------------------------------------------- #
# 1. business_key: exactly one active Y per replacement key; changed keys
#    superseded; UNCHANGED keys NOT deactivated by a changed-only refeed.
# --------------------------------------------------------------------------- #
def test_business_key_changed_only_supersedes_only_changed_keys(conn):
    # Day-2 original load: two business keys, each its own sink output + Y row.
    la, ra, _f = _sink(conn)
    lb, rb, _f2 = _sink(conn)
    _, _, fa = la, ra, _f
    va = _activate(conn, la, ra, _f, scope="business_key", key="K1")
    vb = _activate(conn, lb, rb, _f2, scope="business_key", key="K2")

    # Exactly one Y per replacement key for THIS test's links.
    rows = _active_rows(conn, link_ids=[la, lb])
    keys = sorted(r[2] for r in rows)
    assert keys == ["K1", "K2"]
    assert all(r[1] == "business_key" for r in rows)

    # Refeed CHANGES ONLY K1 (a new corrected sink output activated for K1).
    lc, rc, fc = _sink(conn)
    vc = _activate(conn, lc, rc, fc, scope="business_key", key="K1")

    # K1's original row superseded (N); the corrected row is the new Y.
    assert _status(conn, va) == "N"          # changed key: old -> N
    assert _status(conn, vc) == "Y"          # changed key: corrected -> Y
    # UNCHANGED key K2 is untouched — still its ORIGINAL Y (no blanket slice wipe).
    assert _status(conn, vb) == "Y"

    # Still exactly one Y per replacement key (K1 -> corrected, K2 -> original).
    rows = _active_rows(conn, link_ids=[la, lb, lc])
    by_key = {r[2]: r[0] for r in rows}
    assert set(by_key) == {"K1", "K2"}
    assert by_key["K1"] == vc and by_key["K2"] == vb


# --------------------------------------------------------------------------- #
# 2. failed refeed does NOT activate corrected output (succeeded+recon gate).
# --------------------------------------------------------------------------- #
def test_failed_refeed_does_not_activate(conn):
    # Original good output is active for the slice.
    la, ra, fa = _sink(conn)
    va = _activate(conn, la, ra, fa, scope="business_key", key="K1")
    assert _status(conn, va) == "Y"

    # A refeed sink output whose producer run did NOT reach 'succeeded'.
    lb, rb, fb = _sink(conn)
    conn.execute("SAVEPOINT before_fail")
    runs.finalise(conn, rb, status="failed", commit=False)

    with pytest.raises(psycopg.errors.RaiseException, match="must be succeeded"):
        _activate(conn, lb, rb, fb, scope="business_key", key="K1")
    conn.execute("ROLLBACK TO SAVEPOINT before_fail")

    # The prior good output remains the active Y — the failed refeed changed nothing.
    assert _status(conn, va) == "Y"
    rows = _active_rows(conn, link_ids=[la, lb])
    assert [r[0] for r in rows] == [va]


# --------------------------------------------------------------------------- #
# 3. restart before activation leaves the previous active output visible.
# --------------------------------------------------------------------------- #
def test_restart_before_activation_keeps_prior_visible(conn):
    # Day-1 good output activated for the slice.
    la, ra, fa = _sink(conn)
    va = _activate(conn, la, ra, fa, scope="slice",
                   key=f"{DOMAIN}/{DATASET}/{BD}")
    assert _status(conn, va) == "Y"

    # A refeed run that RESTARTS / never reaches activation: build the sink
    # output but simply DO NOT call activate (modelling a crash before the
    # activation step). No supersession may occur.
    lb, rb, fb = _sink(conn)  # succeeded sink exists, but activate never called

    # The prior Day-1 output is STILL the only active row for this slice.
    rows = _active_rows(conn, link_ids=[la, lb])
    assert [r[0] for r in rows] == [va]
    assert _status(conn, va) == "Y"


# --------------------------------------------------------------------------- #
# 4. slice: activating with scope='slice' deactivates the WHOLE slice — even
#    multiple prior business_key Y rows for the same (domain,dataset,date).
# --------------------------------------------------------------------------- #
def test_slice_replacement_deactivates_whole_slice(conn):
    # Original load activated PER business_key (multiple Y rows in the slice).
    la, ra, fa = _sink(conn)
    lb, rb, fb = _sink(conn)
    va = _activate(conn, la, ra, fa, scope="business_key", key="K1")
    vb = _activate(conn, lb, rb, fb, scope="business_key", key="K2")
    assert _status(conn, va) == "Y" and _status(conn, vb) == "Y"

    # A full-slice refeed: scope='slice' supersedes ALL prior active rows for the
    # (domain,dataset,business_date) regardless of their replacement_key.
    ls, rs, fs = _sink(conn)
    slice_key = f"{DOMAIN}/{DATASET}/{BD}"
    vs = _activate(conn, ls, rs, fs, scope="slice", key=slice_key)

    # Both prior per-key rows are now N; exactly one slice Y remains.
    assert _status(conn, va) == "N"
    assert _status(conn, vb) == "N"
    assert _status(conn, vs) == "Y"
    rows = _active_rows(conn, link_ids=[la, lb, ls])
    assert [r[0] for r in rows] == [vs]
    assert rows[0][1] == "slice"


# --------------------------------------------------------------------------- #
# 5. file: scope='file' supersedes only the prior file-scoped row for that file.
# --------------------------------------------------------------------------- #
def test_file_scope_supersedes_only_that_file(conn):
    la, ra, fa = _sink(conn)
    lb, rb, fb = _sink(conn)
    # Two distinct file-scoped activations keyed by the source file identity.
    va = _activate(conn, la, ra, fa, scope="file", key=str(fa))
    vb = _activate(conn, lb, rb, fb, scope="file", key=str(fb))
    assert _status(conn, va) == "Y" and _status(conn, vb) == "Y"

    # A corrected reload of file A supersedes ONLY file A's prior row.
    lc, rc, fc = _sink(conn)
    vc = _activate(conn, lc, rc, fc, scope="file", key=str(fa))
    assert _status(conn, va) == "N"   # file A's prior row superseded
    assert _status(conn, vc) == "Y"   # file A corrected -> Y
    assert _status(conn, vb) == "Y"   # file B untouched


# --------------------------------------------------------------------------- #
# 6. append_only: add without superseding — the prior stays Y, the new is also Y.
# --------------------------------------------------------------------------- #
def test_append_only_adds_without_superseding(conn):
    la, ra, fa = _sink(conn)
    va = _activate(conn, la, ra, fa, scope="append_only", key=str(uuid.uuid4()))
    assert _status(conn, va) == "Y"

    # An append: distinct per-append replacement_key, supersede=False — both
    # remain active (append_only intentionally keeps multiple Y for the slice).
    lb, rb, fb = _sink(conn)
    vb = _activate(conn, lb, rb, fb, scope="append_only", key=str(uuid.uuid4()),
                   supersede=False)
    assert _status(conn, va) == "Y"   # prior NOT superseded
    assert _status(conn, vb) == "Y"   # new also active
    rows = _active_rows(conn, link_ids=[la, lb])
    assert sorted(r[0] for r in rows) == sorted([va, vb])


# --------------------------------------------------------------------------- #
# 7. manual_approval — DOCUMENTED future option (needs a 'P' pending status,
#    not built; ods.target_visibility.status is CHECK (status IN ('Y','N'))).
# --------------------------------------------------------------------------- #
@pytest.mark.xfail(reason="manual_approval pending ('P') status is a documented "
                          "future option — not implemented (spec area 4).",
                   strict=True, raises=psycopg.errors.CheckViolation)
def test_manual_approval_pending_is_future_option(conn):
    la, ra, fa = _sink(conn)
    # A 'P' (pending) status is what manual_approval would need; the CHECK
    # constraint rejects it today. This xfail documents the future option and
    # FAILS LOUDLY (strict) the day someone adds 'P' so the doc can be updated.
    conn.execute(
        "INSERT INTO ods.target_visibility ("
        "  domain, dataset, business_date, sink_type, target_name,"
        "  file_id, lineage_link_id, producer_run_id, workflow_run_id,"
        "  replacement_scope, replacement_key, status) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'P')",
        (DOMAIN, DATASET, BD, SINK_TYPE, TARGET, fa, la, ra,
         _wfid(conn, ra), "manual_approval", "K1"))
