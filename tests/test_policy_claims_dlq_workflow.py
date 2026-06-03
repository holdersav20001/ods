"""Tests for the insurance policy/claims DLQ DEMO workflow (areas 2 + 3).

Spec: docs/specs/2026-06-03-working-platform-completion-plan.md
      area 2 (DLQ "Required Tests" + DLQ acceptance) + area 3 (schema validation).

Proves the schema-validation -> quarantine -> replay/resolve story:
  1. DLQ row written on validation failure.
  2. dlq row carries reason + failed_payload (enough to diagnose).
  3. first-class quarantine output_link (edge_type='quarantine', record_count=1).
  4. quarantine output is visible in cp.v_provenance.
  5. good output AND quarantine output both trace to the raw claim file.
  6. recon: input(4) == good(3) + dlq(1), status 'ok'.
  7. DLQ replay CONSUMES the prior DLQ/quarantine identity (its input edge
     references the quarantine output_link / dlq_id).
  8. the replayed/fixed row traces back to the original raw + DLQ context.
  9. bad row NOT business-visible before replay; corrected row IS after (dlq
     status resolved + active Y).
Plus schema tests: missing-required-column -> DLQ, nullable-violation -> DLQ,
valid -> good, output records schema_version (target_ref), validation metrics in
run_stage_log.metrics.

Each test runs run_demo(conn, commit=False) once via a function-scoped fixture
that rolls back through the `conn` fixture, so no committed state leaks. Every
global DLQ/visibility/provenance query is SCOPED to THIS run's own wfids/run set
so assertions are robust to committed demo data (mirrors test_07 in the
policy/claims test).
"""
import pathlib

import pytest

from control import schema
from harness.policy_claims_dlq_workflow import (
    AGG_DATASET,
    BAD_CLAIM_ROW,
    BUSINESS_DATE,
    CLAIM_ROWS,
    CONTRACT_DATASET,
    CONTRACT_LAYER,
    CORRECTED_CLAIM_ROW,
    DETAIL_DATASET,
    DLQ_STAGE,
    DOMAIN,
    GOOD_CLAIM_ROWS,
    SCHEMA_VERSION,
    detail_business_key,
    run_demo,
)

TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
             / "control" / "queries" / "trace_row.sql").read_text()


@pytest.fixture
def demo(conn):
    """Run the full DLQ demo once (rolled back by the `conn` fixture)."""
    return run_demo(conn, commit=False)


def _wfids(demo):
    return [demo["normal"]["workflow_run_id"], demo["replay"]["workflow_run_id"]]


def _own_run_ids(conn, demo):
    """Every run_id belonging to THIS demo's executions (for scoped queries)."""
    return [str(r[0]) for r in conn.execute(
        "SELECT run_id FROM cp.run_log WHERE workflow_run_id = ANY(%s)",
        (_wfids(demo),)).fetchall()]


def _trace(conn, link_id):
    return conn.execute(TRACE_SQL, {"link_id": link_id}).fetchall()


def _raw_paths(trace_rows):
    """The non-null raw_s3_path leaves of a trace (column index 5)."""
    return {row[5] for row in trace_rows if row[5] is not None}


# --------------------------------------------------------------------------- #
# (1) DLQ rows written on validation failure.
# --------------------------------------------------------------------------- #
def test_01_dlq_row_written_on_validation_failure(demo, conn):
    run_ids = _own_run_ids(conn, demo)
    n = conn.execute(
        "SELECT count(*) FROM cp.dlq WHERE run_id = ANY(%s::uuid[]) AND stage=%s",
        (run_ids, DLQ_STAGE)).fetchone()[0]
    assert n == 1, "exactly one row (CL900) should be quarantined on validation"
    dlq_id = demo["normal"]["claim_canonical"]["dlq_ids"][0]
    assert dlq_id is not None


# --------------------------------------------------------------------------- #
# (2) dlq row carries reason + failed_payload (diagnosable).
# --------------------------------------------------------------------------- #
def test_02_dlq_row_has_reason_and_failed_payload(demo, conn):
    dlq_id = demo["normal"]["claim_canonical"]["dlq_ids"][0]
    reason, failed_payload = conn.execute(
        "SELECT reason, failed_payload FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)).fetchone()
    assert "policy_id" in reason and "null" in reason
    # the EXACT rejected row is preserved verbatim (enough to diagnose/fix), and
    # is STILL preserved after the later replay/resolve flips status (history is
    # never overwritten — the open->resolved transition is asserted in test_09).
    assert failed_payload == BAD_CLAIM_ROW
    assert failed_payload["claim_id"] == "CL900"
    assert failed_payload["policy_id"] is None


# --------------------------------------------------------------------------- #
# (3) first-class quarantine output_link (edge_type='quarantine', rc=1).
# --------------------------------------------------------------------------- #
def test_03_quarantine_output_link_is_first_class(demo, conn):
    dlq_id = demo["normal"]["claim_canonical"]["dlq_ids"][0]
    qlink, qcount = conn.execute(
        "SELECT quarantine_output_link_id, record_count FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)).fetchone()
    assert qlink is not None
    assert qcount == 1
    edge_type, rc = conn.execute(
        "SELECT edge_type, record_count FROM cp.lineage_link "
        "WHERE lineage_link_id=%s", (qlink,)).fetchone()
    assert edge_type == "quarantine"
    assert rc == 1


# --------------------------------------------------------------------------- #
# (4) quarantine output visible in cp.v_provenance.
# --------------------------------------------------------------------------- #
def test_04_quarantine_output_visible_in_provenance(demo, conn):
    dlq_id = demo["normal"]["claim_canonical"]["dlq_ids"][0]
    qlink = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)).fetchone()[0]
    # v_provenance is a run-adjacency UNION ALL walk, so it may re-emit the same
    # quarantine edge once per downstream path that revisits the run; assert the
    # quarantine output IS a provenance node (>=1) carried by exactly ONE edge.
    seen, distinct_edges = conn.execute(
        "SELECT count(*), count(DISTINCT lineage_edge_id) FROM cp.v_provenance "
        "WHERE lineage_link_id=%s AND edge_type='quarantine'", (qlink,)
    ).fetchone()
    assert seen >= 1
    assert distinct_edges == 1


# --------------------------------------------------------------------------- #
# (5) good output AND quarantine output both trace to the raw claim file.
# --------------------------------------------------------------------------- #
def test_05_good_and_quarantine_trace_to_raw_claim_file(demo, conn):
    normal = demo["normal"]
    raw_claim_path = normal["files"]["claim"]["s3_raw_path"]
    raw_claim_file_id = normal["claim_ingest"]["file_id"]

    # GOOD canonical output traces to the raw claim file.
    good_link = normal["claim_canonical"]["link_id"]
    assert raw_claim_path in _raw_paths(_trace(conn, good_link))

    # The quarantine output + good output are SIBLING provenance nodes of the
    # canonicalization run, which itself consumes the raw claim file (the
    # quarantine link's edge terminates the walk by design, so we anchor on the
    # run's raw-derived sibling). Both edge types appear under the run.
    canon_run = normal["claim_canonical"]["run_id"]
    edge_types = {r[0] for r in conn.execute(
        "SELECT DISTINCT edge_type FROM cp.v_provenance WHERE consumer_run_id=%s",
        (canon_run,)).fetchall()}
    assert "quarantine" in edge_types
    assert "curated_to_canonical" in edge_types
    # The quarantine event's source_ref names the SAME raw file the good output
    # traces to (diagnostic anchor to origin).
    qlink = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE run_id=%s",
        (canon_run,)).fetchone()[0]
    qsrc = conn.execute(
        "SELECT source_ref FROM cp.lineage_edge WHERE lineage_link_id=%s",
        (qlink,)).fetchone()[0]
    assert qsrc["raw_file_id"] == raw_claim_file_id


# --------------------------------------------------------------------------- #
# (6) recon: input(4) == good(3) + dlq(1), status 'ok'.
# --------------------------------------------------------------------------- #
def test_06_recon_input_equals_good_plus_dlq(demo, conn):
    canon_run = demo["normal"]["claim_canonical"]["run_id"]
    source, accounted, status = conn.execute(
        "SELECT source_count, accounted_count, status FROM cp.reconciliation_log "
        "WHERE run_id=%s AND check_type='schema_validation'",
        (canon_run,)).fetchone()
    assert source == 4
    assert accounted == 4          # 3 good + 1 dlq
    assert status == "ok"
    # Cross-check against the actual good link record_count + the dlq record_count.
    good_rc = conn.execute(
        "SELECT record_count FROM cp.lineage_link WHERE lineage_link_id=%s",
        (demo["normal"]["claim_canonical"]["link_id"],)).fetchone()[0]
    dlq_rc = conn.execute(
        "SELECT coalesce(sum(record_count),0) FROM cp.dlq WHERE run_id=%s",
        (canon_run,)).fetchone()[0]
    assert good_rc + dlq_rc == 4


# --------------------------------------------------------------------------- #
# (7) DLQ replay CONSUMES the prior DLQ/quarantine identity.
# --------------------------------------------------------------------------- #
def test_07_replay_consumes_quarantine_identity(demo, conn):
    replay = demo["replay"]
    dlq_id = replay["dlq_id"]
    quarantine_link_id = replay["quarantine_link_id"]
    corrected_link_id = replay["corrected_link_id"]
    replay_run_id = replay["replay_run_id"]

    # The replay run is a replay-typed run whose replay_of_run_id is the ORIGINAL
    # canonicalization run.
    trigger, replay_of = conn.execute(
        "SELECT trigger_type, replay_of_run_id FROM cp.run_log WHERE run_id=%s",
        (replay_run_id,)).fetchone()
    assert trigger == "replay"
    assert str(replay_of) == str(demo["normal"]["claim_canonical"]["run_id"])

    # The corrected canonical output has a 'replay'-annotated input edge whose
    # upstream_output_link_id IS the quarantine output_link the quarantine event
    # created — i.e. the replay literally references the quarantine identity.
    replay_edges = conn.execute(
        "SELECT upstream_lineage_link_id, source_ref FROM cp.lineage_edge "
        "WHERE lineage_link_id=%s AND edge_type='replay'",
        (corrected_link_id,)).fetchall()
    assert len(replay_edges) == 1
    assert str(replay_edges[0][0]) == str(quarantine_link_id)
    assert replay_edges[0][1]["dlq_id"] == dlq_id

    # The quarantine link the replay consumed is exactly the one on the dlq row.
    on_dlq = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)).fetchone()[0]
    assert str(on_dlq) == str(quarantine_link_id)


# --------------------------------------------------------------------------- #
# (8) replayed/fixed row traces back to original raw + DLQ context.
# --------------------------------------------------------------------------- #
def test_08_replayed_row_traces_to_original_raw_and_dlq(demo, conn):
    normal = demo["normal"]
    replay = demo["replay"]
    raw_claim_path = normal["files"]["claim"]["s3_raw_path"]

    # The corrected canonical output traces back to the ORIGINAL raw claim file.
    corrected_link_id = replay["corrected_link_id"]
    assert raw_claim_path in _raw_paths(_trace(conn, corrected_link_id))

    # The corrected detail SINK row (now business-visible) ALSO traces to the
    # original raw claim file via the full merge -> canonical -> raw chain.
    detail_sink_link = replay["detail_sink"]["link_id"]
    assert raw_claim_path in _raw_paths(_trace(conn, detail_sink_link))

    # DLQ context is reachable from the corrected output: a 'replay' edge names
    # the quarantine output_link, which carries the dlq_id in its target_ref.
    qlink = replay["quarantine_link_id"]
    dlq_id_on_link = conn.execute(
        "SELECT target_ref->>'dlq_id' FROM cp.lineage_link WHERE lineage_link_id=%s",
        (qlink,)).fetchone()[0]
    assert dlq_id_on_link == replay["dlq_id"]


# --------------------------------------------------------------------------- #
# (9) bad row NOT business-visible before replay, corrected row IS after.
# --------------------------------------------------------------------------- #
def test_09_bad_row_visibility_before_and_after_replay(demo, conn):
    normal = demo["normal"]
    replay = demo["replay"]
    wfids = _wfids(demo)
    # The corrected row's detail business key (P002:CL900).
    bad_key = detail_business_key(
        {"policy_id": CORRECTED_CLAIM_ROW["policy_id"],
         "claim_id": CORRECTED_CLAIM_ROW["claim_id"]})

    # No active visibility row was EVER produced by the NORMAL execution for the
    # bad row's key (it never reached the sink). Scope to the normal wfid.
    normal_active_for_bad = conn.execute(
        "SELECT count(*) FROM ods.target_visibility "
        "WHERE dataset=%s AND replacement_key=%s AND status='Y' "
        "AND workflow_run_id=%s",
        (DETAIL_DATASET, bad_key, normal["workflow_run_id"])).fetchone()[0]
    assert normal_active_for_bad == 0, "bad row must not be visible pre-replay"

    # After replay, the corrected row IS active (status Y), produced by the
    # replay execution, and the dlq row is 'resolved'.
    active = conn.execute(
        "SELECT status, workflow_run_id FROM ods.target_visibility "
        "WHERE dataset=%s AND replacement_key=%s AND status='Y' "
        "AND workflow_run_id = ANY(%s)",
        (DETAIL_DATASET, bad_key, wfids)).fetchall()
    assert len(active) == 1
    assert active[0][0] == "Y"
    assert active[0][1] == replay["workflow_run_id"]

    status, by_run, by_link = conn.execute(
        "SELECT status, resolved_by_run_id, resolved_by_output_link_id "
        "FROM cp.dlq WHERE dlq_id=%s", (replay["dlq_id"],)).fetchone()
    assert status == "resolved"
    assert str(by_run) == str(replay["replay_run_id"])
    assert str(by_link) == str(replay["corrected_link_id"])


# --------------------------------------------------------------------------- #
# Schema-validation tests (area 3).
# --------------------------------------------------------------------------- #
def test_10_missing_required_column_goes_to_dlq(conn):
    contract = schema.get_contract(
        conn, domain=DOMAIN, dataset=CONTRACT_DATASET, layer=CONTRACT_LAYER,
        schema_version=SCHEMA_VERSION)
    # claim_amount entirely ABSENT (not just null) -> required column missing.
    rows = [{"claim_id": "CLX", "policy_id": "P001", "claim_date": "2026-05-20",
             "claim_status": "open"}]
    good, bad = schema.validate_rows(rows, contract)
    assert good == []
    assert len(bad) == 1
    assert "claim_amount" in bad[0][1] and "missing" in bad[0][1]


def test_11_nullable_violation_goes_to_dlq(conn):
    contract = schema.get_contract(
        conn, domain=DOMAIN, dataset=CONTRACT_DATASET, layer=CONTRACT_LAYER,
        schema_version=SCHEMA_VERSION)
    # policy_id present-but-NULL and NOT in nullable_columns -> bad (the CL900 case).
    good, bad = schema.validate_rows([dict(BAD_CLAIM_ROW)], contract)
    assert good == []
    assert len(bad) == 1
    assert "policy_id" in bad[0][1] and "null" in bad[0][1]


def test_12_valid_rows_pass(conn):
    contract = schema.get_contract(
        conn, domain=DOMAIN, dataset=CONTRACT_DATASET, layer=CONTRACT_LAYER,
        schema_version=SCHEMA_VERSION)
    good, bad = schema.validate_rows([dict(r) for r in GOOD_CLAIM_ROWS], contract)
    assert len(good) == 3
    assert bad == []


def test_13_good_output_records_schema_version_in_target_ref(demo, conn):
    good_link = demo["normal"]["claim_canonical"]["link_id"]
    target_ref = conn.execute(
        "SELECT target_ref FROM cp.lineage_link WHERE lineage_link_id=%s",
        (good_link,)).fetchone()[0]
    assert target_ref["schema_version"] == SCHEMA_VERSION


def test_14_validation_metrics_visible_in_run_stage_log(demo, conn):
    canon_run = demo["normal"]["claim_canonical"]["run_id"]
    metrics = conn.execute(
        "SELECT metrics FROM cp.run_stage_log WHERE run_id=%s AND stage=%s",
        (canon_run, DLQ_STAGE)).fetchone()[0]
    assert metrics["schema_version"] == SCHEMA_VERSION
    assert metrics["rows_in"] == 4
    assert metrics["good"] == 3
    assert metrics["quarantined"] == 1
    assert any("policy_id" in r for r in metrics["reasons"])


# --------------------------------------------------------------------------- #
# Isolation: the DLQ workflow's reset/run must not touch the other demo's data.
# --------------------------------------------------------------------------- #
def test_15_distinct_domain_isolates_from_main_demo(demo, conn):
    # Every run this demo produced is tagged with the DISTINCT 'insurance_dlq'
    # domain (not the existing demo's 'insurance'), so neither demo's
    # domain-scoped reset can touch or FK-violate the other's data.
    domains = {r[0] for r in conn.execute(
        "SELECT DISTINCT domain FROM cp.run_log WHERE workflow_run_id = ANY(%s)",
        (_wfids(demo),)).fetchall()}
    assert domains == {DOMAIN}
    assert DOMAIN == "insurance_dlq"
    # None of this demo's runs use the un-suffixed datasets the main demo owns.
    bad = conn.execute(
        "SELECT count(*) FROM cp.run_log "
        "WHERE workflow_run_id = ANY(%s) "
        "AND dataset IN ('policy','claim','policy_claim','policy_claim_daily')",
        (_wfids(demo),)).fetchone()[0]
    assert bad == 0
    # And the claim row count actually consumed was 4.
    assert len(CLAIM_ROWS) == 4
