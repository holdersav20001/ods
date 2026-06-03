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
    aggregate_business_key,
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
# Cross-hop fact-spine recon (F6, migration 031): the validating NORMAL run
# records a workflow recon row that reconciles ok on the FACT SPINE, and it is
# the headline DLQ guarantee at workflow grain: fact 'claim_dlq' raw 4 == leaf
# 'policy_claim_dlq' detail 3 + dlq_unresolved 1 (the 4 = 3 good + 1 quarantined).
# raw_in excludes the policy DIMENSION; sink_out excludes the daily aggregate.
# The REPLAY execution reconciles at changed-slice grain (per-output
# reconcile_sink_link) and records NO whole-fact workflow recon row.
# --------------------------------------------------------------------------- #
def test_09_workflow_recon_fact_spine_ok(demo, conn):
    normal = demo["normal"]
    rows = conn.execute(
        """
        SELECT rl.status, rl.metrics FROM cp.reconciliation_log rl
        JOIN cp.run_log r ON r.run_id = rl.run_id
        WHERE r.workflow_run_id = %s AND rl.check_type = 'workflow'
        """,
        (normal["workflow_run_id"],),
    ).fetchall()
    assert len(rows) == 1, "normal DLQ run must record one workflow recon row"
    status, metrics = rows[0]
    assert status == "ok", f"DLQ normal workflow recon {status}, {metrics}"
    # The headline 4 = 3 good + 1 quarantined.
    assert metrics["raw_in"] == 4
    assert metrics["sink_out"] == 3
    assert metrics["dlq_out"] == 1
    assert metrics["source_datasets"] == ["claim_dlq"]
    assert metrics["leaf_target"] == DETAIL_DATASET
    assert metrics["aggregates_excluded"] is True
    assert metrics["workflow_run_id"] == normal["workflow_run_id"]

    # The replay reconciles at changed-slice grain — no whole-fact workflow row.
    n = conn.execute(
        """
        SELECT count(*) FROM cp.reconciliation_log rl
        JOIN cp.run_log r ON r.run_id = rl.run_id
        WHERE r.workflow_run_id = %s AND rl.check_type = 'workflow'
        """,
        (demo["replay"]["workflow_run_id"],),
    ).fetchone()[0]
    assert n == 0, "DLQ replay must not record a whole-fact workflow recon"


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


def test_12a_validation_rules_reject_bad_type_enum_range_and_date(conn):
    contract = schema.get_contract(
        conn, domain=DOMAIN, dataset=CONTRACT_DATASET, layer=CONTRACT_LAYER,
        schema_version=SCHEMA_VERSION)
    rows = [
        {**GOOD_CLAIM_ROWS[0], "claim_id": "BADTYPE", "claim_amount": "500.00"},
        {**GOOD_CLAIM_ROWS[0], "claim_id": "BADENUM", "claim_status": "pending"},
        {**GOOD_CLAIM_ROWS[0], "claim_id": "BADMIN", "claim_amount": -1},
        {**GOOD_CLAIM_ROWS[0], "claim_id": "BADDATE", "claim_date": "2026/05/20"},
    ]
    good, bad = schema.validate_rows(rows, contract)
    assert good == []
    reasons = [reason for _row, reason in bad]
    assert any("claim_amount" in r and "number" in r for r in reasons)
    assert any("claim_status" in r and "one of" in r for r in reasons)
    assert any("claim_amount" in r and ">=" in r for r in reasons)
    assert any("claim_date" in r and "date" in r for r in reasons)


def test_12b_duplicate_business_key_is_rejected(conn):
    contract = schema.get_contract(
        conn, domain=DOMAIN, dataset=CONTRACT_DATASET, layer=CONTRACT_LAYER,
        schema_version=SCHEMA_VERSION)
    dup1 = dict(GOOD_CLAIM_ROWS[0])
    dup2 = {**GOOD_CLAIM_ROWS[0], "claim_status": "closed"}
    good, bad = schema.validate_rows([dup1, dup2], contract)
    assert good == []
    assert len(bad) == 2
    assert all("duplicate business key" in reason for _row, reason in bad)


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


# --------------------------------------------------------------------------- #
# (P1b) After replay, the affected aggregate key reflects the corrected counts
# AND is active; the stale aggregate for that key is superseded; unchanged
# aggregate keys stay active.
# --------------------------------------------------------------------------- #
def _agg_visibility_rows(conn, *, replacement_key, wfids):
    """All target_visibility rows for an aggregate business key, scoped to this
    demo's wfids (robust to committed data)."""
    return conn.execute(
        "SELECT status, workflow_run_id, lineage_link_id FROM ods.target_visibility "
        "WHERE dataset=%s AND replacement_key=%s AND workflow_run_id = ANY(%s) "
        "ORDER BY activated_at",
        (AGG_DATASET, replacement_key, wfids)).fetchall()


def test_16_replay_recomputes_affected_aggregate(demo, conn):
    """The replay corrected CL900 (P002 -> 'auto', 300). The 'auto' aggregate for
    this business_date must be recomputed to claim_count=3 / total=1050 (the 2
    originally-good auto claims CL500+CL501 plus the corrected CL900), sunk into
    ods.policy_claim_daily_dlq, and ACTIVE (Y) — produced by the replay wfid."""
    wfids = _wfids(demo)
    auto_key = aggregate_business_key(
        {"business_date": str(BUSINESS_DATE), "policy_type": "auto"})

    # The recomputed aggregate is materialised in the target by the replay.
    replay_agg_sink = demo["replay"]["aggregate_replay"]["aggregate_sink"]
    assert replay_agg_sink is not None, "replay must recompute the aggregate"
    payload = conn.execute(
        "SELECT payload FROM ods.policy_claim_daily_dlq "
        "WHERE _ods_output_link_id = %s",
        (replay_agg_sink["link_id"],)).fetchone()[0]
    assert payload["policy_type"] == "auto"
    assert payload["claim_count"] == 3, "auto must include CL500+CL501+CL900"
    assert float(payload["total_claim_amount"]) == 1050.0

    # The recomputed 'auto' aggregate is the ACTIVE (Y) visibility row, produced
    # by the replay execution.
    active = [r for r in _agg_visibility_rows(conn, replacement_key=auto_key,
                                              wfids=wfids) if r[0] == "Y"]
    assert len(active) == 1, "exactly one active aggregate row for 'auto'"
    assert active[0][1] == demo["replay"]["workflow_run_id"]
    assert str(active[0][2]) == str(replay_agg_sink["link_id"])


def test_17_stale_aggregate_for_affected_key_is_superseded(demo, conn):
    """The NORMAL 'auto' aggregate (count=2 / total=750) was active before replay;
    after replay it must be SUPERSEDED (status N) by the recomputed one."""
    wfids = _wfids(demo)
    auto_key = aggregate_business_key(
        {"business_date": str(BUSINESS_DATE), "policy_type": "auto"})
    rows = _agg_visibility_rows(conn, replacement_key=auto_key, wfids=wfids)
    # The normal aggregate row (produced by the normal wfid) is now N.
    normal_rows = [r for r in rows
                   if r[1] == demo["normal"]["workflow_run_id"]]
    assert normal_rows, "the normal aggregate row for 'auto' should exist"
    assert all(r[0] == "N" for r in normal_rows), (
        "the stale normal 'auto' aggregate must be superseded to N")
    # And the stale normal aggregate row's payload was the pre-replay count=2/750.
    normal_link = normal_rows[0][2]
    stale_payload = conn.execute(
        "SELECT payload FROM ods.policy_claim_daily_dlq "
        "WHERE _ods_output_link_id = %s AND payload->>'policy_type' = 'auto'",
        (normal_link,)).fetchone()
    if stale_payload is not None:
        assert stale_payload[0]["claim_count"] == 2
        assert float(stale_payload[0]["total_claim_amount"]) == 750.0


def test_18_unchanged_aggregate_key_stays_active(demo, conn):
    """The 'home' aggregate (CL502, unchanged by the replay) must stay ACTIVE (Y),
    produced by the NORMAL execution — changed-only recompute does not touch it."""
    wfids = _wfids(demo)
    home_key = aggregate_business_key(
        {"business_date": str(BUSINESS_DATE), "policy_type": "home"})
    active = [r for r in _agg_visibility_rows(conn, replacement_key=home_key,
                                              wfids=wfids) if r[0] == "Y"]
    assert len(active) == 1, "the unchanged 'home' aggregate must stay active"
    assert active[0][1] == demo["normal"]["workflow_run_id"], (
        "'home' must still be the NORMAL execution's aggregate (not recomputed)")
    # The replay must NOT have produced a 'home' aggregate output.
    replay_meta = demo["replay"]["aggregate_replay"]
    assert "home" not in replay_meta["affected_policy_types"]


def test_19_recomputed_aggregate_traces_to_all_contributing_details(demo, conn):
    """COMPLETE provenance: the recomputed 'auto' aggregate (count=3) drew from BOTH
    the ORIGINAL normal detail output (CL500+CL501) and the CORRECTED replay detail
    output (CL900). Its detail_to_aggregate input edges must name BOTH detail sink
    outputs (per-upstream contributing counts 2 + 1 = 3), so tracing the aggregate's
    provenance is complete — not just the corrected slice."""
    agg_link = demo["replay"]["aggregate_replay"]["aggregate"]["link_id"]
    normal_detail_link = str(demo["normal"]["detail_sink"]["link_id"])
    replay_detail_link = str(demo["replay"]["detail_sink"]["link_id"])

    edges = conn.execute(
        "SELECT upstream_output_link_id::text, record_count FROM cp.input_edge "
        "WHERE output_link_id = %s AND edge_type = 'detail_to_aggregate' "
        "ORDER BY input_slot",
        (agg_link,)).fetchall()
    upstreams = {e[0] for e in edges}
    assert normal_detail_link in upstreams, (
        "recomputed aggregate must name the ORIGINAL normal detail output "
        "(it contributed CL500+CL501)")
    assert replay_detail_link in upstreams, (
        "recomputed aggregate must name the CORRECTED replay detail output (CL900)")
    counts = {e[0]: e[1] for e in edges}
    assert counts[normal_detail_link] == 2   # original auto rows
    assert counts[replay_detail_link] == 1   # corrected auto row


# --------------------------------------------------------------------------- #
# (F2 end-to-end) The quarantine OUTPUT now traces to the raw claim file.
#
# FIX-A added cp.quarantine(..., p_source_file_id) which stamps source_file_id on
# the quarantine edge; the harness now passes source_file_id=<raw claim file_id>.
# So the quarantine output_link is no longer a dead-end in cp.v_provenance /
# trace_row.sql — it anchors to the raw claim file (completes F2 in the demo).
# --------------------------------------------------------------------------- #
def test_20_quarantine_output_traces_to_raw(demo, conn):
    normal = demo["normal"]
    raw_claim_path = normal["files"]["claim"]["s3_raw_path"]
    raw_claim_file_id = normal["claim_ingest"]["file_id"]
    dlq_id = normal["claim_canonical"]["dlq_ids"][0]
    qlink = conn.execute(
        "SELECT quarantine_output_link_id FROM cp.dlq WHERE dlq_id=%s",
        (dlq_id,)).fetchone()[0]

    # The quarantine edge now carries the raw claim file_id as source_file_id (the
    # lineage anchor), not merely in source_ref JSON.
    edge_src_file_id = conn.execute(
        "SELECT source_file_id FROM cp.lineage_edge WHERE lineage_link_id=%s "
        "AND edge_type='quarantine'", (qlink,)).fetchone()[0]
    assert str(edge_src_file_id) == str(raw_claim_file_id), (
        "quarantine edge must anchor to the raw claim file_id (FIX-A handoff)")

    # And the quarantine output traces to the raw claim file via trace_row.sql.
    assert raw_claim_path in _raw_paths(_trace(conn, qlink)), (
        "quarantine output must now trace to the raw claim file (F2 end-to-end)")
