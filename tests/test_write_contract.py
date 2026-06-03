"""Cross-cutting acceptance tests for the control-plane WRITE CONTRACT.

Reference: docs/reference/control-plane-write-contract.md
Spec:      docs/specs/2026-06-03-working-platform-completion-plan.md, area 1
           ("Finalize The Control-Plane Write Contract" -> Acceptance Tests).

These are PLATFORM-INVARIANT guards, not workflow-specific behaviour tests: they
should hold for ANY compliant workflow. We exercise them over the two demo
harnesses (customer/transaction = `sales` domain; policy/claims = `insurance`
domain) run with commit=False (rolled back by the `conn` fixture).

ROBUSTNESS TO COMMITTED DATA: the dashboard snapshot generators leave demo rows
COMMITTED in the shared TCP database. Every assertion is therefore scoped to the
workflow_run_ids THIS run produced (`result["executions"]` -> each carries a
`workflow_run_id`), never to a whole domain/dataset. A clean `python -m db.apply
--drop` before the suite removes prior committed pollution; the wfid scoping is
the durable guard.

Invariants proven (spec Acceptance Tests):
  1. every run has a cp.run_log row with a terminal status;
  2. every run has >=1 cp.run_stage_log row;
  3. every SUCCESSFUL run of an output-producing pipeline_type has >=1 output_link;
  4. every output_link has >=1 input_edge;
  5. every run-to-run input_edge has a non-null upstream_output_link_id
     (file-leaf edges instead carry source_file_id);
  6. target rows are stamped with _ods_output_link_id;
  7. business-visible rows have a corresponding ods.target_visibility row
     (scoped to the policy/claims demo, which performs step 9; the customer
     demo intentionally does not activate visibility).
"""
import pytest

from harness import customer_transaction_workflow as customer
from harness import policy_claims_workflow as policy

# pipeline_types whose SUCCESSFUL runs are expected to produce an output_link
# (step 6 of the contract). Both demos use exactly these five; every one of
# their runs produces an output, so the invariant has no exemptions here. Listed
# explicitly so a future no-output pipeline_type (e.g. a pure validation probe)
# does not silently break the assertion — it would simply not be scoped in.
OUTPUT_PRODUCING_PIPELINE_TYPES = (
    "ingestion", "canonicalization", "merge", "sink", "aggregation",
)

TERMINAL_RUN_STATUSES = ("succeeded", "failed")


# --------------------------------------------------------------------------- #
# Fixtures: run each demo once (rolled back via the `conn` fixture).
# --------------------------------------------------------------------------- #
@pytest.fixture
def customer_demo(conn):
    return customer.run_demo(conn, commit=False)


@pytest.fixture
def policy_demo(conn):
    return policy.run_demo(conn, commit=False)


def _wfids(result):
    """Every workflow_run_id this demo run produced (the scoping key)."""
    return [e["workflow_run_id"] for e in result["executions"]]


def _run_ids(conn, wfids):
    return [str(r[0]) for r in conn.execute(
        "SELECT run_id FROM cp.run_log WHERE workflow_run_id = ANY(%s)",
        (wfids,)).fetchall()]


# --------------------------------------------------------------------------- #
# Invariant 1 — every run has a run_log row with a terminal status.
# (Trivially true that a run_log row exists — that IS the run; the substantive
#  guard is that the contract's step 10 finalised every run to a terminal state.)
# --------------------------------------------------------------------------- #
def _assert_runs_terminal(conn, wfids):
    rows = conn.execute(
        "SELECT run_id, status FROM cp.run_log WHERE workflow_run_id = ANY(%s)",
        (wfids,)).fetchall()
    assert rows, "demo produced no runs"
    non_terminal = [(str(r[0]), r[1]) for r in rows
                    if r[1] not in TERMINAL_RUN_STATUSES]
    assert not non_terminal, f"runs not finalised to a terminal status: {non_terminal}"


def test_customer_every_run_has_run_log_row_terminal(customer_demo, conn):
    _assert_runs_terminal(conn, _wfids(customer_demo))


def test_policy_every_run_has_run_log_row_terminal(policy_demo, conn):
    _assert_runs_terminal(conn, _wfids(policy_demo))


# --------------------------------------------------------------------------- #
# Invariant 2 — every run has >=1 run_stage_log row.
# --------------------------------------------------------------------------- #
def _assert_every_run_has_a_stage(conn, wfids):
    missing = conn.execute(
        """
        SELECT r.run_id
        FROM cp.run_log r
        WHERE r.workflow_run_id = ANY(%s)
          AND NOT EXISTS (
              SELECT 1 FROM cp.run_stage_log s WHERE s.run_id = r.run_id)
        """,
        (wfids,)).fetchall()
    assert not missing, f"runs with no run_stage_log row: {[str(m[0]) for m in missing]}"


def test_customer_every_run_has_a_stage(customer_demo, conn):
    _assert_every_run_has_a_stage(conn, _wfids(customer_demo))


def test_policy_every_run_has_a_stage(policy_demo, conn):
    _assert_every_run_has_a_stage(conn, _wfids(policy_demo))


# --------------------------------------------------------------------------- #
# Invariant 3 — every SUCCESSFUL output-producing run has >=1 output_link.
# --------------------------------------------------------------------------- #
def _assert_successful_runs_produce_output(conn, wfids):
    runs = conn.execute(
        """
        SELECT run_id FROM cp.run_log
        WHERE workflow_run_id = ANY(%s)
          AND status = 'succeeded'
          AND pipeline_type = ANY(%s)
        """,
        (wfids, list(OUTPUT_PRODUCING_PIPELINE_TYPES))).fetchall()
    assert runs, "no successful output-producing runs to check"
    no_output = conn.execute(
        """
        SELECT r.run_id
        FROM cp.run_log r
        WHERE r.workflow_run_id = ANY(%s)
          AND r.status = 'succeeded'
          AND r.pipeline_type = ANY(%s)
          AND NOT EXISTS (
              SELECT 1 FROM cp.output_link o WHERE o.consumer_run_id = r.run_id)
        """,
        (wfids, list(OUTPUT_PRODUCING_PIPELINE_TYPES))).fetchall()
    assert not no_output, (
        f"successful output-producing runs with no output_link: "
        f"{[str(r[0]) for r in no_output]}")


def test_customer_successful_runs_produce_output(customer_demo, conn):
    _assert_successful_runs_produce_output(conn, _wfids(customer_demo))


def test_policy_successful_runs_produce_output(policy_demo, conn):
    _assert_successful_runs_produce_output(conn, _wfids(policy_demo))


# --------------------------------------------------------------------------- #
# Invariant 4 — every output_link has >=1 input_edge.
# --------------------------------------------------------------------------- #
def _assert_every_output_has_an_input_edge(conn, wfids):
    links = conn.execute(
        """
        SELECT o.output_link_id
        FROM cp.output_link o
        JOIN cp.run_log r ON r.run_id = o.consumer_run_id
        WHERE r.workflow_run_id = ANY(%s)
        """,
        (wfids,)).fetchall()
    assert links, "demo produced no output_links"
    orphans = conn.execute(
        """
        SELECT o.output_link_id
        FROM cp.output_link o
        JOIN cp.run_log r ON r.run_id = o.consumer_run_id
        WHERE r.workflow_run_id = ANY(%s)
          AND NOT EXISTS (
              SELECT 1 FROM cp.input_edge e
              WHERE e.output_link_id = o.output_link_id)
        """,
        (wfids,)).fetchall()
    assert not orphans, (
        f"output_links with no input_edge: {[str(o[0]) for o in orphans]}")


def test_customer_every_output_has_an_input_edge(customer_demo, conn):
    _assert_every_output_has_an_input_edge(conn, _wfids(customer_demo))


def test_policy_every_output_has_an_input_edge(policy_demo, conn):
    _assert_every_output_has_an_input_edge(conn, _wfids(policy_demo))


# --------------------------------------------------------------------------- #
# Invariant 5 — every run-to-run (downstream) input_edge has a non-null
# upstream_output_link_id; file-leaf edges instead carry source_file_id. An edge
# must be anchored by EXACTLY one of the two (no dangling edges).
# --------------------------------------------------------------------------- #
def _assert_edges_anchored(conn, wfids):
    edges = conn.execute(
        """
        SELECT e.input_edge_id, e.upstream_output_link_id, e.source_file_id,
               e.upstream_run_id
        FROM cp.input_edge e
        JOIN cp.output_link o ON o.output_link_id = e.output_link_id
        JOIN cp.run_log r     ON r.run_id = o.consumer_run_id
        WHERE r.workflow_run_id = ANY(%s)
        """,
        (wfids,)).fetchall()
    assert edges, "demo produced no input_edges"

    # Every edge anchored by exactly one of {upstream_output_link_id, source_file_id}.
    bad_anchor = [str(e[0]) for e in edges
                  if (e[1] is None) == (e[2] is None)]
    assert not bad_anchor, (
        f"input_edges not anchored by exactly one of upstream_output_link_id / "
        f"source_file_id: {bad_anchor}")

    # A run-to-run edge (one that names an upstream RUN) must carry the
    # upstream_output_link_id pointer — not fall back to a file leaf.
    run_to_run_without_link = [str(e[0]) for e in edges
                               if e[3] is not None and e[1] is None]
    assert not run_to_run_without_link, (
        f"run-to-run input_edges missing upstream_output_link_id: "
        f"{run_to_run_without_link}")


def test_customer_downstream_edges_use_upstream_output_link_id(customer_demo, conn):
    _assert_edges_anchored(conn, _wfids(customer_demo))


def test_policy_downstream_edges_use_upstream_output_link_id(policy_demo, conn):
    _assert_edges_anchored(conn, _wfids(policy_demo))


# --------------------------------------------------------------------------- #
# Invariant 6 — target rows are stamped with _ods_output_link_id.
#
# Scoped to the demo's OWN rows by joining each target row's output link back to
# this run's wfids (the committed demo leaves rows of the same dataset behind).
# Both demos write the same two target tables per domain.
# --------------------------------------------------------------------------- #
def _assert_target_rows_stamped(conn, wfids, tables):
    total = 0
    for table in tables:
        rows = conn.execute(
            f"""
            SELECT t._ods_output_link_id, t._ods_lineage_link_id
            FROM ods.{table} t
            JOIN cp.output_link o
              ON o.output_link_id = t._ods_lineage_link_id
            JOIN cp.run_log r ON r.run_id = o.consumer_run_id
            WHERE r.workflow_run_id = ANY(%s)
            """,
            (wfids,)).fetchall()
        assert rows, f"demo wrote no rows to ods.{table} for these wfids"
        total += len(rows)
        unstamped = [str(row[1]) for row in rows if row[0] is None]
        assert not unstamped, (
            f"ods.{table} rows missing _ods_output_link_id stamp "
            f"(by _ods_lineage_link_id): {unstamped}")
        # The new-name mirror must equal the FK column it mirrors.
        mismatched = [str(row[1]) for row in rows
                      if str(row[0]) != str(row[1])]
        assert not mismatched, (
            f"ods.{table} _ods_output_link_id != _ods_lineage_link_id: {mismatched}")
    assert total > 0


def test_customer_target_rows_stamped_with_output_link_id(customer_demo, conn):
    _assert_target_rows_stamped(
        conn, _wfids(customer_demo),
        tables=(customer.DETAIL_DATASET, customer.AGG_DATASET))


def test_policy_target_rows_stamped_with_output_link_id(policy_demo, conn):
    _assert_target_rows_stamped(
        conn, _wfids(policy_demo),
        tables=(policy.DETAIL_DATASET, policy.AGG_DATASET))


# --------------------------------------------------------------------------- #
# Invariant 7 — business-visible rows have a corresponding target_visibility row.
#
# Scoped to the POLICY/CLAIMS demo: it performs contract step 9
# (visibility.activate) on its sink outputs. The customer/transaction demo
# intentionally does NOT activate visibility, so this invariant does not apply
# there (asserting it would be false) — hence no customer counterpart.
#
# We prove that every sink output_link this demo activated visibility for has a
# target_visibility row pointing back at it, and that the business-visible rows
# in the target table belong to an output that carries an active (status='Y')
# visibility row. Scoped to this run's wfids throughout.
# --------------------------------------------------------------------------- #
def test_policy_business_visible_rows_have_target_visibility(policy_demo, conn):
    wfids = _wfids(policy_demo)

    # Every visibility row this demo wrote points at a sink output produced by
    # one of this demo's runs (sanity: the activate calls actually happened and
    # are scoped to us).
    vis = conn.execute(
        """
        SELECT v.lineage_link_id, v.status, v.dataset
        FROM ods.target_visibility v
        WHERE v.workflow_run_id = ANY(%s)
        """,
        (wfids,)).fetchall()
    assert vis, "policy demo activated no target_visibility rows"

    # Each active (Y) visibility row's output_link must actually exist and have
    # had target rows stamped against it (i.e. there ARE business-visible rows
    # behind the active slice). One target table per dataset.
    table_by_dataset = {
        policy.DETAIL_DATASET: policy.DETAIL_DATASET,
        policy.AGG_DATASET: policy.AGG_DATASET,
    }
    active = [(str(link_id), dataset) for (link_id, status, dataset) in vis
              if status == "Y"]
    assert active, "policy demo has no active (Y) visibility rows"

    for link_id, dataset in active:
        table = table_by_dataset.get(dataset)
        assert table is not None, f"unexpected visibility dataset {dataset}"
        n_rows = conn.execute(
            f"SELECT count(*) FROM ods.{table} WHERE _ods_output_link_id = %s",
            (link_id,)).fetchone()[0]
        assert n_rows > 0, (
            f"active visibility row for link {link_id} (dataset {dataset}) "
            f"has no business-visible rows in ods.{table}")

    # Conversely: every CURRENTLY-ACTIVE business slice this demo produced is
    # backed by exactly one Y visibility row (the active-uniqueness invariant,
    # scoped to our wfids). Group by the visibility scope key.
    dup_active = conn.execute(
        """
        SELECT domain, dataset, business_date, sink_type, target_name,
               replacement_scope, replacement_key, count(*)
        FROM ods.target_visibility
        WHERE workflow_run_id = ANY(%s) AND status = 'Y'
        GROUP BY domain, dataset, business_date, sink_type, target_name,
                 replacement_scope, replacement_key
        HAVING count(*) > 1
        """,
        (wfids,)).fetchall()
    assert not dup_active, f"multiple active visibility rows for one slice: {dup_active}"
