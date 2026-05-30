"""R3 — completeness + DB-as-lineage-authority adversarial probes.

Mission (completeness-attacker): for ANY row in ANY state, can we ALWAYS
reconstruct the full chain to raw via cp.v_provenance / trace_row.sql — or find
a row that dead-ends, over-claims, or is unreachable? AND: is the DB genuinely
the lineage authority, or is it bypassable beneath the sanctioned client?

Two kinds of probe:
  * CONFIRMED_*  — proves a HOLE: the system permits a state it should forbid,
    or fails to detect a loss it should. These document the attack landing.
  * SOUND_*      — proves an invariant the system DOES uphold (regression guard
    for the sanctioned happy paths), so the report separates real holes from
    behaviour that is already correct.

Isolation: the rollback `conn` fixture (autocommit off; rolled back at teardown).
NOTHING is committed; the schema is NEVER dropped or mutated. Namespace p9r3_*.

Headline findings (see docs/reviews/2026-05-30-team-r3-completeness.md):
  P2  [CLOSED in P10-B / migration 014] the edge_type-vs-link_type smuggling
      guard formerly lived ONLY in cp.write_lineage_link, so a DIRECT INSERT into
      cp.lineage_edge bypassed it. 014 adds a BEFORE INSERT trigger
      (edge_type_matches_link) that enforces the match at the table, making the DB
      authoritative for edge_type even under direct insert. The P2 probes below
      are FLIPPED to assert the forge/smuggle now RAISE. (Privilege-revoke — which
      would make the DB authoritative for ALL direct mutation — remains deployment
      guidance in 014 and tests/README.md, not applied in this owner-run test DB.)
  A lineage_link with ZERO edges, and an ods.orders row under a 0-edge sink
      link, are both acceptable by direct insert and DEAD-END (trace to no raw).
  A4-S5 cross-hop loss: when a whole upstream fails downstream, its rows are
      lost end-to-end with NO sink breach and NO automated cross-hop check —
      the gap is computable from lineage sums but nothing computes it.
"""
import pathlib
import uuid

import psycopg
import pytest
from psycopg.types.json import Jsonb

from harness import composers

BD = "2026-05-30"
TRACE_SQL = (pathlib.Path(__file__).resolve().parents[1]
             / "control" / "queries" / "trace_row.sql").read_text()


# --------------------------------------------------------------------------- #
# helpers — all write through the rollback conn; nothing is committed.
# --------------------------------------------------------------------------- #
def _run(conn, pipeline_type, *, status="running"):
    return conn.execute(
        "INSERT INTO cp.run_log (workflow_run_id,pipeline_type,domain,dataset,"
        "business_date,trigger_type,status) "
        "VALUES (%s,%s,'sales','orders',%s,'manual',%s) RETURNING run_id",
        (str(uuid.uuid4()), pipeline_type, BD, status),
    ).fetchone()[0]


def _file(conn):
    return conn.execute(
        "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
        (f"s3://raw/{uuid.uuid4()}.csv", uuid.uuid4().hex, BD),
    ).fetchone()[0]


def _tref(tag):
    return {"path": f"s3://x/{tag}", "content_hash": f"h-{tag}", "version": 1}


def _raw_to_curated(conn, run_id, file_id, n=1, tag=None):
    tag = tag or uuid.uuid4().hex
    return conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s,NULL,NULL)",
        (run_id, Jsonb(_tref(tag)), n,
         Jsonb([{"edge_type": "raw_to_curated", "source_file_id": str(file_id),
                 "source_ref": {}, "record_count": n}])),
    ).fetchone()[0]


def _trace(conn, link_id):
    return conn.execute(TRACE_SQL, {"link_id": str(link_id)}).fetchall()


def _raw_paths(rows):
    # trace_row.sql returns raw_s3_path as the LAST column; NULL except raw leaf.
    return [r[-1] for r in rows if r[-1] is not None]


# =========================================================================== #
# CONFIRMED P2 — DB lineage authority is BYPASSABLE (Codex P2).
#   The smuggling guard is only in cp.write_lineage_link. A direct INSERT into
#   cp.lineage_edge adds a raw_to_curated edge under a curated_to_canonical link
#   (passing edge_must_anchor via source_file_id, and raw_edge_requires_source_
#   file by naming a file) — the function guard never runs.
# =========================================================================== #
def test_FIXED_direct_insert_smuggle_rejected_by_trigger(conn):
    """P10-B (014): the edge_type-vs-link smuggling guard is now ALSO at the table
    level (BEFORE INSERT trigger edge_type_matches_link), so the direct-INSERT
    bypass is closed. Forging a raw_to_curated edge under a curated_to_canonical
    link now RAISES whether it goes through the function OR a direct INSERT."""
    canon_run = _run(conn, "sink")
    up_run = _run(conn, "ingestion", status="succeeded")
    file_id = _file(conn)
    up_link = _raw_to_curated(conn, up_run, file_id)
    canon = conn.execute(
        "SELECT cp.write_lineage_link(%s,'curated_to_canonical',%s,1,%s,NULL,NULL)",
        (canon_run, Jsonb(_tref("canon")), Jsonb(
            [{"edge_type": "curated_to_canonical", "upstream_run_id": str(up_run),
              "upstream_lineage_link_id": str(up_link), "source_ref": {},
              "record_count": 1}])),
    ).fetchone()[0]

    # The smuggle through the sanctioned function RAISES (function guard).
    with pytest.raises(psycopg.errors.RaiseException):
        with conn.transaction():
            conn.execute(
                "SELECT cp.write_lineage_link(%s,'curated_to_canonical',%s,1,%s,NULL,NULL)",
                (canon_run, Jsonb(_tref("smug")), Jsonb(
                    [{"edge_type": "raw_to_curated", "source_file_id": str(file_id),
                      "source_ref": {}, "record_count": 1}])))

    # The DIRECT INSERT of the very same mismatched edge now ALSO RAISES — the
    # table trigger fires regardless of caller (the hole is closed).
    with pytest.raises(psycopg.errors.RaiseException):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.lineage_edge "
                "(lineage_link_id, edge_type, source_file_id, source_ref, record_count) "
                "VALUES (%s,'raw_to_curated',%s,%s,1)",
                (canon, str(file_id), Jsonb({"smuggled": True})))

    # Nothing smuggled landed: no mismatched edge is visible under the canonical link.
    seen = conn.execute(
        "SELECT count(*) FROM cp.v_provenance "
        "WHERE lineage_link_id=%s AND edge_type='raw_to_curated'", (canon,),
    ).fetchone()[0]
    assert seen == 0, "a smuggled edge is live in v_provenance — trigger failed"


def test_FIXED_trigger_now_guards_lineage_edge(conn):
    """P10-B (014): the DB IS now authoritative for edge_type by structure — a
    BEFORE INSERT trigger guards cp.lineage_edge, so a direct INSERT can no longer
    forge a mismatched edge_type. (The app role still holds direct INSERT in this
    owner-run test DB; the privilege-revoke that would make the DB authoritative
    for ALL direct mutation is deployment guidance documented in 014 and
    tests/README.md, not applied here.)"""
    triggers = conn.execute(
        "SELECT count(*) FROM pg_trigger t "
        "JOIN pg_class c ON c.oid=t.tgrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='cp' AND c.relname='lineage_edge' AND NOT t.tgisinternal",
    ).fetchone()[0]
    assert triggers >= 1, "edge_type_matches_link trigger missing — P2 fix regressed"

    # Privilege is intentionally NOT revoked in this owner-run test DB; documented
    # as deployment guidance. The trigger — not the privilege — is what now makes
    # edge_type unforgeable.
    can_insert = conn.execute(
        "SELECT has_table_privilege(current_user,'cp.lineage_edge','INSERT')",
    ).fetchone()[0]
    assert can_insert is True, "test DB role unexpectedly lost direct INSERT"


def test_FIXED_forged_edge_rejected_trace_cannot_overclaim(conn):
    """P10-B (014): forging a raw_to_curated edge naming an UNRELATED file under a
    canonical link — which previously made trace_row.sql over-claim a raw the
    canonical never derived from — is now REJECTED by the edge_type_matches_link
    trigger, so the trace stays honest (legit raw only, no forged raw)."""
    sink_run = _run(conn, "sink")
    up_run = _run(conn, "ingestion", status="succeeded")
    f_legit = _file(conn)
    up_link = _raw_to_curated(conn, up_run, f_legit)
    canon = conn.execute(
        "SELECT cp.write_lineage_link(%s,'curated_to_canonical',%s,1,%s,NULL,NULL)",
        (sink_run, Jsonb(_tref("c")), Jsonb(
            [{"edge_type": "curated_to_canonical", "upstream_run_id": str(up_run),
              "upstream_lineage_link_id": str(up_link), "source_ref": {},
              "record_count": 1}])),
    ).fetchone()[0]

    f_fake = _file(conn)  # a raw file this canonical NEVER came from
    # The forge (raw_to_curated edge under a curated_to_canonical link) RAISES.
    with pytest.raises(psycopg.errors.RaiseException):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.lineage_edge "
                "(lineage_link_id, edge_type, source_file_id, source_ref, record_count) "
                "VALUES (%s,'raw_to_curated',%s,%s,1)",
                (canon, str(f_fake), Jsonb({"forged": True})))

    paths = _raw_paths(_trace(conn, canon))
    legit = conn.execute(
        "SELECT s3_raw_path FROM cp.file_catalogue WHERE file_id=%s",
        (f_legit,)).fetchone()[0]
    fake = conn.execute(
        "SELECT s3_raw_path FROM cp.file_catalogue WHERE file_id=%s",
        (f_fake,)).fetchone()[0]
    assert legit in paths, "legit raw vanished from trace"
    assert fake not in paths, "forged raw over-claimed — trigger failed to block it"


# =========================================================================== #
# CONFIRMED orphans — states the function path forbids but the TABLE allows.
# =========================================================================== #
def test_CONFIRMED_orphan_link_zero_edges_dead_ends(conn):
    """write_lineage_link requires >=1 edge; a direct INSERT into cp.lineage_link
    creates a link with ZERO edges that is absent from v_provenance and whose
    trace returns nothing — an unreachable link with no chain to raw."""
    run_id = _run(conn, "sink")
    orphan = conn.execute(
        "INSERT INTO cp.lineage_link (consumer_run_id,edge_type,target_ref,record_count) "
        "VALUES (%s,'curated_to_canonical',%s,1) RETURNING lineage_link_id",
        (run_id, Jsonb(_tref("orphan"))),
    ).fetchone()[0]
    assert orphan is not None
    edges = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE lineage_link_id=%s", (orphan,),
    ).fetchone()[0]
    in_prov = conn.execute(
        "SELECT count(*) FROM cp.v_provenance WHERE lineage_link_id=%s", (orphan,),
    ).fetchone()[0]
    assert edges == 0 and in_prov == 0
    assert _raw_paths(_trace(conn, orphan)) == [], "orphan link reached a raw (?)"


def test_CONFIRMED_orphan_sink_row_traces_to_no_raw(conn):
    """A committed ods.orders row whose lineage link has no provenance chain
    (0-edge sink link, direct-inserted) traces to NO raw — a real row that is
    lost to lineage despite satisfying the FK on _ods_lineage_link_id."""
    run_id = _run(conn, "sink")
    orphan_link = conn.execute(
        "INSERT INTO cp.lineage_link "
        "(consumer_run_id,edge_type,sink_type,target_ref,record_count) "
        "VALUES (%s,'canonical_to_sink','postgres',%s,1) RETURNING lineage_link_id",
        (run_id, Jsonb(_tref("osink"))),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO ods.orders (payload,_ods_workflow_run_id,_ods_lineage_link_id) "
        "VALUES (%s,%s,%s)",
        (Jsonb({"k": 1}), str(uuid.uuid4()), str(orphan_link)))

    assert _raw_paths(_trace(conn, orphan_link)) == [], \
        "orphan sink row unexpectedly traced to raw"


# =========================================================================== #
# CONFIRMED A4-S5 — cross-hop loss is silent (carried forward).
#   60 raw rows arrive; one upstream wholly fails downstream so only 30 reach
#   canonical/sink. Sink recon balances (30==30) and there is NO automated
#   cross-hop check, so the 30 lost rows leave no breach and no trace.
# =========================================================================== #
def test_CONFIRMED_wholly_failed_upstream_loses_rows_with_no_breach(conn):
    wf = str(uuid.uuid4())

    def ingest(n):
        # Two genuinely-distinct physical files (NOT a restart): each ingest run
        # stamps its OWN file_id on run_log — exactly as the real ingest path
        # does — so the two runs are distinct under uq_run_identity (013). The
        # earlier omission of file_id (NULL) relied on pre-013 absence of run
        # identity and would now collide two separate files on the sentinel key.
        f = conn.execute(
            "SELECT cp.register_file(%s,%s,%s,'sales','orders')",
            (f"s3://raw/{uuid.uuid4()}.csv", uuid.uuid4().hex, BD)).fetchone()[0]
        r = conn.execute(
            "INSERT INTO cp.run_log (workflow_run_id,pipeline_type,domain,dataset,"
            "business_date,trigger_type,status,file_id) "
            "VALUES (%s,'ingestion','sales','orders',%s,'manual','succeeded',%s) "
            "RETURNING run_id", (wf, BD, f)).fetchone()[0]
        return r, _raw_to_curated(conn, r, f, n=n)

    ok, fail = ingest(30), ingest(30)  # 60 arrived; both ingested

    cr = conn.execute(
        "INSERT INTO cp.run_log (workflow_run_id,pipeline_type,domain,dataset,"
        "business_date,trigger_type,status) "
        "VALUES (%s,'canonicalize','sales','orders',%s,'manual','succeeded') "
        "RETURNING run_id", (wf, BD)).fetchone()[0]
    canon = conn.execute(
        "SELECT cp.write_lineage_link(%s,'curated_to_canonical',%s,30,%s,NULL,NULL)",
        (cr, Jsonb(_tref("cok")), Jsonb(
            [{"edge_type": "curated_to_canonical", "upstream_run_id": str(ok[0]),
              "upstream_lineage_link_id": str(ok[1]), "source_ref": {},
              "record_count": 30}]))).fetchone()[0]
    sr = conn.execute(
        "INSERT INTO cp.run_log (workflow_run_id,pipeline_type,domain,dataset,"
        "business_date,trigger_type,status) "
        "VALUES (%s,'sink','sales','orders',%s,'manual','running') RETURNING run_id",
        (wf, BD)).fetchone()[0]
    conn.execute(
        "SELECT cp.write_link_then_rows(%s,'canonical_to_sink',%s,30,%s,%s,'postgres',NULL)",
        (sr, Jsonb(_tref("sok")), Jsonb(
            [{"edge_type": "canonical_to_sink", "upstream_run_id": str(cr),
              "upstream_lineage_link_id": str(canon), "source_ref": {},
              "record_count": 30}]),
         Jsonb([{"k": i} for i in range(30)])))

    # Sink recon balances: the sink only knows its own 30 upstream rows.
    conn.execute("SELECT cp.reconcile_sink(%s,%s)", (sr, 30))
    status = conn.execute(
        "SELECT status, discrepancy FROM cp.reconciliation_log "
        "WHERE run_id=%s AND check_type='sink_graph'", (sr,)).fetchone()
    assert status == ("ok", 0), "sink recon unexpectedly breached"

    # The loss IS computable from lineage sums (raw 60 vs canonical 30)...
    raw_rc = conn.execute(
        "SELECT coalesce(sum(e.record_count),0) FROM cp.lineage_edge e "
        "JOIN cp.lineage_link l ON l.lineage_link_id=e.lineage_link_id "
        "JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
        "WHERE r.workflow_run_id=%s AND e.edge_type='raw_to_curated'", (wf,),
    ).fetchone()[0]
    can_rc = conn.execute(
        "SELECT coalesce(sum(record_count),0) FROM cp.lineage_link l "
        "JOIN cp.run_log r ON r.run_id=l.consumer_run_id "
        "WHERE r.workflow_run_id=%s AND l.edge_type='curated_to_canonical'", (wf,),
    ).fetchone()[0]
    assert (raw_rc, can_rc) == (60, 30), "cross-hop counts changed"
    # ...but the failed upstream's curated link is consumed by NOTHING: lost.
    consumed = conn.execute(
        "SELECT count(*) FROM cp.lineage_edge WHERE upstream_lineage_link_id=%s",
        (fail[1],)).fetchone()[0]
    assert consumed == 0, "failed upstream was unexpectedly consumed downstream"


# =========================================================================== #
# SOUND — the sanctioned client paths DO trace every row to raw (regression).
# =========================================================================== #
def test_SOUND_single_ingest_traces_to_raw(conn):
    f = {"s3_raw_path": f"s3://raw/sales/orders/{uuid.uuid4().hex}.csv",
         "file_md5": "md5-" + uuid.uuid4().hex, "business_date": BD,
         "domain": "sales", "dataset": "orders", "record_count": 12}
    res = composers.run_single_file(conn, file=f, commit=False)
    assert _raw_paths(_trace(conn, res["link_id"])), "single ingest dead-ended"


def test_SOUND_fanout_each_sink_traces_to_raw(conn):
    f = {"s3_raw_path": f"s3://raw/sales/orders/{uuid.uuid4().hex}.csv",
         "file_md5": "md5-" + uuid.uuid4().hex, "business_date": BD,
         "domain": "sales", "dataset": "orders", "record_count": 8}
    res = composers.run_to_fanout_sinks(
        conn, file=f, sink_types=("postgres", "kafka"), commit=False)
    sinks = res["sinks"]
    assert set(sinks.keys()) == {"postgres", "kafka"}
    for st, sink in sinks.items():
        assert _raw_paths(_trace(conn, sink["link_id"])), \
            f"{st} sink dead-ended"


def test_SOUND_merge_traces_every_parent_to_raw(conn):
    files = [
        {"s3_raw_path": f"s3://raw/sales/orders/{uuid.uuid4().hex}.csv",
         "file_md5": "md5-" + uuid.uuid4().hex, "business_date": BD,
         "domain": "sales", "dataset": "orders", "record_count": 5}
        for _ in range(3)
    ]
    res = composers.run_multi_file(conn, files=files, commit=False)
    merge_link = res["merge"]["link_id"]
    rows = _trace(conn, merge_link)
    assert _raw_paths(rows), "merge dead-ended"
    # An N-way merge must reach EACH of the N raw files (no parent dropped).
    assert len(set(_raw_paths(rows))) == len(files), \
        "merge did not reach all N raw parents"
