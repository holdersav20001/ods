"""P4 items 3 & 4 — EVERY foreign key, the edge_type FK, and NOT NULL rejections.

This is a CONSTRAINT/negative test: it probes the DB's referential integrity by
inserting dangling references and asserting psycopg raises IntegrityError. Raw
SQL is sanctioned HERE (and only here + the adapter mock) precisely because the
point is to exercise the constraint itself, not the client path.

Each probe runs inside its OWN savepoint (conn.transaction()) so the aborted
sub-transaction does not poison the others; the outer `conn` fixture still rolls
the whole thing back for isolation.

FKs covered (every one in the schema):
  cp.lineage_link.consumer_run_id   -> cp.run_log
  cp.lineage_edge.upstream_run_id   -> cp.run_log
  cp.lineage_edge.source_file_id    -> cp.file_catalogue
  cp.lineage_edge.lineage_link_id   -> cp.lineage_link
  ods.orders._ods_lineage_link_id   -> cp.lineage_link
  cp.dlq.run_id                     -> cp.run_log
  cp.dlq.replay_run_id              -> cp.run_log
  cp.run_log.file_id                -> cp.file_catalogue
  cp.run_log.replay_of_run_id       -> cp.run_log
Plus:
  cp.lineage_link.edge_type -> cp.edge_type (bad edge_type rejected)
  cp.run_log.workflow_run_id NOT NULL (null rejected)
"""
import datetime
import json
import uuid

import psycopg
import pytest

BD = datetime.date(2026, 1, 2)


def _bogus():
    return str(uuid.uuid4())


def _real_run(conn):
    """A real run_log row (so we can isolate which FK fails)."""
    return conn.execute(
        "SELECT cp.start_run(%s,'ingestion','sales','orders',%s,'manual')",
        (str(uuid.uuid4()), BD)).fetchone()[0]


def _real_link(conn, run_id):
    return conn.execute(
        "SELECT cp.write_lineage_link(%s,'raw_to_curated',%s,%s,%s)",
        (run_id, json.dumps({"content_hash": "real-" + uuid.uuid4().hex}), 1,
         json.dumps([{"edge_type": "raw_to_curated",
                      "source_ref": {"k": 1}, "record_count": 1}]))).fetchone()[0]


# --------------------------------------------------------------------------- #
# Foreign keys — each in its own savepoint + pytest.raises.
# --------------------------------------------------------------------------- #
def test_fk_lineage_link_consumer_run_id(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.lineage_link "
                "(consumer_run_id, edge_type, target_ref, record_count) "
                "VALUES (%s,'raw_to_curated',%s,1)",
                (_bogus(), json.dumps({"content_hash": "x"})))


def test_fk_lineage_edge_upstream_run_id(conn):
    run_id = _real_run(conn)
    link_id = _real_link(conn, run_id)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.lineage_edge "
                "(lineage_link_id, upstream_run_id, edge_type, record_count) "
                "VALUES (%s,%s,'raw_to_curated',1)",
                (link_id, _bogus()))


def test_fk_lineage_edge_source_file_id(conn):
    run_id = _real_run(conn)
    link_id = _real_link(conn, run_id)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.lineage_edge "
                "(lineage_link_id, source_file_id, edge_type, record_count) "
                "VALUES (%s,%s,'raw_to_curated',1)",
                (link_id, _bogus()))


def test_fk_lineage_edge_lineage_link_id(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.lineage_edge "
                "(lineage_link_id, edge_type, record_count) "
                "VALUES (%s,'raw_to_curated',1)",
                (_bogus(),))


def test_fk_ods_orders_lineage_link_id(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO ods.orders (payload, _ods_lineage_link_id) "
                "VALUES (%s,%s)",
                (json.dumps({"k": 1}), _bogus()))


def test_fk_dlq_run_id(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.dlq (run_id, stage, reason, record_count) "
                "VALUES (%s,'canonicalize','x',1)",
                (_bogus(),))


def test_fk_dlq_replay_run_id(conn):
    run_id = _real_run(conn)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.dlq (run_id, replay_run_id, stage, reason, record_count) "
                "VALUES (%s,%s,'canonicalize','x',1)",
                (run_id, _bogus()))


def test_fk_run_log_file_id(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.run_log "
                "(workflow_run_id, trigger_type, pipeline_type, domain, dataset, "
                " business_date, file_id) "
                "VALUES (%s,'manual','ingestion','sales','orders',%s,%s)",
                (str(uuid.uuid4()), BD, _bogus()))


def test_fk_run_log_replay_of_run_id(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.run_log "
                "(workflow_run_id, trigger_type, pipeline_type, domain, dataset, "
                " business_date, replay_of_run_id) "
                "VALUES (%s,'replay','ingestion','sales','orders',%s,%s)",
                (str(uuid.uuid4()), BD, _bogus()))


# --------------------------------------------------------------------------- #
# edge_type FK + NOT NULL.
# --------------------------------------------------------------------------- #
def test_bad_edge_type_rejected(conn):
    """An edge_type not in cp.edge_type is an FK violation (lineage_link FK)."""
    run_id = _real_run(conn)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.lineage_link "
                "(consumer_run_id, edge_type, target_ref, record_count) "
                "VALUES (%s,'not_a_real_edge_type',%s,1)",
                (run_id, json.dumps({"content_hash": "x"})))


def test_bad_edge_type_via_write_link_rejected(conn):
    """The client/SQL function path also rejects an unknown edge_type."""
    run_id = _real_run(conn)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "SELECT cp.write_lineage_link(%s,'bogus_edge',%s,%s,%s)",
                (run_id, json.dumps({"content_hash": "x"}), 1,
                 json.dumps([{"edge_type": "bogus_edge",
                              "source_ref": {}, "record_count": 1}])))


def test_run_log_null_workflow_run_id_rejected(conn):
    with pytest.raises(psycopg.errors.NotNullViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO cp.run_log "
                "(workflow_run_id, trigger_type, pipeline_type, domain, dataset, "
                " business_date) "
                "VALUES (NULL,'manual','ingestion','sales','orders',%s)",
                (BD,))


def test_constraint_count_evidence(conn):
    """Aggregate evidence print: count of FK + NOT NULL rejections probed."""
    # 9 FKs + 2 edge_type probes + 1 not-null = 12 negative cases in this module.
    print("\n[CONSTRAINTS] negative cases in this module: 9 FK + 2 edge_type FK "
          "+ 1 NOT NULL = 12 rejections asserted")
