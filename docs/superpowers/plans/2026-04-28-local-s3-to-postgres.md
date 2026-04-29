# Local S3-to-Postgres Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the local docker-compose S3 batch pipeline (SFTP → S3 → Glue → Avro → Kafka → Connect sinks → Postgres + S3 curated) with consolidated `pipeline.*` tables and T0 + T2 reconciliation.

**Architecture:** Existing compose already has localstack, postgres (5440), zookeeper, broker, schema-registry, glue, airflow. Plan extends it with `sftp`, `kafka-connect`, `grafana`. Pipeline metadata is consolidated into `run_log` (header) + `run_stage_log` (child) + `reconciliation_log`; legacy `glue_job_log`/`lineage`/`file_state`/`ingestion_file_state` are renamed to `_deprecated_2026_04_28` and replaced by a compat view.

**Tech Stack:** Confluent Kafka + Schema Registry (existing), Confluent Kafka Connect 7.5 (JDBC + S3 sinks), atmoz/sftp, Postgres 16, Airflow 2.x (existing), Spark 3.3.2 + Java 17 in `ods-glue:local` (existing), Grafana, LocalStack S3.

**Spec:** `docs/superpowers/specs/2026-04-28-local-s3-to-postgres-design.md`

---

## File Structure

**New files:**
- `db/migrations/03_consolidate_pipeline_tables.sql` — new tables, backfill, deprecate legacy.
- `db/migrations/04_dataset_config_extensions.sql` — config_version + tolerance columns.
- `datasets/insurance/policies.yaml` — first dataset config in YAML form.
- `airflow/dags/common/run_log.py` — write helpers (header + stage rows).
- `airflow/dags/common/connect_admin.py` — Kafka Connect REST helpers.
- `airflow/dags/common/kafka_admin.py` — offset queries.
- `airflow/dags/dag_config_sync.py`
- `airflow/dags/dag_drop_to_raw.py`
- `airflow/dags/dag_ingest.py`
- `airflow/dags/dag_recon_t2.py`
- `docker/sftp/users.conf`
- `docker/kafka-connect/Dockerfile`
- `docker/connect-config/jdbc-sink-policies.json`
- `docker/connect-config/s3-sink-policies.json`
- `docker/connect-config/bootstrap.sh`
- `docker/grafana/provisioning/datasources/postgres.yaml`
- `docker/grafana/provisioning/dashboards/dashboards.yaml`
- `docker/grafana/dashboards/run-health.json`
- `docker/grafana/dashboards/recon.json`
- `tests/unit/test_run_log_helpers.py`
- `tests/unit/test_t0_check.py`
- `tests/unit/test_config_sync.py`
- `tests/integration/test_e2e_sftp_to_postgres.py`
- `tests/integration/test_idempotent_drop.py`
- `tests/integration/test_dq_block_to_dlq.py`
- `tests/integration/test_t0_mismatch.py`
- `tests/integration/test_t2_recon.py`
- `tests/integration/test_sink_failure.py`

**Modified files:**
- `docker-compose.yml` — add sftp, kafka-connect, grafana, connect-bootstrap services + volumes.
- `glue/jobs/ods_ingestion.py` — write `run_log`+`run_stage_log` instead of `glue_job_log`+`lineage`; read `record_count_source` for T0.
- `glue/jobs/ods_s3_publish.py` — write to new tables; capture `kafka_offset_start/end`; perform T0 check; switch to Confluent SR Avro serializer.
- `glue/jobs/utils.py` — add `write_run_log_header`, `update_run_log`, `write_stage`, `write_recon`.
- `tests/conftest.py` — add fixtures for kafka-connect REST client, sftp uploader, redpanda admin → confluent broker admin.
- `db/migrations/02_seed_policies.sql` — convert seed to use new `dataset_config` columns OR rely on `dag_config_sync` (decide in Task 2).

---

## Task 1: Migration — consolidated `pipeline.*` tables

**Files:**
- Create: `db/migrations/03_consolidate_pipeline_tables.sql`
- Create: `db/migrations/04_dataset_config_extensions.sql`
- Test: `tests/unit/test_migrations.py` (existing — extend)

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_migrations.py`:

```python
import psycopg2

def test_run_log_table_exists(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema='pipeline' AND table_name='run_log'
            ORDER BY ordinal_position
        """)
        cols = {r[0]: r[1] for r in cur.fetchall()}
    assert 'run_id' in cols and cols['run_id'] == 'uuid'
    assert 'pipeline_type' in cols
    assert 'record_count_published' in cols
    assert 'kafka_offset_start' in cols and cols['kafka_offset_start'] == 'bigint'
    assert 'parents' in cols and cols['parents'] == 'jsonb'

def test_run_stage_log_fk(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_schema='pipeline' AND table_name='run_stage_log'
              AND constraint_type='FOREIGN KEY'
        """)
        assert cur.fetchone() is not None

def test_reconciliation_log_table_exists(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pipeline.reconciliation_log LIMIT 1")
        # no rows is fine; query must succeed
    assert True

def test_legacy_tables_renamed(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='pipeline'
              AND table_name LIKE '%_deprecated_2026_04_28'
        """)
        names = {r[0] for r in cur.fetchall()}
    assert 'glue_job_log_deprecated_2026_04_28' in names
    assert 'lineage_deprecated_2026_04_28' in names
    assert 'file_state_deprecated_2026_04_28' in names
    assert 'ingestion_file_state_deprecated_2026_04_28' in names

def test_dataset_config_has_version_columns(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='pipeline' AND table_name='dataset_config'
        """)
        cols = {r[0] for r in cur.fetchall()}
    assert 'config_version_id' in cols
    assert 'config_yaml_hash' in cols
    assert 'recon_tolerance_records' in cols
    assert 'recon_tolerance_pct' in cols

def test_v_lineage_view_exists(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pipeline.v_lineage LIMIT 1")
    assert True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `docker compose up -d postgres && pytest tests/unit/test_migrations.py -v`
Expected: FAIL — `relation "pipeline.run_log" does not exist`.

- [ ] **Step 3: Write `03_consolidate_pipeline_tables.sql`**

```sql
-- 03_consolidate_pipeline_tables.sql

BEGIN;

-- New: file_catalogue (replaces file_state + ingestion_file_state).
-- Existing pipeline.file_catalogue is a different shape; rename it first then recreate.
ALTER TABLE pipeline.file_catalogue RENAME TO file_catalogue_deprecated_2026_04_28;

CREATE TABLE pipeline.file_catalogue (
    file_id                 UUID PRIMARY KEY,
    domain                  VARCHAR NOT NULL,
    dataset                 VARCHAR NOT NULL,
    business_date           DATE NOT NULL,
    sftp_path               VARCHAR,
    s3_raw_path             VARCHAR,
    s3_staging_parquet_path VARCHAR,
    s3_curated_path         VARCHAR,
    file_size_bytes         BIGINT,
    source_row_count        BIGINT,
    file_md5                CHAR(32) NOT NULL,
    state                   VARCHAR NOT NULL,
    state_updated_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    first_seen_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    last_run_id             UUID,
    UNIQUE (domain, dataset, file_md5)
);
CREATE INDEX idx_file_catalogue_state ON pipeline.file_catalogue(state, state_updated_at);

-- New: run_log (header per run).
CREATE TABLE pipeline.run_log (
    run_id                  UUID PRIMARY KEY,
    pipeline_type           VARCHAR NOT NULL,
    domain                  VARCHAR NOT NULL,
    dataset                 VARCHAR NOT NULL,
    business_date           DATE,
    file_id                 UUID REFERENCES pipeline.file_catalogue(file_id),
    status                  VARCHAR NOT NULL,
    started_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at                TIMESTAMP,
    record_count_source     BIGINT,
    record_count_dq_pass    BIGINT,
    record_count_dq_fail    BIGINT,
    record_count_published  BIGINT,
    kafka_topic             VARCHAR,
    kafka_offset_start      BIGINT,
    kafka_offset_end        BIGINT,
    config_version_id       BIGINT,
    schema_version_id       INT,
    parents                 JSONB,
    error_summary           TEXT,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_run_log_dom_ds_bd ON pipeline.run_log(domain, dataset, business_date);
CREATE INDEX idx_run_log_status_started ON pipeline.run_log(status, started_at);

-- New: run_stage_log (child).
CREATE TABLE pipeline.run_stage_log (
    id               BIGSERIAL PRIMARY KEY,
    run_id           UUID NOT NULL REFERENCES pipeline.run_log(run_id),
    stage            VARCHAR NOT NULL,
    status           VARCHAR NOT NULL,
    started_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at         TIMESTAMP,
    input_ref        TEXT,
    output_ref       TEXT,
    record_count_in  BIGINT,
    record_count_out BIGINT,
    metrics          JSONB,
    error            TEXT
);
CREATE INDEX idx_run_stage_log_run_stage ON pipeline.run_stage_log(run_id, stage);

-- New: reconciliation_log.
CREATE TABLE pipeline.reconciliation_log (
    id                BIGSERIAL PRIMARY KEY,
    check_type        VARCHAR NOT NULL,
    run_id            UUID,
    domain            VARCHAR NOT NULL,
    dataset           VARCHAR NOT NULL,
    business_date     DATE,
    window_start      TIMESTAMP,
    window_end        TIMESTAMP,
    source_count      BIGINT,
    kafka_count       BIGINT,
    postgres_count    BIGINT,
    discrepancy_count BIGINT,
    discrepancy_pct   NUMERIC(8,4),
    status            VARCHAR NOT NULL,
    detail            TEXT,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_recon_dom_ds_check ON pipeline.reconciliation_log(domain, dataset, check_type, created_at);

-- Backfill from existing tables (no FK enforcement on file_id since legacy file_catalogue had a different shape).
INSERT INTO pipeline.run_log (
    run_id, pipeline_type, domain, dataset, business_date,
    status, started_at, ended_at,
    record_count_source, record_count_published,
    kafka_topic, kafka_offset_start, kafka_offset_end,
    config_version_id, schema_version_id, error_summary, created_at
)
SELECT
    g.run_id,
    g.pipeline_type,
    g.domain, g.dataset, g.business_date,
    g.status, g.created_at, g.created_at,
    CASE WHEN g.pipeline_type='ingestion' THEN g.record_count END,
    CASE WHEN g.pipeline_type='publish'   THEN g.record_count END,
    l.target_topic, l.kafka_offset_start, l.kafka_offset_end,
    g.config_version, l.schema_version, g.error_reason, g.created_at
FROM pipeline.glue_job_log g
LEFT JOIN pipeline.lineage l ON l.run_id = g.run_id
ON CONFLICT (run_id) DO NOTHING;

-- Rename legacy tables.
ALTER TABLE pipeline.glue_job_log         RENAME TO glue_job_log_deprecated_2026_04_28;
ALTER TABLE pipeline.lineage              RENAME TO lineage_deprecated_2026_04_28;
ALTER TABLE pipeline.file_state           RENAME TO file_state_deprecated_2026_04_28;
ALTER TABLE pipeline.ingestion_file_state RENAME TO ingestion_file_state_deprecated_2026_04_28;

-- Compat view.
CREATE VIEW pipeline.v_lineage AS
SELECT
    r.run_id, r.pipeline_type, r.domain, r.dataset, r.business_date,
    fc.s3_raw_path AS source_ref,
    r.kafka_topic, r.kafka_offset_start, r.kafka_offset_end,
    r.config_version_id, r.schema_version_id, r.parents, r.created_at
FROM pipeline.run_log r
LEFT JOIN pipeline.file_catalogue fc ON fc.file_id = r.file_id;

COMMIT;
```

- [ ] **Step 4: Write `04_dataset_config_extensions.sql`**

```sql
BEGIN;

ALTER TABLE pipeline.dataset_config
    ADD COLUMN IF NOT EXISTS source_type             VARCHAR NOT NULL DEFAULT 's3_batch',
    ADD COLUMN IF NOT EXISTS schema_def              JSONB,
    ADD COLUMN IF NOT EXISTS postgres_target_table   VARCHAR,
    ADD COLUMN IF NOT EXISTS s3_curated_path         VARCHAR,
    ADD COLUMN IF NOT EXISTS config_version_id       BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS config_yaml_hash        CHAR(64) NOT NULL DEFAULT '0000000000000000000000000000000000000000000000000000000000000000',
    ADD COLUMN IF NOT EXISTS config_pinned_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS recon_tolerance_records INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recon_tolerance_pct     NUMERIC(6,4) NOT NULL DEFAULT 0;

COMMIT;
```

- [ ] **Step 5: Apply migrations**

Run:
```bash
docker compose up -d postgres
docker compose exec -T postgres psql -U postgres -d ods -f /docker-entrypoint-initdb.d/03_consolidate_pipeline_tables.sql
docker compose exec -T postgres psql -U postgres -d ods -f /docker-entrypoint-initdb.d/04_dataset_config_extensions.sql
```
(Or destroy the volume and let init scripts run: `docker compose down -v && docker compose up -d postgres`.)

Expected: `COMMIT` for both.

- [ ] **Step 6: Run tests to verify pass**

Run: `pytest tests/unit/test_migrations.py -v`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add db/migrations/03_consolidate_pipeline_tables.sql \
        db/migrations/04_dataset_config_extensions.sql \
        tests/unit/test_migrations.py
git commit -m "feat(db): consolidate pipeline tables into run_log + run_stage_log + reconciliation_log"
```

---

## Task 2: YAML config + `dag_config_sync`

**Files:**
- Create: `datasets/insurance/policies.yaml`
- Create: `airflow/dags/dag_config_sync.py`
- Create: `airflow/dags/common/__init__.py`
- Create: `airflow/dags/common/yaml_loader.py`
- Test: `tests/unit/test_config_sync.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_config_sync.py
import json, hashlib, pytest, psycopg2
from airflow.dags.common.yaml_loader import load_dataset_yaml, compute_hash, sync_to_db

POLICIES_YAML = """
domain: insurance
dataset: policies
source_type: s3_batch
filename_pattern: '^policies_(?P<bd>\\d{8})\\.csv$'
key_fields: [policy_id]
target_topic: ods.insurance.policies
postgres_target_table: ods.insurance_policies
s3_curated_path: s3://ods-curated/insurance/policies/
schema_def:
  fields:
    - {name: policy_id, type: string}
    - {name: status,    type: string}
    - {name: premium,   type: decimal(10,2)}
dq_rules:
  hard:
    - {rule: not_null, column: policy_id}
  soft: []
recon_tolerance_records: 0
recon_tolerance_pct: 0
"""

def test_load_yaml(tmp_path):
    p = tmp_path / "policies.yaml"
    p.write_text(POLICIES_YAML)
    cfg = load_dataset_yaml(str(p))
    assert cfg['domain'] == 'insurance'
    assert cfg['dataset'] == 'policies'
    assert cfg['key_fields'] == ['policy_id']

def test_hash_is_deterministic():
    h1 = compute_hash({'a': 1, 'b': [1, 2]})
    h2 = compute_hash({'b': [1, 2], 'a': 1})
    assert h1 == h2 and len(h1) == 64

def test_sync_inserts_then_bumps_version(pg_conn, tmp_path):
    p = tmp_path / "policies.yaml"
    p.write_text(POLICIES_YAML)
    sync_to_db(str(p), pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT config_version_id, config_yaml_hash FROM pipeline.dataset_config WHERE domain='insurance' AND dataset='policies'")
        v1, h1 = cur.fetchone()
    # second sync, identical content → no version bump
    sync_to_db(str(p), pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT config_version_id FROM pipeline.dataset_config WHERE domain='insurance' AND dataset='policies'")
        (v2,) = cur.fetchone()
    assert v2 == v1
    # change content → bump
    p.write_text(POLICIES_YAML.replace('recon_tolerance_records: 0', 'recon_tolerance_records: 5'))
    sync_to_db(str(p), pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT config_version_id FROM pipeline.dataset_config WHERE domain='insurance' AND dataset='policies'")
        (v3,) = cur.fetchone()
    assert v3 == v1 + 1
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_config_sync.py -v`
Expected: ImportError — `airflow.dags.common.yaml_loader` not found.

- [ ] **Step 3: Implement `airflow/dags/common/yaml_loader.py`**

```python
import hashlib, json, yaml

def load_dataset_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def compute_hash(cfg: dict) -> str:
    canonical = json.dumps(cfg, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode()).hexdigest()

def sync_to_db(path: str, pg_conn) -> None:
    cfg = load_dataset_yaml(path)
    h = compute_hash(cfg)
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT config_yaml_hash, config_version_id
            FROM pipeline.dataset_config
            WHERE domain=%s AND dataset=%s
        """, (cfg['domain'], cfg['dataset']))
        row = cur.fetchone()
        if row and row[0] == h:
            return
        new_version = (row[1] + 1) if row else 1
        cur.execute("""
            INSERT INTO pipeline.dataset_config
                (domain, dataset, source_type, filename_pattern, target_topic,
                 schema_id, key_fields, dq_rules, schema_def,
                 postgres_target_table, s3_curated_path,
                 config_version_id, config_yaml_hash, config_pinned_at,
                 recon_tolerance_records, recon_tolerance_pct)
            VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s, %s,%s,NOW(), %s,%s)
            ON CONFLICT (domain, dataset) DO UPDATE SET
                source_type=EXCLUDED.source_type,
                filename_pattern=EXCLUDED.filename_pattern,
                target_topic=EXCLUDED.target_topic,
                key_fields=EXCLUDED.key_fields,
                dq_rules=EXCLUDED.dq_rules,
                schema_def=EXCLUDED.schema_def,
                postgres_target_table=EXCLUDED.postgres_target_table,
                s3_curated_path=EXCLUDED.s3_curated_path,
                config_version_id=EXCLUDED.config_version_id,
                config_yaml_hash=EXCLUDED.config_yaml_hash,
                config_pinned_at=NOW(),
                recon_tolerance_records=EXCLUDED.recon_tolerance_records,
                recon_tolerance_pct=EXCLUDED.recon_tolerance_pct
        """, (
            cfg['domain'], cfg['dataset'], cfg.get('source_type', 's3_batch'),
            cfg['filename_pattern'], cfg['target_topic'],
            f"{cfg['domain']}.{cfg['dataset']}", json.dumps(cfg['key_fields']),
            json.dumps(cfg.get('dq_rules', {})), json.dumps(cfg.get('schema_def', {})),
            cfg['postgres_target_table'], cfg['s3_curated_path'],
            new_version, h,
            int(cfg.get('recon_tolerance_records', 0)),
            float(cfg.get('recon_tolerance_pct', 0)),
        ))
    pg_conn.commit()
```

- [ ] **Step 4: Write `datasets/insurance/policies.yaml`**

```yaml
domain: insurance
dataset: policies
source_type: s3_batch
filename_pattern: '^policies_(?P<bd>\d{8})\.csv$'
key_fields: [policy_id]
target_topic: ods.insurance.policies
postgres_target_table: ods.insurance_policies
s3_curated_path: s3://ods-curated/insurance/policies/
schema_def:
  fields:
    - {name: policy_id,    type: string,         nullable: false}
    - {name: status,       type: string,         nullable: false}
    - {name: premium,      type: 'decimal(10,2)', nullable: true}
    - {name: effective_date, type: date,         nullable: true}
dq_rules:
  hard:
    - {rule: not_null, column: policy_id}
    - {rule: unique,   column: policy_id}
  soft:
    - {rule: completeness_pct, column: premium, min_pct: 95}
recon_tolerance_records: 0
recon_tolerance_pct: 0
```

- [ ] **Step 5: Write `airflow/dags/dag_config_sync.py`**

```python
import os
from datetime import datetime
import pendulum, glob
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.yaml_loader import sync_to_db

DATASETS_DIR = os.environ.get('DATASETS_DIR', '/opt/airflow/datasets')
PG_DSN = os.environ['PIPELINE_PG_DSN']

def run_sync():
    conn = psycopg2.connect(PG_DSN)
    try:
        for path in glob.glob(f'{DATASETS_DIR}/**/*.yaml', recursive=True):
            sync_to_db(path, conn)
    finally:
        conn.close()

with DAG(
    dag_id='dag_config_sync',
    start_date=pendulum.datetime(2026, 4, 28, tz='UTC'),
    schedule=None,
    catchup=False,
    tags=['ods', 'config'],
):
    PythonOperator(task_id='sync_yaml_to_db', python_callable=run_sync)
```

- [ ] **Step 6: Run unit tests**

Run: `pytest tests/unit/test_config_sync.py -v`
Expected: 3 passing.

- [ ] **Step 7: Commit**

```bash
git add datasets/ airflow/dags/common/__init__.py airflow/dags/common/yaml_loader.py airflow/dags/dag_config_sync.py tests/unit/test_config_sync.py
git commit -m "feat(config): YAML dataset config + dag_config_sync"
```

---

## Task 3: `run_log` write helpers

**Files:**
- Create: `airflow/dags/common/run_log.py`
- Test: `tests/unit/test_run_log_helpers.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_run_log_helpers.py
import uuid, psycopg2
from airflow.dags.common.run_log import (
    insert_run_header, update_run_header, write_stage, write_recon
)

def test_insert_run_header_and_update(pg_conn):
    run_id = str(uuid.uuid4())
    insert_run_header(pg_conn, run_id=run_id, pipeline_type='s3_batch',
                      domain='insurance', dataset='policies',
                      business_date='2026-04-28', file_id=None,
                      config_version_id=1)
    update_run_header(pg_conn, run_id, status='succeeded',
                      record_count_source=10, record_count_published=10,
                      kafka_offset_start=0, kafka_offset_end=10,
                      kafka_topic='ods.insurance.policies')
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status, record_count_published FROM pipeline.run_log WHERE run_id=%s", (run_id,))
        s, n = cur.fetchone()
    assert s == 'succeeded' and n == 10

def test_write_stage(pg_conn):
    run_id = str(uuid.uuid4())
    insert_run_header(pg_conn, run_id=run_id, pipeline_type='s3_batch',
                      domain='insurance', dataset='policies',
                      business_date='2026-04-28', file_id=None,
                      config_version_id=1)
    write_stage(pg_conn, run_id=run_id, stage='ingest', status='succeeded',
                input_ref='s3://raw/x.csv', output_ref='s3://staging/x.parquet',
                record_count_in=10, record_count_out=10, metrics={'duration_s': 4.2})
    with pg_conn.cursor() as cur:
        cur.execute("SELECT stage, record_count_out, metrics FROM pipeline.run_stage_log WHERE run_id=%s", (run_id,))
        st, n, m = cur.fetchone()
    assert st == 'ingest' and n == 10 and m['duration_s'] == 4.2

def test_write_recon(pg_conn):
    write_recon(pg_conn, check_type='t0_publish_count', run_id=None,
                domain='insurance', dataset='policies', business_date='2026-04-28',
                source_count=10, kafka_count=9, postgres_count=None,
                status='failed', detail='offset delta < source')
    with pg_conn.cursor() as cur:
        cur.execute("SELECT discrepancy_count, status FROM pipeline.reconciliation_log ORDER BY id DESC LIMIT 1")
        d, s = cur.fetchone()
    assert d == -1 and s == 'failed'
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/unit/test_run_log_helpers.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement helpers**

```python
# airflow/dags/common/run_log.py
import json
import psycopg2

def insert_run_header(conn, *, run_id, pipeline_type, domain, dataset,
                      business_date, file_id, config_version_id,
                      schema_version_id=None, parents=None,
                      kafka_topic=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date,
                 file_id, status, kafka_topic, config_version_id, schema_version_id, parents)
            VALUES (%s,%s,%s,%s,%s, %s,'running',%s,%s,%s,%s)
        """, (run_id, pipeline_type, domain, dataset, business_date,
              file_id, kafka_topic, config_version_id, schema_version_id,
              json.dumps(parents) if parents else None))
    conn.commit()

def update_run_header(conn, run_id, **fields):
    if not fields:
        return
    sets = ', '.join(f"{k}=%s" for k in fields)
    sets += ", ended_at=COALESCE(ended_at, CASE WHEN %s IN ('succeeded','failed','partial') THEN NOW() END)"
    params = list(fields.values()) + [fields.get('status')]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE pipeline.run_log SET {sets} WHERE run_id=%s", params + [run_id])
    conn.commit()

def write_stage(conn, *, run_id, stage, status,
                input_ref=None, output_ref=None,
                record_count_in=None, record_count_out=None,
                metrics=None, error=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline.run_stage_log
                (run_id, stage, status, started_at, ended_at,
                 input_ref, output_ref, record_count_in, record_count_out, metrics, error)
            VALUES (%s,%s,%s, NOW(), NOW(), %s,%s,%s,%s,%s,%s)
        """, (run_id, stage, status, input_ref, output_ref,
              record_count_in, record_count_out,
              json.dumps(metrics) if metrics else None, error))
    conn.commit()

def write_recon(conn, *, check_type, run_id, domain, dataset, business_date,
                source_count=None, kafka_count=None, postgres_count=None,
                status, detail=None, window_start=None, window_end=None):
    discrepancy = None
    if source_count is not None and kafka_count is not None:
        discrepancy = (kafka_count or 0) - (source_count or 0)
    elif kafka_count is not None and postgres_count is not None:
        discrepancy = (postgres_count or 0) - (kafka_count or 0)
    pct = None
    if discrepancy is not None and source_count:
        pct = round(100.0 * discrepancy / source_count, 4)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline.reconciliation_log
                (check_type, run_id, domain, dataset, business_date,
                 window_start, window_end,
                 source_count, kafka_count, postgres_count,
                 discrepancy_count, discrepancy_pct, status, detail)
            VALUES (%s,%s,%s,%s,%s, %s,%s, %s,%s,%s, %s,%s,%s,%s)
        """, (check_type, run_id, domain, dataset, business_date,
              window_start, window_end,
              source_count, kafka_count, postgres_count,
              discrepancy, pct, status, detail))
    conn.commit()
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/unit/test_run_log_helpers.py -v`
Expected: 3 passing.

- [ ] **Step 5: Commit**

```bash
git add airflow/dags/common/run_log.py tests/unit/test_run_log_helpers.py
git commit -m "feat(common): run_log + stage + recon write helpers"
```

---

## Task 4: T0 publish-count check (unit-level)

**Files:**
- Create: `airflow/dags/common/recon.py`
- Test: `tests/unit/test_t0_check.py`

- [ ] **Step 1: Failing test**

```python
# tests/unit/test_t0_check.py
import pytest, uuid
from airflow.dags.common.recon import t0_check_publish

def test_t0_pass(pg_conn):
    rid = str(uuid.uuid4())
    res = t0_check_publish(pg_conn, run_id=rid, domain='insurance', dataset='policies',
                           business_date='2026-04-28', source_count=10,
                           kafka_offset_start=100, kafka_offset_end=110)
    assert res.passed is True
    assert res.discrepancy == 0

def test_t0_fail_writes_recon_row(pg_conn):
    rid = str(uuid.uuid4())
    res = t0_check_publish(pg_conn, run_id=rid, domain='insurance', dataset='policies',
                           business_date='2026-04-28', source_count=10,
                           kafka_offset_start=100, kafka_offset_end=109)
    assert res.passed is False
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pipeline.reconciliation_log WHERE run_id=%s AND check_type='t0_publish_count' AND status='failed'", (rid,))
        assert cur.fetchone()[0] == 1
```

- [ ] **Step 2: Run failing test**

Run: `pytest tests/unit/test_t0_check.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement `recon.py`**

```python
# airflow/dags/common/recon.py
from dataclasses import dataclass
from .run_log import write_recon

@dataclass
class T0Result:
    passed: bool
    discrepancy: int
    source_count: int
    kafka_count: int

def t0_check_publish(conn, *, run_id, domain, dataset, business_date,
                     source_count, kafka_offset_start, kafka_offset_end):
    kafka_count = (kafka_offset_end or 0) - (kafka_offset_start or 0)
    discrepancy = kafka_count - source_count
    passed = discrepancy == 0
    write_recon(conn,
                check_type='t0_publish_count',
                run_id=run_id, domain=domain, dataset=dataset,
                business_date=business_date,
                source_count=source_count, kafka_count=kafka_count,
                status='ok' if passed else 'failed',
                detail=None if passed else f'discrepancy={discrepancy}')
    return T0Result(passed, discrepancy, source_count, kafka_count)
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/unit/test_t0_check.py -v`
Expected: 2 passing.

- [ ] **Step 5: Commit**

```bash
git add airflow/dags/common/recon.py tests/unit/test_t0_check.py
git commit -m "feat(recon): T0 publish-count check"
```

---

## Task 5: Adapt `glue/jobs/ods_ingestion.py` to new tables

**Files:**
- Modify: `glue/jobs/ods_ingestion.py`
- Modify: `glue/jobs/utils.py`

- [ ] **Step 1: Add new helpers in `utils.py`**

Append to `glue/jobs/utils.py`:

```python
import json, uuid as _uuid, psycopg2

def write_stage_row(pg_dsn, *, run_id, stage, status,
                    input_ref=None, output_ref=None,
                    record_count_in=None, record_count_out=None,
                    metrics=None, error=None):
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline.run_stage_log
                (run_id, stage, status, started_at, ended_at,
                 input_ref, output_ref, record_count_in, record_count_out, metrics, error)
            VALUES (%s,%s,%s, NOW(), NOW(), %s,%s,%s,%s,%s,%s)
        """, (run_id, stage, status, input_ref, output_ref,
              record_count_in, record_count_out,
              json.dumps(metrics) if metrics else None, error))

def upsert_run_header(pg_dsn, *, run_id, pipeline_type, domain, dataset,
                      business_date, file_id, kafka_topic=None,
                      config_version_id=None):
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline.run_log
                (run_id, pipeline_type, domain, dataset, business_date,
                 file_id, status, kafka_topic, config_version_id)
            VALUES (%s,%s,%s,%s,%s, %s,'running',%s,%s)
            ON CONFLICT (run_id) DO NOTHING
        """, (run_id, pipeline_type, domain, dataset, business_date,
              file_id, kafka_topic, config_version_id))

def update_run_fields(pg_dsn, run_id, **fields):
    if not fields:
        return
    sets = ', '.join(f"{k}=%s" for k in fields)
    with psycopg2.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE pipeline.run_log SET {sets}, ended_at=CASE WHEN %s IN ('succeeded','failed','partial') THEN NOW() ELSE ended_at END WHERE run_id=%s",
                    list(fields.values()) + [fields.get('status'), run_id])
```

- [ ] **Step 2: Replace `write_job_log` calls in `ods_ingestion.py`**

Find every `write_job_log(...)` call in `glue/jobs/ods_ingestion.py` and replace with two calls:

```python
# example replacement at top of main(), after run_id is generated:
upsert_run_header(PG_DSN, run_id=run_id, pipeline_type='s3_batch',
                  domain=domain, dataset=dataset, business_date=business_date,
                  file_id=file_id, config_version_id=config_version_id)
write_stage_row(PG_DSN, run_id=run_id, stage='ingest', status='running',
                input_ref=source_path)

# at success:
update_run_fields(PG_DSN, run_id, status='succeeded',
                  record_count_source=row_count,
                  record_count_dq_pass=dq_pass, record_count_dq_fail=dq_fail)
write_stage_row(PG_DSN, run_id=run_id, stage='ingest', status='succeeded',
                input_ref=source_path, output_ref=parquet_path,
                record_count_in=row_count, record_count_out=row_count,
                metrics={'dq_pass': dq_pass, 'dq_fail': dq_fail})

# at failure:
update_run_fields(PG_DSN, run_id, status='failed', error_summary=str(e))
write_stage_row(PG_DSN, run_id=run_id, stage='ingest', status='failed',
                input_ref=source_path, error=str(e))
```

Remove all references to `pipeline.glue_job_log`.

- [ ] **Step 3: Run existing E2E ingestion tests against new tables**

Run: `pytest tests/integration/test_ingestion.py -v`
Expected: all green; if any test queries `glue_job_log`, switch it to `run_log`/`run_stage_log` (do it now).

- [ ] **Step 4: Commit**

```bash
git add glue/jobs/ods_ingestion.py glue/jobs/utils.py tests/integration/test_ingestion.py
git commit -m "refactor(glue): ods_ingestion writes to run_log + run_stage_log"
```

---

## Task 6: Adapt `glue/jobs/ods_s3_publish.py` — Confluent Avro + T0 check

**Files:**
- Modify: `glue/jobs/ods_s3_publish.py`

- [ ] **Step 1: Switch Avro serialization to Confluent SR**

In `ods_s3_publish.py`, replace any custom Avro writer with `confluent_kafka.schema_registry` + `AvroSerializer`:

```python
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, MessageField, SerializationContext

sr = SchemaRegistryClient({'url': os.environ['SCHEMA_REGISTRY_URL']})
avro_schema_str = json.dumps(build_avro_schema(dataset_config['schema_def']))
value_ser = AvroSerializer(sr, avro_schema_str)
key_ser = StringSerializer('utf_8')
producer = Producer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP']})

def deliver(err, msg):
    if err:
        raise RuntimeError(f'kafka delivery failed: {err}')

# helper to build Avro schema dict from dataset_config.schema_def — keep simple:
def build_avro_schema(schema_def):
    type_map = {'string': 'string', 'int': 'int', 'long': 'long',
                'date': {'type': 'int', 'logicalType': 'date'},
                'decimal(10,2)': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 10, 'scale': 2}}
    return {
        'type': 'record',
        'name': 'Record',
        'fields': [
            {'name': f['name'],
             'type': ['null', type_map.get(f['type'], 'string')] if f.get('nullable', True) else type_map.get(f['type'], 'string')}
            for f in schema_def['fields']
        ]
    }
```

- [ ] **Step 2: Capture offsets and run T0**

```python
# Before publishing:
admin = AdminClient({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP']})
# Use confluent_kafka admin to read end offsets
def topic_end_offsets(topic):
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP'], 'group.id': f'tmp-{uuid.uuid4()}'})
    md = c.list_topics(topic, timeout=5).topics[topic]
    parts = [TopicPartition(topic, p) for p in md.partitions]
    end = sum(c.get_watermark_offsets(tp, timeout=5)[1] for tp in parts)
    c.close()
    return end

offset_start = topic_end_offsets(topic)
# publish all rows
for row in rows:
    key = '|'.join(str(row[k]) for k in dataset_config['key_fields'])
    producer.produce(topic=topic, key=key_ser(key), value=value_ser(row, SerializationContext(topic, MessageField.VALUE)), on_delivery=deliver)
producer.flush()
offset_end = topic_end_offsets(topic)

# T0 check
from airflow.dags.common.recon import t0_check_publish  # importable when PYTHONPATH includes /opt/airflow
import psycopg2
conn = psycopg2.connect(PG_DSN)
res = t0_check_publish(conn, run_id=run_id, domain=domain, dataset=dataset,
                       business_date=business_date,
                       source_count=row_count,
                       kafka_offset_start=offset_start,
                       kafka_offset_end=offset_end)
update_run_fields(PG_DSN, run_id,
                  record_count_published=offset_end - offset_start,
                  kafka_offset_start=offset_start,
                  kafka_offset_end=offset_end,
                  kafka_topic=topic,
                  status='succeeded' if res.passed else 'failed')
write_stage_row(PG_DSN, run_id=run_id, stage='publish',
                status='succeeded' if res.passed else 'failed',
                input_ref=parquet_path, output_ref=f'kafka://{topic}',
                record_count_in=row_count,
                record_count_out=offset_end - offset_start,
                metrics={'offset_start': offset_start, 'offset_end': offset_end},
                error=None if res.passed else f'T0 mismatch: {res.discrepancy}')
if not res.passed:
    sys.exit(1)
```

- [ ] **Step 3: Add to `glue/Dockerfile`**

```dockerfile
RUN pip install --no-cache-dir confluent-kafka[avro,schemaregistry] fastavro
```

Rebuild: `docker compose build glue`.

- [ ] **Step 4: Run existing publish integration test**

Run: `pytest tests/integration/test_publish.py -v`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add glue/jobs/ods_s3_publish.py glue/Dockerfile tests/integration/test_publish.py
git commit -m "refactor(glue): ods_s3_publish uses Confluent SR + T0 check"
```

---

## Task 7: docker-compose — sftp + kafka-connect + grafana

**Files:**
- Modify: `docker-compose.yml`
- Create: `docker/sftp/users.conf`
- Create: `docker/kafka-connect/Dockerfile`
- Create: `docker/connect-config/jdbc-sink-policies.json`
- Create: `docker/connect-config/s3-sink-policies.json`
- Create: `docker/connect-config/bootstrap.sh`
- Create: `docker/grafana/provisioning/datasources/postgres.yaml`
- Create: `docker/grafana/provisioning/dashboards/dashboards.yaml`
- Create: `docker/grafana/dashboards/run-health.json`
- Create: `docker/grafana/dashboards/recon.json`

- [ ] **Step 1: SFTP service**

Add to `docker-compose.yml` `services:`:

```yaml
  sftp:
    image: atmoz/sftp:latest
    ports:
      - "2222:22"
    volumes:
      - ./docker/sftp/users.conf:/etc/sftp/users.conf:ro
      - sftp-data:/home/ods/upload
    networks:
      - default
```

`docker/sftp/users.conf`:
```
ods:odspass:::upload
```

Add to `volumes:` block: `sftp-data:`.

- [ ] **Step 2: Kafka Connect service**

`docker/kafka-connect/Dockerfile`:
```dockerfile
FROM confluentinc/cp-kafka-connect:7.5.3
USER root
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.7 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.5.3
USER appuser
```

Compose service:
```yaml
  kafka-connect:
    build: ./docker/kafka-connect
    depends_on: [broker, schema-registry, postgres, localstack]
    ports: ["8083:8083"]
    environment:
      CONNECT_BOOTSTRAP_SERVERS: broker:29092
      CONNECT_GROUP_ID: ods-connect
      CONNECT_CONFIG_STORAGE_TOPIC: _connect_configs
      CONNECT_OFFSET_STORAGE_TOPIC: _connect_offsets
      CONNECT_STATUS_STORAGE_TOPIC: _connect_status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
      CONNECT_PLUGIN_PATH: /usr/share/confluent-hub-components,/usr/share/java
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1

  connect-bootstrap:
    image: curlimages/curl:8.6.0
    depends_on: [kafka-connect]
    volumes:
      - ./docker/connect-config:/cfg:ro
    entrypoint: ["/bin/sh","/cfg/bootstrap.sh"]
    restart: "no"
```

- [ ] **Step 3: Connector configs + bootstrap script**

`docker/connect-config/jdbc-sink-policies.json`:
```json
{
  "name": "jdbc-sink-policies",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "ods.insurance.policies",
    "connection.url": "jdbc:postgresql://postgres:5432/ods",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "policy_id",
    "auto.create": "false",
    "auto.evolve": "false",
    "table.name.format": "ods.insurance_policies",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

`docker/connect-config/s3-sink-policies.json`:
```json
{
  "name": "s3-sink-policies",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "topics": "ods.insurance.policies",
    "s3.bucket.name": "ods-curated",
    "s3.region": "us-east-1",
    "store.url": "http://localstack:4566",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "flush.size": "10",
    "rotate.interval.ms": "3600000",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
    "partition.duration.ms": "3600000",
    "locale": "en-US",
    "timezone": "UTC",
    "topics.dir": "insurance/policies",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

`docker/connect-config/bootstrap.sh`:
```sh
#!/bin/sh
set -e
until curl -sf http://kafka-connect:8083/ ; do echo "wait connect"; sleep 2; done
for f in /cfg/jdbc-sink-policies.json /cfg/s3-sink-policies.json; do
  name=$(grep -o '"name": *"[^"]*"' "$f" | head -1 | cut -d'"' -f4)
  curl -sf -X DELETE "http://kafka-connect:8083/connectors/$name" || true
  curl -sf -X POST -H 'Content-Type: application/json' --data @"$f" http://kafka-connect:8083/connectors
done
echo "connectors registered"
```

- [ ] **Step 4: Grafana**

```yaml
  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./docker/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./docker/grafana/dashboards:/var/lib/grafana/dashboards:ro
    depends_on: [postgres]
```

`docker/grafana/provisioning/datasources/postgres.yaml`:
```yaml
apiVersion: 1
datasources:
  - name: ODS Postgres
    type: postgres
    url: postgres:5432
    database: ods
    user: postgres
    secureJsonData: { password: postgres }
    jsonData: { sslmode: disable }
    isDefault: true
```

`docker/grafana/provisioning/dashboards/dashboards.yaml`:
```yaml
apiVersion: 1
providers:
  - name: ods
    folder: ODS
    type: file
    options: { path: /var/lib/grafana/dashboards }
```

`docker/grafana/dashboards/run-health.json`: minimal panel JSON listing today's runs by status — single table panel querying `SELECT domain, dataset, status, count(*) FROM pipeline.run_log WHERE business_date=current_date GROUP BY 1,2,3`. (Hand-edit in Grafana UI on first boot, then export here.)

`docker/grafana/dashboards/recon.json`: single panel querying `SELECT created_at, dataset, check_type, status, discrepancy_count FROM pipeline.reconciliation_log ORDER BY created_at DESC LIMIT 200`.

- [ ] **Step 5: Bring stack up**

Run:
```bash
docker compose down -v
docker compose up -d --build
docker compose logs -f connect-bootstrap
```
Expected: `connectors registered`. `curl localhost:8083/connectors` lists `jdbc-sink-policies`, `s3-sink-policies`.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml docker/
git commit -m "feat(infra): add sftp + kafka-connect + grafana to compose"
```

---

## Task 8: `dag_drop_to_raw` (SFTP → S3 raw)

**Files:**
- Create: `airflow/dags/dag_drop_to_raw.py`
- Test: `tests/integration/test_idempotent_drop.py`

- [ ] **Step 1: Failing test**

```python
# tests/integration/test_idempotent_drop.py
import io, time, paramiko, boto3, psycopg2, os

def _sftp_put(filename, body):
    t = paramiko.Transport(('localhost', 2222))
    t.connect(username='ods', password='odspass')
    sftp = paramiko.SFTPClient.from_transport(t)
    try:
        sftp.chdir('upload')
    except IOError:
        pass
    with sftp.file(filename, 'w') as f:
        f.write(body)
    sftp.close(); t.close()

def _wait_file_catalogue_count(pg_conn, md5, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM pipeline.file_catalogue WHERE file_md5=%s", (md5,))
            n = cur.fetchone()[0]
            if n >= 1:
                return n
        time.sleep(1)
    return n

def test_same_file_twice_yields_single_catalogue_row(pg_conn):
    body = "policy_id,status,premium\nP1,ACTIVE,100.00\n"
    import hashlib
    md5 = hashlib.md5(body.encode()).hexdigest()
    _sftp_put('policies_20260428.csv', body)
    n = _wait_file_catalogue_count(pg_conn, md5)
    assert n == 1
    _sftp_put('policies_20260428.csv', body)
    time.sleep(5)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pipeline.file_catalogue WHERE file_md5=%s", (md5,))
        assert cur.fetchone()[0] == 1
```

- [ ] **Step 2: Run failing**

Run: `pytest tests/integration/test_idempotent_drop.py -v`
Expected: FAIL — DAG not registered.

- [ ] **Step 3: Implement DAG**

```python
# airflow/dags/dag_drop_to_raw.py
import os, hashlib, uuid, re, json, io
import pendulum, paramiko, boto3, psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PG_DSN = os.environ['PIPELINE_PG_DSN']
SFTP_HOST = os.environ.get('SFTP_HOST', 'sftp')
SFTP_USER = 'ods'
SFTP_PASS = 'odspass'
S3_RAW_BUCKET = os.environ.get('S3_RAW_BUCKET', 'ods-raw')
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', 'http://localstack:4566')

def _sftp():
    t = paramiko.Transport((SFTP_HOST, 22)); t.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(t), t

def _s3():
    return boto3.client('s3', endpoint_url=S3_ENDPOINT,
                        aws_access_key_id='test', aws_secret_access_key='test',
                        region_name='us-east-1')

def _datasets(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT domain, dataset, filename_pattern FROM pipeline.dataset_config WHERE active=TRUE AND source_type='s3_batch'")
        return cur.fetchall()

@task
def scan_and_register():
    conn = psycopg2.connect(PG_DSN)
    sftp, t = _sftp()
    try:
        sftp.chdir('upload')
        files = sftp.listdir()
    except IOError:
        files = []
    new_files = []
    for filename in files:
        for domain, dataset, pattern in _datasets(conn):
            m = re.match(pattern, filename)
            if not m:
                continue
            with sftp.file(filename, 'r') as fh:
                body = fh.read()
            md5 = hashlib.md5(body).hexdigest()
            with conn.cursor() as cur:
                cur.execute("SELECT file_id FROM pipeline.file_catalogue WHERE domain=%s AND dataset=%s AND file_md5=%s",
                            (domain, dataset, md5))
                if cur.fetchone():
                    continue
                file_id = str(uuid.uuid4())
                bd = m.group('bd')
                bd_iso = f'{bd[:4]}-{bd[4:6]}-{bd[6:8]}'
                s3_key = f'{domain}/{dataset}/{bd_iso}/{filename}'
                _s3().put_object(Bucket=S3_RAW_BUCKET, Key=s3_key, Body=body)
                cur.execute("""
                    INSERT INTO pipeline.file_catalogue
                        (file_id, domain, dataset, business_date, sftp_path, s3_raw_path,
                         file_size_bytes, file_md5, state)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'received')
                """, (file_id, domain, dataset, bd_iso, f'/upload/{filename}',
                      f's3://{S3_RAW_BUCKET}/{s3_key}', len(body), md5))
            conn.commit()
            new_files.append({'file_id': file_id, 'domain': domain, 'dataset': dataset, 'business_date': bd_iso})
    sftp.close(); t.close(); conn.close()
    return new_files

with DAG(
    dag_id='dag_drop_to_raw',
    start_date=pendulum.datetime(2026, 4, 28, tz='UTC'),
    schedule='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['ods'],
):
    files = scan_and_register()
    TriggerDagRunOperator.partial(task_id='trigger_ingest',
        trigger_dag_id='dag_ingest').expand(conf=files)
```

- [ ] **Step 4: Run test**

Run: `pytest tests/integration/test_idempotent_drop.py -v`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add airflow/dags/dag_drop_to_raw.py tests/integration/test_idempotent_drop.py
git commit -m "feat(airflow): dag_drop_to_raw (SFTP -> S3 raw, idempotent)"
```

---

## Task 9: `dag_ingest` (orchestrate stages + sink wait)

**Files:**
- Create: `airflow/dags/dag_ingest.py`
- Create: `airflow/dags/common/connect_admin.py`
- Create: `airflow/dags/common/kafka_admin.py`

- [ ] **Step 1: Connect helpers**

```python
# airflow/dags/common/connect_admin.py
import os, time, requests
CONNECT_URL = os.environ.get('CONNECT_URL', 'http://kafka-connect:8083')

def wait_until_offset_consumed(connector_name: str, topic: str, target_offset: int, timeout_s: int = 120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = requests.get(f'{CONNECT_URL}/connectors/{connector_name}/status', timeout=5)
        if r.ok and r.json().get('connector', {}).get('state') != 'RUNNING':
            time.sleep(2); continue
        # Connect REST exposes offsets in 7.5+; fallback: time-based wait
        time.sleep(3)
        # check committed offsets via /offsets endpoint
        ro = requests.get(f'{CONNECT_URL}/connectors/{connector_name}/offsets', timeout=5)
        if ro.ok:
            for entry in ro.json().get('offsets', []):
                if entry['partition'].get('kafka_topic') == topic:
                    if entry['offset'].get('kafka_offset', 0) >= target_offset:
                        return True
        if time.time() > deadline:
            break
    return False
```

```python
# airflow/dags/common/kafka_admin.py
import os
from confluent_kafka import Consumer, TopicPartition

def topic_offsets(topic: str):
    c = Consumer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP'],
                  'group.id': f'tmp-offset-reader'})
    md = c.list_topics(topic, timeout=5).topics[topic]
    parts = [TopicPartition(topic, p) for p in md.partitions]
    start = sum(c.get_watermark_offsets(tp, timeout=5)[0] for tp in parts)
    end   = sum(c.get_watermark_offsets(tp, timeout=5)[1] for tp in parts)
    c.close()
    return start, end
```

- [ ] **Step 2: DAG**

```python
# airflow/dags/dag_ingest.py
import os, uuid, pendulum, psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from common.run_log import insert_run_header, update_run_header, write_stage
from common.connect_admin import wait_until_offset_consumed

PG_DSN = os.environ['PIPELINE_PG_DSN']

@task
def init_run(conf: dict):
    run_id = str(uuid.uuid4())
    conn = psycopg2.connect(PG_DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT config_version_id FROM pipeline.dataset_config WHERE domain=%s AND dataset=%s",
                    (conf['domain'], conf['dataset']))
        cv = cur.fetchone()[0]
    insert_run_header(conn, run_id=run_id, pipeline_type='s3_batch',
                      domain=conf['domain'], dataset=conf['dataset'],
                      business_date=conf['business_date'], file_id=conf['file_id'],
                      config_version_id=cv)
    conn.close()
    return {**conf, 'run_id': run_id, 'config_version_id': cv}

@task
def wait_sinks(ctx: dict):
    conn = psycopg2.connect(PG_DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT kafka_topic, kafka_offset_end FROM pipeline.run_log WHERE run_id=%s", (ctx['run_id'],))
        topic, target = cur.fetchone()
    ok_jdbc = wait_until_offset_consumed('jdbc-sink-policies', topic, target)
    ok_s3   = wait_until_offset_consumed('s3-sink-policies',   topic, target)
    write_stage(conn, run_id=ctx['run_id'], stage='sink_pg',
                status='succeeded' if ok_jdbc else 'failed',
                output_ref=f'kafka://{topic}#consumed',
                error=None if ok_jdbc else 'jdbc sink did not advance')
    write_stage(conn, run_id=ctx['run_id'], stage='sink_s3',
                status='succeeded' if ok_s3 else 'failed',
                output_ref=f'kafka://{topic}#consumed',
                error=None if ok_s3 else 's3 sink did not advance')
    if not (ok_jdbc and ok_s3):
        update_run_header(conn, ctx['run_id'], status='partial')
        raise RuntimeError('sink wait failed')
    conn.close()
    return ctx

@task
def finalise(ctx: dict):
    conn = psycopg2.connect(PG_DSN)
    update_run_header(conn, ctx['run_id'], status='succeeded')
    with conn.cursor() as cur:
        cur.execute("UPDATE pipeline.file_catalogue SET state='sunk', state_updated_at=NOW(), last_run_id=%s WHERE file_id=%s",
                    (ctx['run_id'], ctx['file_id']))
    conn.commit(); conn.close()

with DAG(
    dag_id='dag_ingest',
    start_date=pendulum.datetime(2026, 4, 28, tz='UTC'),
    schedule=None,
    catchup=False,
    tags=['ods'],
    params={'file_id': None, 'domain': None, 'dataset': None, 'business_date': None},
):
    ctx = init_run("{{ dag_run.conf }}")
    ingest = SparkSubmitOperator(task_id='stage_ingest',
        application='/opt/glue/jobs/ods_ingestion.py',
        application_args=['--run-id','{{ ti.xcom_pull(task_ids="init_run")["run_id"] }}',
                          '--file-id','{{ dag_run.conf["file_id"] }}',
                          '--domain','{{ dag_run.conf["domain"] }}',
                          '--dataset','{{ dag_run.conf["dataset"] }}',
                          '--business-date','{{ dag_run.conf["business_date"] }}'],
        conn_id='spark_default')
    publish = SparkSubmitOperator(task_id='stage_publish',
        application='/opt/glue/jobs/ods_s3_publish.py',
        application_args=['--run-id','{{ ti.xcom_pull(task_ids="init_run")["run_id"] }}',
                          '--domain','{{ dag_run.conf["domain"] }}',
                          '--dataset','{{ dag_run.conf["dataset"] }}',
                          '--business-date','{{ dag_run.conf["business_date"] }}'],
        conn_id='spark_default')
    waited = wait_sinks(ctx)
    fin = finalise(waited)
    ctx >> ingest >> publish >> waited >> fin
```

- [ ] **Step 3: E2E test (happy path)**

```python
# tests/integration/test_e2e_sftp_to_postgres.py
import time, hashlib, paramiko, psycopg2, os

def _put(name, body):
    t = paramiko.Transport(('localhost', 2222)); t.connect(username='ods', password='odspass')
    s = paramiko.SFTPClient.from_transport(t)
    try: s.chdir('upload')
    except IOError: pass
    with s.file(name, 'w') as f: f.write(body)
    s.close(); t.close()

def test_drop_file_lands_in_postgres(pg_conn):
    body = "policy_id,status,premium\nP100,ACTIVE,250.00\nP101,ACTIVE,300.00\n"
    _put('policies_20260428.csv', body)
    deadline = time.time() + 180
    while time.time() < deadline:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ods.insurance_policies WHERE policy_id IN ('P100','P101')")
            n = cur.fetchone()[0]
            if n == 2:
                return
        time.sleep(3)
    raise AssertionError(f'expected 2 rows, got {n}')
```

Add `db/migrations/02_seed_policies.sql` change to also create target table:
```sql
CREATE SCHEMA IF NOT EXISTS ods;
CREATE TABLE IF NOT EXISTS ods.insurance_policies (
    policy_id VARCHAR PRIMARY KEY,
    status VARCHAR,
    premium NUMERIC(10,2),
    effective_date DATE
);
```

- [ ] **Step 4: Run E2E**

```bash
docker compose up -d --build
pytest tests/integration/test_e2e_sftp_to_postgres.py -v -s
```
Expected: pass within 3 minutes.

- [ ] **Step 5: Commit**

```bash
git add airflow/dags/dag_ingest.py airflow/dags/common/connect_admin.py airflow/dags/common/kafka_admin.py tests/integration/test_e2e_sftp_to_postgres.py db/migrations/02_seed_policies.sql
git commit -m "feat(airflow): dag_ingest orchestrates stages + sink wait"
```

---

## Task 10: DQ-block + T0-mismatch + sink-failure tests

**Files:**
- Create: `tests/integration/test_dq_block_to_dlq.py`
- Create: `tests/integration/test_t0_mismatch.py`
- Create: `tests/integration/test_sink_failure.py`

- [ ] **Step 1: DQ block test**

```python
# tests/integration/test_dq_block_to_dlq.py
import time, paramiko, psycopg2, boto3, os

def _put(name, body):
    t = paramiko.Transport(('localhost', 2222)); t.connect(username='ods', password='odspass')
    s = paramiko.SFTPClient.from_transport(t)
    try: s.chdir('upload')
    except IOError: pass
    with s.file(name, 'w') as f: f.write(body)
    s.close(); t.close()

def test_null_policy_id_blocks_and_dlq(pg_conn):
    body = "policy_id,status,premium\n,ACTIVE,100.00\nP200,ACTIVE,250.00\n"
    _put('policies_20260429.csv', body)
    deadline = time.time() + 120
    failed = False
    while time.time() < deadline:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT status FROM pipeline.run_log WHERE business_date='2026-04-29' ORDER BY started_at DESC LIMIT 1")
            r = cur.fetchone()
            if r and r[0] == 'failed':
                failed = True; break
        time.sleep(3)
    assert failed
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
    objs = s3.list_objects_v2(Bucket='ods-dlq', Prefix='insurance/policies/').get('Contents', [])
    assert any(o['Size'] > 0 for o in objs)
```

- [ ] **Step 2: T0 mismatch test (forced)**

```python
# tests/integration/test_t0_mismatch.py
import uuid, psycopg2
from airflow.dags.common.recon import t0_check_publish

def test_t0_mismatch_writes_failed_recon_row(pg_conn):
    rid = str(uuid.uuid4())
    res = t0_check_publish(pg_conn, run_id=rid, domain='insurance', dataset='policies',
                           business_date='2026-04-30', source_count=100,
                           kafka_offset_start=0, kafka_offset_end=98)
    assert not res.passed
    with pg_conn.cursor() as cur:
        cur.execute("SELECT status, discrepancy_count FROM pipeline.reconciliation_log WHERE run_id=%s", (rid,))
        s, d = cur.fetchone()
    assert s == 'failed' and d == -2
```

- [ ] **Step 3: Sink-failure test (pause connector)**

```python
# tests/integration/test_sink_failure.py
import time, requests, paramiko, psycopg2

CONNECT = 'http://localhost:8083'

def _put(name, body):
    t = paramiko.Transport(('localhost', 2222)); t.connect(username='ods', password='odspass')
    s = paramiko.SFTPClient.from_transport(t)
    try: s.chdir('upload')
    except IOError: pass
    with s.file(name, 'w') as f: f.write(body)
    s.close(); t.close()

def test_paused_jdbc_blocks_run(pg_conn):
    requests.put(f'{CONNECT}/connectors/jdbc-sink-policies/pause').raise_for_status()
    try:
        body = "policy_id,status,premium\nP500,ACTIVE,1.00\n"
        _put('policies_20260501.csv', body)
        deadline = time.time() + 240
        partial = False
        while time.time() < deadline:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT status FROM pipeline.run_log WHERE business_date='2026-05-01' ORDER BY started_at DESC LIMIT 1")
                r = cur.fetchone()
                if r and r[0] == 'partial':
                    partial = True; break
            time.sleep(5)
        assert partial
    finally:
        requests.put(f'{CONNECT}/connectors/jdbc-sink-policies/resume').raise_for_status()
```

- [ ] **Step 4: Run**

`pytest tests/integration/ -v -k "dq_block or t0_mismatch or sink_failure"`
Expected: 3 passing.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_dq_block_to_dlq.py tests/integration/test_t0_mismatch.py tests/integration/test_sink_failure.py
git commit -m "test(integration): DQ block, T0 mismatch, sink failure"
```

---

## Task 11: `dag_recon_t2` (hourly cross-plane)

**Files:**
- Create: `airflow/dags/dag_recon_t2.py`
- Create: `tests/integration/test_t2_recon.py`

- [ ] **Step 1: DAG**

```python
# airflow/dags/dag_recon_t2.py
import os, pendulum, psycopg2
from airflow import DAG
from airflow.decorators import task
from common.kafka_admin import topic_offsets
from common.run_log import write_recon

PG_DSN = os.environ['PIPELINE_PG_DSN']

@task
def run_recon():
    conn = psycopg2.connect(PG_DSN)
    today = pendulum.now('UTC').to_date_string()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT domain, dataset, target_topic, postgres_target_table,
                   recon_tolerance_records, recon_tolerance_pct
            FROM pipeline.dataset_config WHERE active=TRUE AND source_type='s3_batch'
        """)
        rows = cur.fetchall()
    for domain, dataset, topic, pg_table, tol_n, tol_pct in rows:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(SUM(record_count_published), 0)
                FROM pipeline.run_log
                WHERE domain=%s AND dataset=%s AND business_date=%s AND status='succeeded'
            """, (domain, dataset, today))
            source_count = cur.fetchone()[0]
        try:
            start, end = topic_offsets(topic)
            kafka_count = end - start
        except Exception as e:
            kafka_count = None
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {pg_table}")
            pg_count = cur.fetchone()[0]

        def status(diff):
            if diff is None: return 'warning'
            if abs(diff) <= tol_n: return 'ok'
            if source_count and abs(diff)/max(source_count,1)*100.0 <= float(tol_pct): return 'ok'
            return 'failed'

        if kafka_count is not None:
            write_recon(conn, check_type='t2_source_kafka', run_id=None,
                        domain=domain, dataset=dataset, business_date=today,
                        source_count=source_count, kafka_count=kafka_count,
                        status=status(kafka_count - source_count))
        write_recon(conn, check_type='t2_kafka_postgres', run_id=None,
                    domain=domain, dataset=dataset, business_date=today,
                    source_count=source_count, kafka_count=kafka_count,
                    postgres_count=pg_count,
                    status=status((pg_count - kafka_count) if kafka_count is not None else None))
    conn.close()

with DAG(
    dag_id='dag_recon_t2',
    start_date=pendulum.datetime(2026, 4, 28, tz='UTC'),
    schedule='0 * * * *',
    catchup=False,
    tags=['ods', 'recon'],
):
    run_recon()
```

- [ ] **Step 2: Test**

```python
# tests/integration/test_t2_recon.py
import psycopg2, time
from airflow.dags.dag_recon_t2 import run_recon  # importable via PYTHONPATH

def test_t2_writes_two_rows_per_dataset(pg_conn):
    run_recon.function()
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT check_type, count(*) FROM pipeline.reconciliation_log
            WHERE created_at > NOW() - INTERVAL '5 minutes'
              AND domain='insurance' AND dataset='policies'
            GROUP BY check_type
        """)
        types = dict(cur.fetchall())
    assert types.get('t2_source_kafka', 0) >= 1
    assert types.get('t2_kafka_postgres', 0) >= 1
```

- [ ] **Step 3: Run**

`pytest tests/integration/test_t2_recon.py -v`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add airflow/dags/dag_recon_t2.py tests/integration/test_t2_recon.py
git commit -m "feat(airflow): dag_recon_t2 hourly source/kafka/postgres count check"
```

---

## Task 12: Migrate existing 9 E2E tests to new tables

**Files:**
- Modify: `tests/integration/test_policies_e2e.py`
- Modify: `tests/integration/test_ingestion.py`
- Modify: `tests/integration/test_publish.py`
- Modify: `tests/conftest.py`

- [ ] **Step 1: Replace `glue_job_log` queries with `run_log`**

In each test file, find queries like `SELECT ... FROM pipeline.glue_job_log` and rewrite to `pipeline.run_log` / `pipeline.run_stage_log`. Field mapping:
- `glue_job_log.record_count` (ingestion) → `run_log.record_count_source`
- `glue_job_log.record_count` (publish) → `run_log.record_count_published`
- `glue_job_log.error_reason` → `run_log.error_summary`
- `lineage.kafka_offset_*` → `run_log.kafka_offset_*`

- [ ] **Step 2: Run full integration suite**

```bash
docker compose down -v && docker compose up -d --build
pytest tests/integration/ -v
```
Expected: all 9 + new 6 = 15 tests passing.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_policies_e2e.py tests/integration/test_ingestion.py tests/integration/test_publish.py tests/conftest.py
git commit -m "test(migrate): existing E2E tests use new run_log/run_stage_log"
```

---

## Task 13: Grafana dashboards (visual validation)

**Files:**
- Modify: `docker/grafana/dashboards/run-health.json`
- Modify: `docker/grafana/dashboards/recon.json`

- [ ] **Step 1: Build dashboards in UI**

Browse `localhost:3000`, login admin/admin. Build two dashboards:
- **Run Health** — table of today's runs (`SELECT started_at, domain, dataset, status, record_count_source, record_count_published, kafka_offset_end - kafka_offset_start AS publish_count FROM pipeline.run_log WHERE business_date=current_date ORDER BY started_at DESC`); stat panel for failure count.
- **Reconciliation** — time-series of `discrepancy_count` per dataset+check_type; table of last 50 `failed`/`warning` rows.

Export each via "Share → Export → Save to file"; replace placeholders.

- [ ] **Step 2: Verify provisioning reload**

`docker compose restart grafana` then re-check dashboards exist under `ODS` folder.

- [ ] **Step 3: Commit**

```bash
git add docker/grafana/dashboards/
git commit -m "feat(grafana): run health + reconciliation dashboards"
```

---

## Task 14: Final verification + README

**Files:**
- Create: `docs/local-dev.md`

- [ ] **Step 1: Bring up clean stack and run all tests**

```bash
docker compose down -v
docker compose up -d --build
docker compose exec airflow-scheduler airflow dags trigger dag_config_sync
sleep 10
pytest tests/ -v
```
Expected: all tests green.

- [ ] **Step 2: Write `docs/local-dev.md`**

Cover:
- One-command bring-up: `docker compose up -d --build`.
- Drop a test file: `scp -P 2222 tests/fixtures/policies_happy.csv ods@localhost:upload/policies_20260428.csv` (password: `odspass`).
- Where to look: Airflow UI 8080, Grafana 3000, Connect 8083, Postgres 5440.
- How to query: `psql -h localhost -p 5440 -U postgres -d ods` then `SELECT * FROM pipeline.run_log ORDER BY started_at DESC LIMIT 5;`.
- DLQ: `aws --endpoint-url=http://localhost:4566 s3 ls s3://ods-dlq/insurance/policies/`.

- [ ] **Step 3: Commit**

```bash
git add docs/local-dev.md
git commit -m "docs: local-dev quickstart"
```

---

## Self-Review

Spec coverage:
- §3 architecture → Tasks 7, 8, 9 (services + DAGs).
- §4 data model → Task 1 (migration).
- §5.1 dag_drop_to_raw → Task 8.
- §5.2 dag_ingest + T0 → Tasks 6 (T0 in publish job), 9 (DAG).
- §5.3 dag_recon_t2 → Task 11.
- §5.4 dag_config_sync → Task 2.
- §6 Connect sinks → Task 7.
- §8 testing six scenarios → Tasks 8 (idempotent), 9 (e2e), 10 (DQ, T0, sink-failure), 11 (T2).
- §9 migration → Task 1.
- §11 implementation order matches plan order.

Placeholder scan: dashboard JSON is built in UI then exported (Task 13 step 1). All other steps have full code or commands.

Type consistency: `run_id` UUID throughout; `t0_check_publish` returns `T0Result` consistently; helper signatures consistent across `run_log.py` and `utils.py`.

---

**Plan complete and saved to `docs/superpowers/plans/2026-04-28-local-s3-to-postgres.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
