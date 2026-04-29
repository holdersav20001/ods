# Local Development — S3-to-Postgres Pipeline

End-to-end local stack that mirrors the production ODS pipeline:
**SFTP → S3 (LocalStack) → Glue ingest → curated parquet → Confluent Kafka → JDBC sink → Postgres**, with a parallel **S3 sink** writing partitioned parquet to a curated bucket. Airflow orchestrates; Postgres holds run/lineage/recon state; Grafana provides observability.

> **Critical:** Run from the WSL2 filesystem (e.g. `~/aviva-ods`), **not** a Windows path (`/mnt/c/...`). Bind mounts on Windows NTFS go through the WSL2 p9 layer and are 5–10× slower; Airflow's DAG scanner will time out.

---

## Components

| Service | Port (host) | Purpose |
|---|---|---|
| `localstack` | 4566 | S3 (`ods-raw-local`, `ods-curated-local`) |
| `zookeeper` | — | Kafka coordination |
| `broker` | 9092 | Confluent Kafka |
| `schema-registry` | 8081 | Avro schemas |
| `postgres` | 5440 | `ods_dev` db, schemas: `pipeline`, `ods`, `airflow` |
| `airflow-scheduler` / `airflow-webserver` | 8080 | DAG orchestration |
| `glue` | — | `ods-glue:local` image, run by DockerOperator |
| `kafka-connect` | 8083 | JDBC + S3 sinks |
| `connect-bootstrap` | — | One-shot, registers connectors on startup |
| `sftp` | 2222 | atmoz/sftp, user `ods`/`odspass`, drop dir `/upload` |
| `grafana` | 3000 | Dashboards (admin/admin) |

---

## Bring up

```bash
cd ~/aviva-ods                          # WSL path
docker compose build kafka-connect      # one-time, ~15s
docker compose up -d
```

Wait until `docker compose ps` shows `(healthy)` for `postgres`, `broker`, `schema-registry`, `kafka-connect`, `localstack`, `airflow-{scheduler,webserver}`. The `connect-bootstrap` sidecar exits with code 0 once both connectors are registered.

Verify:
```bash
curl -s localhost:8083/connectors
# ["jdbc-sink-policies","s3-sink-policies"]
```

Register the dataset Avro schema (one-time per fresh stack):
```bash
SCHEMA='{"type":"record","name":"Policy","namespace":"com.aviva.ods.insurance","fields":[
  {"name":"policy_id","type":"string"},
  {"name":"status","type":"string"},
  {"name":"premium","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}],"default":null},
  {"name":"effective_date","type":["null",{"type":"int","logicalType":"date"}],"default":null},
  {"name":"_ods_business_date","type":"string"},
  {"name":"_ods_run_id","type":"string"}
]}'
PAYLOAD=$(python -c "import json,sys; print(json.dumps({'schema':sys.argv[1],'schemaType':'AVRO'}))" "$SCHEMA")
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data "$PAYLOAD" \
  http://localhost:8081/subjects/ods.insurance.policies-value/versions
```

Unpause DAGs:
```bash
docker compose exec airflow-scheduler airflow dags unpause dag_drop_to_raw
docker compose exec airflow-scheduler airflow dags unpause dag_ingest
docker compose exec airflow-scheduler airflow dags unpause dag_recon_t2
```

---

## Run integration tests

From the project root with `pytest`, `paramiko`, `boto3`, `requests`, `psycopg2-binary` installed locally:

```bash
python -m pytest tests/integration/test_idempotent_drop.py tests/integration/test_e2e_sftp_to_postgres.py tests/integration/test_dr_resume.py tests/integration/test_negative_paths.py -v
```

Expected:
| Test | Outcome |
|---|---|
| `test_same_file_twice_yields_single_catalogue_row` | PASS (≈18 s) — md5-based idempotency |
| `test_drop_file_lands_in_postgres` | PASS (≈30 s) — full pipeline |
| `test_failed_run_resumes_cleanly` | PASS (≈40 s) — DR audit trail preserved |
| `test_dq_block_records_failure` | PASS (≈70 s) — duplicate PK fails DQ |
| `test_sink_failure_marks_run_partial` | XFAIL — known DAG state-interaction bug |

---

## Capture evidence

```bash
python scripts/capture_evidence.py
ls docs/evidence/
# dr.txt  lineage.txt  observability.txt  reconciliation.txt
```

`lineage.txt` contains the per-row trace: `policy_id → run_id → kafka offsets → file_md5 → SFTP/S3 raw path`.

---

## Key tables

```sql
-- Pipeline observability
SELECT * FROM pipeline.run_log         ORDER BY started_at DESC LIMIT 10;
SELECT * FROM pipeline.run_stage_log   ORDER BY started_at DESC LIMIT 30;
SELECT * FROM pipeline.reconciliation_log ORDER BY created_at DESC LIMIT 20;
SELECT * FROM pipeline.file_catalogue  ORDER BY first_seen_at DESC LIMIT 10;

-- Target rows + lineage
SELECT policy_id, _ods_run_id, _ods_business_date, _ods_ingested_at
  FROM ods.insurance_policies;
```

Per-row lineage join:
```sql
SELECT p.policy_id, p._ods_business_date, r.status, r.kafka_topic,
       r.kafka_offset_start || '..' || r.kafka_offset_end AS offsets,
       f.file_md5, f.sftp_path, f.s3_raw_path
  FROM ods.insurance_policies p
  JOIN pipeline.run_log r        ON r.run_id::text = p._ods_run_id
  JOIN pipeline.file_catalogue f ON f.file_id    = r.file_id;
```

---

## Grafana

Browse to <http://localhost:3000> (admin / admin). Two pre-provisioned dashboards under the `ODS` folder:

- **ODS — Run Health**: today's runs by status; recent 50 runs with kafka offsets and counts
- **ODS — Reconciliation**: latest 200 T0 + T2 recon rows with discrepancy detail

Both dashboards bind to the auto-provisioned `ODS Postgres` datasource.

---

## Reconciliation

| Check | Where it runs | What it compares |
|---|---|---|
| **T0** `t0_publish_count` | Inline in `ods_s3_publish.py` after produce | `record_count_source` vs `kafka_offset_end - kafka_offset_start` |
| **T2** `t2_full` | `dag_recon_t2`, hourly | source vs kafka delta vs `count(target WHERE _ods_run_id=run_id AND _ods_business_date=bd)` |

Trigger T2 manually:
```bash
docker compose exec airflow-scheduler airflow dags trigger dag_recon_t2
```

---

## Disaster Recovery

`tests/integration/test_dr_resume.py` simulates a crashed run mid-publish:
1. Inject a synthetic `failed` row in `run_log` with `error_summary='simulated crash mid-publish'`.
2. Drop a recovery file via SFTP.
3. Wait for the row to appear in `ods.insurance_policies`.
4. Verify the failed row remains as an audit trail and a fresh succeeded run was produced for the same `business_date`.

The pipeline never overwrites a failed run — every run is a new `run_id` so DR is provable from `pipeline.run_log` alone.

---

## Tear down

```bash
docker compose down -v          # drops all data volumes
```

---

## Known follow-ups

- Sink-failure path: when ods_ingestion takes the idempotent-skip branch, run_log lands `status=succeeded` before sink wait completes; if the sink later fails, `wait_sinks` doesn't downgrade. Workaround: re-test on a clean stack. Real fix: have `wait_sinks` call `update_run_header(status='partial')` unconditionally when sinks don't advance, regardless of prior status.
- Legacy `test_policies_e2e.py` / `test_ingestion.py` / `test_publish.py` still query `pipeline.glue_job_log` and `pipeline.lineage` — migrate to `run_log` / `run_stage_log` (Plan task 12).
