import json, pytest
import sys, os
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, '..', '..', 'airflow', 'dags')))
from common.yaml_loader import load_dataset_yaml, compute_hash, sync_to_db

POLICIES_YAML = """
domain: insurance
dataset: policies
source_type: s3_batch
filename_pattern: '^policies_(?P<bd>\\\\d{8})\\\\.csv$'
key_fields: [policy_id]
target_topic: ods.insurance.policies
postgres_target_table: ods.insurance_policies
s3_curated_path: s3://ods-curated/insurance/policies/
schema_def:
  fields:
    - {name: policy_id, type: string}
    - {name: status,    type: string}
    - {name: premium,   type: 'decimal(10,2)'}
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
    # Clean any prior state for this dataset.
    # The deprecated file_catalogue table has a FK to dataset_config.id; delete dependents first.
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pipeline.file_catalogue_deprecated_2026_04_28 WHERE dataset_config_id IN "
            "(SELECT id FROM pipeline.dataset_config WHERE domain='insurance' AND dataset='policies')"
        )
        cur.execute("DELETE FROM pipeline.dataset_config WHERE domain='insurance' AND dataset='policies'")
    pg_conn.commit()

    p = tmp_path / "policies.yaml"
    p.write_text(POLICIES_YAML)
    sync_to_db(str(p), pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT config_version_id, config_yaml_hash FROM pipeline.dataset_config WHERE domain='insurance' AND dataset='policies'")
        v1, h1 = cur.fetchone()

    # Identical content -> no version bump.
    sync_to_db(str(p), pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT config_version_id FROM pipeline.dataset_config WHERE domain='insurance' AND dataset='policies'")
        (v2,) = cur.fetchone()
    assert v2 == v1

    # Changed content -> bump.
    p.write_text(POLICIES_YAML.replace('recon_tolerance_records: 0', 'recon_tolerance_records: 5'))
    sync_to_db(str(p), pg_conn)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT config_version_id, recon_tolerance_records FROM pipeline.dataset_config WHERE domain='insurance' AND dataset='policies'")
        v3, tol = cur.fetchone()
    assert v3 == v1 + 1
    assert tol == 5
