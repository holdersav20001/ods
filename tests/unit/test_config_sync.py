import json, pytest
import sys, os
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, '..', '..', 'airflow', 'dags')))
from common.yaml_loader import (
    compute_hash,
    discover_dataset_yaml_paths,
    load_dataset_yaml,
    sync_to_db,
)

POLICIES_YAML = """
domain: testdomain
dataset: testdataset
source_type: s3_batch
filename_pattern: '^policies_(?P<bd>\\\\d{8})\\\\.csv$'
key_fields: [policy_id]
target_topic: ods.testdomain.testdataset
canonical_topic: ods.testdomain.testdataset-canonical
canonical_schema_id: ods.testdomain.testdataset-canonical-value
transform_yaml_path: /home/glue_user/patterns/testdomain/testdataset.yaml
is_canonical: false
postgres_target_table: ods.testdomain_testdataset
s3_curated_path: s3://ods-curated/testdomain/testdataset/
schema_def:
  fields:
    - {name: policy_id, type: string}
    - {name: status,    type: string}
    - {name: premium,   type: 'decimal(10,2)'}
dq_rules:
  hard_blocks:
    - {rule: not_null, field: policy_id}
  soft_warns: []
recon_tolerance_records: 0
recon_tolerance_pct: 0
"""

def test_load_yaml(tmp_path):
    p = tmp_path / "policies.yaml"
    p.write_text(POLICIES_YAML)
    cfg = load_dataset_yaml(str(p))
    assert cfg['domain'] == 'testdomain'
    assert cfg['dataset'] == 'testdataset'
    assert cfg['key_fields'] == ['policy_id']

def test_hash_is_deterministic():
    h1 = compute_hash({'a': 1, 'b': [1, 2]})
    h2 = compute_hash({'b': [1, 2], 'a': 1})
    assert h1 == h2 and len(h1) == 64

def test_load_split_dataset_yaml(tmp_path):
    root = tmp_path / "policies"
    root.mkdir()
    (root / "dataset.yaml").write_text("""
domain: testdomain
dataset: splitdataset
source_type: s3_batch
delivery: file_pipeline
raw_format: csv
filename_pattern: '^split_(?P<bd>\\\\d{8})\\\\.csv$'
""")
    (root / "contract.yaml").write_text("""
schema_id: ods.testdomain.splitdataset-value
schema_version: 1
key_fields: [policy_id]
schema_def:
  fields:
    - {name: policy_id, type: string}
""")
    (root / "quality.yaml").write_text("""
hard_blocks:
  - {rule: not_null, field: policy_id}
soft_warns: []
""")
    (root / "delivery.yaml").write_text("""
write_mode: upsert
target_topic: ods.testdomain.splitdataset
postgres_target_table: ods.testdomain_splitdataset
""")
    (root / "reconciliation.yaml").write_text("""
tolerances:
  records: 0
  pct: 0
checks:
  - name: t0_ingestion_count
""")

    cfg = load_dataset_yaml(str(root / "dataset.yaml"))

    assert cfg["domain"] == "testdomain"
    assert cfg["key_fields"] == ["policy_id"]
    assert cfg["dq_rules"] == {
        "hard_blocks": [{"rule": "not_null", "field": "policy_id"}],
        "soft_warns": [],
    }
    assert cfg["recon_tolerance_records"] == 0
    assert cfg["reconciliation"]["checks"] == [{"name": "t0_ingestion_count"}]

def test_discover_dataset_yaml_paths_skips_split_components(tmp_path):
    root = tmp_path / "datasets" / "insurance" / "policies"
    root.mkdir(parents=True)
    (root / "dataset.yaml").write_text("domain: insurance\ndataset: policies\n")
    (root / "quality.yaml").write_text("hard_blocks: []\n")
    legacy = tmp_path / "datasets" / "insurance" / "legacy.yaml"
    legacy.write_text("domain: insurance\ndataset: legacy\n")

    paths = [os.path.relpath(p, tmp_path) for p in discover_dataset_yaml_paths(str(tmp_path))]

    assert paths == [
        os.path.join("datasets", "insurance", "legacy.yaml"),
        os.path.join("datasets", "insurance", "policies", "dataset.yaml"),
    ]

def test_sync_inserts_then_bumps_version(pg_conn, tmp_path):
    # Clean any prior state for this isolated test dataset.
    def _cleanup():
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline.file_catalogue "
                "WHERE domain='testdomain' AND dataset='testdataset'"
            )
            cur.execute("DELETE FROM pipeline.dataset_config WHERE domain='testdomain' AND dataset='testdataset'")
        pg_conn.commit()

    _cleanup()
    try:
        p = tmp_path / "policies.yaml"
        p.write_text(POLICIES_YAML)
        sync_to_db(str(p), pg_conn)
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT config_version_id, config_yaml_hash, is_canonical, "
                "canonical_topic, canonical_schema_id, transform_yaml_path "
                "FROM pipeline.dataset_config "
                "WHERE domain='testdomain' AND dataset='testdataset'"
            )
            v1, h1, is_canonical, canonical_topic, canonical_schema_id, transform_yaml_path = cur.fetchone()
        assert is_canonical is False
        assert canonical_topic == "ods.testdomain.testdataset-canonical"
        assert canonical_schema_id == "ods.testdomain.testdataset-canonical-value"
        assert transform_yaml_path == "/home/glue_user/patterns/testdomain/testdataset.yaml"

        # Identical content -> no version bump.
        sync_to_db(str(p), pg_conn)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT config_version_id FROM pipeline.dataset_config WHERE domain='testdomain' AND dataset='testdataset'")
            (v2,) = cur.fetchone()
        assert v2 == v1

        # Changed content -> bump.
        p.write_text(POLICIES_YAML.replace('recon_tolerance_records: 0', 'recon_tolerance_records: 5'))
        sync_to_db(str(p), pg_conn)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT config_version_id, recon_tolerance_records FROM pipeline.dataset_config WHERE domain='testdomain' AND dataset='testdataset'")
            v3, tol = cur.fetchone()
        assert v3 == v1 + 1
        assert tol == 5
    finally:
        _cleanup()
