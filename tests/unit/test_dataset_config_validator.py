"""Unit tests for ods_pipeline.config.validate_dataset_config.

The validator is the single source of truth for dataset_config row
invariants. yaml_loader calls it before INSERT; future ``odscli
config validate`` will call it for local lint. These tests pin the
contract so accidental relaxations show up as failing tests instead
of silently shipped breakage.
"""
from __future__ import annotations

import pytest

from ods_pipeline.config import DatasetConfigError, validate_dataset_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _file_pipeline_min() -> dict:
    return {
        "domain": "insurance",
        "dataset": "policies",
        "source_type": "s3_batch",
        "delivery": "file_pipeline",
        "filename_pattern": r"^policies_(?P<bd>\d{8})\.csv$",
        "target_topic": "ods.insurance.policies",
        "is_canonical": True,
        "write_mode": "upsert",
        "key_fields": ["policy_id"],
    }


def _api_pull_min() -> dict:
    return {
        "domain": "insurance",
        "dataset": "api_pull_demo",
        "source_type": "api_pull",
        "delivery": "file_pipeline",
        "target_topic": "ods.insurance.api_pull_demo",
        "is_canonical": True,
        "write_mode": "upsert",
        "key_fields": ["request_id"],
        "source": {
            "url": "https://api.example/items",
            "auth": {"type": "bearer", "secret_ref": "API_TOKEN"},
            "cursor": {"style": "since_timestamp"},
        },
    }


def _direct_postgres_min() -> dict:
    return {
        "domain": "insurance",
        "dataset": "lookup_country",
        "source_type": "s3_batch",
        "delivery": "direct_postgres",
        "filename_pattern": r"^country_(?P<bd>\d{8})\.csv$",
        "is_canonical": True,
        "write_mode": "upsert",
        "key_fields": ["country_code"],
    }


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_file_pipeline_minimum_passes():
    validate_dataset_config(_file_pipeline_min())


def test_api_pull_minimum_passes():
    validate_dataset_config(_api_pull_min())


def test_direct_postgres_minimum_passes():
    validate_dataset_config(_direct_postgres_min())


def test_append_mode_without_key_fields_passes():
    cfg = _file_pipeline_min()
    cfg["write_mode"] = "append"
    cfg["key_fields"] = []
    validate_dataset_config(cfg)


# ---------------------------------------------------------------------------
# Required identity / shape
# ---------------------------------------------------------------------------


def test_missing_domain_or_dataset_raises():
    with pytest.raises(DatasetConfigError, match="non-empty 'domain' and 'dataset'"):
        validate_dataset_config({"dataset": "x"})
    with pytest.raises(DatasetConfigError, match="non-empty 'domain' and 'dataset'"):
        validate_dataset_config({"domain": "x"})


def test_non_mapping_input_raises():
    with pytest.raises(DatasetConfigError, match="must be a mapping"):
        validate_dataset_config(["not", "a", "dict"])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Enum domains
# ---------------------------------------------------------------------------


def test_unknown_source_type_raises():
    cfg = _file_pipeline_min()
    cfg["source_type"] = "carrier_pigeon"
    with pytest.raises(DatasetConfigError, match="unknown source_type"):
        validate_dataset_config(cfg)


def test_unknown_delivery_raises():
    cfg = _file_pipeline_min()
    cfg["delivery"] = "smoke_signal"
    with pytest.raises(DatasetConfigError, match="unknown delivery"):
        validate_dataset_config(cfg)


def test_unknown_write_mode_raises():
    cfg = _file_pipeline_min()
    cfg["write_mode"] = "merge_with_prejudice"
    with pytest.raises(DatasetConfigError, match="unknown write_mode"):
        validate_dataset_config(cfg)


# ---------------------------------------------------------------------------
# source_type ↔ filename_pattern coherence
# ---------------------------------------------------------------------------


def test_s3_batch_without_filename_pattern_raises():
    cfg = _file_pipeline_min()
    cfg.pop("filename_pattern")
    with pytest.raises(DatasetConfigError, match="filename_pattern"):
        validate_dataset_config(cfg)


def test_api_pull_with_filename_pattern_raises():
    cfg = _api_pull_min()
    cfg["filename_pattern"] = r"^anything\.csv$"
    with pytest.raises(DatasetConfigError, match="filename_pattern only applies"):
        validate_dataset_config(cfg)


# ---------------------------------------------------------------------------
# delivery ↔ topic coherence
# ---------------------------------------------------------------------------


def test_file_pipeline_without_target_topic_raises():
    cfg = _file_pipeline_min()
    cfg.pop("target_topic")
    with pytest.raises(DatasetConfigError, match="requires target_topic"):
        validate_dataset_config(cfg)


def test_direct_kafka_without_target_topic_raises():
    cfg = _file_pipeline_min()
    cfg["delivery"] = "direct_kafka"
    cfg["source_type"] = "api_pull"
    cfg.pop("filename_pattern")
    cfg["source"] = _api_pull_min()["source"]
    cfg.pop("target_topic")
    with pytest.raises(DatasetConfigError, match="requires target_topic"):
        validate_dataset_config(cfg)


def test_direct_postgres_with_target_topic_raises():
    cfg = _direct_postgres_min()
    cfg["target_topic"] = "ods.something"
    with pytest.raises(DatasetConfigError, match="must not set target_topic"):
        validate_dataset_config(cfg)


def test_direct_postgres_with_canonical_topic_raises():
    cfg = _direct_postgres_min()
    cfg["canonical_topic"] = "ods.something-canonical"
    with pytest.raises(DatasetConfigError, match="must not set canonical_topic"):
        validate_dataset_config(cfg)


# ---------------------------------------------------------------------------
# Canonicalize requirements
# ---------------------------------------------------------------------------


def test_non_canonical_without_transform_yaml_raises():
    cfg = _file_pipeline_min()
    cfg["is_canonical"] = False
    with pytest.raises(DatasetConfigError, match="transform_yaml_path"):
        validate_dataset_config(cfg)


def test_non_canonical_with_transform_yaml_passes():
    cfg = _file_pipeline_min()
    cfg["is_canonical"] = False
    cfg["transform_yaml_path"] = "/home/glue_user/patterns/insurance/policies.yaml"
    cfg["canonical_topic"] = "ods.insurance.policies-canonical"
    validate_dataset_config(cfg)


# ---------------------------------------------------------------------------
# write_mode ↔ key_fields coherence
# ---------------------------------------------------------------------------


def test_upsert_without_key_fields_raises():
    cfg = _file_pipeline_min()
    cfg["key_fields"] = []
    with pytest.raises(DatasetConfigError, match="upsert.*requires non-empty key_fields"):
        validate_dataset_config(cfg)


def test_replace_without_key_fields_raises():
    cfg = _file_pipeline_min()
    cfg["write_mode"] = "replace"
    cfg["key_fields"] = []
    with pytest.raises(DatasetConfigError, match="replace.*requires non-empty key_fields"):
        validate_dataset_config(cfg)


# ---------------------------------------------------------------------------
# api_pull source-block requirements
# ---------------------------------------------------------------------------


def test_api_pull_missing_source_block_raises():
    cfg = _api_pull_min()
    cfg.pop("source")
    with pytest.raises(DatasetConfigError, match="requires source.url"):
        validate_dataset_config(cfg)


def test_api_pull_missing_source_url_raises():
    cfg = _api_pull_min()
    cfg["source"].pop("url")
    with pytest.raises(DatasetConfigError, match="requires source.url"):
        validate_dataset_config(cfg)


def test_api_pull_missing_cursor_style_raises():
    cfg = _api_pull_min()
    cfg["source"]["cursor"] = {}
    with pytest.raises(DatasetConfigError, match="requires source.cursor.style"):
        validate_dataset_config(cfg)


def test_api_pull_unknown_auth_type_raises():
    cfg = _api_pull_min()
    cfg["source"]["auth"] = {"type": "carrier_pigeon"}
    with pytest.raises(DatasetConfigError, match="unknown source.auth.type"):
        validate_dataset_config(cfg)


def test_api_pull_bearer_without_secret_ref_raises():
    cfg = _api_pull_min()
    cfg["source"]["auth"] = {"type": "bearer"}
    with pytest.raises(DatasetConfigError, match="bearer.*requires secret_ref"):
        validate_dataset_config(cfg)


@pytest.mark.parametrize("forbidden", ["token", "password", "client_secret", "api_key"])
def test_api_pull_inlined_secret_value_raises(forbidden: str):
    cfg = _api_pull_min()
    cfg["source"]["auth"][forbidden] = "literal-secret-NEVER-DO-THIS"
    with pytest.raises(DatasetConfigError, match="must not contain"):
        validate_dataset_config(cfg)
