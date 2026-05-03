"""Pattern registration sanity checks for ``insurance.api_pull_demo``."""
from __future__ import annotations

from pathlib import Path

import yaml

from ods_pipeline.models import PATTERN_CORRELATION_FIELD, PatternType, Stage
from ods_pipeline.patterns import PATTERNS, get


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_api_pull_demo_registered():
    pattern = get("insurance.api_pull_demo")
    assert pattern.pattern_type == PatternType.API
    assert pattern in PATTERNS.values()


def test_correlation_field_is_source_request_id():
    pattern = get("insurance.api_pull_demo")
    assert pattern.correlation_field == PATTERN_CORRELATION_FIELD[PatternType.API]
    assert pattern.correlation_field == "_ods_source_request_id"


def test_stages_include_raw_poll_and_message_archive():
    pattern = get("insurance.api_pull_demo")
    assert Stage.RAW_POLL in pattern.stages
    assert Stage.MESSAGE_ARCHIVE in pattern.stages
    assert Stage.RECON_MESSAGE in pattern.stages
    assert Stage.FINALISE in pattern.stages


def test_topics_and_sinks_present():
    pattern = get("insurance.api_pull_demo")
    assert pattern.topics
    assert pattern.sinks


def test_recon_check_is_api_pull_archive_count():
    pattern = get("insurance.api_pull_demo")
    assert "api_pull_archive_count" in pattern.recon_checks


def test_yaml_config_path_exists_and_round_trips():
    pattern = get("insurance.api_pull_demo")
    yaml_path = REPO_ROOT / pattern.yaml_config
    assert yaml_path.exists(), yaml_path
    cfg = yaml.safe_load(yaml_path.read_text())
    assert cfg["domain"] == "insurance"
    assert cfg["dataset"] == "api_pull_demo"
    assert cfg["source_type"] == "api_pull"
    assert cfg["raw_format"] == "jsonl"
    assert cfg["source"]["auth"]["type"] == "bearer"
    assert cfg["source"]["cursor"]["style"] == "since_timestamp"


def test_raw_poll_stage_is_in_stage_enum():
    assert Stage.RAW_POLL == "raw_poll"
    assert "raw_poll" in Stage.all_values()
