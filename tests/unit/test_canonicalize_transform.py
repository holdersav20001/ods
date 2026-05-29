"""Unit tests for glue.jobs.ods_canonicalize_file transform mechanics.

The Spark application logic in `apply_transform` is unit-tested with a real
local Spark session because the surface it exercises (withColumnRenamed,
drop, cast) cannot be faithfully covered without one. Tests stay fast by
using a tiny in-memory DataFrame.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
import textwrap

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "glue" / "jobs"))

# Import the YAML loader directly without going through the module top-
# level — that import chain pulls in pyspark / ods_pipeline which the
# unit lane does not ship. Sneak in a thin import via importlib so the
# yaml-only tests run everywhere.
import importlib.util  # noqa: E402
import types  # noqa: E402

_JOB_PATH = _REPO_ROOT / "glue" / "jobs" / "ods_canonicalize_file.py"

def _import_ocf_yaml_only():
    """Load _load_transform without triggering pyspark / ods_pipeline imports."""
    src = _JOB_PATH.read_text(encoding="utf-8")
    # Stub modules so the import doesn't fail even if pyspark missing.
    stubs = {
        "pyspark": types.ModuleType("pyspark"),
        "pyspark.sql": types.ModuleType("pyspark.sql"),
        "pyspark.sql.functions": types.ModuleType("pyspark.sql.functions"),
        "ods_pipeline": types.ModuleType("ods_pipeline"),
        "ods_ingestion_control": types.ModuleType("ods_ingestion_control"),
    }
    # ods_ingestion_control needs start_run/patch_run names available.
    stubs["ods_ingestion_control"].start_run = lambda *a, **k: None
    stubs["ods_ingestion_control"].patch_run = lambda *a, **k: None
    stubs["ods_pipeline"].connect = lambda *a, **k: None
    stubs["pyspark.sql"].SparkSession = type("SparkSession", (), {})
    stubs["pyspark.sql.functions"].col = lambda *a, **k: None
    stubs["pyspark.sql.functions"].lit = lambda *a, **k: None
    for name, mod in stubs.items():
        sys.modules.setdefault(name, mod)
    spec = importlib.util.spec_from_file_location("ods_canonicalize_file_ut", str(_JOB_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

ocf = _import_ocf_yaml_only()

# The Spark-touching tests below run only when a real pyspark distribution
# is on the runner. find_spec() can't be used here because we already
# injected a stub above for the yaml-only path. Probe via attribute.
_HAVE_SPARK = False
SparkSession = None  # type: ignore
try:
    _real = sys.modules.get("pyspark")
    if _real is not None and hasattr(_real, "__spec__") and _real.__spec__ is not None:
        from pyspark.sql import SparkSession  # type: ignore  # noqa: E402
        _HAVE_SPARK = True
except Exception:
    _HAVE_SPARK = False


requires_spark = pytest.mark.skipif(not _HAVE_SPARK, reason="pyspark not installed")


@pytest.fixture(scope="module")
def spark():
    if not _HAVE_SPARK:
        pytest.skip("pyspark not installed")
    s = (SparkSession.builder
         .appName("test_canonicalize_transform")
         .master("local[1]")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()


def test_load_transform_reads_canonical_yaml(tmp_path: Path, monkeypatch):
    domain, dataset = "synthetic", "demo_ds"
    ds_dir = tmp_path / "datasets" / domain / dataset
    ds_dir.mkdir(parents=True)
    yaml_path = ds_dir / "transform.yaml"
    yaml_path.write_text(textwrap.dedent("""
        transform_version: 7
        is_canonical: true
        renames:
            old_a: new_a
            old_b: new_b
        drops:
            - tmp_x
        casts:
            premium: decimal(10,2)
    """).strip())

    monkeypatch.setattr(ocf, "_REPO_ROOT", str(tmp_path))

    t = ocf._load_transform(domain, dataset)

    assert t["transform_version"] == 7
    assert t["is_canonical"] is True
    assert t["renames"] == {"old_a": "new_a", "old_b": "new_b"}
    assert t["drops"] == ["tmp_x"]
    assert t["casts"] == {"premium": "decimal(10,2)"}
    assert t["raw_path"].endswith("transform.yaml")


def test_load_transform_missing_file_raises(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(ocf, "_REPO_ROOT", str(tmp_path))
    with pytest.raises(FileNotFoundError):
        ocf._load_transform("nope", "absent")


def test_load_transform_defaults_for_empty_yaml(tmp_path: Path, monkeypatch):
    domain, dataset = "synthetic", "empty_ds"
    ds_dir = tmp_path / "datasets" / domain / dataset
    ds_dir.mkdir(parents=True)
    (ds_dir / "transform.yaml").write_text("")  # explicitly empty
    monkeypatch.setattr(ocf, "_REPO_ROOT", str(tmp_path))

    t = ocf._load_transform(domain, dataset)
    assert t == {
        "transform_version": 1,
        "is_canonical": True,
        "renames": {},
        "drops": [],
        "casts": {},
        "raw_path": str(ds_dir / "transform.yaml"),
    }


@requires_spark
def test_apply_transform_renames(spark):
    df = spark.createDataFrame(
        [("POL1", 100.0), ("POL2", 200.0)],
        schema="policy_id string, premium double",
    )
    transform = {"renames": {"policy_id": "policy_number"},
                 "drops": [], "casts": {}}
    out, actions = ocf.apply_transform(df, transform)
    assert "policy_number" in out.columns
    assert "policy_id" not in out.columns
    assert {a["op"] for a in actions} == {"rename"}
    assert actions[0] == {"op": "rename", "from": "policy_id", "to": "policy_number"}


@requires_spark
def test_apply_transform_drops(spark):
    df = spark.createDataFrame(
        [("POL1", "ACTIVE", "marker")],
        schema="policy_id string, status string, tmp_x string",
    )
    transform = {"renames": {}, "drops": ["tmp_x"], "casts": {}}
    out, actions = ocf.apply_transform(df, transform)
    assert "tmp_x" not in out.columns
    assert actions == [{"op": "drop", "column": "tmp_x"}]


@requires_spark
def test_apply_transform_casts(spark):
    df = spark.createDataFrame(
        [("POL1", "100.123")],
        schema="policy_id string, premium string",
    )
    transform = {"renames": {}, "drops": [],
                 "casts": {"premium": "decimal(10,2)"}}
    out, actions = ocf.apply_transform(df, transform)
    dtype = dict(out.dtypes)["premium"]
    assert dtype == "decimal(10,2)"
    assert actions == [{"op": "cast", "column": "premium", "type": "decimal(10,2)"}]


@requires_spark
def test_apply_transform_combined_order(spark):
    """Renames first; subsequent drops/casts must target post-rename names."""
    df = spark.createDataFrame(
        [("POL1", "100.123", "noise")],
        schema="policy_id string, premium string, tmp_x string",
    )
    transform = {
        "renames": {"policy_id": "policy_number"},
        "drops":   ["tmp_x"],
        "casts":   {"premium": "decimal(10,2)"},
    }
    out, actions = ocf.apply_transform(df, transform)
    assert set(out.columns) == {"policy_number", "premium"}
    ops = [a["op"] for a in actions]
    assert ops == ["rename", "drop", "cast"]


@requires_spark
def test_apply_transform_unknown_columns_silently_skipped(spark):
    df = spark.createDataFrame([("a",)], schema="x string")
    transform = {"renames": {"missing": "also_missing"},
                 "drops": ["never_here"],
                 "casts": {"phantom": "int"}}
    out, actions = ocf.apply_transform(df, transform)
    assert out.columns == ["x"]
    assert actions == []
