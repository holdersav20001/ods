"""Declarative non-canonical -> canonical transform helpers.

The pure helpers in this module are intentionally testable without Spark. The
runtime ``apply_transform`` function imports PySpark lazily so local unit tests
can validate mapping compilation on machines that do not have Spark installed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


def load_mapping(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    transform = cfg.get("transform", cfg)
    if not isinstance(transform, dict):
        raise ValueError(f"transform mapping in {path} must be an object")
    return transform


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _spark_type(field: dict[str, Any]) -> str:
    typ = str(field.get("type", "string")).lower()
    if typ == "decimal":
        precision = int(field.get("precision", 18))
        scale = int(field.get("scale", 2))
        return f"decimal({precision},{scale})"
    return {
        "str": "string",
        "string": "string",
        "int": "int",
        "integer": "int",
        "long": "bigint",
        "float": "float",
        "double": "double",
        "bool": "boolean",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
    }.get(typ, typ)


def _source_expr(field: dict[str, Any], available_columns: set[str] | None) -> str:
    source = field.get("source")
    if source:
        if available_columns is not None and source not in available_columns:
            return "NULL"
        expr = _quote(str(source))
    elif "default" in field:
        expr = _literal(field.get("default"))
    else:
        expr = "NULL"

    typ = str(field.get("type", "string")).lower()
    if typ == "date" and field.get("format") and expr != "NULL":
        return f"to_date({expr}, '{field['format']}')"
    return f"cast({expr} as {_spark_type(field)})"


def compile_transform(
    mapping: dict[str, Any],
    available_columns: set[str] | None = None,
) -> tuple[list[str], list[str], list[str]]:
    """Compile mapping YAML to Spark SQL select expressions.

    Returns ``(select_exprs, required_targets, warnings)``.
    """
    fields = mapping.get("fields", [])
    if not isinstance(fields, list) or not fields:
        raise ValueError("transform.fields must be a non-empty list")

    select_exprs: list[str] = []
    required_targets: set[str] = set(mapping.get("required", []) or [])
    warnings: list[str] = []

    for field in fields:
        if "target" not in field:
            raise ValueError(f"transform field missing target: {field}")
        target = str(field["target"])
        source = field.get("source")
        if source and available_columns is not None and source not in available_columns:
            warnings.append(f"source column missing: {source}")
        if field.get("required"):
            required_targets.add(target)
        select_exprs.append(f"{_source_expr(field, available_columns)} AS {_quote(target)}")

    for derived in mapping.get("derived", []) or []:
        target = derived.get("target")
        expr = derived.get("expr")
        if not target or not expr:
            raise ValueError(f"derived transform missing target/expr: {derived}")
        select_exprs.append(f"{expr} AS {_quote(str(target))}")

    return select_exprs, sorted(required_targets), warnings


def apply_transform(df, mapping: dict[str, Any]):
    """Apply a transform mapping with DataFrame-native operations.

    Returns ``(pass_df, fail_df, warnings)``. Required-field failures are
    separated into ``fail_df`` with ``_ods_error_reason`` populated.
    """
    from functools import reduce

    from pyspark.sql import functions as F

    field_mapping = {
        "fields": mapping.get("fields", []),
        "required": mapping.get("required", []),
    }
    select_exprs, required_targets, warnings = compile_transform(
        field_mapping,
        available_columns=set(df.columns),
    )
    transformed = df.selectExpr(*select_exprs)
    for derived in mapping.get("derived", []) or []:
        transformed = transformed.withColumn(str(derived["target"]), F.expr(derived["expr"]))

    if not required_targets:
        return transformed, transformed.limit(0), warnings

    required_checks = [F.col(c).isNull() for c in required_targets]
    fail_condition = reduce(lambda left, right: left | right, required_checks)
    missing_names = F.array_remove(
        F.array(*[
            F.when(F.col(c).isNull(), F.lit(c)).otherwise(F.lit(None))
            for c in required_targets
        ]),
        None,
    )
    fail_df = (
        transformed
        .filter(fail_condition)
        .withColumn("_ods_error_reason",
                    F.concat(F.lit("missing required canonical fields: "),
                             F.to_json(missing_names)))
    )
    pass_df = transformed.filter(~fail_condition)
    return pass_df, fail_df, warnings


def mapping_summary(path: str) -> str:
    mapping = load_mapping(path)
    exprs, required, warnings = compile_transform(mapping)
    return json.dumps(
        {"select_exprs": exprs, "required": required, "warnings": warnings},
        indent=2,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    args = parser.parse_args()
    print(mapping_summary(str(Path(args.path))))
