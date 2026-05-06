# glue/jobs/dq.py
"""
Data Quality rule evaluator for the ODS ingestion pipeline.

evaluate_dq_rules() is a pure function — it does NOT interact with Postgres,
Kafka, or any Glue context. It is imported by ods_ingestion.py and
ods_s3_publish.py after those jobs have loaded the rules dict from Postgres.

Hard-block rules:
    not_null       — row is null → fails
    unique         — duplicate key value → ALL occurrences fail
    greater_than   — value <= threshold OR null → fails
    valid_date     — to_date() returns null (unparseable / null input) → fails
    date_gte       — date in field < date in value (column or literal) → fails

Soft-warn rules:
    less_than        — count rows where value >= threshold (or null)
    not_past         — count rows where date < as_of.date()
    completeness_pct — for each field in fields list, emit one warning if
                       null fraction exceeds (1 - threshold)

DQConfigurationError is raised BEFORE any Spark action when:
    - total_count <= 0
    - a rule has an unknown rule type
    - a rule references a field absent from df.schema
    - a date_gte rule is missing value_type
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Known rule types (used for early validation)
# ---------------------------------------------------------------------------

_HARD_BLOCK_RULES = {"not_null", "unique", "greater_than", "valid_date", "date_gte"}
_SOFT_WARN_RULES  = {"less_than", "not_past", "completeness_pct"}
_ALL_RULES        = _HARD_BLOCK_RULES | _SOFT_WARN_RULES


# ---------------------------------------------------------------------------
# Public exception
# ---------------------------------------------------------------------------

class DQConfigurationError(Exception):
    """Raised when the rules dict is structurally invalid or references
    columns that do not exist in the DataFrame schema."""


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _schema_fields(df: DataFrame) -> set:
    return {f.name for f in df.schema.fields}


def _validate_rules(df: DataFrame, rules: dict, total_count: int) -> None:
    """Raise DQConfigurationError eagerly — before any Spark action."""
    if total_count <= 0:
        raise DQConfigurationError(
            f"total_count must be > 0, got {total_count}"
        )

    schema_cols = _schema_fields(df)

    for rule_cfg in rules.get("hard_blocks", []):
        _validate_single_rule(rule_cfg, schema_cols, "hard_blocks")

    for rule_cfg in rules.get("soft_warns", []):
        _validate_single_rule(rule_cfg, schema_cols, "soft_warns")


def _validate_single_rule(rule_cfg: dict, schema_cols: set, section: str) -> None:
    rule = rule_cfg.get("rule")
    if rule not in _ALL_RULES:
        raise DQConfigurationError(
            f"[{section}] unknown rule type: {rule!r}. "
            f"Known rules: {sorted(_ALL_RULES)}"
        )

    # Rules that reference a single 'field'
    if rule in {"not_null", "unique", "greater_than", "valid_date",
                "less_than", "not_past"}:
        field = rule_cfg.get("field")
        if not field:
            raise DQConfigurationError(
                f"[{section}] rule {rule!r} requires a 'field' key"
            )
        if field not in schema_cols:
            raise DQConfigurationError(
                f"[{section}] rule {rule!r} references field {field!r} "
                f"which does not exist in the DataFrame schema. "
                f"Available: {sorted(schema_cols)}"
            )

    # date_gte — validate field AND value_type AND (if column) the value column
    if rule == "date_gte":
        field = rule_cfg.get("field")
        if not field:
            raise DQConfigurationError(
                f"[{section}] date_gte requires a 'field' key"
            )
        if field not in schema_cols:
            raise DQConfigurationError(
                f"[{section}] date_gte references field {field!r} "
                f"which does not exist in the DataFrame schema. "
                f"Available: {sorted(schema_cols)}"
            )
        value_type = rule_cfg.get("value_type")
        if not value_type:
            raise DQConfigurationError(
                f"[{section}] date_gte rule is missing required 'value_type' "
                f"('literal' or 'column'). Rule: {rule_cfg!r}"
            )
        if value_type == "column":
            value_col = rule_cfg.get("value")
            if value_col and value_col not in schema_cols:
                raise DQConfigurationError(
                    f"[{section}] date_gte value column {value_col!r} "
                    f"does not exist in the DataFrame schema."
                )

    # completeness_pct — validate each field in the list
    if rule == "completeness_pct":
        for field in rule_cfg.get("fields", []):
            if field not in schema_cols:
                raise DQConfigurationError(
                    f"[{section}] completeness_pct references field {field!r} "
                    f"which does not exist in the DataFrame schema."
                )


# ---------------------------------------------------------------------------
# Hard-block condition builders
# Return a Column expression that is TRUE when the row FAILS the rule.
# ---------------------------------------------------------------------------

def _fail_condition_not_null(rule_cfg: dict) -> "Column":
    field = rule_cfg["field"]
    return F.col(field).isNull()


def _fail_condition_greater_than(rule_cfg: dict) -> "Column":
    field = rule_cfg["field"]
    threshold = rule_cfg["value"]
    return F.col(field).isNull() | (F.col(field) <= threshold)


def _fail_condition_valid_date(rule_cfg: dict) -> "Column":
    field = rule_cfg["field"]
    return F.to_date(F.col(field)).isNull()


def _fail_condition_date_gte(rule_cfg: dict) -> "Column":
    field = rule_cfg["field"]
    value_type = rule_cfg["value_type"]
    value = rule_cfg["value"]

    field_date = F.to_date(F.col(field))

    if value_type == "column":
        value_date = F.to_date(F.col(value))
        # Fails if field_date < value_date OR either is null
        return (
            field_date.isNull() |
            value_date.isNull() |
            (field_date < value_date)
        )
    else:  # literal
        # value is an ISO date string or a date object
        if isinstance(value, str):
            value_date = F.lit(value).cast("date")
        else:
            value_date = F.lit(str(value)).cast("date")
        return field_date.isNull() | (field_date < value_date)


# ---------------------------------------------------------------------------
# Fail-reason string builder  "rule_type:field:detail"
# ---------------------------------------------------------------------------

def _reason_string(rule_type: str, field: str, detail: str = "") -> str:
    return f"{rule_type}:{field}:{detail}"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def evaluate_dq_rules(
    df: DataFrame,
    rules: dict,
    total_count: int,
    as_of: Optional[datetime] = None,
) -> tuple[DataFrame, DataFrame, list[dict]]:
    """
    Evaluate data-quality rules against *df*.

    Parameters
    ----------
    df          : Input PySpark DataFrame.
    rules       : Dict with keys 'hard_blocks' and 'soft_warns'.
    total_count : Total row count denominator for completeness_pct warnings.
                  Must be > 0.
    as_of       : Reference datetime for not_past rule.
                  Defaults to datetime.utcnow() if not supplied.

    Returns
    -------
    passing_df  : Rows that passed ALL hard_block rules (same schema as df).
    failing_df  : Rows that failed ANY hard_block rule + '_dq_fail_reason'
                  column (ArrayType(StringType())).
    warnings    : List of dicts for soft_warn violations.
    """
    # -----------------------------------------------------------------------
    # 1. Validate configuration eagerly (before any Spark action)
    # -----------------------------------------------------------------------
    _validate_rules(df, rules, total_count)

    # -----------------------------------------------------------------------
    # 2. Default as_of
    # -----------------------------------------------------------------------
    if as_of is None:
        as_of = datetime.utcnow()

    # -----------------------------------------------------------------------
    # 3. Empty-DF guard — return immediately with correct schemas
    # -----------------------------------------------------------------------
    if df.isEmpty():
        # failing_df needs _dq_fail_reason; passing_df must not have it
        fail_schema_fields = df.schema.fields + [
            StructField("_dq_fail_reason", ArrayType(StringType()), True)
        ]
        fail_schema = StructType(fail_schema_fields)
        empty_failing = df.sparkSession.createDataFrame([], schema=fail_schema)
        return df, empty_failing, []

    # -----------------------------------------------------------------------
    # 4. Evaluate hard blocks
    # -----------------------------------------------------------------------
    hard_blocks = rules.get("hard_blocks", [])

    # We'll build separate failing DataFrames per rule, each tagged with its
    # reason string, then union them, group by all original columns and
    # aggregate reasons into an array.  Rows that appear in ANY failing DF
    # are excluded from passing_df.

    # Unique rule is handled specially (needs a join, not a row condition).
    # All other rules are handled with a filter condition.

    # We track a "failed" boolean column across all hard-block rules.
    # Strategy:
    #   - For each non-unique rule: compute a fail_col expression, collect
    #     failing rows with reason tagged.
    #   - For unique rule: compute via groupBy + join.
    #   - Union all failing sets → aggregate reasons → that's failing_df.
    #   - passing_df = df minus rows that appear in any failing set.

    # We'll use a row-index approach: add a monotonically_increasing_id,
    # collect failing row ids per rule, union them, then split.

    df_with_id = df.withColumn("__row_id__", F.monotonically_increasing_id()).cache()

    # Set of row IDs that fail at least one rule
    failing_id_sets: list[DataFrame] = []   # each: (__row_id__, __reason__)

    for rule_cfg in hard_blocks:
        rule = rule_cfg["rule"]

        if rule == "not_null":
            field = rule_cfg["field"]
            cond = _fail_condition_not_null(rule_cfg)
            reason = _reason_string("not_null", field, "")
            tagged = (
                df_with_id
                .filter(cond)
                .select(
                    F.col("__row_id__"),
                    F.lit(reason).alias("__reason__"),
                )
            )
            failing_id_sets.append(tagged)

        elif rule == "greater_than":
            field = rule_cfg["field"]
            cond = _fail_condition_greater_than(rule_cfg)
            reason = _reason_string("greater_than", field, f"value<={rule_cfg['value']}")
            tagged = (
                df_with_id
                .filter(cond)
                .select(
                    F.col("__row_id__"),
                    F.lit(reason).alias("__reason__"),
                )
            )
            failing_id_sets.append(tagged)

        elif rule == "valid_date":
            field = rule_cfg["field"]
            cond = _fail_condition_valid_date(rule_cfg)
            reason = _reason_string("valid_date", field, "unparseable_or_null")
            tagged = (
                df_with_id
                .filter(cond)
                .select(
                    F.col("__row_id__"),
                    F.lit(reason).alias("__reason__"),
                )
            )
            failing_id_sets.append(tagged)

        elif rule == "date_gte":
            field = rule_cfg["field"]
            value = rule_cfg["value"]
            cond = _fail_condition_date_gte(rule_cfg)
            reason = _reason_string("date_gte", field, f"not_gte_{value}")
            tagged = (
                df_with_id
                .filter(cond)
                .select(
                    F.col("__row_id__"),
                    F.lit(reason).alias("__reason__"),
                )
            )
            failing_id_sets.append(tagged)

        elif rule == "unique":
            field = rule_cfg["field"]
            reason = _reason_string("unique", field, "duplicate")
            # Find duplicate keys
            dup_keys = (
                df_with_id
                .groupBy(field)
                .agg(F.count("*").alias("__cnt__"))
                .filter(F.col("__cnt__") > 1)
                .select(field)
            )
            # Join back to get all rows with duplicate key
            tagged = (
                df_with_id
                .join(dup_keys, on=field, how="inner")
                .select(
                    F.col("__row_id__"),
                    F.lit(reason).alias("__reason__"),
                )
            )
            failing_id_sets.append(tagged)

    # -----------------------------------------------------------------------
    # 4a. Build failing_df and passing_df
    # -----------------------------------------------------------------------
    if not failing_id_sets:
        # No hard-block rules defined (or all empty) — everything passes
        passing_df = df
        # failing_df is empty with extended schema
        fail_schema = df.schema.add(
            StructField("_dq_fail_reason", ArrayType(StringType()), True)
        )
        failing_df = df.sparkSession.createDataFrame([], schema=fail_schema)
        # Proceed to soft warns
    else:
        # Union all (row_id, reason) pairs
        all_failures = failing_id_sets[0]
        for fdf in failing_id_sets[1:]:
            all_failures = all_failures.union(fdf)

        # Aggregate reasons per row_id into an array
        reasons_by_id = (
            all_failures
            .groupBy("__row_id__")
            .agg(F.collect_list("__reason__").alias("_dq_fail_reason"))
        )

        # Join back to get full row data for failing rows
        failing_df = (
            df_with_id
            .join(reasons_by_id, on="__row_id__", how="inner")
            .drop("__row_id__")
        )

        # passing_df = rows whose __row_id__ is NOT in any failing set
        failing_ids_only = reasons_by_id.select("__row_id__")
        passing_df = (
            df_with_id
            .join(failing_ids_only, on="__row_id__", how="left_anti")
            .drop("__row_id__")
        )

    df_with_id.unpersist()

    # -----------------------------------------------------------------------
    # 5. Evaluate soft warns
    # -----------------------------------------------------------------------
    warnings: list[dict] = []
    as_of_date = as_of.date() if isinstance(as_of, datetime) else as_of

    for rule_cfg in rules.get("soft_warns", []):
        rule = rule_cfg["rule"]

        if rule == "less_than":
            field = rule_cfg["field"]
            threshold = rule_cfg["value"]
            # Rows where value >= threshold OR null
            failing_count = df.filter(
                F.col(field).isNull() | (F.col(field) >= threshold)
            ).count()
            if failing_count > 0:
                warnings.append({
                    "field": field,
                    "rule": "less_than",
                    "failing_count": failing_count,
                    "total_count": total_count,
                    "message": (
                        f"{field}: {failing_count}/{total_count} rows have "
                        f"value >= {threshold} (expected < {threshold})"
                    ),
                })

        elif rule == "not_past":
            field = rule_cfg["field"]
            as_of_lit = F.lit(str(as_of_date)).cast("date")
            failing_count = df.filter(
                F.to_date(F.col(field)).isNull() |
                (F.to_date(F.col(field)) < as_of_lit)
            ).count()
            if failing_count > 0:
                warnings.append({
                    "field": field,
                    "rule": "not_past",
                    "failing_count": failing_count,
                    "total_count": total_count,
                    "message": (
                        f"{field}: {failing_count}/{total_count} rows have "
                        f"a date before as_of={as_of_date}"
                    ),
                })

        elif rule == "completeness_pct":
            threshold = rule_cfg["threshold"]   # e.g. 0.8
            max_null_pct = 1.0 - threshold       # e.g. 0.2
            for field in rule_cfg.get("fields", []):
                null_count = df.filter(F.col(field).isNull()).count()
                null_pct = null_count / total_count
                if null_pct > max_null_pct:
                    warnings.append({
                        "field": field,
                        "rule": "completeness_pct",
                        "failing_count": null_count,
                        "total_count": total_count,
                        "message": (
                            f"{field}: completeness {(1 - null_pct):.1%} is below "
                            f"threshold {threshold:.1%} "
                            f"({null_count}/{total_count} nulls)"
                        ),
                    })

    return passing_df, failing_df, warnings
