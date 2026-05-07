"""DQ rules + DLQ write for the ingestion pipeline.

The DQ engine itself lives in ``glue/jobs/dq.py`` (kept untouched so
existing callers are unaffected). This module orchestrates:

* parsing the configured rules (jsonb or already-deserialized);
* delegating row-level evaluation to :func:`evaluate_dq_rules`;
* writing the failing rows to the DLQ S3 bucket;
* classifying the outcome as ``succeeded`` / ``warned`` / ``failed``.

Returns a small, picklable :class:`DQOutcome` so the caller can decide
how to flip stage_scope (succeeded → no method, warned → ``s.warn``,
failed → re-raise via :class:`DQAllRowsFailed`).
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


class DQAllRowsFailed(RuntimeError):
    """Raised when every input row fails DQ — nothing to curate."""


@dataclass(frozen=True)
class DQOutcome:
    passing_df: Any
    failing_count: int
    warnings: list


def _parse_rules(rules: Any) -> dict:
    if isinstance(rules, dict):
        return rules
    return json.loads(rules)


def _dlq_path(domain: str, dataset: str, business_date: str, run_id: str) -> str:
    env = os.environ.get("ENV", "local")
    return (
        f"s3a://ods-dlq-{env}/{domain}/{dataset}"
        f"/date={business_date}/run_id={run_id}/failed.csv"
    )


def evaluate(
    df: Any,
    *,
    config_dq_rules: Any,
    source_count: int,
    domain: str,
    dataset: str,
    business_date: str,
    run_id: str,
) -> DQOutcome:
    """Run DQ rules; write failing rows to DLQ; return the outcome.

    Raises :class:`DQAllRowsFailed` when the entire input is rejected
    (nothing to curate). Other outcomes return normally and the caller
    chooses succeeded/warned via stage_scope.
    """
    from dq import evaluate_dq_rules  # noqa: WPS433 — Spark-side module

    rules = _parse_rules(config_dq_rules)
    passing_df, failing_df, warnings = evaluate_dq_rules(
        df, rules, total_count=source_count
    )
    failing_count = failing_df.count()

    if failing_count > 0:
        failing_df.write.mode("overwrite").parquet(
            _dlq_path(domain, dataset, business_date, run_id)
        )

    dq_pass_count = source_count - failing_count
    if source_count > 0 and dq_pass_count == 0 and failing_count > 0:
        raise DQAllRowsFailed(
            f"All {source_count} rows failed DQ — nothing curated."
        )

    return DQOutcome(
        passing_df=passing_df,
        failing_count=failing_count,
        warnings=list(warnings or []),
    )
