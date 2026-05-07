"""Curated write step — Parquet to S3 + count verification.

Adds the ODS system columns (file_id / run_id / domain / etc.) onto
the passing DataFrame, writes Parquet to the curated bucket, and
verifies the written count matches the expected count
(``source - dq_failures``). A mismatch raises :class:`CountMismatch`,
which the orchestrator's ``stage_scope`` turns into ``stage_failed``.
"""
from __future__ import annotations

import os
from typing import Any

import ods_pipeline


class CountMismatch(RuntimeError):
    """Raised when the curated row count differs from the expected count."""

    def __init__(self, *, written: int, expected: int) -> None:
        super().__init__(
            f"Count mismatch: written={written}, expected={expected}"
        )
        self.written = written
        self.expected = expected


def enrich_with_metadata(
    df: Any,
    *,
    file_id: str,
    run_id: str,
    domain: str,
    dataset: str,
    business_date: str,
    source_application: str | None = None,
) -> Any:
    """Add the ODS system columns onto each row of ``df``."""
    from pyspark.sql import functions as F  # noqa: WPS433

    application = source_application or os.environ.get(
        "ODS_SOURCE_APPLICATION", "sftp"
    )
    file_meta = ods_pipeline.metadata.file_metadata(
        file_id=str(file_id),
        run_id=run_id,
        domain=domain,
        dataset=dataset,
        business_date=business_date,
        source_application=application,
    )
    enriched = df
    for field, value in file_meta.items():
        enriched = enriched.withColumn(field, F.lit(value))
    return enriched


def curated_uri(domain: str, dataset: str, business_date: str) -> str:
    """Resolve the curated Parquet path for ``(domain, dataset, business_date)``."""
    env = os.environ.get("ENV", "local")
    return (
        f"s3a://ods-curated-{env}/{domain}/{dataset}"
        f"/date={business_date}/"
    )


def write_and_verify(
    passing_df: Any,
    *,
    domain: str,
    dataset: str,
    business_date: str,
    expected_count: int,
) -> tuple[str, int]:
    """Write Parquet to curated and verify the row count.

    Returns ``(curated_s3_uri, written_count)``. ``s3://`` form (not
    ``s3a://``) so callers can store it on the file_catalogue /
    run_log without re-mapping.
    """
    s3a_target = curated_uri(domain, dataset, business_date)
    passing_df.write.mode("overwrite").parquet(s3a_target)

    written = passing_df.count()
    if written != expected_count:
        raise CountMismatch(written=written, expected=expected_count)

    return s3a_target.replace("s3a://", "s3://"), written
