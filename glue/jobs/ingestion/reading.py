"""Raw read step — business_date resolution + Spark reader.

The two raw formats follow different paths:

* ``csv`` — business_date is parsed from the filename via the
  configured regex (file_pipeline pattern).
* ``jsonl`` — business_date already lives on the ``file_catalogue``
  row that ``dag_api_pull`` registered when archiving the response.
  Lookup by ``file_id`` (fast path) or ``s3_raw_path`` (fallback).

Reader returns the Spark DataFrame; callers handle counting and stage
bookkeeping.
"""
from __future__ import annotations

import re
from typing import Any

_DATE_PARTITION_RE = re.compile(
    r"(?:^|/)date=(?P<date>\d{8}|\d{4}-\d{2}-\d{2})(?:/|$)"
)


def _business_date_from_partition(s3_input_path: str) -> str | None:
    """Return YYYY-MM-DD from a date= partition when present."""
    match = _DATE_PARTITION_RE.search(s3_input_path)
    if not match:
        return None

    raw_date = match.group("date")
    if "-" in raw_date:
        return raw_date
    return f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"


def resolve_business_date(
    conn,
    *,
    config: dict,
    s3_input_path: str,
    file_id: str | None,
) -> str:
    """Return ``YYYY-MM-DD`` for the input file."""
    raw_format = (config.get("raw_format") or "csv").lower()
    if raw_format == "csv":
        # Late import keeps the unit-test path free of glue.utils dependencies.
        from utils import extract_business_date  # noqa: WPS433

        filename = s3_input_path.split("/")[-1]
        try:
            bd = extract_business_date(filename, config["filename_pattern"])
            return bd.strftime("%Y-%m-%d")
        except ValueError:
            partition_date = _business_date_from_partition(s3_input_path)
            if partition_date:
                return partition_date
            raise

    if raw_format == "jsonl":
        with conn.cursor() as cur:
            if file_id:
                cur.execute(
                    "SELECT business_date::text "
                    "  FROM pipeline.file_catalogue "
                    " WHERE file_id::text=%s",
                    (file_id,),
                )
            else:
                cur.execute(
                    "SELECT business_date::text "
                    "  FROM pipeline.file_catalogue "
                    " WHERE s3_raw_path=%s "
                    " ORDER BY state_updated_at DESC NULLS LAST LIMIT 1",
                    (s3_input_path,),
                )
            row = cur.fetchone()
        if not row or not row[0]:
            raise ValueError(
                f"jsonl ingestion requires file_catalogue.business_date for "
                f"{s3_input_path!r} (file_id={file_id})"
            )
        return row[0]

    raise ValueError(
        f"unsupported raw_format={raw_format!r}; expected 'csv' or 'jsonl'"
    )


def read_raw(spark: Any, s3_input_path: str, raw_format: str) -> Any:
    """Read the raw S3 object into a Spark DataFrame.

    JSONL responses from api_pull carry a ``payload`` struct under the
    standard ODS envelope; we flatten it onto top-level columns so the
    DQ + curated-write steps see a flat table. The original ``payload``
    struct is preserved as a JSON string so the audit trail keeps the
    source shape intact.
    """
    s3a_path = s3_input_path.replace("s3://", "s3a://")

    if raw_format == "jsonl":
        from pyspark.sql import functions as F  # noqa: WPS433
        from pyspark.sql.types import StructType  # noqa: WPS433

        df = spark.read.json(s3a_path)
        if "payload" in df.columns:
            payload_type = df.schema["payload"].dataType
            if isinstance(payload_type, StructType):
                for field in payload_type.fields:
                    if field.name not in df.columns:
                        df = df.withColumn(field.name, F.col(f"payload.{field.name}"))
                df = df.withColumn("payload", F.to_json(F.col("payload")))
        if "request_id" not in df.columns and "_ods_source_request_id" in df.columns:
            df = df.withColumn("request_id", F.col("_ods_source_request_id"))
        return df

    # csv default
    return (
        spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(s3a_path)
    )
