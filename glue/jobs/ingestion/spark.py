"""Spark session builder for the ingestion job.

Lifted from the previous monolith (`ods_ingestion._build_spark`). Kept
in its own module so the rest of the pipeline can be unit-tested
without importing pyspark.
"""
from __future__ import annotations

import os


def build_spark(dataset: str):
    """Build the Spark session used by the ingestion pipeline.

    The S3A endpoint defaults to LocalStack so dev compose works
    without env-var changes; production runs supply ``LOCALSTACK_ENDPOINT``
    pointing at the real S3 endpoint (or empty for AWS-default).
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName(f"ods_ingestion_{dataset}")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.environ.get("LOCALSTACK_ENDPOINT", ""),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )
        .getOrCreate()
    )
