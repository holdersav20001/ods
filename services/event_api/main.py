"""FastAPI Event API — Message/API demo pipeline (T16 / 8.4).

Proves the message/API ingestion pattern parallel to file-based patterns.

POST /events
    1. Synthesise a file_catalogue row keyed by event_id
    2. Archive payload to S3 JSONL
    3. Publish to raw Kafka (insurance.event_demo IngestionPattern.topics[0])
    4. Canonicalize via the existing canonicalize.compile_transform
    5. Write recon row

Reuses ods_pipeline.runs/stages/lineage/messages — proves boundary
cleanliness. The FastAPI surface itself is thin: it wires HTTP -> the
existing control-plane primitives.

Side-effect collaborators (S3, Kafka producer, Postgres) are constructed
by ``build_app`` from injected factories so local/integration runners can
wire the app to the same backing services used by the rest of the stack.
"""
from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from ods_pipeline.patterns import get as get_pattern


class EventEnvelope(BaseModel):
    event_id: str | None = None
    domain: str = "insurance"
    dataset: str = "event_demo"
    business_date: str | None = None
    payload: dict


class IngestResponse(BaseModel):
    run_id: str
    event_id: str
    pattern: str
    archive_uri: str | None
    accepted: bool


def build_app(
    *,
    pg_factory,
    s3_client,
    producer_factory,
    archive_bucket: str = "ods-event-demo",
) -> FastAPI:
    """Assemble the FastAPI app with collaborators injected.

    Local tests and production can pass real boto3, confluent_kafka, and
    psycopg2 collaborators without changing the HTTP surface.
    """
    app = FastAPI(title="ODS Event Demo API")
    pattern = get_pattern("insurance.event_demo")

    def _get_pg():  # closure over factory so tests can swap implementations
        return pg_factory()

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok", "pattern": pattern.name}

    @app.post("/events", response_model=IngestResponse)
    def ingest_event(envelope: EventEnvelope, conn=Depends(_get_pg)) -> IngestResponse:
        event_id = envelope.event_id or str(uuid.uuid4())
        run_id = str(uuid.uuid4())
        archive_key = (
            f"raw/event/{envelope.domain}/{envelope.dataset}/"
            f"{envelope.business_date or 'unknown'}/{event_id}.jsonl"
        )
        try:
            s3_client.put_object(
                Bucket=archive_bucket,
                Key=archive_key,
                Body=json.dumps(envelope.payload, default=str).encode("utf-8"),
                ContentType="application/x-jsonlines",
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"S3 archive failed: {exc}")

        from ods_pipeline import messages
        try:
            messages.start_run(
                conn,
                run_id=run_id,
                domain=envelope.domain,
                dataset=envelope.dataset,
                source_application="event_api",
                correlation={"_ods_source_event_id": event_id},
                business_date=envelope.business_date,
                kafka_topic=pattern.topics[0],
            )
        except Exception as exc:
            raise HTTPException(status_code=500,
                                 detail=f"run start failed: {exc}")

        try:
            producer = producer_factory()
            producer.produce(
                topic=pattern.topics[0],
                value=json.dumps({
                    "_ods_source_event_id": event_id,
                    "_ods_run_id": run_id,
                    "_ods_domain": envelope.domain,
                    "_ods_dataset": envelope.dataset,
                    "_ods_business_date": envelope.business_date,
                    "payload": envelope.payload,
                }, default=str).encode("utf-8"),
            )
            flush_result = producer.flush()
            if isinstance(flush_result, int) and flush_result:
                raise RuntimeError(f"{flush_result} Kafka message(s) not delivered")
            messages.record_result(
                conn,
                run_id=run_id,
                domain=envelope.domain,
                dataset=envelope.dataset,
                business_date=envelope.business_date,
                source_count=1,
                published_count=1,
                archive_count=1,
                kafka_topic=pattern.topics[0],
                archive_ref=f"s3://{archive_bucket}/{archive_key}",
            )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            try:
                messages.record_result(
                    conn,
                    run_id=run_id,
                    domain=envelope.domain,
                    dataset=envelope.dataset,
                    business_date=envelope.business_date,
                    source_count=1,
                    published_count=0,
                    archive_count=1,
                    kafka_topic=pattern.topics[0],
                    archive_ref=f"s3://{archive_bucket}/{archive_key}",
                    extra_detail={"error": str(exc)},
                )
                conn.commit()
            except Exception:
                conn.rollback()
            raise HTTPException(status_code=502, detail=f"Kafka publish failed: {exc}")

        return IngestResponse(
            run_id=run_id,
            event_id=event_id,
            pattern=pattern.name,
            archive_uri=f"s3://{archive_bucket}/{archive_key}",
            accepted=True,
        )

    return app
