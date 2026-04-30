-- Migration 09: pipeline.run_events — queryable copy of Kafka run-event stream

CREATE TABLE IF NOT EXISTS pipeline.run_events (
    id                     BIGSERIAL PRIMARY KEY,
    run_id                 VARCHAR      NOT NULL,
    event_type             VARCHAR      NOT NULL,
    domain                 VARCHAR      NOT NULL,
    dataset                VARCHAR      NOT NULL,
    business_date          VARCHAR      NOT NULL,
    status                 VARCHAR      NOT NULL,
    record_count_published INTEGER,
    kafka_topic            VARCHAR,
    kafka_offset_end       BIGINT,
    occurred_at            TIMESTAMP    NOT NULL
);

CREATE INDEX IF NOT EXISTS run_events_run_id_idx    ON pipeline.run_events (run_id);
CREATE INDEX IF NOT EXISTS run_events_occurred_idx  ON pipeline.run_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS run_events_domain_ds_idx ON pipeline.run_events (domain, dataset);

GRANT ALL ON pipeline.run_events         TO ods;
GRANT ALL ON pipeline.run_events_id_seq  TO ods;
