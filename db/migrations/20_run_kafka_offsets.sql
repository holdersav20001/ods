-- T6 (B8) + T8 (8.3): normalised per-partition offset table
--
-- Stores the offset range each (run_id, stage, topic, partition) covered.
-- Persisted in the SAME postgres transaction as the run-status update so a
-- crash between Kafka transaction commit and run-status commit is detectable
-- via runs.start() resume-time check (T6: exactly-once on retry).
--
-- Replaces JSONB offset blobs in run_stage_log.metrics. Existing JSONB rows
-- continue to live there during the transition; backfill script lands in T8.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline.run_kafka_offsets (
    run_id        uuid    NOT NULL,
    stage         text    NOT NULL,
    topic         text    NOT NULL,
    partition     int     NOT NULL,
    offset_start  bigint  NOT NULL,
    offset_end    bigint  NOT NULL,
    record_count  bigint  GENERATED ALWAYS AS (offset_end - offset_start) STORED,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage, topic, partition),
    CONSTRAINT run_kafka_offsets_run_fk
        FOREIGN KEY (run_id) REFERENCES pipeline.run_log (run_id) ON DELETE CASCADE,
    CONSTRAINT run_kafka_offsets_range_chk
        CHECK (offset_end >= offset_start)
);

-- Dashboard / freshness queries: latest offsets per topic.
CREATE INDEX IF NOT EXISTS run_kafka_offsets_topic_partition_idx
    ON pipeline.run_kafka_offsets (topic, partition, recorded_at DESC);

-- T6 resume-time idempotency: lookup by (run_id, stage) cheap.
CREATE INDEX IF NOT EXISTS run_kafka_offsets_run_stage_idx
    ON pipeline.run_kafka_offsets (run_id, stage);

COMMENT ON TABLE pipeline.run_kafka_offsets IS
    'Per-partition Kafka offsets covered by each (run_id, stage). Persisted in '
    'same Postgres tx as run status to give exactly-once semantics on retry: '
    'runs.start() detects existing rows and signals republish-skip.';

COMMIT;
