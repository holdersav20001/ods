-- T6 (B8) + T8 (8.3): normalised per-partition offset table
--
-- Stores the offset range each (run_id, stage, topic, partition) covered.
-- runs.start() resume-time check uses has_recorded_offsets() against this
-- table to detect "Kafka transaction committed AND offsets persisted" and
-- skip republish on retry.
--
-- CURRENT semantics: offsets.persist_ranges() commits in its own tx,
-- AFTER the run-status update commits. A crash between the two commits
-- leaves run=succeeded with no offset rows; the next run will republish
-- under the idempotent producer (transactional.id keyed on run_id), so
-- this window does not produce data loss but DOES leave the dashboard
-- view momentarily empty until repair.
--
-- TARGET semantics (T12 follow-up — pattern.atomic(conn) context manager):
-- both writes share a single Postgres transaction; crash leaves an
-- atomically consistent state.
--
-- Replaces JSONB offset blobs in run_stage_log.metrics. Existing JSONB
-- rows continue to live there during the transition.

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
    'Per-partition Kafka offsets covered by each (run_id, stage). Currently '
    'persisted in a separate tx after the run_log update; T12 follow-up will '
    'collapse both into a single tx via pattern.atomic(conn). '
    'has_recorded_offsets() drives runs.start() resume-time republish-skip.';

COMMIT;
