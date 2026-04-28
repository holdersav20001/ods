BEGIN;
CREATE TABLE IF NOT EXISTS pipeline.file_state (
    id            SERIAL PRIMARY KEY,
    s3_path       VARCHAR NOT NULL UNIQUE,
    run_id        UUID NOT NULL,
    status        VARCHAR NOT NULL CHECK (status IN ('new','processing','completed','failed')),
    record_count  INTEGER,
    error_reason  VARCHAR,
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);
COMMIT;
