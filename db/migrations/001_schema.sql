CREATE SCHEMA IF NOT EXISTS cp;
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()

CREATE TABLE cp.edge_type (
    edge_type     TEXT PRIMARY KEY,
    is_provenance BOOLEAN NOT NULL
);
INSERT INTO cp.edge_type(edge_type, is_provenance) VALUES
    ('raw_to_curated', true), ('curated_to_canonical', true),
    ('merge_to_canonical', true), ('canonical_to_sink', true),
    ('quarantine', true), ('replay', true), ('orchestrates', false);

CREATE TABLE cp.dataset_config (
    domain      TEXT NOT NULL, dataset TEXT NOT NULL,
    key_fields  JSONB NOT NULL, write_mode TEXT NOT NULL,
    sink_type   TEXT, sink_config JSONB, dq_rules JSONB,
    UNIQUE (domain, dataset)
);

CREATE TABLE cp.file_catalogue (
    file_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    s3_raw_path   TEXT NOT NULL, file_md5 TEXT NOT NULL,
    business_date DATE NOT NULL, state TEXT NOT NULL DEFAULT 'registered',
    domain TEXT, dataset TEXT,
    UNIQUE (file_md5, business_date)
);

CREATE TABLE cp.run_log (
    run_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id  TEXT NOT NULL,
    trigger_type     TEXT NOT NULL,
    replay_of_run_id UUID REFERENCES cp.run_log(run_id),
    pipeline_type    TEXT NOT NULL,
    domain           TEXT NOT NULL, dataset TEXT NOT NULL,
    business_date    DATE NOT NULL,
    file_id          UUID REFERENCES cp.file_catalogue(file_id),
    status           TEXT NOT NULL DEFAULT 'running',
    record_count_in  BIGINT, record_count_out BIGINT,
    error            TEXT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ
);

CREATE TABLE cp.run_stage_log (
    stage_log_id    BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES cp.run_log(run_id),
    stage           TEXT NOT NULL, attempt INT NOT NULL DEFAULT 1,
    status          TEXT NOT NULL,
    record_count_in BIGINT, record_count_out BIGINT, metrics JSONB,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

CREATE TABLE cp.lineage_link (
    lineage_link_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer_run_id   UUID NOT NULL REFERENCES cp.run_log(run_id),
    edge_type         TEXT NOT NULL REFERENCES cp.edge_type(edge_type),
    sink_type         TEXT,
    target_ref        JSONB NOT NULL,
    transform_version TEXT,
    record_count      BIGINT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sink_type_iff_sink CHECK (
        (edge_type = 'canonical_to_sink') = (sink_type IS NOT NULL)
    )
);
CREATE UNIQUE INDEX uq_lineage_link_target
    ON cp.lineage_link (consumer_run_id, edge_type, (target_ref->>'content_hash'));

CREATE TABLE cp.lineage_edge (
    lineage_edge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link(lineage_link_id),
    upstream_run_id UUID REFERENCES cp.run_log(run_id),
    source_file_id  UUID REFERENCES cp.file_catalogue(file_id),
    input_slot      INT NOT NULL DEFAULT 0,
    edge_type       TEXT NOT NULL REFERENCES cp.edge_type(edge_type),
    source_ref      JSONB,
    record_count    BIGINT NOT NULL
);

CREATE TABLE cp.reconciliation_log (
    recon_id        BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES cp.run_log(run_id),
    check_type      TEXT NOT NULL,
    source_count    BIGINT NOT NULL, accounted_count BIGINT NOT NULL,
    discrepancy     BIGINT NOT NULL, status TEXT NOT NULL,
    metrics         JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cp.dlq (
    dlq_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id        UUID NOT NULL REFERENCES cp.run_log(run_id),
    stage         TEXT NOT NULL, reason TEXT NOT NULL,
    source_ref    JSONB, payload_ref TEXT, record_count BIGINT NOT NULL,
    replayed_at   TIMESTAMPTZ, replay_run_id UUID REFERENCES cp.run_log(run_id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Provenance walk: edges only, provenance edge_types only (excludes 'orchestrates').
CREATE VIEW cp.v_provenance AS
WITH RECURSIVE walk AS (
    SELECT l.lineage_link_id, e.lineage_edge_id, e.upstream_run_id,
           e.source_file_id, e.edge_type, l.consumer_run_id
    FROM cp.lineage_link l
    JOIN cp.lineage_edge e ON e.lineage_link_id = l.lineage_link_id
    JOIN cp.edge_type t    ON t.edge_type = e.edge_type AND t.is_provenance
  UNION ALL
    SELECT pl.lineage_link_id, pe.lineage_edge_id, pe.upstream_run_id,
           pe.source_file_id, pe.edge_type, pl.consumer_run_id
    FROM walk w
    JOIN cp.lineage_link pl ON pl.consumer_run_id = w.upstream_run_id
    JOIN cp.lineage_edge pe ON pe.lineage_link_id = pl.lineage_link_id
    JOIN cp.edge_type t     ON t.edge_type = pe.edge_type AND t.is_provenance
)
SELECT * FROM walk;
