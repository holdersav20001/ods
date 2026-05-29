CREATE SCHEMA IF NOT EXISTS ods;
CREATE TABLE ods.orders (
    row_id               BIGSERIAL PRIMARY KEY,
    payload              JSONB NOT NULL,
    _ods_workflow_run_id TEXT,
    _ods_lineage_link_id UUID NOT NULL REFERENCES cp.lineage_link(lineage_link_id)
);
