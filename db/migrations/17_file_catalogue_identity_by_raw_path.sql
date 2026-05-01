-- Migration 17: treat each landed raw file path as the file identity.
--
-- MD5 is still useful for duplicate-content detection, but it must not be the
-- uniqueness key: two applications/dates can legitimately send byte-identical
-- files and still need separate file_id lineage.

ALTER TABLE pipeline.file_catalogue
    DROP CONSTRAINT IF EXISTS file_catalogue_domain_dataset_file_md5_key;

CREATE UNIQUE INDEX IF NOT EXISTS file_catalogue_domain_dataset_raw_path_key
    ON pipeline.file_catalogue(domain, dataset, s3_raw_path)
    WHERE s3_raw_path IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_file_catalogue_md5
    ON pipeline.file_catalogue(domain, dataset, file_md5);
