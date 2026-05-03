#!/bin/bash
set -e
ENDPOINT=http://localhost:4566

for bucket in ods-raw-local ods-curated-local ods-config-local \
              ods-dlq-local ods-quarantine-local ods-audit-sink-local \
              ods-event-demo; do
  aws --endpoint-url=$ENDPOINT --region eu-west-1 s3 mb s3://$bucket 2>/dev/null || true
  echo "Verified bucket: $bucket"
done

# Validate all buckets exist
for bucket in ods-raw-local ods-curated-local ods-config-local \
              ods-dlq-local ods-quarantine-local ods-audit-sink-local \
              ods-event-demo; do
  aws --endpoint-url=$ENDPOINT --region eu-west-1 s3 ls s3://$bucket
  echo "Bucket confirmed: $bucket"
done

echo "LocalStack init complete."
