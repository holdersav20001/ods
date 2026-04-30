#!/bin/sh
set -e

echo "Waiting for Kafka Connect REST API..."
until curl -sf http://kafka-connect:8083/ > /dev/null; do
  echo "  ...kafka-connect not ready, sleeping 2s"
  sleep 2
done
echo "Kafka Connect is up."

for f in /cfg/jdbc-sink-policies.json /cfg/jdbc-sink-policy-history.json /cfg/s3-sink-policies.json; do
  name=$(grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' "$f" | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
  echo "Re-registering connector: $name"
  curl -sf -X DELETE "http://kafka-connect:8083/connectors/$name" > /dev/null 2>&1 || true
  curl -sf -X POST -H 'Content-Type: application/json' \
    --data @"$f" \
    http://kafka-connect:8083/connectors > /dev/null
done

echo "connectors registered"

echo "Registering pipeline run-events schema..."
SR="http://schema-registry:8081"
until curl -sf "$SR/subjects" > /dev/null; do sleep 2; done
RUN_EVENT_SCHEMA='{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"RunEvent\",\"namespace\":\"com.aviva.ods.pipeline\",\"fields\":[{\"name\":\"run_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"domain\",\"type\":\"string\"},{\"name\":\"dataset\",\"type\":\"string\"},{\"name\":\"business_date\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"record_count_published\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"kafka_topic\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"kafka_offset_end\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"occurred_at\",\"type\":\"string\"}]}"}'
curl -sf -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data "$RUN_EVENT_SCHEMA" \
  "$SR/subjects/ods.pipeline.run-events-value/versions" > /dev/null
echo "run-events schema registered"
