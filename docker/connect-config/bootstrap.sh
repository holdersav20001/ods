#!/bin/sh
set -e

echo "Waiting for Kafka Connect REST API..."
until curl -sf http://kafka-connect:8083/ > /dev/null; do
  echo "  ...kafka-connect not ready, sleeping 2s"
  sleep 2
done
echo "Kafka Connect is up."

for f in /cfg/jdbc-sink-policies.json /cfg/s3-sink-policies.json; do
  name=$(grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' "$f" | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
  echo "Re-registering connector: $name"
  curl -sf -X DELETE "http://kafka-connect:8083/connectors/$name" > /dev/null 2>&1 || true
  curl -sf -X POST -H 'Content-Type: application/json' \
    --data @"$f" \
    http://kafka-connect:8083/connectors > /dev/null
done

echo "connectors registered"
