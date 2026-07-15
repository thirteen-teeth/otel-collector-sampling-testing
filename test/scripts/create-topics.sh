#!/usr/bin/env bash
# create-topics.sh — Create Kafka topics for the tail-sampling test.
#
# Usage:
#   KAFKA_BROKER=kafka:9092 PARTITIONS=6 create-topics.sh
#
# Topics created:
#   otel-traces-raw      All ingested traces (100%)
#   otel-traces-sampled  Retained sampled traces (~10%)

set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
PARTITIONS="${KAFKA_PARTITIONS:-6}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
TIMEOUT_MS="${TIMEOUT_MS:-30000}"

echo "Creating Kafka topics on ${KAFKA_BROKER} (partitions=${PARTITIONS}, rf=${REPLICATION_FACTOR})..."

create_topic() {
    local topic="$1"
    echo "  Creating topic: ${topic}"
    kafka-topics.sh \
        --bootstrap-server "${KAFKA_BROKER}" \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION_FACTOR}" \
        --command-config /dev/null \
        2>&1 || true   # --if-not-exists should make this idempotent
}

create_topic "otel-traces-raw"
create_topic "otel-traces-sampled"

echo "Topics after creation:"
kafka-topics.sh --bootstrap-server "${KAFKA_BROKER}" --list

echo "✅ Topic creation complete"
