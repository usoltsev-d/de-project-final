#!/bin/bash
set -e

echo "Waiting for Kafka..."

until /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list >/dev/null 2>&1; do
    sleep 2
done

echo "Creating topic $KAFKA_TOPIC..."

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic "$KAFKA_TOPIC" \
  --partitions 1 \
  --replication-factor 1

echo "Kafka initialization completed."