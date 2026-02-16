#!/usr/bin/env bash
set -euo pipefail

# Create Kafka topics for the wordcount-kafka job.
#
# Usage:
#   ./create_topics.sh                      # defaults: localhost:9092, 4 partitions
#   ./create_topics.sh kafka:9092           # custom broker (e.g. from inside Docker network)
#   PARTITIONS=8 ./create_topics.sh         # custom partition count

BROKER="${1:-localhost:9092}"
CONTAINER="${KAFKA_CONTAINER:-bicycle-kafka}"
PARTITIONS="${PARTITIONS:-4}"
REPLICATION="${REPLICATION:-1}"

TOPICS=(
  "raw-lines"
  "word-counts"
)

echo "Creating Kafka topics on broker: ${BROKER}"
echo "  Partitions: ${PARTITIONS}  Replication: ${REPLICATION}"
echo ""

for TOPIC in "${TOPICS[@]}"; do
  echo -n "  ${TOPIC} ... "
  if docker exec "${CONTAINER}" \
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "${BROKER}" \
      --create \
      --if-not-exists \
      --topic "${TOPIC}" \
      --partitions "${PARTITIONS}" \
      --replication-factor "${REPLICATION}" \
    2>/dev/null; then
    echo "ok"
  else
    echo "already exists or error (continuing)"
  fi
done

echo ""
echo "Listing topics:"
docker exec "${CONTAINER}" \
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BROKER}" \
    --list
