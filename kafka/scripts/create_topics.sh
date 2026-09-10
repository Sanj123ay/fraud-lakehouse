#!/usr/bin/env bash
# ============================================================================
#  Creates every Kafka topic the platform needs.
#  Topics = named message streams. We create them explicitly (instead of
#  relying on auto-create) so we control partition counts.
#
#  Run from your laptop:   bash kafka/scripts/create_topics.sh   (or: make topics)
#  Safe to run repeatedly — existing topics are skipped.
# ============================================================================
set -euo pipefail

KAFKA="docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092"

create() {  # create <topic> <partitions>
  $KAFKA --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
  echo "topic ready: $1 (partitions=$2)"
}

create raw_transactions 3        # NiFi -> Spark : the firehose of card swipes
create cdc.public.customers 1    # Debezium CDC events for customers
create cdc.public.merchants 1    # Debezium CDC events for merchants

echo "--- current topics ---"
$KAFKA --list
