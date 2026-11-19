#!/bin/sh
set -eu
for topic in example_user_topic example_events_topic example_user_topic.dl example_events_topic.dl; do
  docker exec blueprint-kafka kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done
