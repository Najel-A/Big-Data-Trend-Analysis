#!/bin/bash

BROKER="localhost:9092"
TOPICS=("twitter_sentiment" "reddit_stream" "sports_stream")

# Proceed to create topics
for topic in "${TOPICS[@]}"; do
  echo "🔧 Creating topic: $topic"
  kafka-topics --create \
    --if-not-exists \
    --bootstrap-server "$BROKER" \
    --replication-factor 1 \
    --partitions 1 \
    --topic "$topic"
done