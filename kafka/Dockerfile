FROM confluentinc/cp-kafka:latest

COPY create-topics.sh /tmp/create-topics.sh

# No chmod needed; just execute from a writable location
CMD ["sh", "-c", "/etc/confluent/docker/run & sleep 10 && /tmp/create-topics.sh && wait"]
