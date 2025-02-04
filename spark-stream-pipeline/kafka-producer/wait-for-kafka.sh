#!/bin/sh

KAFKA_BROKER="kafka:9092"
MAX_RETRIES=10
RETRY_INTERVAL=5

echo "⏳ Waiting for Kafka to be ready at $KAFKA_BROKER..."

for i in $(seq 1 $MAX_RETRIES); do
    nc -z kafka 9092 && echo "✅ Kafka is up!" && exit 0
    echo "❌ Kafka not ready yet... retrying in $RETRY_INTERVAL seconds ($i/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done

echo "❌ Kafka did not become available in time. Exiting."
exit 1