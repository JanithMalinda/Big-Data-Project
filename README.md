# Kafka Order Processing System

Producer/consumer system for order messages, using Avro serialization, real-time
running-average aggregation, retry logic for transient failures, and a Dead
Letter Queue for permanently failed messages.

## Components

- `schemas/order.avsc` - Avro schema for an order (`orderId`, `product`, `price`)
- `common/` - shared config and Avro encode/decode helpers
- `producer/producer.py` - generates random orders and publishes them to the `orders` topic
- `consumer/consumer.py` - consumes `orders`, aggregates a running average price, retries
  transient failures, and routes permanently failed messages to `orders-dlq`
- `consumer/dlq_monitor.py` - optional viewer to watch what lands in `orders-dlq`
- `docker-compose.yml` - single-node Kafka (KRaft mode) + Kafka UI at `localhost:8080`

## How failures are simulated

- The producer occasionally emits an order with a negative price (~8% of the time) to
  simulate corrupted data.
- The consumer treats a negative price as a **permanent** failure - it goes straight to
  the DLQ, no point retrying.
- The consumer also randomly simulates a **transient** failure (~25% of attempts) to
  mimic a flaky downstream dependency. These are retried up to 3 times with exponential
  backoff before giving up and sending to the DLQ.

## Running

See the run commands below.
