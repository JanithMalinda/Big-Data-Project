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

### Step 1: Open the project directory

```powershell
cd "C:\Users\acer\Desktop\Big Data Project"
```

### Step 2: Create a virtual environment and install dependencies

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Step 3: Start Kafka

```powershell
docker compose up -d
```

Wait 10-15 seconds for the broker to finish starting. Kafka UI is available at
[http://localhost:8080](http://localhost:8080) to watch topics/messages live.

> **If `docker` isn't recognized:** Docker Desktop's installer updates the system PATH,
> but a terminal opened before the install (or before a reboot) won't see it. Close the
> terminal completely, open a new one, and try again. If it still fails, restart your
> machine once, then retry.

### Step 4: Start the consumer

In a terminal with the virtual environment activated:

```powershell
cd consumer
python consumer.py
```

### Step 5: Start the producer

In a second terminal, with the virtual environment activated:

```powershell
cd producer
python producer.py --interval 1
```

Use `python producer.py --count 50 --interval 1` instead if you want a fixed-length run.

### Step 6: Watch the Dead Letter Queue (optional)

In a third terminal, with the virtual environment activated:

```powershell
cd consumer
python dlq_monitor.py
```

### Stopping

Stop the producer/consumer/monitor with `Ctrl+C` in each terminal. Stop Kafka with:

```powershell
docker compose down
```
