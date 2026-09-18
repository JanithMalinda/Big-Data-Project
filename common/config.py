import os

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_ORDERS = "orders"
TOPIC_DLQ = "orders-dlq"

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order.avsc")

# retry behaviour for messages that fail with a transient error
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1  # doubles on each attempt

# how often the consumer simulates a transient (recoverable) processing error
TRANSIENT_FAILURE_RATE = 0.25

# how often the producer emits a deliberately invalid order (negative price)
# to simulate corrupted data that the consumer can never recover from
INVALID_PRICE_RATE = 0.08
