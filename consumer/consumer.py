import random
import sys
import time

from confluent_kafka import Consumer, Producer

sys.path.append("..")
from common import config
from common.avro_utils import load_schema, deserialize


class TransientError(Exception):
    """Recoverable failure - safe to retry (e.g. a flaky downstream call)."""


class PermanentError(Exception):
    """Non-recoverable failure - retrying will never fix this."""


class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def add(self, price):
        self.count += 1
        self.total += price
        return self.total / self.count


def process_order(order):
    """Validates and 'processes' an order. Raises PermanentError for bad
    data and randomly raises TransientError to simulate a flaky downstream
    dependency (e.g. an inventory service timing out)."""
    if order["price"] < 0:
        raise PermanentError(f"invalid price {order['price']} for orderId={order['orderId']}")

    if random.random() < config.TRANSIENT_FAILURE_RATE:
        raise TransientError(f"simulated transient failure while processing orderId={order['orderId']}")

    return order


def handle_with_retry(order, raw_value, dlq_producer, schema):
    """Runs process_order, retrying transient failures with exponential
    backoff. Anything left unresolved after MAX_RETRIES, or any permanent
    error, is routed to the DLQ."""
    attempt = 0
    backoff = config.RETRY_BACKOFF_SECONDS

    while True:
        attempt += 1
        try:
            return process_order(order)
        except PermanentError as e:
            print(f"[consumer] permanent failure: {e} -> sending to DLQ")
            send_to_dlq(dlq_producer, raw_value, order, reason=str(e))
            return None
        except TransientError as e:
            if attempt >= config.MAX_RETRIES:
                print(f"[consumer] giving up after {attempt} attempts ({e}) -> sending to DLQ")
                send_to_dlq(dlq_producer, raw_value, order, reason=str(e))
                return None

            print(f"[consumer] transient failure (attempt {attempt}/{config.MAX_RETRIES}): {e}, "
                  f"retrying in {backoff}s")
            time.sleep(backoff)
            backoff *= 2


def send_to_dlq(dlq_producer, raw_value, order, reason):
    dlq_producer.produce(
        topic=config.TOPIC_DLQ,
        key=order.get("orderId", "unknown").encode("utf-8"),
        value=raw_value,
        headers={"failure-reason": reason.encode("utf-8")},
    )
    dlq_producer.poll(0)


def main():
    schema = load_schema(config.SCHEMA_PATH)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": "order-consumer-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([config.TOPIC_ORDERS])

    dlq_producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    running_avg = RunningAverage()

    print(f"[consumer] listening on '{config.TOPIC_ORDERS}' (failed messages -> '{config.TOPIC_DLQ}')")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] kafka error: {msg.error()}")
                continue

            order = deserialize(schema, msg.value())
            print(f"[consumer] received orderId={order['orderId']} product={order['product']} "
                  f"price={order['price']:.2f}")

            result = handle_with_retry(order, msg.value(), dlq_producer, schema)
            if result is not None:
                avg = running_avg.add(result["price"])
                print(f"[consumer] running average price: {avg:.2f} (n={running_avg.count})")

            consumer.commit(msg)
    except KeyboardInterrupt:
        print("\n[consumer] stopping...")
    finally:
        consumer.close()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
