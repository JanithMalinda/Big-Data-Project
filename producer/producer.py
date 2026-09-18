import argparse
import random
import sys
import time
import uuid

from confluent_kafka import Producer

sys.path.append("..")
from common import config
from common.avro_utils import load_schema, serialize

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def make_order():
    price = round(random.uniform(5.0, 500.0), 2)

    # occasionally simulate corrupted/invalid data (negative price) so the
    # consumer's DLQ path actually gets exercised during the demo
    if random.random() < config.INVALID_PRICE_RATE:
        price = -price

    return {
        "orderId": str(random.randint(1000, 9999)),
        "product": random.choice(PRODUCTS),
        "price": price,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed for {msg.key()}: {err}")
    else:
        print(f"[producer] sent orderId={msg.key().decode()} -> "
              f"{msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def main():
    parser = argparse.ArgumentParser(description="Produces Avro-encoded order messages to Kafka")
    parser.add_argument("--count", type=int, default=0, help="number of messages to send (0 = run forever)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds to sleep between messages")
    args = parser.parse_args()

    schema = load_schema(config.SCHEMA_PATH)
    producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "client.id": f"order-producer-{uuid.uuid4().hex[:6]}",
    })

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            order = make_order()
            payload = serialize(schema, order)

            producer.produce(
                topic=config.TOPIC_ORDERS,
                key=order["orderId"].encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)

            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[producer] stopping...")
    finally:
        producer.flush()
        print(f"[producer] done, sent {sent} messages")


if __name__ == "__main__":
    main()
