"""Small helper to watch the DLQ topic during a live demo, so failed
messages are visible without digging through kafka-ui."""
import sys

from confluent_kafka import Consumer

sys.path.append("..")
from common import config
from common.avro_utils import load_schema, deserialize


def main():
    schema = load_schema(config.SCHEMA_PATH)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": "dlq-monitor",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([config.TOPIC_DLQ])

    print(f"[dlq-monitor] watching '{config.TOPIC_DLQ}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[dlq-monitor] kafka error: {msg.error()}")
                continue

            order = deserialize(schema, msg.value())
            reason = dict(msg.headers() or []).get("failure-reason", b"unknown").decode()
            print(f"[dlq-monitor] orderId={order['orderId']} product={order['product']} "
                  f"price={order['price']:.2f} reason=\"{reason}\"")
    except KeyboardInterrupt:
        print("\n[dlq-monitor] stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
