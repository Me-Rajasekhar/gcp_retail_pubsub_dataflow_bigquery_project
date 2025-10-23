import time
import json
import uuid
import random
import argparse
from datetime import datetime, timezone
from google.cloud import pubsub_v1

PRODUCTS = [
    {"product_id": "P1001", "name": "Milk", "price": 40.0},
    {"product_id": "P1002", "name": "Bread", "price": 30.0},
    {"product_id": "P1003", "name": "Eggs", "price": 70.0},
    {"product_id": "P1004", "name": "Soap", "price": 50.0},
]

def generate_transaction():
    event_id = str(uuid.uuid4())
    transaction_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    customer_id = f"C{random.randint(1000,9999)}"
    store_id = f"S{random.randint(1,10)}"
    payment_type = random.choice(["cash", "card", "upi"])
    loyalty_id = random.choice(["L123", "L456", None])

    items = []
    for _ in range(random.randint(1,3)):
        p = random.choice(PRODUCTS)
        qty = random.randint(1,5)
        items.append({
            "product_id": p["product_id"],
            "qty": qty,
            "price": p["price"]
        })

    total_amount = sum(i["qty"] * i["price"] for i in items)
    event = {
        "event_id": event_id,
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "customer_id": customer_id,
        "store_id": store_id,
        "items": items,
        "total_amount": total_amount,
        "payment_type": payment_type,
        "loyalty_id": loyalty_id
    }
    return event

def main(project, topic, rate, count):
    publisher = pubsub_v1.PublisherClient()
    topic_path = topic if topic.startswith("projects/") else f"projects/{project}/topics/{topic}"

    for i in range(count):
        event = generate_transaction()
        data = json.dumps(event).encode("utf-8")
        publisher.publish(topic_path, data=data, event_id=event["event_id"])
        print(f"Published event_id={event['event_id']} total={event['total_amount']}")
        time.sleep(1.0 / rate)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--rate", type=float, default=2.0, help="events per second")
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()
    main(args.project, args.topic, args.rate, args.count)
