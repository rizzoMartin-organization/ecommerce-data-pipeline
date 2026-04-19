from kafka import KafkaProducer
import json
import uuid
import random
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 500),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(5.0, 500.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "created"
    }

def generate_navigation_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice([
            "product_view",
            "add_to_cart",
            "remove_from_cart"
        ]),
        "user_id": random.randint(1,1000),
        "product_id": random.randint(1, 500),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": str(uuid.uuid4())
    }

def generate_inventory_update():
    reason = random.choice(["sale", "restock", "return", "adjustment"])
    
    if reason == "restock":
        stock_change = random.randint(20, 200)
    elif reason == "return":
        stock_change = random.randint(1, 5)
    elif reason == "sale":
        stock_change = random.randint(-5, -1)
    else:  # adjustment
        stock_change = random.choice([-1, -2, -3, 1, 2, 3])
    
    return {
        "product_id": random.randint(1, 500),
        "stock_change": stock_change,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "reason": reason
    }

while True:
    order = generate_order()
    producer.send(
        topic='ecommerce.orders',
        key=str(order['user_id']),
        value=order
    )
    navigation_event = generate_navigation_event()
    producer.send(
        topic='ecommerce.navigation_events',
        key=str(navigation_event['user_id']),
        value=navigation_event
    )
    inventory_update=generate_inventory_update()
    producer.send(
        topic='ecommerce.inventory_updates',
        key=str(inventory_update['product_id']),
        value=inventory_update
    )

    print(f"Order sent: {order['order_id']} - user {order['user_id']}")
    print(f"Navigation event sent: {navigation_event['event_id']} - user {navigation_event['user_id']}")
    print(f"Inventory update sent: {inventory_update['reason']} - product {inventory_update['product_id']}")
    time.sleep(1)