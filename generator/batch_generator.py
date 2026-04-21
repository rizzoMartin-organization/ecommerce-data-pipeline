from faker import Faker
import json
import random
import uuid
from datetime import datetime, timezone, timedelta
import os

fake = Faker()

os.makedirs("data", exist_ok=True)

# Generate users
users = []
for user_id in range(1, 1001):
    users.append({
        "user_id": user_id,
        "name": fake.name(),
        "email": fake.email(),
        "country": fake.country(),
        "registration_date": fake.date_between(start_date="-3y", end_date="today").isoformat()
    })

with open("data/users.json", "w") as f:
    json.dump(users, f, indent=2)

print(f"Generated {len(users)} users")

# Generate products
categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Beauty", "Toys", "Food"]

products = []
for product_id in range(1, 501):
    products.append({
        "product_id": product_id,
        "name": fake.word().capitalize() + " " + fake.word().capitalize(),
        "category": random.choice(categories),
        "price": round(random.uniform(5.0, 500.0), 2),
        "initial_stock": random.randint(50, 500)
    })

with open("data/products.json", "w") as f:
    json.dump(products, f, indent=2)

print(f"Generated {len(products)} products")

# Generate orders history
statuses = ["created", "shipped", "delivered", "cancelled"]

orders = []
for _ in range(10000):
    order_date = fake.date_between(start_date="-1y", end_date="today")
    orders.append({
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 500),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(5.0, 500.0), 2),
        "status": random.choice(statuses),
        "order_date": order_date.isoformat()
    })

with open("data/orders_history.json", "w") as f:
    json.dump(orders, f, indent=2)

print(f"Generated {len(orders)} orders")