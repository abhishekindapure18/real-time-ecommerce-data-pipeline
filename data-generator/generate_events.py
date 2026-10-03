import json
import random
import time
from datetime import datetime, timezone


users = list(range(1001, 1101))

products = [
    {
        "product_id": 101,
        "product_name": "Laptop",
        "category": "Electronics",
        "price": 55000
    },
    {
        "product_id": 102,
        "product_name": "Headphones",
        "category": "Electronics",
        "price": 2500
    },
    {
        "product_id": 103,
        "product_name": "Keyboard",
        "category": "Accessories",
        "price": 1800
    },
    {
        "product_id": 104,
        "product_name": "Mouse",
        "category": "Accessories",
        "price": 900
    },
    {
        "product_id": 105,
        "product_name": "Backpack",
        "category": "Fashion",
        "price": 2200
    }
]


def generate_event():

    product = random.choice(products)

    event = {
        "event_id": random.randint(100000, 999999),
        "user_id": random.choice(users),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": random.randint(1, 3),
        "event_type": random.choice(
            ["view", "cart", "purchase"]
        ),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return event


while True:

    event = generate_event()

    print(json.dumps(event))

    time.sleep(1)