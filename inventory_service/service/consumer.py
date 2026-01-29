import os
import sys
import django
import json
from confluent_kafka import Consumer

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inventory.settings")
django.setup()

from service.models import Inventory

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "inventory-service",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe(["orders"])

print(" Inventory service consumer started...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Error:", msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))
    print(" Order received:", data)

    product = data["product_name"]
    qty = int(data["quantity"])

    try:
        inventory = Inventory.objects.get(product_name=product)
        inventory.quantity -= qty
        inventory.save()

        print(f" Stock updated for {product}")

    except Inventory.DoesNotExist:
        print(f" Product not found: {product}")
