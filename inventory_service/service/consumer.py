import json, os, sys, django

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inventory.settings")
django.setup()

from confluent_kafka import Consumer, KafkaError
from service.models import Inventory

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "inventory_service_group",
    "auto.offset.reset": "earliest"
}

c = Consumer(conf)
c.subscribe(["orders"])
print("Inventory consumer started")
while True:
    try:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, not an error
                continue
            print("Error:", msg.error())
            continue
        order_data = json.loads(msg.value().decode("utf-8"))
        product_name = order_data.get("product_name")
        quantity = int(order_data.get("quantity"))  # Convert to int
        x = Inventory.objects.filter(product_name=product_name).first()
        if x:
            try:
                if x.quantity >= quantity:
                    x.quantity -= quantity
                    x.save()
                    print(f"Inventory updated for product {product_name}. New quantity: {x.quantity}")
                else:
                    print(f"Insufficient inventory for product {product_name}. Available quantity: {x.quantity}")
            except Exception as e:
                print("Error:", e)
        else:
            print(f"Product {product_name} not found in inventory.")
    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Exiting...")
        break
        