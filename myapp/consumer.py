import json
import sys , os , django
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "order.settings")
django.setup()
from myapp.models import Inventory
from confluent_kafka import Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'orders-service',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(["orders"])

print("Kafka consumer started...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Error:", msg.error())
        continue
    

    data = json.loads(msg.value().decode('utf-8'))
    print("Received:", data)
    product_name = data.get('product_name')
    quantity = data.get('quantity')
    try:
        x = Inventory.objects.get(product_name=product_name).lower()
        if x and x.quantity > 0:
            x.quantity -=quantity
            x.save()
            print(f"Inventory updated for {product_name}. New quantity: {x.quantity}")
        else:
            print(f"Insufficient inventory for {product_name}")
    except Inventory.DoesNotExist:
        print(f"Product {product_name} does not exist in inventory")


        

