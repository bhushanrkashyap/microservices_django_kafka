import json
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
