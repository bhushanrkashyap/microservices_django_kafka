from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate
from confluent_kafka import Producer
import json

producer = Producer({"bootstrap.servers": "localhost:9092"})

def delivery_report(err, msg):
    if err is not None:
        print(f" Message delivery failed: {err}")
    else:
        print(f" Message delivered to {msg.topic()} [{msg.partition()}]")

@csrf_exempt
def user(request):
    if request.method == "POST":
        data = json.loads(request.body)

        user = authenticate(
            username=data["username"],
            password=data["password"]
        )

        if not user:
            return JsonResponse({"authenticated": False}, status=401)

        event = {
            "user_id": user.id,
            "username": user.username
        }

        print(f" Producing event: {event}")
        producer.produce(
            topic="user_events",
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )
        producer.flush()

        return JsonResponse({"authenticated": True}, status=200)
