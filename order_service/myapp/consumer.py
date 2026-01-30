#!/usr/bin/env python3
"""
Kafka Consumer for Order Service
Listens to user_events topic and processes user authentication events.
"""
import os
import sys
import json
import signal
import logging
from datetime import datetime

# Django setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "order.settings")

import django
django.setup()

from confluent_kafka import Consumer, KafkaError, KafkaException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("order_service.consumer")

# Kafka Configuration - uses environment variables with sensible defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-service-consumer")
KAFKA_USER_EVENTS_TOPIC = os.getenv("KAFKA_USER_EVENTS_TOPIC", "user_events")

kafka_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
    "session.timeout.ms": 30000,
    "max.poll.interval.ms": 300000,
}

# Graceful shutdown handling
running = True


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global running
    logger.info("Shutdown signal received, closing consumer...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def process_user_event(data: dict) -> None:
    """
    Process user authentication event.
    Add your business logic here (e.g., create order records, update caches, etc.)
    """
    event_type = data.get("event_type", "unknown")
    user_id = data.get("user_id")
    username = data.get("username")
    
    logger.info(f"Processing event: type={event_type}, user_id={user_id}, username={username}")
    
    # TODO: Add your business logic here
    # Example: Create initial order record, update user activity, etc.


def run_consumer():
    """Main consumer loop."""
    consumer = Consumer(kafka_config)
    
    try:
        consumer.subscribe([KAFKA_USER_EVENTS_TOPIC])
        logger.info(f"Consumer started - listening to topic: {KAFKA_USER_EVENTS_TOPIC}")
        logger.info(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Consumer group: {KAFKA_GROUP_ID}")
        
        while running:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode("utf-8"))
                logger.info(f"Received message from partition {msg.partition()}, offset {msg.offset()}")
                process_user_event(data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KafkaException as e:
        logger.error(f"Kafka exception: {e}")
    finally:
        logger.info("Closing consumer...")
        consumer.close()
        logger.info("Consumer closed successfully")


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("Order Service Kafka Consumer Starting")
    logger.info("=" * 50)
    run_consumer()
