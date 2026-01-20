import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

KAFKA_TOPIC = "ecommerce_events"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer
    )

def generate_event():
    event_types = ["click", "view", "add_to_cart", "purchase", "cart_abandoned"]
    products = ["Laptop", "Phone", "Headphones", "Monitor", "Keyboard", "Mouse"]
    users = list(range(1, 101)) 
    
    event_type = random.choice(event_types)
    product = random.choice(products) if event_type != "click" else None
    price = round(random.uniform(10.0, 2000.0), 2) if event_type in ["purchase", "add_to_cart"] else 0
    
    return {
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "user_id": random.choice(users),
        "product": product,
        "price": price
    }

def run_producer():
    producer = get_producer()
    print(f"Producer started. Sending events to {KAFKA_TOPIC}...")
    
    try:
        while True:
            event = generate_event()
            producer.send(KAFKA_TOPIC, event)
            print(f"Sent: {event}")
            time.sleep(random.uniform(0.1, 1.0))
            
    except KeyboardInterrupt:
        print("Producer stopped.")
        producer.close()

if __name__ == "__main__":
    run_producer()
