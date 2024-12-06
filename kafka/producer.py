from kafka import KafkaProducer
import json
import time
import random

def generate_wifi_logs():
    return {
        "customer_id": random.randint(1, 1000),
        "timestamp": int(time.time()),
        "device_type": random.choice(["mobile", "laptop", "tablet"]),
        "session_duration": random.randint(5, 300)  # in seconds
    }

def generate_product_views():
    return {
        "customer_id": random.randint(1, 1000),
        "product_id": random.randint(1, 100),
        "timestamp": int(time.time())
    }

def generate_purchases():
    return {
        "order_id": random.randint(1, 10000),
        "customer_id": random.randint(1, 1000),
        "product_id": random.randint(1, 100),
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": int(time.time())
    }

def produce_data(producer, topic, generate_function, interval=1):
    while True:
        message = generate_function()
        producer.send(topic, value=message)
        print(f"Produced to {topic}: {message}")
        time.sleep(interval)

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        produce_data(producer, "wifi-logs", generate_wifi_logs, interval=2)
        # Uncomment below lines to produce data to other topics
        # produce_data(producer, "product-views", generate_product_views, interval=2)
        # produce_data(producer, "purchases", generate_purchases, interval=2)
    except KeyboardInterrupt:
        print("Shutting down producer.")
