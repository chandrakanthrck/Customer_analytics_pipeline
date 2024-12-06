from kafka import KafkaConsumer
import boto3
import json
import os
from datetime import datetime

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_mapping = {
    "wifi-logs": "customer-analytics-wifi-logs",
    "product-views": "customer-analytics-product-views",
    "purchases": "customer-analytics-purchases"
}

# Save data to S3
def save_to_s3(bucket_name, topic, data):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = f"{topic}/{timestamp}.json"
    temp_file = f"/tmp/{file_name}"

    # Write data to a temporary local file
    with open(temp_file, "w") as f:
        f.write(json.dumps(data, indent=4))

    # Upload to S3
    s3_client.upload_file(temp_file, bucket_name, file_name)
    print(f"Uploaded {file_name} to S3 bucket {bucket_name}")

    # Remove the temporary file
    os.remove(temp_file)

# Kafka Consumer Configuration
def consume_and_store(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    batch = []
    batch_size = 10  # Number of records to batch before uploading
    print(f"Consuming data from topic: {topic}")

    for message in consumer:
        batch.append(message.value)

        if len(batch) >= batch_size:
            save_to_s3(bucket_mapping[topic], topic, batch)
            batch = []  # Clear batch after uploading

if __name__ == "__main__":
    try:
        # Uncomment one of the topics below to consume and store its data
        consume_and_store("wifi-logs")
        # consume_and_store("product-views")
        # consume_and_store("purchases")
    except KeyboardInterrupt:
        print("Shutting down consumer.")
