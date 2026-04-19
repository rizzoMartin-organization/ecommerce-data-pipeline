from kafka import KafkaConsumer
import json
import requests
import base64
from dotenv import load_dotenv
import os
import uuid
from datetime import datetime, timezone

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

batches = {
    'orders': [],
    'navigation_events': [],
    'inventory_updates': []
}
BATCH_SIZE = 10

consumer = KafkaConsumer(
    'ecommerce.orders',
    'ecommerce.navigation_events',
    'ecommerce.inventory_updates',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    auto_offset_reset='latest',
    group_id='dbfs_consumer'
)

def write_to_dbfs(data, path):
    batch = {
        "messages": data,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "message_count": len(data)
    }
    content = json.dumps(batch).encode('utf-8')
    
    response = requests.put(
        f"{DATABRICKS_HOST}/api/2.0/fs/files{path}?overwrite=true",
        headers={
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/octet-stream"
            },
        data=content
    )
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")
    return response.status_code


for message in consumer:
    topic_name = message.topic.split('.')[1]
    batches[topic_name].append(message.value)
    
    if len(batches[topic_name]) >= BATCH_SIZE:
        path = f"/Volumes/ecommerce/bronze/streaming_files/{topic_name}/batch_{uuid.uuid4()}.json"
        status = write_to_dbfs(data=batches[topic_name], path=path)
        print(f"Lote escrito en {path} - status {status}")
        batches[topic_name] = []