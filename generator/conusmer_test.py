from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ecommerce.orders',
    'ecommerce.navigation_events',
    'ecommerce.inventory_updates',
    bootstrap_servers='192.168.1.48:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    auto_offset_reset='latest',
    group_id='consumer_test'
)

for message in consumer:
    print(f"Topic: {message.topic} | Partition: {message.partition} | Offset: {message.offset}")
    print(f"Key: {message.key} | Value: {message.value}")
    print("---")