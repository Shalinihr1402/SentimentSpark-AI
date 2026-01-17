from kafka import KafkaProducer
import time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v.encode("utf-8")
)

messages = [
    "I love this product",
    "This app is bad",
    "Service is okay",
    "Amazing experience",
    "Worst update ever",
    "Feeling neutral today"
]

while True:
    msg = random.choice(messages)
    producer.send("sentiment-topic", msg)
    print("Sent:", msg)
    time.sleep(1)
