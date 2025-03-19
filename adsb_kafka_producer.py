
from kafka import KafkaProducer
import requests
import json
import time

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "adsb"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    try:
        response = requests.get("http://127.0.0.1:8080/data/aircraft.json")
        data = response.json()
        
        for aircraft in data.get("aircraft", []):
            producer.send(KAFKA_TOPIC, aircraft)
            print(f"{aircraft}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    time.sleep(1)
