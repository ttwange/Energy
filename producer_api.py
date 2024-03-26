import json
import requests
from kafka import KafkaProducer

def publish_to_kafka():
    kafka_broker ='localhost:9092'
    kafka_topic = 'electricity'

    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            retries=5, 
        )
    except Exception as e:
        print("Error initializing KafkaProducer:", e)
        return

    try:
        response = requests.get(url='https://api.energidataservice.dk/dataset/ElectricityBalanceNonv')
        response.raise_for_status()  
        result = response.json()

        for k, v in result.items():
            print(k, v)

        records = result.get('records', [])

        print('records:')
        for record in records:
            print(' ', record)
            publish_message(producer, kafka_topic, record)

        producer.flush() 
    finally:
        producer.close() 

def publish_message(producer, topic, message):
    producer.send(topic, json.dumps(message).encode('utf-8'))

publish_to_kafka()
