#!/bin/python3

from kafka import KafkaProducer
import json
import random
from time import sleep
from datetime import datetime

events = ['OPEN', 'CLOSE', 'NAVIGATE', 'IDLE']
apps = ['PK', 'IFOOD', 'SYMPLA', 'SUPER PLAYER']

dataset = json.loads(open('dataset-people.json').read())
print("dataset from file: "  + str(len(dataset)))


def generate_random() :
    body = {}
    body["app"] = random.choice(apps)
    body["event"] = random.choice(events)
    body["date_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    return json.dumps(body)

def generate_from_file() :
    entry = random.choice(dataset)
    return json.dumps(entry)

# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: str(v).encode('utf-8'))

print("Ctrl+c to Stop")
while True:
    #Call the producer.send method with a producer-record
    json_str = generate_from_file()
    producer.send('WAVY.TOPIC.1', json_str)
    print("produced: " + json_str)
    sleep(0.3)

