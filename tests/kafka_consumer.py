from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json


consumer = KafkaConsumer(
    'binance_trades',
    bootstrap_servers = 'host.docker.internal:9092',
    value_deserializer = lambda v: loads(v.decode('utf-8')),
)

for c in consumer:
    print(c.value)