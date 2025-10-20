import logging
from kafka import KafkaProducer
from json import dumps
import json
import websocket


# logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BinanceProducer")

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

# Binance WebSocket endpoint
BINANCE_SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"
topic = "binance_trades"

#web socket
ws = websocket.WebSocket()
ws.connect(BINANCE_SOCKET)

#send Binance real-time stock data to kafka
while True :
    response = ws.recv()
    stockdata = json.loads(response)
    producer.send(topic, stockdata)
    logger.info(f"Sent trade: {stockdata['s']} @ {stockdata['p']} USD")

