import websocket
import json
from kafka import KafkaProducer
import logging

# إعداد اللوجر
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BinanceProducer")

# Binance WebSocket endpoint
BINANCE_SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Kafka topic name
TOPIC = "binance_trades"

# إعداد Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="host.docker.internal:9092",  # لو أنت مشغل Kafka في Docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# دالة عند استقبال رسالة من Binance
def on_message(ws, message):
    data = json.loads(message)
    producer.send(TOPIC, value=data)
    logger.info(f"📈 Sent trade: {data['s']} @ {data['p']} USD")

def on_error(ws, error):
    logger.error(f"❌ WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.warning("🔒 WebSocket connection closed")

def on_open(ws):
    logger.info("✅ Connected to Binance stream")

# تشغيل WebSocket
ws = websocket.WebSocketApp(
    BINANCE_SOCKET,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)

logger.info("🚀 Starting Binance Kafka Producer...")
ws.run_forever()
