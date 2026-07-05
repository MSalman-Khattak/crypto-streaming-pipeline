import websocket
import json
import os
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# Load secrets from .env file
load_dotenv()
CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

# Create the Event Hub producer (this sends data to Azure)
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)

SOCKET_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

def on_message(ws, message):
    data = json.loads(message)

    trade_event = {
        "symbol": data["s"],       # e.g. BTCUSDT
        "price": data["p"],
        "quantity": data["q"],
        "trade_time": data["T"]
    }

    # Send to Event Hub
    event_batch = producer.create_batch()
    event_batch.add(EventData(json.dumps(trade_event)))
    producer.send_batch(event_batch)

    print(f"Sent to Event Hub: {trade_event}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    producer.close()

def on_open(ws):
    print("Connected to Binance! Sending live BTC trades to Event Hub...\n")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        SOCKET_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()