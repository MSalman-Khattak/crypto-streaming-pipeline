import websocket
import json

# Binance WebSocket URL for live Bitcoin price (BTC/USDT)
SOCKET_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

def on_message(ws, message):
    data = json.loads(message)
    price = data['p']       # trade price
    quantity = data['q']    # trade quantity
    trade_time = data['T']  # timestamp
    print(f"BTC Price: ${price} | Quantity: {quantity} | Time: {trade_time}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to Binance! Streaming live BTC prices...\n")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        SOCKET_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()