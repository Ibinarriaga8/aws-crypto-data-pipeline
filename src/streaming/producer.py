import subprocess
import json
import random
import re
import string
import time
from datetime import datetime
from typing import Any, Dict

from websocket import create_connection, WebSocketConnectionClosedException


def generate_session() -> str:
    """
    Generate a random TradingView session identifier.

    Returns:
        str: Session ID used for TradingView WebSocket communication.
    """
    string_length = 12
    letters = string.ascii_lowercase
    return "qs_" + "".join(random.choice(letters) for _ in range(string_length))


def prepend_header(content: str) -> str:
    """
    Add TradingView message header.

    Args:
        content (str): JSON message content.

    Returns:
        str: Encoded message with TradingView header.
    """
    return f"~m~{len(content)}~m~{content}"


def construct_message(func: str, param_list: list[Any]) -> str:
    """
    Build a TradingView JSON message.

    Args:
        func (str): TradingView function name.
        param_list (list[Any]): Parameters for the function.

    Returns:
        str: JSON-encoded message.
    """
    return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))


def create_message(func: str, param_list: list[Any]) -> str:
    """
    Create a full TradingView message with header.

    Args:
        func (str): TradingView function name.
        param_list (list[Any]): Parameters for the function.

    Returns:
        str: Fully encoded message ready to send.
    """
    return prepend_header(construct_message(func, param_list))


def send_message(ws, func: str, args: list[Any]) -> None:
    """
    Send a message through the WebSocket.

    Args:
        ws: Active WebSocket connection.
        func (str): TradingView function name.
        args (list[Any]): Function arguments.

    Handles:
        WebSocketConnectionClosedException: Reconnects if connection is closed.
    """
    try:
        ws.send(create_message(func, args))
    except WebSocketConnectionClosedException:
        print("Connection closed while sending message.")
        reconnect(ws)


def send_ping(ws) -> None:
    """
    Send a heartbeat ping to TradingView.

    Args:
        ws: Active WebSocket connection.
    """
    try:
        ws.send("~h~0")
    except Exception as e:
        print(f"Error sending ping: {e}")


def process_data(data: Dict[str, Any]) -> None:
    """
    Process incoming TradingView data and publish it to Kafka.

    Args:
        data (Dict[str, Any]): Raw market data payload.

    Behavior:
        - Extracts price, change, and percentage change
        - Formats a JSON message
        - Sends it to Kafka using kafka-console-producer
    """
    price = data.get("lp", "N/A")
    change = data.get("ch", "N/A")
    change_percentage = data.get("chp", "N/A")

    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    topic = "imat3b_XLM"

    message = f'"{{\\"price\\": {price}, \\"timestamp\\": \\"{timestamp}\\"}}"'

    bootstrap_servers = [
        "b-1-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198",
        "b-2-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198"
    ]

    bootstrap_string = ",".join(bootstrap_servers)

    kafka_cmd = f'''echo {message} | /home/ec2-user/kafka_2.13-3.6.0/bin/kafka-console-producer.sh \
    --bootstrap-server {bootstrap_string} \
    --producer.config ./config/client.properties \
    --topic {topic}'''

    subprocess.run(kafka_cmd, shell=True, capture_output=True, text=True)

    print(
        f"Published to topic {topic}: {timestamp} | "
        f"Price: {price}, Change: {change}, % Change: {change_percentage}"
    )


def reconnect(symbol_id: str) -> None:
    """
    Attempt to reconnect after a connection failure.

    Args:
        symbol_id (str): TradingView symbol identifier.
    """
    print("Reconnecting...")
    time.sleep(5)
    start_socket(symbol_id)


def start_socket(symbol_id: str) -> None:
    """
    Start the TradingView WebSocket connection and stream real-time data.

    Args:
        symbol_id (str): TradingView symbol identifier (e.g., 'COINBASE:XLMUSD').

    Behavior:
        - Opens WebSocket connection
        - Subscribes to symbol
        - Continuously listens for updates
        - Processes incoming market data
        - Handles reconnection on failure
    """
    session = generate_session()
    url = "wss://data.tradingview.com/socket.io/websocket"
    headers = json.dumps({"Origin": "https://data.tradingview.com"})

    try:
        ws = create_connection(url, headers=headers)
        print(f"Connected to {url}")

        send_message(ws, "quote_create_session", [session])
        send_message(ws, "quote_set_fields", [session, "lp", "ch", "chp", "volume"])
        send_message(ws, "quote_add_symbols", [session, symbol_id])

        while True:
            try:
                result = ws.recv()

                if result.startswith("~m~"):
                    data_match = re.search(r"\{.*\}", result)
                    if data_match:
                        message = json.loads(data_match.group(0))

                        if message.get("m") == "qsd":
                            process_data(message["p"][1]["v"])

                elif result.startswith("~h~"):
                    send_ping(ws)

            except WebSocketConnectionClosedException:
                print("Connection closed unexpectedly.")
                reconnect(symbol_id)
                break

            except Exception as e:
                print(f"Error processing message: {e}")
                continue

    except WebSocketConnectionClosedException as e:
        print(f"Connection error: {e}. Reconnecting in 5 seconds...")
        reconnect(symbol_id)

    except Exception as e:
        print(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
        reconnect(symbol_id)


if __name__ == "__main__":
    symbol_id = "COINBASE:XLMUSD"
    start_socket(symbol_id)