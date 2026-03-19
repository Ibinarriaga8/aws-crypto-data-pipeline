import json
import logging
import random
import re
import string
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Optional

from websocket import WebSocket, WebSocketConnectionClosedException, create_connection


logger = logging.getLogger(__name__)

TRADINGVIEW_WS_URL = "wss://data.tradingview.com/socket.io/websocket"
TRADINGVIEW_HEADERS = json.dumps({"Origin": "https://data.tradingview.com"})
DEFAULT_RECONNECT_SECONDS = 5


def generate_session(prefix: str = "qs_") -> str:
    """
    Generate a random TradingView session identifier.

    Args:
        prefix: Prefix used by TradingView for the session.

    Returns:
        A randomly generated session identifier.
    """
    return prefix + "".join(random.choice(string.ascii_lowercase) for _ in range(12))


def prepend_header(content: str) -> str:
    """
    Add the TradingView framing header to a message.

    Args:
        content: JSON payload to frame.

    Returns:
        Framed message expected by TradingView WebSocket.
    """
    return f"~m~{len(content)}~m~{content}"


def construct_message(function_name: str, parameters: list[Any]) -> str:
    """
    Build a TradingView JSON message.

    Args:
        function_name: TradingView function name.
        parameters: Parameters passed to the function.

    Returns:
        Serialized JSON message.
    """
    return json.dumps({"m": function_name, "p": parameters}, separators=(",", ":"))


def create_message(function_name: str, parameters: list[Any]) -> str:
    """
    Create a framed TradingView WebSocket message.

    Args:
        function_name: TradingView function name.
        parameters: Parameters passed to the function.

    Returns:
        Fully framed message ready to send.
    """
    return prepend_header(construct_message(function_name, parameters))


def send_message(ws: WebSocket, function_name: str, arguments: list[Any]) -> None:
    """
    Send a message through the WebSocket connection.

    Args:
        ws: Active WebSocket connection.
        function_name: TradingView function name.
        arguments: Parameters passed to the function.

    Raises:
        WebSocketConnectionClosedException: If the socket is closed.
    """
    ws.send(create_message(function_name, arguments))


def send_ping(ws: WebSocket) -> None:
    """
    Send a heartbeat response to TradingView.

    Args:
        ws: Active WebSocket connection.
    """
    ws.send("~h~0")


def parse_message(raw_message: str) -> Optional[Dict[str, Any]]:
    """
    Parse a raw TradingView WebSocket message.

    Args:
        raw_message: Raw text received from the WebSocket.

    Returns:
        Parsed JSON payload if present; otherwise None.
    """
    if not raw_message.startswith("~m~"):
        return None

    match = re.search(r"\{.*\}", raw_message)
    if not match:
        return None

    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        logger.warning("Failed to decode TradingView message.")
        return None


def build_market_event(symbol_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a normalized market event from a TradingView payload.

    Args:
        symbol_id: Symbol being streamed.
        payload: Raw data payload from TradingView.

    Returns:
        Normalized dictionary with market event fields.
    """
    return {
        "symbol": symbol_id,
        "price": payload.get("lp"),
        "change": payload.get("ch"),
        "change_percentage": payload.get("chp"),
        "volume": payload.get("volume"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def connect_and_subscribe(symbol_id: str) -> WebSocket:
    """
    Open a WebSocket connection and subscribe to a symbol.

    Args:
        symbol_id: TradingView symbol identifier, for example 'BINANCE:BTCUSD'.

    Returns:
        Active subscribed WebSocket connection.
    """
    session = generate_session()
    ws = create_connection(TRADINGVIEW_WS_URL, headers=TRADINGVIEW_HEADERS)

    send_message(ws, "quote_create_session", [session])
    send_message(ws, "quote_set_fields", [session, "lp", "ch", "chp", "volume"])
    send_message(ws, "quote_add_symbols", [session, symbol_id])

    logger.info("Connected to TradingView and subscribed to %s", symbol_id)
    return ws


def stream_market_data(
    symbol_id: str,
    reconnect_seconds: int = DEFAULT_RECONNECT_SECONDS,
) -> Generator[Dict[str, Any], None, None]:
    """
    Continuously stream market events for a TradingView symbol.

    Args:
        symbol_id: TradingView symbol identifier.
        reconnect_seconds: Delay before reconnecting after a failure.

    Yields:
        Normalized market event dictionaries.
    """
    while True:
        ws: Optional[WebSocket] = None

        try:
            ws = connect_and_subscribe(symbol_id)

            while True:
                raw_message = ws.recv()

                if raw_message.startswith("~h~"):
                    send_ping(ws)
                    continue

                parsed_message = parse_message(raw_message)
                if not parsed_message:
                    continue

                if parsed_message.get("m") != "qsd":
                    continue

                payload = parsed_message["p"][1]["v"]
                yield build_market_event(symbol_id, payload)

        except WebSocketConnectionClosedException:
            logger.warning(
                "TradingView connection closed unexpectedly. Reconnecting in %s seconds.",
                reconnect_seconds,
            )
            time.sleep(reconnect_seconds)
        except Exception as exc:
            logger.exception(
                "Unexpected streaming error for %s: %s. Reconnecting in %s seconds.",
                symbol_id,
                exc,
                reconnect_seconds,
            )
            time.sleep(reconnect_seconds)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    for event in stream_market_data("BINANCE:BTCUSD"):
        logger.info(
            "[%s] %s | price=%s | change=%s | change%%=%s | volume=%s",
            event["timestamp"],
            event["symbol"],
            event["price"],
            event["change"],
            event["change_percentage"],
            event["volume"],
        )