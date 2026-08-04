"""
Publishes bid/ask/last tick data to the market-ticks topic.

Two modes, controlled by the PRODUCER_MODE env var:
  simulate  (default) -- deterministic random-walk prices, no external
             dependency. This is what runs by default so the demo is
             reproducible and doesn't depend on an exchange being up.
  coinbase  -- subscribes to Coinbase's public "ticker" channel over
             websocket and republishes real trade data into Kafka.
             No API key needed; it's public market data.

Either way, every message is stamped with `produced_at` = time.time()
right before it's handed to the producer -- that's the timestamp every
downstream consumer measures its latency against.
"""
import json
import os
import random
import signal
import sys
import threading
import time

from confluent_kafka import Producer

sys.path.insert(0, "/app")
from common.kafka_config import producer_config

TOPIC = os.environ.get("TICK_TOPIC", "market-ticks")
MODE = os.environ.get("PRODUCER_MODE", "simulate")
SYMBOLS = os.environ.get("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD").split(",")
TICK_INTERVAL_SEC = float(os.environ.get("TICK_INTERVAL_SEC", "0.2"))

running = True


def handle_shutdown(signum, frame):
    global running
    print(f"[producer] received signal {signum}, shutting down...", flush=True)
    running = False


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed for {msg.key()}: {err}", file=sys.stderr)


def make_producer() -> Producer:
    return Producer(producer_config(client_id="market-tick-producer"))


def publish_tick(producer: Producer, symbol: str, bid: float, ask: float, last: float, source_ts=None):
    tick = {
        "symbol": symbol,
        "bid": round(bid, 2),
        "ask": round(ask, 2),
        "last": round(last, 2),
        "source_ts": source_ts,      # upstream exchange timestamp, if any (coinbase mode)
        "produced_at": time.time(),  # our latency clock starts here
    }
    producer.produce(
        TOPIC,
        key=symbol.encode("utf-8"),   # same key -> same partition -> ordering preserved per symbol
        value=json.dumps(tick).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)  # non-blocking: lets queued delivery callbacks fire without waiting


# ---------------------------------------------------------------------
# Mode 1: simulate -- random-walk price generator
# ---------------------------------------------------------------------
def run_simulate():
    producer = make_producer()
    starting_prices = {"BTC-USD": 65000.0, "ETH-USD": 3200.0, "SOL-USD": 140.0}
    prices = {s: starting_prices.get(s, 100.0) for s in SYMBOLS}

    print(f"[producer] mode=simulate symbols={SYMBOLS} interval={TICK_INTERVAL_SEC}s", flush=True)
    tick_count = 0
    try:
        while running:
            for symbol in SYMBOLS:
                # small pct random walk, mildly mean-reverting so prices don't drift to zero/infinity
                pct_change = random.gauss(0, 0.0006)
                prices[symbol] *= (1 + pct_change)
                spread = prices[symbol] * 0.0003
                last = prices[symbol]
                bid = last - spread / 2
                ask = last + spread / 2
                publish_tick(producer, symbol, bid, ask, last)
                tick_count += 1
            if tick_count % 50 == 0:
                print(f"[producer] published {tick_count} ticks so far", flush=True)
            time.sleep(TICK_INTERVAL_SEC)
    finally:
        print("[producer] flushing remaining messages...", flush=True)
        producer.flush(10)


# ---------------------------------------------------------------------
# Mode 2: coinbase -- real market data over public websocket feed
# ---------------------------------------------------------------------
def run_coinbase():
    import websocket  # imported here so `simulate` mode never needs this dependency at runtime

    producer = make_producer()
    ws_url = "wss://ws-feed.exchange.coinbase.com"

    def on_open(ws):
        sub = {"type": "subscribe", "product_ids": SYMBOLS, "channels": ["ticker"]}
        ws.send(json.dumps(sub))
        print(f"[producer] subscribed to Coinbase ticker for {SYMBOLS}", flush=True)

    def on_message(ws, message):
        data = json.loads(message)
        if data.get("type") != "ticker":
            return
        symbol = data.get("product_id")
        try:
            last = float(data["price"])
            bid = float(data.get("best_bid", last))
            ask = float(data.get("best_ask", last))
        except (KeyError, TypeError, ValueError):
            return
        publish_tick(producer, symbol, bid, ask, last, source_ts=data.get("time"))

    def on_error(ws, error):
        print(f"[producer] websocket error: {error}", file=sys.stderr)

    def on_close(ws, code, msg):
        print(f"[producer] websocket closed: {code} {msg}", flush=True)

    print(f"[producer] mode=coinbase symbols={SYMBOLS}", flush=True)
    try:
        while running:
            ws = websocket.WebSocketApp(
                ws_url, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            wst = threading.Thread(target=ws.run_forever, daemon=True)
            wst.start()
            while running and wst.is_alive():
                time.sleep(1)
            if running:
                print("[producer] websocket dropped, reconnecting in 3s...", flush=True)
                time.sleep(3)
    finally:
        producer.flush(10)


if __name__ == "__main__":
    if MODE == "coinbase":
        run_coinbase()
    else:
        run_simulate()
