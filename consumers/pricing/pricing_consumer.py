"""
Pricing consumer.

Independently reads every message on market-ticks (its own consumer
group == its own copy of the stream), maintains a rolling per-symbol
price window, computes a moving average and a volatility estimate, and
publishes the result to pricing-signals.

Offset handling: auto-commit is off (see common/kafka_config.py). We
commit synchronously, right after a message is fully processed and its
derived signal published. That ordering is what the offset-recovery
demo in step 4 depends on: if this process dies before a commit
happens, that message gets reprocessed on restart (at-least-once, not
exactly-once) -- but nothing committed is ever silently skipped or
replayed. Production systems usually batch commits every N messages or
every few seconds for throughput; this project commits every single
message on purpose, so the offset shown in the logs is unambiguous
when you're watching the fault-tolerance demo happen live.
"""
import json
import os
import statistics
import sys
import time
from collections import defaultdict, deque

from confluent_kafka import Consumer, KafkaError, Producer

sys.path.insert(0, "/app")
from common.kafka_config import consumer_config, producer_config
from common.latency import LatencyTracker

TICK_TOPIC = os.environ.get("TICK_TOPIC", "market-ticks")
SIGNAL_TOPIC = os.environ.get("SIGNAL_TOPIC", "pricing-signals")
GROUP_ID = os.environ.get("CONSUMER_GROUP", "pricing-group")
WINDOW_SIZE = int(os.environ.get("WINDOW_SIZE", "20"))

running = True


def handle_shutdown(signum, frame):
    global running
    print(f"[pricing] received signal {signum}, shutting down...", flush=True)
    running = False


import signal
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def compute_signal(window: deque):
    """Moving average of price level, volatility of returns (not price level --
    stdev of raw price conflates trend with variance; returns are scale-free)."""
    prices = list(window)
    moving_avg = statistics.mean(prices)
    if len(prices) >= 3:
        returns = [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(1, len(prices))]
        volatility = statistics.pstdev(returns)
    else:
        volatility = 0.0
    return moving_avg, volatility


def main():
    consumer = Consumer(consumer_config(GROUP_ID))
    consumer.subscribe([TICK_TOPIC])
    signal_producer = Producer(producer_config(client_id="pricing-signal-producer"))

    windows = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
    latency = LatencyTracker(label="pricing")

    print(f"[pricing] consumer group={GROUP_ID} reading {TICK_TOPIC} -> {SIGNAL_TOPIC}", flush=True)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[pricing] consumer error: {msg.error()}", file=sys.stderr)
                continue

            tick = json.loads(msg.value())
            symbol = tick["symbol"]
            last_price = tick["last"]
            lat_ms = latency.record(tick["produced_at"])

            window = windows[symbol]
            window.append(last_price)
            moving_avg, volatility = compute_signal(window)

            out = {
                "symbol": symbol,
                "moving_avg": round(moving_avg, 4),
                "volatility": round(volatility, 6),
                "window_size": len(window),
                "produced_at": time.time(),
            }
            signal_producer.produce(
                SIGNAL_TOPIC,
                key=symbol.encode("utf-8"),
                value=json.dumps(out).encode("utf-8"),
            )
            signal_producer.poll(0)

            # Commit AFTER the derived signal is published -- this is the
            # line that defines our fault-tolerance guarantee. Everything
            # before it might get redone on a crash; nothing after it
            # ever gets silently lost.
            consumer.commit(msg, asynchronous=False)

            print(
                f"[pricing] {symbol} partition={msg.partition()} offset={msg.offset()} "
                f"latency={lat_ms:.1f}ms avg={moving_avg:.2f} vol={volatility:.5f}",
                flush=True,
            )
    finally:
        print("[pricing] closing consumer...", flush=True)
        consumer.close()
        signal_producer.flush(10)


if __name__ == "__main__":
    main()
