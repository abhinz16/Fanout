"""
Risk consumer.

Independently reads market-ticks (its own consumer group, risk-group),
tracks the last price per symbol, and flags any tick-to-tick move
beyond RISK_THRESHOLD_PCT as a breach.

Real risk systems check many more conditions (position limits,
exposure, correlated-asset moves). This is a deliberately minimal
single-tick circuit-breaker check -- the point of this container isn't
sophisticated risk logic, it's demonstrating that an independent
consumer group can react to the same stream the pricing consumer is
reading, on its own schedule, without the two coordinating at all.
"""
import json
import os
import signal
import sys

from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, "/app")
from common.kafka_config import consumer_config
from common.latency import LatencyTracker

TICK_TOPIC = os.environ.get("TICK_TOPIC", "market-ticks")
GROUP_ID = os.environ.get("CONSUMER_GROUP", "risk-group")
THRESHOLD_PCT = float(os.environ.get("RISK_THRESHOLD_PCT", "0.002"))  # 0.2% single-tick move

running = True


def handle_shutdown(signum, frame):
    global running
    print(f"[risk] received signal {signum}, shutting down...", flush=True)
    running = False


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def main():
    consumer = Consumer(consumer_config(GROUP_ID))
    consumer.subscribe([TICK_TOPIC])
    latency = LatencyTracker(label="risk")
    last_price = {}
    alert_count = 0
    processed_count = 0

    print(f"[risk] consumer group={GROUP_ID} reading {TICK_TOPIC} threshold={THRESHOLD_PCT:.3%}", flush=True)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[risk] consumer error: {msg.error()}", file=sys.stderr)
                continue

            tick = json.loads(msg.value())
            symbol = tick["symbol"]
            last = tick["last"]
            lat_ms = latency.record(tick["produced_at"])
            processed_count += 1

            prev = last_price.get(symbol)
            if prev is not None:
                pct_change = (last - prev) / prev
                if abs(pct_change) >= THRESHOLD_PCT:
                    alert_count += 1
                    direction = "UP" if pct_change > 0 else "DOWN"
                    print(
                        f"[risk] ALERT #{alert_count} {symbol} moved {pct_change:+.3%} {direction} "
                        f"({prev:.2f} -> {last:.2f}) partition={msg.partition()} offset={msg.offset()}",
                        flush=True,
                    )
            last_price[symbol] = last

            # Same per-message synchronous commit pattern as pricing-consumer:
            # simplest to reason about, unambiguous in the offset-recovery demo.
            consumer.commit(msg, asynchronous=False)

            if processed_count % 100 == 0:
                print(
                    f"[risk] processed={processed_count} alerts={alert_count} "
                    f"latest_latency={lat_ms:.1f}ms",
                    flush=True,
                )
    finally:
        print(f"[risk] closing... total processed={processed_count} total alerts={alert_count}", flush=True)
        consumer.close()


if __name__ == "__main__":
    main()
