"""
Persistence consumer.

Independently reads market-ticks (its own consumer group,
persistence-group) and writes batches of ticks to Parquet files under
data/market_ticks/ for later analysis.

Parquet is columnar and not meant to be appended to row-by-row, so
rather than one giant growing file, we write one small file per
flushed batch -- the standard "data lake" pattern: read the whole
directory back later with pandas.read_parquet(dir) or a pyarrow
dataset, not any single file in isolation.

Offsets are committed only AFTER a batch is actually written to disk.
If this process dies mid-batch, those messages were never committed,
so they get reprocessed and rewritten (as a new file) on restart --
duplicated, never lost. Same at-least-once contract as the other two
consumers, just applied to a batch instead of a single message.
"""
import json
import os
import signal
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, "/app")
from common.kafka_config import consumer_config
from common.latency import LatencyTracker

TICK_TOPIC = os.environ.get("TICK_TOPIC", "market-ticks")
GROUP_ID = os.environ.get("CONSUMER_GROUP", "persistence-group")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
FLUSH_INTERVAL_SEC = float(os.environ.get("FLUSH_INTERVAL_SEC", "10"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/data/market_ticks")

running = True


def handle_shutdown(signum, frame):
    global running
    print(f"[persistence] received signal {signum}, shutting down...", flush=True)
    running = False


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def flush_batch(rows: list) -> None:
    if not rows:
        return
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    filename = f"batch_{int(time.time() * 1000)}_{len(rows)}rows.parquet"
    path = os.path.join(OUTPUT_DIR, filename)
    pq.write_table(table, path)
    print(f"[persistence] wrote {len(rows)} rows -> {path}", flush=True)


def main():
    consumer = Consumer(consumer_config(GROUP_ID))
    consumer.subscribe([TICK_TOPIC])
    latency = LatencyTracker(label="persistence")

    rows = []
    last_flush_time = time.time()

    print(
        f"[persistence] consumer group={GROUP_ID} reading {TICK_TOPIC}, "
        f"batch_size={BATCH_SIZE} flush_interval={FLUSH_INTERVAL_SEC}s -> {OUTPUT_DIR}",
        flush=True,
    )

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[persistence] consumer error: {msg.error()}", file=sys.stderr)
                else:
                    tick = json.loads(msg.value())
                    latency.record(tick["produced_at"])
                    rows.append(tick)

            time_since_flush = time.time() - last_flush_time
            should_flush = len(rows) >= BATCH_SIZE or (rows and time_since_flush >= FLUSH_INTERVAL_SEC)

            if should_flush:
                flush_batch(rows)
                # Commits the CURRENT POSITION for every partition this
                # consumer owns (not just whichever partition the most
                # recent message happened to be on) -- important here
                # because we're reading 3 partitions but only flushing
                # periodically, unlike pricing/risk which commit every
                # single message.
                consumer.commit(asynchronous=False)
                print(f"[persistence] committed offsets after flushing {len(rows)} rows", flush=True)
                rows = []
                last_flush_time = time.time()
    finally:
        if rows:
            flush_batch(rows)
            consumer.commit(asynchronous=False)
        print("[persistence] closing consumer...", flush=True)
        consumer.close()


if __name__ == "__main__":
    main()
