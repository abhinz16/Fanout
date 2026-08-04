# Fanout — Distributed Market-Data Pricing Pipeline

A market-data pipeline built to demonstrate two specific distributed-systems
properties with real, measured evidence rather than descriptions of how they
should work: **consumer-group fault tolerance via offset-based recovery**, and
**end-to-end latency under real conditions**.

Four independent processes, each its own Docker container, coordinating
through Kafka (KRaft mode — no ZooKeeper). No process talks to any other
process directly; all coordination happens through the log.

```
                                   ┌─────────────────────┐
                                   │   market-ticks       │
                                   │   (3 partitions)     │
  ┌───────────┐   produces        │                       │
  │ producer  │ ────────────────► │  partition 0 (ETH)   │
  │ (sim or   │                   │  partition 1 (empty) │
  │ Coinbase) │                   │  partition 2 (BTC,   │
  └───────────┘                   │              SOL)    │
                                   └───────────┬───────────┘
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    │                          │                          │
                    ▼                          ▼                          ▼
          ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────────┐
          │ pricing-consumer  │      │  risk-consumer    │      │ persistence-consumer  │
          │ group: pricing-   │      │ group: risk-group │      │ group: persistence-   │
          │ group             │      │                    │      │ group                 │
          │                   │      │ per-tick threshold │      │ batches 200 rows /    │
          │ rolling moving    │      │ breach alerts      │      │ 10s -> Parquet files  │
          │ avg + volatility  │      │                    │      │                       │
          └─────────┬─────────┘      └────────────────────┘      └───────────┬───────────┘
                    │                                                        │
                    ▼                                                        ▼
          ┌──────────────────┐                                    ┌──────────────────────┐
          │ pricing-signals   │                                    │  ./data/market_ticks/ │
          │ (1 partition)     │                                    │  *.parquet             │
          └───────────────────┘                                    └───────────────────────┘
```

Each consumer is in its own **consumer group**, which is what makes this a
fan-out, not a pipeline stage: all three read every message on
`market-ticks` independently, on their own schedule, with their own offset
checkpoint. Killing one has zero effect on the other two — proven below, not
just claimed.

## Why KRaft mode

Kafka's metadata (topic list, partition leadership, consumer group state)
used to require a separate ZooKeeper ensemble. KRaft mode moves that into
Kafka itself via a Raft consensus quorum, so this project runs a single
`apache/kafka` container acting as both broker and controller — one moving
part instead of two clustered systems.

## Design decisions worth explaining out loud

**Manual, synchronous offset commits, not auto-commit.** Every consumer sets
`enable.auto.commit: False` and commits explicitly after finishing work on a
message (`pricing-consumer`, `risk-consumer`: after every message;
`persistence-consumer`: after every flushed batch). This is what makes the
fault-tolerance guarantee real instead of a lucky coincidence — the
committed offset always reflects exactly what's been durably processed, not
whatever a background timer happened to save.

**At-least-once, not exactly-once.** If a consumer dies between processing a
message and committing its offset, that message gets reprocessed on
restart. Nothing is ever silently lost, but duplicates are possible. This is
a deliberate, named tradeoff — exactly-once would require idempotent writes
or transactional producers, which is more machinery than this project needs
to prove the point.

**Commit granularity varies by consumer, and that changes the blast radius
of a crash.** `pricing-consumer` and `risk-consumer` commit after every
single message, so a crash can cost at most ~1 in-flight message.
`persistence-consumer` commits only after a batch (up to 200 rows) is
written to disk — because Parquet is columnar and not meant to be appended
to row-by-row — so a crash mid-batch could mean up to 200 messages get
reprocessed. Same guarantee (at-least-once), different cost per failure.

**Volatility is computed on returns, not price level.** Standard deviation
of raw price conflates trend with variance; standard deviation of
percentage returns is the scale-free measure actually used for this
purpose.

**Message keying.** Every tick is published with `key=symbol`, so all of one
symbol's messages land on the same partition and preserve order. With 3
symbols over 3 partitions, BTC-USD and SOL-USD happened to hash to the same
partition (2), leaving partition 1 empty — expected with this few keys, not
a bug.

## Fault-tolerance demo: offset-based recovery

Two failure modes were tested against `pricing-consumer`, using Kafka's own
`kafka-consumer-groups.sh --describe` as the source of truth rather than
just this project's own log output.

### 1. Graceful stop (SIGTERM)

| | partition 0 | partition 2 |
|---|---|---|
| Before stop | offset 28039, LAG 0 | offset 56078, LAG 0 |
| While stopped (producer still running) | offset 28422, LAG 398 | offset 56845, LAG 795 |
| After restart, caught up | offset 29782, LAG 0 | offset 59564, LAG 0 |

`docker compose stop` sends SIGTERM. The registered signal handler flips a
`running` flag, letting the in-flight loop iteration finish and commit
before the process exits — offsets advanced slightly past the last snapshot
even while "stopping," confirming the graceful path works as intended.

### 2. Hard kill (SIGKILL, no shutdown code runs at all)

| | partition 0 |
|---|---|
| Committed offset immediately before kill | **30287** |
| `docker kill pricing-consumer` — no handler runs, no flush, no commit | — |
| First offset consumed after restart | **30287** |

Exact match. `docker kill` sends SIGKILL, which gives the process no chance
to run any cleanup code whatsoever. The consumer resumed at precisely the
offset Kafka had on record — proof the guarantee holds because of the
commit-after-process design, not because of any graceful-shutdown code
happening to run.

Post-kill, latency briefly read >100,000ms (see below) — not a bug, but the
outage duration itself, timestamped in the data.

## Latency: real numbers, not adjectives

Every tick is stamped `produced_at = time.time()` immediately before being
handed to the Kafka producer. Every consumer computes
`latency = time.time() - produced_at` on receipt and tracks a rolling
p50/p99 over the last 1000 messages. Producer and all consumers run as
containers on the same Docker host, sharing one system clock — no NTP
correction needed for this measurement to be meaningful (across real
machines, clock sync would matter).

**Two very different regimes showed up in the data, and telling them apart
matters:**

- **Backlog replay** — when a consumer group has no prior committed offset
  (first run) or has been down for a while, `auto.offset.reset: earliest`
  means it starts from wherever it left off, potentially far behind the
  live tip of the topic. Every backlogged message's `produced_at` is old,
  so measured "latency" during replay reflects the length of the backlog,
  not pipeline speed. This showed up clearly after the SIGKILL test: p99
  briefly read ~100 seconds, decaying back down as the consumer drained the
  gap — a real, useful signal (it timestamps the outage), just not a
  steady-state latency number.
- **Steady state** — once caught up to the live tip, latency reflects the
  actual pipeline: network round trip to the broker, JSON parse, and
  whatever processing that consumer does.

**Steady-state numbers** (rolling 1000-message window, post-recovery):

| Consumer | p50 | p99 | Commit pattern |
|---|---|---|---|
| pricing-consumer | 8.6 ms | 10.6 ms | every message |
| risk-consumer | 8.4 ms | 10.1 ms | every message |
| persistence-consumer | 7.4 ms | ~20.0 ms | every ~10s batch |

`persistence-consumer`'s lower p50 but roughly 2x higher p99 than the
per-message committers is a real, explainable effect: its periodic
`pq.write_table()` call blocks that iteration of the poll loop on actual
disk I/O, so whichever tick happens to arrive right after a flush sits in
librdkafka's fetch buffer slightly longer before being returned — a
periodic tail effect, invisible in the median, visible in p99. Batching
buys write efficiency (12 large flushes instead of ~1350 tiny disk writes,
at this throughput) in exchange for a worse tail latency. Neither number is
"wrong"; they're different design points on the same tradeoff.

## Running it

```bash
docker compose up -d --build
docker compose logs -f pricing-consumer risk-consumer persistence-consumer
```

Topics are created automatically by the `kafka-init` one-shot service on
first startup. Producer defaults to `PRODUCER_MODE=simulate` (deterministic
random-walk prices, no external dependency); set `PRODUCER_MODE=coinbase` in
`docker-compose.yml` to instead subscribe to Coinbase's public ticker
websocket feed for real market data.

**To reproduce the fault-tolerance demo:**
```bash
# check current committed offsets
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group pricing-group

# graceful stop
docker compose stop pricing-consumer
# ... wait, backlog builds ...
docker compose start pricing-consumer

# hard kill
docker kill pricing-consumer
# ... wait, backlog builds ...
docker compose up -d pricing-consumer
```

## Possible extensions

- Exactly-once semantics via idempotent producer + transactional writes
- Schema registry instead of hand-rolled JSON
- Horizontal scaling: run 3 instances of `pricing-consumer` in the same
  group to use all 3 partitions in parallel
- Replace per-batch Parquet files with a proper table format (Iceberg/Delta)
  for compaction and time-travel queries
- Cross-machine deployment, which would require NTP-synchronized clocks for
  the latency measurement to remain valid
