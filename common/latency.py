"""
Rolling end-to-end latency measurement.

"End-to-end" here means: time.time() when the producer handed the
message to Kafka, vs time.time() when a consumer finished reading it.
Since producer and every consumer are containers on the same Docker
host, they share one system clock -- no NTP/clock-skew correction
needed for this demo. (Across real machines, you'd need synchronized
clocks or you'd be measuring clock drift, not latency.)
"""
import statistics
import time
from collections import deque


class LatencyTracker:
    def __init__(self, label: str, maxlen: int = 1000, log_every: int = 20):
        self.label = label
        self.samples = deque(maxlen=maxlen)  # bounded so memory doesn't grow forever
        self.count = 0
        self.log_every = log_every

    def record(self, produced_at: float) -> float:
        """Call once per consumed message. Returns this message's latency in ms."""
        latency_ms = (time.time() - produced_at) * 1000
        self.samples.append(latency_ms)
        self.count += 1
        if self.count % self.log_every == 0:
            self.log_summary()
        return latency_ms

    def log_summary(self) -> None:
        if len(self.samples) < 2:
            return
        data = sorted(self.samples)
        p50 = statistics.median(data)
        p99 = data[max(0, int(len(data) * 0.99) - 1)]
        print(
            f"[{self.label}] latency over last {len(data)} msgs -> "
            f"p50={p50:.1f}ms  p99={p99:.1f}ms",
            flush=True,
        )
