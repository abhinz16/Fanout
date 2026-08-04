"""
Shared Kafka client configuration.

Every container in this project imports from here rather than building
its own config dict, so a change to (say) how we point at the broker
only has to happen in one place.
"""
import os


def bootstrap_servers() -> str:
    # kafka:29092 is the internal PLAINTEXT listener from docker-compose.yml
    # -- reachable from other containers on the pricing-net network, not
    # from your host machine (that's what the 9092 host-mapped port is for).
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def producer_config(client_id: str) -> dict:
    return {
        "bootstrap.servers": bootstrap_servers(),
        "client.id": client_id,
        # linger.ms: batch messages for up to 5ms before sending, trading a
        # small amount of latency for fewer, larger network requests.
        # librdkafka's own default is already 5ms; set explicitly so it's
        # a visible, deliberate choice rather than an invisible default.
        "linger.ms": 5,
    }


def consumer_config(group_id: str) -> dict:
    return {
        "bootstrap.servers": bootstrap_servers(),
        "group.id": group_id,
        # earliest: a consumer group with no committed offset yet (first
        # run, or after `docker compose down -v`) starts from the
        # beginning of the topic instead of only seeing new messages.
        "auto.offset.reset": "earliest",
        # This is the load-bearing setting for the fault-tolerance demo.
        # With auto-commit off, WE decide exactly when an offset is
        # considered "done" -- after we've finished processing the
        # message, not on a timer running behind our back. That's what
        # makes "kill mid-stream, restart, resume correctly" a real
        # guarantee instead of a coincidence.
        "enable.auto.commit": False,
    }
