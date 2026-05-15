#!/usr/bin/env python3
"""
Streaming processor: consumes raw telemetry from Kafka, applies 5-minute tumbling windows,
aggregates metrics per cell tower, and writes to MongoDB Atlas.

Uses confluent_kafka consumer with in-process windowing. Maintains running statistics
to avoid storing all events in memory (supports high-throughput ingestion).
"""

import os
import json
import time
import signal
import math
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telco-raw-telemetry")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "ods_demo_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "windowed_network_metrics")
WINDOW_SIZE_MINUTES = int(os.getenv("WINDOW_SIZE_MINUTES", "5"))

running = True


def shutdown_handler(signum, frame):
    global running
    print("\nShutting down...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

METRIC_FIELDS = [
    "signal_strength_dbm",
    "throughput_mbps",
    "latency_ms",
    "call_drop_rate_percent",
    "packet_loss_percent",
    "jitter_ms",
]


class RunningStats:
    """Maintains running min/max/sum/count and a reservoir sample for p95."""

    __slots__ = ['count', 'total', 'min_val', 'max_val', 'reservoir', 'reservoir_size']

    def __init__(self, reservoir_size=1000):
        self.count = 0
        self.total = 0.0
        self.min_val = float('inf')
        self.max_val = float('-inf')
        self.reservoir = []
        self.reservoir_size = reservoir_size

    def add(self, value):
        self.count += 1
        self.total += value
        if value < self.min_val:
            self.min_val = value
        if value > self.max_val:
            self.max_val = value
        # Reservoir sampling for p95
        if len(self.reservoir) < self.reservoir_size:
            self.reservoir.append(value)
        else:
            j = int(self.count * (len(self.reservoir) / self.count))
            import random
            j = random.randint(0, self.count - 1)
            if j < self.reservoir_size:
                self.reservoir[j] = value

    def result(self):
        if self.count == 0:
            return {"avg": 0, "min": 0, "max": 0, "p95": 0}
        self.reservoir.sort()
        p95_idx = int(len(self.reservoir) * 0.95)
        return {
            "avg": self.total / self.count,
            "min": self.min_val,
            "max": self.max_val,
            "p95": self.reservoir[min(p95_idx, len(self.reservoir) - 1)],
        }


class CellWindowState:
    """Tracks running stats for a single cell within a window."""

    __slots__ = ['region', 'event_count', 'anomaly_count', 'metrics']

    def __init__(self, region):
        self.region = region
        self.event_count = 0
        self.anomaly_count = 0
        self.metrics = {field: RunningStats() for field in METRIC_FIELDS}

    def add_event(self, event):
        self.event_count += 1
        if is_anomaly(event):
            self.anomaly_count += 1
        for field in METRIC_FIELDS:
            val = event.get(field)
            if val is not None:
                self.metrics[field].add(val)


def is_anomaly(event):
    return (
        event.get("latency_ms", 0) > 100
        or event.get("signal_strength_dbm", 0) < -80
        or event.get("call_drop_rate_percent", 0) > 2.0
        or event.get("packet_loss_percent", 0) > 5.0
    )


def get_window_key(timestamp_s):
    window_seconds = WINDOW_SIZE_MINUTES * 60
    return (timestamp_s // window_seconds) * window_seconds


def flush_window(cell_states, window_end_ts, collection):
    docs = []
    for cell_id, state in cell_states.items():
        if state.event_count == 0:
            continue

        metric_stats = {field: state.metrics[field].result() for field in METRIC_FIELDS}

        doc = {
            "window_end": datetime.fromtimestamp(window_end_ts, tz=timezone.utc),
            "window_size_minutes": WINDOW_SIZE_MINUTES,
            "cell_id": cell_id,
            "region": state.region,
            "event_count": state.event_count,
            "anomaly_event_count": state.anomaly_count,
            **metric_stats,
            "ingested_at": datetime.now(timezone.utc),
        }
        docs.append(doc)

    if docs:
        collection.insert_many(docs)
        print(f"[Window] Flushed {len(docs)} cell aggregates for window ending {datetime.fromtimestamp(window_end_ts, tz=timezone.utc).strftime('%H:%M:%S')} UTC")

    return len(docs)


def main():
    print("=" * 60)
    print("Telco ODS - Streaming Window Processor")
    print("=" * 60)
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Window size: {WINDOW_SIZE_MINUTES} minutes")
    print(f"MongoDB: {MONGODB_DB}.{MONGODB_COLLECTION}")
    print("=" * 60)

    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]

    client.admin.command("ping")
    print("MongoDB connected successfully")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "flink-telco-processor",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "max.poll.interval.ms": 600000,
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Subscribed to {KAFKA_TOPIC}")

    # Window state: {window_key: {cell_id: CellWindowState}}
    windows = {}
    window_seconds = WINDOW_SIZE_MINUTES * 60
    total_events = 0
    total_windows_flushed = 0

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            current_window = get_window_key(int(time.time()))
            expired = [w for w in windows if w + window_seconds <= current_window]
            for w in expired:
                count = flush_window(windows[w], w + window_seconds, collection)
                total_windows_flushed += count
                del windows[w]
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        total_events += 1
        current_time = int(time.time())
        window_key = get_window_key(current_time)

        cell_id = event.get("cell_id", "unknown")

        if window_key not in windows:
            windows[window_key] = {}
        if cell_id not in windows[window_key]:
            windows[window_key][cell_id] = CellWindowState(event.get("region", "unknown"))

        windows[window_key][cell_id].add_event(event)

        # Check for expired windows
        if total_events % 10000 == 0:
            expired = [w for w in windows if w + window_seconds <= window_key]
            for w in expired:
                count = flush_window(windows[w], w + window_seconds, collection)
                total_windows_flushed += count
                del windows[w]

        if total_events % 100000 == 0:
            print(f"[Progress] Consumed {total_events:,} events | Windows flushed: {total_windows_flushed} | Memory windows: {len(windows)}")

    # Flush remaining windows on shutdown
    for w in list(windows.keys()):
        flush_window(windows[w], w + window_seconds, collection)

    consumer.close()
    client.close()
    print(f"Shutdown complete. Total events: {total_events:,}")


if __name__ == "__main__":
    main()
