#!/usr/bin/env python3
"""
Streaming processor: consumes raw telemetry from Kafka, maintains per-cell rolling
statistics over a 5-minute window, and emits snapshots every 30 seconds to MongoDB Atlas.

This gives near-real-time predictions (~30s latency) while still providing meaningful
5-minute rolling averages for ML feature stability.
"""

import os
import json
import time
import signal
import random
from datetime import datetime, timezone
from collections import deque
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telco-raw-telemetry")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "ods_demo_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "windowed_network_metrics")
WINDOW_SIZE_SECONDS = int(os.getenv("WINDOW_SIZE_SECONDS", "300"))
EMIT_INTERVAL_SECONDS = int(os.getenv("EMIT_INTERVAL_SECONDS", "30"))

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
    __slots__ = ['count', 'total', 'min_val', 'max_val', 'reservoir', 'reservoir_size']

    def __init__(self, reservoir_size=500):
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
        if len(self.reservoir) < self.reservoir_size:
            self.reservoir.append(value)
        else:
            j = random.randint(0, self.count - 1)
            if j < self.reservoir_size:
                self.reservoir[j] = value

    def result(self):
        if self.count == 0:
            return {"avg": 0, "min": 0, "max": 0, "p95": 0}
        self.reservoir.sort()
        p95_idx = int(len(self.reservoir) * 0.95)
        return {
            "avg": round(self.total / self.count, 3),
            "min": round(self.min_val, 3),
            "max": round(self.max_val, 3),
            "p95": round(self.reservoir[min(p95_idx, len(self.reservoir) - 1)], 3),
        }


class CellState:
    """Maintains rolling stats for a cell, resets on each emission."""

    __slots__ = ['region', 'event_count', 'anomaly_count', 'metrics', 'last_emit_time']

    def __init__(self, region):
        self.region = region
        self.last_emit_time = time.time()
        self.reset()

    def reset(self):
        self.event_count = 0
        self.anomaly_count = 0
        self.metrics = {field: RunningStats() for field in METRIC_FIELDS}
        self.last_emit_time = time.time()

    def add_event(self, event):
        self.event_count += 1
        if is_anomaly(event):
            self.anomaly_count += 1
        for field in METRIC_FIELDS:
            val = event.get(field)
            if val is not None:
                self.metrics[field].add(val)

    def should_emit(self, now):
        return (now - self.last_emit_time) >= EMIT_INTERVAL_SECONDS and self.event_count > 0


def is_anomaly(event):
    return (
        event.get("latency_ms", 0) > 100
        or event.get("signal_strength_dbm", 0) < -80
        or event.get("call_drop_rate_percent", 0) > 2.0
        or event.get("packet_loss_percent", 0) > 5.0
    )


def emit_cell_snapshot(cell_id, state, collection):
    metric_stats = {field: state.metrics[field].result() for field in METRIC_FIELDS}

    doc = {
        "window_end": datetime.now(timezone.utc),
        "window_size_seconds": EMIT_INTERVAL_SECONDS,
        "rolling_window_seconds": WINDOW_SIZE_SECONDS,
        "cell_id": cell_id,
        "region": state.region,
        "event_count": state.event_count,
        "anomaly_event_count": state.anomaly_count,
        **metric_stats,
        "ingested_at": datetime.now(timezone.utc),
    }

    collection.insert_one(doc)
    return doc


def main():
    print("=" * 60)
    print("Telco ODS - Streaming Processor (Rolling Window)")
    print("=" * 60)
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Rolling window: {WINDOW_SIZE_SECONDS}s | Emit interval: {EMIT_INTERVAL_SECONDS}s")
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

    # Per-cell rolling state
    cells = {}
    total_events = 0
    total_emissions = 0
    last_emit_check = time.time()

    while running:
        msg = consumer.poll(timeout=0.1)

        if msg is not None and not msg.error():
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            else:
                total_events += 1
                cell_id = event.get("cell_id", "unknown")

                if cell_id not in cells:
                    cells[cell_id] = CellState(event.get("region", "unknown"))

                cells[cell_id].add_event(event)
        elif msg is not None and msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")

        # Check for cells ready to emit (every 1 second to avoid overhead)
        now = time.time()
        if now - last_emit_check >= 1.0:
            last_emit_check = now
            for cell_id, state in cells.items():
                if state.should_emit(now):
                    emit_cell_snapshot(cell_id, state, collection)
                    total_emissions += 1
                    state.reset()

            if total_events > 0 and total_events % 50000 == 0:
                print(f"[Progress] Events: {total_events:,} | Emissions: {total_emissions} | Cells: {len(cells)}")

    # Final flush
    for cell_id, state in cells.items():
        if state.event_count > 0:
            emit_cell_snapshot(cell_id, state, collection)

    consumer.close()
    client.close()
    print(f"Shutdown complete. Events: {total_events:,} | Emissions: {total_emissions}")


if __name__ == "__main__":
    main()
