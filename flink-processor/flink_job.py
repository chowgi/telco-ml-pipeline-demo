#!/usr/bin/env python3
"""
Streaming processor: consumes raw telemetry from Kafka, applies 5-minute tumbling windows,
aggregates metrics per cell tower, and writes to MongoDB Atlas.

Uses confluent_kafka consumer with in-process windowing (no Java JARs required).
"""

import os
import json
import time
import signal
import threading
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


def compute_stats(values):
    if not values:
        return {"avg": 0, "min": 0, "max": 0, "p95": 0}
    values_sorted = sorted(values)
    p95_idx = int(len(values_sorted) * 0.95)
    return {
        "avg": sum(values_sorted) / len(values_sorted),
        "min": values_sorted[0],
        "max": values_sorted[-1],
        "p95": values_sorted[min(p95_idx, len(values_sorted) - 1)],
    }


def is_anomaly(event):
    return (
        event.get("latency_ms", 0) > 100
        or event.get("signal_strength_dbm", 0) < -80
        or event.get("call_drop_rate_percent", 0) > 2.0
        or event.get("packet_loss_percent", 0) > 5.0
    )


def get_window_key(timestamp_ms):
    window_seconds = WINDOW_SIZE_MINUTES * 60
    window_start = (timestamp_ms // 1000) // window_seconds * window_seconds
    return window_start


def flush_window(cell_events, window_end_ts, collection):
    docs = []
    for cell_id, events in cell_events.items():
        if not events:
            continue

        region = events[0].get("region", "unknown")
        metric_stats = {}
        for field in METRIC_FIELDS:
            values = [e[field] for e in events if field in e and e[field] is not None]
            metric_stats[field] = compute_stats(values)

        anomaly_count = sum(1 for e in events if is_anomaly(e))

        doc = {
            "window_end": datetime.fromtimestamp(window_end_ts, tz=timezone.utc),
            "window_size_minutes": WINDOW_SIZE_MINUTES,
            "cell_id": cell_id,
            "region": region,
            "event_count": len(events),
            "anomaly_event_count": anomaly_count,
            **metric_stats,
            "ingested_at": datetime.now(timezone.utc),
        }
        docs.append(doc)

    if docs:
        collection.insert_many(docs)
        print(f"[Window] Flushed {len(docs)} cell aggregates for window ending {datetime.fromtimestamp(window_end_ts, tz=timezone.utc).isoformat()}")

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

    # Verify MongoDB connectivity
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

    # Window state: {window_key: {cell_id: [events]}}
    windows = defaultdict(lambda: defaultdict(list))
    window_seconds = WINDOW_SIZE_MINUTES * 60
    total_events = 0
    total_windows_flushed = 0

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            # Check if any windows need flushing based on wall clock
            current_window = get_window_key(int(time.time() * 1000))
            expired = [w for w in windows if w + window_seconds <= current_window]
            for w in expired:
                window_end_ts = w + window_seconds
                count = flush_window(windows[w], window_end_ts, collection)
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
        current_time_ms = int(time.time() * 1000)
        window_key = get_window_key(current_time_ms)

        cell_id = event.get("cell_id", "unknown")
        windows[window_key][cell_id].append(event)

        # Check for expired windows
        expired = [w for w in windows if w + window_seconds <= window_key]
        for w in expired:
            window_end_ts = w + window_seconds
            count = flush_window(windows[w], window_end_ts, collection)
            total_windows_flushed += count
            del windows[w]

        if total_events % 100000 == 0:
            print(f"[Progress] Consumed {total_events:,} events | Windows flushed: {total_windows_flushed}")

    # Flush remaining windows on shutdown
    for w in list(windows.keys()):
        window_end_ts = w + window_seconds
        flush_window(windows[w], window_end_ts, collection)

    consumer.close()
    client.close()
    print(f"Shutdown complete. Total events: {total_events:,}")


if __name__ == "__main__":
    main()
