#!/usr/bin/env python3
"""
PyFlink streaming job: consumes raw telemetry from Kafka, applies per-cell
keyed processing with 30-second emission timers, computes rolling statistics,
and writes to MongoDB Atlas.

Run with: flink run -py flink_job.py -pyexec /opt/flink-env/bin/python3
Flink Web UI: http://<flink-ip>:8081
"""

import os
import json
import time
import random
from datetime import datetime, timezone
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telco-raw-telemetry")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "ods_demo_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "windowed_network_metrics")

METRIC_FIELDS = [
    "signal_strength_dbm",
    "throughput_mbps",
    "latency_ms",
    "call_drop_rate_percent",
    "packet_loss_percent",
    "jitter_ms",
]

RESERVOIR_SIZE = 500
EMIT_INTERVAL_MS = 10000


def is_anomaly(event):
    return (
        event.get("latency_ms", 0) > 100
        or event.get("signal_strength_dbm", 0) < -80
        or event.get("call_drop_rate_percent", 0) > 2.0
        or event.get("packet_loss_percent", 0) > 5.0
    )


def new_accumulator_state():
    """Create a fresh accumulator as a JSON-serializable dict."""
    metrics = {}
    for field in METRIC_FIELDS:
        metrics[field] = {
            "count": 0,
            "total": 0.0,
            "min": 1e18,
            "max": -1e18,
            "reservoir": [],
        }
    return {
        "region": None,
        "event_count": 0,
        "anomaly_count": 0,
        "metrics": metrics,
    }


def add_event_to_state(state, event):
    """Add an event to the accumulator state dict."""
    state["event_count"] += 1
    if state["region"] is None:
        state["region"] = event.get("region", "unknown")
    if is_anomaly(event):
        state["anomaly_count"] += 1

    for field in METRIC_FIELDS:
        val = event.get(field)
        if val is not None:
            m = state["metrics"][field]
            m["count"] += 1
            m["total"] += val
            if val < m["min"]:
                m["min"] = val
            if val > m["max"]:
                m["max"] = val
            if len(m["reservoir"]) < RESERVOIR_SIZE:
                m["reservoir"].append(val)
            else:
                j = random.randint(0, m["count"] - 1)
                if j < RESERVOIR_SIZE:
                    m["reservoir"][j] = val
    return state


def compute_stats(state):
    """Compute final statistics from accumulator state."""
    stats = {}
    for field in METRIC_FIELDS:
        m = state["metrics"][field]
        if m["count"] == 0:
            stats[field] = {"avg": 0, "min": 0, "max": 0, "p95": 0}
        else:
            reservoir = sorted(m["reservoir"])
            p95_idx = int(len(reservoir) * 0.95)
            stats[field] = {
                "avg": round(m["total"] / m["count"], 3),
                "min": round(m["min"], 3),
                "max": round(m["max"], 3),
                "p95": round(reservoir[min(p95_idx, len(reservoir) - 1)], 3),
            }
    return stats


class CellWindowProcessor(KeyedProcessFunction):
    """
    Per-cell keyed process function that accumulates events and emits
    rolling statistics every 30 seconds via processing-time timers.
    """

    def open(self, runtime_context: RuntimeContext):
        from pymongo import MongoClient

        self._mongo_client = MongoClient(MONGODB_URI)
        self._collection = self._mongo_client[MONGODB_DB][MONGODB_COLLECTION]

        # State stored as JSON string (avoids pickle issues with custom classes)
        self._state = runtime_context.get_state(
            ValueStateDescriptor("cell_acc", Types.STRING())
        )
        self._timer_state = runtime_context.get_state(
            ValueStateDescriptor("timer_set", Types.BOOLEAN())
        )

    def close(self):
        if self._mongo_client:
            self._mongo_client.close()

    def process_element(self, value, ctx):
        try:
            event = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return

        # Load or init state
        raw = self._state.value()
        if raw is None:
            acc = new_accumulator_state()
        else:
            acc = json.loads(raw)

        acc = add_event_to_state(acc, event)
        self._state.update(json.dumps(acc))

        # Register timer on first event — stagger by cell_id hash so emissions spread over the interval
        if not self._timer_state.value():
            cell_id = ctx.get_current_key()
            offset_ms = (hash(cell_id) % EMIT_INTERVAL_MS)
            fire_time = ctx.timer_service().current_processing_time() + offset_ms
            ctx.timer_service().register_processing_time_timer(fire_time)
            self._timer_state.update(True)

    def on_timer(self, timestamp, ctx):
        raw = self._state.value()
        if raw is not None:
            acc = json.loads(raw)
            if acc["event_count"] > 0:
                stats = compute_stats(acc)
                doc = {
                    "window_end": datetime.now(timezone.utc),
                    "window_size_seconds": 10,
                    "rolling_window_seconds": 300,
                    "cell_id": ctx.get_current_key(),
                    "region": acc["region"] or "unknown",
                    "event_count": acc["event_count"],
                    "anomaly_event_count": acc["anomaly_count"],
                    **stats,
                    "ingested_at": datetime.now(timezone.utc),
                }
                self._collection.insert_one(doc)

        # Reset state and schedule next timer
        self._state.update(json.dumps(new_accumulator_state()))
        fire_time = timestamp + EMIT_INTERVAL_MS
        ctx.timer_service().register_processing_time_timer(fire_time)
        # PyFlink requires on_timer to be a generator
        return []


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    kafka_jar = "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    env.add_jars(kafka_jar)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("flink-telco-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source",
    )

    # Key by cell_id, process with 30-second emission timer
    stream \
        .key_by(lambda msg: json.loads(msg).get("cell_id", "unknown")) \
        .process(CellWindowProcessor()) \
        .name("Cell Window Aggregator (30s emit)")

    env.execute("Telco ODS - Network Health Streaming Pipeline")


if __name__ == "__main__":
    main()
