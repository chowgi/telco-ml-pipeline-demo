#!/usr/bin/env python3
"""
PyFlink job: consumes raw telemetry from Kafka, applies 5-minute tumbling windows,
aggregates metrics per cell tower, and writes to MongoDB Atlas.
"""

import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext

from mongodb_sink import MongoDBSinkFunction

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telco-raw-telemetry")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "ods_demo_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "windowed_network_metrics")
WINDOW_SIZE_MINUTES = int(os.getenv("WINDOW_SIZE_MINUTES", "5"))


class MetricsWindowAggregator(ProcessWindowFunction):
    """Aggregates network metrics over a tumbling window per cell_id."""

    def process(self, key, context, elements):
        events = [json.loads(e) for e in elements]
        if not events:
            return

        cell_id = key
        region = events[0].get("region", "unknown")

        def stats(field):
            values = [e[field] for e in events if field in e and e[field] is not None]
            if not values:
                return {"avg": 0, "min": 0, "max": 0, "p95": 0}
            values.sort()
            p95_idx = int(len(values) * 0.95)
            return {
                "avg": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
                "p95": values[min(p95_idx, len(values) - 1)],
            }

        signal_stats = stats("signal_strength_dbm")
        throughput_stats = stats("throughput_mbps")
        latency_stats = stats("latency_ms")
        drop_rate_stats = stats("call_drop_rate_percent")
        packet_loss_stats = stats("packet_loss_percent")
        jitter_stats = stats("jitter_ms")

        anomaly_count = sum(
            1 for e in events
            if e.get("latency_ms", 0) > 100
            or e.get("signal_strength_dbm", 0) < -80
            or e.get("call_drop_rate_percent", 0) > 2.0
            or e.get("packet_loss_percent", 0) > 5.0
        )

        window_end = context.window().get_end() / 1000.0

        result = {
            "window_end": window_end,
            "window_size_minutes": WINDOW_SIZE_MINUTES,
            "cell_id": cell_id,
            "region": region,
            "event_count": len(events),
            "anomaly_event_count": anomaly_count,
            "signal_strength_dbm": signal_stats,
            "throughput_mbps": throughput_stats,
            "latency_ms": latency_stats,
            "call_drop_rate_percent": drop_rate_stats,
            "packet_loss_percent": packet_loss_stats,
            "jitter_ms": jitter_stats,
        }

        yield json.dumps(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    # Kafka consumer properties
    kafka_props = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "flink-telco-processor",
        "auto.offset.reset": "latest",
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props,
    )

    stream = env.add_source(kafka_consumer)

    windowed = (
        stream
        .key_by(lambda x: json.loads(x).get("cell_id", "unknown"))
        .window(TumblingProcessingTimeWindows.of(Time.minutes(WINDOW_SIZE_MINUTES)))
        .process(MetricsWindowAggregator())
    )

    windowed.add_sink(MongoDBSinkFunction(MONGODB_URI, MONGODB_DB, MONGODB_COLLECTION))

    env.execute("Telco ODS - Network Metrics Windowed Aggregation")


if __name__ == "__main__":
    main()
