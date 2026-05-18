"""
Window aggregation logic for the Flink streaming job.
Separated into its own module for pickling compatibility with PyFlink/Beam.
"""

import json
import random

METRIC_FIELDS = [
    "signal_strength_dbm",
    "throughput_mbps",
    "latency_ms",
    "call_drop_rate_percent",
    "packet_loss_percent",
    "jitter_ms",
]

RESERVOIR_SIZE = 500


def is_anomaly(event):
    return (
        event.get("latency_ms", 0) > 100
        or event.get("signal_strength_dbm", 0) < -80
        or event.get("call_drop_rate_percent", 0) > 2.0
        or event.get("packet_loss_percent", 0) > 5.0
    )


class MetricsAccumulator:
    """Holds running statistics for all metrics within a window."""

    def __init__(self):
        self.region = None
        self.event_count = 0
        self.anomaly_count = 0
        self.metrics = {}
        for field in METRIC_FIELDS:
            self.metrics[field] = {
                "count": 0,
                "total": 0.0,
                "min": float("inf"),
                "max": float("-inf"),
                "reservoir": [],
            }

    def add(self, event):
        self.event_count += 1
        if self.region is None:
            self.region = event.get("region", "unknown")

        if is_anomaly(event):
            self.anomaly_count += 1

        for field in METRIC_FIELDS:
            val = event.get(field)
            if val is not None:
                m = self.metrics[field]
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

    def merge(self, other):
        self.event_count += other.event_count
        self.anomaly_count += other.anomaly_count
        if self.region is None:
            self.region = other.region

        for field in METRIC_FIELDS:
            m = self.metrics[field]
            o = other.metrics[field]
            m["count"] += o["count"]
            m["total"] += o["total"]
            if o["min"] < m["min"]:
                m["min"] = o["min"]
            if o["max"] > m["max"]:
                m["max"] = o["max"]
            combined = m["reservoir"] + o["reservoir"]
            if len(combined) > RESERVOIR_SIZE:
                random.shuffle(combined)
                combined = combined[:RESERVOIR_SIZE]
            m["reservoir"] = combined

    def result(self):
        stats = {}
        for field in METRIC_FIELDS:
            m = self.metrics[field]
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
