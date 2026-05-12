#!/usr/bin/env python3
"""
Materializes features from MongoDB windowed_network_metrics into the Feast online store.
Demonstrates the MongoDB online store integration for real-time feature serving.
"""

import os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from feast import FeatureStore
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = "ods_demo_db"


def push_features_to_feast():
    """Push latest windowed metrics to Feast online store via the push API."""
    store = FeatureStore(repo_path=".")

    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]

    # Get the latest window for each cell tower
    pipeline = [
        {"$sort": {"window_end": -1}},
        {"$group": {
            "_id": "$cell_id",
            "latest": {"$first": "$$ROOT"},
        }},
        {"$replaceRoot": {"newRoot": "$latest"}},
    ]

    latest_windows = list(db.windowed_network_metrics.aggregate(pipeline))
    print(f"Found {len(latest_windows)} cell towers with recent data")

    if not latest_windows:
        print("No data to materialize")
        return

    import pandas as pd

    records = []
    for doc in latest_windows:
        signal = doc.get("signal_strength_dbm", {})
        throughput = doc.get("throughput_mbps", {})
        latency = doc.get("latency_ms", {})
        drop_rate = doc.get("call_drop_rate_percent", {})
        packet_loss = doc.get("packet_loss_percent", {})
        jitter = doc.get("jitter_ms", {})

        records.append({
            "cell_id": doc["cell_id"],
            "avg_signal_strength_dbm": signal.get("avg", 0) if isinstance(signal, dict) else signal,
            "avg_throughput_mbps": throughput.get("avg", 0) if isinstance(throughput, dict) else throughput,
            "avg_latency_ms": latency.get("avg", 0) if isinstance(latency, dict) else latency,
            "avg_call_drop_rate_percent": drop_rate.get("avg", 0) if isinstance(drop_rate, dict) else drop_rate,
            "avg_packet_loss_percent": packet_loss.get("avg", 0) if isinstance(packet_loss, dict) else packet_loss,
            "avg_jitter_ms": jitter.get("avg", 0) if isinstance(jitter, dict) else jitter,
            "event_count": doc.get("event_count", 0),
            "anomaly_event_count": doc.get("anomaly_event_count", 0),
            "region": doc.get("region", "unknown"),
            "window_end": doc.get("window_end", datetime.now(timezone.utc)),
        })

    df = pd.DataFrame(records)
    print(f"Materializing {len(df)} feature rows to Feast online store (MongoDB)")

    store.write_to_online_store(
        feature_view_name="windowed_cell_metrics",
        df=df,
    )
    print("Materialization complete")

    client.close()


def demo_feature_retrieval():
    """Demonstrates retrieving features from the Feast online store at inference time."""
    store = FeatureStore(repo_path=".")

    entity_rows = [
        {"cell_id": "CELL_0001"},
        {"cell_id": "CELL_0010"},
        {"cell_id": "CELL_0025"},
    ]

    features = store.get_online_features(
        features=[
            "windowed_cell_metrics:avg_signal_strength_dbm",
            "windowed_cell_metrics:avg_throughput_mbps",
            "windowed_cell_metrics:avg_latency_ms",
            "windowed_cell_metrics:avg_call_drop_rate_percent",
            "windowed_cell_metrics:avg_packet_loss_percent",
            "windowed_cell_metrics:avg_jitter_ms",
            "windowed_cell_metrics:event_count",
            "windowed_cell_metrics:anomaly_event_count",
        ],
        entity_rows=entity_rows,
    ).to_dict()

    print("\nFeature retrieval from MongoDB online store:")
    for i, cell_id in enumerate([r["cell_id"] for r in entity_rows]):
        print(f"\n  {cell_id}:")
        for key, values in features.items():
            if key != "cell_id":
                print(f"    {key}: {values[i]}")


if __name__ == "__main__":
    push_features_to_feast()
    demo_feature_retrieval()
