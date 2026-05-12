#!/usr/bin/env python3
"""
Generates synthetic windowed training data for the network health classifier.
Simulates what Flink would produce from 5-minute tumbling windows.
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = "ods_demo_db"
COLLECTION = "training_windowed_metrics"
NUM_SAMPLES = 10000


def generate_training_data():
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]

    print(f"Generating {NUM_SAMPLES} training samples...")
    samples = []

    for i in range(NUM_SAMPLES):
        # Decide target class with balanced distribution
        health_class = random.choices(
            ["excellent", "good", "poor"], weights=[0.33, 0.34, 0.33]
        )[0]

        if health_class == "excellent":
            signal = random.gauss(-50, 5)
            throughput = random.gauss(120, 20)
            latency = random.gauss(20, 5)
            drop_rate = random.gauss(0.3, 0.15)
            packet_loss = random.gauss(0.2, 0.1)
            jitter = random.gauss(1.5, 0.5)
        elif health_class == "good":
            signal = random.gauss(-65, 5)
            throughput = random.gauss(70, 15)
            latency = random.gauss(45, 10)
            drop_rate = random.gauss(1.0, 0.3)
            packet_loss = random.gauss(1.0, 0.4)
            jitter = random.gauss(3.5, 1.0)
        else:  # poor
            signal = random.gauss(-82, 5)
            throughput = random.gauss(25, 10)
            latency = random.gauss(130, 30)
            drop_rate = random.gauss(3.0, 0.8)
            packet_loss = random.gauss(4.0, 1.5)
            jitter = random.gauss(10, 3)

        sample = {
            "window_end": datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 43200)),
            "cell_id": f"CELL_{random.randint(0, 49):04d}",
            "region": random.choice(["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]),
            "event_count": random.randint(500, 5000),
            "signal_strength_dbm": round(max(-95, min(-30, signal)), 2),
            "throughput_mbps": round(max(1, throughput), 2),
            "latency_ms": round(max(5, latency), 2),
            "call_drop_rate_percent": round(max(0, drop_rate), 3),
            "packet_loss_percent": round(max(0, packet_loss), 3),
            "jitter_ms": round(max(0, jitter), 2),
            "network_health_score": health_class,
        }
        samples.append(sample)

    db[COLLECTION].drop()
    db[COLLECTION].insert_many(samples)
    print(f"Inserted {len(samples)} training samples into {DB_NAME}.{COLLECTION}")

    # Distribution check
    for label in ["excellent", "good", "poor"]:
        count = sum(1 for s in samples if s["network_health_score"] == label)
        print(f"  {label}: {count} ({count/len(samples)*100:.1f}%)")

    client.close()


if __name__ == "__main__":
    generate_training_data()
