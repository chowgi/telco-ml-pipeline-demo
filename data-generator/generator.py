#!/usr/bin/env python3
"""
High-throughput Kafka producer simulating telco network telemetry.
Targets ~80k events/sec from a single c5.2xlarge EC2 instance.
"""

import json
import random
import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, KafkaError

from config import (
    KAFKA_PRODUCER_CONFIG, KAFKA_TOPIC, NUM_CELL_TOWERS,
    NUM_PRODUCER_THREADS, EVENTS_PER_BATCH, ANOMALY_RATE, REGIONS,
)
from models import create_cell_towers, generate_normal_event, generate_anomaly_event

running = True
total_events = 0
events_lock = threading.Lock()


def delivery_callback(err, msg):
    if err:
        sys.stderr.write(f"Delivery failed: {err}\n")


def produce_batch(producer: Producer, towers, batch_size: int):
    global total_events
    count = 0
    for _ in range(batch_size):
        tower = random.choice(towers)
        imsi = tower.generate_imsi()

        if random.random() < ANOMALY_RATE:
            event = generate_anomaly_event(tower.cell_id, imsi, tower.region)
        else:
            event = generate_normal_event(tower.cell_id, imsi, tower.region)

        producer.produce(
            KAFKA_TOPIC,
            key=tower.cell_id.encode("utf-8"),
            value=json.dumps(event.to_dict()).encode("utf-8"),
            callback=delivery_callback,
        )
        count += 1

    producer.poll(0)
    with events_lock:
        total_events += count
    return count


def producer_thread(thread_id: int, towers):
    producer = Producer(KAFKA_PRODUCER_CONFIG)
    print(f"[Thread-{thread_id}] Started producing to {KAFKA_TOPIC}")

    while running:
        produce_batch(producer, towers, EVENTS_PER_BATCH)
        producer.poll(0)

    producer.flush(timeout=10)
    print(f"[Thread-{thread_id}] Stopped")


def stats_reporter():
    global total_events
    last_count = 0
    while running:
        time.sleep(5)
        with events_lock:
            current = total_events
        rate = (current - last_count) / 5.0
        print(f"[Stats] Total: {current:,} | Rate: {rate:,.0f} events/sec")
        last_count = current


def shutdown_handler(signum, frame):
    global running
    print("\nShutting down...")
    running = False


def main():
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    print("=" * 60)
    print("Telco ODS - Network Telemetry Generator")
    print("=" * 60)
    print(f"Kafka broker: {KAFKA_PRODUCER_CONFIG['bootstrap.servers']}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Producer threads: {NUM_PRODUCER_THREADS}")
    print(f"Cell towers: {NUM_CELL_TOWERS}")
    print(f"Anomaly rate: {ANOMALY_RATE*100:.1f}%")
    print("=" * 60)

    towers = create_cell_towers(REGIONS, NUM_CELL_TOWERS)
    print(f"Created {len(towers)} cell towers across {len(REGIONS)} regions")

    stats_thread = threading.Thread(target=stats_reporter, daemon=True)
    stats_thread.start()

    with ThreadPoolExecutor(max_workers=NUM_PRODUCER_THREADS) as executor:
        futures = []
        for i in range(NUM_PRODUCER_THREADS):
            futures.append(executor.submit(producer_thread, i, towers))
        for f in futures:
            f.result()

    print(f"\nTotal events produced: {total_events:,}")


if __name__ == "__main__":
    main()
