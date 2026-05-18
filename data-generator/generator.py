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
    NUM_PRODUCER_THREADS, EVENTS_PER_BATCH, ANOMALY_RATE, REGIONS, BATCH_SLEEP,
)
from models import create_cell_towers, generate_normal_event, generate_anomaly_event, generate_excellent_event

running = True
total_events = 0
events_lock = threading.Lock()

# Cell-level degradation state: maps cell_id -> ("poor"|"degraded", expiry_time)
cell_degradation = {}
degradation_lock = threading.Lock()

EXCELLENT_CELL_FRACTION = 0.30
POOR_CELL_FRACTION = 0.06
DEGRADED_CELL_FRACTION = 0.12
DEGRADATION_DURATION_MIN = 300
DEGRADATION_DURATION_MAX = 900


def update_cell_degradation(towers):
    """Periodically assign health states to cells so window averages reflect realistic distribution."""
    now = time.time()
    with degradation_lock:
        expired = [cid for cid, (_, expiry) in cell_degradation.items() if now > expiry]
        for cid in expired:
            del cell_degradation[cid]

        active_poor = sum(1 for _, (state, _) in cell_degradation.items() if state == "poor")
        active_degraded = sum(1 for _, (state, _) in cell_degradation.items() if state == "degraded")
        active_excellent = sum(1 for _, (state, _) in cell_degradation.items() if state == "excellent")

        target_poor = int(len(towers) * POOR_CELL_FRACTION)
        target_degraded = int(len(towers) * DEGRADED_CELL_FRACTION)
        target_excellent = int(len(towers) * EXCELLENT_CELL_FRACTION)

        available = [t for t in towers if t.cell_id not in cell_degradation]
        random.shuffle(available)

        for tower in available:
            if active_poor < target_poor:
                duration = random.uniform(DEGRADATION_DURATION_MIN, DEGRADATION_DURATION_MAX)
                cell_degradation[tower.cell_id] = ("poor", now + duration)
                active_poor += 1
            elif active_degraded < target_degraded:
                duration = random.uniform(DEGRADATION_DURATION_MIN, DEGRADATION_DURATION_MAX)
                cell_degradation[tower.cell_id] = ("degraded", now + duration)
                active_degraded += 1
            elif active_excellent < target_excellent:
                duration = random.uniform(DEGRADATION_DURATION_MIN, DEGRADATION_DURATION_MAX)
                cell_degradation[tower.cell_id] = ("excellent", now + duration)
                active_excellent += 1
            else:
                break


def degradation_manager(towers):
    """Background thread that rotates cell degradation states."""
    while running:
        update_cell_degradation(towers)
        time.sleep(30)


def get_cell_state(cell_id: str) -> str:
    """Returns 'poor', 'degraded', or 'normal' for a cell."""
    with degradation_lock:
        entry = cell_degradation.get(cell_id)
        if entry:
            return entry[0]
    return "normal"


def delivery_callback(err, msg):
    if err:
        sys.stderr.write(f"Delivery failed: {err}\n")


def produce_batch(producer: Producer, towers, batch_size: int):
    global total_events
    count = 0
    for _ in range(batch_size):
        tower = random.choice(towers)
        imsi = tower.generate_imsi()

        cell_state = get_cell_state(tower.cell_id)

        if cell_state == "excellent":
            event = generate_excellent_event(tower.cell_id, imsi, tower.region)
        elif cell_state == "poor":
            event = generate_anomaly_event(tower.cell_id, imsi, tower.region)
        elif cell_state == "degraded":
            if random.random() < 0.6:
                event = generate_anomaly_event(tower.cell_id, imsi, tower.region)
            else:
                event = generate_normal_event(tower.cell_id, imsi, tower.region)
        elif random.random() < ANOMALY_RATE:
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
        if BATCH_SLEEP > 0:
            time.sleep(BATCH_SLEEP)

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
    print(f"Anomaly rate: {ANOMALY_RATE*100:.1f}% (random) + cell degradation")
    print(f"Cell degradation: ~{POOR_CELL_FRACTION*100:.0f}% poor, ~{DEGRADED_CELL_FRACTION*100:.0f}% degraded")
    print("=" * 60)

    towers = create_cell_towers(REGIONS, NUM_CELL_TOWERS)
    print(f"Created {len(towers)} cell towers across {len(REGIONS)} regions")

    # Initialize cell health states
    update_cell_degradation(towers)
    with degradation_lock:
        poor_cells = [cid for cid, (s, _) in cell_degradation.items() if s == "poor"]
        degraded_cells = [cid for cid, (s, _) in cell_degradation.items() if s == "degraded"]
        excellent_cells = [cid for cid, (s, _) in cell_degradation.items() if s == "excellent"]
    print(f"Cell states: {len(excellent_cells)} excellent, {len(degraded_cells)} degraded, {len(poor_cells)} poor")

    degradation_thread = threading.Thread(target=degradation_manager, args=(towers,), daemon=True)
    degradation_thread.start()

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
