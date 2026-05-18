import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telco-raw-telemetry")

NUM_CELL_TOWERS = 50
NUM_PRODUCER_THREADS = 1
EVENTS_PER_BATCH = 100
BATCH_SLEEP = 0.1
ANOMALY_RATE = 0.05

KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BROKER,
    "linger.ms": 5,
    "batch.size": 65536,
    "acks": "1",
    "compression.type": "lz4",
    "queue.buffering.max.messages": 500000,
}

REGIONS = [
    {"name": "Sydney", "lat": -33.8688, "lng": 151.2093},
    {"name": "Melbourne", "lat": -37.8136, "lng": 144.9631},
    {"name": "Brisbane", "lat": -27.4698, "lng": 153.0251},
    {"name": "Perth", "lat": -31.9505, "lng": 115.8605},
    {"name": "Adelaide", "lat": -34.9285, "lng": 138.6007},
]
