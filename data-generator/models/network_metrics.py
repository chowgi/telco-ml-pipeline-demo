import random
import time
from dataclasses import dataclass


@dataclass
class NetworkMetricEvent:
    timestamp: float
    cell_id: str
    imsi: str
    region: str
    signal_strength_dbm: float
    throughput_mbps: float
    latency_ms: float
    call_drop_rate_percent: float
    packet_loss_percent: float
    jitter_ms: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "cell_id": self.cell_id,
            "imsi": self.imsi,
            "region": self.region,
            "signal_strength_dbm": round(self.signal_strength_dbm, 2),
            "throughput_mbps": round(self.throughput_mbps, 2),
            "latency_ms": round(self.latency_ms, 2),
            "call_drop_rate_percent": round(self.call_drop_rate_percent, 3),
            "packet_loss_percent": round(self.packet_loss_percent, 3),
            "jitter_ms": round(self.jitter_ms, 2),
        }


def generate_normal_event(cell_id: str, imsi: str, region: str) -> NetworkMetricEvent:
    hour = time.localtime().tm_hour
    peak_factor = 1.0 if 8 <= hour <= 18 else 0.7

    signal = random.gauss(-60, 10)
    signal = max(-95, min(-30, signal))

    throughput = random.gauss(80, 25) * (1.0 if signal > -65 else 0.5)
    throughput = max(5, min(200, throughput))

    latency = random.gauss(25, 10) * peak_factor
    latency = max(5, min(80, latency))

    return NetworkMetricEvent(
        timestamp=time.time(),
        cell_id=cell_id,
        imsi=imsi,
        region=region,
        signal_strength_dbm=signal,
        throughput_mbps=throughput,
        latency_ms=latency,
        call_drop_rate_percent=max(0, random.gauss(0.5, 0.3)),
        packet_loss_percent=max(0, random.gauss(0.3, 0.2)),
        jitter_ms=max(0, random.gauss(2, 1.5)),
    )


def generate_excellent_event(cell_id: str, imsi: str, region: str) -> NetworkMetricEvent:
    return NetworkMetricEvent(
        timestamp=time.time(),
        cell_id=cell_id,
        imsi=imsi,
        region=region,
        signal_strength_dbm=random.gauss(-48, 4),
        throughput_mbps=max(80, random.gauss(125, 15)),
        latency_ms=max(5, random.gauss(18, 4)),
        call_drop_rate_percent=max(0, random.gauss(0.2, 0.1)),
        packet_loss_percent=max(0, random.gauss(0.1, 0.05)),
        jitter_ms=max(0, random.gauss(1.0, 0.4)),
    )


def generate_anomaly_event(cell_id: str, imsi: str, region: str) -> NetworkMetricEvent:
    anomaly_type = random.choice(["poor_signal", "high_latency", "packet_storm", "degraded"])

    if anomaly_type == "poor_signal":
        signal = random.uniform(-95, -80)
        throughput = random.uniform(2, 15)
        latency = random.uniform(80, 200)
        drop_rate = random.uniform(2.0, 5.0)
        packet_loss = random.uniform(2.0, 8.0)
        jitter = random.uniform(8, 20)
    elif anomaly_type == "high_latency":
        signal = random.uniform(-70, -55)
        throughput = random.uniform(10, 40)
        latency = random.uniform(150, 500)
        drop_rate = random.uniform(1.5, 4.0)
        packet_loss = random.uniform(1.0, 5.0)
        jitter = random.uniform(10, 30)
    elif anomaly_type == "packet_storm":
        signal = random.uniform(-65, -50)
        throughput = random.uniform(5, 20)
        latency = random.uniform(50, 150)
        drop_rate = random.uniform(1.0, 3.0)
        packet_loss = random.uniform(5.0, 15.0)
        jitter = random.uniform(15, 40)
    else:  # degraded
        signal = random.uniform(-80, -70)
        throughput = random.uniform(15, 40)
        latency = random.uniform(60, 120)
        drop_rate = random.uniform(2.0, 4.0)
        packet_loss = random.uniform(2.0, 6.0)
        jitter = random.uniform(5, 15)

    return NetworkMetricEvent(
        timestamp=time.time(),
        cell_id=cell_id,
        imsi=imsi,
        region=region,
        signal_strength_dbm=signal,
        throughput_mbps=throughput,
        latency_ms=latency,
        call_drop_rate_percent=drop_rate,
        packet_loss_percent=packet_loss,
        jitter_ms=jitter,
    )
