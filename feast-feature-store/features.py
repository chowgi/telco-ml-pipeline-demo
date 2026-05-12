"""
Feast feature definitions for the Telco ODS pipeline.
Demonstrates MongoDB as Feast online store backend.
"""

from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, Int64, String


cell_tower = Entity(
    name="cell_id",
    description="Cell tower identifier",
)

# Offline source (for historical feature retrieval / training)
windowed_metrics_source = FileSource(
    path="data/windowed_metrics.parquet",
    timestamp_field="window_end",
)

windowed_cell_metrics = FeatureView(
    name="windowed_cell_metrics",
    entities=[cell_tower],
    ttl=timedelta(minutes=30),
    schema=[
        Field(name="avg_signal_strength_dbm", dtype=Float64),
        Field(name="avg_throughput_mbps", dtype=Float64),
        Field(name="avg_latency_ms", dtype=Float64),
        Field(name="avg_call_drop_rate_percent", dtype=Float64),
        Field(name="avg_packet_loss_percent", dtype=Float64),
        Field(name="avg_jitter_ms", dtype=Float64),
        Field(name="event_count", dtype=Int64),
        Field(name="anomaly_event_count", dtype=Int64),
        Field(name="region", dtype=String),
    ],
    source=windowed_metrics_source,
    online=True,
)
