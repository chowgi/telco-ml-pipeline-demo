#!/usr/bin/env python3
"""
Live dashboard for the Telco ODS Autonomous Networks demo.
Shows real-time predictions, pipeline metrics, and architecture overview.
"""

import os
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify
from pymongo import MongoClient

app = Flask(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = "ods_demo_db"

client = MongoClient(MONGODB_URI)
db = client[MONGODB_DB]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def stats():
    now = datetime.now(timezone.utc)
    one_min_ago = now - timedelta(minutes=1)
    five_min_ago = now - timedelta(minutes=5)
    thirty_min_ago = now - timedelta(minutes=30)

    # Prediction distribution (last 30 min)
    distribution = list(db.network_health_predictions.aggregate([
        {"$match": {"timestamp": {"$gte": thirty_min_ago}}},
        {"$group": {"_id": "$prediction.label", "count": {"$sum": 1}}},
    ]))

    # Predictions per minute (last 30 min)
    predictions_over_time = list(db.network_health_predictions.aggregate([
        {"$match": {"timestamp": {"$gte": thirty_min_ago}}},
        {"$group": {
            "_id": {
                "minute": {"$dateTrunc": {"date": "$timestamp", "unit": "minute"}},
                "label": "$prediction.label",
            },
            "count": {"$sum": 1},
        }},
        {"$sort": {"_id.minute": 1}},
    ]))

    # Recent predictions (latest 20)
    recent = list(db.network_health_predictions.find(
        {},
        {"_id": 0, "cell_id": 1, "region": 1, "prediction": 1, "timestamp": 1},
    ).sort("timestamp", -1).limit(20))

    # Events ingested (last 5 min)
    events_result = list(db.windowed_network_metrics.aggregate([
        {"$match": {"window_end": {"$gte": five_min_ago}}},
        {"$group": {"_id": None, "total_events": {"$sum": "$event_count"}, "docs": {"$sum": 1}}},
    ]))

    # Per-region health (last 10 min)
    region_health = list(db.network_health_predictions.aggregate([
        {"$match": {"timestamp": {"$gte": now - timedelta(minutes=10)}}},
        {"$group": {
            "_id": {"region": "$region", "label": "$prediction.label"},
            "count": {"$sum": 1},
        }},
    ]))

    # Cell-level detail (last emission per cell)
    cell_detail = list(db.windowed_network_metrics.aggregate([
        {"$sort": {"window_end": -1}},
        {"$group": {
            "_id": "$cell_id",
            "region": {"$first": "$region"},
            "signal_avg": {"$first": "$signal_strength_dbm.avg"},
            "latency_avg": {"$first": "$latency_ms.avg"},
            "throughput_avg": {"$first": "$throughput_mbps.avg"},
            "last_seen": {"$first": "$window_end"},
        }},
        {"$sort": {"_id": 1}},
    ]))

    # Predictions rate
    recent_count = db.network_health_predictions.count_documents(
        {"timestamp": {"$gte": one_min_ago}}
    )

    for r in recent:
        if "timestamp" in r:
            r["timestamp"] = r["timestamp"].isoformat()

    for c in cell_detail:
        if "last_seen" in c:
            c["last_seen"] = c["last_seen"].isoformat()

    time_series = {}
    for item in predictions_over_time:
        minute = item["_id"]["minute"].isoformat()
        label = item["_id"]["label"]
        if minute not in time_series:
            time_series[minute] = {"excellent": 0, "good": 0, "poor": 0}
        time_series[minute][label] = item["count"]

    return jsonify({
        "distribution": {item["_id"]: item["count"] for item in distribution},
        "predictions_per_minute": recent_count,
        "total_events": events_result[0]["total_events"] if events_result else 0,
        "total_docs": events_result[0]["docs"] if events_result else 0,
        "recent": recent,
        "region_health": [{"region": r["_id"]["region"], "label": r["_id"]["label"], "count": r["count"]} for r in region_health],
        "cell_detail": cell_detail,
        "time_series": time_series,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
