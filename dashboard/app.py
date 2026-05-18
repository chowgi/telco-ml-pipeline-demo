#!/usr/bin/env python3
"""
Live dashboard for the Telco ODS Autonomous Networks demo.
Shows real-time predictions, pipeline metrics, and architecture overview.
Includes Start/Stop Demo controls that trigger pipeline operations.
"""

import os
import subprocess
import threading
import time
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = "ods_demo_db"

# SSH config for remote commands
SSH_KEY = os.getenv("SSH_KEY", "/opt/dashboard/bennyk_aws_key.pem")
GENERATOR_IP = os.getenv("GENERATOR_IP", "")
FLINK_IP = os.getenv("FLINK_IP", "")
FEAST_IP = os.getenv("FEAST_IP", "")
FLINK_PUBLIC_IP = os.getenv("FLINK_PUBLIC_IP", "")
SSH_OPTS = f"-o ConnectTimeout=10 -o StrictHostKeyChecking=no -i {SSH_KEY}"

client = MongoClient(MONGODB_URI)
db = client[MONGODB_DB]

# Track demo state
demo_state = {"status": "unknown", "message": ""}


def run_ssh(host, command, timeout=60):
    """Run a command on a remote host via SSH."""
    cmd = ["ssh"] + SSH_OPTS.split() + [f"ubuntu@{host}", command]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return result.returncode == 0, result.stdout + result.stderr


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/details")
def details():
    return render_template("details.html")


@app.route("/api/stats")
def stats():
    now = datetime.now(timezone.utc)
    one_min_ago = now - timedelta(minutes=1)
    five_min_ago = now - timedelta(minutes=5)
    thirty_min_ago = now - timedelta(minutes=30)

    distribution = list(db.network_health_predictions.aggregate([
        {"$match": {"timestamp": {"$gte": thirty_min_ago}}},
        {"$group": {"_id": "$prediction.label", "count": {"$sum": 1}}},
    ]))

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

    recent = list(db.network_health_predictions.find(
        {},
        {"_id": 0, "cell_id": 1, "region": 1, "prediction": 1, "timestamp": 1},
    ).sort("timestamp", -1).limit(20))

    events_result = list(db.windowed_network_metrics.aggregate([
        {"$match": {"window_end": {"$gte": five_min_ago}}},
        {"$group": {"_id": None, "total_events": {"$sum": "$event_count"}, "docs": {"$sum": 1}}},
    ]))

    region_health = list(db.network_health_predictions.aggregate([
        {"$match": {"timestamp": {"$gte": now - timedelta(minutes=10)}}},
        {"$group": {
            "_id": {"region": "$region", "label": "$prediction.label"},
            "count": {"$sum": 1},
        }},
    ]))

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

    recent_count = db.network_health_predictions.count_documents(
        {"timestamp": {"$gte": one_min_ago}}
    )

    for r in recent:
        if "timestamp" in r:
            r["timestamp"] = r["timestamp"].isoformat() + "Z"

    for c in cell_detail:
        if "last_seen" in c:
            c["last_seen"] = c["last_seen"].isoformat() + "Z"

    time_series = {}
    for item in predictions_over_time:
        minute = item["_id"]["minute"].isoformat() + "Z"
        label = item["_id"]["label"]
        if minute not in time_series:
            time_series[minute] = {"excellent": 0, "good": 0, "poor": 0}
        time_series[minute][label] = item["count"]

    # Pipeline status check
    pipeline_active = recent_count > 0

    return jsonify({
        "distribution": {item["_id"]: item["count"] for item in distribution},
        "predictions_per_minute": recent_count,
        "total_events": events_result[0]["total_events"] if events_result else 0,
        "total_docs": events_result[0]["docs"] if events_result else 0,
        "recent": recent,
        "region_health": [{"region": r["_id"]["region"], "label": r["_id"]["label"], "count": r["count"]} for r in region_health],
        "cell_detail": cell_detail,
        "time_series": time_series,
        "pipeline_active": pipeline_active,
        "demo_state": demo_state,
        "links": {
            "flink_public_ip": FLINK_PUBLIC_IP or None,
        },
    })


@app.route("/api/demo/start", methods=["POST"])
def start_demo():
    """Clear previous data and start the pipeline."""
    demo_state["status"] = "starting"
    demo_state["message"] = "Clearing previous results..."

    def _start():
        try:
            # Clear MongoDB collections
            db.network_health_predictions.delete_many({})
            db.windowed_network_metrics.delete_many({})

            # Start Flink job
            demo_state["message"] = "Starting Flink job..."
            if FLINK_IP:
                ok, out = run_ssh(FLINK_IP, "/opt/flink-job/restart.sh", timeout=60)
                if not ok:
                    demo_state["status"] = "error"
                    demo_state["message"] = f"Flink failed: {out[-200:]}"
                    return

            # Start generator
            demo_state["message"] = "Starting data generator..."
            if GENERATOR_IP:
                ok, out = run_ssh(GENERATOR_IP, "/opt/telco-generator/start.sh", timeout=30)
                if not ok:
                    demo_state["status"] = "error"
                    demo_state["message"] = f"Generator failed: {out[-200:]}"
                    return

            demo_state["status"] = "running"
            demo_state["message"] = "Pipeline running — data will appear in ~30 seconds"
        except Exception as e:
            demo_state["status"] = "error"
            demo_state["message"] = str(e)[:200]

    threading.Thread(target=_start, daemon=True).start()
    return jsonify({"ok": True, "message": "Starting demo..."})


@app.route("/api/demo/stop", methods=["POST"])
def stop_demo():
    """Stop the pipeline."""
    demo_state["status"] = "stopping"
    demo_state["message"] = "Stopping pipeline..."

    def _stop():
        try:
            if GENERATOR_IP:
                run_ssh(GENERATOR_IP, "pkill -f generator.py 2>/dev/null")

            if FLINK_IP:
                run_ssh(FLINK_IP, "/opt/flink-job/stop.sh")

            time.sleep(3)
            db.network_health_predictions.delete_many({})
            db.windowed_network_metrics.delete_many({})

            demo_state["status"] = "stopped"
            demo_state["message"] = "Pipeline stopped — data cleared"
        except Exception as e:
            demo_state["status"] = "error"
            demo_state["message"] = str(e)[:200]

    threading.Thread(target=_stop, daemon=True).start()
    return jsonify({"ok": True, "message": "Stopping demo..."})


@app.route("/api/feast")
def feast_stats():
    """Query Feast feature server and return feature store status."""
    result = {
        "cells_materialized": 0,
        "last_updated": None,
        "retrieval_ms": None,
        "sample_features": [],
        "status": "unavailable",
    }

    try:
        # Count materialized entities from the feast online store collection
        feast_col = db.get_collection("telco_ods_online")
        result["cells_materialized"] = feast_col.count_documents({})

        # Get last materialization time from Feast online store event_timestamps
        latest = feast_col.find_one(
            {"event_timestamps.windowed_cell_metrics": {"$exists": True}},
            {"event_timestamps.windowed_cell_metrics": 1},
            sort=[("event_timestamps.windowed_cell_metrics", -1)],
        )
        if latest:
            ts = latest.get("event_timestamps", {}).get("windowed_cell_metrics")
            if ts:
                result["last_updated"] = ts.isoformat() + "Z"

        # Query Feast feature server for sample cells
        if FEAST_IP:
            sample_cells = ["CELL_0001", "CELL_0010", "CELL_0020", "CELL_0030", "CELL_0040"]
            feast_url = f"http://{FEAST_IP}:6566/get-online-features"
            payload = {
                "features": [
                    "windowed_cell_metrics:avg_signal_strength_dbm",
                    "windowed_cell_metrics:avg_throughput_mbps",
                    "windowed_cell_metrics:avg_latency_ms",
                    "windowed_cell_metrics:event_count",
                    "windowed_cell_metrics:region",
                ],
                "entities": {"cell_id": sample_cells},
            }

            start_time = time.time()
            resp = requests.post(feast_url, json=payload, timeout=5)
            elapsed_ms = (time.time() - start_time) * 1000
            result["retrieval_ms"] = round(elapsed_ms, 1)

            if resp.status_code == 200:
                data = resp.json()
                metadata = data.get("metadata", {})
                features = metadata.get("feature_names", [])
                results_list = data.get("results", [])

                for i, cell_id in enumerate(sample_cells):
                    row = {"cell_id": cell_id}
                    for j, feat_name in enumerate(features):
                        if j < len(results_list) and i < len(results_list[j].get("values", [])):
                            row[feat_name] = results_list[j]["values"][i]
                    result["sample_features"].append(row)

                result["status"] = "healthy"
            else:
                result["status"] = "error"
        else:
            result["status"] = "not_configured"

    except requests.exceptions.ConnectionError:
        result["status"] = "unavailable"
    except Exception as e:
        result["status"] = "error"

    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
