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
SSH_OPTS = f"-o ConnectTimeout=10 -o StrictHostKeyChecking=no -i {SSH_KEY}"

client = MongoClient(MONGODB_URI)
db = client[MONGODB_DB]

# Track demo state
demo_state = {"status": "unknown", "message": ""}


def run_ssh(host, command, timeout=60):
    """Run a command on a remote host via SSH."""
    cmd = f'ssh {SSH_OPTS} ubuntu@{host} bash -s'
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        input=command, timeout=timeout,
    )
    return result.returncode == 0, result.stdout + result.stderr


@app.route("/")
def index():
    return render_template("index.html")


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
            demo_state["message"] = "Starting data generator..."

            # Kill Flink cluster and restart fresh (PyFlink needs clean task managers)
            if FLINK_IP:
                run_ssh(FLINK_IP, (
                    "ps aux | grep org.apache.flink | grep -v grep | "
                    "awk '{print $2}' | xargs -r kill -9 2>/dev/null; "
                    "sleep 2; "
                    "rm -f /opt/flink/log/*.pid; "
                    "/opt/flink/bin/start-cluster.sh 2>/dev/null"
                ))
                time.sleep(5)
                run_ssh(FLINK_IP, (
                    "source /opt/flink-env/bin/activate && "
                    "export $(cat /opt/flink-job-config.env | xargs) && "
                    "/opt/flink/bin/flink run -py /opt/flink-job/flink_job.py "
                    "-pyexec /opt/flink-env/bin/python3 -d 2>&1 | grep -v WARNING"
                ))

            # Start generator
            if GENERATOR_IP:
                ok, out = run_ssh(GENERATOR_IP, (
                    "pkill -f generator.py 2>/dev/null; sleep 2; "
                    "cd /opt/telco-generator && "
                    "source venv/bin/activate && "
                    "source /opt/telco-generator/env.sh && "
                    "nohup python -u generator.py </dev/null >> /var/log/generator.log 2>&1 & "
                    "sleep 2; ps aux | grep -q '[g]enerator.py'"
                ))

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
                run_ssh(FLINK_IP, (
                    "ps aux | grep org.apache.flink | grep -v grep | "
                    "awk '{print $2}' | xargs -r kill -9 2>/dev/null"
                ))

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
