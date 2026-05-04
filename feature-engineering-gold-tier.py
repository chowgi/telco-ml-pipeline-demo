"""
Telco ODS Demo Aggregations
Showcases MongoDB aggregation pipeline power for ML workflows
"""

from pymongo import MongoClient
from datetime import timedelta, timezone
from dotenv import load_dotenv
import os, datetime, time

# Connect to MongoDB Atlas
load_dotenv(".env")
MONGODB_URI = os.getenv('MONGODB_URI')
client = MongoClient(MONGODB_URI)
db = client.ods_demo_db

# Test connection
try:
    client.admin.command('ping')
    print("Successfully connected to MongoDB Atlas!")
except Exception as e:
    print(f"Connection failed: {e}")

pipeline = [
            # Stage 1: Get data from silver tier collection
            {
                "$match": {
                    "timestamp": {
                        "$gte": datetime.datetime(2025, 8, 20, 0, 0, 0, tzinfo=timezone.utc)
                    }
                }
            },
            # Stage 2: Add computed ML features and business intelligence
            {
                "$addFields": {
                    "gold_tier": {
                        "timestamp": "$timestamp",
                        "imsi": "$imsi",
                        "customer_id": "$customer_id",
                        "cell_id": "$cell_id",
                        "region": "$region",
                        "customer_name": "$customer_name",
                        "service_plan": "$service_plan",
                        "device_type": "$device_type",
                        "cell_technology": "$cell_technology",
                        "cell_capacity": "$cell_capacity",
                        "cell_status": "$cell_status",
                        "signal_strength_dbm": "$signal_strength_dbm",
                        "throughput_mbps": "$throughput_mbps",
                        "connection_quality": "$connection_quality",
                        "latency_ms": "$latency_ms",
                        "call_drop_rate_percent": "$call_drop_rate_percent",
                        "session_failures": "$session_failures",
                        "packet_loss_percent": "$packet_loss_percent",
                        "jitter_ms": "$jitter_ms",
                        "video_buffering_ratio": "$video_buffering_ratio",
                        "voip_clarity_score": "$voip_clarity_score",
                        "app_response_time_ms": "$app_response_time_ms",
                        "qos_level": "$qos_level",
                        # Computed ML features
                        "network_health_score": {
                            "$cond": {
                                "if": {
                                    "$and": [
                                        { "$gte": ["$signal_strength_dbm", -60] },
                                        { "$lte": ["$latency_ms", 50] },
                                        { "$lte": ["$call_drop_rate_percent", 1.0] }
                                    ]
                                },
                                "then": "excellent",
                                "else": {
                                    "$cond": {
                                        "if": {
                                            "$and": [
                                                { "$gte": ["$signal_strength_dbm", -70] },
                                                { "$lte": ["$latency_ms", 100] },
                                                { "$lte": ["$call_drop_rate_percent", 2.0] }
                                            ]
                                        },
                                        "then": "good",
                                        "else": "poor"
                                    }
                                }
                            }
                        },
                        "customer_experience_score": {
                            "$cond": {
                                "if": {
                                    "$and": [
                                        { "$gte": ["$voip_clarity_score", 4.0] },
                                        { "$lte": ["$video_buffering_ratio", 0.05] },
                                        { "$lte": ["$app_response_time_ms", 300] }
                                    ]
                                },
                                "then": "excellent",
                                "else": {
                                    "$cond": {
                                        "if": {
                                            "$and": [
                                                { "$gte": ["$voip_clarity_score", 3.0] },
                                                { "$lte": ["$video_buffering_ratio", 0.15] },
                                                { "$lte": ["$app_response_time_ms", 500] }
                                            ]
                                        },
                                        "then": "good",
                                        "else": "poor"
                                    }
                                }
                            }
                        },
                        "anomaly_flags": {
                            "high_latency": { "$gt": ["$latency_ms", 100] },
                            "poor_signal": { "$lt": ["$signal_strength_dbm", -75] },
                            "high_drop_rate": { "$gt": ["$call_drop_rate_percent", 2.0] },
                            "poor_voip": { "$lt": ["$voip_clarity_score", 3.0] },
                            "high_buffering": { "$gt": ["$video_buffering_ratio", 0.1] }
                        },
                        # Business intelligence features
                        "revenue_impact_score": {
                            "$cond": {
                                "if": { "$eq": ["$service_plan", "enterprise"] },
                                "then": {
                                    "$multiply": [
                                        { "$subtract": [5, "$qos_level"] },
                                        100
                                    ]
                                },
                                "else": {
                                    "$multiply": [
                                        { "$subtract": [5, "$qos_level"] },
                                        50
                                    ]
                                }
                            }
                        },
                        "network_efficiency_score": {
                            "$divide": [
                                { "$multiply": ["$throughput_mbps", 100] },
                                { "$add": ["$latency_ms", 1] }
                            ]
                        }
                    }
                }
            },
            # Stage 3: Project only the gold tier data
            {
                "$project": {
                    "gold_tier": 1,
                    "_id": 0
                }
            },
            # Stage 4: Replace the document with gold_tier content
            {
                "$replaceRoot": {
                    "newRoot": "$gold_tier"
                }
            },
            # Stage 5: Output to gold tier collection
            {
                "$out": "gold_tier_features"
            }
        ]


#Run the pipeline
db.silver_tier_features.aggregate(pipeline)

# Get the first 3 results from the silver_tier_analytics collection
gold_tier_results = db.gold_tier_features.find().limit(3)

# Print the first 3 results nicely
print("First 3 results from gold_tier_features:")
for i, doc in enumerate(gold_tier_results):
    print(f"\nResult {i+1}:")
    print(doc)