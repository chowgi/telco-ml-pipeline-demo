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
            # Stage 1: Get recent RAN metrics (8th, August 2025)
            {
                "$match": {
                    "timestamp": {
                        "$gte": datetime.datetime(2025, 8, 20, 0, 0, 0, tzinfo=timezone.utc)
                    }
                }
            },
            # Stage 2: Lookup customer information
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "imsi",
                    "foreignField": "imsi",
                    "as": "customer_info"
                }
            },
            # Stage 3: Unwind customer array
            {
                "$unwind": {
                    "path": "$customer_info",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # Stage 4: Lookup cell information
            {
                "$lookup": {
                    "from": "cells",
                    "localField": "cell_id",
                    "foreignField": "cell_id",
                    "as": "cell_info"
                }
            },
            # Stage 5: Unwind cell array
            {
                "$unwind": {
                    "path": "$cell_info",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # Stage 6: Lookup core network metrics
            {
                "$lookup": {
                    "from": "core_network_metrics",
                    "let": {
                        "imsi": "$imsi",
                        "timestamp": "$timestamp"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        { "$eq": ["$imsi", "$$imsi"] },
                                        { "$gte": ["$timestamp", { "$subtract": ["$$timestamp", 60000] }] },
                                        { "$lte": ["$timestamp", { "$add": ["$$timestamp", 60000] }] }
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "core_metrics"
                }
            },
            # Stage 7: Unwind core metrics array
            {
                "$unwind": {
                    "path": "$core_metrics",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # Stage 8: Lookup mobile service metrics
            {
                "$lookup": {
                    "from": "mobile_service_metrics",
                    "let": {
                        "imsi": "$imsi",
                        "timestamp": "$timestamp"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        { "$eq": ["$imsi", "$$imsi"] },
                                        { "$gte": ["$timestamp", { "$subtract": ["$$timestamp", 60000] }] },
                                        { "$lte": ["$timestamp", { "$add": ["$$timestamp", 60000] }] }
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "service_metrics"
                }
            },
            # Stage 9: Unwind service metrics array
            {
                "$unwind": {
                    "path": "$service_metrics",
                    "preserveNullAndEmptyArrays": True
                }
            },
            # Stage 10: Project only the silver tier data
            {
                "$addFields": {
                    "silver_tier": {
                        "timestamp": "$timestamp",
                        "imsi": "$imsi",
                        "customer_id": "$customer_id",
                        "cell_id": "$cell_id",
                        "region": "$region",
                        "customer_name": "$customer_info.name",
                        "service_plan": "$customer_info.service_plan",
                        "device_type": "$customer_info.device_type",
                        "cell_technology": "$cell_info.technology",
                        "cell_capacity": "$cell_info.capacity",
                        "cell_status": "$cell_info.status",
                        "signal_strength_dbm": "$signal_strength_dbm",
                        "throughput_mbps": "$throughput_mbps",
                        "connection_quality": "$connection_quality",
                        "latency_ms": "$core_metrics.latency_ms",
                        "call_drop_rate_percent": "$core_metrics.call_drop_rate_percent",
                        "session_failures": "$core_metrics.session_failures",
                        "packet_loss_percent": "$core_metrics.packet_loss_percent",
                        "jitter_ms": "$core_metrics.jitter_ms",
                        "video_buffering_ratio": "$service_metrics.video_buffering_ratio",
                        "voip_clarity_score": "$service_metrics.voip_clarity_score",
                        "app_response_time_ms": "$service_metrics.app_response_time_ms",
                        "qos_level": "$service_metrics.qos_level"
                    }
                }
            },
            # Stage 11: Project only the silver tier data
            {
                "$project": {
                    "silver_tier": 1,
                    "_id": 0
                }
            },
            # Stage 12: Replace the document with silver_tier content
            {
                "$replaceRoot": {
                    "newRoot": "$silver_tier"
                }
            },
            # Stage 13: Output to silver tier collection
            {
                "$out": "silver_tier_features"
            }
        ]

#Run the pipeline
db.ran_network_metrics.aggregate(pipeline)

# Get the first 3 results from the silver_tier_analytics collection
silver_tier_results = db.silver_tier_features.find().limit(3)

# Print the first 3 results nicely
print("First 3 results from silver_tier_features:")
for i, doc in enumerate(silver_tier_results):
    print(f"\nResult {i+1}:")
    print(doc)