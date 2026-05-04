#!/usr/bin/env python3
"""
High-Volume Real-time Network Health Inference Test with Confluent Cloud Kafka
Tests the streaming pipeline with 100 records for performance testing
"""

import os
import time
import json
import random
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class HighVolumeKafkaTester:
    def __init__(self):
        """Initialize the high-volume Kafka tester"""
        self.mongodb_uri = os.getenv('MONGODB_URI')
        self.db_name = 'ods_demo_db'
        
        # Initialize MongoDB connection
        try:
            self.client = MongoClient(self.mongodb_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            print(f"✅ Connected to MongoDB Atlas: {self.db_name}")
        except ServerSelectionTimeoutError:
            print("❌ Failed to connect to MongoDB Atlas. Please check your URI.")
            raise
        
        # Collections
        self.incoming_collection = self.db.incoming_network_data
        self.results_collection = self.db.network_health_predictions
        
        # Kafka configuration
        self.kafka_topic = "network_telemetry"
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET'),
            'batch.size': 16384,  # Increase batch size for better throughput
            'linger.ms': 10,       # Wait up to 10ms to batch messages
            'compression.type': 'snappy'  # Enable compression
        }
        
        # Initialize Kafka producer
        try:
            self.producer = Producer(self.kafka_config)
            print(f"✅ Connected to Confluent Cloud Kafka (optimized for high volume)")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            raise

    def generate_test_network_data(self, scenario_type="random"):
        """Generate test network data based on scenario type"""
        
        if scenario_type == "excellent":
            return {
                "signal_strength_dbm": random.uniform(-45, -50),
                "throughput_mbps": random.uniform(120, 200),
                "latency_ms": random.uniform(10, 25),
                "call_drop_rate_percent": random.uniform(0.0, 0.5),
                "packet_loss_percent": random.uniform(0.0, 0.5),
                "jitter_ms": random.uniform(0.5, 2.0),
                "imsi": f"IMSI_{random.randint(100000, 999999)}",
                "customer_id": f"CUST_{random.randint(1000, 9999)}",
                "region": random.choice(["Sydney", "Melbourne", "Brisbane"]),
                "device_type": random.choice(["iPhone", "Samsung", "Huawei"]),
                "cell_technology": random.choice(["5G", "4G"])
            }
        elif scenario_type == "good":
            return {
                "signal_strength_dbm": random.uniform(-60, -70),
                "throughput_mbps": random.uniform(50, 100),
                "latency_ms": random.uniform(30, 60),
                "call_drop_rate_percent": random.uniform(0.5, 2.0),
                "packet_loss_percent": random.uniform(0.5, 2.0),
                "jitter_ms": random.uniform(2.0, 5.0),
                "imsi": f"IMSI_{random.randint(100000, 999999)}",
                "customer_id": f"CUST_{random.randint(1000, 9999)}",
                "region": random.choice(["Sydney", "Melbourne", "Brisbane"]),
                "device_type": random.choice(["iPhone", "Samsung", "Huawei"]),
                "cell_technology": random.choice(["5G", "4G"])
            }
        elif scenario_type == "poor":
            return {
                "signal_strength_dbm": random.uniform(-80, -95),
                "throughput_mbps": random.uniform(5, 30),
                "latency_ms": random.uniform(100, 300),
                "call_drop_rate_percent": random.uniform(3.0, 8.0),
                "packet_loss_percent": random.uniform(2.0, 6.0),
                "jitter_ms": random.uniform(8.0, 20.0),
                "imsi": f"IMSI_{random.randint(100000, 999999)}",
                "customer_id": f"CUST_{random.randint(1000, 9999)}",
                "region": random.choice(["Sydney", "Melbourne", "Brisbane"]),
                "device_type": random.choice(["iPhone", "Samsung", "Huawei"]),
                "cell_technology": random.choice(["5G", "4G"])
            }
        else:  # random
            return {
                "signal_strength_dbm": random.uniform(-100, -30),
                "throughput_mbps": random.uniform(1, 300),
                "latency_ms": random.uniform(5, 500),
                "call_drop_rate_percent": random.uniform(0.0, 10.0),
                "packet_loss_percent": random.uniform(0.0, 10.0),
                "jitter_ms": random.uniform(0.1, 50.0),
                "imsi": f"IMSI_{random.randint(100000, 999999)}",
                "customer_id": f"CUST_{random.randint(1000, 9999)}",
                "region": random.choice(["Sydney", "Melbourne", "Brisbane"]),
                "device_type": random.choice(["iPhone", "Samsung", "Huawei"]),
                "cell_technology": random.choice(["5G", "4G"])
            }

    def _delivery_callback(self, err, msg):
        """Kafka delivery callback - silent for high volume"""
        if err:
            print(f"❌ Message delivery failed: {err}")

    def produce_network_data_batch(self, scenario_type="random", count=100):
        """Send test network data to Kafka topic one at a time with 1-second intervals"""
        print(f"📤 Sending {count} {scenario_type} network data records to Kafka (1 per second)...")
        
        start_time = time.time()
        produced_messages = []
        test_id_prefix = f"TEST_{int(time.time())}"
        
        for i in range(count):
            test_data = self.generate_test_network_data(scenario_type)
            test_data["timestamp"] = datetime.now(timezone.utc)
            test_data["test_scenario"] = scenario_type
            test_data["test_id"] = f"{test_id_prefix}_{i}"
            
            message_key = f"{scenario_type}_{test_data['test_id']}"
            message_value = json.dumps(test_data, default=str)
            
            try:
                self.producer.produce(
                    topic=self.kafka_topic,
                    key=message_key,
                    value=message_value,
                    callback=self._delivery_callback
                )
                produced_messages.append({
                    "test_id": test_data["test_id"],
                    "scenario": scenario_type,
                    "key": message_key
                })
                
                print(f"  📤 Sent {i+1}/{count} - {test_data['customer_id']} ({test_data['region']})")
                
                # Flush immediately to ensure message is sent
                self.producer.flush()
                
                # Wait 1 second before next message (except for the last one)
                if i < count - 1:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"  ❌ Failed to send message {i+1}: {e}")
        
        end_time = time.time()
        duration = end_time - start_time
        rate = count / duration if duration > 0 else 0
        
        print(f"  ✅ Batch completed: {count} messages in {duration:.2f}s ({rate:.1f} msg/s)")
        return produced_messages

    def monitor_predictions_fast(self, test_ids, timeout=120):
        """Fast monitoring for predictions with progress updates"""
        print(f"👀 Monitoring for predictions (timeout: {timeout}s)...")
        print(f"   Checking MongoDB collection: network_health_predictions")
        print(f"   Looking for {len(test_ids)} test IDs...")
        
        start_time = time.time()
        predictions_found = []
        last_count = 0
        
        while time.time() - start_time < timeout:
            # Check for predictions
            predictions = list(self.results_collection.find({
                "metadata.test_id": {"$in": test_ids}
            }))
            
            current_count = len(predictions)
            if current_count > last_count:
                print(f"  📈 Found {current_count}/{len(test_ids)} predictions...")
                last_count = current_count
            
            if current_count == len(test_ids):
                print(f"  ✅ All {len(test_ids)} predictions received!")
                break
                
            time.sleep(2)  # Check every 2 seconds
        
        final_count = len(predictions)
        if final_count < len(test_ids):
            print(f"  ⚠️ Only {final_count}/{len(test_ids)} predictions received within timeout")
        
        return predictions

    def run_high_volume_test(self):
        """Run high-volume real-time inference test"""
        print("🚀 Starting High-Volume Network Health Inference Test via Kafka")
        print("=" * 80)
        print("📊 Test Configuration:")
        print("  • Total records: 100")
        print("  • Scenarios: excellent (25), good (25), poor (25), random (25)")
        print("  • Timing: 1 record per second")
        print("  • Timeout: 120 seconds")
        print("=" * 80)
        
        all_predictions = []
        test_scenarios = [
            ("excellent", 25),
            ("good", 25), 
            ("poor", 25),
            ("random", 25)
        ]
        
        total_start_time = time.time()
        
        for scenario_type, count in test_scenarios:
            print(f"\n🧪 Testing {scenario_type} scenario ({count} records)...")
            print("-" * 50)
            
            # Send data to Kafka
            produced_messages = self.produce_network_data_batch(scenario_type, count)
            test_ids = [msg["test_id"] for msg in produced_messages]
            
            # Wait briefly for Kafka processing
            print("⏳ Waiting 3 seconds for Kafka processing...")
            time.sleep(3)
            
            # Monitor for predictions
            predictions = self.monitor_predictions_fast(test_ids, timeout=120)
            all_predictions.extend(predictions)
            
            # Brief pause between scenarios
            if scenario_type != test_scenarios[-1][0]:
                print("⏳ Brief pause before next scenario...")
                time.sleep(2)
        
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        
        # Summary
        print("\n" + "=" * 80)
        print("📊 HIGH-VOLUME TEST SUMMARY")
        print("=" * 80)
        print(f"Total test duration: {total_duration:.2f} seconds")
        print(f"Total predictions received: {len(all_predictions)}")
        print(f"Success rate: {len(all_predictions)}/100 ({len(all_predictions)}%)")
        
        if all_predictions:
            # Analyze prediction distribution
            prediction_counts = {}
            for pred in all_predictions:
                label = pred['prediction']['label']
                prediction_counts[label] = prediction_counts.get(label, 0) + 1
            
            print(f"\nPrediction distribution:")
            for label, count in prediction_counts.items():
                print(f"  {label}: {count}")
            
            # Performance metrics
            avg_processing_time = total_duration / len(all_predictions) if all_predictions else 0
            print(f"\nPerformance metrics:")
            print(f"  Average processing time per prediction: {avg_processing_time:.2f}s")
            print(f"  Throughput: {len(all_predictions)/total_duration:.2f} predictions/second")
        
        print(f"\n🎉 High-volume Kafka testing completed!")
        return all_predictions

    def cleanup_test_data(self):
        """Clean up test data from collections"""
        print("🧹 Cleaning up test data...")
        
        # Remove test data from incoming collection
        incoming_deleted = self.incoming_collection.delete_many({
            "test_scenario": {"$exists": True}
        })
        print(f"  Removed {incoming_deleted.deleted_count} test records from incoming_network_data")
        
        # Remove predictions from results collection
        results_deleted = self.results_collection.delete_many({
            "metadata.test_id": {"$regex": "^TEST_"}
        })
        print(f"  Removed {results_deleted.deleted_count} prediction records from network_health_predictions")

def main():
    """Main function to run high-volume Kafka real-time inference tests"""
    try:
        tester = HighVolumeKafkaTester()
        
        print("🔧 High-Volume Real-time Network Health Inference Tester (Kafka)")
        print("This script tests the streaming pipeline with 100 records for performance testing")
        print("Optimized for speed and high throughput.\n")
        
        # Run high-volume test
        predictions = tester.run_high_volume_test()
        
        # Ask if user wants to cleanup
        cleanup = input("\n🧹 Clean up test data? (y/n): ").lower().strip()
        if cleanup == 'y':
            tester.cleanup_test_data()
        
        print("\n✅ High-volume Kafka testing completed!")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        raise

if __name__ == "__main__":
    main()

