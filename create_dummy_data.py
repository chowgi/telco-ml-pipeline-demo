#!/usr/bin/env python3
"""
Telco ODS Dummy Data Generator
Creates realistic dummy data for Operational Data Store demo
"""

import os
import json
import random
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import numpy as np
import pandas as pd
from faker import Faker
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TelcoODSDataGenerator:
    def __init__(self):
        """Initialize the data generator with MongoDB connection"""
        self.fake = Faker('en_AU')  # Australian locale for telco demo
        self.mongodb_uri = os.getenv('MONGODB_URI')
        self.db_name = 'ods_demo_db'
        
        # Initialize MongoDB connection
        try:
            self.client = MongoClient(self.mongodb_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')  # Test connection
            self.db = self.client[self.db_name]
            print(f"✅ Connected to MongoDB Atlas: {self.db_name}")
        except ServerSelectionTimeoutError:
            print("❌ Failed to connect to MongoDB Atlas. Please check your URI.")
            raise
        
        # Data generation parameters
        self.num_customers = 1000
        self.num_cells = 50
        self.num_regions = 5
        self.days_of_data = 30
        self.data_interval_seconds = 60
        
        # Australian regions for telco demo
        self.regions = [
            {'name': 'Sydney', 'lat': -33.8688, 'lng': 151.2093},
            {'name': 'Melbourne', 'lat': -37.8136, 'lng': 144.9631},
            {'name': 'Brisbane', 'lat': -27.4698, 'lng': 153.0251},
            {'name': 'Perth', 'lat': -31.9505, 'lng': 115.8605},
            {'name': 'Adelaide', 'lat': -34.9285, 'lng': 138.6007}
        ]
        
        # Service plans
        self.service_plans = [
            {'plan_id': 'basic', 'name': 'Basic Plan', 'qos_level': 1, 'data_limit_gb': 10},
            {'plan_id': 'standard', 'name': 'Standard Plan', 'qos_level': 2, 'data_limit_gb': 50},
            {'plan_id': 'premium', 'name': 'Premium Plan', 'qos_level': 3, 'data_limit_gb': 100},
            {'plan_id': 'enterprise', 'name': 'Enterprise Plan', 'qos_level': 4, 'data_limit_gb': 500}
        ]
        
        # Device types
        self.device_types = [
            'iPhone 15', 'iPhone 14', 'Samsung Galaxy S24', 'Samsung Galaxy S23',
            'Google Pixel 8', 'OnePlus 11', 'Xiaomi 13', 'Huawei P60'
        ]

    def generate_customer_data(self) -> List[Dict]:
        """Generate customer profiles with IMSI mapping"""
        print("📱 Generating customer data...")
        
        customers = []
        for i in range(self.num_customers):
            region = random.choice(self.regions)
            service_plan = random.choice(self.service_plans)
            
            # Generate realistic IMSI (Australian format: 505 + 2 digits + 8 digits)
            imsi = f"505{random.randint(10, 99)}{random.randint(10000000, 99999999)}"
            
            customer = {
                'customer_id': f"CUST_{i:06d}",
                'imsi': imsi,
                'name': self.fake.name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'region': region['name'],
                'service_plan': service_plan['plan_id'],
                'device_type': random.choice(self.device_types),
                'activation_date': datetime.combine(self.fake.date_between(start_date='-2y', end_date='today'), datetime.min.time()),
                'status': random.choices(['active', 'suspended', 'inactive'], weights=[0.85, 0.10, 0.05])[0],
                'created_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc)
            }
            customers.append(customer)
        
        return customers

    def generate_cell_data(self) -> List[Dict]:
        """Generate cell tower information"""
        print("📡 Generating cell tower data...")
        
        cells = []
        for i in range(self.num_cells):
            region = random.choice(self.regions)
            
            # Generate realistic cell coordinates within region
            lat = region['lat'] + random.uniform(-0.1, 0.1)
            lng = region['lng'] + random.uniform(-0.1, 0.1)
            
            cell = {
                'cell_id': f"CELL_{i:04d}",
                'cell_name': f"Cell Tower {i+1}",
                'region': region['name'],
                'latitude': lat,
                'longitude': lng,
                'technology': random.choice(['4G', '5G']),
                'capacity': random.randint(100, 1000),
                'status': random.choices(['active', 'maintenance', 'degraded'], weights=[0.90, 0.05, 0.05])[0],
                'created_at': datetime.now(timezone.utc)
            }
            cells.append(cell)
        
        return cells

    def _generate_customer_samples_for_all_timestamps(self, customers: List[Dict]) -> Dict[int, List[Dict]]:
        """Pre-generate customer samples for all timestamps to ensure consistency"""
        print("🔄 Pre-generating customer samples for all timestamps...")
        
        samples = {}
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self.days_of_data)
        
        current_time = start_time
        while current_time <= end_time:
            # Use timestamp as seed for consistent sampling
            seed_value = int(current_time.timestamp())
            random.seed(seed_value)
            sample = random.sample(customers, min(3, len(customers)))
            samples[seed_value] = sample
            current_time += timedelta(seconds=self.data_interval_seconds)
        
        random.seed()  # Reset seed
        print(f"✅ Generated {len(samples)} customer samples")
        return samples

    def _get_customer_sample_for_timestamp(self, customers: List[Dict], timestamp: datetime, samples: Dict[int, List[Dict]]) -> List[Dict]:
        """Get consistent customer sample for a given timestamp across all collections"""
        seed_value = int(timestamp.timestamp())
        return samples.get(seed_value, random.sample(customers, min(3, len(customers))))

    def generate_ran_metrics(self, customers: List[Dict], cells: List[Dict], customer_samples: Dict[int, List[Dict]]) -> List[Dict]:
        """Generate RAN network metrics time series data"""
        print("📶 Generating RAN network metrics...")
        
        metrics = []
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self.days_of_data)
        
        current_time = start_time
        while current_time <= end_time:
            customer_sample = self._get_customer_sample_for_timestamp(customers, current_time, customer_samples)
            for customer in customer_sample:
                cell = random.choice(cells)
                
                # Generate realistic signal strength (dBm)
                base_signal = random.uniform(-85, -45)
                # Add some variation based on time (peak hours)
                hour = current_time.hour
                if 8 <= hour <= 18:  # Peak hours
                    base_signal += random.uniform(-5, 0)
                else:  # Off-peak
                    base_signal += random.uniform(0, 5)
                
                # Generate throughput (Mbps)
                base_throughput = random.uniform(10, 100)
                if base_signal < -70:  # Poor signal
                    base_throughput *= 0.3
                elif base_signal > -50:  # Excellent signal
                    base_throughput *= 1.2
                
                metric = {
                    'timestamp': current_time,
                    'imsi': customer['imsi'],
                    'customer_id': customer['customer_id'],
                    'cell_id': cell['cell_id'],
                    'region': cell['region'],
                    'signal_strength_dbm': round(base_signal, 2),
                    'throughput_mbps': round(base_throughput, 2),
                    'connection_quality': self._get_connection_quality(base_signal),
                    'technology': cell['technology'],
                    'data_usage_mb': round(random.uniform(0.1, 5.0), 2),
                    'created_at': datetime.now(timezone.utc)
                }
                metrics.append(metric)
            
            current_time += timedelta(seconds=self.data_interval_seconds)
        
        return metrics

    def generate_core_metrics(self, customers: List[Dict], cells: List[Dict], customer_samples: Dict[int, List[Dict]]) -> List[Dict]:
        """Generate Core network metrics time series data"""
        print("🌐 Generating Core network metrics...")
        
        metrics = []
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self.days_of_data)
        
        current_time = start_time
        while current_time <= end_time:
            customer_sample = self._get_customer_sample_for_timestamp(customers, current_time, customer_samples)
            for customer in customer_sample:
                cell = random.choice(cells)
                
                # Generate realistic latency (ms)
                base_latency = random.uniform(10, 50)
                # Add congestion during peak hours
                hour = current_time.hour
                if 8 <= hour <= 18:
                    base_latency += random.uniform(0, 20)
                
                # Generate call drop rate (%)
                base_drop_rate = random.uniform(0.1, 2.0)
                if base_latency > 60:  # High latency
                    base_drop_rate *= 2
                
                # Generate session failures
                session_failures = random.randint(0, 3)
                if base_drop_rate > 1.5:
                    session_failures += random.randint(1, 2)
                
                metric = {
                    'timestamp': current_time,
                    'imsi': customer['imsi'],
                    'customer_id': customer['customer_id'],
                    'cell_id': cell['cell_id'],
                    'region': cell['region'],
                    'latency_ms': round(base_latency, 2),
                    'call_drop_rate_percent': round(base_drop_rate, 2),
                    'session_failures': session_failures,
                    'packet_loss_percent': round(random.uniform(0, 5), 2),
                    'jitter_ms': round(random.uniform(0, 10), 2),
                    'connection_status': 'active' if session_failures == 0 else 'degraded',
                    'created_at': datetime.now(timezone.utc)
                }
                metrics.append(metric)
            
            current_time += timedelta(seconds=self.data_interval_seconds)
        
        return metrics

    def generate_mobile_service_metrics(self, customers: List[Dict], cells: List[Dict], customer_samples: Dict[int, List[Dict]]) -> List[Dict]:
        """Generate Mobile service metrics time series data"""
        print("📱 Generating Mobile service metrics...")
        
        metrics = []
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self.days_of_data)
        
        current_time = start_time
        while current_time <= end_time:
            customer_sample = self._get_customer_sample_for_timestamp(customers, current_time, customer_samples)
            for customer in customer_sample:
                cell = random.choice(cells)
                
                # Generate video buffering metrics
                video_buffering_ratio = random.uniform(0, 0.1)  # 0-10% buffering
                if random.random() < 0.05:  # 5% chance of poor video performance
                    video_buffering_ratio = random.uniform(0.1, 0.3)
                
                # Generate VoIP clarity score (1-5)
                voip_clarity = random.uniform(3.5, 5.0)
                if video_buffering_ratio > 0.15:
                    voip_clarity = random.uniform(2.0, 4.0)
                
                # Generate application response time
                app_response_time = random.uniform(100, 500)  # ms
                if voip_clarity < 3.0:
                    app_response_time += random.uniform(200, 500)
                
                metric = {
                    'timestamp': current_time,
                    'imsi': customer['imsi'],
                    'customer_id': customer['customer_id'],
                    'cell_id': cell['cell_id'],
                    'region': cell['region'],
                    'device_type': customer['device_type'],
                    'service_plan': customer['service_plan'],
                    'video_buffering_ratio': round(video_buffering_ratio, 3),
                    'voip_clarity_score': round(voip_clarity, 1),
                    'app_response_time_ms': round(app_response_time, 2),
                    'qos_level': self._get_qos_level(customer['service_plan']),
                    'service_status': 'active',
                    'created_at': datetime.now(timezone.utc)
                }
                metrics.append(metric)
            
            current_time += timedelta(seconds=self.data_interval_seconds)
        
        return metrics

    def _get_connection_quality(self, signal_strength: float) -> str:
        """Convert signal strength to connection quality"""
        if signal_strength >= -50:
            return 'excellent'
        elif signal_strength >= -60:
            return 'good'
        elif signal_strength >= -70:
            return 'fair'
        else:
            return 'poor'

    def _get_qos_level(self, service_plan: str) -> int:
        """Get QoS level from service plan"""
        plan_map = {'basic': 1, 'standard': 2, 'premium': 3, 'enterprise': 4}
        return plan_map.get(service_plan, 1)

    def create_collections_and_indexes(self):
        """Create collections and indexes for optimal performance"""
        print("🗄️ Creating collections and indexes...")
        
        # Create time series collections
        try:
            # RAN metrics time series collection
            self.db.create_collection(
                "ran_network_metrics",
                timeseries={
                    "timeField": "timestamp",
                    "metaField": "imsi",
                    "granularity": "seconds"
                }
            )
            print("✅ Created RAN metrics time series collection")
        except Exception as e:
            print(f"ℹ️ RAN collection already exists or error: {e}")
        
        try:
            # Core metrics time series collection
            self.db.create_collection(
                "core_network_metrics",
                timeseries={
                    "timeField": "timestamp",
                    "metaField": "imsi",
                    "granularity": "seconds"
                }
            )
            print("✅ Created Core metrics time series collection")
        except Exception as e:
            print(f"ℹ️ Core collection already exists or error: {e}")
        
        try:
            # Mobile service metrics time series collection
            self.db.create_collection(
                "mobile_service_metrics",
                timeseries={
                    "timeField": "timestamp",
                    "metaField": "imsi",
                    "granularity": "seconds"
                }
            )
            print("✅ Created Mobile service metrics time series collection")
        except Exception as e:
            print(f"ℹ️ Mobile service collection already exists or error: {e}")
        
        # Create indexes for optimal query performance
        collections_to_index = [
            ('customers', [('imsi', 1), ('customer_id', 1), ('region', 1)]),
            ('cells', [('cell_id', 1), ('region', 1), ('technology', 1)]),
            ('service_plans', [('plan_id', 1)]),
            ('ran_network_metrics', [('cell_id', 1), ('region', 1), ('technology', 1)]),
            ('core_network_metrics', [('cell_id', 1), ('region', 1), ('connection_status', 1)]),
            ('mobile_service_metrics', [('cell_id', 1), ('region', 1), ('service_plan', 1), ('device_type', 1)])
        ]
        
        for collection_name, indexes in collections_to_index:
            collection = self.db[collection_name]
            for field, direction in indexes:
                try:
                    collection.create_index([(field, direction)])
                    print(f"✅ Created index on {collection_name}.{field}")
                except Exception as e:
                    print(f"ℹ️ Index on {collection_name}.{field} already exists or error: {e}")

    def insert_data(self, customers: List[Dict], cells: List[Dict], 
                   ran_metrics: List[Dict], core_metrics: List[Dict], 
                   mobile_metrics: List[Dict]):
        """Insert all generated data into MongoDB"""
        print("💾 Inserting data into MongoDB...")
        
        # Insert reference data
        self.db.customers.insert_many(customers)
        print(f"✅ Inserted {len(customers)} customers")
        
        self.db.cells.insert_many(cells)
        print(f"✅ Inserted {len(cells)} cells")
        
        self.db.service_plans.insert_many(self.service_plans)
        print(f"✅ Inserted {len(self.service_plans)} service plans")
        
        # Insert time series data in batches
        batch_size = 1000
        
        # RAN metrics
        for i in range(0, len(ran_metrics), batch_size):
            batch = ran_metrics[i:i + batch_size]
            self.db.ran_network_metrics.insert_many(batch)
        print(f"✅ Inserted {len(ran_metrics)} RAN metrics")
        
        # Core metrics
        for i in range(0, len(core_metrics), batch_size):
            batch = core_metrics[i:i + batch_size]
            self.db.core_network_metrics.insert_many(batch)
        print(f"✅ Inserted {len(core_metrics)} Core metrics")
        
        # Mobile service metrics
        for i in range(0, len(mobile_metrics), batch_size):
            batch = mobile_metrics[i:i + batch_size]
            self.db.mobile_service_metrics.insert_many(batch)
        print(f"✅ Inserted {len(mobile_metrics)} Mobile service metrics")

    def generate_all_data(self):
        """Generate and insert all dummy data"""
        print("🚀 Starting Telco ODS dummy data generation...")
        start_time = time.time()
        
        # Create collections and indexes
        self.create_collections_and_indexes()
        
        # Generate data
        customers = self.generate_customer_data()
        cells = self.generate_cell_data()
        
        # Pre-generate customer samples for all timestamps to ensure consistency
        customer_samples = self._generate_customer_samples_for_all_timestamps(customers)
        
        ran_metrics = self.generate_ran_metrics(customers, cells, customer_samples)
        core_metrics = self.generate_core_metrics(customers, cells, customer_samples)
        mobile_metrics = self.generate_mobile_service_metrics(customers, cells, customer_samples)
        
        # Insert data
        self.insert_data(customers, cells, ran_metrics, core_metrics, mobile_metrics)
        
        end_time = time.time()
        print(f"\n🎉 Data generation completed in {end_time - start_time:.2f} seconds!")
        print(f"📊 Generated data summary:")
        print(f"   - {len(customers)} customers")
        print(f"   - {len(cells)} cells")
        print(f"   - {len(ran_metrics)} RAN metrics")
        print(f"   - {len(core_metrics)} Core metrics")
        print(f"   - {len(mobile_metrics)} Mobile service metrics")
        print(f"   - Total records: {len(customers) + len(cells) + len(ran_metrics) + len(core_metrics) + len(mobile_metrics)}")

def main():
    """Main function to run the data generation"""
    try:
        generator = TelcoODSDataGenerator()
        generator.generate_all_data()
    except Exception as e:
        print(f"❌ Error during data generation: {e}")
        raise

if __name__ == "__main__":
    main()
