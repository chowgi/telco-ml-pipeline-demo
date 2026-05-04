#!/usr/bin/env python3
"""
Comprehensive Model Prediction Tests for Telco ODS Network Health Classifier
Tests the trained model against real data patterns to verify correct responses
"""

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Tuple
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv

# ML libraries
import mlflow
import mlflow.sklearn
from sklearn.preprocessing import StandardScaler, LabelEncoder
import warnings
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

class ModelPredictionTester:
    def __init__(self):
        """Initialize the model tester with MongoDB and MLflow connections"""
        self.mongodb_uri = os.getenv('MONGODB_URI')
        self.db_name = 'ods_demo_db'
        self.mlflow_tracking_uri = "http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5002/"
        
        # Initialize MongoDB connection
        try:
            self.client = MongoClient(self.mongodb_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            print(f"✅ Connected to MongoDB Atlas: {self.db_name}")
        except ServerSelectionTimeoutError:
            print("❌ Failed to connect to MongoDB Atlas. Please check your URI.")
            raise
        
        # Initialize MLflow
        try:
            mlflow.set_tracking_uri(self.mlflow_tracking_uri)
            print(f"✅ Connected to MLflow: {self.mlflow_tracking_uri}")
        except Exception as e:
            print(f"❌ Failed to connect to MLflow: {e}")
            raise

    def load_real_test_cases(self) -> Dict[str, List[Dict]]:
        """Load real test cases from the gold tier data"""
        print("📊 Loading real test cases from gold tier data...")
        
        test_cases = {
            'excellent': [],
            'good': [],
            'poor': []
        }
        
        # Get excellent cases (only 2 in dataset)
        excellent_cases = list(self.db.gold_tier_features.find(
            {'network_health_score': 'excellent'}, 
            {'_id': 0, 'signal_strength_dbm': 1, 'throughput_mbps': 1, 'latency_ms': 1, 
             'call_drop_rate_percent': 1, 'packet_loss_percent': 1, 'jitter_ms': 1, 
             'network_health_score': 1}
        ).limit(2))
        
        # Get good cases
        good_cases = list(self.db.gold_tier_features.find(
            {'network_health_score': 'good'}, 
            {'_id': 0, 'signal_strength_dbm': 1, 'throughput_mbps': 1, 'latency_ms': 1, 
             'call_drop_rate_percent': 1, 'packet_loss_percent': 1, 'jitter_ms': 1, 
             'network_health_score': 1}
        ).limit(5))
        
        # Get poor cases
        poor_cases = list(self.db.gold_tier_features.find(
            {'network_health_score': 'poor'}, 
            {'_id': 0, 'signal_strength_dbm': 1, 'throughput_mbps': 1, 'latency_ms': 1, 
             'call_drop_rate_percent': 1, 'packet_loss_percent': 1, 'jitter_ms': 1, 
             'network_health_score': 1}
        ).limit(5))
        
        test_cases['excellent'] = excellent_cases
        test_cases['good'] = good_cases
        test_cases['poor'] = poor_cases
        
        print(f"✅ Loaded test cases: {len(excellent_cases)} excellent, {len(good_cases)} good, {len(poor_cases)} poor")
        return test_cases

    def create_edge_case_tests(self) -> List[Dict]:
        """Create edge case tests based on network performance characteristics"""
        print("🧪 Creating edge case tests...")
        
        edge_cases = [
            {
                "name": "Perfect Network Conditions",
                "description": "Ideal network with excellent signal, high throughput, low latency",
                "data": [-40, 200, 5, 0.0, 0.0, 0.5],
                "expected": "excellent",
                "features": ['signal_strength_dbm', 'throughput_mbps', 'latency_ms', 
                           'call_drop_rate_percent', 'packet_loss_percent', 'jitter_ms']
            },
            {
                "name": "Very Poor Signal",
                "description": "Extremely weak signal strength",
                "data": [-100, 5, 200, 5.0, 5.0, 15.0],
                "expected": "poor",
                "features": ['signal_strength_dbm', 'throughput_mbps', 'latency_ms', 
                           'call_drop_rate_percent', 'packet_loss_percent', 'jitter_ms']
            },
            {
                "name": "High Latency Network",
                "description": "Good signal but very high latency",
                "data": [-50, 100, 500, 1.0, 1.0, 2.0],
                "expected": "poor",
                "features": ['signal_strength_dbm', 'throughput_mbps', 'latency_ms', 
                           'call_drop_rate_percent', 'packet_loss_percent', 'jitter_ms']
            },
            {
                "name": "High Drop Rate",
                "description": "Good conditions but high call drop rate",
                "data": [-55, 80, 30, 8.0, 0.5, 1.0],
                "expected": "poor",
                "features": ['signal_strength_dbm', 'throughput_mbps', 'latency_ms', 
                           'call_drop_rate_percent', 'packet_loss_percent', 'jitter_ms']
            },
            {
                "name": "Moderate Network",
                "description": "Average network conditions",
                "data": [-70, 50, 50, 2.0, 1.5, 5.0],
                "expected": "good",
                "features": ['signal_strength_dbm', 'throughput_mbps', 'latency_ms', 
                           'call_drop_rate_percent', 'packet_loss_percent', 'jitter_ms']
            },
            {
                "name": "Borderline Good Network",
                "description": "Network on the edge between good and poor",
                "data": [-75, 30, 80, 3.0, 2.0, 8.0],
                "expected": "poor",  # Likely to be poor based on data distribution
                "features": ['signal_strength_dbm', 'throughput_mbps', 'latency_ms', 
                           'call_drop_rate_percent', 'packet_loss_percent', 'jitter_ms']
            }
        ]
        
        print(f"✅ Created {len(edge_cases)} edge case tests")
        return edge_cases

    def load_trained_model(self):
        """Load the trained model from MLflow"""
        print("🤖 Loading trained model from MLflow...")
        
        try:
            # Get the latest model version
            client = mlflow.tracking.MlflowClient()
            
            # Get the latest version of the registered model
            model_versions = client.get_latest_versions("telco_ods_network_health_classifier")
            if not model_versions:
                raise ValueError("No registered model found")
            
            latest_version = model_versions[0]
            model_uri = f"models:/telco_ods_network_health_classifier/{latest_version.version}"
            
            # Load the model
            model = mlflow.sklearn.load_model(model_uri)
            print(f"✅ Loaded model version {latest_version.version}")
            
            return model, latest_version
            
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            raise

    def test_model_predictions(self, model, test_cases: Dict[str, List[Dict]], 
                             edge_cases: List[Dict]) -> Dict[str, Any]:
        """Test model predictions against real and edge cases"""
        print("🧪 Testing model predictions...")
        
        results = {
            'real_data_tests': {'excellent': [], 'good': [], 'poor': []},
            'edge_case_tests': [],
            'summary': {}
        }
        
        # Test with real data cases
        print("\n📊 Testing with real data cases:")
        for category, cases in test_cases.items():
            print(f"\n  Testing {category} cases:")
            for i, case in enumerate(cases):
                # Prepare features
                features = [
                    case['signal_strength_dbm'],
                    case['throughput_mbps'], 
                    case['latency_ms'],
                    case['call_drop_rate_percent'],
                    case['packet_loss_percent'],
                    case['jitter_ms']
                ]
                
                # Make prediction
                prediction = model.predict([features])[0]
                probabilities = model.predict_proba([features])[0]
                
                # Get class names (assuming 0=excellent, 1=good, 2=poor)
                class_names = ['excellent', 'good', 'poor']
                predicted_class = class_names[int(prediction)]
                
                test_result = {
                    'case_id': i,
                    'input_features': case,
                    'expected': case['network_health_score'],
                    'predicted': predicted_class,
                    'prediction_encoded': int(prediction),
                    'probabilities': dict(zip(class_names, probabilities)),
                    'correct': predicted_class == case['network_health_score']
                }
                
                results['real_data_tests'][category].append(test_result)
                
                status = "✅" if test_result['correct'] else "❌"
                print(f"    {status} Case {i}: Expected {case['network_health_score']}, Got {predicted_class}")
                print(f"      Features: {features}")
                print(f"      Probabilities: {dict(zip(class_names, probabilities))}")
        
        # Test with edge cases
        print("\n🧪 Testing with edge cases:")
        for case in edge_cases:
            features = case['data']
            prediction = model.predict([features])[0]
            probabilities = model.predict_proba([features])[0]
            
            # Get class names
            class_names = ['excellent', 'good', 'poor']
            predicted_class = class_names[int(prediction)]
            
            test_result = {
                'name': case['name'],
                'description': case['description'],
                'input_features': dict(zip(case['features'], features)),
                'expected': case['expected'],
                'predicted': predicted_class,
                'prediction_encoded': int(prediction),
                'probabilities': dict(zip(class_names, probabilities)),
                'correct': predicted_class == case['expected']
            }
            
            results['edge_case_tests'].append(test_result)
            
            status = "✅" if test_result['correct'] else "❌"
            print(f"  {status} {case['name']}: Expected {case['expected']}, Got {predicted_class}")
            print(f"    Features: {features}")
            print(f"    Probabilities: {dict(zip(class_names, probabilities))}")
        
        # Calculate summary statistics
        total_real_tests = sum(len(cases) for cases in test_cases.values())
        correct_real_tests = sum(
            sum(1 for test in tests if test['correct']) 
            for tests in results['real_data_tests'].values()
        )
        
        total_edge_tests = len(edge_cases)
        correct_edge_tests = sum(1 for test in results['edge_case_tests'] if test['correct'])
        
        results['summary'] = {
            'real_data_accuracy': correct_real_tests / total_real_tests if total_real_tests > 0 else 0,
            'edge_case_accuracy': correct_edge_tests / total_edge_tests if total_edge_tests > 0 else 0,
            'overall_accuracy': (correct_real_tests + correct_edge_tests) / (total_real_tests + total_edge_tests),
            'total_tests': total_real_tests + total_edge_tests,
            'correct_tests': correct_real_tests + correct_edge_tests
        }
        
        return results

    def analyze_prediction_patterns(self, results: Dict[str, Any]) -> None:
        """Analyze prediction patterns and provide insights"""
        print("\n📈 Analyzing prediction patterns...")
        
        # Real data analysis
        print("\n📊 Real Data Analysis:")
        for category, tests in results['real_data_tests'].items():
            if not tests:
                continue
                
            correct_count = sum(1 for test in tests if test['correct'])
            total_count = len(tests)
            accuracy = correct_count / total_count if total_count > 0 else 0
            
            print(f"  {category.capitalize()}: {correct_count}/{total_count} ({accuracy:.2%})")
            
            # Show incorrect predictions
            incorrect_tests = [test for test in tests if not test['correct']]
            if incorrect_tests:
                print(f"    Incorrect predictions:")
                for test in incorrect_tests:
                    print(f"      Expected: {test['expected']}, Got: {test['predicted']}")
                    print(f"      Features: {test['input_features']}")
        
        # Edge case analysis
        print("\n🧪 Edge Case Analysis:")
        correct_edge = sum(1 for test in results['edge_case_tests'] if test['correct'])
        total_edge = len(results['edge_case_tests'])
        edge_accuracy = correct_edge / total_edge if total_edge > 0 else 0
        
        print(f"  Edge Cases: {correct_edge}/{total_edge} ({edge_accuracy:.2%})")
        
        # Show incorrect edge cases
        incorrect_edge = [test for test in results['edge_case_tests'] if not test['correct']]
        if incorrect_edge:
            print(f"    Incorrect edge case predictions:")
            for test in incorrect_edge:
                print(f"      {test['name']}: Expected {test['expected']}, Got {test['predicted']}")
                print(f"      Features: {test['input_features']}")
        
        # Overall summary
        print(f"\n📋 Overall Results:")
        print(f"  Real Data Accuracy: {results['summary']['real_data_accuracy']:.2%}")
        print(f"  Edge Case Accuracy: {results['summary']['edge_case_accuracy']:.2%}")
        print(f"  Overall Accuracy: {results['summary']['overall_accuracy']:.2%}")
        print(f"  Total Tests: {results['summary']['total_tests']}")
        print(f"  Correct Tests: {results['summary']['correct_tests']}")

    def save_test_results(self, results: Dict[str, Any]) -> None:
        """Save test results to MongoDB"""
        print("💾 Saving test results to MongoDB...")
        
        test_doc = {
            'test_date': datetime.now(),
            'test_type': 'model_prediction_validation',
            'results': results,
            'summary': results['summary']
        }
        
        # Save to MongoDB
        self.db.model_test_results.insert_one(test_doc)
        print("✅ Test results saved to MongoDB")

    def run_comprehensive_tests(self):
        """Run comprehensive model prediction tests"""
        print("🚀 Starting Comprehensive Model Prediction Tests")
        print("=" * 80)
        
        try:
            # Load real test cases from database
            test_cases = self.load_real_test_cases()
            
            # Create edge case tests
            edge_cases = self.create_edge_case_tests()
            
            # Load trained model
            model, model_version = self.load_trained_model()
            
            # Run tests
            results = self.test_model_predictions(model, test_cases, edge_cases)
            
            # Analyze results
            self.analyze_prediction_patterns(results)
            
            # Save results
            self.save_test_results(results)
            
            print("\n" + "=" * 80)
            print("🎉 Comprehensive Model Testing Completed!")
            print("=" * 80)
            
            return results
            
        except Exception as e:
            print(f"❌ Error during model testing: {e}")
            raise

def main():
    """Main function to run the model prediction tests"""
    try:
        tester = ModelPredictionTester()
        results = tester.run_comprehensive_tests()
        
        # Print final summary
        print(f"\n📊 Final Test Summary:")
        print(f"  Overall Accuracy: {results['summary']['overall_accuracy']:.2%}")
        print(f"  Total Tests: {results['summary']['total_tests']}")
        print(f"  Correct Tests: {results['summary']['correct_tests']}")
        
    except Exception as e:
        print(f"❌ Error during model testing: {e}")
        raise

if __name__ == "__main__":
    main()
