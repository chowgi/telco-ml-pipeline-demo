#!/usr/bin/env python3
"""
Telco ODS ML Model Training
Trains machine learning models on gold tier data and logs experiments to MLflow
"""

import os
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Tuple
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv

# ML libraries
import mlflow
import mlflow.sklearn
import mlflow.models
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report, confusion_matrix, accuracy_score, precision_score, 
    recall_score, f1_score
)
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.utils.class_weight import compute_class_weight
import warnings
warnings.filterwarnings('ignore')


# Load environment variables
load_dotenv()

class TelcoODSModelTrainer:
    def __init__(self):
        """Initialize the model trainer with MongoDB and MLflow connections"""
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
        
        # Model configuration
        self.experiment_name = "telco_ods_network_health"
        self.models = {
            'network_health_classifier': {
                'type': 'classification',
                'target': 'network_health_score',
                'features': [
                    'signal_strength_dbm', 'throughput_mbps', 'latency_ms', 
                    'call_drop_rate_percent', 'packet_loss_percent', 'jitter_ms'
                ]
            }
        }

    def load_gold_tier_data(self) -> pd.DataFrame:
        """Load gold tier data from MongoDB"""
        print("📊 Loading gold tier data from MongoDB...")
        
        try:
            # Get all data from gold_tier_features collection
            pipeline = [
                {
                    "$project": {
                        "_id": 0,
                        "timestamp": 1,
                        "imsi": 1,
                        "customer_id": 1,
                        "region": 1,
                        "service_plan": 1,
                        "device_type": 1,
                        "cell_technology": 1,
                        "signal_strength_dbm": 1,
                        "throughput_mbps": 1,
                        "latency_ms": 1,
                        "call_drop_rate_percent": 1,
                        "packet_loss_percent": 1,
                        "jitter_ms": 1,
                        "video_buffering_ratio": 1,
                        "voip_clarity_score": 1,
                        "app_response_time_ms": 1,
                        "qos_level": 1,
                        "network_health_score": 1,
                        "customer_experience_score": 1,
                        "network_efficiency_score": 1,
                        "customer_satisfaction_prediction": 1,
                        "revenue_impact_score": 1
                    }
                }
            ]
            
            cursor = self.db.gold_tier_features.aggregate(pipeline)
            data = list(cursor)
            
            if not data:
                raise ValueError("No gold tier data found in gold_tier_features collection")
            
            df = pd.DataFrame(data)
            print(f"✅ Loaded {len(df):,} records from gold_tier_features collection")
            
            # Convert timestamp to datetime if it's a string
            if 'timestamp' in df.columns and df['timestamp'].dtype == 'object':
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return df
            
        except Exception as e:
            print(f"❌ Error loading gold tier data: {e}")
            raise

    def preprocess_data(self, df: pd.DataFrame) -> Dict[str, Tuple[pd.DataFrame, pd.Series]]:
        """Preprocess data for each model"""
        print("🔧 Preprocessing data for model training...")
        
        datasets = {}
        
        for model_name, config in self.models.items():
            print(f"  📋 Processing {model_name}...")
            
            # Select features and target
            features = config['features']
            target = config['target']
            
            # Filter out rows with missing values
            model_df = df[features + [target]].dropna()
            
            if len(model_df) == 0:
                print(f"  ⚠️ No data available for {model_name} after removing missing values")
                continue
            
            # Prepare features and target
            X = model_df[features].copy()
            y = model_df[target].copy()
            
            # Handle categorical features
            categorical_features = X.select_dtypes(include=['object']).columns
            for col in categorical_features:
                if col in X.columns:
                    le = LabelEncoder()
                    X[col] = le.fit_transform(X[col].astype(str))
            
            # Handle categorical target for classification
            label_encoder = None
            if config['type'] == 'classification':
                label_encoder = LabelEncoder()
                y_original = y.copy()  # Keep original labels
                y = label_encoder.fit_transform(y.astype(str))
            
            # Convert to numeric
            X = X.astype(float)
            y = y.astype(float)
            
            # Store label encoder for later use
            datasets[model_name] = (X, y, label_encoder)
            print(f"  ✅ {model_name}: {len(X):,} samples, {len(features)} features")
        
        return datasets

    def train_network_health_classifier(self, X: pd.DataFrame, y: pd.Series, label_encoder=None) -> Dict[str, Any]:
        """Train network health classification model with class balancing"""
        print("🏥 Training Network Health Classifier...")
        
        # Check data distribution
        print(f"📈 Data distribution:")
        unique, counts = np.unique(y, return_counts=True)
        for val, count in zip(unique, counts):
            class_name = label_encoder.inverse_transform([int(val)])[0] if label_encoder else val
            print(f"  {class_name}: {count} samples")
        
        # Calculate class weights for balancing
        class_weights = compute_class_weight(
            'balanced',
            classes=np.unique(y),
            y=y
        )
        class_weight_dict = dict(zip(np.unique(y), class_weights))
        print(f"⚖️  Class weights: {class_weight_dict}")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Define models to try with class balancing
        models = {
            'RandomForest': RandomForestClassifier(
                n_estimators=100, 
                random_state=42,
                class_weight=class_weight_dict,
                max_depth=10,
                min_samples_split=10,
                min_samples_leaf=5
            ),
            'LogisticRegression': LogisticRegression(
                random_state=42, 
                max_iter=1000,
                class_weight=class_weight_dict
            )
        }
        
        best_model = None
        best_score = 0
        best_model_name = None
        
        for name, model in models.items():
            # Cross-validation
            cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5)
            mean_cv_score = cv_scores.mean()
            
            if mean_cv_score > best_score:
                best_score = mean_cv_score
                best_model = model
                best_model_name = name
        
        # Train best model
        best_model.fit(X_train_scaled, y_train)
        y_pred = best_model.predict(X_test_scaled)
        
        # Create a pipeline that includes the scaler
        from sklearn.pipeline import Pipeline
        model_pipeline = Pipeline([
            ('scaler', scaler),
            ('classifier', best_model)
        ])
        
        # Calculate metrics
        metrics = {
            'accuracy': accuracy_score(y_test, y_pred),
            'precision': precision_score(y_test, y_pred, average='weighted'),
            'recall': recall_score(y_test, y_pred, average='weighted'),
            'f1': f1_score(y_test, y_pred, average='weighted'),
            'cv_score': best_score
        }
        
        # Test with sample cases to verify balanced predictions
        print(f"\n🧪 Testing balanced model with sample cases...")
        test_cases = [
            {
                "name": "Excellent Network",
                "data": [-45, 150, 15, 0.1, 0.2, 0.5],
                "expected": 0  # excellent
            },
            {
                "name": "Good Network", 
                "data": [-65, 75, 35, 0.8, 0.8, 2.5],
                "expected": 1  # good
            },
            {
                "name": "Poor Network",
                "data": [-85, 20, 120, 3.0, 2.5, 8.0],
                "expected": 2  # poor
            }
        ]
        
        for test_case in test_cases:
            test_data = [test_case['data']]  # Use raw data, pipeline will handle scaling
            prediction = model_pipeline.predict(test_data)[0]
            probabilities = model_pipeline.predict_proba(test_data)[0]
            
            print(f"  {test_case['name']}:")
            print(f"    Input: {test_case['data']}")
            print(f"    Prediction: {prediction} ({label_encoder.inverse_transform([int(prediction)])[0] if label_encoder else prediction})")
            print(f"    Expected: {test_case['expected']} ({label_encoder.inverse_transform([int(test_case['expected'])])[0] if label_encoder else test_case['expected']})")
            print(f"    Probabilities: {dict(zip(label_encoder.classes_, probabilities)) if label_encoder else probabilities}")
            
            if prediction == test_case['expected']:
                print("    ✅ CORRECT")
            else:
                print("    ❌ INCORRECT")
        
        return {
            'model': model_pipeline,
            'scaler': scaler,
            'label_encoder': label_encoder,
            'model_name': best_model_name,
            'metrics': metrics,
            'feature_names': X.columns.tolist()
        }



    def log_experiment_to_mlflow(self, model_results: Dict[str, Dict[str, Any]], 
                                data_info: Dict[str, Any], datasets: Dict[str, Tuple[pd.DataFrame, pd.Series]]) -> None:
        """Log experiment results to MLflow"""
        print("📊 Logging experiment to MLflow...")
        
        # Set experiment
        mlflow.set_experiment(self.experiment_name)
        
        for model_name, results in model_results.items():
            if results is None:
                continue
                
            print(f"  📝 Logging {model_name}...")
            
            with mlflow.start_run(run_name=f"{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
                # Set tags for better organization
                mlflow.set_tag("project", "telco_ods")
                mlflow.set_tag("model_type", results['model_name'])
                mlflow.set_tag("target", self.models[model_name]['target'])
                mlflow.set_tag("data_source", "gold_tier_features")
                # Log parameters
                mlflow.log_param("model_type", results['model_name'])
                mlflow.log_param("n_features", len(results['feature_names']))
                mlflow.log_param("feature_names", json.dumps(results['feature_names']))
                mlflow.log_param("data_samples", data_info['total_samples'])
                mlflow.log_param("training_date", datetime.now().isoformat())
                mlflow.log_param("model_name", model_name)
                mlflow.log_param("experiment_name", self.experiment_name)
                mlflow.log_param("class_balancing", "applied")
                mlflow.log_param("data_correction", "applied_class_balancing")
                
                # Log metrics
                for metric_name, metric_value in results['metrics'].items():
                    mlflow.log_metric(metric_name, metric_value)
                
                # Create input example for model signature
                if model_name in datasets:
                    X_data, _, _ = datasets[model_name]
                    input_example = X_data.head(1)
                else:
                    # Fallback: create dummy input example
                    feature_names = results['feature_names']
                    dummy_data = [[0.0] * len(feature_names)]
                    input_example = pd.DataFrame(dummy_data, columns=feature_names)
                
                # Create output example with categorical values
                if results.get('label_encoder') is not None:
                    # Get a sample prediction to create output example
                    sample_prediction = results['model'].predict(input_example)
                    # Convert to categorical using label encoder
                    categorical_prediction = results['label_encoder'].inverse_transform(sample_prediction.astype(int))
                    output_example = pd.DataFrame(categorical_prediction, columns=['network_health_score'])
                else:
                    output_example = None
                
                # Log model with signature
                mlflow.sklearn.log_model(
                    results['model'], 
                    artifact_path=f"{model_name}_model",
                    registered_model_name=f"telco_ods_{model_name}",
                    input_example=input_example,
                    signature=mlflow.models.infer_signature(input_example, output_example) if output_example is not None else None
                )
                
                # Log model info
                model_info = {
                    'model_name': results['model_name'],
                    'feature_names': results['feature_names'],
                    'metrics': results['metrics'],
                    'training_date': datetime.now().isoformat(),
                    'model_config': self.models[model_name],
                    'label_encoder_classes': results.get('label_encoder').classes_.tolist() if results.get('label_encoder') else None
                }
                mlflow.log_dict(model_info, f"{model_name}_info.json")
                
                # Log feature importance for tree-based models
                if hasattr(results['model'], 'feature_importances_'):
                    feature_importance = dict(zip(results['feature_names'], results['model'].feature_importances_))
                    mlflow.log_dict(feature_importance, f"{model_name}_feature_importance.json")
                
                # Log label encoder classes for proper output interpretation
                if results.get('label_encoder') is not None:
                    class_mapping = {
                        'encoded_values': results['label_encoder'].classes_.tolist(),
                        'description': 'Label encoder mapping: 0=excellent, 1=good, 2=poor (or similar mapping)'
                    }
                    mlflow.log_dict(class_mapping, f"{model_name}_label_encoder_classes.json")
                
                # Log training configuration
                training_config = {
                    'test_size': 0.2,
                    'random_state': 42,
                    'cv_folds': 5,
                    'n_estimators': 100
                }
                mlflow.log_dict(training_config, f"{model_name}_training_config.json")
                
                print(f"  ✅ {model_name} logged successfully")

    def save_model_artifacts(self, model_results: Dict[str, Dict[str, Any]]) -> None:
        """Save model artifacts to MongoDB for later use"""
        print("💾 Saving model artifacts to MongoDB...")
        
        for model_name, results in model_results.items():
            if results is None:
                continue
            
            # Save model info to MongoDB
            model_doc = {
                'model_name': model_name,
                'model_type': results['model_name'],
                'feature_names': results['feature_names'],
                'metrics': results['metrics'],
                'training_date': datetime.now(timezone.utc),
                'status': 'trained'
            }
            
            # Upsert to avoid duplicates
            self.db.mlflow_results.update_one(
                {'model_name': model_name},
                {'$set': model_doc},
                upsert=True
            )
            
            print(f"  ✅ {model_name} artifacts saved to MongoDB")

    def train_all_models(self):
        """Train all models and log to MLflow"""
        print("🚀 Starting Telco ODS Model Training Pipeline")
        print("=" * 80)
        
        start_time = time.time()
        
        try:
            # Load data
            df = self.load_gold_tier_data()
            
            # Preprocess data
            datasets = self.preprocess_data(df)
            
            if not datasets:
                print("❌ No valid datasets found for training")
                return
            
            # Train Network Health Classifier
            model_results = {}
            
            if 'network_health_classifier' in datasets:
                X, y, label_encoder = datasets['network_health_classifier']
                model_results['network_health_classifier'] = self.train_network_health_classifier(X, y, label_encoder)
            
            # Log to MLflow
            data_info = {'total_samples': len(df)}
            self.log_experiment_to_mlflow(model_results, data_info, datasets)
            
            # Save artifacts
            self.save_model_artifacts(model_results)
            
            end_time = time.time()
            
            # Print results
            print("\n" + "=" * 80)
            print("🎉 Model Training Pipeline Completed!")
            print("=" * 80)
            print(f"📊 Training Results:")
            
            for model_name, results in model_results.items():
                if results:
                    print(f"\n🏆 {model_name}:")
                    print(f"   Model: {results['model_name']}")
                    print(f"   Features: {len(results['feature_names'])}")
                    for metric_name, metric_value in results['metrics'].items():
                        print(f"   {metric_name}: {metric_value:.4f}")
            
            print(f"\n⏱️  Total Training Time: {end_time - start_time:.2f} seconds")
            print(f"📈 Models logged to MLflow: {self.mlflow_tracking_uri}")
            print(f"💾 Artifacts saved to MongoDB: {self.db_name}.mlflow_results")
            
        except Exception as e:
            print(f"❌ Error during model training: {e}")
            raise

def main():
    """Main function to run the model training"""
    try:
        trainer = TelcoODSModelTrainer()
        trainer.train_all_models()
    except Exception as e:
        print(f"❌ Error during model training: {e}")
        raise

if __name__ == "__main__":
    main()
