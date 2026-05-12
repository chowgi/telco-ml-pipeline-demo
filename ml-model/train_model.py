#!/usr/bin/env python3
"""
Trains a network health classifier on windowed metrics and registers with MLflow.
Adapted from existing train_ml_model.py for the streaming pipeline architecture.
"""

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.utils.class_weight import compute_class_weight
import mlflow
import mlflow.sklearn

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5002")
DB_NAME = "ods_demo_db"
TRAINING_COLLECTION = "training_windowed_metrics"
EXPERIMENT_NAME = "telco_ods_network_health"

FEATURES = [
    "signal_strength_dbm",
    "throughput_mbps",
    "latency_ms",
    "call_drop_rate_percent",
    "packet_loss_percent",
    "jitter_ms",
]
TARGET = "network_health_score"


def load_training_data() -> pd.DataFrame:
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    cursor = db[TRAINING_COLLECTION].find({}, {"_id": 0})
    df = pd.DataFrame(list(cursor))
    client.close()
    print(f"Loaded {len(df)} training samples")
    return df


def train():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    df = load_training_data()
    X = df[FEATURES].astype(float)
    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform(df[TARGET])

    print(f"Classes: {label_encoder.classes_}")
    print(f"Distribution: {np.bincount(y)}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    class_weights = compute_class_weight("balanced", classes=np.unique(y), y=y)
    class_weight_dict = dict(zip(np.unique(y), class_weights))

    model_pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("classifier", RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=10,
            min_samples_leaf=5,
            class_weight=class_weight_dict,
            random_state=42,
        )),
    ])

    cv_scores = cross_val_score(model_pipeline, X_train, y_train, cv=5)
    model_pipeline.fit(X_train, y_train)
    y_pred = model_pipeline.predict(X_test)

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, average="weighted"),
        "recall": recall_score(y_test, y_pred, average="weighted"),
        "f1": f1_score(y_test, y_pred, average="weighted"),
        "cv_score_mean": cv_scores.mean(),
    }

    print("\nMetrics:")
    for k, v in metrics.items():
        print(f"  {k}: {v:.4f}")

    # Verify predictions
    test_cases = [
        {"name": "Excellent", "data": [[-45, 150, 15, 0.1, 0.2, 0.5]], "expected": 0},
        {"name": "Good", "data": [[-65, 75, 35, 0.8, 0.8, 2.5]], "expected": 1},
        {"name": "Poor", "data": [[-85, 20, 120, 3.0, 2.5, 8.0]], "expected": 2},
    ]
    print("\nVerification:")
    for tc in test_cases:
        pred = model_pipeline.predict(tc["data"])[0]
        label = label_encoder.inverse_transform([pred])[0]
        status = "PASS" if pred == tc["expected"] else "FAIL"
        print(f"  {tc['name']}: predicted={label} [{status}]")

    with mlflow.start_run(run_name=f"network_health_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.set_tag("project", "telco_ods")
        mlflow.set_tag("model_type", "RandomForestClassifier")
        mlflow.log_param("features", json.dumps(FEATURES))
        mlflow.log_param("n_samples", len(df))
        mlflow.log_param("class_balancing", "applied")

        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        input_example = X_test.head(1)
        mlflow.sklearn.log_model(
            model_pipeline,
            artifact_path="network_health_classifier_model",
            registered_model_name="telco_ods_network_health_classifier",
            input_example=input_example,
        )

        mlflow.log_dict(
            {"classes": label_encoder.classes_.tolist(), "mapping": "0=excellent, 1=good, 2=poor"},
            "label_encoder_info.json",
        )

    print(f"\nModel registered in MLflow: {MLFLOW_TRACKING_URI}")
    print("Serve with:")
    print(f"  mlflow models serve -m models:/telco_ods_network_health_classifier/latest -p 5003 --host 0.0.0.0 --no-conda")


if __name__ == "__main__":
    train()
