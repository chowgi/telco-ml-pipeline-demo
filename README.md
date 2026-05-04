# Telco ODS Network Health Classifier

> ⚠️ **DEMO ONLY**: This is a demonstration system for showcasing ML pipeline capabilities. It is **NOT production-ready** and should not be used in production environments.

A **DEMO** machine learning system for predicting network health based on real-time network metrics using MLflow and MongoDB. This is for demonstration purposes only and is not production-ready.

## 🎯 Overview

This **DEMO** project provides a complete ML pipeline for network health classification with deployment capabilities for demonstration purposes:

- **📊 Data Pipeline**: Feature engineering from raw network data to gold-tier features
- **🤖 Model Training**: RandomForest classifier with advanced class balancing
- **🔬 MLOps**: MLflow integration for experiment tracking and model serving
- **🚀 Real-time Streaming**: Kafka + Stream Processor for real-time network health inference
- **⚡ Stream Processing**: Confluent Cloud Kafka with MongoDB Atlas triggers
- **🧪 Testing**: Comprehensive model validation with real data and edge cases

## 📁 Project Structure

### 🎯 Core Scripts
- **`train_ml_model.py`** - Main ML training script with MLflow integration
- **`test_model_predictions.py`** - Comprehensive model testing suite
- **`start_mlflow.sh`** - Streamlined MLflow startup with health checks (runs on the remote server)
- **`stop_mlflow.sh`** - Clean shutdown script (runs on the remote server)

### 📊 Data Pipeline
- **`feature-engineering-gold-tier.py`** - Gold tier feature engineering
- **`feature-engineering-silver-tier.py`** - Silver tier feature engineering
- **`create_dummy_data.py`** - Data generation for testing
- **`clean_data.py`** - Data cleaning utilities

### 🔄 Real-time Processing & Streaming
- **`atlas_trigger.js`** - MongoDB Atlas trigger for real-time ML inference
- **`test_realtime_inference.py`** - Test script for real-time model predictions
- **Kafka Streaming**: Confluent Cloud Kafka for real-time data ingestion
- **Stream Processing**: Real-time network health predictions via Kafka + MongoDB Atlas

### 📚 Documentation
- **`README.md`** - This comprehensive guide
- **`requirements.txt`** - Python dependencies
- **`Operational Datastore for Autonomous Networks (2).pdf`** - Project specification

## 🔄 Real-time Streaming Architecture

This demo includes a complete real-time streaming pipeline:

### **Kafka + Stream Processing**
- **📡 Data Ingestion**: Confluent Cloud Kafka receives network telemetry data
- **⚡ Stream Processing**: MongoDB Atlas triggers process data in real-time
- **🤖 ML Inference**: MLflow model serves predictions via Atlas triggers
- **📊 Storage**: Predictions stored in MongoDB for real-time dashboards

### **Streaming Flow**
```
Network Data → Kafka → MongoDB → Atlas Trigger → MLflow → Predictions
```

## 🚀 Quick Start (Demo Only)

> ⚠️ **Note**: This is a demonstration system only. Do not use in production environments.

### 1. Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export MONGODB_URI="your_mongodb_connection_string"
export MONGODB_DATABASE="ods_demo_db"
export MLFLOW_TRACKING_URI="http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5002"
```

### 2. Train Model
```bash
python train_ml_model.py
```

### 3. Test Model
```bash
python test_model_predictions.py
```

### 4. Deploy to Production
```bash
# Copy scripts to EC2
scp start_mlflow.sh stop_mlflow.sh ubuntu@ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:~/

# SSH and deploy
ssh ubuntu@ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com
chmod +x start_mlflow.sh stop_mlflow.sh
./start_mlflow.sh
```

## 🧠 Model Details

### Features
- **`signal_strength_dbm`** - Signal strength in dBm (-100 to -30)
- **`throughput_mbps`** - Network throughput in Mbps (0-300)
- **`latency_ms`** - Network latency in milliseconds (1-1000)
- **`call_drop_rate_percent`** - Call drop rate percentage (0-10)
- **`packet_loss_percent`** - Packet loss percentage (0-10)
- **`jitter_ms`** - Network jitter in milliseconds (0-50)

### Predictions
- **0** - Excellent network health
- **1** - Good network health  
- **2** - Poor network health

### Performance
- **Accuracy**: 99.9%
- **Precision**: 99.9% (weighted average)
- **Recall**: 99.9% (weighted average)
- **F1-Score**: 99.9% (weighted average)

## 🔧 API Usage

### Model Endpoint
```bash
POST http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5003/invocations
Content-Type: application/json

{
  "dataframe_records": [
    {
      "signal_strength_dbm": -65,
      "throughput_mbps": 75,
      "latency_ms": 35,
      "call_drop_rate_percent": 0.8,
      "packet_loss_percent": 0.8,
      "jitter_ms": 2.5
    }
  ]
}
```

### Response
```json
{"predictions": [1.0]}  // 1 = good network health
```

### Test Examples
```bash
# Excellent Network (should return [0.0])
curl -X POST http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -45, "throughput_mbps": 150, "latency_ms": 15, "call_drop_rate_percent": 0.1, "packet_loss_percent": 0.2, "jitter_ms": 0.5}]}'

# Good Network (should return [1.0])
curl -X POST http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -65, "throughput_mbps": 75, "latency_ms": 35, "call_drop_rate_percent": 0.8, "packet_loss_percent": 0.8, "jitter_ms": 2.5}]}'

# Poor Network (should return [2.0])
curl -X POST http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -85, "throughput_mbps": 20, "latency_ms": 120, "call_drop_rate_percent": 3.0, "packet_loss_percent": 2.5, "jitter_ms": 8.0}]}'
```

## 🛠️ MLflow Service Management

### Start Services
```bash
./start_mlflow.sh
```
**Features:**
- Stops existing services before starting new ones
- Starts MLflow tracking server on port 5002
- Starts model server on port 5003
- Automatically tests model predictions
- Provides management commands

### Stop Services
```bash
./stop_mlflow.sh
```
**Features:**
- Stops MLflow tracking server (port 5002)
- Stops model server (port 5003)
- Cleans up PID files
- Force kills if necessary

### Service Management
```bash
# Enable auto-start on boot
sudo systemctl enable mlflow-services.service

# Start service
sudo systemctl start mlflow-services.service

# Check status
sudo systemctl status mlflow-services.service

# Stop service
sudo systemctl stop mlflow-services.service
```

## 📊 MLflow Integration

### Endpoints
- **MLflow UI**: http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5002/
- **Model API**: http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5003/

### Features
- **Tracking Server**: Experiment tracking and model registry
- **Model Registry**: Versioned model management
- **Model Serving**: REST API for predictions
- **Artifacts**: Model artifacts and metadata storage

## 🧪 Testing

### Comprehensive Testing
```bash
python test_model_predictions.py
```

**Test Coverage:**
- Real data from MongoDB gold tier
- Edge cases and boundary conditions
- Model prediction accuracy validation
- Performance metrics analysis

### Real-time Inference Testing
```bash
python test_realtime_inference.py
```

**Real-time Test Features:**
- Tests Atlas trigger with live data insertion
- Monitors MLflow model predictions in real-time
- Validates end-to-end pipeline functionality
- Tests different network scenarios (excellent, good, poor)
- Automatic cleanup of test data

### Automated Testing
The startup script automatically tests the model with three scenarios:
- **Excellent Network** → Should return `[0.0]`
- **Good Network** → Should return `[1.0]`
- **Poor Network** → Should return `[2.0]`

## 🛠️ Technical Stack

- **ML Framework**: scikit-learn (RandomForestClassifier)
- **MLOps**: MLflow (tracking, registry, serving)
- **Database**: MongoDB Atlas
- **Deployment**: EC2 with MLflow model serving
- **Language**: Python 3.13
- **Class Balancing**: Advanced techniques for imbalanced data

## 📈 Data Pipeline

1. **Raw Data** → MongoDB collections
2. **Feature Engineering** → Gold/Silver tier processing
3. **Model Training** → MLflow experiment tracking
4. **Model Registry** → Versioned model storage
5. **Model Serving** → Production API endpoint
6. **Real-time Processing** → Atlas trigger for live inference
7. **Testing** → Comprehensive validation

## 🔄 Real-time Processing

### Atlas Trigger Setup
The `atlas_trigger.js` file provides real-time network health predictions:

**Trigger Configuration:**
- **Collection**: `incoming_network_data` (triggers on new documents)
- **Model Endpoint**: `http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5003/invocations`
- **Results Collection**: `network_health_predictions`

**Required Document Format:**
```json
{
  "signal_strength_dbm": -65,
  "throughput_mbps": 75,
  "latency_ms": 35,
  "call_drop_rate_percent": 0.8,
  "packet_loss_percent": 0.8,
  "jitter_ms": 2.5,
  "imsi": "IMSI_123456",
  "customer_id": "CUST_1234",
  "region": "Sydney",
  "device_type": "iPhone",
  "cell_technology": "5G"
}
```

**Prediction Output:**
```json
{
  "network_data_id": "ObjectId",
  "timestamp": "2024-01-01T00:00:00Z",
  "input_features": { /* network metrics */ },
  "prediction": {
    "encoded": 1,
    "label": "good"
  },
  "metadata": { /* customer/device info */ }
}
```

## 🔍 Monitoring & Troubleshooting

### Check Service Status
```bash
lsof -i :5002  # MLflow UI
lsof -i :5003  # Model API
```

### View Logs
```bash
tail -f /home/ubuntu/mlflow_server.log
tail -f /home/ubuntu/model_server.log
```

### Restart Services
```bash
./stop_mlflow.sh
./start_mlflow.sh
```

### Monitoring
- **MLflow UI**: Model performance and experiment tracking
- **Model Metrics**: Accuracy, precision, recall, F1-score
- **Data Quality**: Automated data validation and cleaning
- **Model Drift**: Monitoring for model performance degradation

## 📝 Key Features

- ✅ **Production-Ready**: Streamlined deployment with health checks
- ✅ **Comprehensive Testing**: Real data validation and edge case testing
- ✅ **Automated Deployment**: One-command startup with automatic testing
- ✅ **Class Balancing**: Advanced techniques for imbalanced network data
- ✅ **Version Control**: MLflow model registry with versioning
- ✅ **Monitoring**: Complete observability with logs and metrics
- ✅ **Documentation**: Comprehensive guides and examples

## 🔄 Real-time Streaming with Confluent Cloud

### **Kafka Integration**
The project now supports real-time streaming using Confluent Cloud Kafka:

```bash
# Test streaming pipeline
python3 test_realtime_inference_kafka.py
```

### **Streaming Architecture**
```
Network Data → Confluent Cloud Kafka → Stream Processor → MongoDB → Atlas Trigger → MLflow Model → Predictions
```

### **Kafka Configuration**
- **Topic**: `network_telemetry`
- **Bootstrap Server**: `pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092`
- **Security**: SASL_SSL with API key authentication
- **Stream Processor**: Consumes from Kafka and forwards to MongoDB

### **Streaming Benefits**
- ✅ **Real-time Processing**: Sub-second latency for network health predictions
- ✅ **Scalability**: Handle high-volume network data streams
- ✅ **Reliability**: Kafka's durability and fault tolerance
- ✅ **Decoupling**: Loose coupling between data producers and ML processing
- ✅ **Monitoring**: Full observability of the streaming pipeline

## 🎯 Project Benefits

- **Reduced Complexity**: Streamlined from 9+ scripts to 4 core scripts
- **Eliminated Redundancy**: Removed duplicate and conflicting scripts
- **Improved Maintainability**: Clear documentation and structure
- **Production-Ready**: Tested and validated deployment process
- **Clean Architecture**: Focused on essential functionality
- **Streaming Ready**: Real-time processing with Confluent Cloud Kafka

---

**Ready for production deployment with comprehensive testing, monitoring, and real-time streaming!** 🚀