#!/bin/bash

echo "🚀 Starting MLflow Services"
echo "============================"

# Set working directory
cd /home/ubuntu

# Set MLflow tracking URI
export MLFLOW_TRACKING_URI="http://localhost:5002"

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null ; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Function to stop existing services
stop_existing_services() {
    echo "🛑 Stopping existing MLflow services..."
    
    # Stop MLflow tracking server
    if check_port 5002; then
        echo "   Stopping MLflow tracking server on port 5002..."
        pkill -f "mlflow server.*5002" || echo "   No MLflow tracking server found"
    fi
    
    # Stop model server
    if check_port 5003; then
        echo "   Stopping model server on port 5003..."
        pkill -f "mlflow models serve.*5003" || echo "   No model server found"
    fi
    
    # Wait for processes to stop
    sleep 5
}

# Function to start MLflow tracking server
start_tracking_server() {
    echo "📊 Starting MLflow tracking server..."
    nohup /home/ubuntu/mlflow/bin/mlflow server --host 0.0.0.0 --port 5002 > mlflow_server.log 2>&1 &
    MLFLOW_PID=$!
    echo "   MLflow server PID: $MLFLOW_PID"
    
    # Wait for server to start
    echo "   Waiting for server to start..."
    sleep 20
    
    # Check if server is running
    if curl -s http://localhost:5002/health > /dev/null; then
        echo "   ✅ MLflow tracking server is running"
        echo "$MLFLOW_PID" > /home/ubuntu/mlflow_server.pid
        return 0
    else
        echo "   ❌ MLflow tracking server failed to start"
        echo "   Check log: cat /home/ubuntu/mlflow_server.log"
        return 1
    fi
}

# Function to start model server
start_model_server() {
    echo "🤖 Starting model server..."
    nohup /home/ubuntu/mlflow/bin/mlflow models serve -m models:/telco_ods_network_health_classifier/latest -p 5003 --host 0.0.0.0 > model_server.log 2>&1 &
    MODEL_PID=$!
    echo "   Model server PID: $MODEL_PID"
    
    # Wait for server to start
    echo "   Waiting for model server to start..."
    sleep 30
    
    # Check if server is running
    if curl -s http://localhost:5003/health > /dev/null; then
        echo "   ✅ Model server is running"
        echo "$MODEL_PID" > /home/ubuntu/model_server.pid
        return 0
    else
        echo "   ❌ Model server failed to start"
        echo "   Check log: cat /home/ubuntu/model_server.log"
        return 1
    fi
}

# Function to test the model
test_model() {
    echo "🧪 Testing model predictions..."
    
    # Test excellent network (should return [0])
    echo "   Testing excellent network..."
    EXCELLENT_RESPONSE=$(curl -s -X POST http://localhost:5003/invocations \
        -H "Content-Type: application/json" \
        -d '{
            "dataframe_records": [
                {
                    "signal_strength_dbm": -45,
                    "throughput_mbps": 150,
                    "latency_ms": 15,
                    "call_drop_rate_percent": 0.1,
                    "packet_loss_percent": 0.2,
                    "jitter_ms": 0.5
                }
            ]
        }')
    
    # Test good network (should return [1])
    echo "   Testing good network..."
    GOOD_RESPONSE=$(curl -s -X POST http://localhost:5003/invocations \
        -H "Content-Type: application/json" \
        -d '{
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
        }')
    
    # Test poor network (should return [2])
    echo "   Testing poor network..."
    POOR_RESPONSE=$(curl -s -X POST http://localhost:5003/invocations \
        -H "Content-Type: application/json" \
        -d '{
            "dataframe_records": [
                {
                    "signal_strength_dbm": -85,
                    "throughput_mbps": 20,
                    "latency_ms": 120,
                    "call_drop_rate_percent": 3.0,
                    "packet_loss_percent": 2.5,
                    "jitter_ms": 8.0
                }
            ]
        }')
    
    echo "   Test Results:"
    echo "     Excellent: $EXCELLENT_RESPONSE (expected: [0.0])"
    echo "     Good: $GOOD_RESPONSE (expected: [1.0])"
    echo "     Poor: $POOR_RESPONSE (expected: [2.0])"
    
    # Check if all tests passed
    if [[ "$EXCELLENT_RESPONSE" == *"0.0"* ]] && [[ "$GOOD_RESPONSE" == *"1.0"* ]] && [[ "$POOR_RESPONSE" == *"2.0"* ]]; then
        echo "   ✅ All model tests passed!"
        return 0
    else
        echo "   ⚠️  Some model tests failed - check model configuration"
        return 1
    fi
}

# Main execution
main() {
    # Stop existing services
    stop_existing_services
    
    # Start MLflow tracking server
    if ! start_tracking_server; then
        echo "❌ Failed to start MLflow tracking server"
        exit 1
    fi
    
    # Start model server
    if ! start_model_server; then
        echo "❌ Failed to start model server"
        exit 1
    fi
    
    # Test the model
    test_model
    
    echo ""
    echo "🎉 All services started successfully!"
    echo "====================================="
    echo "🌐 MLflow UI: http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5002/"
    echo "🌐 Model API: http://ec2-13-236-153-18.ap-southeast-2.compute.amazonaws.com:5003/"
    echo ""
    echo "📋 Management commands:"
    echo "   Stop MLflow: kill \$(cat /home/ubuntu/mlflow_server.pid)"
    echo "   Stop Model: kill \$(cat /home/ubuntu/model_server.pid)"
    echo "   Check MLflow logs: tail -f /home/ubuntu/mlflow_server.log"
    echo "   Check Model logs: tail -f /home/ubuntu/model_server.log"
    echo "   Restart services: ./start_mlflow.sh"
}

# Run main function
main "$@"
