#!/bin/bash

echo "🛑 Stopping MLflow Services"
echo "==========================="

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null ; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Stop MLflow tracking server
if check_port 5002; then
    echo "Stopping MLflow tracking server on port 5002..."
    pkill -f "mlflow server.*5002" || echo "No MLflow tracking server found"
    sleep 3
    if check_port 5002; then
        echo "Force killing MLflow tracking server..."
        pkill -9 -f "mlflow server.*5002"
    fi
else
    echo "MLflow tracking server not running on port 5002"
fi

# Stop model server
if check_port 5003; then
    echo "Stopping model server on port 5003..."
    pkill -f "mlflow models serve.*5003" || echo "No model server found"
    sleep 3
    if check_port 5003; then
        echo "Force killing model server..."
        pkill -9 -f "mlflow models serve.*5003"
    fi
else
    echo "Model server not running on port 5003"
fi

# Clean up PID files
rm -f /home/ubuntu/mlflow_server.pid
rm -f /home/ubuntu/model_server.pid

echo "✅ All MLflow services stopped"
