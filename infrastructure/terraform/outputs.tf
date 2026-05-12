output "kafka_private_ip" {
  value       = aws_instance.kafka.private_ip
  description = "Kafka broker private IP (used by other EC2 instances)"
}

output "kafka_public_ip" {
  value       = aws_instance.kafka.public_ip
  description = "Kafka broker public IP"
}

output "generator_public_ip" {
  value       = aws_instance.generator.public_ip
  description = "Data generator EC2 public IP"
}

output "flink_public_ip" {
  value       = aws_instance.flink.public_ip
  description = "Flink EC2 public IP"
}

output "flink_web_ui" {
  value       = "http://${aws_instance.flink.public_ip}:8081"
  description = "Flink Web UI URL"
}

output "mlflow_public_ip" {
  value       = aws_instance.mlflow.public_ip
  description = "MLflow EC2 public IP"
}

output "mlflow_tracking_url" {
  value       = "http://${aws_instance.mlflow.public_ip}:5002"
  description = "MLflow tracking server URL"
}

output "mlflow_serving_url" {
  value       = "http://${aws_instance.mlflow.public_ip}:5003/invocations"
  description = "MLflow model serving endpoint"
}

output "atlas_whitelist_ips" {
  value       = [aws_instance.kafka.public_ip, aws_instance.generator.public_ip, aws_instance.flink.public_ip, aws_instance.mlflow.public_ip]
  description = "IPs to add to MongoDB Atlas IP Access List"
}
