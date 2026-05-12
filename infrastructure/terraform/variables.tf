variable "aws_region" {
  description = "AWS region for all resources"
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "Project name used for resource naming"
  default     = "telco-ods-demo"
}

variable "key_pair_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
}

variable "mongodb_uri" {
  description = "MongoDB Atlas connection string"
  type        = string
  sensitive   = true
  default     = "mongodb+srv://admin:IpAQeOM854tLQ5rP@democluster.hyszr.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster"
}

variable "mongodb_atlas_public_key" {
  description = "MongoDB Atlas API public key (for IP whitelist)"
  type        = string
  default     = ""
}

variable "mongodb_atlas_private_key" {
  description = "MongoDB Atlas API private key (for IP whitelist)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "mongodb_atlas_project_id" {
  description = "MongoDB Atlas project ID"
  type        = string
  default     = ""
}

variable "allowed_ssh_cidr" {
  description = "CIDR block allowed to SSH into EC2 instances"
  type        = string
  default     = "0.0.0.0/0"
}
