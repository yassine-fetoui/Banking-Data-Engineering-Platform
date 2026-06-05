variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for MSK security group"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block for security group ingress rules"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for MSK broker nodes"
  type        = list(string)
}

variable "kms_key_arn" {
  description = "KMS key ARN for MSK encryption at rest"
  type        = string
}

variable "logs_bucket" {
  description = "S3 bucket name for MSK broker logs"
  type        = string
}
