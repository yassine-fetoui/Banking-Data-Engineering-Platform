variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for Redshift security group"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block for security group ingress rules"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for Redshift workgroup"
  type        = list(string)
}

variable "admin_password" {
  description = "Redshift admin user password"
  type        = string
  sensitive   = true
}

variable "data_lake_kms_arn" {
  description = "KMS key ARN for S3 data lake decryption"
  type        = string
}
