variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "logs_bucket" {
  description = "S3 bucket name for access logs"
  type        = string
  default     = ""
}
