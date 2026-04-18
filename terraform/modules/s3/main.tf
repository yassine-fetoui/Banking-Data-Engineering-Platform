# =============================================================================
# terraform/modules/s3/main.tf
# Banking Data Lake Module - Medallion Architecture (Landing + Bronze/Silver/Gold)
# High-security, compliant, cost-optimized S3 setup for regulated financial data
# =============================================================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ── Local values for consistency ─────────────────────────────────────────────
locals {
  common_tags = {
    Environment     = var.environment
    Project         = "banking-data-platform"
    ManagedBy       = "Terraform"
    Compliance      = "AML,SOX,BASEL-III,PCI-DSS"
    DataClassification = "Sensitive"
    Owner           = "data-platform-team"
  }

  bucket_suffix = var.environment == "prod" ? "" : "-${var.environment}"
}

# ── Landing Bucket (Raw ingestion zone - short retention) ─────────────────────
resource "aws_s3_bucket" "landing" {
  bucket        = "banking-landing${local.bucket_suffix}"
  force_destroy = var.environment != "prod"

  tags = merge(local.common_tags, { Layer = "Landing" })
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket = aws_s3_bucket.landing.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Main Data Lake Bucket (Bronze / Silver / Gold layers) ────────────────────
resource "aws_s3_bucket" "data_lake" {
  bucket                  = "banking-data-lake${local.bucket_suffix}"
  force_destroy           = var.environment != "prod"
  object_lock_enabled     = true   # Required for regulatory immutability

  tags = merge(local.common_tags, { Layer = "DataLake" })
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── KMS Customer-Managed Key for encryption at rest ──────────────────────────
resource "aws_kms_key" "data_lake" {
  description             = "CMK for Banking Data Lake (${var.environment}) - Envelope encryption"
  deletion_window_in_days = var.environment == "prod" ? 30 : 7
  enable_key_rotation     = true
  multi_region            = false

  tags = local.common_tags
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/banking-data-lake-${var.environment}"
  target_key_id = aws_kms_key.data_lake.key_id
}

# Server-Side Encryption with KMS (bucket default)
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_lake.arn
    }
    bucket_key_enabled = true
  }
}

# ── Versioning (required for compliance + recovery) ───────────────────────────
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ── Object Lock (Compliance mode for regulatory retention) ────────────────────
resource "aws_s3_bucket_object_lock_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    default_retention {
      mode  = "COMPLIANCE"   # Strictest mode - cannot be bypassed even by root
      years = 7              # AML / regulatory requirement
    }
  }
}

# ── Lifecycle Configuration (cost optimization + cleanup) ─────────────────────
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Bronze layer: Raw/ingested data → move to cheaper tiers quickly
  rule {
    id     = "bronze-tiering"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"   # Glacier Instant Retrieval
    }

    # Optional: noncurrent version cleanup
    noncurrent_version_expiration {
      noncurrent_days = 180
    }
  }

  # Silver layer: Cleaned/validated data
  rule {
    id     = "silver-tiering"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER_IR"
    }

    noncurrent_version_expiration {
      noncurrent_days = 730
    }
  }

  # Gold layer: Business-ready / frequently queried (keep hot, just cleanup multipart)
  rule {
    id     = "gold-multipart-cleanup"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Landing cleanup (raw files are duplicated in Bronze)
  rule {
    id     = "landing-cleanup"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 7
    }
  }
}

# ── Event Notifications (e.g., for Airflow / event-driven pipelines) ───────────
resource "aws_sns_topic" "landing_events" {
  name = "banking-landing-events-${var.environment}"
  tags = local.common_tags
}

resource "aws_s3_bucket_notification" "landing_notification" {
  bucket = aws_s3_bucket.landing.id

  topic {
    topic_arn = aws_sns_topic.landing_events.arn
    events    = ["s3:ObjectCreated:*"]
    filter_prefix = "transactions/"   # Adjust based on your ingestion paths
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "landing_bucket" {
  description = "Landing zone S3 bucket name"
  value       = aws_s3_bucket.landing.id
}

output "data_lake_bucket" {
  description = "Main data lake S3 bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_kms_key_arn" {
  description = "KMS key ARN used for data lake encryption"
  value       = aws_kms_key.data_lake.arn
}

output "data_lake_kms_alias" {
  description = "KMS alias for easier reference"
  value       = aws_kms_alias.data_lake.name
}

output "sns_landing_events_topic_arn" {
  description = "SNS topic for landing zone events"
  value       = aws_sns_topic.landing_events.arn
}
