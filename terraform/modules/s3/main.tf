# =============================================================================
# terraform/modules/s3/main.tf
# Banking data lake: Landing, Bronze, Silver, Gold buckets
# with versioning, encryption, lifecycle tiering, and Object Lock
# =============================================================================

resource "aws_s3_bucket" "landing" {
  bucket        = "banking-landing-${var.environment}"
  force_destroy = var.environment != "prod"
  tags          = local.common_tags
}

resource "aws_s3_bucket" "data_lake" {
  bucket              = "banking-data-lake-${var.environment}"
  force_destroy       = var.environment != "prod"
  object_lock_enabled = true    # Immutability for regulatory compliance
  tags                = local.common_tags
}

# ── Encryption ────────────────────────────────────────────────────────────────

resource "aws_kms_key" "data_lake" {
  description             = "Banking data lake CMK (${var.environment})"
  deletion_window_in_days = var.environment == "prod" ? 30 : 7
  enable_key_rotation     = true
  tags                    = local.common_tags
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/banking-data-lake-${var.environment}"
  target_key_id = aws_kms_key.data_lake.key_id
}

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

# ── Versioning ────────────────────────────────────────────────────────────────

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

# ── Block public access ───────────────────────────────────────────────────────

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Object Lock (regulatory retention) ───────────────────────────────────────

resource "aws_s3_bucket_object_lock_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    default_retention {
      mode  = "COMPLIANCE"
      years = 7    # AML regulatory retention requirement
    }
  }
}

# ── Lifecycle: intelligent tiering ───────────────────────────────────────────

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Bronze: audit data — move to cheaper storage after 30 days
  rule {
    id     = "bronze-tiering"
    status = "Enabled"
    filter { prefix = "bronze/" }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }

  # Silver: keep hot for 60 days (used for incremental re-runs)
  rule {
    id     = "silver-tiering"
    status = "Enabled"
    filter { prefix = "silver/" }

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 365
      storage_class = "GLACIER_IR"
    }
  }

  # Gold: always hot — queried by Redshift and BI tools
  rule {
    id     = "gold-abort-multipart"
    status = "Enabled"
    filter { prefix = "gold/" }
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }

  # Landing: delete raw files after 7 days (they're safe in Bronze Delta)
  rule {
    id     = "landing-cleanup"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 7 }
  }
}

# ── Notification → SNS (for Airflow S3 sensor) ───────────────────────────────

resource "aws_s3_bucket_notification" "landing_notification" {
  bucket = aws_s3_bucket.landing.id

  topic {
    topic_arn     = aws_sns_topic.landing_events.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "transactions/"
  }
}

resource "aws_sns_topic" "landing_events" {
  name = "banking-landing-events-${var.environment}"
  tags = local.common_tags
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "data_lake_bucket"    { value = aws_s3_bucket.data_lake.id }
output "landing_bucket"      { value = aws_s3_bucket.landing.id }
output "data_lake_kms_arn"   { value = aws_kms_key.data_lake.arn }

locals {
  common_tags = {
    Environment = var.environment
    Project     = "banking-data-platform"
    ManagedBy   = "Terraform"
    Compliance  = "AML,SOX,BASEL3"
  }
}
