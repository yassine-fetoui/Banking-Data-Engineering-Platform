# =============================================================================
# terraform/modules/redshift/main.tf
# Amazon Redshift Serverless — serving layer for BI and regulatory reports
# =============================================================================

resource "aws_redshiftserverless_namespace" "banking" {
  namespace_name      = "banking-${var.environment}"
  db_name             = "banking"
  admin_username      = "banking_admin"
  admin_user_password = var.admin_password    # Rotated via Secrets Manager
  iam_roles           = [aws_iam_role.redshift_s3.arn]

  log_exports = ["useractivitylog", "userlog", "connectionlog"]

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_redshiftserverless_workgroup" "banking" {
  namespace_name = aws_redshiftserverless_namespace.banking.namespace_name
  workgroup_name = "banking-${var.environment}"

  base_capacity  = var.environment == "prod" ? 128 : 32  # RPUs

  security_group_ids = [aws_security_group.redshift.id]
  subnet_ids         = var.private_subnet_ids

  config_parameter {
    parameter_key   = "enable_user_activity_logging"
    parameter_value = "true"
  }

  config_parameter {
    parameter_key   = "require_ssl"
    parameter_value = "true"
  }

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ── IAM role: Redshift → S3 (for COPY from Delta Lake) ───────────────────────

resource "aws_iam_role" "redshift_s3" {
  name = "banking-redshift-s3-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "s3-read-data-lake"
  role = aws_iam_role.redshift_s3.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          "arn:aws:s3:::banking-data-lake-${var.environment}",
          "arn:aws:s3:::banking-data-lake-${var.environment}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey"]
        Resource = [var.data_lake_kms_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"]
        Resource = ["*"]
      }
    ]
  })
}

# ── Schemas (bootstrapped via Terraform null_resource) ────────────────────────

resource "aws_redshiftserverless_endpoint_access" "banking" {
  workgroup_name = aws_redshiftserverless_workgroup.banking.workgroup_name
  endpoint_name  = "banking-${var.environment}"
  subnet_ids     = var.private_subnet_ids
}

# ── Security group ────────────────────────────────────────────────────────────

resource "aws_security_group" "redshift" {
  name        = "banking-redshift-${var.environment}"
  description = "Redshift Serverless — banking data platform"
  vpc_id      = var.vpc_id

  ingress {
    description = "Redshift from banking VPC"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "banking-redshift-${var.environment}"
    Environment = var.environment
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "workgroup_endpoint" { value = aws_redshiftserverless_workgroup.banking.endpoint }
output "redshift_role_arn"  { value = aws_iam_role.redshift_s3.arn }
output "namespace_id"       { value = aws_redshiftserverless_namespace.banking.id }
