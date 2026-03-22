# =============================================================================
# terraform/modules/kafka/main.tf
# AWS MSK (Managed Streaming for Apache Kafka) cluster
# with IAM auth, TLS encryption, and Schema Registry
# =============================================================================

resource "aws_msk_cluster" "banking" {
  cluster_name           = "banking-kafka-${var.environment}"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = var.environment == "prod" ? 6 : 3

  broker_node_group_info {
    instance_type   = var.environment == "prod" ? "kafka.m5.2xlarge" : "kafka.t3.small"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.environment == "prod" ? 1000 : 100

        # Auto-scale storage in production
        provisioned_throughput {
          enabled           = var.environment == "prod"
          volume_throughput = var.environment == "prod" ? 250 : null
        }
      }
    }
  }

  # IAM authentication (no username/password)
  client_authentication {
    sasl {
      iam = true
    }
    tls {}
  }

  # Encryption in transit and at rest
  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = var.kms_key_arn
  }

  # Enhanced monitoring for CloudWatch
  enhanced_monitoring = "PER_TOPIC_PER_PARTITION"

  open_monitoring {
    prometheus {
      jmx_exporter  { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = "/aws/msk/banking-${var.environment}"
      }
      s3 {
        enabled = true
        bucket  = var.logs_bucket
        prefix  = "msk/banking-${var.environment}/"
      }
    }
  }

  tags = {
    Environment = var.environment
    Project     = "banking-data-platform"
    ManagedBy   = "Terraform"
  }
}

# ── Topics (created via Terraform for IaC governance) ────────────────────────

resource "aws_msk_topic" "transactions" {
  cluster_arn         = aws_msk_cluster.banking.arn
  name                = "banking.transactions"
  partitions          = var.environment == "prod" ? 24 : 6
  replication_factor  = var.environment == "prod" ? 3  : 2
  configs = {
    "retention.ms"          = "604800000"     # 7 days
    "cleanup.policy"        = "delete"
    "compression.type"      = "lz4"
    "min.insync.replicas"   = var.environment == "prod" ? "2" : "1"
    "message.max.bytes"     = "1048576"       # 1 MB
  }
}

resource "aws_msk_topic" "fraud_signals" {
  cluster_arn        = aws_msk_cluster.banking.arn
  name               = "banking.fraud-signals"
  partitions         = var.environment == "prod" ? 12 : 3
  replication_factor = var.environment == "prod" ? 3  : 2
  configs = {
    "retention.ms"        = "2592000000"  # 30 days
    "cleanup.policy"      = "compact"     # Compact: keep latest per key
    "compression.type"    = "lz4"
  }
}

resource "aws_msk_topic" "transactions_dlq" {
  cluster_arn        = aws_msk_cluster.banking.arn
  name               = "banking.transactions.dlq"
  partitions         = 3
  replication_factor = var.environment == "prod" ? 3 : 2
  configs = {
    "retention.ms"   = "2592000000"  # 30 days — for investigation
    "cleanup.policy" = "delete"
  }
}

# ── Security group ────────────────────────────────────────────────────────────

resource "aws_security_group" "msk" {
  name        = "banking-msk-${var.environment}"
  description = "MSK Kafka cluster - banking platform"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Kafka TLS from banking VPC"
    from_port       = 9098   # IAM auth port
    to_port         = 9098
    protocol        = "tcp"
    cidr_blocks     = [var.vpc_cidr]
  }

  ingress {
    description     = "Zookeeper (internal)"
    from_port       = 2181
    to_port         = 2181
    protocol        = "tcp"
    cidr_blocks     = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "banking-msk-${var.environment}"
    Environment = var.environment
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "bootstrap_brokers_iam" { value = aws_msk_cluster.banking.bootstrap_brokers_sasl_iam }
output "cluster_arn"           { value = aws_msk_cluster.banking.arn }
output "zookeeper_connect"     { value = aws_msk_cluster.banking.zookeeper_connect_string }
