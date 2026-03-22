# =============================================================================
# terraform/environments/dev/main.tf
# =============================================================================

terraform {
  required_version = ">= 1.7.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.40" }
  }
  backend "s3" {
    bucket         = "banking-terraform-state-dev"
    key            = "banking-data-platform/dev/terraform.tfstate"
    region         = "eu-west-1"
    dynamodb_table = "banking-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Environment = "dev"
      Project     = "banking-data-platform"
      ManagedBy   = "Terraform"
      CostCentre  = "DATA-ENGINEERING"
    }
  }
}

module "s3" {
  source      = "../../modules/s3"
  environment = "dev"
  logs_bucket = "banking-logs-dev"
}

module "kafka" {
  source             = "../../modules/kafka"
  environment        = "dev"
  vpc_id             = var.vpc_id
  vpc_cidr           = var.vpc_cidr
  private_subnet_ids = var.private_subnet_ids
  kms_key_arn        = module.s3.data_lake_kms_arn
  logs_bucket        = "banking-logs-dev"
}

module "redshift" {
  source             = "../../modules/redshift"
  environment        = "dev"
  vpc_id             = var.vpc_id
  vpc_cidr           = var.vpc_cidr
  private_subnet_ids = var.private_subnet_ids
  admin_password     = var.redshift_admin_password
  data_lake_kms_arn  = module.s3.data_lake_kms_arn
}

module "iam" {
  source            = "../../modules/iam"
  environment       = "dev"
  data_lake_bucket  = module.s3.data_lake_bucket
  msk_cluster_arn   = module.kafka.cluster_arn
  kms_key_arn       = module.s3.data_lake_kms_arn
}

variable "aws_region"               { default = "eu-west-1" }
variable "vpc_id"                   {}
variable "vpc_cidr"                 {}
variable "private_subnet_ids"       { type = list(string) }
variable "redshift_admin_password"  { sensitive = true }

output "msk_bootstrap"        { value = module.kafka.bootstrap_brokers_iam }
output "redshift_endpoint"    { value = module.redshift.workgroup_endpoint }
output "data_lake_bucket"     { value = module.s3.data_lake_bucket }
output "landing_bucket"       { value = module.s3.landing_bucket }
