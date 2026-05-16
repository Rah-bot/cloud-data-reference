terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "env" {
  type    = string
  default = "dev"
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# Storage: separate buckets per layer + checkpoints + UC metastore root.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "landing" {
  bucket = "lakehouse-${var.env}-landing"
}

resource "aws_s3_bucket" "bronze" {
  bucket = "lakehouse-${var.env}-bronze"
}

resource "aws_s3_bucket" "silver" {
  bucket = "lakehouse-${var.env}-silver"
}

resource "aws_s3_bucket" "gold" {
  bucket = "lakehouse-${var.env}-gold"
}

resource "aws_s3_bucket" "checkpoints" {
  bucket = "lakehouse-${var.env}-checkpoints"
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ---------------------------------------------------------------------------
# Databricks Unity Catalog
# ---------------------------------------------------------------------------

resource "databricks_catalog" "retail" {
  name    = "retail_${var.env}"
  comment = "Retail lakehouse — managed by Terraform"
  properties = {
    purpose = "lakehouse"
  }
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.retail.name
  name         = "bronze"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.retail.name
  name         = "silver"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.retail.name
  name         = "gold"
}

# ---------------------------------------------------------------------------
# Cluster policy — enforces cost & security guardrails
# ---------------------------------------------------------------------------

resource "databricks_cluster_policy" "lakehouse" {
  name = "lakehouse-${var.env}-policy"
  definition = jsonencode({
    "spark_version" : {
      "type" : "regex",
      "pattern" : "^13\\..*"
    },
    "node_type_id" : {
      "type" : "allowlist",
      "values" : ["i3.xlarge", "i3.2xlarge", "i3.4xlarge"]
    },
    "autoscale.max_workers" : {
      "type" : "range",
      "maxValue" : 16
    },
    "data_security_mode" : {
      "type" : "fixed",
      "value" : "USER_ISOLATION"
    }
  })
}
