variable "aws_region" {
  type        = string
  description = "AWS region for infrastructure deployment"
  default     = "us-east-1"
}

variable "project_name" {
  type        = string
  description = "Project name identifier"
  default     = "yelp-bigdata"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, staging, prod)"
  default     = "dev"
}

variable "bronze_bucket_name" {
  type        = string
  description = "Name of the S3 Bronze (raw) bucket"
}

variable "silver_bucket_name" {
  type        = string
  description = "Name of the S3 Silver (cleaned) bucket"
}

variable "gold_bucket_name" {
  type        = string
  description = "Name of the S3 Gold (analytics BI/ML/RAG) bucket"
}

variable "glue_service_role_arn" {
  type        = string
  description = "IAM Role ARN for Glue (use LabRole for AWS Academy)"
  default     = ""
}
