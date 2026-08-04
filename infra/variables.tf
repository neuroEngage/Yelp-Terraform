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

variable "bucket_name" {
  type        = string
  description = "Name of the S3 Data Lake bucket"
}

variable "glue_service_role_arn" {
  type        = string
  description = "Optional ARN for AWS Glue IAM service role (for AWS Academy LabRole compatibility)"
  default     = ""
}
