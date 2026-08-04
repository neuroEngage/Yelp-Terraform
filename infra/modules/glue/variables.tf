variable "project_name" {
  type        = string
  description = "Project name identifier"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "bronze_bucket_id" {
  type        = string
  description = "Bronze S3 Bucket ID (scripts location + source data)"
}

variable "bronze_bucket_arn" {
  type        = string
  description = "Bronze S3 Bucket ARN"
}

variable "silver_bucket_id" {
  type        = string
  description = "Silver S3 Bucket ID (output from Glue ETL)"
}

variable "silver_bucket_arn" {
  type        = string
  description = "Silver S3 Bucket ARN"
}

variable "gold_bucket_id" {
  type        = string
  description = "Gold S3 Bucket ID (output from Glue Silver to Gold ETL)"
}

variable "gold_bucket_arn" {
  type        = string
  description = "Gold S3 Bucket ARN"
}

variable "glue_service_role_arn" {
  type        = string
  description = "IAM Role ARN for Glue (LabRole for AWS Academy)"
  default     = ""
}
