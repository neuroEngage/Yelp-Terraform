variable "project_name" {
  type        = string
  description = "Project name identifier"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "bucket_id" {
  type        = string
  description = "S3 Data Lake Bucket ID"
}

variable "bucket_arn" {
  type        = string
  description = "S3 Data Lake Bucket ARN"
}

variable "glue_service_role_arn" {
  type        = string
  description = "Custom Glue IAM Role ARN if provided (for AWS Academy)"
  default     = ""
}
