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

variable "project_name" {
  type        = string
  description = "Project name"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}
