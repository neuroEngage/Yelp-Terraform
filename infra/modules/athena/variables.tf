variable "project_name" {
  type        = string
  description = "Project name identifier"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "athena_results_path" {
  type        = string
  description = "S3 URI for storing Athena query results"
}
