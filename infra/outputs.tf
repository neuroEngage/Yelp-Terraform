output "bronze_bucket_name" {
  value       = module.s3.bronze_bucket_id
  description = "S3 Bronze Bucket (raw Kaggle JSON)"
}

output "silver_bucket_name" {
  value       = module.s3.silver_bucket_id
  description = "S3 Silver Bucket (cleaned Parquet from Glue)"
}

output "glue_database_name" {
  value       = module.glue.database_name
  description = "Glue Catalog Database Name"
}

output "glue_workflow_name" {
  value       = module.glue.workflow_name
  description = "Glue ETL Workflow Name"
}
