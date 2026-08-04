output "bronze_bucket_name" {
  value       = module.s3.bronze_bucket_id
  description = "S3 Bronze Bucket (raw Kaggle JSON)"
}

output "silver_bucket_name" {
  value       = module.s3.silver_bucket_id
  description = "S3 Silver Bucket (cleaned Parquet from Glue)"
}

output "gold_bucket_name" {
  value       = module.s3.gold_bucket_id
  description = "S3 Gold Bucket (analytics BI/ML/RAG Parquet from Glue)"
}

output "glue_database_name" {
  value       = module.glue.database_name
  description = "Glue Catalog Database Name (Bronze JSON)"
}

output "glue_silver_database_name" {
  value       = module.glue.silver_database_name
  description = "Glue Catalog Silver Database Name (Silver Parquet tables)"
}

output "glue_gold_database_name" {
  value       = module.glue.gold_database_name
  description = "Glue Catalog Gold Database Name (BI/ML/RAG Parquet)"
}

output "glue_workflow_name" {
  value       = module.glue.workflow_name
  description = "Glue ETL Workflow Name"
}

output "glue_silver_crawler_name" {
  value       = module.glue.silver_crawler_name
  description = "Glue Silver Crawler Name (runs after bronze_to_silver, before silver_to_gold)"
}
