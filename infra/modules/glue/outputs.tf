output "database_name" {
  value       = aws_glue_catalog_database.yelp_db.name
  description = "Glue Catalog Database Name (Bronze JSON)"
}

output "silver_database_name" {
  value       = aws_glue_catalog_database.yelp_db_silver.name
  description = "Glue Catalog Silver Database Name (Silver Parquet)"
}

output "workflow_name" {
  value       = aws_glue_workflow.etl_workflow.name
  description = "Glue ETL Workflow Name"
}

output "bronze_to_silver_job_name" {
  value       = aws_glue_job.bronze_to_silver.name
  description = "Glue Job: Bronze to Silver"
}

output "bronze_crawler_name" {
  value       = aws_glue_crawler.bronze_crawler.name
  description = "Glue Crawler Name for Bronze layer"
}

output "silver_crawler_name" {
  value       = aws_glue_crawler.silver_crawler.name
  description = "Glue Crawler Name for Silver layer (runs after bronze_to_silver, before silver_to_gold)"
}

output "gold_database_name" {
  value       = aws_glue_catalog_database.yelp_db_gold.name
  description = "Glue Catalog Gold Database Name"
}

output "silver_to_gold_job_name" {
  value       = aws_glue_job.silver_to_gold.name
  description = "Glue Job: Silver to Gold"
}

output "gold_crawler_name" {
  value       = aws_glue_crawler.gold_crawler.name
  description = "Glue Crawler Name for Gold layer"
}
