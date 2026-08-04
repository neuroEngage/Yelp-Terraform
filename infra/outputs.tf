output "s3_bucket_name" {
  value       = module.s3.bucket_id
  description = "S3 Data Lake Bucket Name"
}

output "glue_database_name" {
  value       = module.glue.database_name
  description = "Glue Catalog Database Name"
}

output "glue_workflow_name" {
  value       = module.glue.workflow_name
  description = "Glue Workflow Name"
}

output "athena_workgroup_name" {
  value       = module.athena.workgroup_name
  description = "Athena Workgroup Name"
}
