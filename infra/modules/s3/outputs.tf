output "bucket_id" {
  value       = aws_s3_bucket.datalake.id
  description = "S3 Data Lake Bucket Name"
}

output "bucket_arn" {
  value       = aws_s3_bucket.datalake.arn
  description = "S3 Data Lake Bucket ARN"
}

output "bronze_path" {
  value       = "s3://${aws_s3_bucket.datalake.id}/bronze/"
  description = "S3 Bronze prefix location"
}

output "silver_path" {
  value       = "s3://${aws_s3_bucket.datalake.id}/silver/"
  description = "S3 Silver prefix location"
}

output "gold_path" {
  value       = "s3://${aws_s3_bucket.datalake.id}/gold/"
  description = "S3 Gold prefix location"
}

output "scripts_path" {
  value       = "s3://${aws_s3_bucket.datalake.id}/scripts/"
  description = "S3 Scripts prefix location"
}

output "athena_results_path" {
  value       = "s3://${aws_s3_bucket.datalake.id}/athena-results/"
  description = "S3 Athena query results prefix location"
}
