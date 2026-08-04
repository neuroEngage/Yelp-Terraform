output "bronze_bucket_id" {
  value       = aws_s3_bucket.bronze.id
  description = "Bronze S3 Bucket Name"
}

output "bronze_bucket_arn" {
  value       = aws_s3_bucket.bronze.arn
  description = "Bronze S3 Bucket ARN"
}

output "silver_bucket_id" {
  value       = aws_s3_bucket.silver.id
  description = "Silver S3 Bucket Name"
}

output "silver_bucket_arn" {
  value       = aws_s3_bucket.silver.arn
  description = "Silver S3 Bucket ARN"
}

output "gold_bucket_id" {
  value       = aws_s3_bucket.gold.id
  description = "Gold S3 Bucket Name"
}

output "gold_bucket_arn" {
  value       = aws_s3_bucket.gold.arn
  description = "Gold S3 Bucket ARN"
}
