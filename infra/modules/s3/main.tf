resource "aws_s3_bucket" "datalake" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "datalake_privacy" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create Data Lake Folder Prefixes
resource "aws_s3_object" "bronze_folder" {
  bucket = aws_s3_bucket.datalake.id
  key    = "bronze/"
}

resource "aws_s3_object" "silver_folder" {
  bucket = aws_s3_bucket.datalake.id
  key    = "silver/"
}

resource "aws_s3_object" "gold_folder" {
  bucket = aws_s3_bucket.datalake.id
  key    = "gold/"
}

resource "aws_s3_object" "scripts_folder" {
  bucket = aws_s3_bucket.datalake.id
  key    = "scripts/"
}

resource "aws_s3_object" "athena_results_folder" {
  bucket = aws_s3_bucket.datalake.id
  key    = "athena-results/"
}

# Upload PySpark ETL Scripts to S3
resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = aws_s3_bucket.datalake.id
  key    = "scripts/bronze_to_silver.py"
  source = "${path.module}/../../../glue/scripts/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../../../glue/scripts/bronze_to_silver.py")
}

resource "aws_s3_object" "silver_to_gold_script" {
  bucket = aws_s3_bucket.datalake.id
  key    = "scripts/silver_to_gold.py"
  source = "${path.module}/../../../glue/scripts/silver_to_gold.py"
  etag   = filemd5("${path.module}/../../../glue/scripts/silver_to_gold.py")
}
