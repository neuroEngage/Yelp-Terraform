# ─────────────────────────────────────────────
# BRONZE BUCKET  (raw JSON from Kaggle)
# ─────────────────────────────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket        = var.bronze_bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─────────────────────────────────────────────
# SILVER BUCKET  (cleaned Parquet from Glue)
# ─────────────────────────────────────────────
resource "aws_s3_bucket" "silver" {
  bucket        = var.silver_bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─────────────────────────────────────────────
# SCRIPTS BUCKET PREFIX (Glue ETL scripts)
# stored inside the bronze bucket /scripts/
# ─────────────────────────────────────────────
resource "aws_s3_object" "scripts_folder" {
  bucket  = aws_s3_bucket.bronze.id
  key     = "scripts/"
  content = ""
}

# Upload the Glue scripts to bronze/scripts/
resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = aws_s3_bucket.bronze.id
  key    = "scripts/bronze_to_silver.py"
  source = "${path.module}/../../../glue/scripts/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../../../glue/scripts/bronze_to_silver.py")
}
