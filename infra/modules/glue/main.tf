# ─────────────────────────────────────────────
# GLUE CATALOG DATABASE
# ─────────────────────────────────────────────
resource "aws_glue_catalog_database" "yelp_db" {
  name        = "yelp_db"
  description = "Glue Catalog for Yelp Bronze JSON tables (crawled from S3)"
}

# ─────────────────────────────────────────────
# IAM ROLE (only created if LabRole not provided)
# ─────────────────────────────────────────────
resource "aws_iam_role" "glue_role" {
  count = var.glue_service_role_arn == "" ? 1 : 0
  name  = "${var.project_name}-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  count      = var.glue_service_role_arn == "" ? 1 : 0
  role       = aws_iam_role.glue_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_policy" {
  count = var.glue_service_role_arn == "" ? 1 : 0
  name  = "${var.project_name}-glue-s3-access"
  role  = aws_iam_role.glue_role[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        var.bronze_bucket_arn, "${var.bronze_bucket_arn}/*",
        var.silver_bucket_arn, "${var.silver_bucket_arn}/*"
      ]
    }]
  })
}

locals {
  role_arn = var.glue_service_role_arn != "" ? var.glue_service_role_arn : aws_iam_role.glue_role[0].arn
}

# ─────────────────────────────────────────────
# GLUE CRAWLER  (crawls bronze bucket → registers tables in yelp_db)
# Must run BEFORE the ETL job on first deployment
# ─────────────────────────────────────────────
resource "aws_glue_crawler" "bronze_crawler" {
  name          = "${var.project_name}_bronze_crawler"
  database_name = aws_glue_catalog_database.yelp_db.name
  role          = local.role_arn
  description   = "Crawls raw JSON files in the Bronze S3 bucket"

  s3_target {
    path = "s3://${var.bronze_bucket_id}/"

    exclusions = ["scripts/**"]
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

# ─────────────────────────────────────────────
# GLUE JOB  bronze → silver
# ─────────────────────────────────────────────
resource "aws_glue_job" "bronze_to_silver" {
  name         = "${var.project_name}_bronze_to_silver"
  role_arn     = local.role_arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  timeout      = 120

  command {
    name            = "glueetl"
    script_location = "s3://${var.bronze_bucket_id}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--S3_BUCKET"                        = var.silver_bucket_id
    "--DATABASE_NAME"                    = aws_glue_catalog_database.yelp_db.name
  }
}

# ─────────────────────────────────────────────
# GLUE WORKFLOW + TRIGGERS
# Order: Crawler → bronze_to_silver job
# ─────────────────────────────────────────────
resource "aws_glue_workflow" "etl_workflow" {
  name        = "${var.project_name}_etl_workflow"
  description = "Yelp ETL: crawl bronze → run bronze_to_silver"
}

# Trigger 1: start crawler on demand (ingest.py calls start-workflow-run)
resource "aws_glue_trigger" "start_crawler" {
  name          = "${var.project_name}_trigger_start_crawler"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.etl_workflow.name

  actions {
    crawler_name = aws_glue_crawler.bronze_crawler.name
  }
}

# Trigger 2: after crawler succeeds → run bronze_to_silver job
resource "aws_glue_trigger" "start_bronze_to_silver" {
  name          = "${var.project_name}_trigger_bronze_to_silver"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_workflow.name

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.bronze_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.bronze_to_silver.name
  }
}
