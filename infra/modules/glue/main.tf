# Glue Database Catalog
resource "aws_glue_catalog_database" "yelp_db" {
  name        = "${var.project_name}_db_${var.environment}"
  description = "Glue Database for Yelp Data Lake (Bronze/Silver/Gold layers)"
}

# IAM Role for AWS Glue (fallback if glue_service_role_arn is not provided)
resource "aws_iam_role" "glue_role" {
  count = var.glue_service_role_arn == "" ? 1 : 0
  name  = "${var.project_name}-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
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
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.bucket_arn,
          "${var.bucket_arn}/*"
        ]
      }
    ]
  })
}

locals {
  role_arn = var.glue_service_role_arn != "" ? var.glue_service_role_arn : aws_iam_role.glue_role[0].arn
}

# Glue Job 1: Bronze to Silver
resource "aws_glue_job" "bronze_to_silver" {
  name         = "${var.project_name}_bronze_to_silver"
  role_arn     = local.role_arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_id}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--S3_BUCKET"                       = var.bucket_id
    "--DATABASE_NAME"                   = aws_glue_catalog_database.yelp_db.name
  }
}

# Glue Job 2: Silver to Gold
resource "aws_glue_job" "silver_to_gold" {
  name         = "${var.project_name}_silver_to_gold"
  role_arn     = local.role_arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_id}/scripts/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--S3_BUCKET"                       = var.bucket_id
    "--DATABASE_NAME"                   = aws_glue_catalog_database.yelp_db.name
  }
}

# Glue Crawler for Gold Layer (Optional Metadata Cataloging)
resource "aws_glue_crawler" "gold_crawler" {
  database_name = aws_glue_catalog_database.yelp_db.name
  name          = "${var.project_name}_gold_crawler"
  role          = local.role_arn

  s3_target {
    path = "s3://${var.bucket_id}/gold/"
  }
}

# Glue Workflow: Orchestrator
resource "aws_glue_workflow" "etl_workflow" {
  name        = "${var.project_name}_etl_workflow"
  description = "Orchestrates Yelp Bronze -> Silver -> Gold ETL Pipeline"
}

# Workflow Trigger 1: Starts Bronze to Silver job
resource "aws_glue_trigger" "start_bronze_to_silver" {
  name          = "${var.project_name}_trigger_bronze_to_silver"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.etl_workflow.name

  actions {
    job_name = aws_glue_job.bronze_to_silver.name
  }
}

# Workflow Trigger 2: Starts Silver to Gold job after Bronze to Silver succeeds
resource "aws_glue_trigger" "start_silver_to_gold" {
  name          = "${var.project_name}_trigger_silver_to_gold"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_workflow.name

  predicate {
    conditions {
      job_name = aws_glue_job.bronze_to_silver.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.silver_to_gold.name
  }
}

# Workflow Trigger 3: Starts Crawler after Silver to Gold succeeds
resource "aws_glue_trigger" "start_gold_crawler" {
  name          = "${var.project_name}_trigger_gold_crawler"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_workflow.name

  predicate {
    conditions {
      job_name = aws_glue_job.silver_to_gold.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = aws_glue_crawler.gold_crawler.name
  }
}
