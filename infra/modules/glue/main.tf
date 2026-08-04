# ─────────────────────────────────────────────
# GLUE CATALOG DATABASES
# ─────────────────────────────────────────────
resource "aws_glue_catalog_database" "yelp_db" {
  name        = "yelp_db"
  description = "Glue Catalog for Yelp Bronze JSON tables (crawled from S3)"
}

resource "aws_glue_catalog_database" "yelp_db_silver" {
  name        = "yelp_db_silver"
  description = "Glue Catalog for Yelp Silver Parquet tables (crawled after bronze_to_silver ETL)"
}

resource "aws_glue_catalog_database" "yelp_db_gold" {
  name        = "yelp_db_gold"
  description = "Glue Catalog for Yelp Gold analytics tables (BI + ML + RAG)"
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
        var.silver_bucket_arn, "${var.silver_bucket_arn}/*",
        var.gold_bucket_arn, "${var.gold_bucket_arn}/*"
      ]
    }]
  })
}

locals {
  role_arn = var.glue_service_role_arn != "" ? var.glue_service_role_arn : aws_iam_role.glue_role[0].arn
}

# ─────────────────────────────────────────────
# GLUE CRAWLERS
# ─────────────────────────────────────────────

# Bronze Crawler: crawls raw JSON → registers in yelp_db
resource "aws_glue_crawler" "bronze_crawler" {
  name          = "${var.project_name}_bronze_crawler"
  database_name = aws_glue_catalog_database.yelp_db.name
  role          = local.role_arn
  description   = "Crawls raw JSON files in Bronze S3 bucket"

  s3_target {
    path       = "s3://${var.bronze_bucket_id}/"
    exclusions = ["scripts/**"]
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

# Silver Crawler: crawls Parquet written by bronze_to_silver → registers in yelp_db_silver
# CRITICAL: Must run AFTER bronze_to_silver and BEFORE silver_to_gold so the Gold job always
# reads from a freshly catalogued, up-to-date schema and never hits stale/missing file errors.
resource "aws_glue_crawler" "silver_crawler" {
  name          = "${var.project_name}_silver_crawler"
  database_name = aws_glue_catalog_database.yelp_db_silver.name
  role          = local.role_arn
  description   = "Crawls Silver Parquet tables after bronze_to_silver ETL writes them"

  s3_target {
    path = "s3://${var.silver_bucket_id}/silver/"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })
}

# Gold Crawler: crawls final Parquet → registers in yelp_db_gold
resource "aws_glue_crawler" "gold_crawler" {
  name          = "${var.project_name}_gold_crawler"
  database_name = aws_glue_catalog_database.yelp_db_gold.name
  role          = local.role_arn
  description   = "Crawls Gold Parquet tables (BI, ML, RAG) in Gold S3 bucket"

  s3_target {
    path = "s3://${var.gold_bucket_id}/gold/"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

# ─────────────────────────────────────────────
# GLUE JOBS
# ─────────────────────────────────────────────

# Job 1: Bronze → Silver ETL
resource "aws_glue_job" "bronze_to_silver" {
  name              = "${var.project_name}_bronze_to_silver"
  role_arn          = local.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 120

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

# Job 2: Silver → Gold ETL (BI + ML + RAG)
resource "aws_glue_job" "silver_to_gold" {
  name              = "${var.project_name}_silver_to_gold"
  role_arn          = local.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${var.bronze_bucket_id}/scripts/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--SILVER_BUCKET"                    = var.silver_bucket_id
    "--GOLD_BUCKET"                      = var.gold_bucket_id
  }
}

# ─────────────────────────────────────────────
# GLUE WORKFLOW + TRIGGERS
# Updated Sequence:
#   1. Bronze Crawler  (crawls raw JSON → yelp_db)
#   2. bronze_to_silver Job  (writes Parquet → Silver S3)
#   3. Silver Crawler  (crawls Silver S3 → yelp_db_silver) ← NEW: ensures fresh schema
#   4. silver_to_gold Job  (writes Parquet BI/ML/RAG → Gold S3)
#   5. Gold Crawler  (crawls Gold S3 → yelp_db_gold)
# ─────────────────────────────────────────────
resource "aws_glue_workflow" "etl_workflow" {
  name        = "${var.project_name}_etl_workflow"
  description = "Yelp Full Medallion ETL: Bronze Crawler → Bronze-to-Silver Job → Silver Crawler → Silver-to-Gold Job → Gold Crawler"
}

# Trigger 1: start Bronze Crawler on demand (ingest.py calls start-workflow-run)
resource "aws_glue_trigger" "start_crawler" {
  name          = "${var.project_name}_trigger_start_crawler"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.etl_workflow.name

  actions {
    crawler_name = aws_glue_crawler.bronze_crawler.name
  }
}

# Trigger 2: after Bronze Crawler succeeds → run bronze_to_silver job
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

# Trigger 3: after bronze_to_silver job succeeds → run Silver Crawler
# This is the key fix: cataloguing updated Silver schema BEFORE silver_to_gold runs.
resource "aws_glue_trigger" "start_silver_crawler" {
  name          = "${var.project_name}_trigger_silver_crawler"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_workflow.name

  predicate {
    conditions {
      job_name = aws_glue_job.bronze_to_silver.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = aws_glue_crawler.silver_crawler.name
  }
}

# Trigger 4: after Silver Crawler succeeds → run silver_to_gold job
resource "aws_glue_trigger" "start_silver_to_gold" {
  name          = "${var.project_name}_trigger_silver_to_gold"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_workflow.name

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.silver_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.silver_to_gold.name
  }
}

# Trigger 5: after silver_to_gold job succeeds → run Gold Crawler
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
