# Yelp Big Data Pipeline

End-to-end automated Medallion Data Lakehouse pipeline using **Terraform**, **GitHub Actions**, **AWS Glue (PySpark)**, and an **S3 Data Lake** (Bronze → Silver → Gold).

## How It Works

Push to `main` → GitHub Actions runs the 3-Stage CI/CD Pipeline automatically:

1. **Job 1 · Terraform Apply**: Provisions S3 buckets (`bronze`, `silver`, `gold`), Glue Databases (`yelp_db`, `yelp_db_silver`, `yelp_db_gold`), Crawlers, PySpark Jobs, and Workflow, and uploads Glue scripts to S3.
2. **Job 2 · Kaggle Ingest → S3 Bronze → Glue**: Downloads the Yelp dataset from Kaggle API, uploads raw JSON to Bronze S3, and triggers the AWS Glue Workflow.
3. **AWS Glue Workflow Execution**:
   - **Step 1**: `bronze_crawler` registers raw JSON tables in `yelp_db`.
   - **Step 2**: `bronze_to_silver` PySpark job cleans JSON and writes Parquet to Silver S3 (`s3://<silver_bucket>/silver/`).
   - **Step 3**: `silver_crawler` catalogs updated Silver schema into `yelp_db_silver`.
   - **Step 4**: `silver_to_gold` PySpark job computes BI Star Schema, ML Feature Store, and RAG Documents into Gold S3 (`s3://<gold_bucket>/gold/`).
   - **Step 5**: `gold_crawler` catalogs analytics tables into `yelp_db_gold` for Athena & Power BI querying.
4. **Job 3 · Silver to Gold ETL Verification**: `trigger_gold.py` safely waits for the active Glue Workflow, verifies Silver datasets, runs/monitors `silver_to_gold` and `gold_crawler`, and verifies final Gold S3 outputs.

## Repository Structure

```
.
├── .github/
│   └── workflows/
│       ├── terraform-apply.yml   # Main: 3-Job Pipeline (runs on push to main)
│       ├── terraform-plan.yml    # PR check: shows terraform plan before merge
│       └── terraform-destroy.yml # Manual: teardown all AWS resources
│
├── infra/
│   ├── modules/
│   │   ├── s3/      # Bronze, Silver, and Gold S3 buckets
│   │   └── glue/    # Glue DBs (Bronze, Silver, Gold), Crawlers, Jobs, Workflow
│   ├── main.tf · variables.tf · outputs.tf
│   ├── provider.tf · versions.tf
│   ├── backend.tf         # HCP Terraform remote state
│   └── terraform.tfvars
│
├── ingestion/
│   ├── ingest.py          # Kaggle download → S3 Bronze upload → Glue trigger
│   ├── trigger_gold.py    # Workflow waiter & Silver-to-Gold Glue runner
│   └── requirements.txt
│
├── glue/
│   └── scripts/
│       ├── bronze_to_silver.py  # PySpark ETL: raw JSON → clean Silver Parquet
│       └── silver_to_gold.py    # PySpark ETL: Silver Parquet → Gold (BI + ML + RAG)
│
├── docs/
│   ├── architecture.md          # Visual pipeline & Medallion data architecture
│   └── project_documentation.md # In-depth technical documentation
│
├── .gitignore
└── README.md
```

## GitHub Secrets Required

| Secret | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS IAM access key ID |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM secret access key |
| `AWS_SESSION_TOKEN` | AWS IAM session token (for AWS Academy / temporary credentials) |
| `KAGGLE_USERNAME` | Kaggle username |
| `KAGGLE_KEY` | Kaggle API key |
| `TF_API_TOKEN` | HCP Terraform API token |

## S3 Data Lake Layout

```
yelp-bronze-raw-us-east-1/
├── yelp_academic_dataset_business.json
├── yelp_academic_dataset_review.json
├── yelp_academic_dataset_user.json
├── yelp_academic_dataset_tip.json
├── yelp_academic_dataset_checkin.json
├── photos.json
└── scripts/
    ├── bronze_to_silver.py
    └── silver_to_gold.py

yelp-silver-clean-us-east-1/
└── silver/
    ├── business/     (Parquet, Snappy)
    ├── review/       (Parquet, Snappy)
    ├── user/         (Parquet, Snappy)
    ├── checkin/      (Parquet, Snappy)
    └── tip/          (Parquet, Snappy)

yelp-gold-analytics-us-east-1/
└── gold/
    ├── bi/           (Star-Schema Analytics Tables)
    │   ├── dim_date
    │   ├── dim_business
    │   ├── dim_business_hours
    │   ├── fact_business
    │   ├── fact_review_trend
    │   ├── fact_rating_distribution
    │   ├── fact_checkin_day
    │   └── fact_checkin_hour
    │
    ├── ml/           (Machine Learning Feature Store)
    │   ├── sentiment_features
    │   ├── rating_prediction
    │   ├── collaborative_filtering
    │   ├── content_based_filtering
    │   └── customer_segmentation
    │
    └── rag/          (RAG Vector Search Context Documents)
        ├── business_documents
        └── review_documents
```

## To Teardown Infrastructure

Go to **GitHub → Actions → Terraform Destroy → Run workflow**
