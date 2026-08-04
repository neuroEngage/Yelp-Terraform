# Yelp Big Data Pipeline

End-to-end automated Medallion Data Lakehouse pipeline using **Terraform**, **GitHub Actions**, **AWS Glue (PySpark)**, and an **S3 Data Lake** (Bronze → Silver → Gold).

## How it works

Push to `main` → GitHub Actions runs automatically:

1. **Terraform** provisions Bronze, Silver, and Gold S3 buckets, Glue Databases (`yelp_db`, `yelp_db_gold`), Crawlers, Jobs, and Workflow
2. **ingest.py** downloads Yelp dataset from Kaggle and uploads raw JSON to Bronze S3
3. **AWS Glue Workflow** executes automatically:
   - **Step 1**: Bronze Crawler registers JSON tables in `yelp_db`
   - **Step 2**: `bronze_to_silver.py` cleans and writes Parquet to Silver S3
   - **Step 3**: `silver_to_gold.py` computes BI Star Schema, ML Features, and RAG Documents into Gold S3
   - **Step 4**: Gold Crawler catalogs analytics tables into `yelp_db_gold` for Athena & Power BI

## Repository Structure

```
.
├── .github/
│   └── workflows/
│       ├── terraform-apply.yml   # Main: Terraform + Ingest (runs on push to main)
│       ├── terraform-plan.yml    # PR check: shows plan before merge
│       └── terraform-destroy.yml # Manual: teardown all AWS resources
│
├── infra/
│   ├── modules/
│   │   ├── s3/      # Bronze, Silver, and Gold S3 buckets
│   │   └── glue/    # Glue DBs, Crawlers, Jobs, Workflow
│   ├── main.tf · variables.tf · outputs.tf
│   ├── provider.tf · versions.tf
│   ├── backend.tf         # HCP Terraform remote state
│   └── terraform.tfvars
│
├── ingestion/
│   ├── ingest.py          # Kaggle download → S3 Bronze upload → Glue trigger
│   └── requirements.txt
│
├── glue/
│   └── scripts/
│       ├── bronze_to_silver.py  # PySpark ETL: raw JSON → clean Parquet
│       └── silver_to_gold.py    # PySpark ETL: Silver Parquet → Gold (BI + ML + RAG)
│
├── docs/
│   └── architecture.md
│
├── .gitignore
└── README.md
```

## GitHub Secrets Required

| Secret | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials |
| `AWS_SESSION_TOKEN` | AWS session token |
| `KAGGLE_USERNAME` | Kaggle account username |
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
├── business/     (Parquet, Snappy)
├── review/
├── user/
├── tip/
└── checkin/

yelp-gold-analytics-us-east-1/
└── gold/
    ├── bi/       (Star-schema BI dimension & fact tables)
    ├── ml/       (Machine Learning feature store)
    └── rag/      (RAG vector search documents)
```

## To Destroy All Resources

Go to **GitHub → Actions → Terraform Destroy → Run workflow**
