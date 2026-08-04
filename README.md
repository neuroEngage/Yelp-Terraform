# Yelp Big Data Pipeline

End-to-end automated Big Data pipeline using **Terraform**, **GitHub Actions**, **AWS Glue (PySpark)**, and an **S3 Data Lake** (Bronze → Silver).

## How it works

Push to `main` → GitHub Actions runs automatically:

1. **Terraform** provisions Bronze + Silver S3 buckets, Glue DB, Crawler, Job, and Workflow
2. **ingest.py** downloads Yelp dataset from Kaggle and uploads raw JSON to Bronze S3
3. **Glue Workflow** runs: Crawler catalogs the JSON → `bronze_to_silver.py` cleans and writes Parquet to Silver S3

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
│   │   ├── s3/      # Bronze + Silver S3 buckets
│   │   └── glue/    # Glue DB, Crawler, Job, Workflow
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── provider.tf
│   ├── versions.tf
│   ├── backend.tf         # HCP Terraform remote state
│   └── terraform.tfvars
│
├── ingestion/
│   ├── ingest.py          # Kaggle download → S3 Bronze upload → Glue trigger
│   └── requirements.txt
│
├── glue/
│   └── scripts/
│       └── bronze_to_silver.py  # PySpark ETL: raw JSON → clean Parquet
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
| `AWS_ACCESS_KEY_ID` | AWS Academy credentials |
| `AWS_SECRET_ACCESS_KEY` | AWS Academy credentials |
| `AWS_SESSION_TOKEN` | AWS Academy session token |
| `KAGGLE_USERNAME` | Kaggle account username |
| `KAGGLE_KEY` | Kaggle API key |
| `TF_API_TOKEN` | HCP Terraform API token |

## S3 Layout After Pipeline Runs

```
yelp-bronze-raw-us-east-1/
├── yelp_academic_dataset_business.json
├── yelp_academic_dataset_review.json
├── yelp_academic_dataset_user.json
├── yelp_academic_dataset_tip.json
├── yelp_academic_dataset_checkin.json
├── photos.json
└── scripts/
    └── bronze_to_silver.py

yelp-silver-clean-us-east-1/
├── business/     (Parquet, snappy compressed)
├── review/
├── user/
├── tip/
└── checkin/
```

## To Destroy All Resources

Go to **GitHub → Actions → Terraform Destroy → Run workflow**
