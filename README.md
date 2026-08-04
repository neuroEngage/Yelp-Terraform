# Yelp Big Data Platform — Automated Data Engineering Pipeline

Production-inspired, serverless Big Data platform for ingesting, transforming, and analyzing the Yelp Academic Dataset using **Terraform (IaC)**, **GitHub Actions (CI/CD)**, **AWS S3**, **AWS Glue (PySpark)**, **Amazon Athena**, and **Power BI**.

---

## 🏛️ Repository Architecture

```text
yelp-project-automation/
├── .github/
│   └── workflows/
│       ├── terraform-plan.yml      # CI lint & plan checks
│       ├── terraform-apply.yml     # Automated infrastructure deployment
│       ├── ingest.yml              # Kaggle data download & Glue workflow trigger
│       └── terraform-destroy.yml   # Tear down cloud resources
├── infra/
│   ├── modules/
│   │   ├── s3/                     # S3 Data Lake bucket & script uploader
│   │   ├── glue/                   # Glue DB, Jobs, Workflow & Crawler
│   │   ├── athena/                 # Athena Workgroup configuration
│   │   └── cloudwatch/             # Log Groups & Monitoring
│   ├── provider.tf
│   ├── backend.tf
│   ├── versions.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── terraform.tfvars
│   └── main.tf
├── ingestion/
│   ├── ingest.py                   # Python Kaggle API ingestion & S3 streaming
│   └── requirements.txt
├── glue/
│   └── scripts/
│       ├── bronze_to_silver.py     # PySpark: Raw JSON -> Parquet (Silver)
│       └── silver_to_gold.py       # PySpark: Silver Parquet -> Aggregated Gold KPIs
├── analytics/
│   └── README.md                   # Power BI & Athena querying guide
├── docs/
│   └── architecture.md             # In-depth architectural design & diagrams
└── README.md
```

---

## 🚀 Phase-Wise Pipeline Workflow

### Phase 1 — Infrastructure Deployment
Terraform provisions all cloud resources:
- **S3 Data Lake Bucket**: Prefixes `bronze/`, `silver/`, `gold/`, `scripts/`, `athena-results/`.
- **AWS Glue**: Catalog Database (`yelp-bigdata_db_dev`), PySpark Jobs, Workflows, and Crawlers.
- **Amazon Athena**: Workgroup configuration pointing query outputs to S3.
- **CloudWatch**: Centralized log groups (`/aws/glue/yelp-bigdata-dev`).

### Phase 2 — Data Ingestion (GitHub Actions + Python)
1. GitHub Action workflow (`ingest.yml`) executes `ingestion/ingest.py`.
2. Authenticates with Kaggle API (`yelp-dataset/yelp-dataset`).
3. Extracts dataset, removes heavy `photos/` image folder while preserving `photos.json` metadata.
4. Streams `business.json`, `review.json`, `user.json`, `tip.json`, `checkin.json`, and `photos.json` to `s3://<bucket>/bronze/`.
5. Triggers AWS Glue Workflow (`aws glue start-workflow-run`).

### Phase 3 — PySpark ETL Pipeline (AWS Glue)
1. **Bronze -> Silver Job**: Cleans raw JSON data, casts timestamps/datatypes, flattens schemas, and writes partitioned Parquet files to `s3://<bucket>/silver/`.
2. **Silver -> Gold Job**: Calculates aggregated business KPIs (average ratings, regional summaries, user engagement), performs joins, and writes optimized Parquet tables to `s3://<bucket>/gold/`.
3. **Glue Crawler**: Catalogues Gold layer Parquet tables into the Glue Data Catalog.

### Phase 4 — Analytics & Reporting
- Query curated Gold tables using **Amazon Athena**.
- Connect Athena to **Power BI / QuickSight** for interactive executive dashboards.

---

## 🔐 GitHub Secrets Configuration

Before triggering GitHub Action workflows, configure the following secrets in your repository settings (**Settings > Secrets and variables > Actions**):

| Secret Name | Description |
| ----------- | ----------- |
| `AWS_ACCESS_KEY_ID` | AWS Access Key ID |
| `AWS_SECRET_ACCESS_KEY` | AWS Secret Access Key |
| `AWS_SESSION_TOKEN` | AWS Session Token (for AWS Academy Learner Labs) |
| `KAGGLE_USERNAME` | Kaggle Account Username |
| `KAGGLE_KEY` | Kaggle API Key |
| `TF_API_TOKEN` | HCP Terraform API Token (Optional) |

---

## 🛠️ Local Development & Execution

### Deploy Infrastructure
```bash
cd infra
terraform init
terraform plan
terraform apply -auto-approve
```

### Run Data Ingestion Script Locally
```bash
pip install -r ingestion/requirements.txt
export KAGGLE_USERNAME="your-username"
export KAGGLE_KEY="your-key"
export S3_BUCKET_NAME="yelp-bigdata-datalake-us-east-1-dev"
python ingestion/ingest.py
```

---

## 📄 License
MIT License. Created for educational and production-grade data engineering demonstration.
