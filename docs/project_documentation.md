# Yelp Big Data Pipeline — Complete Project Documentation

---

## 1. High-Level Flow Overview

```
Developer pushes code to GitHub (main branch)
            │
            ▼
   GitHub Actions triggers automatically (3 Sequential Jobs)
            │
    ┌───────┼────────────────────────┐
    │       │                        │
  Job 1   Job 2                    Job 3
Terraform Kaggle Ingest            Silver to Gold ETL
 Apply     (runs after Job 1)      (runs after Job 2)
    │       │                        │
    ▼       ▼                        ▼
 AWS Infra Downloads Yelp           Waits for active Glue Workflow
 Provision Dataset from Kaggle      Executes Silver Crawler
            │                        Executes silver_to_gold PySpark Job
            ▼                        Executes Gold Crawler
     Uploads raw JSON                Verifies Gold S3 outputs
     → S3 Bronze Bucket
            │
            ▼
     Triggers AWS Glue Workflow
            │
     ┌──────┴──────────────────────────┐
     │                                 │
 [Step 1] Bronze Crawler           [Step 2] bronze_to_silver ETL
 catalog raw JSON → yelp_db        writes clean Parquet → Silver S3
                                       │
                                   [Step 3] Silver Crawler
                                   catalog Silver Parquet → yelp_db_silver
                                       │
                                   [Step 4] silver_to_gold ETL
                                   writes BI + ML + RAG → Gold S3
                                       │
                                   [Step 5] Gold Crawler
                                   catalog Gold Parquet → yelp_db_gold
```

---

## 2. Repository Structure — Every File Explained

```
Yelp-Terraform/
│
├── .github/workflows/           ← CI/CD automation (GitHub Actions)
│   ├── terraform-apply.yml      # Main: 3-Job Pipeline (runs on push to main)
│   ├── terraform-plan.yml       # Pull Request check: shows terraform plan
│   └── terraform-destroy.yml    # Manual: teardown all AWS resources
│
├── infra/                       ← Infrastructure as Code (Terraform)
│   ├── backend.tf               # Remote state storage in HCP Terraform
│   ├── provider.tf              # AWS provider setup & default tags
│   ├── versions.tf              # Terraform & AWS provider version constraints
│   ├── variables.tf             # Input variable declarations
│   ├── terraform.tfvars         # Project variable values
│   ├── main.tf                  # Entry point calling s3 & glue modules
│   ├── outputs.tf               # Terraform outputs captured by CI/CD
│   └── modules/
│       ├── s3/                  # Provisions Bronze, Silver, Gold buckets
│       │   ├── main.tf
│       │   ├── variables.tf
│       │   └── outputs.tf
│       └── glue/                # Provisions Glue DBs, Crawlers, Jobs, Workflow
│           ├── main.tf
│           ├── variables.tf
│           └── outputs.tf
│
├── ingestion/                   ← Python data acquisition & orchestration
│   ├── ingest.py                # Kaggle download → S3 Bronze upload → Glue trigger
│   ├── trigger_gold.py          # Workflow waiter, Silver crawler & Gold runner
│   └── requirements.txt         # Ingestion dependencies
│
├── glue/scripts/                ← PySpark ETL Scripts
│   ├── bronze_to_silver.py      # PySpark ETL: raw JSON → clean Silver Parquet
│   └── silver_to_gold.py        # PySpark ETL: Silver Parquet → Gold (BI + ML + RAG)
│
├── docs/                        ← Architecture & Documentation
│   ├── architecture.md          # Medallion Lakehouse visual architecture
│   └── project_documentation.md # Complete project documentation (this document)
│
├── .gitignore
└── README.md
```

---

## 3. GitHub Actions Workflows (`.github/workflows/`)

### `terraform-apply.yml` — The Main Pipeline

**When it runs:** Every push to `main` branch, or manually from GitHub Actions UI.

**What it does:** Master orchestration pipeline running 3 sequential jobs:

#### Job 1 — `1 · Terraform Apply`
- Checks out repository code.
- Authenticates to AWS using GitHub Secrets (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`).
- Authenticates to HCP Terraform using `TF_API_TOKEN`.
- Runs `terraform init` → connects to HCP Terraform remote backend.
- Runs `terraform validate` → syntax check.
- Runs `terraform apply -auto-approve` → provisions all AWS S3 & Glue infrastructure.
- Captures outputs (`bronze_bucket`, `silver_bucket`, `gold_bucket`, `glue_workflow`, `silver_crawler`).
- Uploads Glue ETL scripts from runner to `s3://<bronze_bucket>/scripts/`.

#### Job 2 — `2 · Kaggle Ingest → S3 Bronze → Glue`
- Runs **after Job 1 succeeds** (`needs: terraform`).
- Configures Kaggle API credentials on runner.
- Runs `ingestion/ingest.py` with env vars:
  - `KAGGLE_USERNAME`, `KAGGLE_KEY` → dataset access
  - `BRONZE_BUCKET_NAME` → dynamically from Terraform output
  - `GLUE_WORKFLOW_NAME` → dynamically from Terraform output
  - `TRIGGER_GLUE: "true"` → triggers AWS Glue Workflow async after upload.

#### Job 3 — `3 · Silver to Gold ETL → Gold S3 Verification`
- Runs **after Job 2 succeeds** (`needs: [terraform, ingest]`).
- Executes `ingestion/trigger_gold.py` passing dynamic arguments from Job 1 outputs.
- Waits for any running Glue Workflow executions to complete (prevents file read/write race conditions).
- Verifies presence of all required Silver datasets (`business`, `review`, `user`, `checkin`).
- Triggers and monitors `silver_crawler` to catalog latest Silver schema.
- Triggers and monitors `silver_to_gold` PySpark Glue job.
- Triggers and monitors `gold_crawler` to catalog final Gold tables.
- Verifies Parquet objects generated under `s3://<gold_bucket>/gold/`.

---

### `terraform-plan.yml` — Pull Request Safety Check

**When it runs:** Every Pull Request targeting `main`.

**What it does:**
- Runs `terraform init` + `terraform validate` + `terraform plan`.
- Previews proposed infrastructure changes before merging.
- Read-only check — does not alter AWS infrastructure.

---

### `terraform-destroy.yml` — Teardown Workflow

**When it runs:** Manually only (`workflow_dispatch`).

**What it does:**
- Runs `terraform destroy -auto-approve`.
- Safely deletes all S3 buckets, Glue DBs, Crawlers, Jobs, and Workflows.
- Used to clean up AWS Academy lab sessions.

---

## 4. Infrastructure as Code (`infra/`)

### Module Overview

- **S3 Module (`infra/modules/s3/`)**: Provisions 3 S3 buckets (`yelp-bronze-raw-us-east-1`, `yelp-silver-clean-us-east-1`, `yelp-gold-analytics-us-east-1`) with AES-256 server-side encryption and public access blocks.
- **Glue Module (`infra/modules/glue/`)**: Provisions 3 Catalog Databases (`yelp_db`, `yelp_db_silver`, `yelp_db_gold`), 3 Crawlers, 2 PySpark Jobs, and the Glue Workflow.

### Glue Databases & Crawlers

| Catalog DB Name | Associated Crawler | S3 Source Path | Purpose |
|---|---|---|---|
| `yelp_db` | `yelp-bigdata_bronze_crawler` | `s3://<bronze_bucket>/` | Catalogs raw JSON files (excludes `scripts/`) |
| `yelp_db_silver` | `yelp-bigdata_silver_crawler` | `s3://<silver_bucket>/silver/` | Catalogs cleaned Parquet Silver tables |
| `yelp_db_gold` | `yelp-bigdata_gold_crawler` | `s3://<gold_bucket>/gold/` | Catalogs BI, ML, and RAG Gold Parquet tables |

---

## 5. Ingestion & Orchestration Layer (`ingestion/`)

### `ingest.py` — Data Acquisition Engine
1. Authenticates with Kaggle REST API stream (fallback to Kaggle CLI).
2. Downloads `adamamer2001/yelp-complete-open-dataset-2024` archive.
3. Extracts raw JSON datasets (`business`, `review`, `user`, `tip`, `checkin`, `photos`), skipping images.
4. Uploads raw JSON files to `s3://<bronze_bucket>/`.
5. Triggers AWS Glue Workflow (`start_workflow_run`) and cleans up runner disk space.

### `trigger_gold.py` — Pipeline Monitor & Race Condition Shield
1. **`wait_for_glue_workflow()`**: Polls AWS Glue Workflow until `RUNNING` executions count drops to zero. Fixes file-not-found issues caused by concurrent writes.
2. **`check_silver_all_datasets()`**: Confirms presence of `.parquet` files under `silver/business/`, `silver/review/`, `silver/user/`, and `silver/checkin/`.
3. **`start_and_wait_crawler()`**: Runs Glue Crawlers and blocks until state reaches `READY`.
4. **`start_and_wait_glue_job()`**: Runs Glue PySpark Jobs and polls execution status (`SUCCEEDED` / `FAILED`).
5. **`verify_gold_s3_data()`**: Confirms generated Gold datasets in S3.

---

## 6. PySpark ETL Engine (`glue/scripts/`)

### `bronze_to_silver.py` — Raw JSON to Clean Parquet
- **Source**: `yelp_db` catalog / Bronze S3
- **Target**: `s3://<silver_bucket>/silver/<dataset>/`
- **Key Operations**:
  - `flatten_df()`: Recursively expands nested StructType fields into flat columns.
  - `clean_string_columns()`: Trims strings and converts empty strings to `null`.
  - Column name standardization (`snake_case`).
  - Deduplication on primary keys (`business_id`, `review_id`, `user_id`).
  - Computes `weighted_score` for reviews based on rating and engagement votes (`useful`, `funny`, `cool`).
  - Appends `etl_processed_timestamp`.

---

### `silver_to_gold.py` — Consolidated Silver to Gold Transformation

**Source**: `s3://<silver_bucket>/silver/` (reads with `mergeSchema=True`)  
**Target**: `s3://<gold_bucket>/gold/` divided into `bi/`, `ml/`, and `rag/`

#### Key Transformations & Architectural Improvements
1. **Shared Cached Reads**: Reads `business`, `review`, `user`, and `checkin` once from Silver, caches them in memory, and reuses across BI, ML, and RAG branches.
2. **Checkin Date Explosion (`explode_checkin()`)**: Splits raw Yelp comma-separated checkin date strings (`"2016-04-26 19:49:16, 2016-08-30 18:36:57"`) into individual timestamp rows, enabling accurate time-based aggregation.
3. **Distributed Hours Pivot (`build_dim_business_hours()`)**: Uses Spark `stack()` SQL expression for 100% in-memory distributed pivoting without driver `collect()`.

---

### Gold Layer Output Datasets Detailed

#### 1. BI Star Schema (`gold/bi/`)
- `dim_date`: Calendar dimension table (DateKey, Month, Quarter, Year, DayOfWeek, WeekOfYear).
- `dim_business`: Business dimension table (BusinessID, BusinessName, City, State, PrimaryCategory, Categories).
- `dim_business_hours`: Operating hours per day of week (BusinessID, DayOfWeekNum, DayOfWeek, OpenTime, CloseTime).
- `fact_business`: Business performance summary (ReviewCount, AvgRating, CheckinCount, BusinessHealthScore, CustomerEngagementScore).
- `fact_review_trend`: Time-series review metrics aggregated by date.
- `fact_rating_distribution`: Review counts grouped by star rating values per business.
- `fact_checkin_day`: Aggregated check-in volume by day of week.
- `fact_checkin_hour`: Aggregated check-in volume by hour of day.

#### 2. ML Feature Store (`gold/ml/`)
- `sentiment_features`: Cleaned review text, character/word counts, punctuation ratios, sentiment labels (`positive`, `neutral`, `negative`), partitioned by `review_year`.
- `rating_prediction`: User rating bias, tenure, elite status, business review count, and target star rating for regression models.
- `collaborative_filtering`: Interaction matrix (`user_id`, `business_id`, `stars`, `weighted_score`) for ALS recommendation algorithms.
- `content_based_filtering`: Business metadata, categories, location, and attribute flags for content-based similarity models.
- `customer_segmentation`: Aggregated user features (`avg_stars_given`, `rating_variance`, `distinct_businesses_reviewed`, `user_tenure_years`) for user clustering (RFM/behavioral).

#### 3. RAG Documents (`gold/rag/`)
- `business_documents`: Rich formatted text chunks (`document_id`, `business_id`, `document_text`, `document_type='business'`) combining location, categories, operating hours, and features for LLM context retrieval.
- `review_documents`: Structured review text chunks (`document_id`, `review_id`, `business_id`, `user_id`, `sentiment`, `document_text`, `document_type='review'`) for vector search indexing.

---

## 7. End-to-End Execution Sequence

```
[GitHub Push to main]
       │
       ▼
[Job 1: Terraform Apply]
  ├── Provision S3 Buckets (Bronze, Silver, Gold)
  ├── Provision Glue DBs (yelp_db, yelp_db_silver, yelp_db_gold)
  ├── Provision Glue Crawlers (bronze, silver, gold)
  ├── Provision Glue Jobs (bronze_to_silver, silver_to_gold)
  └── Upload PySpark Scripts to s3://<bronze_bucket>/scripts/
       │
       ▼
[Job 2: Kaggle Ingest]
  ├── Download & Extract Kaggle Yelp Dataset
  ├── Upload JSON files to s3://<bronze_bucket>/
  └── Start AWS Glue Workflow (Async)
           │
           ├── [Step 1] bronze_crawler scans S3 → updates yelp_db
           └── [Step 2] bronze_to_silver job runs → writes Parquet to Silver S3
       │
       ▼
[Job 3: Silver-to-Gold ETL]
  ├── Wait for Glue Workflow completion (wait_for_glue_workflow)
  ├── Verify Silver Datasets present (check_silver_all_datasets)
  ├── Run silver_crawler → updates yelp_db_silver schema
  ├── Run silver_to_gold PySpark Job → writes BI/ML/RAG Parquet to Gold S3
  ├── Run gold_crawler → updates yelp_db_gold schema
  └── Verify Gold S3 Outputs
```

---

## 8. Credentials & Secrets Management

### GitHub Repository Secrets (`Settings → Secrets and variables → Actions`)

| Secret Name | Purpose |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS IAM Access Key |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM Secret Access Key |
| `AWS_SESSION_TOKEN` | AWS IAM Session Token (for temporary lab credentials) |
| `KAGGLE_USERNAME` | Kaggle API account username |
| `KAGGLE_KEY` | Kaggle API account key |
| `TF_API_TOKEN` | HCP Terraform API token for remote state management |

---

## 9. Verification & Querying Gold Data

Once the pipeline completes, you can query the Gold Layer directly in AWS Athena:

```sql
-- Query BI Star Schema Fact Table
SELECT 
    b.BusinessName,
    b.City,
    b.State,
    f.AvgRating,
    f.ReviewCount,
    f.BusinessHealthScore,
    f.HealthStatus
FROM yelp_db_gold.fact_business f
JOIN yelp_db_gold.dim_business b ON f.BusinessID = b.BusinessID
ORDER BY f.BusinessHealthScore DESC
LIMIT 20;
```
