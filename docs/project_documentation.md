# Yelp Big Data Pipeline — Complete Project Documentation

---

## 1. High-Level Flow Overview

```
Developer pushes code to GitHub (main branch)
            │
            ▼
   GitHub Actions triggers automatically
            │
    ┌───────┴────────┐
    │                │
  Job 1            Job 2
Terraform        Kaggle Ingest
  Apply           (runs after
    │              Job 1 done)
    │                │
    ▼                ▼
AWS Infra      Downloads Yelp
Provisioned    Dataset from
               Kaggle API
                    │
                    ▼
              Uploads raw JSON
              → S3 Bronze Bucket
                    │
                    ▼
              Triggers Glue Workflow
                    │
              ┌─────┴──────┐
              │            │
          Glue          Then Glue
         Crawler          Job
        (catalogs        (ETL)
         bronze)          │
              │            ▼
              └──► Clean Parquet
                   → S3 Silver Bucket
```

---

## 2. Repository Structure — Every File Explained

```
Yelp-Terraform/
│
├── .github/workflows/           ← CI/CD automation (GitHub Actions)
│   ├── terraform-apply.yml
│   ├── terraform-plan.yml
│   └── terraform-destroy.yml
│
├── infra/                       ← Infrastructure as Code (Terraform)
│   ├── backend.tf
│   ├── provider.tf
│   ├── versions.tf
│   ├── variables.tf
│   ├── terraform.tfvars
│   ├── main.tf
│   ├── outputs.tf
│   └── modules/
│       ├── s3/
│       │   ├── main.tf
│       │   ├── variables.tf
│       │   └── outputs.tf
│       └── glue/
│           ├── main.tf
│           ├── variables.tf
│           └── outputs.tf
│
├── ingestion/                   ← Python data acquisition
│   ├── ingest.py
│   └── requirements.txt
│
├── glue/scripts/                ← PySpark ETL
│   └── bronze_to_silver.py
│
├── docs/
│   └── architecture.md
│
├── .gitignore
└── README.md
```

---

## 3. GitHub Actions Workflows (`.github/workflows/`)

### `terraform-apply.yml` — The Main Pipeline

**When it runs:** Every push to `main` branch, or manually from GitHub Actions UI.

**What it does:** This is the master orchestration file. It runs two sequential jobs:

#### Job 1 — `Terraform Apply`
- Checks out the code
- Authenticates to AWS using GitHub Secrets (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
- Authenticates to HCP Terraform using `TF_API_TOKEN`
- Runs `terraform init` → connects to HCP Terraform for remote state
- Runs `terraform validate` → syntax check
- Runs `terraform apply -auto-approve` → provisions all AWS infrastructure
- Captures outputs (bucket names, workflow name) and passes them to Job 2

#### Job 2 — `Kaggle Ingest → S3 Bronze → Glue`
- Only runs **after Job 1 succeeds** (`needs: terraform`)
- Installs Python dependencies
- Runs `ingestion/ingest.py` with env vars:
  - `KAGGLE_USERNAME`, `KAGGLE_KEY` → for dataset download
  - `BRONZE_BUCKET_NAME` → dynamically from Terraform output
  - `GLUE_WORKFLOW_NAME` → dynamically from Terraform output
  - `TRIGGER_GLUE: "true"` → automatically triggers ETL after upload

---

### `terraform-plan.yml` — Pull Request Safety Check

**When it runs:** Every Pull Request targeting `main`.

**What it does:**
- Runs `terraform init` + `terraform validate` + `terraform plan`
- Shows what infrastructure changes *would* happen before you merge
- Does **not** apply anything — read-only preview
- Prevents broken Terraform code from reaching main

---

### `terraform-destroy.yml` — Teardown Workflow

**When it runs:** Manually only (`workflow_dispatch`) — you click a button in GitHub Actions.

**What it does:**
- Runs `terraform destroy -auto-approve`
- Deletes all AWS resources: both S3 buckets, Glue DB, Crawler, Job, Workflow
- Used at end of AWS Academy lab session to avoid wasting credits

---

## 4. Infrastructure as Code (`infra/`)

### `backend.tf` — Remote State Storage

```hcl
terraform {
  cloud {
    organization = "cdac-bda-group06"
    workspaces {
      name = "yelp-bigdata-workspace"
    }
  }
}
```

**Purpose:** Instead of storing the Terraform state file (`.tfstate`) locally or in GitHub, it is stored remotely in **HCP Terraform** (app.terraform.io). The state file tracks all AWS resources Terraform has created. Storing it in HCP means:
- Multiple runs don't conflict
- State is never lost even if CI runner is destroyed
- Locking prevents two runs from applying simultaneously

---

### `provider.tf` — AWS Provider Configuration

**Purpose:** Tells Terraform which cloud to talk to (AWS), which region to use (`us-east-1`), and applies default tags (`Project`, `Environment`, `ManagedBy`) to every AWS resource automatically.

---

### `versions.tf` — Terraform & Provider Version Constraints

**Purpose:** Pins the minimum version of Terraform (`>= 1.5.0`) and the AWS provider (`~> 5.0`). This prevents unexpected breakage if a new version introduces breaking changes.

---

### `variables.tf` — Input Variable Declarations

**Purpose:** Declares all configurable inputs the Terraform root module accepts:

| Variable | Type | Purpose |
|---|---|---|
| `aws_region` | string | AWS region (default: `us-east-1`) |
| `project_name` | string | Used as prefix for all resource names |
| `environment` | string | `dev`, `staging`, `prod` |
| `bronze_bucket_name` | string | Name of the raw data S3 bucket |
| `silver_bucket_name` | string | Name of the S3 Silver bucket |
| `glue_service_role_arn` | string | IAM role ARN for Glue (uses AWS Academy LabRole) |

---

### `terraform.tfvars` — Variable Values

**Purpose:** The actual values assigned to the variables declared above. This is the file you edit to change bucket names, regions, or the role ARN.

```hcl
bronze_bucket_name    = "yelp-bronze-raw-us-east-1"
silver_bucket_name    = "yelp-silver-clean-us-east-1"
glue_service_role_arn = "arn:aws:iam::339712764081:role/LabRole"
```

> **Important:** This file is committed to Git. Never put secrets here — secrets go in GitHub Secrets.

---

### `main.tf` — Root Module Entry Point

**Purpose:** Wires the child modules together. Calls the `s3` and `glue` modules, passing outputs from `s3` as inputs to `glue`.

```
main.tf
  ├── calls module "s3"  → creates both buckets
  └── calls module "glue"
          ├── receives bronze_bucket_id from s3 module
          └── receives silver_bucket_id from s3 module
```

---

### `outputs.tf` — Published Values

**Purpose:** Exposes key values after `terraform apply` runs. These are captured in GitHub Actions and passed to the ingest job:
- `bronze_bucket_name` → passed to `ingest.py` as `BRONZE_BUCKET_NAME`
- `glue_workflow_name` → passed to `ingest.py` as `GLUE_WORKFLOW_NAME`

---

## 5. S3 Module (`infra/modules/s3/`)

### `main.tf` — Creates Both S3 Buckets

**Purpose:** Provisions two completely separate S3 buckets:

#### Bronze Bucket (`yelp-bronze-raw-us-east-1`)
- Stores raw JSON files exactly as downloaded from Kaggle
- Also stores the Glue ETL script at `scripts/bronze_to_silver.py`
- Server-side encryption (AES-256) enabled
- All public access blocked

#### Silver Bucket (`yelp-silver-clean-us-east-1`)
- Stores cleaned, flattened Parquet files output by the Glue job
- Organized as `/<table_name>/` (e.g. `/business/`, `/review/`)
- Server-side encryption (AES-256) enabled
- All public access blocked

The module also uploads `bronze_to_silver.py` from `glue/scripts/` to `s3://yelp-bronze-raw-us-east-1/scripts/`. Glue reads the script from this S3 path.

### `variables.tf` / `outputs.tf`
Accepts bucket names, outputs bucket IDs and ARNs for use by the Glue module.

---

## 6. Glue Module (`infra/modules/glue/`)

### `main.tf` — Creates the Entire ETL Infrastructure

**Purpose:** Provisions all AWS Glue resources in the correct dependency order.

#### Glue Catalog Database (`yelp_db`)
- A logical container in the AWS Glue Data Catalog
- Stores metadata (schema, location) of all tables
- Think of it as the "database name" when querying with Athena

#### IAM Role (conditional)
- If `glue_service_role_arn` is empty → Terraform creates a new role with S3 + Glue permissions
- If `glue_service_role_arn` is set (LabRole) → Terraform **skips** creating a role and uses the existing one
- This is the AWS Academy compatibility pattern

#### Glue Crawler (`yelp-bigdata_bronze_crawler`)
- Points to `s3://yelp-bronze-raw-us-east-1/` (excludes `scripts/` prefix)
- When run: scans the JSON files, infers schemas, and registers tables in `yelp_db`
- Table names created: `yelp_academic_dataset_business_json`, `yelp_academic_dataset_review_json`, etc.
- These exact table names are what `bronze_to_silver.py` reads from

#### Glue Job (`yelp-bigdata_bronze_to_silver`)
- Glue version 4.0, Spark engine, G.1X workers (1 vCPU, 8 GB RAM each), 2 workers
- Script location: `s3://yelp-bronze-raw-us-east-1/scripts/bronze_to_silver.py`
- Job arguments: `--S3_BUCKET` = silver bucket name, `--DATABASE_NAME` = `yelp_db`

#### Glue Workflow (`yelp-bigdata_etl_workflow`)
- Triggered by `ingest.py` using `boto3 glue.start_workflow_run()`
- Chains Crawler → Job sequentially

#### Glue Triggers (chained)

```
Workflow Started (ON_DEMAND)
        │
  Trigger 1: start_crawler      ← fires immediately
        │
  Glue Crawler runs
        │ (SUCCEEDED)
  Trigger 2: start_bronze_to_silver  ← fires after crawler
        │
  Glue Job runs (bronze_to_silver.py)
```

---

## 7. Ingestion Layer (`ingestion/`)

### `requirements.txt`

| Package | Purpose |
|---|---|
| `kaggle` | Kaggle Python API client |
| `boto3` | AWS SDK for Python (S3 upload, Glue trigger) |
| `requests` | HTTP client |
| `tqdm` | Download progress bars |

---

### `ingest.py` — The Data Acquisition Engine

**Purpose:** A Python script that downloads the Yelp dataset from Kaggle and uploads it to S3 Bronze, then triggers the Glue ETL Workflow.

**Step-by-step:**

| Step | What happens |
|---|---|
| 1 | Reads `KAGGLE_USERNAME` + `KAGGLE_KEY` env vars, writes `~/.kaggle/kaggle.json` |
| 2 | Downloads `adamamer2001/yelp-complete-open-dataset-2024` (~11 GB ZIP) to temp dir |
| 3 | Extracts ZIP, skipping `photos/` images — keeps only JSON files |
| 4 | Uploads 6 JSON files to `s3://yelp-bronze-raw-us-east-1/<filename>` |
| 5 | Calls `boto3 glue.start_workflow_run(Name=GLUE_WORKFLOW_NAME)` |
| 6 | Deletes temp dir |

**Files uploaded:**
```
yelp_academic_dataset_business.json
yelp_academic_dataset_review.json
yelp_academic_dataset_user.json
yelp_academic_dataset_tip.json
yelp_academic_dataset_checkin.json
photos.json
```

---

## 8. Glue ETL Script (`glue/scripts/`)

### `bronze_to_silver.py` — PySpark ETL: Raw JSON → Clean Parquet

**Reads from:** Glue Data Catalog (`yelp_db`) via `create_dynamic_frame.from_catalog()`
**Writes to:** `s3://yelp-silver-clean-us-east-1/<table>/` (Parquet, Snappy compressed)

#### Helper Functions

| Function | What it does |
|---|---|
| `log(msg)` | Prefixed print for Glue CloudWatch logs |
| `standardize_column_name(name)` | Strips spaces, special chars, converts to `snake_case` |
| `flatten_df(df)` | Recursively expands nested StructType columns into flat columns |
| `clean_string_columns(df)` | Trims whitespace, converts empty strings to `null` |

#### Per-Table Processing

| Table | Key Transformations |
|---|---|
| **business** | Filter null `business_id`, dedup, cast `stars`/`review_count`/lat/long/`is_open`, flatten nested structs |
| **review** | Filter null `review_id`, dedup, parse timestamp, extract date & time, cast stars, fill `useful/funny/cool` with 0, compute `weighted_score` (0–10) |
| **user** | Filter null `user_id`, dedup, parse `yelping_since` as full timestamp |
| **checkin** | Filter null `business_id`, dedup |
| **tip** | Parse `date` timestamp, cast `compliment_count`, filter nulls, add `text_length` |
| **all** | Add `etl_processed_timestamp` column, write Parquet with Snappy compression |

#### `weighted_score` formula (review table)
```
weighted_score = LEAST(
    (0.7 × (stars / 5) + 0.3 × ((useful + funny + cool) / 30)) × 10,
    10
)
```
Capped at 10 to handle edge cases where engagement votes are very high.

---

## 9. Supporting Files

### `.gitignore`
Excludes: `.terraform/`, `*.tfstate`, `*.tfstate.backup`, `temp_yelp_data/`, `*.zip`, Python cache, OS metadata files.

### `docs/architecture.md`
Visual pipeline diagram and AWS service mapping. Used for project report.

### `README.md`
Project overview, structure, secrets reference, S3 layout after pipeline runs, and teardown instructions.

---

## 10. End-to-End Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│  git push to main                                                │
└───────────────────────────┬──────────────────────────────────────┘
                            │
               ┌────────────▼────────────┐
               │   terraform-apply.yml   │
               │   (GitHub Actions)      │
               └────────────┬────────────┘
                            │
             ┌──────────────▼──────────────┐
             │         JOB 1               │
             │      Terraform Apply        │
             │                             │
             │  State ←→ HCP Terraform     │
             │  Creates on AWS:            │
             │  ┌─────────────────────┐    │
             │  │ S3 Bronze Bucket    │    │
             │  │ S3 Silver Bucket    │    │
             │  │ Glue Catalog DB     │    │
             │  │ Glue Crawler        │    │
             │  │ Glue Job            │    │
             │  │ Glue Workflow       │    │
             │  │ ETL script → S3     │    │
             │  └─────────────────────┘    │
             └──────────────┬──────────────┘
                            │ outputs: bucket + workflow names
             ┌──────────────▼──────────────┐
             │         JOB 2               │
             │     ingest.py runs          │
             │                             │
             │  Kaggle API                 │
             │  ↓ download ZIP (~11 GB)    │
             │  ↓ extract, skip photos/    │
             │  ↓ upload 6 JSON files      │
             │    → S3 Bronze              │
             │  ↓ trigger Glue Workflow    │
             └──────────────┬──────────────┘
                            │
             ┌──────────────▼──────────────┐
             │    AWS Glue Workflow         │
             │                             │
             │  [1] Glue Crawler           │
             │      scans Bronze S3        │
             │      → registers tables     │
             │        in yelp_db           │
             │                             │
             │  [2] Glue Job               │
             │      bronze_to_silver.py    │
             │      reads yelp_db tables   │
             │      → flatten / clean      │
             │      → write Parquet        │
             │        → S3 Silver          │
             └──────────────┬──────────────┘
                            │
             ┌──────────────▼──────────────┐
             │   S3 Silver Bucket          │
             │   yelp-silver-clean-*       │
             │                             │
             │   /business/  *.parquet     │
             │   /review/    *.parquet     │
             │   /user/      *.parquet     │
             │   /tip/       *.parquet     │
             │   /checkin/   *.parquet     │
             └─────────────────────────────┘
```

---

## 11. GitHub Secrets & HCP Variables Reference

### GitHub Secrets (`Settings → Secrets → Actions`)

| Secret | Used By |
|---|---|
| `AWS_ACCESS_KEY_ID` | GitHub Actions: AWS auth |
| `AWS_SECRET_ACCESS_KEY` | GitHub Actions: AWS auth |
| `AWS_SESSION_TOKEN` | GitHub Actions: AWS auth |
| `KAGGLE_USERNAME` | `ingest.py` |
| `KAGGLE_KEY` | `ingest.py` |
| `TF_API_TOKEN` | Terraform CLI → HCP auth |

### HCP Terraform Workspace Variables (`Environment variable` type)

| Key | Type |
|---|---|
| `AWS_ACCESS_KEY_ID` | Environment variable ✅ |
| `AWS_SECRET_ACCESS_KEY` | Environment variable ✅ |
| `AWS_SESSION_TOKEN` | Environment variable ✅ |

> ⚠️ **AWS Academy:** Update all three AWS credentials (GitHub Secrets + HCP Variables) every time you start a new lab session — they expire when the session ends.
