# Yelp Big Data Architecture

## Current Pipeline Scope (Phase 1 + 2)

```
Developer pushes to main
        │
        ▼
GitHub Actions (terraform-apply.yml)
        │
        ├── Job 1: Terraform Apply
        │       ├── S3 Bronze Bucket  (yelp-bronze-raw-us-east-1)
        │       ├── S3 Silver Bucket  (yelp-silver-clean-us-east-1)
        │       ├── Glue Catalog DB   (yelp_db)
        │       ├── Glue Crawler      (bronze_crawler)
        │       ├── Glue Job          (bronze_to_silver)
        │       └── Glue Workflow     (yelp-bigdata_etl_workflow)
        │
        └── Job 2: Kaggle Ingest → Glue Trigger
                ├── Download: adamamer2001/yelp-complete-open-dataset-2024
                ├── Filter:   exclude photos/ images, keep photos.json
                ├── Upload:   → s3://yelp-bronze-raw-us-east-1/
                └── Trigger:  Glue Workflow → Crawler → bronze_to_silver job
                                                              │
                                                              ▼
                                              s3://yelp-silver-clean-us-east-1/
                                                ├── business/
                                                ├── review/
                                                ├── user/
                                                ├── tip/
                                                └── checkin/
```

## AWS Services Used

| Service | Purpose |
|---|---|
| GitHub Actions | CI/CD — triggers on push to main |
| HCP Terraform | Remote state management |
| Terraform | Infrastructure as Code |
| S3 (Bronze) | Raw Kaggle JSON storage |
| S3 (Silver) | Cleaned Parquet output from Glue |
| Glue Catalog DB | Metadata catalog for Bronze tables |
| Glue Crawler | Discovers Bronze JSON schema |
| Glue Job | PySpark ETL: Bronze → Silver |
| Glue Workflow | Orchestrates Crawler → Job sequence |

## Planned Future Phases

- **Phase 3**: Silver → Gold (aggregated KPIs)
- **Phase 4**: Athena + Power BI dashboard
