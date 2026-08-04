# Yelp Big Data Architecture — Medallion Data Lakehouse

## Full Pipeline Architecture (Bronze → Silver → Gold)

```
Developer pushes code to main
        │
        ▼
GitHub Actions (Deploy & Ingest Pipeline)
        │
        ├── Job 1: Terraform Apply
        │       ├── S3 Bronze Bucket  (yelp-bronze-raw-us-east-1)
        │       ├── S3 Silver Bucket  (yelp-silver-clean-us-east-1)
        │       ├── S3 Gold Bucket    (yelp-gold-analytics-us-east-1)
        │       ├── Glue Database     (yelp_db - Bronze JSON)
        │       ├── Glue Gold DB      (yelp_db_gold - BI/ML/RAG Parquet)
        │       ├── Glue Crawlers     (bronze_crawler, gold_crawler)
        │       ├── Glue Jobs         (bronze_to_silver, silver_to_gold)
        │       └── Glue Workflow     (yelp-bigdata_etl_workflow)
        │
        └── Job 2: Kaggle Ingest → S3 Bronze → Trigger Glue Workflow
                ├── Download: adamamer2001/yelp-complete-open-dataset-2024
                ├── Upload:   JSON datasets → s3://yelp-bronze-raw-us-east-1/
                └── Trigger:  AWS Glue Workflow
                                   │
                                   ▼
                    [Step 1] Bronze Crawler (catalog raw JSON → yelp_db)
                                   │
                                   ▼
                    [Step 2] bronze_to_silver PySpark ETL Job
                                   │  writes Parquet → Silver S3
                                   ▼
                    [Step 3] silver_to_gold PySpark ETL Job
                                   │  writes BI + ML + RAG → Gold S3
                                   ▼
                    [Step 4] Gold Crawler (catalog Gold Parquet → yelp_db_gold)
```

## Gold Layer Architecture

| Branch | Output Path | Format | Tables Generated |
|---|---|---|---|
| **BI Star Schema** | `s3://yelp-gold-analytics-us-east-1/gold/bi/` | Parquet (Snappy) | `dim_date`, `dim_business`, `fact_business`, `fact_review_trend`, `fact_rating_distribution`, `dim_business_hours`, `fact_checkin_day`, `fact_checkin_hour` |
| **ML Feature Store** | `s3://yelp-gold-analytics-us-east-1/gold/ml/` | Parquet (Snappy) | `business_features`, `user_features`, `interaction_matrix` |
| **RAG Documents** | `s3://yelp-gold-analytics-us-east-1/gold/rag/` | Parquet (Snappy) | `business_context`, `high_utility_reviews` |
