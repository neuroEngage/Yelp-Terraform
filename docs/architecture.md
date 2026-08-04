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
        │       ├── Glue Bronze DB    (yelp_db - raw JSON catalog)
        │       ├── Glue Silver DB    (yelp_db_silver - clean Parquet catalog)
        │       ├── Glue Gold DB      (yelp_db_gold - BI/ML/RAG Parquet catalog)
        │       ├── Glue Crawlers     (bronze_crawler, silver_crawler, gold_crawler)
        │       ├── Glue Jobs         (bronze_to_silver, silver_to_gold)
        │       └── Glue Workflow     (yelp-bigdata_etl_workflow)
        │
        ├── Job 2: Kaggle Ingest → S3 Bronze → Trigger Glue Workflow
        │       ├── Download: adamamer2001/yelp-complete-open-dataset-2024
        │       ├── Upload:   JSON datasets → s3://yelp-bronze-raw-us-east-1/
        │       └── Trigger:  AWS Glue Workflow (starts background execution)
        │                         │
        │                         ▼
        │             [Step 1] Bronze Crawler (catalog raw JSON → yelp_db)
        │                         │
        │                         ▼
        │             [Step 2] bronze_to_silver PySpark ETL Job
        │                         │  writes Parquet → s3://<silver_bucket>/silver/
        │                         ▼
        │             [Step 3] Silver Crawler (catalog Silver Parquet → yelp_db_silver)
        │                         │
        │                         ▼
        │             [Step 4] silver_to_gold PySpark ETL Job
        │                         │  writes BI + ML + RAG → s3://<gold_bucket>/gold/
        │                         ▼
        │             [Step 5] Gold Crawler (catalog Gold Parquet → yelp_db_gold)
        │
        └── Job 3: Silver to Gold ETL & Verification (trigger_gold.py)
                ├── Wait:     Polls Glue Workflow until all active runs finish (prevents race condition)
                ├── Verify:   Confirms all Silver datasets exist (business, review, user, checkin)
                ├── Crawl:    Executes silver_crawler to update Glue Catalog schema
                ├── Run:      Executes silver_to_gold PySpark ETL job (BI + ML + RAG)
                ├── Crawl:    Executes gold_crawler to update Gold Glue Catalog schema
                └── Validate: Confirms Parquet files landed in Gold S3 bucket
```

## Medallion Layers & Schema Architecture

| Layer | Bucket / Storage Path | Data Format | Glue Catalog DB | Description |
|---|---|---|---|---|
| **Bronze** | `s3://yelp-bronze-raw-us-east-1/` | Raw JSON | `yelp_db` | Direct raw Kaggle open dataset export (Business, Review, User, Checkin, Tip) |
| **Silver** | `s3://yelp-silver-clean-us-east-1/silver/` | Parquet (Snappy) | `yelp_db_silver` | Flattened struct attributes, sanitized column names, trimmed strings, deduplicated primary keys |
| **Gold** | `s3://yelp-gold-analytics-us-east-1/gold/` | Parquet (Snappy) | `yelp_db_gold` | Production analytics datasets for BI, ML feature store, and RAG document vectors |

## Gold Layer Output Datasets

| Branch | Output Subpath | Tables / Datasets Generated | Purpose & Target Application |
|---|---|---|---|
| **BI Star Schema** | `gold/bi/` | `dim_date`, `dim_business`, `dim_business_hours`, `fact_business`, `fact_review_trend`, `fact_rating_distribution`, `fact_checkin_day`, `fact_checkin_hour` | Power BI, Amazon QuickSight, Athena SQL analytics dashboards |
| **ML Feature Store** | `gold/ml/` | `sentiment_features`, `rating_prediction`, `collaborative_filtering`, `content_based_filtering`, `customer_segmentation` | ML training pipelines (NLP sentiment classification, rating regression, ALS recommender system, customer RFM/behavioral clustering) |
| **RAG Documents** | `gold/rag/` | `business_documents`, `review_documents` | Vector database ingestion (Pinecone, OpenSearch, LangChain, LlamaIndex) for LLM Question Answering & Retrieval-Augmented Generation |
