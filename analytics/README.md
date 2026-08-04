# Analytics & Power BI Integration Guide

This directory contains instructions and sample SQL queries to query the curated Yelp Gold dataset via **Amazon Athena** and build business dashboards in **Power BI**.

---

## 1. Athena SQL Querying

Once the AWS Glue Crawler scans the `gold/` layer in S3, tables will be available in the Glue Data Catalog (`yelp-bigdata_db_dev`).

### Sample Athena Queries

#### Top 10 Cities by Business Count & Average Rating
```sql
SELECT 
    state, 
    city, 
    total_businesses, 
    regional_avg_rating, 
    regional_total_reviews
FROM "yelp-bigdata_db_dev"."regional_analytics"
ORDER BY total_businesses DESC
LIMIT 10;
```

#### High-Volume Businesses with Calculated Ratings vs Official Ratings
```sql
SELECT 
    business_name, 
    city, 
    state, 
    categories, 
    official_stars, 
    avg_star_rating, 
    total_reviews_calc, 
    total_useful_votes
FROM "yelp-bigdata_db_dev"."business_kpi_summary"
WHERE total_reviews_calc > 50
ORDER BY total_useful_votes DESC
LIMIT 25;
```

---

## 2. Power BI Connection Instructions

1. Open **Power BI Desktop**.
2. Click **Get Data** -> **More...** -> Search for **Amazon Athena**.
3. Configure Connection Details:
   - **Server**: `athena.us-east-1.amazonaws.com` (or your target AWS region)
   - **Workgroup**: `yelp-bigdata_workgroup_dev`
   - **Data Source**: `AWSDataCatalog`
   - **S3 Output Location**: `s3://<your-bucket-name>/athena-results/`
4. Select **DirectQuery** or **Import** mode.
5. Authenticate using your AWS Access Key ID and Secret Access Key.
6. Select tables: `business_kpi_summary`, `regional_analytics`, and `user_influencers` to create visualizations.
