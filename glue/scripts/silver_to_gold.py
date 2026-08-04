import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, count, sum, round, desc, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'DATABASE_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['S3_BUCKET']
database_name = args['DATABASE_NAME']

silver_base_path = f"s3://{bucket_name}/silver"
gold_base_path = f"s3://{bucket_name}/gold"

# Load Silver Data
business_df = spark.read.parquet(f"{silver_base_path}/business")
review_df = spark.read.parquet(f"{silver_base_path}/review")
user_df = spark.read.parquet(f"{silver_base_path}/user")

# 1. Gold Aggregation: Business KPI Analytics
print("Computing Gold layer: Business KPI Aggregations...")
business_review_kpi = review_df.groupBy("business_id").agg(
    count("review_id").alias("total_reviews_calc"),
    round(avg("stars"), 2).alias("avg_star_rating"),
    sum("useful").alias("total_useful_votes"),
    sum("funny").alias("total_funny_votes"),
    sum("cool").alias("total_cool_votes")
)

gold_business_summary = business_df.join(business_review_kpi, "business_id", "left") \
    .select(
        col("business_id"),
        col("name").alias("business_name"),
        col("city"),
        col("state"),
        col("postal_code"),
        col("stars").alias("official_stars"),
        col("avg_star_rating"),
        col("review_count"),
        col("total_reviews_calc"),
        col("categories"),
        col("is_open"),
        col("total_useful_votes"),
        current_timestamp().alias("processed_at")
    )

gold_business_summary.write \
    .mode("overwrite") \
    .parquet(f"{gold_base_path}/business_kpi_summary")

# 2. Gold Aggregation: City & State Performance Metrics
print("Computing Gold layer: Regional Business Analytics...")
gold_regional_summary = gold_business_summary.groupBy("state", "city").agg(
    count("business_id").alias("total_businesses"),
    round(avg("official_stars"), 2).alias("regional_avg_rating"),
    sum("total_reviews_calc").alias("regional_total_reviews")
).orderBy(desc("total_businesses"))

gold_regional_summary.write \
    .mode("overwrite") \
    .parquet(f"{gold_base_path}/regional_analytics")

# 3. Gold Aggregation: User Engagement & Influencer Profiles
print("Computing Gold layer: User Analytics...")
gold_user_summary = user_df.select(
    col("user_id"),
    col("name"),
    col("review_count"),
    col("yelping_since"),
    col("average_stars"),
    col("fans"),
    current_timestamp().alias("processed_at")
).filter(col("review_count") > 10)

gold_user_summary.write \
    .mode("overwrite") \
    .parquet(f"{gold_base_path}/user_influencers")

print("Silver to Gold ETL pipeline completed successfully!")
job.commit()
