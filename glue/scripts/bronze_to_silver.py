import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, year, month, when, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'DATABASE_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['S3_BUCKET']
database_name = args['DATABASE_NAME']

bronze_base_path = f"s3://{bucket_name}/bronze"
silver_base_path = f"s3://{bucket_name}/silver"

# 1. Process Business Data
print("Processing Business data (Bronze -> Silver)...")
business_df = spark.read.json(f"{bronze_base_path}/business.json")

clean_business_df = business_df.filter(col("business_id").isNotNull()) \
    .withColumn("is_open", col("is_open").cast("integer")) \
    .withColumn("stars", col("stars").cast("double")) \
    .withColumn("review_count", col("review_count").cast("integer")) \
    .withColumn("ingested_at", current_timestamp())

clean_business_df.write \
    .mode("overwrite") \
    .parquet(f"{silver_base_path}/business")

# 2. Process Review Data
print("Processing Review data (Bronze -> Silver)...")
review_df = spark.read.json(f"{bronze_base_path}/review.json")

clean_review_df = review_df.filter(col("review_id").isNotNull()) \
    .withColumn("review_date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("year", year(col("review_date"))) \
    .withColumn("month", month(col("review_date"))) \
    .withColumn("stars", col("stars").cast("double")) \
    .withColumn("useful", col("useful").cast("integer")) \
    .withColumn("funny", col("funny").cast("integer")) \
    .withColumn("cool", col("cool").cast("integer")) \
    .withColumn("ingested_at", current_timestamp())

clean_review_df.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(f"{silver_base_path}/review")

# 3. Process User Data
print("Processing User data (Bronze -> Silver)...")
user_df = spark.read.json(f"{bronze_base_path}/user.json")

clean_user_df = user_df.filter(col("user_id").isNotNull()) \
    .withColumn("yelping_since", to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("review_count", col("review_count").cast("integer")) \
    .withColumn("average_stars", col("average_stars").cast("double")) \
    .withColumn("ingested_at", current_timestamp())

clean_user_df.write \
    .mode("overwrite") \
    .parquet(f"{silver_base_path}/user")

# 4. Process Tip Data
print("Processing Tip data (Bronze -> Silver)...")
tip_df = spark.read.json(f"{bronze_base_path}/tip.json")

clean_tip_df = tip_df.filter(col("business_id").isNotNull()) \
    .withColumn("tip_date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("compliment_count", col("compliment_count").cast("integer")) \
    .withColumn("ingested_at", current_timestamp())

clean_tip_df.write \
    .mode("overwrite") \
    .parquet(f"{silver_base_path}/tip")

# 5. Process Checkin Data
print("Processing Checkin data (Bronze -> Silver)...")
checkin_df = spark.read.json(f"{bronze_base_path}/checkin.json")

clean_checkin_df = checkin_df.filter(col("business_id").isNotNull()) \
    .withColumn("ingested_at", current_timestamp())

clean_checkin_df.write \
    .mode("overwrite") \
    .parquet(f"{silver_base_path}/checkin")

print("Bronze to Silver ETL pipeline completed successfully!")
job.commit()
