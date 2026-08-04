"""
================================================================================
OPTIMIZED — FINAL MERGED ETL GLUE JOB SCRIPT - Silver to Gold Layer Transformation
(BI star-schema branch + ML feature-engineering branch + RAG document branch)
================================================================================
Purpose : Transform Yelp data from Silver Layer to Gold Layer
Source  : s3://<SILVER_BUCKET>/silver/
Target  : s3://<GOLD_BUCKET>/gold/
             ├── bi/    (Star-schema BI tables)
             ├── ml/    (ML feature tables)
             └── rag/   (RAG documents)
================================================================================
"""

import sys
import math
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, trim, lower, when, lit, coalesce, size, split, length,
    regexp_replace, current_date, datediff,
    year, month, quarter, weekofyear, dayofweek, round as spark_round,
    avg, stddev, broadcast, countDistinct, least,
)
from pyspark.sql.types import *
import pyspark.sql.functions as F

# ================================================================================
# SETUP & RESOLVE GLUE ARGUMENTS
# ================================================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_BUCKET", "GOLD_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --- AQE / shuffle tuning ---
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "64")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ================================================================================
# CONFIGURATION
# ================================================================================
SILVER_BUCKET = args["SILVER_BUCKET"]
GOLD_BUCKET   = args["GOLD_BUCKET"]

SILVER_PATH = f"s3://{SILVER_BUCKET}/silver"
GOLD_ROOT   = f"s3://{GOLD_BUCKET}/gold"

GOLD_PATH_BI  = f"{GOLD_ROOT}/bi"
GOLD_PATH_ML  = f"{GOLD_ROOT}/ml"
GOLD_PATH_RAG = f"{GOLD_ROOT}/rag"

logger.info("=" * 80)
logger.info(f"GLUE ETL JOB: Silver to Gold Transformation (BI + ML + RAG)")
logger.info(f"Silver Source: {SILVER_PATH}")
logger.info(f"Gold Target  : {GOLD_ROOT}")
logger.info("=" * 80)

LOG1P_10000 = math.log1p(10000)
LOG1P_50000 = math.log1p(50000)

ATTRIBUTE_COLUMN_CANDIDATES = {
    "wifi": ["attributes_wifi"],
    "parking": ["attributes_businessparking"],
    "alcohol": ["attributes_alcohol"],
    "takeout": ["attributes_restaurantstakeout"],
    "delivery": ["attributes_restaurantsdelivery"],
    "good_for_kids": ["attributes_goodforkids"],
    "noise_level": ["attributes_noiselevel"],
    "attire": ["attributes_restaurantsattire"],
    "reservations": ["attributes_restaurantsreservations"],
    "credit_cards": ["attributes_businessacceptscreditcards"],
    "price_range": ["attributes_restaurantspricerange2", "attributes_pricerange2"],
    "by_appointment_only": ["attributes_byappointmentonly"],
    "outdoor_seating": ["attributes_outdoorseating"],
}


def log(tag, message):
    print(f"[{tag}_GOLD] {message}")


def first_existing_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def safe_attribute_column(df, attribute_key, alias, default=None):
    candidates = ATTRIBUTE_COLUMN_CANDIDATES.get(attribute_key, [])
    found = first_existing_column(df, candidates)
    if found:
        return col(found).alias(alias)
    return lit(default).alias(alias)


def write_gold(tag, df, root, subpath, partition_by=None, num_output_files=None):
    """Writes a gold table."""
    output_path = f"{root}/{subpath}/"
    if num_output_files:
        df = df.coalesce(num_output_files)
    log(tag, f"Writing to {output_path}" + (f" (partitioned by {partition_by})" if partition_by else ""))
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(output_path)
    log(tag, f"'{subpath}' written.")


# =======================================================================
# Stage 0 — Read Silver ONCE, shared by BI branch and ML+RAG branch
# =======================================================================

def read_silver_table(spark, dataset_name):
    path = f"{SILVER_PATH}/{dataset_name}/"
    log("READ", f"Reading Silver dataset from: {path}")
    try:
        df = spark.read.parquet(path)
        log("READ", f"Successfully read '{dataset_name}' from {path}")
        return df
    except Exception as e:
        alt_path = f"s3://{SILVER_BUCKET}/{dataset_name}/"
        log("READ_WARN", f"Path {path} not found. Trying fallback path: {alt_path}")
        try:
            df = spark.read.parquet(alt_path)
            log("READ", f"Successfully read '{dataset_name}' from fallback {alt_path}")
            return df
        except Exception as e2:
            log("READ_ERROR", f"Failed to read '{dataset_name}' from both {path} and {alt_path}: {e2}")
            raise e2

def read_all_silver_shared():
    business_df = read_silver_table(spark, "business")
    review_df   = read_silver_table(spark, "review")
    user_df     = read_silver_table(spark, "user")
    checkin_df  = read_silver_table(spark, "checkin")

    business_df = business_df.cache()
    review_df   = review_df.cache()
    user_df     = user_df.cache()
    checkin_df  = checkin_df.cache()
    return business_df, review_df, user_df, checkin_df


# =======================================================================
# Stage — Defensive cleaning
# =======================================================================

def clean_business(business_df):
    return business_df.filter(col("business_id").isNotNull()).dropDuplicates(["business_id"])


def clean_review(review_df):
    return (
        review_df
        .filter(col("review_id").isNotNull() & col("business_id").isNotNull() & col("user_id").isNotNull())
        .dropDuplicates(["review_id"])
    )


def clean_user(user_df):
    return user_df.filter(col("user_id").isNotNull()).dropDuplicates(["user_id"])


# =======================================================================
# Helper for dim_business_hours
# =======================================================================

HOURS_DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

def build_dim_business_hours(business_df):
    hour_cols_present = [f"hours_{d}" for d in HOURS_DAYS if f"hours_{d}" in business_df.columns]
    if not hour_cols_present:
        schema = StructType([
            StructField("BusinessHoursKey", StringType()),
            StructField("BusinessID", StringType()),
            StructField("DayOfWeek", StringType()),
            StructField("OpenTime", StringType()),
            StructField("CloseTime", StringType()),
            StructField("IsOpenThatDay", IntegerType()),
        ])
        return spark.createDataFrame([], schema)

    select_exprs = [col("business_id").alias("BusinessID")]
    stack_args = []
    for d in HOURS_DAYS:
        col_name = f"hours_{d}"
        if col_name in business_df.columns:
            stack_args.append(f"'{d.capitalize()}', {col_name}")
        else:
            stack_args.append(f"'{d.capitalize()}', CAST(NULL AS STRING)")

    stack_expr = f"stack({len(HOURS_DAYS)}, " + ", ".join(stack_args) + ") as (DayOfWeek, HoursRaw)"

    stacked_df = (
        business_df
        .select(col("business_id").alias("BusinessID"), F.expr(stack_expr))
        .filter(col("HoursRaw").isNotNull() & (trim(col("HoursRaw")) != ""))
    )

    split_hours = split(col("HoursRaw"), "-")
    return (
        stacked_df
        .withColumn("OpenTime", trim(split_hours.getItem(0)))
        .withColumn("CloseTime", trim(split_hours.getItem(1)))
        .withColumn("IsOpenThatDay", lit(1))
        .withColumn("BusinessHoursKey", F.concat_ws("_", col("BusinessID"), col("DayOfWeek")))
        .select("BusinessHoursKey", "BusinessID", "DayOfWeek", "OpenTime", "CloseTime", "IsOpenThatDay")
    )


# =======================================================================
# ML + RAG BRANCH
# =======================================================================

def run_ml_and_rag(business_df, review_df, user_df):
    logger.info("Starting ML + RAG Branch...")

    # ---- 1. Business Features ----
    category_count_expr = size(split(col("categories"), ","))

    business_features_df = business_df.select(
        col("business_id"),
        col("stars").alias("business_stars"),
        col("review_count").alias("business_review_count"),
        col("is_open"),
        col("latitude"),
        col("longitude"),
        col("state"),
        col("city"),
        safe_attribute_column(business_df, "price_range", "price_range_raw"),
        safe_attribute_column(business_df, "alcohol", "alcohol_raw"),
        safe_attribute_column(business_df, "wifi", "wifi_raw"),
        safe_attribute_column(business_df, "parking", "parking_raw"),
        safe_attribute_column(business_df, "takeout", "takeout_raw"),
        safe_attribute_column(business_df, "delivery", "delivery_raw"),
        safe_attribute_column(business_df, "good_for_kids", "good_for_kids_raw"),
        safe_attribute_column(business_df, "outdoor_seating", "outdoor_seating_raw"),
        safe_attribute_column(business_df, "reservations", "reservations_raw"),
        safe_attribute_column(business_df, "attire", "attire_raw"),
        safe_attribute_column(business_df, "by_appointment_only", "by_appointment_only_raw"),
        category_count_expr.alias("num_categories"),
        col("categories")
    ).withColumn(
        "price_range",
        when(col("price_range_raw").isin("1", "2", "3", "4"), col("price_range_raw").cast(IntegerType())).otherwise(lit(None))
    ).withColumn(
        "has_alcohol",
        when(col("alcohol_raw").isNotNull() & ~lower(col("alcohol_raw")).contains("none"), 1).otherwise(0)
    ).withColumn(
        "has_wifi",
        when(col("wifi_raw").isNotNull() & ~lower(col("wifi_raw")).contains("no"), 1).otherwise(0)
    ).withColumn(
        "has_parking",
        when(col("parking_raw").isNotNull() & lower(col("parking_raw")).contains("true"), 1).otherwise(0)
    ).withColumn(
        "has_takeout",
        when(col("takeout_raw").isNotNull() & lower(col("takeout_raw")).contains("true"), 1).otherwise(0)
    ).withColumn(
        "has_delivery",
        when(col("delivery_raw").isNotNull() & lower(col("delivery_raw")).contains("true"), 1).otherwise(0)
    ).withColumn(
        "good_for_kids",
        when(col("good_for_kids_raw").isNotNull() & lower(col("good_for_kids_raw")).contains("true"), 1).otherwise(0)
    ).withColumn(
        "has_outdoor_seating",
        when(col("outdoor_seating_raw").isNotNull() & lower(col("outdoor_seating_raw")).contains("true"), 1).otherwise(0)
    ).withColumn(
        "takes_reservations",
        when(col("reservations_raw").isNotNull() & lower(col("reservations_raw")).contains("true"), 1).otherwise(0)
    ).withColumn(
        "by_appointment_only",
        when(col("by_appointment_only_raw").isNotNull() & lower(col("by_appointment_only_raw")).contains("true"), 1).otherwise(0)
    ).withColumn(
        "log_review_count",
        F.log1p(coalesce(col("business_review_count"), lit(0)))
    ).withColumn(
        "review_count_norm",
        spark_round(col("log_review_count") / lit(LOG1P_10000), 4)
    ).drop(
        "price_range_raw", "alcohol_raw", "wifi_raw", "parking_raw", "takeout_raw",
        "delivery_raw", "good_for_kids_raw", "outdoor_seating_raw", "reservations_raw",
        "attire_raw", "by_appointment_only_raw"
    )

    write_gold("ML", business_features_df, GOLD_PATH_ML, "business_features", num_output_files=10)

    # ---- 2. User Features ----
    user_features_df = user_df.select(
        col("user_id"),
        col("review_count").alias("user_review_count"),
        col("yelping_since"),
        col("useful").alias("user_useful"),
        col("funny").alias("user_funny"),
        col("cool").alias("user_cool"),
        col("fans"),
        col("average_stars").alias("user_avg_stars"),
        coalesce(col("compliment_hot"), lit(0)).alias("compliment_hot"),
        coalesce(col("compliment_more"), lit(0)).alias("compliment_more"),
        coalesce(col("compliment_profile"), lit(0)).alias("compliment_profile"),
        coalesce(col("compliment_cute"), lit(0)).alias("compliment_cute"),
        coalesce(col("compliment_list"), lit(0)).alias("compliment_list"),
        coalesce(col("compliment_note"), lit(0)).alias("compliment_note"),
        coalesce(col("compliment_plain"), lit(0)).alias("compliment_plain"),
        coalesce(col("compliment_cool"), lit(0)).alias("compliment_cool"),
        coalesce(col("compliment_funny"), lit(0)).alias("compliment_funny"),
        coalesce(col("compliment_writer"), lit(0)).alias("compliment_writer"),
        coalesce(col("compliment_photos"), lit(0)).alias("compliment_photos"),
    ).withColumn(
        "user_total_compliments",
        col("compliment_hot") + col("compliment_more") + col("compliment_profile") +
        col("compliment_cute") + col("compliment_list") + col("compliment_note") +
        col("compliment_plain") + col("compliment_cool") + col("compliment_funny") +
        col("compliment_writer") + col("compliment_photos")
    ).withColumn(
        "account_age_days",
        datediff(current_date(), col("yelping_since"))
    ).withColumn(
        "user_engagement_score",
        col("user_useful") + col("user_funny") + col("user_cool") + (col("fans") * 2)
    ).withColumn(
        "log_user_review_count",
        F.log1p(coalesce(col("user_review_count"), lit(0)))
    ).withColumn(
        "user_review_count_norm",
        spark_round(col("log_user_review_count") / lit(LOG1P_50000), 4)
    )

    write_gold("ML", user_features_df, GOLD_PATH_ML, "user_features", num_output_files=10)

    # ---- 3. Interaction Matrix ----
    interaction_matrix_df = review_df.select(
        col("review_id"),
        col("user_id"),
        col("business_id"),
        col("stars").alias("rating"),
        col("date").alias("review_timestamp"),
        col("useful"),
        col("funny"),
        col("cool"),
        col("weighted_score"),
        (col("useful") + col("funny") + col("cool")).alias("total_feedback_votes"),
        when(col("useful") + col("funny") + col("cool") > 0, 1).otherwise(0).alias("has_feedback"),
        length(col("text")).alias("review_length_chars"),
        size(split(col("text"), "\\s+")).alias("review_word_count")
    )

    write_gold("ML", interaction_matrix_df, GOLD_PATH_ML, "interaction_matrix", num_output_files=20)

    # ---- 4. RAG Business Context ----
    rag_business_context_df = business_df.select(
        col("business_id"),
        col("name"),
        col("city"),
        col("state"),
        col("postal_code"),
        col("stars"),
        col("review_count"),
        col("categories"),
        safe_attribute_column(business_df, "price_range", "price_range"),
        safe_attribute_column(business_df, "alcohol", "alcohol"),
        safe_attribute_column(business_df, "wifi", "wifi"),
        safe_attribute_column(business_df, "parking", "parking"),
        safe_attribute_column(business_df, "outdoor_seating", "outdoor_seating")
    ).withColumn(
        "doc_id",
        F.concat(lit("doc_biz_"), col("business_id"))
    ).withColumn(
        "document_type",
        lit("business_profile")
    ).withColumn(
        "text_content",
        F.concat(
            lit("Business Name: "), coalesce(col("name"), lit("N/A")), lit("\n"),
            lit("Location: "), coalesce(col("city"), lit("N/A")), lit(", "), coalesce(col("state"), lit("N/A")), lit(" "), coalesce(col("postal_code"), lit("")), lit("\n"),
            lit("Rating: "), coalesce(col("stars").cast(StringType()), lit("N/A")), lit(" stars ("), coalesce(col("review_count").cast(StringType()), lit("0")), lit(" reviews)\n"),
            lit("Categories: "), coalesce(col("categories"), lit("N/A")), lit("\n"),
            lit("Price Range: "), coalesce(col("price_range").cast(StringType()), lit("N/A")), lit("\n"),
            lit("Amenities: Alcohol: "), coalesce(col("alcohol").cast(StringType()), lit("N/A")),
            lit(", WiFi: "), coalesce(col("wifi").cast(StringType()), lit("N/A")),
            lit(", Parking: "), coalesce(col("parking").cast(StringType()), lit("N/A")),
            lit(", Outdoor Seating: "), coalesce(col("outdoor_seating").cast(StringType()), lit("N/A"))
        )
    ).select("doc_id", "business_id", "document_type", "name", "city", "state", "categories", "stars", "review_count", "text_content")

    write_gold("RAG", rag_business_context_df, GOLD_PATH_RAG, "business_context", num_output_files=10)

    # ---- 5. RAG High Utility Reviews ----
    rag_high_utility_reviews_df = review_df.filter(
        (col("useful") >= 3) & (length(col("text")) >= 100)
    ).join(
        broadcast(business_df.select(col("business_id"), col("name").alias("business_name"), col("city").alias("business_city"), col("state").alias("business_state"))),
        on="business_id",
        how="inner"
    ).select(
        col("review_id"),
        col("business_id"),
        col("user_id"),
        col("business_name"),
        col("business_city"),
        col("business_state"),
        col("stars"),
        col("date"),
        col("useful"),
        col("funny"),
        col("cool"),
        col("weighted_score"),
        col("text")
    ).withColumn(
        "doc_id",
        F.concat(lit("doc_rev_"), col("review_id"))
    ).withColumn(
        "document_type",
        lit("customer_review")
    ).withColumn(
        "text_content",
        F.concat(
            lit("Review for: "), coalesce(col("business_name"), lit("N/A")), lit(" ("), coalesce(col("business_city"), lit("N/A")), lit(", "), coalesce(col("business_state"), lit("N/A")), lit(")\n"),
            lit("Rating: "), coalesce(col("stars").cast(StringType()), lit("N/A")), lit(" stars | Date: "), coalesce(col("date").cast(StringType()), lit("N/A")), lit("\n"),
            lit("Feedback: "), coalesce(col("useful").cast(StringType()), lit("0")), lit(" useful votes | Weighted Score: "), coalesce(col("weighted_score").cast(StringType()), lit("N/A")), lit("\n"),
            lit("Review Text:\n"), col("text")
        )
    ).select("doc_id", "review_id", "business_id", "user_id", "document_type", "business_name", "stars", "date", "useful", "weighted_score", "text_content")

    write_gold("RAG", rag_high_utility_reviews_df, GOLD_PATH_RAG, "high_utility_reviews", num_output_files=20)

    logger.info("ML + RAG Branch Completed Successfully.")


# =======================================================================
# BI STAR-SCHEMA BRANCH
# =======================================================================

def run_bi(business_df, review_df, user_df, checkin_df):
    logger.info("Starting BI Star-Schema Branch...")

    dim_date = (
        review_df.select(col("review_date").alias("Date"))
        .filter(col("Date").isNotNull())
        .distinct()
        .withColumn("DateKey", date_format(col("Date"), "yyyyMMdd").cast(LongType()))
        .withColumn("Year", year(col("Date")))
        .withColumn("Quarter", quarter(col("Date")))
        .withColumn("Month", month(col("Date")))
        .withColumn("WeekOfYear", weekofyear(col("Date")))
        .withColumn("DayOfWeek", dayofweek(col("Date")))
        .withColumn("IsWeekend", when(dayofweek(col("Date")).isin(1, 7), 1).otherwise(0))
        .select("DateKey", "Date", "Year", "Quarter", "Month", "WeekOfYear", "DayOfWeek", "IsWeekend")
    )

    dim_business = business_df.select(
        col("business_id").alias("BusinessID"),
        col("name").alias("BusinessName"),
        col("address").alias("Address"),
        col("city").alias("City"),
        col("state").alias("State"),
        col("postal_code").alias("PostalCode"),
        col("latitude").alias("Latitude"),
        col("longitude").alias("Longitude"),
        col("stars").alias("CurrentStars"),
        col("review_count").alias("CurrentReviewCount"),
        col("is_open").alias("IsOpen"),
        col("categories").alias("Categories"),
        safe_attribute_column(business_df, "price_range", "PriceRange"),
        safe_attribute_column(business_df, "alcohol", "Alcohol"),
        safe_attribute_column(business_df, "wifi", "WiFi"),
        safe_attribute_column(business_df, "parking", "BusinessParking"),
        safe_attribute_column(business_df, "takeout", "RestaurantsTakeOut"),
        safe_attribute_column(business_df, "delivery", "RestaurantsDelivery"),
        safe_attribute_column(business_df, "good_for_kids", "GoodForKids"),
        safe_attribute_column(business_df, "noise_level", "NoiseLevel"),
        safe_attribute_column(business_df, "attire", "RestaurantsAttire"),
        safe_attribute_column(business_df, "reservations", "RestaurantsReservations"),
        safe_attribute_column(business_df, "credit_cards", "BusinessAcceptsCreditCards"),
        safe_attribute_column(business_df, "by_appointment_only", "ByAppointmentOnly"),
        safe_attribute_column(business_df, "outdoor_seating", "OutdoorSeating")
    )

    fact_business = review_df.groupBy("business_id").agg(
        avg("stars").alias("CalculatedAvgStars"),
        F.count("review_id").alias("TotalReviewsInDataset"),
        avg("useful").alias("AvgUsefulVotes"),
        avg("funny").alias("AvgFunnyVotes"),
        avg("cool").alias("AvgCoolVotes"),
        avg("weighted_score").alias("AvgWeightedScore"),
        stddev("stars").alias("RatingStdDev"),
        countDistinct("user_id").alias("UniqueReviewersCount")
    ).join(
        broadcast(business_df.select(col("business_id"), col("stars").alias("BusinessCatalogStars"), col("review_count").alias("BusinessCatalogReviewCount"), col("is_open").alias("IsOpen"))),
        on="business_id",
        how="right"
    ).select(
        col("business_id").alias("BusinessID"),
        col("BusinessCatalogStars"),
        col("BusinessCatalogReviewCount"),
        spark_round(col("CalculatedAvgStars"), 2).alias("CalculatedAvgStars"),
        coalesce(col("TotalReviewsInDataset"), lit(0)).alias("TotalReviewsInDataset"),
        spark_round(col("AvgUsefulVotes"), 2).alias("AvgUsefulVotes"),
        spark_round(col("AvgFunnyVotes"), 2).alias("AvgFunnyVotes"),
        spark_round(col("AvgCoolVotes"), 2).alias("AvgCoolVotes"),
        spark_round(col("AvgWeightedScore"), 2).alias("AvgWeightedScore"),
        spark_round(col("RatingStdDev"), 2).alias("RatingStdDev"),
        coalesce(col("UniqueReviewersCount"), lit(0)).alias("UniqueReviewersCount"),
        col("IsOpen")
    )

    fact_review_trend = review_df.select(
        col("review_id").alias("ReviewID"),
        col("business_id").alias("BusinessID"),
        col("user_id").alias("UserID"),
        date_format(col("date"), "yyyyMMdd").cast(LongType()).alias("DateKey"),
        col("stars").alias("Stars"),
        col("useful").alias("UsefulVotes"),
        col("funny").alias("FunnyVotes"),
        col("cool").alias("CoolVotes"),
        col("weighted_score").alias("WeightedScore"),
        year(col("date")).alias("Year"),
        month(col("date")).alias("Month")
    )

    fact_rating_distribution = review_df.groupBy("business_id", "stars").agg(
        F.count("review_id").alias("RatingCount")
    ).select(
        col("business_id").alias("BusinessID"),
        col("stars").alias("StarRating"),
        col("RatingCount")
    )

    dim_business_hours = build_dim_business_hours(business_df)

    fact_checkin_day = checkin_df.select(
        col("business_id").alias("BusinessID"),
        col("date").cast(DateType()).alias("CheckinDate"),
        dayofweek(col("date")).alias("DayOfWeek"),
        date_format(col("date"), "yyyyMMdd").cast(LongType()).alias("DateKey"),
    ).groupBy("BusinessID", "DateKey", "CheckinDate", "DayOfWeek").agg(
        F.count("*").alias("CheckinCount")
    )

    fact_checkin_hour = checkin_df.select(
        col("business_id").alias("BusinessID"),
        col("date").cast(DateType()).alias("CheckinDate"),
        F.hour(col("date")).alias("HourOfDay"),
        date_format(col("date"), "yyyyMMdd").cast(LongType()).alias("DateKey"),
    ).groupBy("BusinessID", "DateKey", "CheckinDate", "HourOfDay").agg(
        F.count("*").alias("CheckinCount")
    )

    logger.info("[BI] Writing gold tables...")
    dim_date.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_PATH_BI}/dim_date/")
    dim_business.coalesce(10).write.mode("overwrite").parquet(f"{GOLD_PATH_BI}/dim_business/")
    fact_business.coalesce(10).write.mode("overwrite").parquet(f"{GOLD_PATH_BI}/fact_business/")
    fact_review_trend.coalesce(20).write.mode("overwrite").parquet(f"{GOLD_PATH_BI}/fact_review_trend/")
    fact_rating_distribution.coalesce(10).write.mode("overwrite").parquet(f"{GOLD_PATH_BI}/fact_rating_distribution/")
    dim_business_hours.coalesce(5).write.mode("overwrite").parquet(f"{GOLD_PATH_BI}/dim_business_hours/")
    fact_checkin_day.coalesce(20).write.mode("overwrite").parquet(f"{GOLD_PATH_BI}/fact_checkin_day/")
    fact_checkin_hour.coalesce(20).write.mode("overwrite").parquet(f"{GOLD_PATH_BI}/fact_checkin_hour/")
    logger.info("[BI] All BI gold tables written.")

    return business_df, review_df, user_df


# =======================================================================
# RUN + COMMIT
# =======================================================================

try:
    business_df, review_df, user_df, checkin_df = read_all_silver_shared()

    business_clean = clean_business(business_df).cache()
    review_clean = clean_review(review_df).cache()
    user_clean = clean_user(user_df).cache()

    run_bi(business_clean, review_clean, user_clean, checkin_df)
    print("\nBI Gold dataset built successfully.")

    run_ml_and_rag(business_clean, review_clean, user_clean)
    print("\nAll Gold datasets (BI + ML + RAG) built successfully.")

except Exception as e:
    print(f"\n!!!!!!!!!! GOLD JOB FAILED: {e} !!!!!!!!!!")
    raise
finally:
    job.commit()
    print("========== GLUE CONSOLIDATED SILVER -> GOLD (BI + ML + RAG) JOB FINISHED ==========")
