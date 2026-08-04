"""
================================================================================
OPTIMIZED — FINAL MERGED ETL GLUE JOB SCRIPT - Silver to Gold Layer Transformation
(BI star-schema branch + ML feature-engineering branch + RAG document branch)
================================================================================
Purpose : Transform Yelp data from Silver Layer to Gold Layer
Source  : s3://<SILVER_BUCKET>/silver/ (or /silver_layer/)
Target  : s3://<GOLD_BUCKET>/gold/
             ├── bi/    (Star-schema BI tables)
             ├── ml/    (ML feature tables)
             └── rag/   (RAG documents)

WHAT CHANGED VS THE ORIGINAL (performance & dynamic S3 bucket parameterization):
  1. Business/review/user/checkin are read from Silver ONCE and cached, then
     shared by BOTH the BI branch and the ML+RAG branch.
  2. All redundant "count-just-to-log" calls removed to eliminate unnecessary full S3 scans.
  3. dim_business_hours uses pure Spark stack() explosion — no driver-side collect().
  4. Removed unnecessary global orderBy() before writes for Parquet outputs.
  5. repartition(N) -> coalesce(N) for aggregated output tables.
  6. Dynamic resolution of S3 input/output paths via Glue job parameters (SILVER_BUCKET, GOLD_BUCKET).
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
spark.conf.set("spark.sql.shuffle.partitions", "64")  # down from default 200

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ================================================================================
# CONFIGURATION
# ================================================================================
SILVER_BUCKET = args["SILVER_BUCKET"]
GOLD_BUCKET   = args["GOLD_BUCKET"]

GOLD_ROOT = f"s3://{GOLD_BUCKET}/gold"
GOLD_PATH_BI  = f"{GOLD_ROOT}/bi"
GOLD_PATH_ML  = f"{GOLD_ROOT}/ml"
GOLD_PATH_RAG = f"{GOLD_ROOT}/rag"

logger.info("=" * 80)
logger.info("GLUE ETL JOB (OPTIMIZED): Silver to Gold Layer Transformation (BI + ML + RAG)")
logger.info(f"Silver Bucket: {SILVER_BUCKET}")
logger.info(f"Gold Root    : {GOLD_ROOT}")
logger.info("=" * 80)

LOG1P_10000 = math.log1p(10000)
LOG1P_50000 = math.log1p(50000)

ATTRIBUTE_COLUMN_CANDIDATES = {
    "wifi": ["attributes_wifi"],
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


def safe_col(df, col_name, default=None):
    if col_name in df.columns:
        return col(col_name)
    return lit(default)


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
    """
    Read a Silver Parquet dataset from S3.

    Uses mergeSchema=True so that if successive bronze_to_silver runs produced
    slightly different column sets (e.g. new attribute columns), Spark unions
    all schemas and fills missing columns with null instead of erroring out.

    Tries three candidate prefix layouts in order:
      1. silver/<dataset>/     ← written by bronze_to_silver (most common)
      2. silver_layer/<dataset>/ ← alternate prefix seen in some runs
      3. <dataset>/             ← root of the bucket
    """
    paths_to_try = [
        f"s3://{SILVER_BUCKET}/silver/{dataset_name}",
        f"s3://{SILVER_BUCKET}/silver_layer/{dataset_name}",
        f"s3://{SILVER_BUCKET}/{dataset_name}",
    ]
    for path in paths_to_try:
        try:
            log("READ", f"Attempting to read '{dataset_name}' from {path}")
            df = (
                spark.read
                     .option("mergeSchema", "true")
                     .parquet(path)
            )
            log("READ", f"Successfully read '{dataset_name}' from {path}")
            return df
        except Exception as e:
            log("READ_WARN", f"Could not read from {path}: {e}")
            continue
    raise RuntimeError(
        f"Failed to read silver dataset '{dataset_name}' from all candidate paths: {paths_to_try}"
    )



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
# ML branch — prepare / master join / features
# =======================================================================

def prepare_business(business_df):
    price_range_col = safe_attribute_column(business_df, "price_range", "price_range")
    wifi_col = safe_attribute_column(business_df, "wifi", "has_wifi")
    appt_only_col = safe_attribute_column(business_df, "by_appointment_only", "by_appointment_only")

    df = business_df.select(
        col("business_id"), col("name").alias("business_name"), col("city"), col("state"),
        col("categories"), col("stars").alias("business_avg_rating"),
        col("review_count").alias("business_review_count"), col("is_open"),
        col("latitude"), col("longitude"), price_range_col, wifi_col, appt_only_col,
    )

    df = (
        df.withColumn("categories_array", when(col("categories").isNotNull(), split(col("categories"), ",\\s*")).otherwise(None))
          .withColumn("primary_category", when(col("categories_array").isNotNull(), col("categories_array")[0]).otherwise(lit("Unknown")))
          .withColumn("has_wifi", when(lower(col("has_wifi").cast(StringType())).isin("free", "yes", "true"), lit(True)).otherwise(lit(False)))
    )
    return df


def prepare_user(user_df):
    df = user_df.select(
        col("user_id"), col("review_count").alias("user_review_count"),
        col("average_stars").alias("user_average_stars"), col("useful").alias("user_useful"),
        col("funny").alias("user_funny"), col("cool").alias("user_cool"),
        col("fans"), col("elite"), col("yelping_since"),
    )
    df = (
        df.withColumn("is_elite", when(col("elite").isNotNull() & (trim(col("elite")) != "") & (trim(col("elite")) != "None"), lit(1)).otherwise(lit(0)))
          .withColumn("user_tenure_years", spark_round(datediff(current_date(), col("yelping_since")) / 365.25, 1))
    )
    return df


def build_master_df(review_df, business_prepared, user_prepared):
    return (
        review_df
        .join(broadcast(business_prepared), on="business_id", how="left")
        .join(broadcast(user_prepared), on="user_id", how="left")
    )


def engineer_features(master_df):
    df = master_df
    df = (
        df.withColumn("review_length_chars", length(col("text")))
          .withColumn("review_word_count", size(split(trim(col("text")), "\\s+")))
          .withColumn("sentence_count", length(col("text")) - length(regexp_replace(col("text"), "[.!?]", "")))
          .withColumn("avg_word_length", when(col("review_word_count") > 0, col("review_length_chars") / col("review_word_count")).otherwise(lit(0.0)))
          .withColumn("uppercase_ratio", when(col("review_length_chars") > 0, (length(col("text")) - length(regexp_replace(col("text"), "[A-Z]", ""))) / col("review_length_chars")).otherwise(lit(0.0)))
          .withColumn("exclamation_count", size(split(col("text"), "!")) - 1)
          .withColumn("question_count", size(split(col("text"), "\\?")) - 1)
          .withColumn("review_text_clean", trim(regexp_replace(regexp_replace(regexp_replace(lower(col("text")), r"http\S+|www\.\S+", " "), r"<[^>]+>", " "), r"[^a-z0-9\s]", " ")))
          .withColumn("review_length_category", when(col("review_word_count") < 30, "short").when(col("review_word_count") < 100, "medium").otherwise("long"))
    )
    df = (
        df.withColumn("review_year", year(col("date")))
          .withColumn("review_month", month(col("date")))
          .withColumn("review_quarter", quarter(col("date")))
          .withColumn("review_week", weekofyear(col("date")))
          .withColumn("review_weekday", dayofweek(col("date")))
          .withColumn("days_since_review", datediff(current_date(), col("date")))
    )
    df = df.withColumn(
        "sentiment_label",
        when(col("stars") >= 4, "positive").when(col("stars") == 3, "neutral").otherwise("negative"),
    )
    df = (
        df.withColumn("review_rating_deviation", spark_round(col("stars") - col("business_avg_rating"), 2))
          .withColumn("user_rating_bias", spark_round(col("stars") - col("user_average_stars"), 2))
          .withColumn("review_popularity_ratio", when(col("business_review_count") > 0, col("user_review_count") / col("business_review_count")).otherwise(lit(0.0)))
    )
    return df


def build_sentiment_features(features_df):
    return features_df.select(
        "review_id", "text", "review_text_clean", "review_length_chars", "review_word_count",
        "sentence_count", "avg_word_length", "uppercase_ratio", "exclamation_count", "question_count",
        "is_elite", "user_review_count", "business_avg_rating", "sentiment_label", "stars", "review_year",
    ).withColumnRenamed("text", "review_text")


def build_rating_prediction(features_df):
    return features_df.select(
        "review_id", "business_id", "primary_category", "city", "state", "price_range",
        "user_rating_bias", "days_since_review", "review_length_category",
        "is_elite", "user_review_count", "business_review_count", "stars",
    )


def build_collaborative_filtering(features_df):
    df = features_df.withColumn(
        "review_credibility_weight",
        spark_round(
            lit(1.0)
            + (coalesce(col("is_elite"), lit(0)) * lit(0.3))
            + (least(coalesce(col("useful"), lit(0)), lit(10)) * lit(0.02))
            + (least(coalesce(col("funny"), lit(0)) + coalesce(col("cool"), lit(0)), lit(10)) * lit(0.01)),
            3,
        ),
    ).withColumn("weighted_score", spark_round(col("stars") * col("review_credibility_weight"), 3))
    return df.select("user_id", "business_id", "stars", "weighted_score")


def build_content_based_filtering(business_df):
    price_range_col = safe_attribute_column(business_df, "price_range", "attributes_restaurantspricerange2")
    wifi_col = safe_attribute_column(business_df, "wifi", "attributes_wifi")
    outdoor_seating_col = safe_attribute_column(business_df, "outdoor_seating", "attributes_outdoorseating")
    return business_df.select(
        col("business_id"), col("categories"), col("city"), col("state"), col("latitude"), col("longitude"),
        col("stars"), col("review_count"), col("is_open"), price_range_col, wifi_col, outdoor_seating_col,
    )


def build_customer_segmentation(features_df, user_prepared):
    review_level = features_df.groupBy("user_id").agg(
        avg("stars").alias("avg_stars_given"),
        stddev("stars").alias("rating_variance"),
        avg("review_word_count").alias("avg_review_length"),
        countDistinct("business_id").alias("distinct_businesses_reviewed"),
    )
    return (
        review_level
        .join(user_prepared.select("user_id", "user_review_count", "is_elite", "user_tenure_years", "user_useful"), on="user_id", how="left")
        .withColumnRenamed("user_useful", "useful_votes_received")
        .fillna({"rating_variance": 0.0})
    )


# =======================================================================
# RAG branch
# =======================================================================

def transform_business_documents(biz_df):
    categories_split = F.split(F.col("categories"), r",\s*")
    primary_cat = F.trim(categories_split.getItem(0))

    hours_concat = F.concat_ws(
        " | ",
        F.when(safe_col(biz_df, "hours_monday").isNotNull(), F.concat(F.lit("Monday: "), safe_col(biz_df, "hours_monday"))),
        F.when(safe_col(biz_df, "hours_tuesday").isNotNull(), F.concat(F.lit("Tuesday: "), safe_col(biz_df, "hours_tuesday"))),
        F.when(safe_col(biz_df, "hours_wednesday").isNotNull(), F.concat(F.lit("Wednesday: "), safe_col(biz_df, "hours_wednesday"))),
        F.when(safe_col(biz_df, "hours_thursday").isNotNull(), F.concat(F.lit("Thursday: "), safe_col(biz_df, "hours_thursday"))),
        F.when(safe_col(biz_df, "hours_friday").isNotNull(), F.concat(F.lit("Friday: "), safe_col(biz_df, "hours_friday"))),
        F.when(safe_col(biz_df, "hours_saturday").isNotNull(), F.concat(F.lit("Saturday: "), safe_col(biz_df, "hours_saturday"))),
        F.when(safe_col(biz_df, "hours_sunday").isNotNull(), F.concat(F.lit("Sunday: "), safe_col(biz_df, "hours_sunday")))
    )

    features_concat = F.concat_ws(
        ", ",
        F.when(safe_col(biz_df, "attributes_businessacceptscreditcards").isNotNull(), F.concat(F.lit("creditcards="), safe_col(biz_df, "attributes_businessacceptscreditcards"))),
        F.when(safe_col(biz_df, "attributes_businessparking").isNotNull(), F.concat(F.lit("parking="), safe_col(biz_df, "attributes_businessparking"))),
        F.when(safe_col(biz_df, "attributes_restaurantspricerange2").isNotNull(), F.concat(F.lit("pricerange="), safe_col(biz_df, "attributes_restaurantspricerange2"))),
        F.when(safe_col(biz_df, "attributes_restaurantsdelivery").isNotNull(), F.concat(F.lit("delivery="), safe_col(biz_df, "attributes_restaurantsdelivery"))),
        F.when(safe_col(biz_df, "attributes_restaurantstakeout").isNotNull(), F.concat(F.lit("takeout="), safe_col(biz_df, "attributes_restaurantstakeout"))),
        F.when(safe_col(biz_df, "attributes_outdoorseating").isNotNull(), F.concat(F.lit("outdoorseating="), safe_col(biz_df, "attributes_outdoorseating"))),
        F.when(safe_col(biz_df, "attributes_wifi").isNotNull(), F.concat(F.lit("wifi="), safe_col(biz_df, "attributes_wifi")))
    )

    doc_text = F.concat(
        F.lit("Business Name: "), F.coalesce(F.col("name"), F.lit("Unknown")),
        F.lit("\nPrimary Category: "), F.coalesce(primary_cat, F.lit("N/A")),
        F.lit("\nCategories: "), F.coalesce(F.col("categories"), F.lit("N/A")),
        F.lit("\nLocation: "), F.coalesce(F.col("address"), F.lit("")), F.lit(", "), F.coalesce(F.col("city"), F.lit("")), F.lit(", "), F.coalesce(F.col("state"), F.lit("")), F.lit(" "), F.coalesce(F.col("postal_code"), F.lit("")),
        F.lit("\nCoordinates: Lat "), F.coalesce(F.col("latitude").cast("string"), F.lit("N/A")), F.lit(", Lon "), F.coalesce(F.col("longitude").cast("string"), F.lit("N/A")),
        F.lit("\nRating: "), F.coalesce(F.col("stars").cast("string"), F.lit("N/A")), F.lit(" stars ("), F.coalesce(F.col("review_count").cast("string"), F.lit("0")), F.lit(" reviews)"),
        F.lit("\nPrice Range: "), F.coalesce(safe_col(biz_df, "attributes_restaurantspricerange2"), F.lit("N/A")),
        F.lit("\nOperating Status: "), F.when(F.col("is_open") == 1, F.lit("Open")).otherwise(F.lit("Closed")),
        F.lit("\nHours: "), F.when(hours_concat != "", hours_concat).otherwise(F.lit("N/A")),
        F.lit("\nFeatures: "), F.when(features_concat != "", features_concat).otherwise(F.lit("N/A"))
    )

    return biz_df.select(
        F.concat(F.lit("doc_biz_"), F.col("business_id")).alias("document_id"),
        F.col("business_id"), F.col("name").alias("business_name"), F.col("address"), F.col("city"),
        F.col("state"), F.col("postal_code"), F.col("latitude").cast(DoubleType()), F.col("longitude").cast(DoubleType()),
        primary_cat.alias("primary_category"), categories_split.alias("category_list"),
        F.col("stars").cast(DoubleType()).alias("business_rating"), F.col("review_count").cast(LongType()),
        safe_col(biz_df, "attributes_restaurantspricerange2").alias("price_range"), F.col("is_open").cast(IntegerType()),
        hours_concat.alias("hours"), features_concat.alias("business_features"),
        doc_text.alias("document_text"), F.lit("business").alias("document_type"),
        F.current_timestamp().alias("last_updated")
    )


def transform_review_documents(rev_df, biz_df):
    biz_sub = biz_df.select(
        F.col("business_id"), F.col("name").alias("business_name"), F.col("city"), F.col("state"),
        F.trim(F.split(F.col("categories"), r",\s*").getItem(0)).alias("primary_category")
    )
    rev_joined = rev_df.join(broadcast(biz_sub), on="business_id", how="left")

    sentiment_col = F.when(F.col("stars") >= 4, F.lit("positive")).when(F.col("stars") == 3, F.lit("neutral")).otherwise(F.lit("negative"))
    review_len = F.length(F.coalesce(F.col("text"), F.lit(""))).cast(IntegerType())

    doc_text = F.concat(
        F.lit("Review for "), F.coalesce(F.col("business_name"), F.lit("Unknown Business")),
        F.lit(" ("), F.coalesce(F.col("city"), F.lit("")), F.lit(", "), F.coalesce(F.col("state"), F.lit("")), F.lit(")"),
        F.lit("\nCategory: "), F.coalesce(F.col("primary_category"), F.lit("N/A")),
        F.lit("\nReview Rating: "), F.coalesce(F.col("stars").cast("string"), F.lit("N/A")), F.lit(" stars"),
        F.lit("\nDate: "), F.coalesce(F.col("date").cast("string"), F.lit("N/A")),
        F.lit("\nSentiment: "), sentiment_col,
        F.lit("\nContent: "), F.coalesce(F.col("text"), F.lit(""))
    )

    return rev_joined.select(
        F.concat(F.lit("doc_rev_"), F.col("review_id")).alias("document_id"),
        F.col("review_id"), F.col("business_id"), F.col("user_id"), F.col("business_name"),
        F.col("city"), F.col("state"), F.col("primary_category"), F.col("date").alias("review_date"),
        F.col("stars").cast(DoubleType()), sentiment_col.alias("sentiment"), review_len.alias("review_length"),
        doc_text.alias("document_text"), F.lit("review").alias("document_type"),
        F.current_timestamp().alias("last_updated")
    )


def run_rag_branch(business_clean, review_clean):
    log("RAG", "Building business_documents + review_documents...")
    gold_biz_df = transform_business_documents(business_clean)
    gold_rev_df = transform_review_documents(review_clean, business_clean)

    write_gold("RAG", gold_biz_df, GOLD_PATH_RAG, "business_documents")
    write_gold("RAG", gold_rev_df, GOLD_PATH_RAG, "review_documents")
    log("RAG", "RAG gold outputs written.")


def run_ml_and_rag(business_clean, review_clean, user_clean):
    business_prepared = prepare_business(business_clean).cache()
    user_prepared = prepare_user(user_clean).cache()

    master_df = build_master_df(review_clean, business_prepared, user_prepared)
    features_df = engineer_features(master_df).cache()

    write_gold("ML", build_sentiment_features(features_df), GOLD_PATH_ML, "sentiment_features", partition_by=["review_year"])
    write_gold("ML", build_rating_prediction(features_df), GOLD_PATH_ML, "rating_prediction")
    write_gold("ML", build_collaborative_filtering(features_df), GOLD_PATH_ML, "collaborative_filtering")
    write_gold("ML", build_content_based_filtering(business_clean), GOLD_PATH_ML, "content_based_filtering")
    write_gold("ML", build_customer_segmentation(features_df, user_prepared), GOLD_PATH_ML, "customer_segmentation")

    features_df.unpersist()
    business_prepared.unpersist()
    user_prepared.unpersist()

    run_rag_branch(business_clean, review_clean)


# =======================================================================
# Checkin date explosion helper
# =======================================================================

def explode_checkin(checkin_df):
    """
    The raw Yelp checkin record stores ALL check-in timestamps for a business
    in a single comma-separated string:

        {"business_id": "abc", "date": "2016-04-26 19:49:16, 2016-08-30 18:36:57"}

    bronze_to_silver writes this as-is (one row per business, date = string).
    If we try to do F.col("date").cast(DateType()) on that string we get NULL
    for every row, making fact_checkin_day / fact_checkin_hour completely empty.

    This function splits the comma-separated string and explodes it so each
    individual timestamp becomes its own row, then casts to TimestampType so
    downstream date/hour/dayofweek functions work correctly.
    """
    log("CHECKIN", "Exploding comma-separated checkin dates into individual rows...")
    exploded = (
        checkin_df
        .withColumn(
            "checkin_ts",
            F.explode(F.split(F.col("date"), ",\\s*"))
        )
        .withColumn(
            "checkin_ts",
            F.to_timestamp(F.trim(F.col("checkin_ts")), "yyyy-MM-dd HH:mm:ss")
        )
        .filter(F.col("checkin_ts").isNotNull())
        .drop("date")
        .withColumnRenamed("checkin_ts", "date")
    )
    log("CHECKIN", "Checkin date explosion complete.")
    return exploded


# =======================================================================
# BI branch (star schema)
# =======================================================================

def build_dim_business_hours(business_df):
    """
    Fully distributed replacement for the original collect()-based loop.
    Uses stack() to pivot the 7 hours_* columns into long format, entirely
    in Spark — no rows are ever pulled to the driver.
    """
    day_map = [
        ("hours_monday", "Monday", 1), ("hours_tuesday", "Tuesday", 2),
        ("hours_wednesday", "Wednesday", 3), ("hours_thursday", "Thursday", 4),
        ("hours_friday", "Friday", 5), ("hours_saturday", "Saturday", 6),
        ("hours_sunday", "Sunday", 7),
    ]
    existing = [(c, name, num) for c, name, num in day_map if c in business_df.columns]

    if not existing:
        return spark.createDataFrame(
            [], "BusinessID string, DayOfWeekNum int, DayOfWeek string, OpenTime string, CloseTime string"
        )

    stack_args = ", ".join(f"'{name}', {num}, `{c}`" for c, name, num in existing)
    stack_expr = f"stack({len(existing)}, {stack_args}) as (DayOfWeek, DayOfWeekNum, HoursStr)"

    long_df = business_df.select(F.col("business_id"), F.expr(stack_expr))

    parts = F.split(F.col("HoursStr"), "-")
    dim_business_hours = (
        long_df
        .filter(F.col("HoursStr").isNotNull() & (F.col("HoursStr") != "-"))
        .withColumn("OpenTime", F.trim(parts.getItem(0)))
        .withColumn("CloseTime", F.trim(parts.getItem(1)))
        .select(
            F.col("business_id").alias("BusinessID"),
            F.col("DayOfWeekNum").cast(IntegerType()),
            F.col("DayOfWeek"),
            F.col("OpenTime"),
            F.col("CloseTime"),
        )
    )
    return dim_business_hours


def run_bi(business_df, review_df, user_df, checkin_df):
    logger.info("\n[BI] Building star schema...")

    # Fix: Yelp checkin stores all timestamps as one comma-separated string per
    # business row. Explode into individual rows so cast(DateType) / hour() / etc.
    # produce real values instead of NULLs.
    checkin_exploded = explode_checkin(checkin_df).cache()

    # ---- dim_date ----
    review_dates  = review_df.select(F.col("date")).distinct()
    checkin_dates = checkin_exploded.select(F.col("date")).distinct()
    all_dates = review_dates.union(checkin_dates).distinct().filter(F.col("date").isNotNull())

    dim_date = all_dates.select(F.col("date").cast(DateType()).alias("Date")).distinct().select(
        F.date_format(F.col("Date"), "yyyyMMdd").cast(LongType()).alias("DateKey"),
        F.col("Date"),
        F.month(F.col("Date")).alias("Month"),
        F.date_format(F.col("Date"), "MMMM").alias("MonthName"),
        F.quarter(F.col("Date")).alias("Quarter"),
        F.year(F.col("Date")).alias("Year"),
        F.dayofweek(F.col("Date")).alias("DayOfWeek"),
        F.dayofmonth(F.col("Date")).alias("DayOfMonth"),
        F.weekofyear(F.col("Date")).alias("WeekOfYear"),
    )

    # ---- dim_business ----
    dim_business = business_df.select(
        F.col("business_id").alias("BusinessID"), F.col("name").alias("BusinessName"),
        F.col("city").alias("City"), F.col("state").alias("State"), F.col("address").alias("Address"),
        F.col("postal_code").alias("PostalCode"), F.col("latitude").alias("Latitude"),
        F.col("longitude").alias("Longitude"), F.split(F.col("categories"), ",")[0].alias("PrimaryCategory"),
        F.col("categories").alias("Categories"),
    ).distinct()

    # ---- fact_business ----
    review_metrics = review_df.groupBy("business_id").agg(
        F.count("review_id").alias("ReviewCount"),
        F.round(F.avg("stars"), 2).alias("AvgRating"),
        F.round(F.avg(F.col("useful") + F.col("funny") + F.col("cool")), 2).alias("AvgReviewEngagement"),
        F.round(F.avg(F.length("text")), 2).alias("AvgReviewLength"),
    )
    checkin_metrics = checkin_exploded.groupBy("business_id").agg(F.count("*").alias("CheckinCount"))

    fact_business = (
        business_df.select("business_id").distinct()
        .join(review_metrics, "business_id", "left")
        .join(checkin_metrics, "business_id", "left")
        .fillna(0)
    )

    rating_score = F.round((F.col("AvgRating") / 5) * 100, 2)
    review_score = F.round(F.least(F.log1p(F.col("ReviewCount")) / LOG1P_10000 * 100, F.lit(100)), 2)
    checkin_score = F.round(F.least(F.log1p(F.col("CheckinCount")) / LOG1P_50000 * 100, F.lit(100)), 2)
    health_score = F.round(rating_score * 0.4 + review_score * 0.35 + checkin_score * 0.25, 2)
    engagement_score = F.round(review_score * 0.6 + checkin_score * 0.4, 2)

    fact_business = fact_business.select(
        F.col("business_id").alias("BusinessID"),
        F.col("ReviewCount").cast(IntegerType()),
        F.col("AvgRating").cast(DoubleType()),
        F.col("CheckinCount").cast(IntegerType()),
        F.col("AvgReviewEngagement").cast(DoubleType()),
        F.col("AvgReviewLength").cast(DoubleType()),
        rating_score.alias("RatingScore"),
        review_score.alias("ReviewScore"),
        checkin_score.alias("CheckinScore"),
        health_score.alias("BusinessHealthScore"),
        engagement_score.alias("CustomerEngagementScore"),
        F.when(rating_score >= 75, "Excellent").when(rating_score >= 60, "Good").when(rating_score >= 45, "Average").otherwise("Poor").alias("HealthStatus"),
        F.when(engagement_score >= 70, "High").when(engagement_score >= 40, "Medium").otherwise("Low").alias("EngagementStatus"),
        F.current_timestamp().alias("LoadTimestamp"),
    )

    # ---- fact_review_trend ----
    fact_review_trend = review_df.select(
        F.col("business_id").alias("BusinessID"),
        F.col("date").cast(DateType()).alias("ReviewDate"),
        F.date_format(F.col("date"), "yyyyMMdd").cast(LongType()).alias("DateKey"),
        F.col("stars").alias("Rating"),
    ).groupBy("BusinessID", "DateKey", "ReviewDate").agg(
        F.count("Rating").alias("ReviewCount"),
        F.round(F.avg("Rating"), 2).alias("AvgRating"),
        F.min("Rating").alias("MinRating"),
        F.max("Rating").alias("MaxRating"),
    )

    # ---- fact_rating_distribution ----
    fact_rating_distribution = review_df.select(
        F.col("business_id").alias("BusinessID"), F.col("stars").alias("RatingValue")
    ).groupBy("BusinessID", "RatingValue").agg(F.count("*").alias("ReviewCount"))

    # ---- dim_business_hours ----
    dim_business_hours = build_dim_business_hours(business_df)

    # ---- fact_checkin_day (uses exploded timestamps) ----
    fact_checkin_day = checkin_exploded.select(
        F.col("business_id").alias("BusinessID"),
        F.col("date").cast(DateType()).alias("CheckinDate"),
        F.dayofweek(F.col("date")).alias("DayOfWeek"),
        F.date_format(F.col("date"), "yyyyMMdd").cast(LongType()).alias("DateKey"),
    ).groupBy("BusinessID", "DateKey", "CheckinDate", "DayOfWeek").agg(
        F.count("*").alias("CheckinCount")
    )

    # ---- fact_checkin_hour (uses exploded timestamps) ----
    fact_checkin_hour = checkin_exploded.select(
        F.col("business_id").alias("BusinessID"),
        F.col("date").cast(DateType()).alias("CheckinDate"),
        F.hour(F.col("date")).alias("HourOfDay"),
        F.date_format(F.col("date"), "yyyyMMdd").cast(LongType()).alias("DateKey"),
    ).groupBy("BusinessID", "DateKey", "CheckinDate", "HourOfDay").agg(
        F.count("*").alias("CheckinCount")
    )

    # ---- write ----
    logger.info("[BI] Writing gold tables...")
    write_gold("BI", dim_date,                 GOLD_PATH_BI, "dim_date",                 num_output_files=1)
    write_gold("BI", dim_business,             GOLD_PATH_BI, "dim_business",             num_output_files=10)
    write_gold("BI", fact_business,            GOLD_PATH_BI, "fact_business",            num_output_files=10)
    write_gold("BI", fact_review_trend,        GOLD_PATH_BI, "fact_review_trend",        num_output_files=20)
    write_gold("BI", fact_rating_distribution, GOLD_PATH_BI, "fact_rating_distribution", num_output_files=10)
    write_gold("BI", dim_business_hours,       GOLD_PATH_BI, "dim_business_hours",       num_output_files=5)
    write_gold("BI", fact_checkin_day,         GOLD_PATH_BI, "fact_checkin_day",         num_output_files=20)
    write_gold("BI", fact_checkin_hour,        GOLD_PATH_BI, "fact_checkin_hour",        num_output_files=20)
    checkin_exploded.unpersist()
    logger.info("[BI] All BI gold tables written.")

    return business_df, review_df, user_df


# =======================================================================
# RUN + COMMIT
# =======================================================================

try:
    business_df, review_df, user_df, checkin_df = read_all_silver_shared()

    # Clean once, share cleaned frames across BI and ML+RAG
    business_clean = clean_business(business_df).cache()
    review_clean   = clean_review(review_df).cache()
    user_clean     = clean_user(user_df).cache()

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
