import os
import sys
import re

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==========================================================
# GLUE INITIALIZATION
# ==========================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "DATABASE_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ==========================================================
# CONFIGURATION
# ==========================================================

S3_BUCKET = args["S3_BUCKET"]
DATABASE_NAME = args["DATABASE_NAME"]

OUTPUT_BASE_PATH = f"s3://{S3_BUCKET}/silver/"

DATASETS = ["business", "review", "user", "checkin", "tip"]

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================

def log(msg):
    print(f"[SILVER_ETL] {msg}")

def standardize_column_name(name):
    name = name.strip()
    name = name.replace(" ", "_")
    name = name.replace("-", "_")
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    name = re.sub(r"_+", "_", name)
    return name.lower()

def flatten_df(df):
    while True:
        struct_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StructType)
        ]
        if len(struct_cols) == 0:
            break

        for struct_col in struct_cols:
            expanded_cols = [
                col(f"{struct_col}.{field.name}")
                .alias(f"{struct_col}_{field.name}")
                for field in df.schema[struct_col].dataType.fields
            ]
            df = df.select(
                *[c for c in df.columns if c != struct_col],
                *expanded_cols
            )
    return df

def clean_string_columns(df):
    string_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
    for c in string_cols:
        df = df.withColumn(
            c,
            when(trim(col(c)) == "", None)
            .otherwise(col(c))
        )
    return df

def get_catalog_table_name(dataset_name):
    """Finds exact catalog table name in Glue Catalog for dataset."""
    try:
        tables = [t.name for t in spark.catalog.listTables(DATABASE_NAME)]
        log(f"Catalog tables in '{DATABASE_NAME}': {tables}")
        for t in tables:
            if dataset_name in t:
                return t
    except Exception as e:
        log(f"Could not list catalog tables: {e}")

    # Fallbacks
    return f"yelp_academic_dataset_{dataset_name}_json"

# ==========================================================
# PROCESS EACH DATASET
# ==========================================================

for dataset_name in DATASETS:

    log("=" * 70)
    log(f"PROCESSING DATASET: {dataset_name}")

    OUTPUT_PATH = OUTPUT_BASE_PATH + dataset_name + "/"

    table_name = get_catalog_table_name(dataset_name)
    log(f"Reading from Catalog table: {DATABASE_NAME}.{table_name}")

    # Read from catalog or S3 direct fallback
    df = None
    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME,
            table_name=table_name
        )
        df = dynamic_frame.toDF()
    except Exception as e:
        log(f"Catalog read failed for {table_name}: {e}. Trying direct S3 JSON read...")

    if df is None or df.rdd.isEmpty():
        # Fallback direct S3 read
        bronze_bucket_env = os.getenv("BRONZE_BUCKET_NAME", "yelp-bronze-raw-us-east-1")
        s3_json_path = f"s3://{bronze_bucket_env}/yelp_academic_dataset_{dataset_name}.json"
        log(f"Direct S3 read path: {s3_json_path}")
        try:
            df = spark.read.json(s3_json_path)
        except Exception as e2:
            log(f"Direct S3 read also failed: {e2}")
            continue

    log(f"Read '{dataset_name}' from catalog/S3 successfully.")

    # ======================================================
    # FLATTEN STRUCTS
    # ======================================================

    df = flatten_df(df)

    # ======================================================
    # STANDARDIZE COLUMN NAMES
    # ======================================================

    df = df.toDF(
        *[
            standardize_column_name(c)
            for c in df.columns
        ]
    )

    # ======================================================
    # CLEAN STRINGS
    # ======================================================

    df = clean_string_columns(df)

    # ======================================================
    # BUSINESS
    # ======================================================

    if dataset_name == "business":
        df = df.filter(col("business_id").isNotNull())
        df = df.dropDuplicates(["business_id"])
        df = (
            df
            .withColumn("stars", col("stars").cast("double"))
            .withColumn("review_count", col("review_count").cast("int"))
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("longitude", col("longitude").cast("double"))
            .withColumn("is_open", col("is_open").cast("int"))
        )

    # ======================================================
    # REVIEW
    # ======================================================

    elif dataset_name == "review":
        df = df.filter(col("review_id").isNotNull())
        df = df.dropDuplicates(["review_id"])
        df = (
            df
            .withColumn("date", col("date").cast("timestamp"))
            .withColumn("time_stamp", date_format(col("date"), "HH:mm:ss"))
            .withColumn("review_date", to_date(col("date")))
            .withColumn("stars", col("stars").cast("double"))
        )
        for c in ["useful", "funny", "cool"]:
            df = df.withColumn(c, coalesce(col(c).cast("int"), lit(0)))

        df = df.fillna({"text": ""})
        df = df.withColumn(
            "weighted_score",
            round(
                least(
                    (
                        0.7 * (col("stars") / 5)
                        +
                        0.3 * (
                            (col("useful") + col("funny") + col("cool")) / 30
                        )
                    ) * 10,
                    lit(10)
                )
            ).cast("int")
        )

    # ======================================================
    # USER
    # ======================================================

    elif dataset_name == "user":
        df = df.filter(col("user_id").isNotNull())
        df = df.dropDuplicates(["user_id"])
        if "yelping_since" in df.columns:
            df = df.withColumn(
                "yelping_since",
                to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss")
            )

    # ======================================================
    # CHECKIN
    # ======================================================

    elif dataset_name == "checkin":
        df = df.filter(col("business_id").isNotNull())
        df = df.dropDuplicates(["business_id"])

    # ======================================================
    # TIP
    # ======================================================

    elif dataset_name == "tip":
        df = (
            df
            .withColumn("date", to_timestamp(col("date")))
            .withColumn("compliment_count", col("compliment_count").cast(IntegerType()))
        )
        df = df.filter(col("user_id").isNotNull())
        df = df.filter(col("business_id").isNotNull())
        df = df.dropDuplicates()
        df = df.fillna({"text": ""})
        df = df.withColumn("text_length", length(col("text")))

    # ======================================================
    # ETL TIMESTAMP
    # ======================================================

    df = df.withColumn("etl_processed_timestamp", current_timestamp())

    # ======================================================
    # WRITE SILVER
    # ======================================================

    final_count = df.count()
    log(f"Final Rows for {dataset_name}: {final_count:,}")

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(OUTPUT_PATH)
    )

    log(f"Successfully Written: {OUTPUT_PATH}")

# ==========================================================
# JOB COMMIT
# ==========================================================

job.commit()

log("ALL SILVER DATASETS PROCESSED SUCCESSFULLY")
