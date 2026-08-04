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

TABLES = [
    "yelp_academic_dataset_business_json",
    "yelp_academic_dataset_review_json",
    "yelp_academic_dataset_user_json",
    "yelp_academic_dataset_checkin_json",
    "yelp_academic_dataset_tip_json"
]

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

# ==========================================================
# PROCESS EACH TABLE
# ==========================================================

for TABLE_NAME in TABLES:

    log("=" * 70)
    log(f"PROCESSING : {TABLE_NAME}")

    dataset_name = (
        TABLE_NAME
        .replace("yelp_academic_dataset_", "")
        .replace("_json", "")
    )

    OUTPUT_PATH = OUTPUT_BASE_PATH + dataset_name + "/"

    # ======================================================
    # READ DATA
    # ======================================================

    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=DATABASE_NAME,
        table_name=TABLE_NAME
    )

    df = dynamic_frame.toDF()

    log(f"Raw Rows : {df.count():,}")

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

        df = df.filter(
            col("review_id").isNotNull()
        )

        df = df.dropDuplicates(["review_id"])

        df = (
            df
            .withColumn("date", col("date").cast("timestamp"))
            .withColumn(
                "time_stamp",
                date_format(col("date"), "HH:mm:ss")
            )
            .withColumn(
                "review_date",
                to_date(col("date"))
            )
            .withColumn(
                "stars",
                col("stars").cast("double")
            )
        )

        for c in ["useful", "funny", "cool"]:

            df = df.withColumn(
                c,
                coalesce(
                    col(c).cast("int"),
                    lit(0)
                )
            )

        df = df.fillna({"text": ""})

        # FIX: weighted_score capped at 10 (previously could exceed 10
        # if useful+funny+cool > 30)
        df = df.withColumn(
            "weighted_score",
            round(
                least(
                    (
                        0.7 * (col("stars") / 5)
                        +
                        0.3 * (
                            (
                                col("useful")
                                + col("funny")
                                + col("cool")
                            ) / 30
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

        df = df.filter(
            col("user_id").isNotNull()
        )

        df = df.dropDuplicates(["user_id"])

        if "yelping_since" in df.columns:

            # REVERTED: actual data confirmed via Glue job run error
            # ("Text '2014-07-21 15:34:06' could not be parsed,
            # unparsed text found at index 10") that yelping_since IS
            # a full timestamp (date + time), not date-only. Original
            # format was correct.
            df = df.withColumn(
                "yelping_since",
                to_timestamp(
                    col("yelping_since"),
                    "yyyy-MM-dd HH:mm:ss"
                )
            )

    # ======================================================
    # CHECKIN
    # ======================================================

    elif dataset_name == "checkin":

        df = df.filter(
            col("business_id").isNotNull()
        )

        df = df.dropDuplicates(
            ["business_id"]
        )

    # ======================================================
    # TIP
    # ======================================================

    elif dataset_name == "tip":

        df = (
            df
            .withColumn(
                "date",
                to_timestamp(col("date"))
            )
            .withColumn(
                "compliment_count",
                col("compliment_count")
                .cast(IntegerType())
            )
        )

        df = df.filter(
            col("user_id").isNotNull()
        )

        df = df.filter(
            col("business_id").isNotNull()
        )

        df = df.dropDuplicates()

        # FIX: fillna text before computing length, so empty/null
        # tip text gives text_length = 0 instead of null
        df = df.fillna({"text": ""})

        df = df.withColumn(
            "text_length",
            length(col("text"))
        )

    # ======================================================
    # ETL TIMESTAMP
    # ======================================================

    df = df.withColumn(
        "etl_processed_timestamp",
        current_timestamp()
    )

    # ======================================================
    # WRITE SILVER
    # ======================================================

    final_count = df.count()

    log(f"Final Rows : {final_count:,}")

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(OUTPUT_PATH)
    )

    log(f"Written : {OUTPUT_PATH}")

# ==========================================================
# JOB COMMIT
# ==========================================================

job.commit()

log("ALL SILVER DATASETS PROCESSED SUCCESSFULLY")
