import sys
import time
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("gold_etl_trigger")

REGION = "us-east-1"
BRONZE_TO_SILVER_JOB    = "yelp-bigdata_bronze_to_silver"
SILVER_TO_GOLD_JOB_NAME = "yelp-bigdata_silver_to_gold"
GOLD_CRAWLER_NAME       = "yelp-bigdata_gold_crawler"
SILVER_BUCKET_NAME      = "yelp-silver-clean-us-east-1"
GOLD_BUCKET_NAME        = "yelp-gold-analytics-us-east-1"

def start_and_wait_glue_job(glue, job_name):
    logger.info(f"Starting Glue Job: '{job_name}' ...")
    try:
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response["JobRunId"]
        logger.info(f"Glue Job '{job_name}' started with JobRunId: {job_run_id}")
    except ClientError as e:
        logger.error(f"Failed to start Glue Job '{job_name}': {e}")
        sys.exit(1)

    while True:
        try:
            status_resp = glue.get_job_run(JobName=job_name, RunId=job_run_id)
            state = status_resp["JobRun"]["JobRunState"]
            logger.info(f"  Job '{job_name}' (RunId: {job_run_id}) Status: {state}")

            if state == "SUCCEEDED":
                logger.info(f"✅ Glue Job '{job_name}' completed successfully!")
                return True
            elif state in ["FAILED", "STOPPED", "TIMEOUT"]:
                error_msg = status_resp["JobRun"].get("ErrorMessage", "Unknown error")
                logger.error(f"❌ Glue Job '{job_name}' failed with state '{state}': {error_msg}")
                sys.exit(1)
        except Exception as e:
            logger.warning(f"Error checking job status: {e}")

        time.sleep(20)

def start_and_wait_crawler(glue, crawler_name):
    logger.info(f"Starting Glue Crawler: '{crawler_name}' ...")
    try:
        glue.start_crawler(Name=crawler_name)
        logger.info(f"Crawler '{crawler_name}' started.")
    except ClientError as e:
        if "CrawlerRunningException" in str(e):
            logger.info(f"Crawler '{crawler_name}' is already running.")
        else:
            logger.error(f"Failed to start crawler '{crawler_name}': {e}")
            return False

    time.sleep(10)
    while True:
        try:
            resp = glue.get_crawler(Name=crawler_name)
            state = resp["Crawler"]["State"]
            logger.info(f"  Crawler '{crawler_name}' Status: {state}")

            if state == "READY":
                logger.info(f"✅ Crawler '{crawler_name}' finished.")
                return True
        except Exception as e:
            logger.warning(f"Error checking crawler status: {e}")

        time.sleep(15)

def check_silver_ready(s3, silver_bucket):
    """Ensures Silver Parquet tables exist before running Silver-to-Gold job."""
    logger.info(f"Checking Silver Parquet tables in s3://{silver_bucket}/ ...")
    try:
        resp = s3.list_objects_v2(Bucket=silver_bucket, Prefix="silver/", MaxKeys=10)
        contents = resp.get("Contents", [])
        if len(contents) > 0:
            logger.info(f"Silver bucket has {len(contents)}+ objects present.")
            return True
    except Exception as e:
        logger.warning(f"Could not check Silver bucket: {e}")
    return False

def verify_gold_s3_data(s3, bucket_name):
    logger.info(f"Verifying Gold Parquet objects in s3://{bucket_name}/gold/ ...")
    try:
        resp = s3.list_objects_v2(Bucket=bucket_name, Prefix="gold/", MaxKeys=50)
        contents = resp.get("Contents", [])
        if not contents:
            logger.error(f"❌ No objects found in s3://{bucket_name}/gold/")
            sys.exit(1)

        logger.info(f"🎉 GOLD BUCKET VERIFIED! Found {len(contents)} Gold datasets in S3:")
        for obj in contents[:20]:
            size_mb = obj['Size'] / (1024 * 1024)
            logger.info(f"  - s3://{bucket_name}/{obj['Key']} ({size_mb:.2f} MB)")
    except Exception as e:
        logger.error(f"Failed to list Gold S3 objects: {e}")
        sys.exit(1)

def main():
    glue = boto3.client("glue", region_name=REGION)
    s3   = boto3.client("s3",   region_name=REGION)

    gold_job_name = sys.argv[1] if len(sys.argv) > 1 else SILVER_TO_GOLD_JOB_NAME
    crawler_name  = sys.argv[2] if len(sys.argv) > 2 else GOLD_CRAWLER_NAME
    gold_bucket   = sys.argv[3] if len(sys.argv) > 3 else GOLD_BUCKET_NAME

    # Step 1: Ensure Silver data is generated
    if not check_silver_ready(s3, SILVER_BUCKET_NAME):
        logger.info("Silver layer empty/missing. Running bronze_to_silver Glue job first...")
        start_and_wait_glue_job(glue, BRONZE_TO_SILVER_JOB)

    # Step 2: Run Silver to Gold Job & wait for completion
    start_and_wait_glue_job(glue, gold_job_name)

    # Step 3: Run Gold Crawler to catalog Gold tables into yelp_db_gold
    start_and_wait_crawler(glue, crawler_name)

    # Step 4: Verify Parquet files land in S3 Gold bucket
    verify_gold_s3_data(s3, gold_bucket)

if __name__ == "__main__":
    main()
