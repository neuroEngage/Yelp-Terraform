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
SILVER_CRAWLER_NAME     = "yelp-bigdata_silver_crawler"
SILVER_TO_GOLD_JOB_NAME = "yelp-bigdata_silver_to_gold"
GOLD_CRAWLER_NAME       = "yelp-bigdata_gold_crawler"
GLUE_WORKFLOW_NAME      = "yelp-bigdata_etl_workflow"
SILVER_BUCKET_NAME      = "yelp-silver-clean-us-east-1"
GOLD_BUCKET_NAME        = "yelp-gold-analytics-us-east-1"

# All 4 datasets that bronze_to_silver must write before silver_to_gold can start
REQUIRED_SILVER_DATASETS = ["business", "review", "user", "checkin"]


# ── Glue Job helper ──────────────────────────────────────────────────────────

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


# ── Glue Crawler helper ──────────────────────────────────────────────────────

def start_and_wait_crawler(glue, crawler_name):
    """Start a Glue Crawler and wait until it finishes (READY state)."""
    logger.info(f"Starting Glue Crawler: '{crawler_name}' ...")
    try:
        glue.start_crawler(Name=crawler_name)
        logger.info(f"Crawler '{crawler_name}' started.")
    except ClientError as e:
        if "CrawlerRunningException" in str(e):
            logger.info(f"Crawler '{crawler_name}' is already running — will wait for it to finish.")
        else:
            logger.error(f"Failed to start crawler '{crawler_name}': {e}")
            return False

    time.sleep(10)
    while True:
        try:
            resp = glue.get_crawler(Name=crawler_name)
            state = resp["Crawler"]["State"]
            last_crawl = resp["Crawler"].get("LastCrawl", {})
            last_status = last_crawl.get("Status", "N/A")
            logger.info(f"  Crawler '{crawler_name}' State: {state} | Last Crawl Status: {last_status}")

            if state == "READY":
                if last_status == "FAILED":
                    logger.error(f"❌ Crawler '{crawler_name}' last crawl FAILED: {last_crawl.get('ErrorMessage', 'Unknown')}")
                    sys.exit(1)
                logger.info(f"✅ Crawler '{crawler_name}' finished successfully.")
                return True
        except Exception as e:
            logger.warning(f"Error checking crawler status: {e}")

        time.sleep(15)


# ── Glue Workflow wait helper ─────────────────────────────────────────────────

def wait_for_glue_workflow(glue, workflow_name, timeout_minutes=90):
    """
    Wait until the Glue Workflow has no RUNNING executions.

    Bug fixed vs. previous version: The old code checked `runs[0].Status` and
    returned True if it was COMPLETED. But that run could be the PREVIOUS pipeline's
    run (already done). This caused trigger_gold.py to proceed even though the
    current bronze_to_silver was still mid-write — creating the race condition.

    New logic: if ANY run is RUNNING, wait. Only proceed when all known runs are
    in a terminal state (COMPLETED / STOPPED / ERROR) or there are no active runs.
    """
    logger.info(f"Checking Glue Workflow '{workflow_name}' for any active (RUNNING) executions ...")
    deadline = time.time() + timeout_minutes * 60

    while time.time() < deadline:
        try:
            runs_resp = glue.get_workflow_runs(Name=workflow_name, MaxResults=10)
            runs = runs_resp.get("Runs", [])

            if not runs:
                logger.info(f"No workflow runs found for '{workflow_name}'. Safe to proceed.")
                return True

            running_runs = [r for r in runs if r.get("Status") == "RUNNING"]

            if running_runs:
                run_id = running_runs[0]["WorkflowRunId"]
                logger.info(
                    f"  Workflow '{workflow_name}' has {len(running_runs)} RUNNING execution(s). "
                    f"Latest running RunId: {run_id} — waiting 30s ..."
                )
            else:
                latest = runs[0]
                logger.info(
                    f"✅ No RUNNING executions in '{workflow_name}'. "
                    f"Latest run '{latest['WorkflowRunId']}' is in terminal state: {latest.get('Status')}. "
                    f"Safe to proceed."
                )
                return True

        except Exception as e:
            logger.warning(f"Error checking Glue Workflow status: {e}")

        time.sleep(30)

    logger.error(f"❌ Timed out ({timeout_minutes}min) waiting for Glue Workflow '{workflow_name}' to finish.")
    sys.exit(1)


# ── Silver readiness check ────────────────────────────────────────────────────

def check_silver_all_datasets(s3, silver_bucket):
    """
    Verify that ALL required Silver datasets were written by bronze_to_silver.
    Checks that each dataset folder (business, review, user, checkin) has
    at least one .parquet file — a single object is NOT enough.

    Root cause note: The old check_silver_ready() returned True with just 1
    object anywhere in silver/, which meant stale files from a previous run
    would pass even if the current bronze_to_silver hadn't finished writing.
    This caused silver_to_gold to start concurrently with bronze_to_silver,
    which was actively overwriting those files with mode("overwrite") — leading
    to "No such file or directory" errors on Spark task execution.
    """
    logger.info(f"Verifying all Silver datasets in s3://{silver_bucket}/silver/ ...")
    all_ready = True

    for dataset in REQUIRED_SILVER_DATASETS:
        prefix = f"silver/{dataset}/"
        try:
            resp = s3.list_objects_v2(Bucket=silver_bucket, Prefix=prefix, MaxKeys=5)
            parquet_files = [
                obj for obj in resp.get("Contents", [])
                if obj["Key"].endswith(".parquet") or obj["Key"].endswith(".snappy.parquet")
            ]
            if parquet_files:
                logger.info(f"  ✅ {dataset}: {len(parquet_files)}+ parquet files found under {prefix}")
            else:
                logger.warning(f"  ⚠️  {dataset}: No parquet files found under {prefix}")
                all_ready = False
        except Exception as e:
            logger.warning(f"  ⚠️  Could not check {prefix}: {e}")
            all_ready = False

    return all_ready


# ── Gold verification ─────────────────────────────────────────────────────────

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


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    glue = boto3.client("glue", region_name=REGION)
    s3   = boto3.client("s3",   region_name=REGION)

    # Allow overriding names via CLI args (used by GitHub Actions)
    gold_job_name       = sys.argv[1] if len(sys.argv) > 1 else SILVER_TO_GOLD_JOB_NAME
    gold_crawler_name   = sys.argv[2] if len(sys.argv) > 2 else GOLD_CRAWLER_NAME
    gold_bucket         = sys.argv[3] if len(sys.argv) > 3 else GOLD_BUCKET_NAME
    silver_crawler_name = sys.argv[4] if len(sys.argv) > 4 else SILVER_CRAWLER_NAME
    workflow_name       = sys.argv[5] if len(sys.argv) > 5 else GLUE_WORKFLOW_NAME

    # ── Step 1: Wait for Glue Workflow to finish (CRITICAL) ──────────────────
    # ingest.py fires the Glue Workflow and does NOT wait for it to finish.
    # If we don't wait here, bronze_to_silver may still be writing Silver files
    # with mode("overwrite") while silver_to_gold tries to read them — causing
    # "No such file or directory" errors on specific Parquet part files.
    logger.info("=" * 65)
    logger.info("STEP 1: Waiting for Glue Workflow to complete (if running)...")
    logger.info("=" * 65)
    wait_for_glue_workflow(glue, workflow_name)

    # ── Step 2: Verify all 4 Silver datasets are fully written ───────────────
    logger.info("=" * 65)
    logger.info("STEP 2: Verifying all Silver datasets are present ...")
    logger.info("=" * 65)
    if not check_silver_all_datasets(s3, SILVER_BUCKET_NAME):
        logger.info("Silver layer incomplete. Running bronze_to_silver Glue job...")
        start_and_wait_glue_job(glue, BRONZE_TO_SILVER_JOB)
        # Re-check after job completes
        if not check_silver_all_datasets(s3, SILVER_BUCKET_NAME):
            logger.error("❌ Silver datasets still missing after bronze_to_silver. Aborting.")
            sys.exit(1)

    # ── Step 3: Run Silver Crawler → catalog fresh Silver schema ─────────────
    logger.info("=" * 65)
    logger.info("STEP 3: Running Silver Crawler to catalog latest Silver schema...")
    logger.info("=" * 65)
    start_and_wait_crawler(glue, silver_crawler_name)

    # ── Step 4: Run Silver → Gold job ────────────────────────────────────────
    logger.info("=" * 65)
    logger.info("STEP 4: Running Silver-to-Gold Glue Job (BI + ML + RAG)...")
    logger.info("=" * 65)
    start_and_wait_glue_job(glue, gold_job_name)

    # ── Step 5: Run Gold Crawler → catalog Gold tables ───────────────────────
    logger.info("=" * 65)
    logger.info("STEP 5: Running Gold Crawler to catalog Gold schema...")
    logger.info("=" * 65)
    start_and_wait_crawler(glue, gold_crawler_name)

    # ── Step 6: Verify Gold S3 output ────────────────────────────────────────
    logger.info("=" * 65)
    logger.info("STEP 6: Verifying Gold S3 output...")
    logger.info("=" * 65)
    verify_gold_s3_data(s3, gold_bucket)

    logger.info("🏆 Full Silver → Gold pipeline completed successfully!")


if __name__ == "__main__":
    main()
