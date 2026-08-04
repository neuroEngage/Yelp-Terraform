import os
import sys
import glob
import shutil
import subprocess
import logging
import traceback
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("yelp_ingestion")

# Kaggle dataset identifiers (primary and fallback)
PRIMARY_DATASET  = os.getenv("KAGGLE_DATASET_NAME", "adamamer2001/yelp-complete-open-dataset-2024")
FALLBACK_DATASET = "yelp-dataset/yelp-dataset"

TEMP_DIR = os.path.join(os.getcwd(), "temp_yelp_data")

# Maps local filename → S3 key inside bronze bucket
# Supports both full names (yelp_academic_dataset_*.json) and short names (*.json)
FILE_MAPPINGS = {
    # Full dataset naming
    "yelp_academic_dataset_business.json" : "yelp_academic_dataset_business.json",
    "yelp_academic_dataset_review.json"   : "yelp_academic_dataset_review.json",
    "yelp_academic_dataset_user.json"     : "yelp_academic_dataset_user.json",
    "yelp_academic_dataset_tip.json"      : "yelp_academic_dataset_tip.json",
    "yelp_academic_dataset_checkin.json"  : "yelp_academic_dataset_checkin.json",
    # Short dataset naming fallback
    "business.json"                       : "yelp_academic_dataset_business.json",
    "review.json"                         : "yelp_academic_dataset_review.json",
    "user.json"                           : "yelp_academic_dataset_user.json",
    "tip.json"                            : "yelp_academic_dataset_tip.json",
    "checkin.json"                        : "yelp_academic_dataset_checkin.json",
    "photos.json"                         : "photos.json",
}

# ──────────────────────────────────────────────────────────────
def log_disk():
    total, used, free = shutil.disk_usage("/")
    logger.info(
        f"Disk — Total: {total >> 30} GB | "
        f"Used: {used >> 30} GB | "
        f"Free: {free >> 30} GB"
    )

# ──────────────────────────────────────────────────────────────
def setup_kaggle_credentials():
    username = os.getenv("KAGGLE_USERNAME")
    key      = os.getenv("KAGGLE_KEY")
    if not username or not key:
        logger.error("KAGGLE_USERNAME or KAGGLE_KEY environment variables not set.")
        sys.exit(1)

    kaggle_dir  = os.path.expanduser("~/.kaggle")
    kaggle_json = os.path.join(kaggle_dir, "kaggle.json")
    os.makedirs(kaggle_dir, exist_ok=True)
    with open(kaggle_json, "w") as f:
        f.write(f'{{"username":"{username}","key":"{key}"}}')
    os.chmod(kaggle_json, 0o600)
    logger.info(f"Kaggle credentials written for user: {username}")

# ──────────────────────────────────────────────────────────────
def run_download_cmd(dataset_slug):
    logger.info(f"Attempting download for dataset: '{dataset_slug}' ...")
    cmd = [
        "kaggle", "datasets", "download",
        "--dataset", dataset_slug,
        "--path",    TEMP_DIR,
        "--unzip",
        "--force",
    ]
    logger.info(f"Executing: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False, text=True)
    return result.returncode == 0

# ──────────────────────────────────────────────────────────────
def download_dataset():
    os.makedirs(TEMP_DIR, exist_ok=True)
    log_disk()

    # Try primary dataset
    success = run_download_cmd(PRIMARY_DATASET)
    
    # If primary failed, try fallback dataset
    if not success and PRIMARY_DATASET != FALLBACK_DATASET:
        logger.warning(f"Primary dataset '{PRIMARY_DATASET}' download failed. Trying fallback '{FALLBACK_DATASET}' ...")
        success = run_download_cmd(FALLBACK_DATASET)

    if not success:
        logger.error("❌ Kaggle dataset download failed for both primary and fallback datasets.")
        logger.error(
            "\n"
            "======================================================================\n"
            "CRITICAL: KAGGLE DATASET TERMS ACCEPTANCE REQUIRED\n"
            "======================================================================\n"
            "Kaggle returns 'Error: The operation was canceled' when the Kaggle account\n"
            "has not accepted the dataset rules / license terms on the Kaggle website.\n\n"
            "TO FIX THIS:\n"
            "1. Log into Kaggle.com using the account for KAGGLE_USERNAME secret.\n"
            f"2. Visit: https://www.kaggle.com/datasets/{PRIMARY_DATASET}\n"
            f"   and https://www.kaggle.com/datasets/{FALLBACK_DATASET}\n"
            "3. Click 'Download' or 'Accept Rules/Terms' on the dataset page.\n"
            "4. Re-run the GitHub Actions workflow.\n"
            "======================================================================\n"
        )
        sys.exit(1)

    logger.info("Download and extraction complete.")
    log_disk()

    # Log extracted files
    for root, dirs, files in os.walk(TEMP_DIR):
        for f in files:
            full = os.path.join(root, f)
            size = os.path.getsize(full) >> 20   # MB
            logger.info(f"  Extracted: {full} ({size} MB)")

# ──────────────────────────────────────────────────────────────
def upload_to_s3(bucket_name):
    s3 = boto3.client("s3")
    logger.info(f"Uploading files to s3://{bucket_name}/ ...")
    uploaded = 0

    for root, _, files in os.walk(TEMP_DIR):
        for fname in files:
            if fname in FILE_MAPPINGS:
                local = os.path.join(root, fname)
                key   = FILE_MAPPINGS[fname]
                size  = os.path.getsize(local) >> 20
                logger.info(f"  Uploading {fname} ({size} MB) → s3://{bucket_name}/{key}")
                s3.upload_file(local, bucket_name, key)
                uploaded += 1

    if uploaded == 0:
        logger.error("❌ No matching JSON files found in extracted data.")
        logger.error("Files present in temp dir:")
        for root, _, files in os.walk(TEMP_DIR):
            for f in files:
                logger.error(f"  {os.path.join(root, f)}")
        sys.exit(1)

    logger.info(f"Upload complete — {uploaded} files uploaded to S3 Bronze.")

# ──────────────────────────────────────────────────────────────
def trigger_glue_workflow(workflow_name):
    glue = boto3.client("glue", region_name="us-east-1")
    logger.info(f"Triggering AWS Glue Workflow: {workflow_name}")
    try:
        resp   = glue.start_workflow_run(Name=workflow_name)
        run_id = resp.get("RunId")
        logger.info(f"Glue Workflow started — RunId: {run_id}")
    except ClientError as e:
        logger.error(f"Glue trigger failed: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

# ──────────────────────────────────────────────────────────────
def cleanup():
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
        logger.info("Temp dir cleaned up.")

# ──────────────────────────────────────────────────────────────
def main():
    bronze_bucket = os.getenv("BRONZE_BUCKET_NAME", "yelp-bronze-raw-us-east-1")
    workflow_name = os.getenv("GLUE_WORKFLOW_NAME",  "yelp-bigdata_etl_workflow")
    trigger_glue  = os.getenv("TRIGGER_GLUE", "true").lower() == "true"

    try:
        setup_kaggle_credentials()
        download_dataset()
        upload_to_s3(bronze_bucket)
        if trigger_glue:
            trigger_glue_workflow(workflow_name)
    finally:
        cleanup()

if __name__ == "__main__":
    main()
