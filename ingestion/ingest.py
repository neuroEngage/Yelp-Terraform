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

# Exact Kaggle dataset identifier
DATASET_NAME = "adamamer2001/yelp-complete-open-dataset-2024"
TEMP_DIR     = os.path.join(os.getcwd(), "temp_yelp_data")

# Files we want → S3 keys they map to
FILE_MAPPINGS = {
    "yelp_academic_dataset_business.json" : "yelp_academic_dataset_business.json",
    "yelp_academic_dataset_review.json"   : "yelp_academic_dataset_review.json",
    "yelp_academic_dataset_user.json"     : "yelp_academic_dataset_user.json",
    "yelp_academic_dataset_tip.json"      : "yelp_academic_dataset_tip.json",
    "yelp_academic_dataset_checkin.json"  : "yelp_academic_dataset_checkin.json",
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
        logger.error("KAGGLE_USERNAME or KAGGLE_KEY not set.")
        sys.exit(1)

    kaggle_dir  = os.path.expanduser("~/.kaggle")
    kaggle_json = os.path.join(kaggle_dir, "kaggle.json")
    os.makedirs(kaggle_dir, exist_ok=True)
    with open(kaggle_json, "w") as f:
        f.write(f'{{"username":"{username}","key":"{key}"}}')
    os.chmod(kaggle_json, 0o600)
    logger.info(f"Kaggle credentials written for user: {username}")

# ──────────────────────────────────────────────────────────────
def download_dataset():
    os.makedirs(TEMP_DIR, exist_ok=True)
    log_disk()

    logger.info(f"Downloading '{DATASET_NAME}' via Kaggle CLI ...")

    cmd = [
        "kaggle", "datasets", "download",
        "--dataset", DATASET_NAME,
        "--path",    TEMP_DIR,
        "--unzip",   # extract automatically — no manual zip handling
        "--force",   # overwrite if already exists
    ]

    logger.info(f"Running: {' '.join(cmd)}")

    # stream output live so GitHub Actions shows progress
    result = subprocess.run(cmd, capture_output=False, text=True)

    if result.returncode != 0:
        logger.error("Kaggle CLI download failed.")
        logger.error(
            "Possible causes:\n"
            "  1. Dataset terms not accepted — visit the dataset page on\n"
            f"     https://www.kaggle.com/datasets/{DATASET_NAME}\n"
            "     and click Download / Accept terms.\n"
            "  2. Wrong KAGGLE_USERNAME or KAGGLE_KEY secret.\n"
            "  3. Dataset ID changed or was removed."
        )
        sys.exit(1)

    logger.info("Download and extraction complete.")
    log_disk()

    # Log what was extracted
    for root, dirs, files in os.walk(TEMP_DIR):
        for f in files:
            full = os.path.join(root, f)
            size = os.path.getsize(full) >> 20   # MB
            logger.info(f"  Extracted: {full} ({size} MB)")

# ──────────────────────────────────────────────────────────────
def upload_to_s3(bucket_name):
    s3 = boto3.client("s3")
    logger.info(f"Uploading to s3://{bucket_name}/ ...")
    uploaded = 0

    for root, _, files in os.walk(TEMP_DIR):
        for fname in files:
            if fname in FILE_MAPPINGS:
                local = os.path.join(root, fname)
                key   = FILE_MAPPINGS[fname]
                size  = os.path.getsize(local) >> 20
                logger.info(f"  {fname} ({size} MB) → s3://{bucket_name}/{key}")
                s3.upload_file(local, bucket_name, key)
                uploaded += 1

    if uploaded == 0:
        # Log what IS in the temp dir so we can debug
        logger.warning("No expected files matched FILE_MAPPINGS.")
        logger.warning("Files found in temp dir:")
        for root, _, files in os.walk(TEMP_DIR):
            for f in files:
                logger.warning(f"  {os.path.join(root, f)}")
        sys.exit(1)

    logger.info(f"Upload complete — {uploaded} files.")

# ──────────────────────────────────────────────────────────────
def trigger_glue_workflow(workflow_name):
    glue = boto3.client("glue", region_name="us-east-1")
    logger.info(f"Triggering Glue Workflow: {workflow_name}")
    try:
        resp   = glue.start_workflow_run(Name=workflow_name)
        run_id = resp.get("RunId")
        logger.info(f"Workflow started — RunId: {run_id}")
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
