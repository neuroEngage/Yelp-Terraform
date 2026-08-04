import os
import sys
import glob
import shutil
import zipfile
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
DATASET_NAME  = "adamamer2001/yelp-complete-open-dataset-2024"
TEMP_DIR      = os.path.join(os.getcwd(), "temp_yelp_data")

# These are the files we want in the bronze bucket
# Mapped as: local filename → s3 key inside bronze bucket
FILE_MAPPINGS = {
    # yelp_dataset/ subfolder files
    "yelp_academic_dataset_business.json" : "yelp_academic_dataset_business.json",
    "yelp_academic_dataset_review.json"   : "yelp_academic_dataset_review.json",
    "yelp_academic_dataset_user.json"     : "yelp_academic_dataset_user.json",
    "yelp_academic_dataset_tip.json"      : "yelp_academic_dataset_tip.json",
    "yelp_academic_dataset_checkin.json"  : "yelp_academic_dataset_checkin.json",
    # yelp_photos/ subfolder file (metadata only, no images)
    "photos.json"                         : "photos.json",
}

def log_disk_space():
    total, used, free = shutil.disk_usage("/")
    logger.info(f"Disk Space — Total: {total // (2**30)} GB | Used: {used // (2**30)} GB | Free: {free // (2**30)} GB")

def setup_kaggle_credentials():
    username = os.getenv("KAGGLE_USERNAME")
    key      = os.getenv("KAGGLE_KEY")
    if not username or not key:
        logger.error("KAGGLE_USERNAME or KAGGLE_KEY not set in environment.")
        sys.exit(1)
    # Write kaggle.json so the library picks it up
    kaggle_dir = os.path.expanduser("~/.kaggle")
    os.makedirs(kaggle_dir, exist_ok=True)
    kaggle_json = os.path.join(kaggle_dir, "kaggle.json")
    with open(kaggle_json, "w") as f:
        f.write(f'{{"username":"{username}","key":"{key}"}}')
    os.chmod(kaggle_json, 0o600)
    logger.info(f"Kaggle credentials written for user: {username}")

def download_dataset():
    os.makedirs(TEMP_DIR, exist_ok=True)
    log_disk_space()
    logger.info(f"Downloading Kaggle dataset '{DATASET_NAME}' ...")
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(DATASET_NAME, path=TEMP_DIR, unzip=False, quiet=False)
        logger.info("Dataset download complete.")
        log_disk_space()
    except Exception as e:
        logger.error(f"Kaggle download failed: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

def extract_dataset():
    logger.info("Extracting ZIP archive ...")
    zip_files = glob.glob(os.path.join(TEMP_DIR, "*.zip"))
    if not zip_files:
        logger.error("No ZIP file found after download.")
        sys.exit(1)
    for zf in zip_files:
        logger.info(f"Unzipping {zf} ...")
        with zipfile.ZipFile(zf, "r") as z:
            # Extract everything EXCEPT the photos/ image folder
            members = [
                m for m in z.namelist()
                if not m.startswith("photos/") and not m.endswith((".jpg", ".jpeg", ".png"))
            ]
            z.extractall(TEMP_DIR, members=members)
            logger.info(f"Extracted {len(members)} entries (photos/ images excluded).")
        os.remove(zf)
    log_disk_space()

def upload_to_s3_bronze(bucket_name):
    s3 = boto3.client("s3")
    logger.info(f"Uploading JSON files to s3://{bucket_name}/ ...")
    uploaded = 0
    for root, _, files in os.walk(TEMP_DIR):
        for fname in files:
            if fname in FILE_MAPPINGS:
                local_path = os.path.join(root, fname)
                s3_key     = FILE_MAPPINGS[fname]
                logger.info(f"  Uploading {fname} → s3://{bucket_name}/{s3_key}")
                s3.upload_file(local_path, bucket_name, s3_key)
                uploaded += 1
    if uploaded == 0:
        logger.warning("No expected files were found/uploaded. Check the zip structure.")
    else:
        logger.info(f"Upload complete. {uploaded} files uploaded to Bronze bucket.")

def cleanup():
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
        logger.info("Temporary files cleaned up.")

def trigger_glue_workflow(workflow_name):
    glue = boto3.client("glue", region_name="us-east-1")
    logger.info(f"Triggering Glue Workflow: {workflow_name} ...")
    try:
        resp   = glue.start_workflow_run(Name=workflow_name)
        run_id = resp.get("RunId")
        logger.info(f"Glue Workflow started. RunId: {run_id}")
    except ClientError as e:
        logger.error(f"Failed to start Glue Workflow: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

def main():
    bronze_bucket  = os.getenv("BRONZE_BUCKET_NAME", "yelp-bronze-raw-us-east-1")
    workflow_name  = os.getenv("GLUE_WORKFLOW_NAME",  "yelp-bigdata_etl_workflow")
    trigger_glue   = os.getenv("TRIGGER_GLUE", "true").lower() == "true"

    try:
        setup_kaggle_credentials()
        download_dataset()
        extract_dataset()
        upload_to_s3_bronze(bronze_bucket)
        if trigger_glue:
            trigger_glue_workflow(workflow_name)
    finally:
        cleanup()

if __name__ == "__main__":
    main()
