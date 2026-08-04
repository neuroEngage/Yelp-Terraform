import os
import sys
import glob
import shutil
import zipfile
import subprocess
import logging
import traceback
import requests
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("yelp_ingestion")

DATASET_NAME = "adamamer2001/yelp-complete-open-dataset-2024"
TEMP_DIR     = os.path.join(os.getcwd(), "temp_yelp_data")

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
        logger.error("KAGGLE_USERNAME or KAGGLE_KEY not set in environment.")
        sys.exit(1)

    kaggle_dir  = os.path.expanduser("~/.kaggle")
    kaggle_json = os.path.join(kaggle_dir, "kaggle.json")
    os.makedirs(kaggle_dir, exist_ok=True)
    with open(kaggle_json, "w") as f:
        f.write(f'{{"username":"{username}","key":"{key}"}}')
    os.chmod(kaggle_json, 0o600)
    logger.info(f"Kaggle credentials written for user: {username}")
    return username, key

# ──────────────────────────────────────────────────────────────
def download_via_http_direct(username, key):
    """Bypasses Kaggle CLI bugs by querying the Kaggle REST API directly over HTTP stream."""
    url = f"https://www.kaggle.com/api/v1/datasets/download/{DATASET_NAME}"
    logger.info(f"Direct HTTP streaming download from: {url}")
    
    zip_path = os.path.join(TEMP_DIR, "yelp_dataset.zip")
    
    try:
        response = requests.get(
            url,
            auth=(username, key),
            stream=True,
            allow_redirects=True,
            timeout=30
        )
        logger.info(f"Kaggle HTTP API response status code: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"❌ Kaggle Direct HTTP API failed with Status {response.status_code}")
            logger.error(f"Response snippet: {response.text[:500]}")
            return False

        content_length = response.headers.get('content-length')
        total_size = int(content_length) if content_length else 0
        logger.info(f"Downloading stream (Size: {total_size >> 20} MB) ...")

        downloaded = 0
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1048576): # 1MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0 and downloaded % (50 * 1048576) == 0:
                        logger.info(f"  Downloaded: {downloaded >> 20} MB / {total_size >> 20} MB ({(downloaded/total_size)*100:.1f}%)")
        
        logger.info(f"Direct HTTP download completed successfully: {zip_path}")
        return True

    except Exception as e:
        logger.error(f"HTTP Direct Download failed: {e}")
        logger.error(traceback.format_exc())
        return False

# ──────────────────────────────────────────────────────────────
def download_dataset(username, key):
    os.makedirs(TEMP_DIR, exist_ok=True)
    log_disk()

    logger.info(f"Downloading dataset '{DATASET_NAME}' via Direct Kaggle REST API stream ...")
    success = download_via_http_direct(username, key)

    if not success:
        logger.warning("Direct HTTP REST API stream failed. Attempting Kaggle CLI as fallback...")
        cmd = [
            "kaggle", "datasets", "download",
            "-d", DATASET_NAME,
            "-p", TEMP_DIR,
            "--force",
        ]
        logger.info(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=False, text=True)

        if result.returncode != 0:
            logger.error("❌ Both Direct Kaggle REST API stream and Kaggle CLI download failed.")
            sys.exit(1)

    logger.info("Download completed successfully.")
    log_disk()

# ──────────────────────────────────────────────────────────────
def extract_dataset():
    logger.info("Extracting ZIP archive ...")
    zip_files = glob.glob(os.path.join(TEMP_DIR, "*.zip"))
    if not zip_files:
        logger.error("No ZIP file found after download.")
        sys.exit(1)

    for zf in zip_files:
        logger.info(f"Unzipping {zf} ...")
        with zipfile.ZipFile(zf, "r") as z:
            members = [
                m for m in z.namelist()
                if not m.startswith("photos/") and not m.endswith((".jpg", ".jpeg", ".png"))
            ]
            z.extractall(TEMP_DIR, members=members)
            logger.info(f"Extracted {len(members)} entries (photos/ images excluded).")
        os.remove(zf)

    log_disk()

# ──────────────────────────────────────────────────────────────
def upload_to_s3(bucket_name):
    s3 = boto3.client("s3")
    logger.info(f"Uploading JSON files to s3://{bucket_name}/ ...")
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
        logger.error("❌ No expected JSON files were found or uploaded.")
        sys.exit(1)

    logger.info(f"Upload complete — {uploaded} raw JSON files uploaded to S3 Bronze.")

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
        username, key = setup_kaggle_credentials()
        download_dataset(username, key)
        extract_dataset()
        upload_to_s3(bronze_bucket)
        if trigger_glue:
            trigger_glue_workflow(workflow_name)
    finally:
        cleanup()

if __name__ == "__main__":
    main()
