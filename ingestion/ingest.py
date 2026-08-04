import os
import sys
import glob
import shutil
import zipfile
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("yelp_ingestion")

DATASET_NAME = "yelp-dataset/yelp-dataset"
TEMP_DIR = os.path.join(os.getcwd(), "temp_yelp_data")

EXPECTED_FILES = [
    "business.json",
    "review.json",
    "user.json",
    "tip.json",
    "checkin.json",
    "photos.json"
]

def setup_kaggle_credentials():
    username = os.getenv("KAGGLE_USERNAME")
    key = os.getenv("KAGGLE_KEY")
    if not username or not key:
        logger.warning("KAGGLE_USERNAME or KAGGLE_KEY environment variables not set.")
    else:
        logger.info(f"Kaggle API configured for user: {username}")

def download_dataset():
    os.makedirs(TEMP_DIR, exist_ok=True)
    logger.info(f"Downloading Kaggle dataset '{DATASET_NAME}' into {TEMP_DIR}...")
    
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(DATASET_NAME, path=TEMP_DIR, unzip=False)
        logger.info("Dataset downloaded successfully.")
    except Exception as e:
        logger.error(f"Failed to download Kaggle dataset: {str(e)}")        # If running in test mode without Kaggle API credentials, log warning and exit cleanly
        sys.exit(1)

def extract_and_clean():
    logger.info("Extracting downloaded zip archive...")
    zip_files = glob.glob(os.path.join(TEMP_DIR, "*.zip"))
    
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(TEMP_DIR)
        os.remove(zip_file)

    # Remove photos/ image folder if extracted, keeping photos.json
    photos_dir = os.path.join(TEMP_DIR, "photos")
    if os.path.exists(photos_dir) and os.path.isdir(photos_dir):
        logger.info("Removing image directory 'photos/' to save space while retaining photos.json metadata...")
        shutil.rmtree(photos_dir)

def upload_to_s3_bronze(bucket_name):
    s3_client = boto3.client("s3")
    logger.info(f"Uploading extracted JSON files to S3 bucket '{bucket_name}' under 'bronze/' prefix...")

    for root, _, files in os.walk(TEMP_DIR):
        for file in files:
            if file.endswith(".json"):
                local_path = os.path.join(root, file)
                # Standardize target s3 key name (strip prefixes like yelp_academic_dataset_)
                clean_name = file.replace("yelp_academic_dataset_", "")
                s3_key = f"bronze/{clean_name}"

                logger.info(f"Uploading {file} -> s3://{bucket_name}/{s3_key}")
                s3_client.upload_file(local_path, bucket_name, s3_key)

    logger.info("S3 Bronze upload completed successfully.")

def cleanup_temp_files():
    if os.path.exists(TEMP_DIR):
        logger.info(f"Cleaning up temporary directory {TEMP_DIR}...")
        shutil.rmtree(TEMP_DIR)

def trigger_glue_workflow(workflow_name):
    glue_client = boto3.client("glue")
    logger.info(f"Triggering AWS Glue Workflow: {workflow_name}...")
    try:
        response = glue_client.start_workflow_run(Name=workflow_name)
        run_id = response.get("RunId")
        logger.info(f"Successfully started Glue Workflow '{workflow_name}'. Run ID: {run_id}")
    except ClientError as e:
        logger.error(f"Error starting Glue Workflow: {e}")

def main():
    bucket_name = os.getenv("S3_BUCKET_NAME", "yelp-bigdata-datalake-us-east-1-dev")
    workflow_name = os.getenv("GLUE_WORKFLOW_NAME", "yelp-bigdata_etl_workflow")
    should_trigger = os.getenv("TRIGGER_GLUE_WORKFLOW", "true").lower() == "true"

    try:
        setup_kaggle_credentials()
        download_dataset()
        extract_and_clean()
        upload_to_s3_bronze(bucket_name)
        
        if should_trigger:
            trigger_glue_workflow(workflow_name)
    finally:
        cleanup_temp_files()

if __name__ == "__main__":
    main()
