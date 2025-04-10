# src/ateda_platform/orchestration/assets/ingestion_assets.py
import os
# import requests - No longer needed for download
import subprocess # Needed for calling aria2c
import tarfile
import logging
from dagster import asset, get_dagster_logger, Output, AssetIn, AssetExecutionContext
from dagster_aws.s3 import S3Resource
from io import BytesIO
import tempfile
from pathlib import Path
from datetime import date
import shutil # For directory cleanup
from ateda_platform.partitions import daily_partitions

# Use the UW public alert archive URL
ZTF_ALERT_ARCHIVE_BASE_URL = "https://ztf.uw.edu/alerts/public"
# Prefix now just represents structure *within* the bronze bucket
BRONZE_ZTF_ALERTS_DATE_PREFIX = "ztf_alerts"
# Keep prefix for MinIO simple, the tarball structure will handle subdirs
ZTF_RAW_PREFIX = "alerts"

logger = get_dagster_logger()

@asset(
    group_name="ingestion",
    description="Downloads a ZTF public alert archive tarball for a specific date using aria2c.",
    io_manager_key="io_manager",
    partitions_def=daily_partitions
)
def ztf_alert_archive_tarball(context: AssetExecutionContext) -> Path:
    """
    Downloads a ZTF public alert archive tarball for a given date using aria2c
    to a *local temporary directory* within the run container.
    Returns the Path object to the downloaded file.
    The configured IO Manager will handle moving this to shared storage.
    """
    # Get the date from the partition key
    partition_date_str = context.partition_key
    context.log.info(f"Processing partition: {partition_date_str}")
    # date_str = partition_date_str # Already in YYYY-MM-DD format from partition key
    # Convert YYYY-MM-DD to YYYYMMDD for the filename
    filename_date_str = partition_date_str.replace("-", "")

    filename = f"ztf_public_{filename_date_str}.tar.gz"
    archive_url = f"{ZTF_ALERT_ARCHIVE_BASE_URL}/{filename}"

    temp_dir = tempfile.mkdtemp() # Create a local temp directory
    temp_file_path = Path(temp_dir) / filename
    logger.info(f"Downloading {archive_url} to local temp {temp_file_path} using aria2c")

    # Construct aria2c command
    # -c: continue download if interrupted
    # -x 4: use up to 4 connections per server
    # -s 4: split download into 4 parts
    # --dir: specify download directory
    # --out: specify output filename
    # --log-level=warn: control aria2c logging verbosity
    # --log=- : redirect aria2c log to stdout (so we can capture it)
    # --show-console-readout=false : cleaner progress info for parsing
    # --summary-interval=30 : show summary every 30 seconds
    cmd = [
        "aria2c",
        "-c",
        "-x", "4",
        "-s", "4",
        "--dir", str(temp_dir),
        "--out", filename,
        "--log-level=warn",
        "--log=-",
        "--show-console-readout=false",
        "--summary-interval=30",
        archive_url
    ]

    logger.info(f"Executing command: {' '.join(cmd)}")

    # Use Popen to stream output
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, # Redirect stderr to stdout
        text=True,
        bufsize=1, # Line buffered
        encoding='utf-8'
    )

    # Stream stdout/stderr from aria2c to Dagster logs
    if process.stdout:
        for line in iter(process.stdout.readline, ''):
            # Strip trailing newline and log
            logger.info(f"[aria2c] {line.rstrip()}")
        process.stdout.close()

    # Wait for the process to finish and get the return code
    return_code = process.wait()

    if return_code == 0:
        logger.info(f"aria2c completed successfully. Local file is at {temp_file_path}")
        # Return the Path object to the file in the *local* temp dir.
        # The IO Manager will take this Path and store its contents.
        return temp_file_path
    else:
        error_msg = f"aria2c failed with return code {return_code} for URL {archive_url}"
        logger.error(error_msg)
        # Clean up local temp directory on failure
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up local temp directory after failure: {temp_dir}")
        except OSError as e:
            logger.error(f"Error removing local temp directory {temp_dir} after failure: {e}")
        raise Exception(error_msg)
    # No finally block here - IO Manager might need the file. Cleanup happens elsewhere.
    # We rely on the run container being removed.


@asset(
    group_name="ingestion",
    description="Extracts Avro alert files from a ZTF tarball (read via IO manager) and uploads them to the Bronze bucket.",
    ins={"alert_tarball": AssetIn(key=ztf_alert_archive_tarball.key)},
    io_manager_key="io_manager",
    partitions_def=daily_partitions
)
def raw_ztf_avro_alert_files(context: AssetExecutionContext, alert_tarball: Path, s3: S3Resource) -> list[str]:
    """
    Extracts Avro files from the alert tarball and uploads them to the Bronze bucket
    (s3://<S3_BRONZE_BUCKET>/ztf_alerts/YYYYMMDD/alert.avro).
    """
    # Get BRONZE bucket name from environment
    bucket_name = os.getenv("S3_BRONZE_BUCKET")
    if not bucket_name:
        raise ValueError("S3_BRONZE_BUCKET environment variable not set!")

    # Get the date string (YYYYMMDD) from the partition key for the S3 prefix
    partition_date_str = context.partition_key # Format: YYYY-MM-DD
    s3_date_prefix = partition_date_str.replace("-", "")
    context.log.info(f"Using partition date {partition_date_str} -> S3 date prefix {s3_date_prefix}")

    uploaded_keys = []
    s3_client = s3.get_client()
    temp_file_path = alert_tarball
    logger.info(f"Processing alert tarball from IO Manager path: {temp_file_path}")
    logger.info(f"Target S3 Bucket (Bronze): {bucket_name}")

    try:
        with tarfile.open(temp_file_path, "r:gz") as tar:
            members = tar.getmembers()
            logger.info(f"Found {len(members)} members in the tarball.")
            for member in members:
                if member.isfile() and member.name.endswith(".avro"):
                    file_obj = tar.extractfile(member)
                    if file_obj:
                        file_content = file_obj.read()
                        # Construct target key: prefix/YYYYMMDD/alert_filename.avro
                        target_key = f"{BRONZE_ZTF_ALERTS_DATE_PREFIX}/{s3_date_prefix}/{member.name}"

                        logger.info(f"Uploading {member.name} to s3://{bucket_name}/{target_key}")
                        try:
                            s3_client.put_object(
                                Bucket=bucket_name,
                                Key=target_key,
                                Body=file_content,
                            )
                            uploaded_keys.append(target_key)
                        except Exception as e:
                            logger.error(f"Error uploading {target_key} to S3 Bucket {bucket_name}: {e}")
                            continue
                    else:
                        logger.warning(f"Could not extract file content for member: {member.name}")

        logger.info(f"Finished processing tarball. Uploaded {len(uploaded_keys)} Avro files to Bronze bucket.")
        return uploaded_keys

    except tarfile.TarError as e:
        logger.error(f"Error opening or reading tarfile {temp_file_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during extraction/upload: {e}")
        raise
    # No finally block needed here - the IO Manager handles the lifecycle
    # of the file in shared storage. 