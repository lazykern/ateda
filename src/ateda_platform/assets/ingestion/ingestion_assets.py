# src/ateda_platform/orchestration/assets/ingestion_assets.py
import os
# import requests - No longer needed for download
import subprocess # Needed for calling aria2c
import tarfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import tempfile
from pathlib import Path
from datetime import date
import shutil # For directory cleanup

from dagster import DailyPartitionsDefinition, asset, get_dagster_logger, Output, AssetIn, AssetExecutionContext, Config
from dagster_aws.s3 import S3Resource

# Import the shared partitions definition
from ...partitions import daily_partitions

ZTF_ALERT_ARCHIVE_BASE_URL = "https://ztf.uw.edu/alerts/public"

logger = get_dagster_logger()

@asset(
    group_name="ingestion",
    description="Downloads a ZTF public alert archive tarball for a specific date using aria2c.",
    io_manager_key="io_manager",
    partitions_def=daily_partitions,
    kinds={"shell"}
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


class RawZtfAvroConfig(Config):
    """Configuration for the raw_ztf_avro_alert_files asset."""
    s3_prefix: str = "ztf_alerts"

def _upload_to_s3(s3_client, bucket: str, key: str, file_content: bytes, logger) -> tuple[str, bool]:
    """Helper function to upload a single file to S3.
    Returns a tuple of (key, success)."""
    try:
        file_obj = BytesIO(file_content)
        logger.info(f"Starting upload: s3://{bucket}/{key}")
        s3_client.upload_fileobj(
            file_obj,
            bucket,
            key,
            ExtraArgs={'ContentType': 'avro/binary'}
        )
        logger.info(f"Successfully uploaded: s3://{bucket}/{key}")
        return key, True
    except Exception as e:
        logger.error(f"Failed to upload s3://{bucket}/{key}: {e}")
        return key, False

@asset(
    group_name="ingestion",
    description="Extracts Avro alert files from a ZTF tarball (read via IO manager) and uploads them to the Bronze bucket using Hive-style partitioning. The S3 prefix is configurable.",
    ins={"alert_tarball": AssetIn(key=ztf_alert_archive_tarball.key)},
    io_manager_key="io_manager",
    partitions_def=daily_partitions,
    kinds={"python", "avro", "s3"}
)
def raw_ztf_avro_alert_files(context: AssetExecutionContext, config: RawZtfAvroConfig, alert_tarball: Path, s3: S3Resource) -> list[str]:
    """
    Extracts Avro files from the alert tarball and uploads them to the Bronze bucket
    using Hive-style partitioning (s3://<S3_BRONZE_BUCKET>/ztf_alerts/year=YYYY/month=MM/day=DD/alert.avro).
    Now uses parallel uploads for better performance.
    """
    bucket_name = os.getenv("S3_BRONZE_BUCKET")
    if not bucket_name:
        raise ValueError("S3_BRONZE_BUCKET environment variable not set!")

    # Get the date parts from the partition key for the S3 prefix
    partition_date_str = context.partition_key
    try:
        partition_date = date.fromisoformat(partition_date_str)
        year = partition_date.year
        month = f"{partition_date.month:02d}"
        day = f"{partition_date.day:02d}"
        s3_partition_prefix = f"year={year}/month={month}/day={day}"
        context.log.info(f"Using partition date {partition_date_str} -> S3 partition prefix {s3_partition_prefix}")
    except ValueError:
        context.log.error(f"Invalid date format in partition key: {partition_date_str}")
        raise

    uploaded_keys = []
    failed_keys = []
    s3_client = s3.get_client()
    temp_file_path = alert_tarball
    logger.info(f"Processing alert tarball from IO Manager path: {temp_file_path}")
    logger.info(f"Target S3 Bucket (Bronze): {bucket_name}")

    try:
        with tarfile.open(temp_file_path, "r:gz") as tar:
            members = [m for m in tar.getmembers() if m.isfile() and m.name.endswith(".avro")]
            logger.info(f"Found {len(members)} Avro files in the tarball.")
            
            # Create a thread pool for parallel uploads
            # Number of workers: min(32, number of files) to avoid creating too many threads
            max_workers = min(32, len(members))
            logger.info(f"Using ThreadPoolExecutor with {max_workers} workers for parallel uploads")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Dictionary to track futures
                future_to_key = {}
                
                # Submit all upload tasks
                for member in members:
                    file_obj = tar.extractfile(member)
                    if file_obj:
                        file_content = file_obj.read()
                        target_key = f"{config.s3_prefix}/{s3_partition_prefix}/{member.name}"
                        
                        # Submit the upload task
                        future = executor.submit(
                            _upload_to_s3,
                            s3_client,
                            bucket_name,
                            target_key,
                            file_content,
                            logger
                        )
                        future_to_key[future] = target_key
                    else:
                        logger.warning(f"Could not extract file content for member: {member.name}")
                
                # Process completed uploads as they finish
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        _, success = future.result()
                        if success:
                            uploaded_keys.append(key)
                        else:
                            failed_keys.append(key)
                    except Exception as e:
                        logger.error(f"Unexpected error processing upload result for {key}: {e}")
                        failed_keys.append(key)

        # Final status report
        logger.info(f"Upload complete. Successfully uploaded {len(uploaded_keys)} files.")
        if failed_keys:
            logger.warning(f"Failed to upload {len(failed_keys)} files: {', '.join(failed_keys)}")
        
        return uploaded_keys

    except tarfile.TarError as e:
        logger.error(f"Error opening or reading tarfile {temp_file_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during extraction/upload: {e}")
        raise
    # No finally block needed here - the IO Manager handles the lifecycle
    # of the file in shared storage.
