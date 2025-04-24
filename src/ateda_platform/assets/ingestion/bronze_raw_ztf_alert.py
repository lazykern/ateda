import subprocess
import tarfile
import tempfile
from pathlib import Path
import os
import shutil
import sys
import hashlib
import urllib.request

from datetime import datetime

from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    ResourceParam,
)
from dagster_aws.s3 import S3Resource
import duckdb

from ...resources import S3Config
from ...partitions import daily_partitions
from ...resources import AWSConfig

ZTF_ALERT_ARCHIVE_BASE_URL = "https://ztf.uw.edu/alerts/public"
MD5SUMS_URL = f"{ZTF_ALERT_ARCHIVE_BASE_URL}/MD5SUMS"


logger = get_dagster_logger()

# --- Helper Functions ---
def calculate_md5(filepath: Path, chunk_size=8192) -> str:
    """Calculate MD5 hash of a file efficiently."""
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        logger.error(f"File not found when calculating MD5: {filepath}")
        return ""
    except Exception as e:
        logger.error(f"Error calculating MD5 for {filepath}: {e}")
        return ""

def get_expected_md5s(url: str) -> dict[str, str]:
    """Download and parse the MD5SUMS file."""
    expected_md5s = {}
    try:
        with urllib.request.urlopen(url) as response:
            if response.status == 200:
                lines = response.read().decode('utf-8').splitlines()
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 2:
                        md5_hash = parts[0]
                        # Handle potential path prefix in filename
                        filename = Path(parts[-1]).name
                        expected_md5s[filename] = md5_hash
            else:
                logger.error(f"Failed to download MD5SUMS from {url}. Status: {response.status}")
    except urllib.error.URLError as e:
        logger.error(f"Error accessing MD5SUMS URL {url}: {e}")
    except Exception as e:
        logger.error(f"Error processing MD5SUMS from {url}: {e}")
    return expected_md5s

# --- Assets ---
@asset(
    name="bronze_raw_ztf_alert",
    key_prefix=["bronze"],
    group_name="ingestion",
    description="Raw ZTF public alert archive tarball stored in the bronze zone.",
    partitions_def=daily_partitions,
    kinds={"shell", "s3"},
    metadata={
        "data_zone": "bronze",
        "source": "ztf_public_archive",
        "input_format": "tar.gz (avro)",
        "output_format": "parquet",
        "output_storage": "s3",
    },
)
def bronze_raw_ztf_alert(
    context: AssetExecutionContext,
    s3: ResourceParam[S3Resource],
    s3_config: ResourceParam[S3Config],
    aws_config: ResourceParam[AWSConfig],
) -> str:
    partition_date_str = context.partition_key
    date_obj = datetime.strptime(partition_date_str, "%Y-%m-%d")
    filename_date_str = date_obj.strftime("%Y%m%d")
    filename = f"ztf_public_{filename_date_str}.tar.gz"
    archive_url = f"{ZTF_ALERT_ARCHIVE_BASE_URL}/{filename}"

    bucket = s3_config.bronze_bucket
    if not bucket:
        logger.error("S3_BRONZE_BUCKET environment variable not set.")
        raise ValueError("S3_BRONZE_BUCKET environment variable not set.")

    prefix = f"alerts/year={date_obj.year:04d}/month={date_obj.month:02d}/day={date_obj.day:02d}"
    s3_path = f"s3://{bucket}/{prefix}"

    # Construct predictable temporary directory path based on asset key and partition
    # Sanitize asset key for directory name
    sanitized_asset_key = context.asset_key.to_user_string().replace("/", "_")
    base_asset_temp_dir = Path(tempfile.gettempdir()) / sanitized_asset_key
    temp_dir_path = base_asset_temp_dir / f"download_{partition_date_str}"
    temp_file_path = temp_dir_path / filename

    expected_md5s = get_expected_md5s(MD5SUMS_URL)
    expected_md5 = expected_md5s.get(filename)

    try:
        # Ensure base asset temp directory exists
        base_asset_temp_dir.mkdir(parents=True, exist_ok=True)

        download_needed = True
        skip_reason = ""

        # Check if the directory and valid tarball already exist
        if temp_dir_path.exists():
            logger.info(f"Found existing temporary directory: {temp_dir_path}")
            if temp_file_path.is_file():
                if expected_md5:
                    logger.info(f"Calculating MD5 for existing file: {temp_file_path}")
                    local_md5 = calculate_md5(temp_file_path)
                    if local_md5 and local_md5 == expected_md5:
                        logger.info(f"MD5 match ({local_md5}). Skipping download.")
                        download_needed = False
                        skip_reason = "MD5 match on existing file"
                    elif local_md5:
                        logger.warning(f"MD5 mismatch for {temp_file_path}. Expected: {expected_md5}, Found: {local_md5}. File is corrupt or incomplete. Re-downloading.")
                        skip_reason = "MD5 mismatch"
                    else:
                         logger.warning(f"Could not calculate MD5 for existing {temp_file_path}. Re-downloading.")
                         skip_reason = "MD5 calculation failed"
                else:
                    logger.warning(f"Could not find expected MD5 for {filename} in {MD5SUMS_URL}. Cannot verify existing file integrity. Re-downloading.")
                    skip_reason = "Expected MD5 not found"

                # If download IS needed (MD5 mismatch/error), cleanup needed before redownload
                if download_needed:
                     logger.info(f"Cleaning up potentially corrupt directory due to: {skip_reason}")
                     try:
                         shutil.rmtree(temp_dir_path)
                     except OSError as e:
                         logger.error(f"Error removing directory {temp_dir_path}: {e}")
                         raise # Cannot proceed if cleanup fails
                     temp_dir_path.mkdir() # Recreate clean directory

            else: # Directory exists, but file is missing
                 logger.info(f"Tarball {temp_file_path} missing in existing directory. Proceeding with download.")
                 # download_needed is already True

        else: # Directory doesn't exist
            logger.info(f"Creating temporary directory: {temp_dir_path}")
            temp_dir_path.mkdir()
            # download_needed is already True

        # --- Download (if needed) ---
        if download_needed:
            logger.info(f"Downloading {archive_url} to {temp_file_path} using aria2c")
            cmd = [
                "aria2c",
                "-c",  # Resume partial downloads
                "-x",
                "4",
                "-s",
                "4",
                "--file-allocation=falloc",
                "--dir",
                str(temp_dir_path),
                "--out",
                filename,
                "--log-level=warn",
                "--log=-",
                "--show-console-readout=false",
                "--summary-interval=30",
                "--enable-color=false",
                archive_url,
            ]

            logger.info(f"Executing command: {' '.join(cmd)}")
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
            )

            if process.stdout:
                for line in iter(process.stdout.readline, ""):
                    logger.info(f"[aria2c] {line.rstrip()}")
                process.stdout.close()

            return_code = process.wait()

            if return_code != 0:
                raise Exception(
                    f"aria2c failed with return code {return_code} for URL {archive_url}"
                )

            # Verify MD5 after download
            if expected_md5:
                logger.info(f"Verifying MD5 for downloaded file: {temp_file_path}")
                local_md5 = calculate_md5(temp_file_path)
                if local_md5 and local_md5 == expected_md5:
                    logger.info(f"MD5 check passed after download ({local_md5}).")
                elif local_md5:
                    raise Exception(f"MD5 mismatch after download! Expected {expected_md5}, got {local_md5}. Archive source may be corrupted.")
                else:
                    raise Exception(f"Could not calculate MD5 after download for {temp_file_path}. Download likely failed silently.")
            else:
                 logger.warning(f"Could not find expected MD5 for {filename}. Skipping post-download verification.")

            logger.info(f"Download completed successfully.")
        # else: Already logged that download is skipped

        # --- Extract the tarball --- 
        extract_dir = temp_dir_path / "extracted"
        logger.info(f"Extracting {temp_file_path} to {extract_dir}")
        try:
            extract_dir.mkdir(exist_ok=True) # Create extraction subdir
            with tarfile.open(temp_file_path, "r:gz") as tar:
                tar.extractall(path=extract_dir) # Extract into subdir
            alert_files = list(
                extract_dir.glob("*.avro") # Glob within subdir
            )  # Re-list after extraction
            logger.info(
                f"Extraction successful. Found {len(alert_files)} .avro files in {extract_dir}."
            )
        except tarfile.ReadError as e:
            logger.error(
                f"Failed to extract tarfile {temp_file_path}: {e}. It might be corrupted."
            )
            # Keep the dir for inspection, but raise to fail the asset
            raise Exception(f"Tarfile extraction failed: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred during extraction: {e}")
            raise  # Re-raise other unexpected errors

        # --- Compact with duckdb then upload to S3 ---
        if not alert_files:
            logger.warning(
                f"No .avro files found in {extract_dir} after download/extraction steps."
            )
            # Assume empty archive for the day is possible, return success
            # If this should be an error, raise an exception here instead.
            return s3_path

        logger.info(
            f"Processing {len(alert_files)} avro files from {extract_dir} and uploading to {s3_path}/part_*.parquet"
        )
        batch_size = 512 * 1024 * 1024  # 512 MiB
        batches = []
        curr_size = 0
        total_size = 0
        batch_files = []
        for file in alert_files:
            file_size = file.stat().st_size
            total_size += file_size
            if batch_files and curr_size + file_size > batch_size:
                batches.append(batch_files)
                batch_files = [file]  # Start new batch
                curr_size = file_size
            else:
                batch_files.append(file)
                curr_size += file_size
        if batch_files:
            batches.append(batch_files)

        if not batches:
            logger.info(
                f"No data batches to process for {partition_date_str} despite finding avro files (this shouldn't happen). Returning success for safety."
            )
            return s3_path

        conn = duckdb.connect(database=":memory:", read_only=False)

        try:
            conn.execute("INSTALL httpfs;")
            conn.execute("LOAD httpfs;")

            aws_key = getattr(aws_config, "access_key_id", None)
            aws_secret = getattr(aws_config, "secret_access_key", None)
            aws_region = getattr(aws_config, "region", "us-east-1")
            s3_endpoint_host = getattr(s3_config, "endpoint_host", None)
            s3_endpoint_port = getattr(s3_config, "endpoint_port", None)
            s3_use_ssl = str(getattr(s3_config, "use_ssl", False)).lower()

            if not all([aws_key, aws_secret, s3_endpoint_host, s3_endpoint_port]):
                raise ValueError(
                    "Missing required AWS/S3 configuration for DuckDB Secret."
                )

            endpoint_url = f"{s3_endpoint_host}:{s3_endpoint_port}"

            secret_sql = f"""
            CREATE OR REPLACE SECRET secret (
                TYPE S3,
                PROVIDER CONFIG,
                ENDPOINT '{endpoint_url}',
                KEY_ID '{aws_key}',
                SECRET '{aws_secret}',
                REGION '{aws_region}',
                USE_SSL {s3_use_ssl},
                URL_STYLE 'path'
            );
            """
            logger.debug(
                f"Creating DuckDB S3 Secret (Endpoint: {endpoint_url}, UseSSL: {s3_use_ssl})"
            )
            conn.execute(secret_sql)

            logger.info(f"Starting upload of {len(batches)} batches to {s3_path}/")
            for batch_idx, batch in enumerate(batches):
                file_list_str = ", ".join(
                    [f"'{str(file.resolve())}'" for file in batch]
                )
                # Use the original prefix which doesn't end in /
                # DuckDB/S3FS handles the path joining correctly
                target_s3_path = f"{s3_path}/part_{batch_idx:05d}.parquet"
                copy_sql = f"""
                    COPY (SELECT * FROM read_avro([{file_list_str}]))
                    TO '{target_s3_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
                logger.info(
                    f"Executing DuckDB COPY for batch {batch_idx+1}/{len(batches)} to {target_s3_path}"
                )
                conn.execute(copy_sql)
                logger.info(
                    f"Finished DuckDB COPY for batch {batch_idx+1}/{len(batches)}"
                )

        finally:
            conn.close()

        context.add_output_metadata(
            {
                "num_batches": len(batches),
                "num_files": len(alert_files),
                "total_size": total_size,
                "source_url": archive_url,
                "output_path": s3_path,
                "download_skipped": not download_needed,
            }
        )

        logger.info(f"Successfully processed and uploaded files to {s3_path}")
        return s3_path

    except Exception as e:
        logger.error(
            f"Failed during processing for partition {partition_date_str}: {e}",
            exc_info=True,
        )
        if temp_dir_path and temp_dir_path.exists():
            logger.warning(
                f"Temporary directory {temp_dir_path} has been kept for inspection due to failure."
            )
        raise

    finally:
        # Cleanup *only* if the directory exists AND no exception occurred/is being handled
        if temp_dir_path and temp_dir_path.exists():
            current_exception = sys.exc_info()[1]
            if current_exception is None:  # No exception occurred or is being handled
                try:
                    logger.info(
                        f"Processing successful. Cleaning up temporary directory: {temp_dir_path}"
                    )
                    shutil.rmtree(temp_dir_path)
                except Exception as cleanup_error:
                    logger.error(
                        f"Error during temporary directory cleanup {temp_dir_path}: {cleanup_error}",
                        exc_info=True,
                    )
            # else: An exception occurred. The 'except' block logged it and we leave the directory.
