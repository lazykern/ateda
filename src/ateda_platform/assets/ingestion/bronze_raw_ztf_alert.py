import subprocess
import tarfile
import tempfile
from pathlib import Path
import os
import shutil
import sys
import hashlib
import urllib.request
import multiprocessing
import logging

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
                lines = response.read().decode("utf-8").splitlines()
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 2:
                        md5_hash = parts[0]
                        # Handle potential path prefix in filename
                        filename = Path(parts[-1]).name
                        expected_md5s[filename] = md5_hash
            else:
                logger.error(
                    f"Failed to download MD5SUMS from {url}. Status: {response.status}"
                )
    except urllib.error.URLError as e:
        logger.error(f"Error accessing MD5SUMS URL {url}: {e}")
    except Exception as e:
        logger.error(f"Error processing MD5SUMS from {url}: {e}")
    return expected_md5s


def _download_and_verify_archive(
    archive_url: str,
    temp_dir_path: Path,
    filename: str,
    expected_md5: str | None,
) -> tuple[Path, bool]:
    """
    Downloads the ZTF archive if needed, verifies MD5 checksum.

    Args:
        archive_url: URL of the archive file.
        temp_dir_path: Path to the temporary directory for download.
        filename: Name of the archive file.
        expected_md5: Expected MD5 hash, or None if not available.

    Returns:
        A tuple containing:
            - Path to the downloaded (or existing validated) archive file.
            - Boolean indicating if the download was skipped (True if skipped).

    Raises:
        Exception: If download fails or MD5 verification fails after download.
        OSError: If directory cleanup fails before re-download.
    """
    temp_file_path = temp_dir_path / filename
    download_needed = True
    skip_reason = ""

    # Add logging to check existence explicitly
    logger.info(f"Checking existence of temporary directory: {temp_dir_path}")
    dir_exists = temp_dir_path.exists()
    logger.info(f"Directory exists: {dir_exists}")

    # Check if the directory and valid tarball already exist
    if dir_exists:
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
                    logger.warning(
                        f"MD5 mismatch for {temp_file_path}. Expected: {expected_md5}, Found: {local_md5}. File is corrupt or incomplete. Re-downloading."
                    )
                    skip_reason = "MD5 mismatch"
                else:
                    logger.warning(
                        f"Could not calculate MD5 for existing {temp_file_path}. Re-downloading."
                    )
                    skip_reason = "MD5 calculation failed"
            else:
                logger.warning(
                    f"Could not find expected MD5 for {filename} in {MD5SUMS_URL}. Cannot verify existing file integrity. Re-downloading."
                )
                skip_reason = "Expected MD5 not found"

            # If download IS needed (MD5 mismatch/error), cleanup needed before redownload
            if download_needed:
                logger.info(
                    f"Cleaning up potentially corrupt directory due to: {skip_reason}"
                )
                try:
                    shutil.rmtree(temp_dir_path)
                except OSError as e:
                    logger.error(f"Error removing directory {temp_dir_path}: {e}")
                    raise  # Cannot proceed if cleanup fails
                temp_dir_path.mkdir()  # Recreate clean directory

        else:  # Directory exists, but file is missing
            logger.info(
                f"Tarball {temp_file_path} missing in existing directory. Proceeding with download."
            )
            # download_needed is already True

    else:  # Directory doesn't exist
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
                raise Exception(
                    f"MD5 mismatch after download! Expected {expected_md5}, got {local_md5}. Archive source may be corrupted."
                )
            else:
                raise Exception(
                    f"Could not calculate MD5 after download for {temp_file_path}. Download likely failed silently."
                )
        else:
            logger.warning(
                f"Could not find expected MD5 for {filename}. Skipping post-download verification."
            )

        logger.info("Download completed successfully.")
    else:
        logger.info(f"Download skipped: {skip_reason}")

    return temp_file_path, not download_needed


def _process_batch(
    batch_files: list[Path],
    batch_idx: int,
    s3_base_path: str,
    aws_key: str,
    aws_secret: str,
    s3_endpoint_url: str,
    s3_use_ssl: str,
) -> str:
    """
    Processes a single batch of Avro files using DuckDB and uploads to S3.
    Designed to be run in a separate process (multiprocessing.Pool).

    Args:
        batch_files: List of Path objects for the Avro files in this batch.
        batch_idx: The index of this batch (for naming output file).
        s3_base_path: The base S3 path (e.g., s3://bucket/prefix) without the filename.
        aws_key: AWS Access Key ID.
        aws_secret: AWS Secret Access Key.
        s3_endpoint_url: S3 endpoint URL (e.g., host:port).
        s3_use_ssl: Whether to use SSL ('true' or 'false').

    Returns:
        The S3 path of the generated Parquet file.

    Raises:
        Exception: If DuckDB connection, secret creation, or COPY command fails.
    """
    # Use standard Python logging for multiprocessing workers
    worker_logger = logging.getLogger(f"process_batch_{batch_idx}")
    worker_logger.setLevel(logging.INFO)  # Adjust level as needed
    # Basic handler, configure more robustly if needed (e.g., QueueHandler)
    if not worker_logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        worker_logger.addHandler(handler)

    target_s3_path = f"{s3_base_path}/part_{batch_idx:05d}.parquet"
    worker_logger.info(
        f"Processing batch {batch_idx} ({len(batch_files)} files) -> {target_s3_path}"
    )

    conn = duckdb.connect(database=":memory:", read_only=False)
    try:
        # Configure DuckDB for S3 access
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        secret_sql = f"""
        CREATE OR REPLACE SECRET secret (
            TYPE S3,
            PROVIDER CONFIG,
            ENDPOINT '{s3_endpoint_url}',
            KEY_ID '{aws_key}',
            SECRET '{aws_secret}',
            USE_SSL {s3_use_ssl},
            URL_STYLE 'path'
        );
        """
        worker_logger.debug(f"Creating DuckDB S3 Secret for batch {batch_idx}")
        conn.execute(secret_sql)

        # Prepare and execute the COPY command
        file_list_str = ", ".join([f"'{str(file.resolve())}'" for file in batch_files])
        copy_sql = f"""
            COPY (SELECT * FROM read_avro([{file_list_str}]))
            TO '{target_s3_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        worker_logger.info(f"Executing DuckDB COPY for batch {batch_idx}")
        conn.execute(copy_sql)
        worker_logger.info(f"Finished DuckDB COPY for batch {batch_idx}")
        return target_s3_path
    except Exception as e:
        worker_logger.error(
            f"Error processing batch {batch_idx} -> {target_s3_path}: {e}",
            exc_info=True,
        )
        # Re-raise the exception so the main process knows about the failure
        raise Exception(f"Failed to process batch {batch_idx}: {e}") from e
    finally:
        conn.close()


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
    base_asset_temp_dir = Path("data/") / sanitized_asset_key
    temp_dir_path = base_asset_temp_dir / f"download_{partition_date_str}"

    expected_md5s = get_expected_md5s(MD5SUMS_URL)
    expected_md5 = expected_md5s.get(filename)

    try:
        # Ensure base asset temp directory exists (create if needed)
        base_asset_temp_dir.mkdir(parents=True, exist_ok=True)

        # Download and verify the archive
        downloaded_file_path, download_skipped = _download_and_verify_archive(
            archive_url=archive_url,
            temp_dir_path=temp_dir_path,
            filename=filename,
            expected_md5=expected_md5,
        )

        # --- Extract the tarball ---
        extract_dir = temp_dir_path / "extracted"
        logger.info(f"Extracting {downloaded_file_path} to {extract_dir}")
        try:
            extract_dir.mkdir(exist_ok=True)  # Create extraction subdir
            with tarfile.open(downloaded_file_path, "r:gz") as tar:
                tar.extractall(path=extract_dir)  # Extract into subdir
            alert_files = list(
                extract_dir.glob("*.avro")  # Glob within subdir
            )  # Re-list after extraction
            logger.info(
                f"Extraction successful. Found {len(alert_files)} .avro files in {extract_dir}."
            )
        except tarfile.ReadError as e:
            logger.error(
                f"Failed to extract tarfile {downloaded_file_path}: {e}. It might be corrupted."
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
        batch_size = 256 * 1024 * 1024  # 256 MiB
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

        # --- Extract Config for Parallel Processing ---
        aws_key = aws_config.access_key_id
        aws_secret = aws_config.secret_access_key
        s3_endpoint_host = s3_config.endpoint_host
        s3_endpoint_port = s3_config.endpoint_port
        # Ensure use_ssl is fetched correctly and converted to string
        s3_use_ssl = str(getattr(s3_config, "use_ssl", False)).lower()

        if not all([aws_key, aws_secret, s3_endpoint_host, s3_endpoint_port]):
            raise ValueError("Missing required AWS/S3 configuration for DuckDB Secret.")

        endpoint_url = f"{s3_endpoint_host}:{s3_endpoint_port}"

        # --- Parallel Processing with DuckDB using multiprocessing.Pool ---
        logger.info(
            f"Starting parallel upload of {len(batches)} batches to {s3_path}/ using multiprocessing.Pool"
        )
        results = {}  # Store results keyed by batch_idx
        pool = None  # Initialize pool variable

        # Limit the number of concurrent worker processes
        max_worker_processes = 4
        logger.info(f"Using up to {max_worker_processes} worker processes.")

        try:
            pool = multiprocessing.Pool(processes=max_worker_processes)
            async_results = []
            for batch_idx, batch in enumerate(batches):
                # Submit tasks using apply_async
                res = pool.apply_async(
                    _process_batch,
                    args=(
                        batch,
                        batch_idx,
                        s3_path,
                        aws_key,
                        aws_secret,
                        endpoint_url,
                        s3_use_ssl,
                    ),
                )
                async_results.append((batch_idx, res))

            # Wait for results with fail-fast logic
            num_completed = 0
            total_batches = len(async_results)
            processed_indices = set()

            while num_completed < total_batches:
                remaining_results = [
                    (idx, r) for idx, r in async_results if idx not in processed_indices
                ]
                if not remaining_results:
                    break  # Should not happen if num_completed < total_batches, but safety break

                # Check one result at a time with a short timeout
                # This allows faster detection of failure than iterating completed()
                idx, async_result = remaining_results[0]
                try:
                    # Use a timeout to avoid blocking indefinitely if a task hangs
                    result_path = async_result.get(timeout=1.0)  # Check every second
                    results[idx] = result_path
                    logger.info(f"Successfully processed batch {idx} -> {result_path}")
                    processed_indices.add(idx)
                    num_completed += 1
                except multiprocessing.TimeoutError:
                    # Task not finished yet, continue checking others or loop again
                    pass
                except Exception as exc:
                    # A task failed!
                    logger.error(
                        f"Batch {idx} generated an exception: {exc}. Terminating pool."
                    )
                    pool.terminate()  # Forcefully stop other workers
                    pool.join()  # Wait for workers to terminate
                    # Re-raise the first exception encountered to fail the asset
                    raise Exception(
                        f"Processing failed for batch {idx}: {exc}"
                    ) from exc

            # If loop finishes without exceptions, close pool cleanly
            pool.close()
            pool.join()
            logger.info(f"Finished processing all {len(results)} batches successfully.")

        except (
            Exception
        ) as main_exc:  # Catch exceptions raised within the try (incl. re-raised ones)
            logger.error(f"An error occurred during parallel processing: {main_exc}")
            if pool:
                # Ensure pool is terminated even if error happened outside the get() loop
                pool.terminate()
                pool.join()
            raise  # Re-raise the exception to fail the asset
        finally:
            # Final safety net for pool cleanup if something unexpected happened
            if pool and not pool._state == multiprocessing.pool.TERMINATE:
                try:
                    pool.terminate()
                    pool.join()
                except Exception as pool_term_exc:
                    logger.error(
                        f"Error during final pool termination: {pool_term_exc}"
                    )

        context.add_output_metadata(
            {
                "num_batches": len(batches),
                "num_files": len(alert_files),
                "total_size": total_size,
                "source_url": archive_url,
                "output_path": s3_path,
                "download_skipped": download_skipped,
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
