import subprocess
import tarfile
import tempfile
from pathlib import Path
import os
import shutil
import sys
import hashlib
import urllib.request
import logging
import json
from avro.datafile import DataFileReader
from avro.io import DatumReader

from datetime import datetime

from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    ResourceParam,
    Config,
)
from dagster_aws.s3 import S3Resource

from ...resources import S3Config
from ...partitions import daily_partitions
from ...resources import AWSConfig
from dagster_pipes import open_dagster_pipes

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

class BronzeRawZtfAlertConfig(Config):
    concurrency: int = 32
    num_alert_writers: int = 10
    num_cutout_writers: int = 2000
    alert_writer_buffer_size: int = 1024
    cutout_writer_buffer_size: int = 1024
    s3_bucket_alerts: str = "ateda-landing"
    s3_prefix_alerts: str = "alerts/"
    s3_bucket_cutout: str = "ateda-cutout"
    s3_prefix_cutout: str = "cutouts/"

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
    config: BronzeRawZtfAlertConfig,
    s3_config: ResourceParam[S3Config],
    aws_config: ResourceParam[AWSConfig],
) -> str:
    partition_date_str = context.partition_key
    date_obj = datetime.strptime(partition_date_str, "%Y-%m-%d")
    filename_date_str = date_obj.strftime("%Y%m%d")
    filename = f"ztf_public_{filename_date_str}.tar.gz"
    archive_url = f"{ZTF_ALERT_ARCHIVE_BASE_URL}/{filename}"

    prefix = f"alerts/year={date_obj.year:04d}/month={date_obj.month:02d}/day={date_obj.day:02d}"
    s3_path = f"s3://{config.s3_bucket_alerts}/{config.s3_prefix_alerts}{prefix}"

    # Construct predictable temporary directory path based on asset key and partition
    sanitized_asset_key = context.asset_key.to_user_string().replace("/", "_")
    base_asset_temp_dir = Path("data/") / sanitized_asset_key
    temp_dir_path = base_asset_temp_dir / f"download_{partition_date_str}"

    expected_md5s = get_expected_md5s(MD5SUMS_URL)
    expected_md5 = expected_md5s.get(filename)

    try:
        base_asset_temp_dir.mkdir(parents=True, exist_ok=True)
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
            extract_dir.mkdir(exist_ok=True)
            with tarfile.open(downloaded_file_path, "r:gz") as tar:
                tar.extractall(path=extract_dir)
            alert_files = list(extract_dir.glob("*.avro"))
            logger.info(
                f"Extraction successful. Found {len(alert_files)} .avro files in {extract_dir}."
            )
        except tarfile.ReadError as e:
            logger.error(
                f"Failed to extract tarfile {downloaded_file_path}: {e}. It might be corrupted."
            )
            raise Exception(f"Tarfile extraction failed: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred during extraction: {e}")
            raise

        if not alert_files:
            logger.warning(
                f"No .avro files found in {extract_dir} after download/extraction steps."
            )
            return s3_path

        schema_file = None
        cutout_fields = {"cutoutScience", "cutoutTemplate", "cutoutDifference"}
        for avro_file in alert_files:
            with open(avro_file, "rb") as f:
                reader = DataFileReader(f, DatumReader())
                schema_str = reader.get_meta("avro.schema").decode()
                schema = json.loads(schema_str)
                if "fields" in schema:
                    schema["fields"] = [
                        field for field in schema["fields"]
                        if field.get("name") not in cutout_fields
                    ]
                schema_file = extract_dir / "schema_nocutout.avsc"
                with open(schema_file, "w") as sf:
                    sf.write(json.dumps(schema))
                break

        if not schema_file:
            logger.error("Could not extract schema from Avro files.")
            raise Exception("Schema extraction failed.")

        ingestor_bin = "ateda-ingestor"
        alert_writer_buffer_size = config.alert_writer_buffer_size
        concurrency = config.concurrency
        num_alert_writers = config.num_alert_writers
        cutout_writer_buffer_size = config.cutout_writer_buffer_size
        num_cutout_writers = config.num_cutout_writers
        s3_bucket_cutout = config.s3_bucket_cutout
        s3_prefix_cutout = config.s3_prefix_cutout

        # Open dagster pipes context and set env var for subprocess
        with open_dagster_pipes() as pipes:
            env = os.environ.copy()
            env["DAGSTER_PIPES_CONTEXT"] = pipes.get_bootstrap_env_json()
            cmd = [
                ingestor_bin,
                "--input-dir", str(extract_dir),
                "--alert-schema-file", str(schema_file),
                "--temp-dir", str(temp_dir_path / "ingestor_tmp"),
                "--aws-access-key-id", aws_config.access_key_id,
                "--aws-secret-access-key", aws_config.secret_access_key,
                "--s3-endpoint-url", s3_config.endpoint_url,
                "--s3-bucket-alerts", config.s3_bucket_alerts,
                "--s3-prefix-alerts", config.s3_prefix_alerts,
                "--file-prefix", "ztf_alert",
                "--partition-name", "observation_date",
                "--partition-value", partition_date_str,
                "--alert-writer-buffer-size", str(alert_writer_buffer_size),
                "--concurrency", str(concurrency),
                "--num-alert-writers", str(num_alert_writers),
                "--s3-bucket-cutout", s3_bucket_cutout,
                "--s3-prefix-cutout", s3_prefix_cutout,
                "--cutout-writer-buffer-size", str(cutout_writer_buffer_size),
                "--num-cutout-writers", str(num_cutout_writers),
            ]
            logger.info(f"Running ateda-ingestor with dagster-pipes: {' '.join(cmd)}")
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
            for line in proc.stdout:
                logger.info(f"[ateda-ingestor] {line.rstrip()}")
            retcode = proc.wait()
            if retcode != 0:
                logger.error(f"ateda-ingestor failed with exit code {retcode}")
                raise Exception(f"ateda-ingestor failed with exit code {retcode}")

        context.add_output_metadata(
            {
                "num_files": len(alert_files),
                "source_url": archive_url,
                "output_path": s3_path,
                "download_skipped": download_skipped,
                "ingestor": "ateda-ingestor (alert+cutout)",
            }
        )

        logger.info(f"Successfully processed and uploaded alert records to {s3_path}")
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
        if temp_dir_path and temp_dir_path.exists():
            current_exception = sys.exc_info()[1]
            if current_exception is None:
                try:
                    logger.info(
                        f"Processing successful. Cleaning up temporary directory: {temp_dir_path}"
                    )
                    shutil.rmtree(temp_dir_path)
                except Exception as cleanup_error:
                    logger.error(
                        f"Error during successful temporary directory cleanup {temp_dir_path}: {cleanup_error}",
                        exc_info=True,
                    )
            else:
                logger.warning(
                    f"Failure detected. Performing selective cleanup of {temp_dir_path}, keeping the downloaded archive."
                )
                extract_dir_cleanup = temp_dir_path / "extracted"
                schema_file_cleanup = extract_dir_cleanup / "schema_nocutout.avsc"
                ingestor_tmp_dir_cleanup = temp_dir_path / "ingestor_tmp"

                if extract_dir_cleanup.exists():
                    try:
                        logger.info(f"Removing extracted files directory: {extract_dir_cleanup}")
                        shutil.rmtree(extract_dir_cleanup)
                    except Exception as cleanup_error:
                        logger.error(f"Error removing extracted files directory {extract_dir_cleanup} during failure cleanup: {cleanup_error}", exc_info=True)

                if schema_file_cleanup.exists():
                    try:
                        logger.info(f"Removing schema file: {schema_file_cleanup}")
                        schema_file_cleanup.unlink()
                    except Exception as cleanup_error:
                         logger.error(f"Error removing schema file {schema_file_cleanup} during failure cleanup: {cleanup_error}", exc_info=True)

                if ingestor_tmp_dir_cleanup.exists():
                     try:
                         logger.info(f"Removing ingestor temporary directory: {ingestor_tmp_dir_cleanup}")
                         shutil.rmtree(ingestor_tmp_dir_cleanup)
                     except Exception as cleanup_error:
                         logger.error(f"Error removing ingestor temporary directory {ingestor_tmp_dir_cleanup} during failure cleanup: {cleanup_error}", exc_info=True)
