import subprocess
import tempfile
from pathlib import Path
import shutil
import os

from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    ResourceParam,
    AssetIn,
)
from dagster_aws.s3 import S3Resource

from ...resources import S3Config
from ...partitions import daily_partitions

ZTF_ALERT_ARCHIVE_BASE_URL = "https://ztf.uw.edu/alerts/public"

logger = get_dagster_logger()


# --- Assets ---
@asset(
    name="ztf_alert_archive",
    key_prefix=["bronze"],
    group_name="ingestion",
    description="Raw ZTF public alert archive tarball stored in the bronze zone.",
    partitions_def=daily_partitions,
    kinds={"shell", "s3"},
    metadata={"partition_key": "string"},
)
def ztf_alert_archive(
    context: AssetExecutionContext,
    s3: ResourceParam[S3Resource],
    s3_config: ResourceParam[S3Config],
) -> str:
    partition_date_str = context.partition_key
    filename_date_str = partition_date_str.replace("-", "")
    filename = f"ztf_public_{filename_date_str}.tar.gz"
    archive_url = f"{ZTF_ALERT_ARCHIVE_BASE_URL}/{filename}"

    s3_bucket = s3_config.bronze_bucket
    if not s3_bucket:
        logger.error("S3_BRONZE_BUCKET environment variable not set.")
        raise ValueError("S3_BRONZE_BUCKET environment variable not set.")

    s3_key = f"alerts_archive/{filename}"
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    try:
        with tempfile.TemporaryDirectory(
            prefix=f"download_{partition_date_str}_"
        ) as temp_dir:
            temp_file_path = Path(temp_dir) / filename
            logger.info(f"Downloading {archive_url} to {temp_file_path} using aria2c")

            cmd = [
                "aria2c",
                "-c",
                "-x",
                "4",
                "-s",
                "4",
                "--file-allocation=falloc",
                "--dir",
                temp_dir,
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

            # Log aria2c output directly
            if process.stdout:
                for line in iter(process.stdout.readline, ""):
                    logger.info(f"[aria2c] {line.rstrip()}")
                process.stdout.close()

            return_code = process.wait()

            if return_code != 0:
                raise Exception(
                    f"aria2c failed with return code {return_code} for URL {archive_url}"
                )

            logger.info(
                f"aria2c completed successfully. Local file is at {temp_file_path}"
            )

            # --- Upload to S3 ---
            logger.info(f"Uploading {temp_file_path} to {s3_path}")
            s3_client = s3.get_client()
            s3_client.upload_file(
                Filename=str(temp_file_path), Bucket=s3_bucket, Key=s3_key
            )
            logger.info(f"Successfully uploaded to {s3_path}")

            os.remove(temp_file_path)

            context.add_output_metadata({"s3_path": s3_path})
            return s3_path

    except Exception as e:
        logger.error(
            f"Failed during processing for partition {partition_date_str}: {e}"
        )
        raise


@asset(
    name="raw_ztf_alerts",
    key_prefix=["bronze"],
    ins={"upstream_archive_path": AssetIn(key=ztf_alert_archive.key)},
    group_name="ingestion",
    description="Downloads ZTF archives from S3 and extracts raw AVRO alerts onto a shared volume.",
    partitions_def=daily_partitions,
    kinds={"local", "shell", "avro"},
    metadata={"partition_key": "string", "num_files": "int"},
)
def raw_ztf_alerts(
    context: AssetExecutionContext,
    s3: ResourceParam[S3Resource],
    s3_config: ResourceParam[S3Config],
    upstream_archive_path: str,
) -> str:
    partition_date_str = context.partition_key
    logger.info(f"Processing ZTF alerts for partition: {partition_date_str}")
    logger.info(f"Using input tarball path: {upstream_archive_path}")

    container_shared_mount_path = Path("/mnt/ateda/shared")
    logger.info(
        f"Using hardcoded shared volume mount path: {container_shared_mount_path}"
    )

    # --- S3 Configuration from Resources ---
    s3_bronze_bucket = s3_config.bronze_bucket

    # --- Get S3 client ---
    s3_client = s3.get_client()

    # --- Parse input S3 path ---
    if not upstream_archive_path.startswith(f"s3://{s3_bronze_bucket}/"):
        raise ValueError(
            f"Input path {upstream_archive_path} does not match expected bucket {s3_bronze_bucket}"
        )
    s3_key = upstream_archive_path[len(f"s3://{s3_bronze_bucket}/") :]
    input_filename = Path(s3_key).name

    # --- Define Output Paths ---
    year, month, day = partition_date_str.split("-")

    # --- Create temporary directory for processing ---
    with tempfile.TemporaryDirectory(
        prefix=f"process_{partition_date_str}_"
    ) as temp_dir:
        temp_dir_path = Path(temp_dir)
        temp_file_path = temp_dir_path / input_filename
        # Construct path relative to the container mount point
        extracted_dir_path = (
            container_shared_mount_path
            / "alerts"
            / f"year={year}"
            / f"month={month}"
            / f"day={day}"
        )
        # Ensure the directory exists on the mounted volume
        # NOTE: This assumes the process running this asset has write access to the mounted volume
        extracted_dir_path.mkdir(parents=True, exist_ok=True)

        # --- Download the tarball from S3 ---
        logger.info(f"Downloading {upstream_archive_path} to {temp_file_path}")
        try:
            s3_client.download_file(
                Bucket=s3_bronze_bucket, Key=s3_key, Filename=str(temp_file_path)
            )
            logger.info(f"Successfully downloaded {input_filename}")
        except Exception as e:
            logger.error(f"Failed to download {upstream_archive_path}: {e}")
            raise

        # --- Extract the tarball ---
        logger.info(f"Extracting {temp_file_path} to {extracted_dir_path}")
        try:
            shutil.unpack_archive(str(temp_file_path), str(extracted_dir_path))
            logger.info("Successfully extracted archive.")
        except Exception as e:
            logger.error(f"Failed to extract archive {temp_file_path}: {e}")
            raise

    # Return the absolute container path
    logger.info(
        f"Returning final container path for extracted alerts: {extracted_dir_path}"
    )
    return str(extracted_dir_path)
