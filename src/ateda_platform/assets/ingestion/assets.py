import subprocess
import tempfile
from pathlib import Path
import shutil
import os
import duckdb
import math
import time
import concurrent.futures

from dagster import (
    asset, get_dagster_logger, AssetExecutionContext, ResourceParam, Config, AssetIn
)
from dagster_aws.s3 import S3Resource

from ...resources import AWSConfig, S3Config
from ...partitions import daily_partitions

ZTF_ALERT_ARCHIVE_BASE_URL = "https://ztf.uw.edu/alerts/public"

logger = get_dagster_logger()

class AlertAssetConfig(Config):
    max_workers: int = 6
    threads_per_worker: int = 2
    memory_per_worker: str = "2GB"
    safe_max_batch_size: int = 15000

# --- Assets ---
@asset(
    name="alerts_archive",
    key_prefix=["landing"],
    group_name="ingestion",
    description="Raw ZTF public alert archive tarball stored in the landing zone.",
    partitions_def=daily_partitions,
    kinds={"shell", "s3"},
    metadata={"partition_key": "string"}
)
def ztf_landing_alerts_archive(
    context: AssetExecutionContext, 
    s3: ResourceParam[S3Resource],
    s3_config: ResourceParam[S3Config]
) -> str:
    partition_date_str = context.partition_key
    filename_date_str = partition_date_str.replace("-", "")
    filename = f"ztf_public_{filename_date_str}.tar.gz"
    archive_url = f"{ZTF_ALERT_ARCHIVE_BASE_URL}/{filename}"

    s3_bucket = s3_config.landing_bucket
    if not s3_bucket:
        logger.error("S3_LANDING_BUCKET environment variable not set.")
        raise ValueError("S3_LANDING_BUCKET environment variable not set.")

    s3_key = f"alerts_archive/{filename}"
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    try:
        with tempfile.TemporaryDirectory(prefix=f"download_{partition_date_str}_") as temp_dir:
            temp_file_path = Path(temp_dir) / filename
            logger.info(f"Downloading {archive_url} to {temp_file_path} using aria2c")

            cmd = [
                "aria2c",
                "-c",
                "-x", "4",
                "-s", "4",
                "--file-allocation=falloc",
                "--dir", temp_dir,
                "--out", filename,
                "--log-level=warn",
                "--log=-",
                "--show-console-readout=false",
                "--summary-interval=30",
                "--enable-color=false",
                archive_url
            ]

            logger.info(f"Executing command: {' '.join(cmd)}")
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8'
            )

            # Log aria2c output directly
            if process.stdout:
                for line in iter(process.stdout.readline, ''):
                    logger.info(f"[aria2c] {line.rstrip()}")
                process.stdout.close()

            return_code = process.wait()

            if return_code != 0:
                raise Exception(f"aria2c failed with return code {return_code} for URL {archive_url}")

            logger.info(f"aria2c completed successfully. Local file is at {temp_file_path}")

            # --- Upload to S3 ---
            logger.info(f"Uploading {temp_file_path} to {s3_path}")
            s3_client = s3.get_client()
            s3_client.upload_file(
                Filename=str(temp_file_path),
                Bucket=s3_bucket,
                Key=s3_key
            )
            logger.info(f"Successfully uploaded to {s3_path}")

            os.remove(temp_file_path)

            context.add_output_metadata({"s3_path": s3_path})
            return s3_path

    except Exception as e:
        logger.error(f"Failed during processing for partition {partition_date_str}: {e}")
        raise


@asset(
    name="alerts",
    key_prefix=["bronze"],
    ins={
        "upstream_archive_path": AssetIn(
            key=ztf_landing_alerts_archive.key
        )
    },
    group_name="ingestion",
    description="ZTF alerts extracted from archives and stored as Parquet files in the bronze layer.",
    partitions_def=daily_partitions,
    kinds={ "s3", "duckdb", "parquet"},
    metadata={"partition_key": "string", "num_files": "int"}
)
def ztf_bronze_alerts(
    context: AssetExecutionContext,
    s3: ResourceParam[S3Resource],
    aws_config: ResourceParam[AWSConfig],
    s3_config: ResourceParam[S3Config],
    upstream_archive_path: str,
    config: AlertAssetConfig
) -> str:
    partition_date_str = context.partition_key
    logger.info(f"Processing ZTF bronze alerts for partition: {partition_date_str}")
    logger.info(f"Using input tarball path: {upstream_archive_path}")

    # --- S3 Configuration from Resources ---
    s3_landing_bucket = s3_config.landing_bucket
    s3_bronze_bucket = s3_config.bronze_bucket
    s3_access_key_id = aws_config.access_key_id
    s3_secret_access_key = aws_config.secret_access_key
    s3_use_ssl = s3_config.endpoint_scheme == "https"

    # --- DuckDB Configuration ---
    max_workers = config.max_workers
    threads_per_worker = config.threads_per_worker
    memory_per_worker = config.memory_per_worker
    safe_max_batch_size = config.safe_max_batch_size

    # --- Get S3 client ---
    s3_client = s3.get_client()

    # --- Parse input S3 path ---
    if not upstream_archive_path.startswith(f"s3://{s3_landing_bucket}/"):
         raise ValueError(f"Input path {upstream_archive_path} does not match expected bucket {s3_landing_bucket}")
    s3_key = upstream_archive_path[len(f"s3://{s3_landing_bucket}/"):]
    input_filename = Path(s3_key).name

    # --- Define Output Paths ---
    year, month, day = partition_date_str.split('-')
    # Final destination path (prefix)
    final_output_s3_prefix = f"alerts/year={year}/month={month}/day={day}"
    final_output_s3_path = f"s3://{s3_bronze_bucket}/{final_output_s3_prefix}"
    # Temporary staging path (prefix)
    temp_output_s3_prefix = f"alerts/_tmp_/{partition_date_str}_{context.run_id}"
    temp_output_s3_path = f"s3://{s3_bronze_bucket}/{temp_output_s3_prefix}"
    logger.info(f"Using temporary S3 path for staging: {temp_output_s3_path}")
    logger.info(f"Final output S3 path: {final_output_s3_path}")

    # --- Create temporary directory for processing ---
    with tempfile.TemporaryDirectory(prefix=f"process_{partition_date_str}_") as temp_dir:
        temp_dir_path = Path(temp_dir)
        temp_file_path = temp_dir_path / input_filename
        extracted_dir_path = temp_dir_path / "extracted"
        extracted_dir_path.mkdir()
        duckdb_temp_spill_path = temp_dir_path / "duckdb_spill"
        duckdb_temp_spill_path.mkdir()

        # --- Download the tarball from S3 ---
        logger.info(f"Downloading {upstream_archive_path} to {temp_file_path}")
        try:
            s3_client.download_file(Bucket=s3_landing_bucket, Key=s3_key, Filename=str(temp_file_path))
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

        # --- Find Avro files ---
        all_avro_files = list(extracted_dir_path.glob("*.avro"))
        total_files = len(all_avro_files)
        if total_files == 0:
            logger.warning(f"No .avro files found in extracted archive for partition {partition_date_str}. Returning empty prefix.")
            # Decide if this should be an error or just an empty result
            return f"{final_output_s3_path}/" # Return prefix even if empty

        # --- Calculate Batching ---
        ideal_batch_size_float = total_files / max_workers
        ideal_batch_size = math.ceil(ideal_batch_size_float)
        files_per_batch = max(1, min(safe_max_batch_size, ideal_batch_size))
        num_batches = math.ceil(total_files / files_per_batch)

        logger.info(f"Found {total_files} avro files. Processing in {num_batches} batches of up to {files_per_batch} files each.")
        logger.info(f"Using MAX_WORKERS={max_workers}, THREADS_PER_WORKER={threads_per_worker}, MEMORY_PER_WORKER={memory_per_worker}")

        # --- Define Batch Processing Function ---
        def process_batch(batch_index):
            batch_con = None # Ensure connection is closable in finally block
            try:
                batch_con = duckdb.connect()
                batch_con.execute("INSTALL httpfs; LOAD httpfs;")
                batch_con.execute("INSTALL json; LOAD json;")
                batch_con.execute(f"SET s3_access_key_id='{s3_access_key_id}';")
                batch_con.execute(f"SET s3_secret_access_key='{s3_secret_access_key}';")
                batch_con.execute(f"SET s3_endpoint='{s3_config.endpoint_host}:{s3_config.endpoint_port}';")
                batch_con.execute("SET s3_url_style='path';")
                batch_con.execute(f"SET s3_use_ssl={str(s3_use_ssl).lower()};")
                batch_con.execute(f"SET memory_limit='{memory_per_worker}';")
                batch_con.execute(f"SET threads TO {threads_per_worker};")
                batch_con.execute(f"SET temp_directory = '{duckdb_temp_spill_path.as_posix()}';")

                start_index = batch_index * files_per_batch
                end_index = min(start_index + files_per_batch, total_files)
                batch_files = all_avro_files[start_index:end_index]
                batch_files_str = ",".join(f"'{file.as_posix()}'" for file in batch_files)

                # Write to temporary S3 path
                output_parquet_path = f"{temp_output_s3_path}/part-{batch_index:05d}.parquet"

                query = f"""
                COPY (
                    SELECT * EXCLUDE (prv_candidates) FROM read_avro([{batch_files_str}], filename=true)
                ) TO '{output_parquet_path}'
                (FORMAT PARQUET, COMPRESSION zstd);
                """

                logger.info(f"Starting processing batch {batch_index} ({len(batch_files)} files) -> {output_parquet_path}")
                batch_start_time = time.time()
                batch_con.execute(query)
                batch_end_time = time.time()
                logger.info(f"Batch {batch_index} completed in {batch_end_time - batch_start_time:.2f} seconds.")
                return batch_index, output_parquet_path # Return the temp path for metadata
            except Exception as e:
                 logger.error(f"Error processing batch {batch_index}: {e}")
                 raise
            finally:
                if batch_con:
                    batch_con.close()

        # --- Execute Batches in Parallel ---
        completed_batches = 0
        processed_temp_files = [] # Store paths of successfully created temp files
        all_batches_successful = True
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_batch, i): i for i in range(num_batches)}

            for future in concurrent.futures.as_completed(futures):
                batch_idx = futures[future]
                try:
                    result_idx, temp_parquet_file_path = future.result()
                    logger.info(f"Successfully completed batch {result_idx}.")
                    processed_temp_files.append(temp_parquet_file_path)
                    completed_batches += 1
                except Exception as exc:
                    all_batches_successful = False
                    logger.error(f'Batch {batch_idx} generated an exception: {exc}')
                    # Don't re-raise immediately, allow other batches to finish/fail
                    # and cleanup temp files later

        if not all_batches_successful:
             logger.error(f"One or more batches failed. Only {completed_batches}/{num_batches} completed successfully.")
             # Attempt cleanup of any files created in the temporary S3 directory
             logger.warning(f"Attempting to clean up temporary S3 prefix: {temp_output_s3_prefix}")
             keys_to_delete = [{'Key': f"{temp_output_s3_prefix}/{Path(f).name}"} for f in processed_temp_files]
             if keys_to_delete:
                 try:
                     delete_response = s3_client.delete_objects(Bucket=s3_bronze_bucket, Delete={'Objects': keys_to_delete})
                     logger.info(f"Cleanup response for temporary files: {delete_response}")
                 except Exception as cleanup_exc:
                     logger.error(f"Failed to cleanup temporary S3 files: {cleanup_exc}")
             raise Exception("Not all processing batches finished successfully.")

        logger.info(f"Successfully processed all {num_batches} batches into temporary location: {temp_output_s3_path}")

        # --- Atomically move files from temporary to final S3 location ---
        logger.info(f"Committing files: Copying from {temp_output_s3_path} to {final_output_s3_path}")
        final_parquet_files = []
        try:
            files_to_copy = [Path(f).name for f in processed_temp_files]
            if not files_to_copy and total_files > 0:
                 raise Exception(f"Processed {total_files} files but no output files recorded in {temp_output_s3_path}")

            for filename in files_to_copy:
                source_key = f"{temp_output_s3_prefix}/{filename}"
                destination_key = f"{final_output_s3_prefix}/{filename}"
                logger.debug(f"Copying {source_key} to {destination_key}")
                s3_client.copy_object(
                    Bucket=s3_bronze_bucket,
                    CopySource={'Bucket': s3_bronze_bucket, 'Key': source_key},
                    Key=destination_key
                )
                final_parquet_files.append(f"s3://{s3_bronze_bucket}/{destination_key}")

            logger.info(f"Successfully copied {len(files_to_copy)} files to {final_output_s3_path}")

            # --- Clean up temporary S3 directory ---
            logger.info(f"Cleaning up temporary directory: {temp_output_s3_path}")
            keys_to_delete = [{'Key': f"{temp_output_s3_prefix}/{filename}"} for filename in files_to_copy]
            if keys_to_delete:
                delete_response = s3_client.delete_objects(Bucket=s3_bronze_bucket, Delete={'Objects': keys_to_delete})
                logger.info(f"Cleanup response for temporary files: {delete_response}")

        except Exception as commit_exc:
            logger.error(f"Failed during commit (copy/delete) stage: {commit_exc}")
            logger.error(f"Final destination {final_output_s3_path} might be in an inconsistent state.")
            logger.warning(f"Attempting to clean up temporary S3 prefix: {temp_output_s3_prefix}")
            try:
                keys_to_delete = [{'Key': f"{temp_output_s3_prefix}/{filename}"} for filename in files_to_copy]
                if keys_to_delete:
                    delete_response = s3_client.delete_objects(Bucket=s3_bronze_bucket, Delete={'Objects': keys_to_delete})
                    logger.info(f"Cleanup response for temporary files: {delete_response}")
            except Exception as cleanup_exc:
                logger.error(f"Failed to cleanup temporary S3 files during commit error handling: {cleanup_exc}")
            raise Exception("Failed to atomically commit files to final destination") from commit_exc


    # Add metadata about the output
    context.add_output_metadata({
        "output_s3_path": final_output_s3_path,
        "num_avro_files_processed": total_files,
        "num_parquet_files_created": completed_batches,
        "parquet_files": final_parquet_files,
    })
    return final_output_s3_path

