import argparse
import math
import os
import boto3
from dagster_pipes import (
    PipesCliArgsParamsLoader,
    PipesS3ContextLoader,
    PipesS3MessageWriter,
    open_dagster_pipes,
)
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Target size for output Parquet files (in MB)
TARGET_FILE_SIZE_MB = 256

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=str, required=True)
    parser.add_argument("--output-dir", type=str, required=True)
    args, _ = parser.parse_known_args()

    # --- Setup for Pipes --- #
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"]

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=s3_endpoint_url,
    )

    with open_dagster_pipes(
        message_writer=PipesS3MessageWriter(client=s3_client),
        context_loader=PipesS3ContextLoader(client=s3_client),
        params_loader=PipesCliArgsParamsLoader(),
    ) as pipes:

        spark = SparkSession.builder.appName("CombineAlerts").getOrCreate()
        sc = spark.sparkContext

        spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

        input_path = args.input_dir
        output_path = args.output_dir

        try:
            # Read the input Avro files
            df = spark.read.format("avro").load(input_path)
            
            # --- Calculate optimal number of partitions ---
            total_size_bytes = 0
            num_input_files = 0
            try:
                # More robust way to list files and get size if direct FS access works
                fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
                input_status = fs.globStatus(sc._jvm.org.apache.hadoop.fs.Path(input_path + "/*.avro")) # Assuming pattern
                if input_status: # Check if list is not empty
                    total_size_bytes = sum([f.getLen() for f in input_status])
                    num_input_files = len(input_status)
                    pipes.log.info(f"Approximate total input size: {total_size_bytes / (1024**2):.2f} MB from {num_input_files} files.")
                else:
                    pipes.log.warning(f"No *.avro files found matching pattern in {input_path}")
            except Exception as e:
                # Fallback: Use RDD count * estimated size (less accurate)
                pipes.log.warning(f"Could not get precise input size via Hadoop FS: {e}. Falling back to estimation.")
                # Need a way to estimate size if FS fails, this is rough:
                # distinct().count() can be expensive, avoid if possible
                # num_input_files = df.select(input_file_name()).distinct().count() # Rough estimate, might be slow
                # Instead, just use a default or fail if size needed?
                # For now, let's rely on the Hadoop FS method or handle zero size below.
                pass # total_size_bytes remains 0

            if total_size_bytes > 0:
                target_size_bytes = TARGET_FILE_SIZE_MB * 1024 * 1024
                num_partitions = math.ceil(total_size_bytes / target_size_bytes)
                # Add a small buffer to avoid too few partitions if estimation is off
                num_partitions = max(1, num_partitions + 2) # Ensure at least 1, add buffer
                pipes.log.info(f"Calculated target partitions based on {TARGET_FILE_SIZE_MB}MB file size: {num_partitions}")
            elif num_input_files > 0: # If we counted files but not size
                 pipes.log.warning("Could not determine input size, using default partitions per 1000 files.")
                 num_partitions = max(1, math.ceil(num_input_files / 1000)) # Arbitrary heuristic
            else: # No files found or size calculation failed badly
                 pipes.log.warning("No input files found or size could not be estimated. Will write 1 partition if possible.")
                 num_partitions = 1 # Default fallback for empty/failed input

            # Coalesce and write as Parquet
            pipes.log.info(f"Coalescing into {num_partitions} partitions and writing to {output_path}")
            df.coalesce(num_partitions).write.format("parquet").mode("overwrite").save(
                output_path
            )

            pipes.log.info("Successfully combined alerts into Parquet format.")

            # Report Materialization
            pipes.report_asset_materialization(
                metadata={
                    # Report the S3 path, not the URI, but it's already an S3 path
                    "output_path": output_path,
                    "output_format": "parquet",
                    "calculated_partitions": num_partitions,
                    "target_file_size_mb": TARGET_FILE_SIZE_MB,
                    # Report the OS path for input too
                    "input_path": input_path.replace("file://", ""),
                    "input_files_count": num_input_files, # Might be 0 if estimation failed
                    "input_total_size_mb": round(total_size_bytes / (1024**2), 2), # Might be 0
                }
            )

        except AnalysisException as e:
            if "Path does not exist" in str(e) or "Input path does not exist" in str(e):
                pipes.log.warning(f"Input path not found or empty: {input_path}. Skipping processing and reporting empty materialization.")
                # Report an empty materialization to indicate the asset ran but found no data
                pipes.report_asset_materialization(
                     metadata={
                        # Report S3 path here too
                        "output_path": output_path,
                        "status": "skipped_empty_input",
                        # Input path is still local file system
                        "input_path": input_path.replace("file://", ""),
                    }
                )
                pass # Exit cleanly after reporting
            else:
                pipes.log.error(f"Spark Analysis Error: {e}")
                raise
        except Exception as e:
            pipes.log.error(f"An error occurred during Spark processing: {e}")
            raise
        finally:
            spark.stop()

if __name__ == "__main__":
    main() 