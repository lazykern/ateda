import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
import shutil
import tempfile
import tarfile
import urllib.parse
import json
import boto3
from dagster import (
    PipesExecutionResult,
    asset,
    AssetExecutionContext,
    ResourceParam,
    AssetIn,
    get_dagster_logger,
    Failure,
    open_pipes_session,
    AssetMaterialization,
    Output,
    Config,
)
from dagster_aws.s3 import S3Resource
from dagster_aws.pipes import (
    PipesS3ContextInjector,
    PipesS3MessageReader,
)
from pydantic import Field as PydanticField

# Imports from the project
from ...resources import (
    AWSConfig,
    S3Config,
    PipesConfig,
    SparkOperatorResource,
)
from ...partitions import daily_partitions
# Import upstream asset from this module
from .bronze_ztf_alert_archive import bronze_ztf_alert_archive

# Import K8s models needed to build the SparkApplication object
from ...kubernetes.spark_models import (
    SparkApplication,
    ObjectMeta,
    SparkApplicationSpec,
    DriverSpec,
    ExecutorSpec,
    RestartPolicy,
    DynamicAllocation,
    Volume,
    VolumeMount,
    EmptyDirVolumeSource,
    HostPathVolumeSource,
)

logger = get_dagster_logger()


# --- Configuration specific to this asset --- #
class BronzeZtfAlertParquetConfig(Config): # Renamed config class
    combine_script_s3_key: str = PydanticField(
        default="spark/process_combine_alerts.py",
        description="S3 key for the Python script that extracts and combines alerts.", # Fixed quotes
    )
    output_s3_prefix: str = PydanticField(
        default="combined_alerts_parquet", # Changed default prefix slightly
        description="S3 prefix within the bronze bucket to write combined Parquet files.", # Fixed quotes
    )
    # Define Kubernetes/Spark specific configs here
    namespace: str = PydanticField(
        default="ateda-dev", description="Kubernetes namespace." # Fixed quotes
    )
    spark_image: str = PydanticField(
        default="ghcr.io/lazykern/ateda-spark-runtime:latest",
        description="Spark runtime image.",
    )
    spark_version: str = PydanticField(default="3.5.5", description="Spark version.")
    service_account: str = PydanticField(
        default="spark-operator-spark", description="Service account for Spark driver."
    )
    image_pull_policy: str = PydanticField(
        default="IfNotPresent", description="Image pull policy."
    )
    driver_cores: int = PydanticField(default=1, description="Driver cores.")
    driver_memory: str = PydanticField(default="1g", description="Driver memory.")
    executor_cores: int = PydanticField(default=1, description="Executor cores.")
    executor_memory: str = PydanticField(
        default="2g", description="Executor memory (overridden default)."
    )
    dynamic_allocation_enabled: bool = PydanticField(
        default=True, description="Enable dynamic allocation." # Fixed quotes
    )
    min_executors: int = PydanticField(default=1, description="Min executors.")
    max_executors: int = PydanticField(default=2, description="Max executors.")
    executor_memory_overhead_override: str = PydanticField(
        default="768m", description="Executor memory overhead." # Fixed quotes
    )
    kryo_buffer_max_override: str = PydanticField(
        default="128m", description="Kryo buffer max size." # Fixed quotes
    )


# --- Asset Definition --- #
@asset(
    name="bronze_combined_alerts", # Renamed asset
    key_prefix=["bronze"],
    group_name="ingestion", # Moved to ingestion group
    description="Downloads ZTF alert archive from S3, extracts AVRO, combines into Parquet using Spark, outputs to Bronze S3.", # Fixed quotes
    partitions_def=daily_partitions,
    kinds={"kubernetes", "spark", "parquet"},
    ins={"s3_archive_path": AssetIn(key=bronze_ztf_alert_archive.key)}, # Depends on archive asset
)
def bronze_ztf_alert_parquet( # Renamed function
    context: AssetExecutionContext,
    aws_config: ResourceParam[AWSConfig],
    s3_config: ResourceParam[S3Config],
    pipes_config: ResourceParam[PipesConfig],
    spark_operator: ResourceParam[SparkOperatorResource],
    s3: ResourceParam[S3Resource],
    s3_archive_path: str,
    config: BronzeZtfAlertParquetConfig,
) -> Iterator[PipesExecutionResult]:
    """Dagster asset to run Spark job for extracting alert archive & combining into Parquet.""" # Fixed quotes

    logger.info(f"Starting asset for partition: {context.partition_key}")
    logger.info(f"Input S3 Archive path: {s3_archive_path}")

    # Define paths
    container_shared_mount_path = Path("/mnt/ateda/shared")
    s3_client = s3.get_client()
    bronze_bucket = s3_config.bronze_bucket
    partition_date_str = context.partition_key
    year, month, day = partition_date_str.split("-")

    # Path on shared volume where files will be extracted for Spark
    shared_extract_path = (
        container_shared_mount_path / "alerts_extracted" / f"year={year}" / f"month={month}" / f"day={day}"
    )
    # S3 output path for final Parquet files
    output_prefix_partitioned = (
        f"{config.output_s3_prefix}/year={year}/month={month}/day={day}"
    )
    output_s3_uri_str = f"s3a://{bronze_bucket}/{output_prefix_partitioned}"

    # Temporary local directory for download
    temp_download_dir = None
    spark_job_success = False

    output = None

    try:
        # --- Step 1: Download archive locally --- #
        temp_download_dir = tempfile.mkdtemp(prefix=f"download_{partition_date_str}_")
        temp_download_dir_path = Path(temp_download_dir)
        parsed_s3_path = urllib.parse.urlparse(s3_archive_path)
        s3_key = parsed_s3_path.path.lstrip('/')
        archive_filename = Path(s3_key).name
        local_archive_path = temp_download_dir_path / archive_filename

        logger.info(f"Downloading {s3_archive_path} to temporary file {local_archive_path}")
        try:
            # Assuming bucket name is implicitly handled by S3Resource config or needs extraction
            # Let's extract bucket from the s3_archive_path assuming s3://bucket/key format
            input_bucket = parsed_s3_path.netloc
            s3_client.download_file(
                Bucket=input_bucket, Key=s3_key, Filename=str(local_archive_path)
            )
            logger.info("Successfully downloaded archive locally.")
        except Exception as e:
            logger.error(f"Failed to download {s3_archive_path}: {e}")
            raise Failure(f"Failed to download archive {s3_archive_path}") from e

        # --- Step 2: Extract archive to shared volume --- #
        logger.info(f"Extracting {local_archive_path} to shared volume path {shared_extract_path}")
        # Ensure target directory exists and is empty
        if shared_extract_path.exists():
            logger.warning(f"Shared extract path {shared_extract_path} already exists. Removing it.")
            shutil.rmtree(shared_extract_path)
        shared_extract_path.mkdir(parents=True)

        try:
            with tarfile.open(local_archive_path, "r:gz") as tar:
                 # Safer extraction
                 for member in tar.getmembers():
                     member_path = shared_extract_path / Path(member.name)
                     # Basic check to prevent writing outside the target directory
                     if not member_path.resolve().is_relative_to(shared_extract_path.resolve()):
                         raise IOError(f"Attempted Path Traversal in Tar File: {member.name}")
                     tar.extract(member, path=shared_extract_path)
            logger.info("Successfully extracted archive to shared volume.")
        except Exception as e:
            logger.error(f"Failed to extract archive {local_archive_path} to {shared_extract_path}: {e}")
            raise Failure(f"Failed to extract archive to shared volume: {e}") from e

        # --- Step 3: Run Spark Job --- #
        with open_pipes_session(
            context=context,
            context_injector=PipesS3ContextInjector(client=s3.get_client(), bucket=pipes_config.bucket),
            message_reader=PipesS3MessageReader(client=s3.get_client(), bucket=pipes_config.bucket, include_stdio_in_messages=True),
        ) as session:

            pipes_cli_args_list = []
            for key, value in session.get_bootstrap_cli_arguments().items():
                pipes_cli_args_list.extend([key, value])

            # Spark job reads from the shared volume path
            spark_input_dir_uri = f"file://{shared_extract_path}"
            spark_args = [
                "--input-dir",
                spark_input_dir_uri,
                "--output-dir",
                output_s3_uri_str,
            ]
            final_spark_script_args = spark_args + pipes_cli_args_list

            # Check Spark script existence (remains the same)
            main_application_file_key = config.combine_script_s3_key
            code_bucket = s3_config.code_bucket
            try:
                s3_client.head_object(Bucket=code_bucket, Key=main_application_file_key)
                logger.info(f"Confirmed Spark script exists at s3://{code_bucket}/{main_application_file_key}")
            except s3_client.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    logger.error(f"Spark script not found at s3://{code_bucket}/{main_application_file_key}")
                    raise FileNotFoundError(f"Spark script {main_application_file_key} not found in bucket {code_bucket}.") from e
                else:
                    logger.error(f"Error checking Spark script existence: {e}")
                    raise Failure(f"Error checking Spark script s3://{code_bucket}/{main_application_file_key}") from e
            except Exception as e:
                logger.error(f"Unexpected error checking Spark script existence: {e}")
                raise Failure(f"Unexpected error checking Spark script s3://{code_bucket}/{main_application_file_key}") from e

            # Build SparkApplication Object
            asset_name_base = context.asset_key.to_user_string().replace('/', '_')
            app_name = f"{asset_name_base}-{context.partition_key or 'unpartitioned'}-{context.run_id[:8]}"
            app_name = "".join(c for c in app_name if c.isalnum() or c == "-").lower()[:63]
            main_application_file = f"s3a://{code_bucket}/{main_application_file_key}"
            internal_s3_url = s3_config.internal_endpoint_url
            access_key_id = aws_config.access_key_id
            secret_access_key = aws_config.secret_access_key

            common_labels = {
                "dagster.io/run_id": context.run_id,
                "dagster.io/asset": context.asset_key.to_user_string().replace('/', '_'),
                "dagster.io/partition": context.partition_key or "default",
                "version": config.spark_version,
            }
            spark_pod_env_vars = {
                "S3_ENDPOINT_URL": internal_s3_url,
                "AWS_ACCESS_KEY_ID": access_key_id,
                "AWS_SECRET_ACCESS_KEY": secret_access_key,
            }
            # Add back hostPath mount for shared volume
            common_volume_mounts = [
                VolumeMount(name="spark-local-dir-1", mountPath="/tmp/spark-local-dir"),
                VolumeMount(name="shared-volume", mountPath=str(container_shared_mount_path)),
            ]
            common_volumes = [
                Volume(name="spark-local-dir-1", emptyDir=EmptyDirVolumeSource()),
                Volume(name="shared-volume", hostPath=HostPathVolumeSource(path=str(container_shared_mount_path), type="DirectoryOrCreate")),
            ]

            base_spark_conf = {
                "spark.hadoop.fs.s3a.endpoint": internal_s3_url,
                "spark.hadoop.fs.s3a.access.key": access_key_id,
                "spark.hadoop.fs.s3a.secret.key": secret_access_key,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", # Keep for robustness
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
                "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
                "spark.sql.sources.commitProtocolClass": "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
                "spark.sql.parquet.output.committer.class": "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
                "spark.hadoop.fs.s3a.committer.name": "magic",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.local.dir": "/tmp/spark-local-dir",
                "spark.shuffle.service.enabled": "false",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.files.maxPartitionBytes": "1024m",
                "spark.executor.memoryOverhead": config.executor_memory_overhead_override,
                "spark.kryoserializer.buffer.max": config.kryo_buffer_max_override,
                "spark.hadoop.fs.s3a.connection.maximum": "100",
            }

            spark_app = SparkApplication(
                metadata=ObjectMeta(name=app_name, namespace=config.namespace, labels=common_labels),
                spec=SparkApplicationSpec(
                    type="Python",
                    pythonVersion="3",
                    mode="cluster",
                    image=config.spark_image,
                    imagePullPolicy=config.image_pull_policy,
                    mainApplicationFile=main_application_file,
                    arguments=final_spark_script_args,
                    sparkVersion=config.spark_version,
                    restartPolicy=RestartPolicy(type="Never"),
                    volumes=common_volumes,
                    driver=DriverSpec(
                        cores=config.driver_cores,
                        memory=config.driver_memory,
                        labels=common_labels,
                        serviceAccount=config.service_account,
                        envVars=spark_pod_env_vars,
                        volumeMounts=common_volume_mounts,
                    ),
                    executor=ExecutorSpec(
                        cores=config.executor_cores,
                        instances=config.min_executors,
                        memory=config.executor_memory,
                        labels=common_labels,
                        envVars=spark_pod_env_vars,
                        volumeMounts=common_volume_mounts,
                    ),
                    dynamicAllocation=DynamicAllocation(
                        enabled=config.dynamic_allocation_enabled,
                        minExecutors=config.min_executors,
                        maxExecutors=config.max_executors,
                    ),
                    sparkConf=base_spark_conf,
                ),
            )

            spark_job_success = spark_operator.run_spark_application(spark_app)

            if spark_job_success:
                output = session.get_results()
            else:
                raise Failure(f"Spark job {app_name} failed or timed out.")

    finally:
        # --- Step 4: Cleanup --- #
        # Cleanup temporary download directory
        if temp_download_dir:
            try:
                logger.info(f"Cleaning up temporary download directory: {temp_download_dir}")
                shutil.rmtree(temp_download_dir)
                logger.info("Successfully removed temporary download directory.")
            except Exception as e:
                logger.warning(f"Failed to cleanup temporary download directory {temp_download_dir}: {e}")

        # Cleanup extracted directory on shared volume *only on success*?
        # For now, let's clean up on success to avoid leaving data if the main goal was met.
        if spark_job_success and shared_extract_path.exists():
            try:
                logger.info(f"Cleaning up shared volume extract directory: {shared_extract_path}")
                shutil.rmtree(shared_extract_path)
                logger.info("Successfully removed shared volume extract directory.")
            except Exception as e:
                logger.warning(f"Failed to cleanup shared extract directory {shared_extract_path}: {e}") 

    yield from output
    
    return output_s3_uri_str