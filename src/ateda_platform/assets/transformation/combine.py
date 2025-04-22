import os
from pathlib import Path
from typing import Any, Dict, List, Optional  # Added Dict, List, Optional

import boto3
from dagster import (
    asset,
    AssetExecutionContext,
    ResourceParam,
    AssetIn,
    get_dagster_logger,
    Failure,
    open_pipes_session,
    AssetMaterialization,
    Output,
    Config,  # Added Config
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
)  # Added SparkOperatorResource
from ...partitions import daily_partitions
from ..ingestion.assets import raw_ztf_alerts

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
class CombineAlertsConfig(Config):  # Inherit directly from Config
    combine_script_s3_key: str = PydanticField(
        default="spark/process_combine_alerts.py",
        description="S3 key for the Python script that combines alerts.",
    )
    output_s3_prefix: str = PydanticField(
        default="combined_alerts",
        description="S3 prefix within the bronze bucket to write combined Parquet files.",
    )
    # Define Kubernetes/Spark specific configs here, mirroring KubeSparkAssetConfig if needed
    namespace: str = PydanticField(
        default="ateda-dev", description="Kubernetes namespace."
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
        default="3g", description="Executor memory (overridden default)."
    )
    dynamic_allocation_enabled: bool = PydanticField(
        default=True, description="Enable dynamic allocation."
    )
    min_executors: int = PydanticField(default=1, description="Min executors.")
    max_executors: int = PydanticField(default=3, description="Max executors.")
    # Add specific overrides if needed
    executor_memory_overhead_override: str = PydanticField(
        default="768m", description="Executor memory overhead."
    )
    kryo_buffer_max_override: str = PydanticField(
        default="128m", description="Kryo buffer max size."
    )


# --- Asset Definition --- #
@asset(
    name="combined_ztf_alerts",
    key_prefix=["silver"],  # Or "intermediate"
    group_name="transformation_kube",
    description="Combines small raw ZTF AVRO alert files into larger Parquet files using Spark.",
    partitions_def=daily_partitions,
    kinds={"kubernetes", "spark", "parquet"},
    ins={"raw_alerts_path": AssetIn(key=raw_ztf_alerts.key)},
)
def combined_ztf_alerts(
    context: AssetExecutionContext,
    aws_config: ResourceParam[AWSConfig],
    s3_config: ResourceParam[S3Config],
    pipes_config: ResourceParam[PipesConfig],
    spark_operator: ResourceParam[SparkOperatorResource],  # Added SparkOperatorResource
    s3: ResourceParam[S3Resource],
    raw_alerts_path: str,  # Input path from upstream asset
    config: CombineAlertsConfig,  # Use the specific config type
) -> Any:  # Still returns results from pipes, but we yield Output
    """Dagster asset to run the Spark job for combining alerts using Pipes."""

    # --- Determine Input Path (OS Path) --- #
    container_shared_mount_path = Path(
        "/mnt/ateda/shared"  # Shared volume path in container
    )
    input_os_path_str = raw_alerts_path
    input_path = Path(input_os_path_str)
    logger.info(f"Combine Job - Input OS path: {input_path}")

    # --- Pre-check Input Path Existence --- #
    if not input_path.exists() or not input_path.is_dir():
        logger.warning(
            f"Input path {input_path} does not exist or is not a directory. Skipping combine step."
        )
        partition_date_str = context.partition_key
        year, month, day = partition_date_str.split("-")
        output_prefix_partitioned = (
            f"{config.output_s3_prefix}/year={year}/month={month}/day={day}"
        )
        output_s3_uri_str = (
            f"s3a://{s3_config.bronze_bucket}/{output_prefix_partitioned}"
        )

        context.log_event(
            AssetMaterialization(
                asset_key=context.asset_key,
                description="Skipped: Input path did not exist.",
                metadata={
                    "input_path": str(input_path),
                    "output_path_expected": output_s3_uri_str,
                    "status": "skipped_input_missing",
                },
                partition=context.partition_key,
            )
        )
        return None

    # --- Setup (Only if input exists) --- #
    partition_date_str = context.partition_key
    year, month, day = partition_date_str.split("-")
    output_prefix_partitioned = (
        f"{config.output_s3_prefix}/year={year}/month={month}/day={day}"
    )
    output_s3_uri_str = f"s3a://{s3_config.bronze_bucket}/{output_prefix_partitioned}"
    logger.info(f"Combine Job - Output S3 URI: {output_s3_uri_str}")

    # --- Pipes Setup --- #
    pipes_bucket = pipes_config.bucket
    context_injector = PipesS3ContextInjector(
        client=s3.get_client(), bucket=pipes_bucket
    )
    message_reader = PipesS3MessageReader(
        client=s3.get_client(), bucket=pipes_bucket, include_stdio_in_messages=True
    )

    with open_pipes_session(
        context=context,
        context_injector=context_injector,
        message_reader=message_reader,
    ) as session:

        # Get Pipes CLI Arguments
        pipes_cli_params_dict = session.get_bootstrap_cli_arguments()
        pipes_cli_args_list = []
        for key, value in pipes_cli_params_dict.items():
            pipes_cli_args_list.extend([key, value])

        # --- Spark Job Arguments --- #
        spark_input_uri = f"file://{input_path}"  # Spark needs file:// for local FS
        spark_output_uri = output_s3_uri_str  # S3a URI for output
        spark_args = [
            "--input-dir",
            spark_input_uri,
            "--output-dir",
            spark_output_uri,
        ]

        # Combine Spark script args and Pipes args
        final_spark_script_args = spark_args + pipes_cli_args_list

        # --- Check Spark script existence ---
        main_application_file_key = config.combine_script_s3_key
        code_bucket = s3_config.code_bucket
        s3_client = s3.get_client()  # Use the S3Resource client
        try:
            s3_client.head_object(Bucket=code_bucket, Key=main_application_file_key)
            logger.info(
                f"Confirmed Spark script exists at s3://{code_bucket}/{main_application_file_key}"
            )
        except s3_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.error(
                    f"Spark script not found at s3://{code_bucket}/{main_application_file_key}"
                )
                raise FileNotFoundError(
                    f"Spark script {main_application_file_key} not found in bucket {code_bucket}."
                ) from e
            else:
                logger.error(f"Error checking Spark script existence: {e}")
                raise Failure(
                    f"Error checking Spark script s3://{code_bucket}/{main_application_file_key}"
                ) from e
        except Exception as e:
            logger.error(f"Unexpected error checking Spark script existence: {e}")
            raise Failure(
                f"Unexpected error checking Spark script s3://{code_bucket}/{main_application_file_key}"
            ) from e

        # --- Build Spark Application Object --- #
        app_name = f"combine-alerts-{context.partition_key or 'unpartitioned'}-{context.run_id[:8]}"
        app_name = "".join(c for c in app_name if c.isalnum() or c == "-").lower()[:63]
        main_application_file = (
            f"s3a://{code_bucket}/{main_application_file_key}"  # Use the verified key
        )
        internal_s3_url = s3_config.internal_endpoint_url
        access_key_id = aws_config.access_key_id
        secret_access_key = aws_config.secret_access_key

        # Common elements
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
        common_volume_mounts = [
            VolumeMount(name="spark-local-dir-1", mountPath="/tmp/spark-local-dir"),
            VolumeMount(
                name="shared-volume", mountPath=str(container_shared_mount_path)
            ),
        ]
        common_volumes = [
            Volume(name="spark-local-dir-1", emptyDir=EmptyDirVolumeSource()),
            Volume(
                name="shared-volume",
                hostPath=HostPathVolumeSource(
                    path=str(container_shared_mount_path), type="DirectoryOrCreate"
                ),
            ),
        ]

        # Base SparkConf (incorporating defaults and overrides)
        base_spark_conf = {
            "spark.hadoop.fs.s3a.endpoint": internal_s3_url,
            "spark.hadoop.fs.s3a.access.key": access_key_id,
            "spark.hadoop.fs.s3a.secret.key": secret_access_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",  # Assuming MinIO uses HTTP
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
            # Apply overrides from config
            "spark.executor.memoryOverhead": config.executor_memory_overhead_override,
            "spark.kryoserializer.buffer.max": config.kryo_buffer_max_override,
            # Default connection maximum, can be overridden if needed
            "spark.hadoop.fs.s3a.connection.maximum": "100",
        }

        spark_app = SparkApplication(
            metadata=ObjectMeta(
                name=app_name, namespace=config.namespace, labels=common_labels
            ),
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
                    instances=config.min_executors,  # Start with min instances
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

        # --- Run the Spark Job using the Resource --- #
        success = spark_operator.run_spark_application(spark_app)

        if success:
            yield Output(value=output_s3_uri_str, output_name="result")
            for event in session.get_results():
                if isinstance(event, AssetMaterialization):
                    context.log_event(event)
        else:
            # The resource raises Failure on error
            raise Failure(f"Spark job {app_name} failed or timed out.")
