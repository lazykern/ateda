import os
from pathlib import Path
from typing import Dict, List, Optional, Any

from dagster import (
    asset,
    AssetExecutionContext,
    ResourceParam,
    AssetIn,
    get_dagster_logger,
    open_pipes_session,
    Failure,
    AssetMaterialization,
    Config
)
from dagster_aws.s3 import S3Resource
from dagster_aws.pipes import (
    PipesS3ContextInjector,
    PipesS3MessageReader,
)
from pydantic import Field as PydanticField
import boto3

# Imports from the project
from ...resources import (
    AWSConfig,
    S3Config,
    RestCatalogConfig,
    PipesConfig,
    SparkOperatorResource
)
from ...partitions import daily_partitions

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
from .combine import combined_ztf_alerts

logger = get_dagster_logger()

# --- Configuration specific to this asset --- #
class FactDetectionConfig(Config):
    detection_script_s3_key: str = PydanticField(
        default="spark/process_fact_detection.py",
        description="S3 key for the Python script that performs fact detection and Iceberg merge."
    )
    namespace: str = PydanticField(default="ateda-dev", description="Kubernetes namespace.")
    spark_image: str = PydanticField(
        default="ghcr.io/lazykern/ateda-spark-runtime:latest",
        description="Spark runtime image."
    )
    spark_version: str = PydanticField(default="3.5.5", description="Spark version.")
    service_account: str = PydanticField(
        default="spark-operator-spark", description="Service account for Spark driver."
    )
    image_pull_policy: str = PydanticField(default="IfNotPresent", description="Image pull policy.")
    driver_cores: int = PydanticField(default=1, description="Driver cores.")
    driver_memory: str = PydanticField(default="1g", description="Driver memory.")
    executor_cores: int = PydanticField(default=1, description="Executor cores.")
    executor_memory: str = PydanticField(default="1536m", description="Executor memory (default).")
    dynamic_allocation_enabled: bool = PydanticField(default=True, description="Enable dynamic allocation.")
    min_executors: int = PydanticField(default=1, description="Min executors.")
    max_executors: int = PydanticField(default=3, description="Max executors.")
    executor_memory_overhead: str = PydanticField(default="512m", description="Executor memory overhead for Iceberg merge.")
    shuffle_partitions: str = PydanticField(default="100", description="Spark SQL shuffle partitions.")
    s3_max_connections: str = PydanticField(default="200", description="Max S3 connections.")

# --- Asset Definition --- #
@asset(
    name="fact_detection_kube",
    key_prefix=["silver"],
    group_name="transformation_kube",
    description="Processes combined ZTF alerts for fact detection and merges into Iceberg using Spark.",
    partitions_def=daily_partitions,
    kinds={"kubernetes", "spark", "iceberg"},
    ins={"combined_alerts_path": AssetIn(key=combined_ztf_alerts.key)},
)
def silver_fact_detection_kube(
    context: AssetExecutionContext,
    aws_config: ResourceParam[AWSConfig],
    s3_config: ResourceParam[S3Config],
    rest_catalog_config: ResourceParam[RestCatalogConfig],
    pipes_config: ResourceParam[PipesConfig],
    spark_operator: ResourceParam[SparkOperatorResource],
    s3: ResourceParam[S3Resource],
    combined_alerts_path: str,
    config: FactDetectionConfig,
) -> Any:
    """Dagster asset to run the Spark job for fact detection and Iceberg merge."""

    input_s3_uri = combined_alerts_path
    if not input_s3_uri or not input_s3_uri.startswith("s3a://"):
        raise Failure(f"Received invalid input path from upstream. Expected S3 URI, got: {input_s3_uri}")
    logger.info(f"Detection Job - Input S3 URI: {input_s3_uri}")

    # --- Prepare Paths and Parameters --- #
    internal_rest_catalog_uri = rest_catalog_config.internal_uri
    silver_bucket = s3_config.silver_bucket
    silver_warehouse_name = rest_catalog_config.silver_warehouse_name
    silver_database_name = rest_catalog_config.silver_database_name
    pipes_bucket = pipes_config.bucket
    internal_s3_url = s3_config.internal_endpoint_url
    access_key_id = aws_config.access_key_id
    secret_access_key = aws_config.secret_access_key

    # --- Set up Pipes --- #
    pipes_s3_client = boto3.client(
        's3',
        endpoint_url=s3_config.endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )
    context_injector = PipesS3ContextInjector(client=pipes_s3_client, bucket=pipes_bucket)
    message_reader = PipesS3MessageReader(client=pipes_s3_client, bucket=pipes_bucket, include_stdio_in_messages=True)

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
        spark_args = [
            "--input-dir", input_s3_uri,
            "--warehouse-name", silver_warehouse_name,
            "--database-name", silver_database_name,
        ]
        final_spark_script_args = spark_args + pipes_cli_args_list

        # --- Check Spark script existence ---
        main_application_file_key = config.detection_script_s3_key
        code_bucket = s3_config.code_bucket
        s3_client = s3.get_client() # Use the S3Resource client
        try:
            s3_client.head_object(Bucket=code_bucket, Key=main_application_file_key)
            logger.info(f"Confirmed Spark script exists at s3://{code_bucket}/{main_application_file_key}")
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f"Spark script not found at s3://{code_bucket}/{main_application_file_key}")
                raise FileNotFoundError(f"Spark script {main_application_file_key} not found in bucket {code_bucket}.") from e
            else:
                logger.error(f"Error checking Spark script existence: {e}")
                raise Failure(f"Error checking Spark script s3://{code_bucket}/{main_application_file_key}") from e
        except Exception as e:
            logger.error(f"Unexpected error checking Spark script existence: {e}")
            raise Failure(f"Unexpected error checking Spark script s3://{code_bucket}/{main_application_file_key}") from e

        # --- Build Spark Application Object --- #
        app_name = f"fact-detection-{context.partition_key or 'unpartitioned'}-{context.run_id[:8]}"
        app_name = "".join(c for c in app_name if c.isalnum() or c == '-').lower()[:63]
        main_application_file = f"s3a://{code_bucket}/{main_application_file_key}" # Use verified key

        # Common elements
        common_labels = {
            "dagster.io/run_id": context.run_id,
            "dagster.io/asset": context.asset_key.to_user_string().replace('/', '_'),
            "dagster.io/partition": context.partition_key or "default",
            "version": config.spark_version,
        }
        container_shared_mount_path = Path("/mnt/ateda/shared")
        common_volume_mounts = [
            VolumeMount(name="spark-local-dir-1", mountPath="/tmp/spark-local-dir"),
        ]
        common_volumes = [
            Volume(name="spark-local-dir-1", emptyDir=EmptyDirVolumeSource()),
        ]

        # Base SparkConf
        base_spark_conf = {
            # S3 Config
            "spark.hadoop.fs.s3a.endpoint": internal_s3_url,
            "spark.hadoop.fs.s3a.access.key": access_key_id,
            "spark.hadoop.fs.s3a.secret.key": secret_access_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            # Magic Committer
            "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
            "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
            "spark.sql.sources.commitProtocolClass": "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
            "spark.sql.parquet.output.committer.class": "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
            "spark.hadoop.fs.s3a.committer.name": "magic",
            # Iceberg Catalog Config
            f"spark.sql.catalog.{silver_warehouse_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{silver_warehouse_name}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
            f"spark.sql.catalog.{silver_warehouse_name}.uri": internal_rest_catalog_uri,
            f"spark.sql.catalog.{silver_warehouse_name}.warehouse": f"{silver_warehouse_name}",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            # General Spark Settings
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.local.dir": "/tmp/spark-local-dir",
            "spark.shuffle.service.enabled": "false",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.dynamicAllocation.shuffleTracking.enabled": "true",
            # Apply Overrides from Config
            "spark.executor.memoryOverhead": config.executor_memory_overhead,
            "spark.sql.shuffle.partitions": config.shuffle_partitions,
            "spark.hadoop.fs.s3a.connection.maximum": config.s3_max_connections,
            "spark.hadoop.fs.s3a.fast.upload": "true",
        }

        # Extra Environment Variables
        extra_env = {
            "S3_SILVER_BUCKET": silver_bucket,
            # Pass AWS creds via env for Spark Pods
            "AWS_ACCESS_KEY_ID": access_key_id,
            "AWS_SECRET_ACCESS_KEY": secret_access_key,
            "S3_ENDPOINT_URL": internal_s3_url,
        }

        spark_app = SparkApplication(
            metadata=ObjectMeta(
                name=app_name,
                namespace=config.namespace,
                labels=common_labels
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
                    envVars=extra_env,
                    volumeMounts=common_volume_mounts,
                ),
                executor=ExecutorSpec(
                    cores=config.executor_cores,
                    instances=config.min_executors,
                    memory=config.executor_memory,
                    labels=common_labels,
                    envVars=extra_env,
                    volumeMounts=common_volume_mounts,
                ),
                dynamicAllocation=DynamicAllocation(
                    enabled=config.dynamic_allocation_enabled,
                    minExecutors=config.min_executors,
                    maxExecutors=config.max_executors,
                ),
                sparkConf=base_spark_conf,
            )
        )

        # --- Run the Spark Job using the Resource --- #
        success = spark_operator.run_spark_application(spark_app)

        if success:
            return session.get_results()
        else:
            raise Failure(f"Spark job {app_name} for fact detection failed or timed out.") 