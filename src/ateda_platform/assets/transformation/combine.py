import os
from pathlib import Path
from typing import Any  # Keep Any for session.get_results() but yield Output

import boto3
from s3fs import S3FileSystem
from dagster import (
    asset,
    AssetExecutionContext,
    ResourceParam,
    AssetIn,
    AssetOut,
    get_dagster_logger,
    Failure,
    open_pipes_session,  # Added open_pipes_session
    AssetMaterialization,
    Output,  # Added Output
)
from dagster_aws.s3 import S3Resource
from dagster_aws.pipes import (
    PipesS3ContextInjector,  # Added pipes imports
    PipesS3MessageReader,
)
from mypy_boto3_s3 import S3Client
from pydantic import Field as PydanticField

# Imports from the project
from ...resources import AWSConfig, S3Config, PipesConfig  # Added PipesConfig
from ...partitions import daily_partitions
from ..ingestion.assets import raw_ztf_alerts

# Import shared utilities
from .spark_kube_utils import KubeSparkAssetConfig, run_spark_on_kubernetes

logger = get_dagster_logger()


# --- Configuration specific to this asset --- #
class CombineAlertsConfig(KubeSparkAssetConfig):  # Inherit common settings
    combine_script_s3_key: str = PydanticField(
        default="spark/process_combine_alerts.py",
        description="S3 key for the Python script that combines alerts.",
    )
    # Add any other specific config needed for combining, if any
    output_s3_prefix: str = PydanticField(
        default="combined_alerts",
        description="S3 prefix within the bronze bucket to write combined Parquet files.",
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
    s3: ResourceParam[S3Resource],
    raw_alerts_path: str,  # Input path from upstream asset
    config: CombineAlertsConfig,  # Use the specific config type
) -> Any:  # Still returns results from pipes, but we yield Output
    """Dagster asset to run the Spark job for combining alerts using Pipes."""

    # --- Determine Input Path (OS Path) --- #
    container_shared_mount_path = Path(
        "/mnt/ateda/shared"
    )  # Still needed for input reading by Spark
    # Input path comes from the upstream asset (should be standard OS path)
    input_os_path_str = raw_alerts_path
    input_path = Path(input_os_path_str)
    logger.info(f"Combine Job - Input OS path: {input_path}")

    # --- Pre-check Input Path Existence --- #
    if not input_path.exists() or not input_path.is_dir():
        logger.warning(
            f"Input path {input_path} does not exist or is not a directory. Skipping combine step."
        )
        # Construct expected output S3 path for metadata reporting
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
        return None  # Or handle differently if downstream should run regardless

    # --- Setup (Only if input exists) --- #
    # Construct output S3 URI
    partition_date_str = context.partition_key
    year, month, day = partition_date_str.split("-")
    output_prefix_partitioned = (
        f"{config.output_s3_prefix}/year={year}/month={month}/day={day}"
    )
    output_s3_uri_str = f"s3a://{s3_config.bronze_bucket}/{output_prefix_partitioned}"
    logger.info(f"Combine Job - Output S3 URI: {output_s3_uri_str}")

    # --- Pipes Setup --- #
    pipes_bucket = pipes_config.bucket
    # s3_client already obtained above for deletion, reuse if needed for pipes
    # s3_client_boto can be used here too if Pipes doesn't need a specific interface
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

        # --- Spark Job Arguments (Use file:// URIs here) --- #
        spark_input_uri = f"file://{input_path}"
        spark_output_uri = output_s3_uri_str  # Use the S3 URI directly
        spark_args = [
            "--input-dir",
            spark_input_uri,
            "--output-dir",
            spark_output_uri,
        ]

        # --- Spark Configuration Overrides (if any) --- #
        # Job-specific performance tuning or overrides can go here.
        # S3 committer settings are now handled globally in spark_kube_utils.py
        # Staging path and conflict mode are not used by the Magic Committer.
        spark_conf_overrides = {
            "spark.kryoserializer.buffer.max": "128m",
            "spark.executor.memoryOverhead": "768m",
        }

        # --- Run the Spark Job using the helper --- #
        success = run_spark_on_kubernetes(
            context=context,
            k8s_config=config,
            aws_config=aws_config,
            s3_config=s3_config,
            s3_resource=s3,
            app_name_prefix="combine-alerts",
            spark_script_s3_key=config.combine_script_s3_key,
            spark_script_args=spark_args,
            asset_key_label="combined_ztf_alerts",
            extra_spark_conf=spark_conf_overrides,
            executor_memory_override="3g",
            use_pipes=True,
            pipes_bootstrap_args=pipes_cli_args_list,
        )

        if success:
            # Yield the S3 URI for downstream assets (the primary data flow)
            yield Output(value=output_s3_uri_str, output_name="result")

            # Process events from Pipes for observability ONLY
            # We log the AssetMaterialization reported by the Spark script,
            # but DO NOT yield anything else from this loop.
            for event in session.get_results():
                if isinstance(event, AssetMaterialization):
                    # Log the materialization reported by the script for UI visibility
                    context.log_event(event)
                # Could add logging for other event types if desired

            # Don't yield or return anything after this point in the success case.
        else:
            # The helper function raises Failure on error
            raise Failure("Spark job submission or monitoring failed unexpectedly.")
