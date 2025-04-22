import os
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    ResourceParam,
    AssetIn,
    get_dagster_logger,
    open_pipes_session,
    Failure,
    AssetMaterialization
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
    RestCatalogConfig,
    PipesConfig
)
from ...partitions import daily_partitions

# Import shared utilities and upstream asset
from .spark_kube_utils import (
    KubeSparkAssetConfig,
    run_spark_on_kubernetes
)
from .combine import combined_ztf_alerts # Import the upstream asset key

logger = get_dagster_logger()

# --- Configuration specific to this asset --- #
class FactDetectionConfig(KubeSparkAssetConfig): # Inherit common settings
    detection_script_s3_key: str = PydanticField(
        default="spark/process_fact_detection.py",
        description="S3 key for the Python script that performs fact detection and Iceberg merge."
    )

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
    s3: ResourceParam[S3Resource],
    combined_alerts_path: str, # Input path from upstream asset
    config: FactDetectionConfig, # Use the specific config type
):
    """Dagster asset to run the Spark job for fact detection and Iceberg merge."""

    # Input path comes from upstream asset (should be S3a URI)
    input_os_path_str = combined_alerts_path
    input_s3_uri = combined_alerts_path # Already an s3a:// URI
    # Validate it looks like an S3 path
    if not input_s3_uri or not input_s3_uri.startswith("s3a://"):
        raise Failure(f"Received invalid input path from upstream. Expected S3 URI, got: {input_s3_uri}")
    logger.info(f"Detection Job - Input S3 URI: {input_s3_uri}")

    # --- Prepare Paths and Parameters --- #
    internal_rest_catalog_uri = rest_catalog_config.internal_uri
    silver_bucket = s3_config.silver_bucket # Needed for env var
    silver_warehouse_name = rest_catalog_config.silver_warehouse_name
    silver_database_name = rest_catalog_config.silver_database_name
    pipes_bucket = pipes_config.bucket

    # --- Set up Pipes --- #
    s3_client = s3.get_client() # Reusable client
    context_injector = PipesS3ContextInjector(client=s3_client, bucket=pipes_bucket)
    message_reader = PipesS3MessageReader(client=s3_client, bucket=pipes_bucket, include_stdio_in_messages=True)

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

        # --- Spark Job Arguments (including pipes) --- #
        # Prepend file:// URI scheme for Spark argument
        spark_input_uri = input_s3_uri
        spark_args = [
            "--input-dir", spark_input_uri, # Use the URI here
            "--warehouse-name", silver_warehouse_name,
            "--database-name", silver_database_name,
        ]
        # Note: pipes args are passed separately to the helper function

        # --- Spark Configuration Overrides --- #
        spark_conf_overrides = {
            # Iceberg Catalog Configuration
            f"spark.sql.catalog.{silver_warehouse_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{silver_warehouse_name}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
            f"spark.sql.catalog.{silver_warehouse_name}.uri": internal_rest_catalog_uri,
            f"spark.sql.catalog.{silver_warehouse_name}.warehouse": silver_warehouse_name,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

            # Tuning specific to this job
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.dynamicAllocation.shuffleTracking.enabled": "true",
            "spark.sql.shuffle.partitions": "100", # Explicitly set shuffle partitions for merge
            "spark.executor.memoryOverhead": "512m", # Might need more overhead for Iceberg merge
            "spark.hadoop.fs.s3a.connection.maximum": "200",
            "spark.hadoop.fs.s3a.fast.upload": "true",
        }

        # --- Extra Environment Variables for Spark Pods --- #
        extra_env = {
            "S3_SILVER_BUCKET": silver_bucket, # Example specific env var
            # Pass REST Catalog URI via env if needed by script (or rely on Spark Conf)
            # "INTERNAL_REST_CATALOG_URI": internal_rest_catalog_uri,
        }

        # --- Run the Spark Job using the helper --- #
        success = run_spark_on_kubernetes(
            context=context,
            k8s_config=config,
            aws_config=aws_config,
            s3_config=s3_config,
            s3_resource=s3,
            app_name_prefix="fact-detection",
            spark_script_s3_key=config.detection_script_s3_key,
            spark_script_args=spark_args,
            asset_key_label="fact_detection_kube",
            extra_spark_conf=spark_conf_overrides,
            extra_env_vars=extra_env,
            use_pipes=True,
            pipes_bootstrap_args=pipes_cli_args_list,
        )

        if success:
            # Get results (like materializations) reported via Pipes
            return session.get_results()
        else:
            # Helper function raises Failure
            raise Failure("Spark job for fact detection failed.") 