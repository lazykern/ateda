import os
import time
import yaml
import kubernetes
from typing import Dict, Optional
from dagster import (
    ResourceParam,
    asset,
    AssetExecutionContext,
    AssetIn,
    get_dagster_logger,
    Failure,
    open_pipes_session,
    Config,
)
from pydantic import Field
from dagster_aws.s3 import S3Resource
from dagster_aws.pipes import (
    PipesS3ContextInjector,
    PipesS3MessageReader,
)
from ...resources import AWSConfig, S3Config, RestCatalogConfig, PipesConfig

from ...partitions import daily_partitions
from ..ingestion.assets import ztf_bronze_alerts # Assuming bronze alerts are the input

logger = get_dagster_logger()

# Define the SparkApplication group, version, and plural
SPARK_OP_GROUP = "sparkoperator.k8s.io"
SPARK_OP_VERSION = "v1beta2"
SPARK_OP_PLURAL = "sparkapplications"


class KubeSparkAssetConfig(Config):
    """Configuration for Kubernetes Spark assets."""
    namespace: str = Field(
        default_factory=lambda: os.getenv("K8S_NAMESPACE", "ateda-dev"),
        description=(
            "Kubernetes namespace for Spark job. "
            "Defaults to K8S_NAMESPACE env var or 'ateda-dev'."
        )
    )
    spark_image: str = Field(
        default="ghcr.io/lazykern/ateda-spark-runtime:latest",
        description="Docker image for Spark driver and executors."
    )
    spark_version: str = Field(
        default="3.5.5",
        description="Spark version used in the image."
    )
    spark_script_s3_key: str = Field(
        default="spark/process_fact_detection.py",
        description="S3 key for the main Python application file within the code bucket."
    )
    service_account: str = Field(
        default="spark-operator-spark",
        description="Kubernetes service account for the Spark driver pod."
    )
    image_pull_policy: str = Field(
        default="IfNotPresent",
        description="Kubernetes image pull policy (e.g., Always, Never, IfNotPresent)."
    )
    driver_cores: int = Field(
        default=1,
        description="Number of cores for the Spark driver."
    )
    driver_memory: str = Field(
        default="2048m",
        description="Memory for the Spark driver (e.g., '1g', '2048m')."
    )
    executor_cores: int = Field(
        default=1,
        description="Number of cores per Spark executor."
    )
    executor_memory: str = Field(
        default="2048m",
        description="Memory per Spark executor (e.g., '1g', '2048m')."
    )
    dynamic_allocation_enabled: bool = Field(
        default=True,
        description="Enable Spark dynamic allocation."
    )
    min_executors: int = Field(
        default=1,
        description="Minimum number of executors for dynamic allocation."
    )
    max_executors: int = Field(
        default=2,
        description="Maximum number of executors for dynamic allocation."
    )
    monitor_timeout_seconds: int = Field(
        default=1800,
        description="Timeout in seconds for waiting for Spark job completion."
    )
    monitor_interval_seconds: int = Field(
        default=15,
        description="Polling interval in seconds for checking Spark job status."
    )


# --- Spark Application Monitoring ---

def _monitor_spark_application(
    custom_objects_api: kubernetes.client.CustomObjectsApi,
    namespace: str,
    app_name: str,
    timeout_seconds: int,
    monitor_interval_seconds: int,
    logger,
) -> bool:
    """Monitors the SparkApplication status until completion, failure, or timeout."""
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout_seconds:
            logger.error(f"Timeout waiting for SparkApplication '{app_name}' to complete.")
            try:
                custom_objects_api.delete_namespaced_custom_object(
                    group=SPARK_OP_GROUP, version=SPARK_OP_VERSION, namespace=namespace,
                    plural=SPARK_OP_PLURAL, name=app_name)
                logger.info(f"Deleted timed-out SparkApplication '{app_name}'")
            except kubernetes.client.ApiException as del_e:
                logger.warning(f"Failed to delete timed-out SparkApplication '{app_name}': {del_e}")
            raise Failure(f"Timeout waiting for SparkApplication '{app_name}'")

        try:
            status_response = custom_objects_api.get_namespaced_custom_object_status(
                group=SPARK_OP_GROUP,
                version=SPARK_OP_VERSION,
                namespace=namespace,
                plural=SPARK_OP_PLURAL,
                name=app_name,
            )
            app_state = status_response.get("status", {}).get("applicationState", {}).get("state")
            error_message = status_response.get("status", {}).get("applicationState", {}).get("errorMessage")

            logger.info(f"SparkApplication '{app_name}' state: {app_state}")

            if app_state == "COMPLETED":
                logger.info(f"SparkApplication '{app_name}' completed successfully.")
                return True

            elif app_state in ["FAILED", "SUBMISSION_FAILED", "UNKNOWN"]:
                logger.error(f"SparkApplication '{app_name}' failed with state {app_state}.")
                if error_message:
                    logger.error(f"Error Message: {error_message}")
                logger.warning("Spark application failed. Manual cleanup of Pipes S3 files might be needed.")
                raise Failure(f"SparkApplication '{app_name}' failed. State: {app_state}. Error: {error_message or 'None'}")

            # Other states: continue polling

        except kubernetes.client.ApiException as e:
            if e.status == 404:
                logger.warning(f"SparkApplication '{app_name}' not found, may have been deleted or not yet fully created. Retrying...")
            else:
                logger.error(f"Error monitoring SparkApplication '{app_name}': {e.reason}")
                # Allow retry within timeout loop
        except Exception as e:
            logger.error(f"Unexpected error monitoring SparkApplication '{app_name}': {e}")
            raise Failure(f"Unexpected error monitoring SparkApplication {app_name}") from e

        time.sleep(monitor_interval_seconds)

# --- Kubernetes Spark Asset ---

@asset(
    name="fact_detection_kube", # New name to distinguish
    key_prefix=["silver"],
    group_name="transformation_kube", # New group name
    description="Processes ZTF alerts for fact detection using Spark Operator on Kubernetes via Dagster Pipes.",
    partitions_def=daily_partitions,
    kinds={"kubernetes", "spark", "iceberg"},
    ins={
        "staged_parquet_s3_path": AssetIn(key=ztf_bronze_alerts.key)
    }
)
def silver_fact_detection_kube(
    context: AssetExecutionContext,
    aws_config: ResourceParam[AWSConfig],
    s3_config: ResourceParam[S3Config],
    rest_catalog_config: ResourceParam[RestCatalogConfig],
    pipes_config: ResourceParam[PipesConfig],
    s3: ResourceParam[S3Resource],
    staged_parquet_s3_path: str,
    config: KubeSparkAssetConfig,
):
    namespace = config.namespace
    k8s_config_file = os.getenv("KUBECONFIG")
    internal_s3_url = s3_config.internal_endpoint_url
    access_key_id = aws_config.access_key_id
    secret_access_key = aws_config.secret_access_key
    internal_rest_catalog_uri = rest_catalog_config.internal_uri
    silver_bucket = s3_config.silver_bucket
    silver_warehouse_name = rest_catalog_config.silver_warehouse_name
    silver_database_name = rest_catalog_config.silver_database_name
    code_bucket = s3_config.code_bucket
    pipes_bucket = pipes_config.bucket

    spark_image = config.spark_image
    spark_version = config.spark_version

    script_s3_key = config.spark_script_s3_key
    main_application_file = f"s3a://{code_bucket}/{script_s3_key}"

    # --- Upload script if it doesn't exist (optional, could be done separately) ---
    script_local_path = os.path.join(os.path.dirname(__file__), "..", "..", "spark", "process_fact_detection.py")
    script_local_path = os.path.abspath(script_local_path)
    if not os.path.exists(script_local_path):
        raise FileNotFoundError(f"Spark script not found at: {script_local_path}")

    s3_client = s3.get_client()
    logger.info(f"Ensuring Spark script exists at {main_application_file}")
    with open(script_local_path, 'rb') as f:
        script_content = f.read()
    s3_client.put_object(Bucket=code_bucket, Key=script_s3_key, Body=script_content)
    logger.info("Spark script uploaded successfully.")

    # --- Initialize Pipes Components ---
    context_injector = PipesS3ContextInjector(
        client=s3_client,
        bucket=pipes_bucket,
    )
    message_reader = PipesS3MessageReader(
        client=s3_client,
        bucket=pipes_bucket,
        include_stdio_in_messages=True, 
    )

    # --- Open Pipes Session ---
    with open_pipes_session(
        context=context,
        context_injector=context_injector,
        message_reader=message_reader,
    ) as session:

        # --- Get Pipes Bootstrap CLI Arguments ---
        pipes_cli_params_dict = session.get_bootstrap_cli_arguments()
        pipes_cli_args_list = []
        for key, value in pipes_cli_params_dict.items():
            pipes_cli_args_list.extend([key, value])

        # --- Define SparkApplication Resource ---
        app_name = f"fact-detection-{context.partition_key}-{context.run_id[:8]}"
        app_name = "".join(c for c in app_name if c.isalnum() or c == '-').lower()[:63]

        # Define environment variables for Spark pods (NO LONGER INCLUDES PIPES VARS)
        spark_pod_env_vars = {
            # Use resource attributes for Spark pods' environment
            "S3_ENDPOINT_URL": internal_s3_url,
            "AWS_ACCESS_KEY_ID": access_key_id,
            "AWS_SECRET_ACCESS_KEY": secret_access_key,
            "S3_SILVER_BUCKET": silver_bucket,
        }

        # --- Construct Arguments List ---
        spark_app_arguments = [
            "--input-dir", staged_parquet_s3_path.replace("s3://", "s3a://", 1),
            "--warehouse-name", silver_warehouse_name,
            "--database-name", silver_database_name,
        ]
        spark_app_arguments.extend(pipes_cli_args_list)


        spark_app_manifest = {
            "apiVersion": f"{SPARK_OP_GROUP}/{SPARK_OP_VERSION}",
            "kind": "SparkApplication",
            "metadata": {
                "name": app_name,
                "namespace": namespace,
                "labels": {
                    "dagster.io/run_id": context.run_id,
                    "dagster.io/asset": "fact_detection_kube",
                    "dagster.io/partition": context.partition_key,
                }
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": spark_image,
                "imagePullPolicy": config.image_pull_policy,
                "mainApplicationFile": main_application_file,
                "arguments": spark_app_arguments,
                "sparkVersion": spark_version,
                "restartPolicy": {
                    "type": "Never",
                },
                "volumes": [
                    {
                        "name": "spark-local-dir-1",
                        "emptyDir": {}
                    }
                ],
                "driver": {
                    "cores": config.driver_cores,
                    "memory": config.driver_memory,
                    "labels": {
                        "version": spark_version,
                        "dagster.io/run_id": context.run_id,
                    },
                    "serviceAccount": config.service_account,
                    # Use deprecated envVars for debugging
                    "envVars": spark_pod_env_vars,
                    "volumeMounts": [
                        {
                            "name": "spark-local-dir-1",
                            "mountPath": "/tmp/spark-local-dir"
                        }
                    ]
                },
                "executor": {
                    "cores": config.executor_cores,
                    "instances": config.min_executors,
                    "memory": config.executor_memory,
                    "labels": {
                        "version": spark_version,
                        "dagster.io/run_id": context.run_id,
                    },
                    "envVars": spark_pod_env_vars,
                    "volumeMounts": [
                        {
                            "name": "spark-local-dir-1",
                            "mountPath": "/tmp/spark-local-dir"
                        }
                    ]
                },
                "dynamicAllocation": {
                    "enabled": config.dynamic_allocation_enabled,
                    "minExecutors": config.min_executors,
                    "maxExecutors": config.max_executors,
                },
                "sparkConf": {
                    "spark.hadoop.fs.s3a.endpoint": internal_s3_url,
                    "spark.hadoop.fs.s3a.access.key": access_key_id,
                    "spark.hadoop.fs.s3a.secret.key": secret_access_key,
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    # Dynamic catalog config
                    f"spark.sql.catalog.{silver_warehouse_name}": "org.apache.iceberg.spark.SparkCatalog",
                    f"spark.sql.catalog.{silver_warehouse_name}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
                    f"spark.sql.catalog.{silver_warehouse_name}.uri": internal_rest_catalog_uri,
                    f"spark.sql.catalog.{silver_warehouse_name}.warehouse": silver_warehouse_name,
                    # Static config
                    "spark.sql.adaptive.enabled": "true",
                    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                    "spark.dynamicAllocation.minExecutors": str(config.min_executors), # Use config value (ensure string)
                    "spark.local.dir": "/tmp/spark-local-dir",
                    "spark.shuffle.service.enabled": "false",
                },
            }
        }

        # --- Submit SparkApplication to Kubernetes (within pipes session) ---
        logger.info(f"Loading Kubernetes configuration (using config file: {k8s_config_file or 'in-cluster or default'})")
        try:
            if k8s_config_file:
                kubernetes.config.load_kube_config(config_file=k8s_config_file)
            else:
                try:
                     kubernetes.config.load_incluster_config()
                     logger.info("Using in-cluster Kubernetes config.")
                except kubernetes.config.ConfigException:
                     logger.info("In-cluster config failed, trying default kubeconfig.")
                     kubernetes.config.load_kube_config()   
        except Exception as e:
            logger.error(f"Could not load Kubernetes config: {e}")
            raise

        k8s_client = kubernetes.client.ApiClient()
        custom_objects_api = kubernetes.client.CustomObjectsApi(k8s_client)

        logger.info(f"Submitting SparkApplication '{app_name}' to namespace '{namespace}'")
        logger.debug(f"Manifest: {yaml.dump(spark_app_manifest)}")
        try:
            custom_objects_api.create_namespaced_custom_object(
                group=SPARK_OP_GROUP,
                version=SPARK_OP_VERSION,
                namespace=namespace,
                plural=SPARK_OP_PLURAL,
                body=spark_app_manifest,
            )
            logger.info(f"SparkApplication '{app_name}' submitted successfully.")
        except kubernetes.client.ApiException as e:
            logger.error(f"Failed to submit SparkApplication: {e.reason} (status: {e.status})")
            logger.error(f"Response body: {e.body}")
            raise Failure(f"Failed to submit SparkApplication {app_name}") from e

        # --- Monitor SparkApplication Status (within pipes session) ---
        monitor_interval_seconds = config.monitor_interval_seconds
        timeout_seconds = config.monitor_timeout_seconds

        try:
            # Use the helper function to monitor
            if _monitor_spark_application(
                custom_objects_api=custom_objects_api,
                namespace=namespace,
                app_name=app_name,
                timeout_seconds=timeout_seconds,
                monitor_interval_seconds=monitor_interval_seconds,
                logger=logger
            ):
                # If monitoring returns True (success), get results from Pipes
                return session.get_results()
            else:
                # Should not happen if the monitor function raises Failure correctly,
                # but handle defensively.
                raise Failure(f"SparkApplication '{app_name}' monitoring finished without explicit success.")
        except Failure as e:
            # Re-raise failures from monitoring or submission
            raise e
        except Exception as e:
            # Catch any other unexpected errors during monitoring
            logger.error(f"Unexpected error during SparkApplication '{app_name}' monitoring phase: {e}")
            raise Failure(f"Unexpected error monitoring SparkApplication {app_name}") from e