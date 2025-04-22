import os
import time
import yaml
import kubernetes
from typing import Dict, List, Optional
from pathlib import Path

from dagster import (
    Config,
    get_dagster_logger,
    Failure,
    ResourceParam,
    AssetExecutionContext,
)
from dagster_aws.s3 import S3Resource
from pydantic import Field as PydanticField

# Import necessary resources/configs likely needed by the helper
from ...resources import AWSConfig, S3Config # Assuming these are needed for creds/endpoints
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
    SPARK_OP_GROUP,
    SPARK_OP_VERSION
)

SPARK_OP_PLURAL = "sparkapplications"
logger = get_dagster_logger() # Use logger within the helper

# --- Shared Configuration Model --- #
class KubeSparkAssetConfig(Config):
    """Configuration for Kubernetes Spark assets."""
    namespace: str = PydanticField(
        default_factory=lambda: os.getenv("K8S_NAMESPACE", "ateda-dev"),
        description=(
            "Kubernetes namespace for Spark job. "
            "Defaults to K8S_NAMESPACE env var or 'ateda-dev'."
        )
    )
    spark_image: str = PydanticField(
        default="ghcr.io/lazykern/ateda-spark-runtime:latest",
        description="Docker image for Spark driver and executors."
    )
    spark_version: str = PydanticField(
        default="3.5.5",
        description="Spark version used in the image."
    )
    # Spark script paths moved to specific assets, but keep shared resource configs
    service_account: str = PydanticField(
        default="spark-operator-spark",
        description="Kubernetes service account for the Spark driver pod."
    )
    image_pull_policy: str = PydanticField(
        default="IfNotPresent",
        description="Kubernetes image pull policy (e.g., Always, Never, IfNotPresent)."
    )
    driver_cores: int = PydanticField(
        default=1,
        description="Number of cores for the Spark driver."
    )
    driver_memory: str = PydanticField(
        default="1g",
        description="Memory for the Spark driver (e.g., '1g', '2048m')."
    )
    executor_cores: int = PydanticField(
        default=1,
        description="Number of cores per Spark executor."
    )
    executor_memory: str = PydanticField(
        default="1536m",
        description="Memory per Spark executor (e.g., '1g', '2048m')."
    )
    dynamic_allocation_enabled: bool = PydanticField(
        default=True,
        description="Enable Spark dynamic allocation."
    )
    min_executors: int = PydanticField(
        default=1,
        description="Minimum number of executors for dynamic allocation."
    )
    max_executors: int = PydanticField(
        default=3,
        description="Maximum number of executors for dynamic allocation."
    )
    monitor_timeout_seconds: int = PydanticField(
        default=1800,
        description="Timeout in seconds for waiting for Spark job completion."
    )
    monitor_interval_seconds: int = PydanticField(
        default=15,
        description="Polling interval in seconds for checking Spark job status."
    )
    # Removed combine_script_s3_key and spark_script_s3_key as they are specific

# --- Spark Application Monitoring --- # (Moved from assets.py)
def _monitor_spark_application(
    custom_objects_api: kubernetes.client.CustomObjectsApi,
    namespace: str,
    app_name: str,
    timeout_seconds: int,
    monitor_interval_seconds: int,
    logger, # Pass logger instance
) -> bool:
    """Monitors the SparkApplication status until completion, failure, or timeout."""
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout_seconds:
            logger.error(
                f"Timeout ({timeout_seconds}s) waiting for SparkApplication "
                f"'{app_name}' in namespace '{namespace}' to complete."
            )
            logger.warning(
                f"SparkApplication '{app_name}' was not deleted and might need "
                f"manual inspection or cleanup."
            )
            # Consider deleting the failed app here?
            # try:
            #     custom_objects_api.delete_namespaced_custom_object(...)
            # except ApiException as del_e:
            #     logger.warning(f"Failed to delete timed-out Spark app {app_name}: {del_e})")
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
                # Optionally delete successful apps
                # try:
                #     custom_objects_api.delete_namespaced_custom_object(...)
                # except ApiException as del_e:
                #     logger.warning(f"Failed to delete completed Spark app {app_name}: {del_e})")
                return True

            elif app_state in ["FAILED", "SUBMISSION_FAILED", "UNKNOWN"]:
                logger.error(f"SparkApplication '{app_name}' failed with state {app_state}.")
                if error_message:
                    logger.error(f"Error Message: {error_message}")
                # Optionally delete failed apps
                # try:
                #     custom_objects_api.delete_namespaced_custom_object(...)
                # except ApiException as del_e:
                #     logger.warning(f"Failed to delete failed Spark app {app_name}: {del_e})")
                raise Failure(f"SparkApplication '{app_name}' failed. State: {app_state}. Error: {error_message or 'None'}")

        except kubernetes.client.ApiException as e:
            if e.status == 404:
                # If it's not found shortly after submission, it might still be creating.
                # If it persists, the app might have been deleted externally or never created properly.
                if time.time() - start_time < 60: # Check for 60 seconds
                    logger.warning(f"SparkApplication '{app_name}' not found yet, likely still creating. Retrying...")
                else:
                    logger.error(f"SparkApplication '{app_name}' not found after 60 seconds. Assuming creation failed or deleted externally.")
                    raise Failure(f"SparkApplication '{app_name}' disappeared or failed to create.")
            else:
                logger.error(f"API Error monitoring SparkApplication '{app_name}': {e.reason}")
                # Decide if this is retryable or fatal
                # raise Failure(f"API Error monitoring SparkApplication {app_name}") from e
        except Exception as e:
            logger.error(f"Unexpected error monitoring SparkApplication '{app_name}': {e}")
            raise Failure(f"Unexpected error monitoring SparkApplication {app_name}") from e

        time.sleep(monitor_interval_seconds)

# --- Helper Function to Run Spark Job --- #
def run_spark_on_kubernetes(
    context: AssetExecutionContext,
    k8s_config: KubeSparkAssetConfig,
    aws_config: AWSConfig,
    s3_config: S3Config,
    s3_resource: S3Resource, # Pass the resource for script check
    app_name_prefix: str,
    spark_script_s3_key: str,
    spark_script_args: List[str],
    asset_key_label: str, # For labeling the K8s object
    extra_spark_conf: Optional[Dict[str, str]] = None,
    extra_env_vars: Optional[Dict[str, str]] = None,
    use_pipes: bool = False,
    pipes_bootstrap_args: Optional[List[str]] = None,
    # Optional resource overrides
    executor_memory_override: Optional[str] = None,
    executor_cores_override: Optional[int] = None,
    driver_memory_override: Optional[str] = None,
    driver_cores_override: Optional[int] = None,
) -> bool:
    """Builds, submits, and monitors a SparkApplication on Kubernetes."""
    namespace = k8s_config.namespace
    k8s_config_file = os.getenv("KUBECONFIG")
    internal_s3_url = s3_config.internal_endpoint_url
    access_key_id = aws_config.access_key_id
    secret_access_key = aws_config.secret_access_key
    code_bucket = s3_config.code_bucket

    # --- Basic Setup --- #
    container_shared_mount_path = Path("/mnt/ateda/shared") # Standard mount path
    spark_image = k8s_config.spark_image
    spark_version = k8s_config.spark_version

    main_application_file = f"s3a://{code_bucket}/{spark_script_s3_key}"

    # --- Check if Spark script exists in S3 --- #
    s3_client = s3_resource.get_client()
    try:
        s3_client.head_object(Bucket=code_bucket, Key=spark_script_s3_key)
        logger.info(f"Confirmed Spark script exists at s3://{code_bucket}/{spark_script_s3_key}")
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.error(f"Spark script not found at s3://{code_bucket}/{spark_script_s3_key}")
            raise FileNotFoundError(f"Spark script {spark_script_s3_key} not found in bucket {code_bucket}. Please ensure it is uploaded.") from e
        else:
            logger.error(f"Error checking Spark script existence: {e}")
            raise

    # --- Define SparkApplication Name --- #
    app_name = f"{app_name_prefix}-{context.partition_key or 'unpartitioned'}-{context.run_id[:8]}"
    app_name = "".join(c for c in app_name if c.isalnum() or c == '-').lower()[:63]

    # --- Define common elements --- #
    common_labels = {
        "dagster.io/run_id": context.run_id,
        "dagster.io/asset": asset_key_label,
        "dagster.io/partition": context.partition_key or "default",
        "version": spark_version,
    }

    # Base ENV vars (can be extended)
    spark_pod_env_vars = {
        "S3_ENDPOINT_URL": internal_s3_url,
        "AWS_ACCESS_KEY_ID": access_key_id,
        "AWS_SECRET_ACCESS_KEY": secret_access_key,
    }
    if extra_env_vars:
        spark_pod_env_vars.update(extra_env_vars)

    common_volume_mounts = [
        VolumeMount(name="spark-local-dir-1", mountPath="/tmp/spark-local-dir"),
        VolumeMount(name="shared-volume", mountPath=str(container_shared_mount_path))
    ]

    # --- Arguments --- #
    final_spark_script_args = list(spark_script_args) # Copy
    if use_pipes:
        if not pipes_bootstrap_args:
            raise ValueError("pipes_bootstrap_args must be provided when use_pipes is True")
        final_spark_script_args.extend(pipes_bootstrap_args)

    # --- Base SparkConf --- #
    base_spark_conf = {
        "spark.hadoop.fs.s3a.endpoint": internal_s3_url,
        "spark.hadoop.fs.s3a.access.key": access_key_id,
        "spark.hadoop.fs.s3a.secret.key": secret_access_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.connection.maximum": "100", # Default, can be overridden
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false", # Assuming MinIO uses HTTP

        # --- Default S3A Committer Settings ---
        # These are suitable defaults for writing partitioned data to S3-compatible storage
        # Individual jobs can override spark.hadoop.fs.s3a.committer.staging.conflict-mode based on write mode
        # Using Magic Committer for potentially better performance and avoiding local staging dependency
        "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
        "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
        "spark.sql.sources.commitProtocolClass": "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
        # Use BindingParquetOutputCommitter when magic committer is enabled
        "spark.sql.parquet.output.committer.class": "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
        "spark.hadoop.fs.s3a.committer.name": "magic",

        "spark.driver.memoryOverhead": "512m", # Default, can be overridden
        "spark.executor.memoryOverhead": "384m", # Default, can be overridden
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "256m", # Default, can be overridden
        "spark.local.dir": "/tmp/spark-local-dir",
        "spark.shuffle.service.enabled": "false",

        "spark.dynamicAllocation.enabled": str(k8s_config.dynamic_allocation_enabled).lower(),
        "spark.dynamicAllocation.minExecutors": str(k8s_config.min_executors),
        "spark.dynamicAllocation.maxExecutors": str(k8s_config.max_executors),
        "spark.sql.adaptive.enabled": "true", # Generally recommended
        "spark.sql.files.maxPartitionBytes": "1024m", # Good default for reading
    }
    # Merge extra SparkConf provided by the asset
    if extra_spark_conf:
        base_spark_conf.update(extra_spark_conf)

    # --- Build SparkApplication Manifest --- #
    try:
        manifest = SparkApplication(
            metadata=ObjectMeta(
                name=app_name,
                namespace=k8s_config.namespace,
                labels=common_labels
            ),
            spec=SparkApplicationSpec(
                type="Python",
                pythonVersion="3",
                mode="cluster",
                image=k8s_config.spark_image,
                imagePullPolicy=k8s_config.image_pull_policy,
                mainApplicationFile=main_application_file,
                arguments=final_spark_script_args,
                sparkVersion=k8s_config.spark_version,
                restartPolicy=RestartPolicy(type="Never"),
                volumes=[
                    Volume(name="spark-local-dir-1", emptyDir=EmptyDirVolumeSource()),
                    Volume(name="shared-volume", hostPath=HostPathVolumeSource(path=str(container_shared_mount_path), type="DirectoryOrCreate"))
                ],
                driver=DriverSpec(
                    cores=driver_cores_override if driver_cores_override is not None else k8s_config.driver_cores,
                    memory=driver_memory_override if driver_memory_override is not None else k8s_config.driver_memory,
                    labels=common_labels,
                    serviceAccount=k8s_config.service_account,
                    envVars=spark_pod_env_vars,
                    volumeMounts=common_volume_mounts,
                ),
                executor=ExecutorSpec(
                    cores=executor_cores_override if executor_cores_override is not None else k8s_config.executor_cores,
                    instances=k8s_config.min_executors,
                    memory=executor_memory_override if executor_memory_override is not None else k8s_config.executor_memory,
                    labels=common_labels,
                    envVars=spark_pod_env_vars,
                    volumeMounts=common_volume_mounts,
                ),
                dynamicAllocation=DynamicAllocation(
                    enabled=k8s_config.dynamic_allocation_enabled,
                    minExecutors=k8s_config.min_executors,
                    maxExecutors=k8s_config.max_executors,
                ),
                sparkConf=base_spark_conf, # Use the merged config
            )
        )
        spark_app_manifest_dict = manifest.to_dict()
    except Exception as e:
        logger.error(f"Error constructing SparkApplication manifest for job '{app_name}': {e}")
        raise Failure(f"Failed to construct SparkApplication manifest for {app_name}") from e

    # --- Load K8s Config --- #
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

    # --- Submit and Monitor Spark Application --- #
    k8s_client = kubernetes.client.ApiClient()
    custom_objects_api = kubernetes.client.CustomObjectsApi(k8s_client)

    logger.info(f"Submitting SparkApplication '{app_name}' to namespace '{namespace}'")
    logger.debug(f"Manifest: {yaml.dump(spark_app_manifest_dict)}")
    try:
        # Add cleanup for existing app before creating? Optional.
        # try: 
        #     custom_objects_api.delete_namespaced_custom_object(..., name=app_name)
        # except ApiException as del_e:
        #     if del_e.status != 404:
        #         logger.warning(f"Could not delete pre-existing app {app_name}: {del_e}")

        custom_objects_api.create_namespaced_custom_object(
            group=SPARK_OP_GROUP,
            version=SPARK_OP_VERSION,
            namespace=namespace,
            plural=SPARK_OP_PLURAL,
            body=spark_app_manifest_dict,
        )
        logger.info(f"SparkApplication '{app_name}' submitted successfully.")
    except kubernetes.client.ApiException as e:
        logger.error(f"Failed to submit SparkApplication: {e.reason} (status: {e.status})")
        logger.error(f"Response body: {e.body}")
        raise Failure(f"Failed to submit SparkApplication {app_name}") from e

    # Monitor using the dedicated function
    try:
        success = _monitor_spark_application(
            custom_objects_api=custom_objects_api,
            namespace=namespace,
            app_name=app_name,
            timeout_seconds=k8s_config.monitor_timeout_seconds,
            monitor_interval_seconds=k8s_config.monitor_interval_seconds,
            logger=logger
        )
        return success # Returns True if completed successfully
    except Failure as e:
        # Failure already logged by monitor function
        raise e # Re-raise the specific Failure
    except Exception as e:
        # Catch any other unexpected errors during monitoring
        logger.error(f"Unexpected error during SparkApplication '{app_name}' monitoring phase: {e}")
        raise Failure(f"Unexpected error monitoring SparkApplication {app_name}") from e
    # No finally block needed here, monitor handles cleanup/logging on failure/timeout 