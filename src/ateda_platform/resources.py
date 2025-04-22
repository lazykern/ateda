from dagster import ConfigurableResource, EnvVar, InitResourceContext, get_dagster_logger, Failure
import time
import yaml
import kubernetes
from typing import Dict, Optional, List, Any

# Import the SparkApplication model
from .kubernetes.spark_models import (
    SparkApplication,
    SPARK_OP_GROUP,
    SPARK_OP_VERSION,
    SPARK_OP_PLURAL,
)

class AWSConfig(ConfigurableResource):
    """Configuration for AWS credentials."""
    access_key_id: str
    secret_access_key: str

class S3Config(ConfigurableResource):
    """Configuration for S3 URIs and Buckets."""
    # Endpoint Components
    endpoint_scheme: str = "http"
    endpoint_host: str
    endpoint_port: str
    internal_endpoint_scheme: str = "http"
    internal_endpoint_host: str
    internal_endpoint_port: str
    # Buckets
    bronze_bucket: str
    silver_bucket: str
    gold_bucket: str
    code_bucket: str

    @property
    def endpoint_url(self) -> str:
        return f"{self.endpoint_scheme}://{self.endpoint_host}:{self.endpoint_port}"

    @property
    def internal_endpoint_url(self) -> str:
        return f"{self.internal_endpoint_scheme}://{self.internal_endpoint_host}:{self.internal_endpoint_port}"

class RestCatalogConfig(ConfigurableResource):
    """Configuration for Rest Catalog URIs."""
    uri: str
    internal_uri: str
    silver_warehouse_name: str = "silver"
    silver_database_name: str = "ztf"

class PipesConfig(ConfigurableResource):
    """Configuration for Dagster Pipes."""
    bucket: str

# --- New SparkOperatorResource ---
class SparkOperatorResource(ConfigurableResource):
    """
    Resource for interacting with the Kubernetes Spark Operator.
    Handles submission and monitoring of SparkApplication CRDs.
    """
    kubeconfig_path: Optional[str] = EnvVar("KUBECONFIG").get_value()
    monitor_timeout_seconds: int = 1800
    monitor_interval_seconds: int = 15
    cleanup_on_success: bool = False
    cleanup_on_failure: bool = False

    _k8s_client: Optional[kubernetes.client.ApiClient] = None
    _k8s_custom_objects_api: Optional[kubernetes.client.CustomObjectsApi] = None
    _logger: Optional[Any] = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._logger = get_dagster_logger()
        self._load_k8s_config()
        self._k8s_client = kubernetes.client.ApiClient()
        self._k8s_custom_objects_api = kubernetes.client.CustomObjectsApi(self._k8s_client)

    def _load_k8s_config(self):
        """Loads Kubernetes configuration."""
        self._logger.info(f"Loading Kubernetes configuration (using config file: {self.kubeconfig_path or 'in-cluster or default'})")
        try:
            if self.kubeconfig_path:
                kubernetes.config.load_kube_config(config_file=self.kubeconfig_path)
                self._logger.info(f"Loaded K8s config from: {self.kubeconfig_path}")
            else:
                try:
                     kubernetes.config.load_incluster_config()
                     self._logger.info("Using in-cluster Kubernetes config.")
                except kubernetes.config.ConfigException:
                     self._logger.info("In-cluster config failed, trying default kubeconfig.")
                     kubernetes.config.load_kube_config()
                     self._logger.info("Loaded default kubeconfig.")
        except Exception as e:
            self._logger.error(f"Could not load Kubernetes config: {e}")
            raise Failure(f"Could not load Kubernetes configuration: {e}") from e

    def _monitor_spark_application(
        self,
        namespace: str,
        app_name: str,
    ) -> bool:
        """Monitors the SparkApplication status until completion, failure, or timeout."""
        if not self._k8s_custom_objects_api or not self._logger:
             raise Failure("Kubernetes API client or logger not initialized. Call setup_for_execution first.")

        start_time = time.time()
        api = self._k8s_custom_objects_api
        logger = self._logger

        while True:
            if time.time() - start_time > self.monitor_timeout_seconds:
                logger.error(
                    f"Timeout ({self.monitor_timeout_seconds}s) waiting for SparkApplication "
                    f"'{app_name}' in namespace '{namespace}' to complete."
                )
                logger.warning(
                    f"SparkApplication '{app_name}' was not deleted and might need "
                    f"manual inspection or cleanup."
                )
                raise Failure(f"Timeout waiting for SparkApplication '{app_name}'")

            try:
                status_response = api.get_namespaced_custom_object_status(
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
                    # Optionally delete successful app
                    try:
                        if self.cleanup_on_success:
                            logger.info(f"Cleaning up successful SparkApplication '{app_name}'...")
                            api.delete_namespaced_custom_object(
                                group=SPARK_OP_GROUP, version=SPARK_OP_VERSION,
                                namespace=namespace, plural=SPARK_OP_PLURAL, name=app_name
                            )
                            logger.info(f"Successfully deleted SparkApplication '{app_name}'.")
                    except kubernetes.client.ApiException as del_e:
                        logger.warning(f"Failed to delete completed Spark app {app_name}: {del_e}")
                    return True

                elif app_state in ["FAILED", "SUBMISSION_FAILED", "UNKNOWN"]:
                    logger.error(f"SparkApplication '{app_name}' failed with state {app_state}.")
                    if error_message:
                        logger.error(f"Error Message: {error_message}")
                    # Optionally delete failed app
                    try:
                        if self.cleanup_on_failure:
                            logger.info(f"Cleaning up failed SparkApplication '{app_name}'...")
                            api.delete_namespaced_custom_object(
                                group=SPARK_OP_GROUP, version=SPARK_OP_VERSION,
                                namespace=namespace, plural=SPARK_OP_PLURAL, name=app_name
                            )
                            logger.info(f"Successfully deleted failed SparkApplication '{app_name}'.")
                    except kubernetes.client.ApiException as del_e:
                        logger.warning(f"Failed to delete failed Spark app {app_name}: {del_e}")
                    raise Failure(f"SparkApplication '{app_name}' failed. State: {app_state}. Error: {error_message or 'None'}")

            except kubernetes.client.ApiException as e:
                if e.status == 404:
                    if time.time() - start_time < 60:
                        logger.warning(f"SparkApplication '{app_name}' not found yet, likely still creating. Retrying...")
                    else:
                        logger.error(f"SparkApplication '{app_name}' not found after 60 seconds. Assuming creation failed or deleted externally.")
                        raise Failure(f"SparkApplication '{app_name}' disappeared or failed to create.")
                else:
                    logger.error(f"API Error monitoring SparkApplication '{app_name}': {e.reason} (status: {e.status})")
                    raise Failure(f"API Error monitoring SparkApplication {app_name}") from e
            except Exception as e:
                logger.error(f"Unexpected error monitoring SparkApplication '{app_name}': {e}")
                raise Failure(f"Unexpected error monitoring SparkApplication {app_name}") from e

            time.sleep(self.monitor_interval_seconds)


    def run_spark_application(self, spark_application: SparkApplication) -> bool:
        """
        Submits a SparkApplication CRD to Kubernetes and monitors its execution.

        Args:
            spark_application: A fully constructed SparkApplication object.

        Returns:
            True if the application completes successfully.

        Raises:
            Failure: If submission fails, monitoring fails, the job times out, or the job fails.
        """
        if not self._k8s_custom_objects_api or not self._logger:
            raise Failure("Resource not initialized. Ensure setup_for_execution was called.")

        app_name = spark_application.metadata.name
        namespace = spark_application.metadata.namespace
        manifest_dict = spark_application.to_dict()
        api = self._k8s_custom_objects_api
        logger = self._logger

        logger.info(f"Submitting SparkApplication '{app_name}' to namespace '{namespace}'")
        logger.debug(f"Manifest: {yaml.dump(manifest_dict)}")

        try:
            api.create_namespaced_custom_object(
                group=SPARK_OP_GROUP,
                version=SPARK_OP_VERSION,
                namespace=namespace,
                plural=SPARK_OP_PLURAL,
                body=manifest_dict,
            )
            logger.info(f"SparkApplication '{app_name}' submitted successfully.")
        except kubernetes.client.ApiException as e:
            logger.error(f"Failed to submit SparkApplication: {e.reason} (status: {e.status})")
            try:
                error_body = yaml.safe_load(e.body)
                logger.error(f"Response body: {error_body}")
            except:
                 logger.error(f"Raw response body: {e.body}")
            raise Failure(f"Failed to submit SparkApplication {app_name}") from e
        except Exception as e:
             logger.error(f"Unexpected error during SparkApplication submission: {e}")
             raise Failure(f"Unexpected error submitting SparkApplication {app_name}") from e

        job_failed = False
        try:
            # Monitor will raise Failure on error/timeout, or return True on success
            success = self._monitor_spark_application(
                namespace=namespace,
                app_name=app_name,
            )
            return success # Returns True
        except Failure as e:
            job_failed = True # Mark as failed for potential cleanup
            logger.error(f"SparkApplication '{app_name}' monitoring resulted in Failure: {e}")
            raise e # Re-raise the Failure
        except Exception as e:
            job_failed = True # Mark as failed for potential cleanup
            logger.error(f"Unexpected error during SparkApplication '{app_name}' monitoring phase: {e}")
            raise Failure(f"Unexpected error monitoring SparkApplication {app_name}") from e
        finally:
            # Cleanup on failure/timeout happens within _monitor_spark_application
            # But if monitoring itself raised an unexpected Exception, we might need cleanup here
            if job_failed and self.cleanup_on_failure:
                try:
                    # Check if app still exists before trying delete (might have been deleted by timeout logic)
                    api.get_namespaced_custom_object(
                        group=SPARK_OP_GROUP, version=SPARK_OP_VERSION,
                        namespace=namespace, plural=SPARK_OP_PLURAL, name=app_name
                    )
                    logger.info(f"Cleaning up SparkApplication '{app_name}' after unexpected monitoring failure...")
                    api.delete_namespaced_custom_object(
                        group=SPARK_OP_GROUP, version=SPARK_OP_VERSION,
                        namespace=namespace, plural=SPARK_OP_PLURAL, name=app_name
                    )
                    logger.info(f"Successfully deleted SparkApplication '{app_name}'.")
                except kubernetes.client.ApiException as del_e:
                    if del_e.status == 404:
                        logger.info(f"SparkApplication '{app_name}' already gone, no cleanup needed after monitoring failure.")
                    else:
                        logger.warning(f"Failed to delete Spark app {app_name} after monitoring failure: {del_e}")
                except Exception as cleanup_e:
                    logger.warning(f"Unexpected error during final cleanup of Spark app {app_name}: {cleanup_e}") 