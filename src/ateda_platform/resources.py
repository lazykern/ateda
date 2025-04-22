from dagster import ConfigurableResource, EnvVar

class AWSConfig(ConfigurableResource):
    """Configuration for AWS credentials."""
    access_key_id: str
    secret_access_key: str

class S3Config(ConfigurableResource):
    """Configuration for S3 URIs and Buckets."""
    # Endpoint Components
    endpoint_scheme: str
    endpoint_host: str
    endpoint_port: str
    internal_endpoint_scheme: str
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
    # Keeping these as full URIs for now as per .env
    # If they also need splitting, let me know.
    uri: str
    internal_uri: str
    silver_warehouse_name: str
    silver_database_name: str

class PipesConfig(ConfigurableResource):
    """Configuration for Dagster Pipes."""
    bucket: str 