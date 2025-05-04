from dagster import (
    ConfigurableResource,
)



class AWSConfig(ConfigurableResource):
    """Configuration for AWS credentials."""

    access_key_id: str
    secret_access_key: str


class S3Config(ConfigurableResource):
    """Configuration for S3 URIs and Buckets."""

    # Endpoint Components
    endpoint_url: str
    internal_endpoint_url: str
    # Buckets
    landing_bucket: str
    cutout_bucket: str
    warehouse_bucket: str


class RestCatalogConfig(ConfigurableResource):
    """Configuration for Rest Catalog URIs."""

    uri: str
    internal_uri: str
    warehouse_name: str


class PipesConfig(ConfigurableResource):
    """Configuration for Dagster Pipes."""

    bucket: str