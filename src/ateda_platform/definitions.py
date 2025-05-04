from dagster import Definitions, EnvVar
from dagster_aws.s3 import S3Resource
from .resources import (
    AWSConfig,
    S3Config,
    RestCatalogConfig,
    PipesConfig,
)

# --- Import Assets ---
from .assets import (
    ingestion,
)
# --- Define Resources ---

# Instantiate configuration resources using EnvVar
aws_config = AWSConfig(
    access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
)
s3_config = S3Config(
    endpoint_url=EnvVar("S3_ENDPOINT_URL"),
    internal_endpoint_url=EnvVar("INTERNAL_S3_ENDPOINT_URL"),
    landing_bucket=EnvVar("S3_LANDING_BUCKET"),
    cutout_bucket=EnvVar("S3_CUTOUT_BUCKET"),
    warehouse_bucket=EnvVar("S3_WAREHOUSE_BUCKET"),
)
rest_catalog_config = RestCatalogConfig(
    uri=EnvVar("REST_CATALOG_URI"),
    internal_uri=EnvVar("INTERNAL_REST_CATALOG_URI"),
    warehouse_name=EnvVar("WAREHOUSE_NAME"),
)
pipes_config = PipesConfig(bucket=EnvVar("DAGSTER_PIPES_BUCKET"))

resolved_access_key_id = EnvVar("AWS_ACCESS_KEY_ID").get_value(None)
resolved_secret_access_key = EnvVar("AWS_SECRET_ACCESS_KEY").get_value(None)
resolved_pipes_bucket = EnvVar("DAGSTER_PIPES_BUCKET").get_value(None)

# Define operational resources using the resolved values
s3_resource = S3Resource(
    endpoint_url=s3_config.endpoint_url,
    aws_access_key_id=resolved_access_key_id,
    aws_secret_access_key=resolved_secret_access_key,
)


# Define global resources separately
global_resources = {
    "s3": s3_resource,
    # Config resources
    "aws_config": aws_config,
    "s3_config": s3_config,
    "rest_catalog_config": rest_catalog_config,
    "pipes_config": pipes_config,
}

assets = [
    *ingestion.assets,
]

defs = Definitions(resources=global_resources, assets=assets)
