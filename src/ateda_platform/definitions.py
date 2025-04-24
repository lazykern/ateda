from dagster import Definitions, EnvVar
from dagster_aws.s3 import S3Resource
from .resources import (
    AWSConfig,
    S3Config,
    RestCatalogConfig,
    PipesConfig,
    SparkOperatorResource,
)

# --- Import Assets ---
from .assets import (
    ingestion,
    transformation,
)
# --- Define Resources ---

# Instantiate configuration resources using EnvVar
aws_config = AWSConfig(
    access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
)
s3_config = S3Config(
    endpoint_scheme=EnvVar("S3_ENDPOINT_SCHEME"),
    endpoint_host=EnvVar("S3_ENDPOINT_HOST"),
    endpoint_port=EnvVar("S3_ENDPOINT_PORT"),
    internal_endpoint_scheme=EnvVar("INTERNAL_S3_ENDPOINT_SCHEME"),
    internal_endpoint_host=EnvVar("INTERNAL_S3_ENDPOINT_HOST"),
    internal_endpoint_port=EnvVar("INTERNAL_S3_ENDPOINT_PORT"),
    bronze_bucket=EnvVar("S3_BRONZE_BUCKET"),
    silver_bucket=EnvVar("S3_SILVER_BUCKET"),
    gold_bucket=EnvVar("S3_GOLD_BUCKET"),
    code_bucket=EnvVar("S3_CODE_BUCKET"),
)
rest_catalog_config = RestCatalogConfig(
    uri=EnvVar("REST_CATALOG_URI"),
    internal_uri=EnvVar("INTERNAL_REST_CATALOG_URI"),
    silver_warehouse_name=EnvVar("SILVER_WAREHOUSE_NAME"),
    silver_database_name=EnvVar("SILVER_DATABASE_NAME"),
)
pipes_config = PipesConfig(bucket=EnvVar("DAGSTER_PIPES_BUCKET"))

# Resolve endpoint URL and credentials at definition time for S3Resource
# Check if EnvVars exist before trying to get value
resolved_endpoint_url = None
scheme = EnvVar("S3_ENDPOINT_SCHEME").get_value(None)
host = EnvVar("S3_ENDPOINT_HOST").get_value(None)
port = EnvVar("S3_ENDPOINT_PORT").get_value(None)
if scheme and host and port:
    resolved_endpoint_url = f"{scheme}://{host}:{port}"

resolved_access_key_id = EnvVar("AWS_ACCESS_KEY_ID").get_value(None)
resolved_secret_access_key = EnvVar("AWS_SECRET_ACCESS_KEY").get_value(None)
resolved_pipes_bucket = EnvVar("DAGSTER_PIPES_BUCKET").get_value(None)

# Define operational resources using the resolved values
s3_resource = S3Resource(
    endpoint_url=resolved_endpoint_url,
    aws_access_key_id=resolved_access_key_id,
    aws_secret_access_key=resolved_secret_access_key,
)

# Configure Pipes Resources with resolved values
# pipes_boto3_s3_client = boto3.client(
#     "s3",
#     endpoint_url=resolved_endpoint_url,
#     aws_access_key_id=resolved_access_key_id,
#     aws_secret_access_key=resolved_secret_access_key,
# ) # This client doesn't seem to be used, commenting out for now

# --- Define Merged Definitions ---

spark_operator_resource = SparkOperatorResource(
    cleanup_on_success=True,
    cleanup_on_failure=False,
)

# Define global resources separately
global_resources = {
    "s3": s3_resource,
    # Config resources
    "aws_config": aws_config,
    "s3_config": s3_config,
    "rest_catalog_config": rest_catalog_config,
    "pipes_config": pipes_config,
    "spark_operator": spark_operator_resource,
}

assets = [
    *ingestion.assets,
    *transformation.assets,
]

defs = Definitions(resources=global_resources, assets=assets)
