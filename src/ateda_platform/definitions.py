from dagster import Definitions, load_assets_from_modules, EnvVar
from dagster_aws.s3 import S3Resource
import boto3
from .resources import (
    AWSConfig,
    S3Config,
    RestCatalogConfig,
    PipesConfig,
)

# Load assets
from .assets.ingestion import assets as ingestion_assets
from .assets.transformation import combine, detection

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
resolved_endpoint_url = f"{EnvVar('S3_ENDPOINT_SCHEME').get_value()}://{EnvVar('S3_ENDPOINT_HOST').get_value()}:{EnvVar('S3_ENDPOINT_PORT').get_value()}"
resolved_access_key_id = EnvVar("AWS_ACCESS_KEY_ID").get_value()
resolved_secret_access_key = EnvVar("AWS_SECRET_ACCESS_KEY").get_value()
resolved_pipes_bucket = EnvVar("DAGSTER_PIPES_BUCKET").get_value()

# Define operational resources using the resolved values
s3_resource = S3Resource(
    endpoint_url=resolved_endpoint_url,
    aws_access_key_id=resolved_access_key_id,
    aws_secret_access_key=resolved_secret_access_key,
)

# Configure Pipes Resources with resolved values
pipes_boto3_s3_client = boto3.client(
    "s3",
    endpoint_url=resolved_endpoint_url,
    aws_access_key_id=resolved_access_key_id,
    aws_secret_access_key=resolved_secret_access_key,
)

# --- Load Assets ---

all_assets = load_assets_from_modules(
    [
        ingestion_assets,
        combine,
        detection,
    ]
)


# --- Define Definitions ---

defs = Definitions(
    assets=all_assets,
    resources={
        "s3": s3_resource,
        # Config resources
        "aws_config": aws_config,
        "s3_config": s3_config,
        "rest_catalog_config": rest_catalog_config,
        "pipes_config": pipes_config,
    },
)
