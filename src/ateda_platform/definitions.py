# src/ateda_platform/definitions.py
import os
from dagster import Definitions, load_assets_from_modules, EnvVar
from dagster_aws.s3 import S3PickleIOManager, S3Resource

# Import asset modules from their new locations
from .assets.ingestion import hello_world_asset
from .assets.ingestion import ingestion_assets

# Import partitions from their dedicated file
from .partitions import daily_partitions

# --- Define Resources --- 

# Explicitly configure the S3 resource using EnvVar
s3_resource = S3Resource(
    endpoint_url=EnvVar("S3_ENDPOINT_URL"),
    aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
    # region_name can often be omitted for MinIO unless specifically required
)

# --- Define IO Manager --- 

s3_pickle_io_manager = S3PickleIOManager(
    s3_resource=s3_resource,
    s3_bucket=EnvVar("S3_LANDING_BUCKET"),
    s3_prefix="landing",
)

# --- Load Assets --- 

all_assets = load_assets_from_modules([
    hello_world_asset,
    ingestion_assets
])

# --- Define Definitions --- 

defs = Definitions(
    assets=all_assets,
    resources={
        "s3": s3_resource,
        "io_manager": s3_pickle_io_manager,
    },
)

# Removed commented out old repository definition
