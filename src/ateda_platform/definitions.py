# src/ateda_platform/definitions.py
import os
from dagster import Definitions, load_assets_from_modules, DailyPartitionsDefinition, EnvVar
from datetime import datetime

# Import S3 IO Manager
from dagster_aws.s3 import S3PickleIOManager

# Import asset modules from their new locations
from .assets.ingestion import hello_world_asset
from .assets.ingestion import ingestion_assets

# Import resources from their new location
from .resources.s3 import minio_s3_resource

# Import partitions from their dedicated file
from .partitions import daily_partitions

# --- Define IO Manager for Intermediates ---
# Use S3PickleIOManager to store intermediate outputs (like the downloaded tarball)
# in a dedicated S3/MinIO landing zone bucket.
# Following the documentation example: pass the resource instance directly
s3_pickle_io_manager = S3PickleIOManager(
    s3_resource=minio_s3_resource, # Pass the instantiated S3 resource
    # This bucket MUST be defined via the environment variable
    s3_bucket=EnvVar("S3_LANDING_BUCKET"),
    s3_prefix="landing", # Store landing zone files under this prefix
)

# Load assets from all relevant modules
# load_assets_from_modules automatically finds assets defined in the modules
all_assets = load_assets_from_modules([
    hello_world_asset,
    ingestion_assets
])

defs = Definitions(
    assets=all_assets,
    resources={
        "s3": minio_s3_resource, # Resource for final data storage (e.g., bronze bucket)
        "io_manager": s3_pickle_io_manager, # IO Manager for intermediate data
    },
    # Add partitions definition to assets later
    # Add schedules/sensors here later
)

# Optional: Keep the old @repository definition commented out or remove it
# @repository
# def ateda_platform_repo():
#     """
#     Defines the ATEDA platform repository containing assets, jobs, etc.
#     """
#     # Collect all definitions (assets, jobs, schedules, sensors) here
#     return [hello_world_asset]
