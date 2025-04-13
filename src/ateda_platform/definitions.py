# src/ateda_platform/definitions.py
import os
# import boto3 # No longer needed for Pipes client setup
from dagster import Definitions, load_assets_from_modules, EnvVar, define_asset_job, AssetSelection, ScheduleDefinition
from dagster_aws.s3 import S3PickleIOManager, S3Resource
import boto3
from dagster_aws.pipes import PipesS3ContextInjector, PipesS3MessageReader
# Import asset modules from their new locations
from .assets.ingestion import hello_world_asset
from .assets.ingestion import ingestion_assets
# Import the new processing assets module
from .assets.processing import spark_processing_assets


from .resources.nessie import NessieResource

# --- Define Resources --- 

s3_client = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

# Explicitly configure the S3 resource using EnvVar
s3_resource = S3Resource(
    endpoint_url=EnvVar("S3_ENDPOINT_URL"),
    aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
)

# Define S3 Pipes components as resources
pipes_s3_context_injector = PipesS3ContextInjector(
    client=s3_client,
    bucket=os.getenv("DAGSTER_PIPES_BUCKET"),
)

pipes_s3_message_reader = PipesS3MessageReader(
    client=s3_client,
    bucket=os.getenv("DAGSTER_PIPES_BUCKET"),
    include_stdio_in_messages=True,
)

# --- Define IO Manager --- 

s3_pickle_io_manager = S3PickleIOManager(
    s3_resource=s3_resource,
    s3_bucket=EnvVar("S3_LANDING_BUCKET"),
)

# --- Load Assets --- 

all_assets = load_assets_from_modules([
    hello_world_asset,
    ingestion_assets,
    spark_processing_assets # Add the new module
])

# --- Define Jobs (Optional but useful for Presets/Schedules) ---

# A job that targets only the hello_iceberg_table asset
hello_iceberg_job = define_asset_job(
    name="hello_iceberg_job",
    selection=AssetSelection.keys("hello_iceberg_table")
)

# --- Define Schedules (For automated runs) ---

# Example: Run the hello_iceberg_job daily with specific config
daily_hello_iceberg_schedule = ScheduleDefinition(
    job=hello_iceberg_job,
    cron_schedule="0 0 * * *",  # Run daily at midnight UTC
    run_config={
        "ops": {
            "hello_iceberg_table": {
                "config": {
                    "table": "scheduled_hello_table",
                    "database": "scheduled_db"
                }
            }
        }
    }
)

# --- Define Definitions --- 

defs = Definitions(
    assets=all_assets,
    resources={
        "s3": s3_resource,
        "io_manager": s3_pickle_io_manager,
        "pipes_s3_context_injector": pipes_s3_context_injector,
        "pipes_s3_message_reader": pipes_s3_message_reader,
        "nessie": NessieResource(
            uri=EnvVar("NESSIE_URI"),
        )
    },
    jobs=[hello_iceberg_job], # Include the job
    schedules=[daily_hello_iceberg_schedule], # Add the schedule
    # Add asset checks defined in the assets
    # Note: AssetCheckSpecs defined directly on assets are automatically collected,
    # so explicitly listing them here is not strictly necessary unless defining them separately.
    # asset_checks=[spark_processing_assets.silver_ztf_alerts_non_empty_check]
)

# Removed commented out old repository definition
