# src/ateda_platform/resources/s3.py
import os
from dagster_aws.s3 import S3Resource

# Configure the S3 resource for MinIO/S3
minio_s3_resource = S3Resource(
    # Use the endpoint URL from the environment variable
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    # boto3/s3 will automatically pick up AWS_ACCESS_KEY_ID
    # and AWS_SECRET_ACCESS_KEY from the environment
) 