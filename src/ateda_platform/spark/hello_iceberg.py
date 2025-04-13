# src/ateda_platform/spark/hello_iceberg.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import boto3

from dagster_pipes import (
    PipesCliArgsParamsLoader,
    PipesS3ContextLoader,
    PipesS3MessageWriter,
    open_dagster_pipes,
)

def main():
    # Explicitly get endpoint for the Pipes client
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
    if not s3_endpoint_url:
        print("FATAL: Missing required environment variable S3_ENDPOINT_URL for Dagster Pipes S3 communication.")
        raise ValueError("Missing required environment variable S3_ENDPOINT_URL")

    # Initialize S3 client needed for Pipes components
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint_url
        )
    except Exception as e:
        print(f"FATAL: Failed to initialize S3 client for Dagster Pipes: {e}")
        raise

    # Use S3 writer, S3 context loader, and CLI params loader
    with open_dagster_pipes(
        message_writer=PipesS3MessageWriter(client=s3_client),
        context_loader=PipesS3ContextLoader(client=s3_client),
        params_loader=PipesCliArgsParamsLoader(),
    ) as pipes:
        pipes.log.info("Starting hello_iceberg Spark job via Dagster Pipes (S3 comms).")

        # Get configuration from pipes extras
        table = pipes.get_extra("table")
        database = pipes.get_extra("database")
        bucket = pipes.get_extra("bucket")
        
        # --- Check Required Extras ---
        required_extras = ["table", "database", "bucket"]
        missing = [k for k in required_extras if not pipes.get_extra(k)]
        if missing:
            error_msg = f"Missing required extras from Pipes: {missing}"
            pipes.log.error(error_msg)
            raise ValueError(error_msg)

        # Construct warehouse path and log config info
        warehouse_path = f"s3a://{bucket}/"
        pipes.log.info(f"Using Iceberg warehouse path: {warehouse_path}")

        # --- Simplified Spark Session Acquisition ---
        pipes.log.info("Getting existing SparkSession (configured by spark-submit)...")
        spark = SparkSession.builder.appName("HelloIcebergNessiePipesS3").getOrCreate()
        pipes.log.info("SparkSession obtained successfully.")

        # Log effective catalog configuration for verification
        try:
            spark_catalog_impl = spark.conf.get("spark.sql.catalog.nessie.catalog-impl")
            spark_catalog_uri = spark.conf.get("spark.sql.catalog.nessie.uri")
            spark_catalog_wh = spark.conf.get("spark.sql.catalog.nessie.warehouse")
            spark_catalog_ref = spark.conf.get("spark.sql.catalog.nessie.ref")
            pipes.log.info(
                f"Verified Spark Conf: Catalog 'nessie' "
                f"impl='{spark_catalog_impl}', "
                f"uri='{spark_catalog_uri}', "
                f"warehouse='{spark_catalog_wh}', "
                f"ref='{spark_catalog_ref}'"
            )
        except Exception as e:
            pipes.log.warning(f"Could not verify all Spark catalog configurations: {e}")

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("message", StringType(), True)
        ])
        data = [(1, "Hello"), (2, "Iceberg")]
        df = spark.createDataFrame(data, schema=schema)
        pipes.log.info("Sample DataFrame created.")

        # --- Write to Iceberg Table using Nessie Catalog ---
        # First ensure the database exists
        pipes.log.info(f"Creating database nessie.{database} if not exists...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS nessie.{database}")
        
        # Define table name using catalog, database and table name from config
        qualified_table_name = f"nessie.{database}.{table}"
        pipes.log.info(f"Writing DataFrame to Nessie Iceberg table: {qualified_table_name}")
        
        # Write using Iceberg format and table name
        (df.write
            .format("iceberg")
            .mode("overwrite")
            .saveAsTable(qualified_table_name))
        
        pipes.log.info(f"Successfully wrote to Nessie Iceberg table: {qualified_table_name}")

        # --- Report Asset Materialization to Dagster ---
        num_rows = df.count()
        pipes.report_asset_materialization(
            asset_key="hello_iceberg_table",
            metadata={
                "num_rows": num_rows,
                "warehouse_path": warehouse_path,
                "database": database,
                "table": table,
                "spark_app_name": spark.sparkContext.appName,
                "spark_version": spark.version,
            }
        )
        pipes.log.info("Asset materialization reported to Dagster.")

        # --- Clean Up ---
        spark.stop()

if __name__ == "__main__":
    main() 