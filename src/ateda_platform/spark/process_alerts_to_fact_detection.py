# src/ateda_platform/spark/process_alerts_to_fact_detection.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, FloatType, BooleanType
import boto3

from dagster_pipes import (
    PipesCliArgsParamsLoader,
    PipesS3ContextLoader,
    PipesS3MessageWriter,
    open_dagster_pipes,
)

# Define the expected schema for the input candidate data (subset of Avro)
# Include fields needed for transformation and target table
CANDIDATE_SCHEMA = StructType([
    StructField("candid", LongType(), True),
    StructField("jd", DoubleType(), True),
    StructField("fid", IntegerType(), True),
    StructField("pid", LongType(), True),
    StructField("diffmaglim", FloatType(), True),
    StructField("isdiffpos", StringType(), True), # Read as string first
    StructField("programid", IntegerType(), True),
    StructField("ra", DoubleType(), True),
    StructField("dec", DoubleType(), True),
    StructField("magpsf", FloatType(), True),
    StructField("sigmapsf", FloatType(), True),
    StructField("distnr", FloatType(), True),
    StructField("magnr", FloatType(), True),
    StructField("sigmagnr", FloatType(), True),
    StructField("fwhm", FloatType(), True),
    StructField("classtar", FloatType(), True),
    # Add other fields if needed later
])

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
        pipes.log.info("Starting Spark job: Process Bronze ZTF Alerts to Silver Fact Detection table.")

        # Get configuration from pipes extras
        bronze_partition_path = pipes.get_extra("bronze_partition_path") # e.g., s3a://ateda-bronze/ztf_alerts/year=2023/month=10/day=26
        target_database = pipes.get_extra("database")
        target_table = pipes.get_extra("table")
        
        required_extras = ["bronze_partition_path", "database", "table"] # Check for the specific partition path
        missing = [k for k in required_extras if not pipes.get_extra(k)]
        if missing:
            error_msg = f"Missing required extras from Pipes: {missing}"
            pipes.log.error(error_msg)
            raise ValueError(error_msg)
            
        pipes.log.info(f"Reading Avro data from specific partition path: {bronze_partition_path}") # Log the specific path
        pipes.log.info(f"Target Nessie Catalog Table: nessie.{target_database}.{target_table}")

        spark = SparkSession.builder.appName(f"ZTFAlertsToFactDetection-{target_database}-{target_table}").getOrCreate()
        pipes.log.info("SparkSession obtained successfully.")

        # Ensure the target namespace exists in Nessie
        try:
            create_namespace_sql = f"CREATE NAMESPACE IF NOT EXISTS nessie.{target_database}"
            pipes.log.info(f"Executing: {create_namespace_sql}")
            spark.sql(create_namespace_sql)
            pipes.log.info(f"Namespace nessie.{target_database} ensured.")
        except Exception as e:
            pipes.log.error(f"Failed to create or ensure namespace nessie.{target_database}: {e}")
            raise

        # --- Read Bronze Avro Data ---
        # Use pathGlobFilter for efficient partition reading if needed, 
        # or rely on Spark's partition discovery.
        # Specify Avro format and schema
        try:
            # Use the Spark Avro package
            # Load directly from the specific partition path provided
            alerts_df = spark.read.format("avro").load(f"{bronze_partition_path}/*") # Add /* to read files within the partition dir
            pipes.log.info("Successfully read Avro data from Bronze layer partition.")
            alerts_df.printSchema()
        except Exception as e:
            pipes.log.error(f"Failed to read Avro data from {bronze_partition_path}: {e}")
            # Potentially log specific Spark error messages if available
            raise

        # --- Extract Candidate and Prv_Candidate Data ---
        # Select necessary fields, including the objectId for FK relation
        candidates = alerts_df.select(
            F.col("objectId"),
            F.col("candidate.*")
        ).select(
            F.col("objectId"),
            *[F.col(field.name).alias(field.name) for field in CANDIDATE_SCHEMA]
        )
        
        # Handle prv_candidates: explode array, filter out non-detections (candid is null)
        # Ensure prv_candidate schema matches CANDIDATE_SCHEMA structure
        prv_candidates_exploded = alerts_df.select(
            F.col("objectId"),
            F.explode_outer("prv_candidates").alias("prv_cand") # Use explode_outer if prv_candidates can be null/empty
        )

        prv_candidates_filtered = prv_candidates_exploded.filter(F.col("prv_cand.candid").isNotNull()) \
                                                    .select(
                                                        F.col("objectId"),
                                                        F.col("prv_cand.*")
                                                    )
                
        # Select only the columns matching CANDIDATE_SCHEMA from prv_candidates
        # Required because prv_candidates has fewer fields than candidate
        prv_candidates_cols_present = [field.name for field in CANDIDATE_SCHEMA if field.name in prv_candidates_filtered.columns]
        prv_candidates = prv_candidates_filtered.select(
             F.col("objectId"),
             *[F.col(field_name).alias(field_name) for field_name in prv_candidates_cols_present]
        )

        # --- Union and Deduplicate ---
        all_detections = candidates.unionByName(prv_candidates, allowMissingColumns=True)
        
        # Deduplicate based on 'candid', keep the first instance (arbitrary)
        # Consider window function if specific record (e.g., latest alert) is preferred during duplicates
        unique_detections = all_detections.dropDuplicates(["candid"])
        
        # --- Transformations ---
        # 1. Convert isdiffpos ('t'/'f'/'1'/'0') to Boolean
        transformed_detections = unique_detections.withColumn(
            "isdiffpos_bool",
            F.when(F.lower(F.col("isdiffpos")) == "t", True)
             .when(F.col("isdiffpos") == "1", True)
             .otherwise(False) # Assumes anything else (f, 0, null) is false
        ).drop("isdiffpos").withColumnRenamed("isdiffpos_bool", "isdiffpos")

        # 2. Derive timestamp and date parts from jd using Spark SQL functions
        # Convert JD to Unix timestamp (seconds since epoch), then to TimestampType
        transformed_detections = transformed_detections.withColumn(
            "timestamp_from_jd", 
            (F.expr("(jd - 2440587.5) * 86400.0")).cast("timestamp") 
        )
        
        # Filter out rows where jd was null or conversion failed
        transformed_detections = transformed_detections.filter(F.col("timestamp_from_jd").isNotNull())
        
        # Extract year, month, day from the derived timestamp
        transformed_detections = transformed_detections.select(
            "*",
            F.year(F.col("timestamp_from_jd")).alias("year"),
            F.month(F.col("timestamp_from_jd")).alias("month"),
            F.dayofmonth(F.col("timestamp_from_jd")).alias("day")
        ) # Keep timestamp_from_jd if needed, otherwise drop it later
        
        # Select final columns for the fact table
        # Ensure column order matches table schema, especially partition columns last
        final_df = transformed_detections.select(
            "candid", "objectid", "jd", "fid", "pid", "ra", "dec", "magpsf", "sigmapsf", 
            "diffmaglim", "isdiffpos", "programid", "distnr", "magnr", "sigmagnr",
            "fwhm", "classtar",
            "year", "month", "day" # Partition columns MUST be last
        )
        final_df.printSchema()

        # --- Write to Iceberg Table ---
        target_iceberg_table = f"nessie.{target_database}.{target_table}"
        pipes.log.info(f"Writing DataFrame to Nessie Iceberg table: {target_iceberg_table}")
        
        try:
            (final_df.write
                .format("iceberg")
                .mode("append") # Append new detections, assumes candid uniqueness is handled by dropDuplicates
                .option("write.distribution-mode", "hash") # Improve write parallelism
                .option("write.spark.fanout.enabled", "true") # Usually good for partitioned writes
                .partitionBy("year", "month", "day")
                .saveAsTable(target_iceberg_table))
            pipes.log.info(f"Successfully wrote to Nessie Iceberg table: {target_iceberg_table}")
        except Exception as e:
            pipes.log.error(f"Failed to write to Iceberg table {target_iceberg_table}: {e}")
            # Consider logging specific Spark error details
            raise

        
        pipes.report_asset_materialization(
            metadata={
                "target_iceberg_table": target_iceberg_table,
                "bronze_source_partition_path": bronze_partition_path,
                "spark_app_name": spark.sparkContext.appName,
            }
        )
        pipes.log.info("Asset materialization reported to Dagster.")

        # --- Clean Up ---
        # alerts_df.unpersist() # If cached
        spark.stop()

if __name__ == "__main__":
    main() 