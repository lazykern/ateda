import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr
import argparse
import sys
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
    FloatType,
    ArrayType,
    BinaryType,
)
# --- Logging Setup ---
logger = logging.getLogger(__name__)

# --- Spark Session ---
spark = SparkSession.builder.getOrCreate()

# --- Define Alert Schema (based on alert_schema.json) ---
parser = argparse.ArgumentParser(
    description="Ingest ZTF alerts into Bronze Iceberg table."
)
parser.add_argument(
    "--input-path",
    type=str,
    required=True,
    help="Input path for ZTF alert files (e.g., 's3a://bucket/raw/ztf/YYYYMMDD/')",
)
parser.add_argument(
    "--input-format",
    type=str,
    required=True,
    help="Input format for ZTF alert files (e.g., 'parquet', 'avro')",
)
parser.add_argument(
    "--catalog-name",
    type=str,
    required=True,
    help="Name of the Iceberg Catalog (configured in Spark)",
)
parser.add_argument(
    "--database-name",
    type=str,
    required=True,
    help="Target database name within the Iceberg Catalog",
)
parser.add_argument(
    "--table-name",
    type=str,
    required=True,
    help="Target table name within the Iceberg Database",
)
args, unknown = parser.parse_known_args()

input_path = args.input_path
input_format = args.input_format
catalog_name = args.catalog_name
database_name = args.database_name
table_name = args.table_name
target_table_fqn = (
    f"{catalog_name}.{database_name}.{table_name}"  # Fully qualified name
)


# --- 1. Read Raw Data ---
try:
    df_raw: DataFrame = (
        spark.read.format(input_format)
        .load(input_path)
    )
except Exception as e:
    logger.error(f"Failed to read data from {input_path}. Error: {e}", exc_info=True)
    sys.exit(1)  # Exit if reading fails


# --- 2. Prepare Data for Partitioning ---

partition_timestamp_col = (
    "observation_timestamp"
)

try:
    df_prepared = df_raw.withColumn(
        partition_timestamp_col,
        expr("to_timestamp((candidate.jd - 2440587.5) * 86400.0)"),
    )
except Exception as e:
    logger.error(f"Failed to convert JD column. Error: {e}", exc_info=True)
    sys.exit(1)

# --- 3. Write to Bronze Iceberg Table ---

df_prepared.createTempView("ztf_alert")

try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{database_name}")

    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {target_table_fqn}
              USING iceberg
              PARTITIONED BY (days({partition_timestamp_col}))
              AS SELECT * FROM ztf_alert
              LIMIT 0
              """)
except Exception as e:
    logger.error(f"Failed to create or verify database/table {target_table_fqn}. Error: {e}", exc_info=True)
    sys.exit(1)


try:
    (
        df_prepared.write.format("iceberg")
        .mode("append")
        .option("mergeSchema", "true")
        .save(target_table_fqn)
    )
except Exception as e:
    logger.error(f"Failed to write data to Iceberg table {target_table_fqn}. Error: {e}", exc_info=True)
    sys.exit(1)


# Spark session stop is usually handled by the environment (like Databricks, EMR)
# spark.stop()