import argparse
import os
import boto3
from dagster_pipes import (
    PipesCliArgsParamsLoader,
    PipesS3ContextLoader,
    PipesS3MessageWriter,
    open_dagster_pipes,
)
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=str, required=True)
    parser.add_argument("--warehouse-name", type=str, required=True)
    parser.add_argument("--database-name", type=str, required=True)
    args, _ = parser.parse_known_args()

    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"]

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=s3_endpoint_url,
    )

    with open_dagster_pipes(
        message_writer=PipesS3MessageWriter(client=s3_client),
        context_loader=PipesS3ContextLoader(client=s3_client),
        params_loader=PipesCliArgsParamsLoader(),
    ) as pipes:
        spark = SparkSession.builder.getOrCreate()

        pipes.log.info(f"Starting Spark processing for input: {args.input_dir}")

        df = (
            spark.read.format("parquet")
            .load(args.input_dir)
            .select(["objectId", "candidate.*"])
            .withColumn(
                "isdiffpos",
                F.when(F.col("isdiffpos").isin("t", "1"), True)
                .when(F.col("isdiffpos").isin("f", "0"), False)
                .otherwise(None),
            )
            .withColumn(
                "filter_name",
                F.when(F.col("fid") == 1, "g")
                .when(F.col("fid") == 2, "r")
                .when(F.col("fid") == 3, "i")
                .otherwise(None),
            )
            .withColumn(
                "timestamp", F.to_timestamp(((F.col("jd") - 2440587.5) * 86400.0))
            )
            .withColumn("year", F.year("timestamp"))
            .withColumn("month", F.month("timestamp"))
            .withColumn("day", F.dayofmonth("timestamp"))
        )

        df.createOrReplaceTempView("updates")

        spark.sql(
            f"""
            CREATE NAMESPACE IF NOT EXISTS {args.warehouse_name}.{args.database_name} 
            """
        )

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {args.warehouse_name}.{args.database_name}.fact_detection
            USING iceberg
            PARTITIONED BY (year, month, day)
            AS SELECT * FROM updates LIMIT 0
            """
        )

        spark.sql(
            f"""
            MERGE INTO {args.warehouse_name}.{args.database_name}.fact_detection t
            USING updates s
            ON t.candid = s.candid
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        )

        count = spark.sql("SELECT COUNT(*) FROM updates").first()[0]
        table_name = f"{args.warehouse_name}.{args.database_name}.fact_detection"
        pipes.log.info(f"Successfully merged {count} records into {table_name}")

        pipes.report_asset_materialization(
            metadata={
                "table": table_name,
                "num_records_merged": count,
            }
        )

        spark.stop()


if __name__ == "__main__":
    main()
