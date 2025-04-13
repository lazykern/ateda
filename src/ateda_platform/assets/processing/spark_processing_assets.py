import os
import subprocess
from dagster import MaterializeResult, TableColumn, TableSchema, asset, file_relative_path, get_dagster_logger, AssetExecutionContext, open_pipes_session, Config, MetadataValue

logger = get_dagster_logger()

# --- Package Versions ---
SPARK_VERSION = "3.5.5"  
SPARK_VERSION_SHORT = "3.5"
ICEBERG_VERSION = "1.8.1"
HADOOP_VERSION = "3.3.4"  # Match Spark's built-in Hadoop version
AWS_SDK_VERSION = "2.20.162"  # Updated to AWS SDK V2

# Define the config schema for the hello_iceberg_table asset
class HelloIcebergConfig(Config):
    """Configuration for the hello_iceberg_table asset."""
    table: str  # Table name is now required
    database: str  # Database name is now required

# Removed silver_ztf_alerts asset definition

# --- Hello Iceberg Asset (Refactored for local[*] Spark via Subprocess) ---

@asset(
    group_name="processing",
    description="Creates a simple 'hello world' Iceberg table via spark-submit local[*]",
    compute_kind="spark",
    required_resource_keys={"pipes_s3_context_injector", "pipes_s3_message_reader", "nessie"},
    metadata={
        "storage_location": MetadataValue.text(f"s3a://{os.getenv('S3_SILVER_BUCKET')}/"),
        "dagster/column_schema": TableSchema(
            columns=[
                TableColumn(
                    name="id", 
                    type="bigint",
                    description="Unique identifier"
                ),
                TableColumn(
                    name="value", 
                    type="string",
                    description="Sample data value"
                ),
                TableColumn(
                    name="timestamp", 
                    type="timestamp",
                    description="Record creation time"
                )
            ]
        )
    }
)
def hello_iceberg_table(
    context: AssetExecutionContext,
    config: HelloIcebergConfig,
) -> MaterializeResult:
    """Create a simple Iceberg table using PySpark via spark-submit."""

    # --- Environment Variables for Spark/Iceberg/S3 Config ---
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    silver_bucket = os.getenv("S3_SILVER_BUCKET")

    # --- Check Required Environment Variables ---
    required_env = {
        "S3_ENDPOINT_URL": s3_endpoint_url,
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
        "S3_SILVER_BUCKET": silver_bucket,
    }
    missing = [k for k, v in required_env.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")

    # --- Spark Package Dependencies ---
    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION_SHORT}_2.12:{ICEBERG_VERSION}",
        # Add Hadoop Common and Client explicitly
        f"org.apache.hadoop:hadoop-common:{HADOOP_VERSION}",
        f"org.apache.hadoop:hadoop-client:{HADOOP_VERSION}",
        f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",
        # Include both AWS SDK versions for compatibility
        f"software.amazon.awssdk:bundle:{AWS_SDK_VERSION}",
        f"software.amazon.awssdk:s3:{AWS_SDK_VERSION}",
        f"software.amazon.awssdk:url-connection-client:{AWS_SDK_VERSION}"
    ]
    spark_packages_str = ",".join(packages)

    # --- Path to PySpark Script ---
    script_path = os.path.join(file_relative_path(__file__, "../../spark/hello_iceberg.py"))
    script_path = os.path.abspath(script_path)
    context.log.info(f"Using PySpark script at: {script_path}")
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"PySpark script not found at expected location: {script_path}")

    # --- Environment Variables for Subprocess ---
    subprocess_env = os.environ.copy()
    subprocess_env.update({
        "PYSPARK_PYTHON": os.path.join(os.environ["VIRTUAL_ENV"], "bin/python3"),
        "PYSPARK_DRIVER_PYTHON": os.path.join(os.environ["VIRTUAL_ENV"], "bin/python3"),
    })

    # --- Open Pipes Session and Launch Spark ---
    with open_pipes_session(
        context=context,
        context_injector=context.resources.pipes_s3_context_injector,
        message_reader=context.resources.pipes_s3_message_reader,
        extras={
            "table": config.table,
            "database": config.database,
            "bucket": silver_bucket,
        }
    ) as session:
        # Get bootstrap CLI arguments from session
        bootstrap_args = []
        for key, value in session.get_bootstrap_cli_arguments().items():
            bootstrap_args.extend([key, value])

        # --- spark-submit Command Construction ---
        # Use bucket root as warehouse path for Iceberg
        warehouse_path = f"s3a://{silver_bucket}/"

        # Get Nessie configuration from the resource
        nessie_conf = context.resources.nessie.get_spark_conf(warehouse_path=warehouse_path)

        # Construct dynamic app name
        app_name = f"spark-{context.asset_key.to_user_string()}"
        context.log.info(f"Setting Spark application name to: {app_name}")

        command = [
            "spark-submit",
            "--master", "local[*]",
            "--name", app_name,
            "--packages", spark_packages_str,
            # S3A filesystem configuration
            "--conf", f"spark.hadoop.fs.s3a.endpoint={s3_endpoint_url}",
            "--conf", f"spark.hadoop.fs.s3a.access.key={aws_access_key}",
            "--conf", f"spark.hadoop.fs.s3a.secret.key={aws_secret_key}",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            # Additional S3A configurations for AWS SDK compatibility
            "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=true",
            "--conf", "spark.hadoop.fs.s3a.fast.upload=true",
            "--conf", "spark.hadoop.fs.s3a.multipart.size=64M",
        ]

        # Add Nessie configuration from the resource
        for key, value in nessie_conf.items():
            command.extend(["--conf", f"{key}={value}"])

        # Add script path
        command.append(script_path)
        
        # Append Dagster Pipes bootstrap arguments
        command.extend(bootstrap_args)

        # Run the spark-submit command
        subprocess.run(command, env=subprocess_env, check=True)

        return session.get_results()