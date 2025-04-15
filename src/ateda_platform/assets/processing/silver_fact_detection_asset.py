# src/ateda_platform/assets/processing/silver_fact_detection_asset.py
import os
import subprocess
from datetime import datetime
from dagster import MaterializeResult, TableColumn, TableSchema, asset, file_relative_path, get_dagster_logger, AssetExecutionContext, open_pipes_session, Config, AssetKey, AssetIn

# Import the shared partitions definition
from ...partitions import daily_partitions

logger = get_dagster_logger()

# --- Package Versions ---
SPARK_VERSION = "3.5.5"
SPARK_VERSION_SHORT = "3.5"
ICEBERG_VERSION = "1.8.1"
HADOOP_VERSION = "3.3.4"  # Match Spark's built-in Hadoop version
AWS_SDK_VERSION = "2.20.162"  # Updated to AWS SDK V2

# --- ZTF Alert Processing Assets ---

class SilverFactDetectionConfig(Config):
    """Configuration for the silver_fact_detection asset."""
    bronze_s3_prefix: str = "ztf_alerts" # Matches default in ingestion asset
    database: str = "silver"
    table: str = "fact_detection"

@asset(
    group_name="processing",
    description="Processes Bronze ZTF Avro alerts into the Silver fact_detection Iceberg table using Spark.",
    kinds={"spark", "iceberg", "s3"},
    required_resource_keys={"pipes_s3_context_injector", "pipes_s3_message_reader", "nessie", "s3"},
    partitions_def=daily_partitions,
    ins={"upstream_partitioned": AssetIn(key=AssetKey(["raw_ztf_avro_alert_files"]))},
    metadata={
        "table_schema": TableSchema(
            columns=[
                TableColumn("candid", "bigint", description="Unique identifier for the detection"),
                TableColumn("objectid", "bigint", description="Unique identifier for the object"),
                TableColumn("jd", "double", description="Observation Julian date"),
                TableColumn("fid", "int", description="Filter ID (1=g, 2=r, 3=i)"),
                TableColumn("pid", "bigint", description="Processing ID for the source image"),
                TableColumn("ra", "double", description="Right Ascension (J2000)"),
                TableColumn("dec", "double", description="Declination (J2000)"),
                TableColumn("magpsf", "float", description="Magnitude from PSF-fit photometry"),
                TableColumn("sigmapsf", "float", description="1-sigma uncertainty in magpsf"),
                TableColumn("diffmaglim", "float", description="5-sigma limiting magnitude in difference image"),
                TableColumn("isdiffpos", "boolean", description="True if detection is from positive (sci-ref) subtraction"),
                TableColumn("programid", "int", description="Program ID"),
                TableColumn("distnr", "float", description="Distance to nearest source in reference image [pixels]"),
                TableColumn("magnr", "float", description="Magnitude of nearest source in reference image"),
                TableColumn("sigmagnr", "float", description="Uncertainty in magnr"),
                TableColumn("fwhm", "float", description="FWHM assuming a Gaussian core [pixels]"),
                TableColumn("classtar", "float", description="Star/Galaxy classification score"),
                TableColumn("year", "int", description="Partition column: Year derived from jd"),
                TableColumn("month", "int", description="Partition column: Month derived from jd"),
                TableColumn("day", "int", description="Partition column: Day derived from jd"),
            ]
        ),
        "partition_columns": ["year", "month", "day"]
    }
)
def silver_fact_detection(
    context: AssetExecutionContext,
    config: SilverFactDetectionConfig,
    upstream_partitioned: list[str]
) -> MaterializeResult:
    """Asset that runs a Spark job via Dagster Pipes to create the fact_detection table."""
    # NOTE: The 'upstream_partitioned' argument is required by Dagster to map the dependency,
    # but its value (list of S3 keys from the bronze layer) is not directly used here
    # because the Spark script reads the bronze data based on the passed prefix.
    
    # --- Get Partition Key --- 
    partition_key = context.partition_key
    try:
        partition_date = datetime.strptime(partition_key, "%Y-%m-%d").date()
        year = partition_date.year
        month = f"{partition_date.month:02d}"
        day = f"{partition_date.day:02d}"
        partition_suffix = f"year={year}/month={month}/day={day}"
        context.log.info(f"Processing partition: {partition_key} -> {partition_suffix}")
    except ValueError as e:
        context.log.error(f"Invalid partition key format '{partition_key}'. Expected YYYY-MM-DD. Error: {e}")
        raise

    # Environment variables needed by the Spark script (and spark-submit)
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    silver_bucket = os.getenv("S3_SILVER_BUCKET") # Used for Nessie warehouse path
    bronze_bucket = os.getenv("S3_BRONZE_BUCKET") # Used to construct input path
    
    required_env = {
        "S3_ENDPOINT_URL": s3_endpoint_url,
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
        "S3_SILVER_BUCKET": silver_bucket,
        "S3_BRONZE_BUCKET": bronze_bucket,
        "VIRTUAL_ENV": os.getenv("VIRTUAL_ENV") # Needed for spark-submit PYSPARK vars
    }
    missing = [k for k, v in required_env.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables for Spark job: {missing}")

    # Construct the specific S3 path for the bronze data partition
    bronze_base_path = f"s3a://{bronze_bucket}/{config.bronze_s3_prefix}"
    bronze_partition_path = f"{bronze_base_path}/{partition_suffix}"
    context.log.info(f"Bronze S3 partition path for Spark: {bronze_partition_path}")

    # Path to the PySpark script
    # Assuming script is in ../../spark/process_alerts_to_fact_detection.py relative to this asset file
    script_path = os.path.abspath(file_relative_path(__file__, "../../spark/process_alerts_to_fact_detection.py"))
    context.log.info(f"Using PySpark script at: {script_path}")
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"PySpark script not found at expected location: {script_path}")

    # Environment for spark-submit subprocess
    subprocess_env = os.environ.copy()
    subprocess_env.update({
        "PYSPARK_PYTHON": os.path.join(os.environ["VIRTUAL_ENV"], "bin/python3"),
        "PYSPARK_DRIVER_PYTHON": os.path.join(os.environ["VIRTUAL_ENV"], "bin/python3"),
    })

    # Spark package dependencies (ensure these match versions needed by script/Iceberg/Nessie)
    # Assuming Iceberg runtime and Nessie extensions are needed, plus Spark's Avro package

    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION_SHORT}_2.12:{ICEBERG_VERSION}",
        # Spark Avro package (often included, but explicit can help)
        f"org.apache.spark:spark-avro_2.12:{SPARK_VERSION}",
        f"org.apache.hadoop:hadoop-common:{HADOOP_VERSION}",
        f"org.apache.hadoop:hadoop-client:{HADOOP_VERSION}",
        f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",
        f"software.amazon.awssdk:bundle:{AWS_SDK_VERSION}",
        f"software.amazon.awssdk:s3:{AWS_SDK_VERSION}",
        f"software.amazon.awssdk:url-connection-client:{AWS_SDK_VERSION}"
    ]
    spark_packages_str = ",".join(packages)

    # Open Pipes Session
    with open_pipes_session(
        context=context,
        context_injector=context.resources.pipes_s3_context_injector,
        message_reader=context.resources.pipes_s3_message_reader,
        extras={
            # Pass the specific partition path instead of the base prefix path
            "bronze_partition_path": bronze_partition_path, 
            "database": config.database,
            "table": config.table,
        }
    ) as session:
        # Get bootstrap CLI arguments
        bootstrap_args = []
        for key, value in session.get_bootstrap_cli_arguments().items():
            bootstrap_args.extend([key, value])

        # Nessie/Iceberg/S3 configuration
        warehouse_path = f"s3a://{silver_bucket}/" # Warehouse per database
        nessie_conf = context.resources.nessie.get_spark_conf(warehouse_path=warehouse_path)
        app_name = f"spark-{context.asset_key.to_user_string()}-{config.database}-{config.table}"

        command = [
            "spark-submit",
            "--master", "local[*]",
            "--name", app_name,
            "--packages", spark_packages_str,
            "--conf", "spark.driver.memory=4g",
            "--conf", "spark.executor.memory=2g",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", f"spark.sql.files.maxPartitionBytes={256 * 1024 * 1024}", # 256 MB
            "--conf", f"spark.hadoop.fs.s3a.endpoint={s3_endpoint_url}",
            "--conf", f"spark.hadoop.fs.s3a.access.key={aws_access_key}",
            "--conf", f"spark.hadoop.fs.s3a.secret.key={aws_secret_key}",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        ]

        # Add Nessie/Iceberg config
        for key, value in nessie_conf.items():
            command.extend(["--conf", f"{key}={value}"])

        # Add SQL extensions separately for clarity
        command.extend(["--conf", f"spark.sql.extensions={nessie_conf['spark.sql.extensions']}"])
        # Add catalog implementation config
        command.extend(["--conf", f"spark.sql.catalog.{context.resources.nessie.catalog_name}.catalog-impl={nessie_conf[f'spark.sql.catalog.{context.resources.nessie.catalog_name}.catalog-impl']}"])
        command.extend(["--conf", f"spark.sql.catalog.{context.resources.nessie.catalog_name}.warehouse={warehouse_path}"])


        # Add script and pipes args
        command.append(script_path)
        command.extend(bootstrap_args)

        context.log.info(f"Running command: {' '.join(command)}")
        
        # Run spark-submit
        # Use check=True to raise error on failure
        try:
            subprocess.run(command, env=subprocess_env, check=True, text=True, capture_output=True)
            # Log stdout/stderr if needed for debugging, Pipes should capture primary output
            # context.log.info(f"Spark submit stdout:\n{result.stdout}")
            # if result.stderr:
            #     context.log.warning(f"Spark submit stderr:\n{result.stderr}")
        except subprocess.CalledProcessError as e:
            context.log.error(f"spark-submit failed with return code {e.returncode}")
            context.log.error(f"stdout: {e.stdout}")
            context.log.error(f"stderr: {e.stderr}")
            raise
        except Exception as e:
            context.log.error(f"An unexpected error occurred running spark-submit: {e}")
            raise

        return session.get_results() 