## Spark

### Commands

#### PySpark

```bash
pyspark \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.spark:spark-avro_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-client:3.3.4,software.amazon.awssdk:bundle:2.20.162,software.amazon.awssdk:s3:2.20.162,software.amazon.awssdk:url-connection-client:2.20.162 \
--conf spark.driver.memory=2g \
--conf spark.executor.memory=2560m \
--conf spark.executor.memoryOverhead=512m \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=1m \
--conf spark.sql.files.maxPartitionBytes=512m \
--conf spark.sql.shuffle.partitions=200 \
--conf spark.dynamicAllocation.shuffleTracking.enabled=true \
--conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
--conf spark.hadoop.fs.s3a.access.key=minioadmin \
--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
--conf spark.hadoop.fs.s3a.connection.maximum=200 \
--conf spark.hadoop.fs.s3a.fast.upload=true \
--conf spark.hadoop.fs.s3a.committer.magic.enabled=true \
--conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
--conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
--conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
--conf spark.hadoop.fs.s3a.committer.name=magic \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.ateda_silver=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.ateda_silver.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
--conf spark.sql.catalog.ateda_silver.uri=http://localhost:19120/iceberg \
--conf spark.sql.catalog.ateda_silver.warehouse=ateda_silver \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.local.dir=/tmp \
--master "local[*]"
```

#### SQL shell

```bash
spark-sql \
--master "local[*]" \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.spark:spark-avro_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-client:3.3.4,software.amazon.awssdk:bundle:2.20.162,software.amazon.awssdk:s3:2.20.162,software.amazon.awssdk:url-connection-client:2.20.162 \
--conf spark.driver.memory=2g \
--conf spark.executor.memory=2560m \
--conf spark.executor.memoryOverhead=512m \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=1m \
--conf spark.sql.files.maxPartitionBytes=512m \
--conf spark.sql.shuffle.partitions=200 \
--conf spark.dynamicAllocation.shuffleTracking.enabled=true \
--conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
--conf spark.hadoop.fs.s3a.access.key=minioadmin \
--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
--conf spark.hadoop.fs.s3a.connection.maximum=200 \
--conf spark.hadoop.fs.s3a.fast.upload=true \
--conf spark.hadoop.fs.s3a.committer.magic.enabled=true \
--conf spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
--conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
--conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
--conf spark.hadoop.fs.s3a.committer.name=magic \
--conf spark.sql.catalog.ateda_silver=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.ateda_silver.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
--conf spark.sql.catalog.ateda_silver.uri=http://localhost:19120/iceberg \
--conf spark.sql.catalog.ateda_silver.warehouse=ateda_silver \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.local.dir=/tmp
```