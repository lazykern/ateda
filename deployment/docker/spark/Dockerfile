# deployment/docker/spark/Dockerfile

# Use a specific Spark version for reproducibility
ARG SPARK_VERSION=3.5.5
FROM bitnami/spark:${SPARK_VERSION}

# Switch to root user to install packages and modify permissions
USER root

# --- Install System Dependencies ---
# curl is needed for downloading JARs. procps is helpful for debugging (ps command).
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl procps && \
    rm -rf /var/lib/apt/lists/*

# --- Download Required JARs ---
# Define versions (ensure compatibility with Spark 3.5.1)
ARG ICEBERG_VERSION=1.5.0
ARG AWS_SDK_VERSION=1.12.780  # For S3 access
ARG HADOOP_AWS_VERSION=3.3.6
ARG NESSIE_VERSION=0.103.3    # For Nessie Catalog integration

ENV SPARK_HOME=/opt/bitnami/spark
ENV SPARK_JARS_DIR=${SPARK_HOME}/jars

# Download Required JARs (Iceberg, AWS, Nessie)
RUN echo "Downloading Iceberg Runtime JAR..." && \
    curl -fSL --retry 3 --retry-delay 5 "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" -o "${SPARK_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" && \
    echo "Downloading Hadoop AWS JAR..." && \
    curl -fSL --retry 3 --retry-delay 5 "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -o "${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    echo "Downloading AWS SDK JAR..." && \
    curl -fSL --retry 3 --retry-delay 5 "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" -o "${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" && \
    echo "Downloading Nessie Spark Extensions JAR..." && \
    curl -fSL --retry 3 --retry-delay 5 "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar" -o "${SPARK_JARS_DIR}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar"

# --- Install Python Dependencies ---
# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /bin/

# Install only necessary Python packages for the Spark job environment
# dagster-pipes for communication, boto3 for S3 interaction (used by dagster-pipes)
# PySpark is already included in the bitnami/spark base image.
RUN echo "Installing Python packages using uv..." && \
    uv pip install --system --no-cache "dagster-pipes>=0.24.0" "boto3>=1.28"

# --- Application Code ---
WORKDIR /app
# Copy the entire src directory containing Spark scripts
COPY ./src /app/src

# --- Permissions and User ---
# Ensure the spark user (1001) can execute scripts and write logs if needed
RUN chown -R 1001:1001 /app && \
    chmod -R g+w /opt/bitnami/spark/logs

# Switch back to the default spark user (UID 1001 in bitnami images)
USER 1001

# --- Entrypoint/Command ---
# Inherited from bitnami/spark base image

EXPOSE 8080 7077 8081