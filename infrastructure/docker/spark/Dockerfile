FROM apache/spark:3.5.5-python3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dagster-pipes boto3

ARG SPARK_VERSION=3.5.5
ARG SPARK_VERSION_SHORT=3.5
ARG SCALA_VERSION=2.12
ARG ICEBERG_VERSION=1.8.1
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=2.20.162

ARG MAVEN_REPO=https://repo1.maven.org/maven2

ARG SPARK_JARS_DIR=$SPARK_HOME/jars

WORKDIR /tmp/jars

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar

RUN cp *.jar ${SPARK_JARS_DIR}/ && \
    cd / && rm -rf /tmp/jars
