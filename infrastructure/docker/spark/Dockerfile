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
ARG AWS_SDK_V1_VERSION=1.12.367
ARG MAVEN_REPO=https://repo1.maven.org/maven2

ARG SPARK_JARS_DIR=$SPARK_HOME/jars

WORKDIR /tmp/jars

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/hadoop/hadoop-client/${HADOOP_VERSION}/hadoop-client-${HADOOP_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/software/amazon/awssdk/s3/${AWS_SDK_VERSION}/s3-${AWS_SDK_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/software/amazon/awssdk/url-connection-client/${AWS_SDK_VERSION}/url-connection-client-${AWS_SDK_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar

RUN wget --no-verbose ${MAVEN_REPO}/org/apache/spark/spark-hadoop-cloud_${SCALA_VERSION}/${SPARK_VERSION}/spark-hadoop-cloud_${SCALA_VERSION}-${SPARK_VERSION}.jar

RUN cp *.jar ${SPARK_JARS_DIR}/ && \
    cd / && rm -rf /tmp/jars
