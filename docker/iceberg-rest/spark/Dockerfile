ARG SPARK_BASE
FROM ${SPARK_BASE}

ARG ICEBERG_VERSION
ARG ICEBERG_SPARK_RUNTIME
ARG HADOOP_VERSION
ARG AWS_SDK_BUNDLE_VERSION
USER root

COPY artifacts/${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar /opt/spark/jars/
COPY artifacts/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
COPY artifacts/hadoop-aws-${HADOOP_VERSION}.jar /opt/spark/jars/
COPY artifacts/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/

RUN chmod 0644 \
    /opt/spark/jars/${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
    /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar
