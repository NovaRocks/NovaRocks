ARG SPARK_VERSION=3.5.5-java17
FROM apache/spark:${SPARK_VERSION}

ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_SPARK_RUNTIME=iceberg-spark-runtime-3.5_2.12
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG MAVEN_REPO=https://repo.maven.apache.org/maven2

USER root

ADD ${MAVEN_REPO}/org/apache/iceberg/${ICEBERG_SPARK_RUNTIME}/${ICEBERG_VERSION}/${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD ${MAVEN_REPO}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD ${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar /opt/spark/jars/
ADD ${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/

RUN chmod 0644 \
    /opt/spark/jars/${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
    /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar
