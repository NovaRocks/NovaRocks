# Standalone Hive Metastore for the NovaRocks Iceberg test fixture.
# Adds the base image's Hadoop S3A jars to Hive's classpath so the metastore
# can resolve s3a:// warehouse locations against MinIO. Backed by its Derby DB
# (persisted in the catalog-scoped HMS volume).
FROM apache/hive:4.0.0

USER root

# These versions must match the jars bundled under /opt/hadoop/share/hadoop.
ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_VERSION=1.12.367
ARG HMS_WAREHOUSE_DIR=s3a://warehouse/shared/hms

RUN set -eux; \
    mkdir -p /opt/hive/data; chown hive:hive /opt/hive/data; \
    test -f "/opt/hadoop/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar"; \
    cp "/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar" /opt/hive/lib/; \
    cp "/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" /opt/hive/lib/; \
    grep -q '<value>/opt/hive/data/warehouse</value>' /opt/hive/conf/hive-site.xml; \
    sed -i "s#<value>/opt/hive/data/warehouse</value>#<value>${HMS_WAREHOUSE_DIR}</value>#" /opt/hive/conf/hive-site.xml

COPY core-site.xml /opt/hive/conf/core-site.xml

USER hive
