# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This Dockerfile is used only by docker/fixture-inputs/provision.py. Its
# SPARK_BASE and artifacts/ come from the centralized creation recipe.
# Existing writers are used without environment preflight.
ARG SPARK_BASE
FROM --platform=linux/amd64 ${SPARK_BASE}

ARG PAIMON_VERSION
ARG PAIMON_SPARK_ARTIFACT
ARG PAIMON_SPARK_JAR_SHA1=4a85e61ee8535df9756d2700760cc6d2e232f166
ARG PAIMON_SPARK_JAR_SIZE
ARG PAIMON_S3_ARTIFACT=paimon-s3
ARG PAIMON_S3_JAR_SHA1=1be7c161970d2e909958d7e38679d679779a679b
ARG PAIMON_S3_JAR_SIZE=31776897
USER root

COPY artifacts/paimon-spark.jar /opt/spark/jars/paimon-spark.jar
COPY artifacts/paimon-s3.jar /opt/spark/jars/paimon-s3.jar

RUN set -eux; \
    test "$(wc -c < /opt/spark/jars/paimon-spark.jar)" -eq "${PAIMON_SPARK_JAR_SIZE}"; \
    echo "${PAIMON_SPARK_JAR_SHA1}  /opt/spark/jars/paimon-spark.jar" | sha1sum -c -; \
    test "$(wc -c < /opt/spark/jars/paimon-s3.jar)" -eq "${PAIMON_S3_JAR_SIZE}"; \
    echo "${PAIMON_S3_JAR_SHA1}  /opt/spark/jars/paimon-s3.jar" | sha1sum -c -; \
    chmod 0644 /opt/spark/jars/paimon-spark.jar /opt/spark/jars/paimon-s3.jar

USER spark
