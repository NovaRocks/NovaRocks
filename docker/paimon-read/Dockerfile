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

# Design: ADR-0141 (docs/adr/ADR-0141-fixture-images-never-pull.md)
# The pinned base is the Linux/amd64 manifest behind apache/spark:3.5.3. The
# fixture deliberately runs this platform on every host so the external writer
# is byte-for-byte reproducible, including on Apple Silicon Docker Desktop.
#
# SPARK_BASE must be a reference BuildKit can resolve from the local image
# store, and fixture.py supplies a local alias tag for exactly the manifest
# recorded below. A digest reference here would defeat that: BuildKit resolves
# `FROM repo@sha256:...` against the registry even when the image is already
# local and its RepoDigest matches, so a digest-pinned FROM turns every build
# -- a fully cached one included -- into a registry round-trip and fails on a
# host that cannot reach the registry. The digest stays the sole authority for
# which manifest may be used; fixture.py verifies it against the local image
# store before it creates the alias, so the check moved out of BuildKit's
# resolver into an explicit preflight rather than being dropped.
ARG SPARK_BASE_MANIFEST_DIGEST=sha256:b2da01c5855fdf791328a6fa1267b406336a535d39abc05699214a48bee95955
ARG SPARK_BASE=novarocks/paimon-read-base:b2da01c5855f
FROM --platform=linux/amd64 ${SPARK_BASE}

ARG PAIMON_VERSION=1.3.1
ARG PAIMON_SPARK_ARTIFACT=paimon-spark-3.5
ARG PAIMON_SPARK_JAR_SHA1=4a85e61ee8535df9756d2700760cc6d2e232f166
ARG PAIMON_SPARK_JAR_SIZE=41895267
ARG PAIMON_S3_ARTIFACT=paimon-s3
ARG PAIMON_S3_JAR_SHA1=1be7c161970d2e909958d7e38679d679779a679b
ARG PAIMON_S3_JAR_SIZE=31776897
ARG MAVEN_REPOSITORY=https://repo.maven.apache.org/maven2

USER root

ADD ${MAVEN_REPOSITORY}/org/apache/paimon/${PAIMON_SPARK_ARTIFACT}/${PAIMON_VERSION}/${PAIMON_SPARK_ARTIFACT}-${PAIMON_VERSION}.jar /opt/spark/jars/paimon-spark.jar
ADD ${MAVEN_REPOSITORY}/org/apache/paimon/${PAIMON_S3_ARTIFACT}/${PAIMON_VERSION}/${PAIMON_S3_ARTIFACT}-${PAIMON_VERSION}.jar /opt/spark/jars/paimon-s3.jar

RUN set -eux; \
    test "$(wc -c < /opt/spark/jars/paimon-spark.jar)" -eq "${PAIMON_SPARK_JAR_SIZE}"; \
    echo "${PAIMON_SPARK_JAR_SHA1}  /opt/spark/jars/paimon-spark.jar" | sha1sum -c -; \
    test "$(wc -c < /opt/spark/jars/paimon-s3.jar)" -eq "${PAIMON_S3_JAR_SIZE}"; \
    echo "${PAIMON_S3_JAR_SHA1}  /opt/spark/jars/paimon-s3.jar" | sha1sum -c -; \
    chmod 0644 /opt/spark/jars/paimon-spark.jar /opt/spark/jars/paimon-s3.jar

USER spark
