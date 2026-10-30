# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

ARG BUILDER
ARG REST_BASE
FROM ${BUILDER} AS prep
ARG NOVA_FIXTURE_URL_PREFIXES={}
USER root
WORKDIR /build
ENV CI=true \
    NOVA_FIXTURE_URL_PREFIXES=${NOVA_FIXTURE_URL_PREFIXES} \
    REST_MV_GRADLE_ARGS="--init-script /build/transport.init.gradle --no-daemon --console=plain --max-workers=2 -Dorg.gradle.jvmargs=-Xmx1536m -DsparkVersions= -DflinkVersions= -DkafkaVersions= :iceberg-core:test --tests org.apache.iceberg.view.TestViewVersionParser --tests org.apache.iceberg.view.TestViewMetadata --tests org.apache.iceberg.rest.TestRESTCatalogServlet :iceberg-open-api:shadowJar"
COPY transport.init.gradle /build/transport.init.gradle
COPY artifacts/apache-iceberg-1.10.1.tar.gz /build/source.tar.gz
RUN tar -xzf source.tar.gz && rm source.tar.gz && \
    test "$(sed -n 's/^git.commit.id=//p' apache-iceberg-1.10.1/iceberg-build.properties)" = "ccb8bc435062171e64bc8b7e5f56e6aed9c5b934"
COPY patches/ /build/patches/
WORKDIR /build/apache-iceberg-1.10.1
RUN for patch in /build/patches/*.patch; do \
      test -f "$patch" || continue; \
      git apply --check "$patch" && git apply "$patch" || exit 1; \
    done

# This target is invoked explicitly by recipe maintenance, never by provision.
FROM prep AS verification-metadata
RUN --mount=type=cache,target=/home/gradle/.gradle,id=novarocks-rest-mv-gradle-8.14.3-jdk17,sharing=locked \
    rm -f gradle/verification-metadata.xml && \
    GRADLE_USER_HOME=/home/gradle/.gradle gradle $REST_MV_GRADLE_ARGS --refresh-dependencies --write-verification-metadata sha256
FROM scratch AS verification-metadata-export
COPY --from=verification-metadata /build/apache-iceberg-1.10.1/gradle/verification-metadata.xml /

FROM prep AS build
COPY verification-metadata.xml gradle/verification-metadata.xml
RUN --mount=type=cache,target=/home/gradle/.gradle,id=novarocks-rest-mv-gradle-8.14.3-jdk17,sharing=locked \
    GRADLE_USER_HOME=/home/gradle/.gradle gradle $REST_MV_GRADLE_ARGS --dependency-verification strict && \
    cp open-api/build/libs/iceberg-open-api-test-fixtures-runtime-1.10.1.jar /build/iceberg-rest-adapter.jar && \
    if test -r /sys/fs/cgroup/memory.peak; then \
      printf 'REST_MV_BUILD_MEMORY_PEAK_BYTES='; cat /sys/fs/cgroup/memory.peak; \
    fi

FROM ${REST_BASE}
COPY --from=build --chown=iceberg:iceberg /build/iceberg-rest-adapter.jar /usr/lib/iceberg-rest/iceberg-rest-adapter.jar
