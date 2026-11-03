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

ARG REST_IMAGE
ARG BUILDER_IMAGE

FROM ${REST_IMAGE} AS rest_fixture

FROM ${BUILDER_IMAGE} AS builder
USER root
COPY --from=rest_fixture /usr/lib/iceberg-rest/iceberg-rest-adapter.jar /tmp/iceberg-rest-adapter.jar
COPY src/main/java /tmp/src/main/java
RUN mkdir -p /tmp/classes \
    && javac --release 17 \
        -classpath /tmp/iceberg-rest-adapter.jar \
        -d /tmp/classes \
        $(find /tmp/src/main/java -name '*.java' -print) \
    && jar --create --file /tmp/uea7-publication-hook.jar -C /tmp/classes .

FROM ${REST_IMAGE}
USER root
COPY --from=builder /tmp/uea7-publication-hook.jar /usr/lib/iceberg-rest/uea7-publication-hook.jar
RUN chown iceberg:iceberg /usr/lib/iceberg-rest/uea7-publication-hook.jar
USER iceberg

ENV CATALOG_CATALOG__IMPL=org.apache.iceberg.rest.fixture.HookedJdbcCatalog
ENV UEA7_FAULT_CONTROL_PORT=8182
ENV UEA7_FAULT_MAX_HOLD_SECONDS=120

CMD ["java", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/uea7-publication-hook.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
