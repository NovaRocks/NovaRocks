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
ARG SERVICE_BASE
FROM ${BUILDER} AS build
USER root
WORKDIR /build
COPY artifacts/ /build/artifacts/
COPY src/ /build/source/src/
COPY runtime-jars.txt build.sh /build/source/
RUN bash /build/source/build.sh /build/artifacts /build/output --test

FROM ${SERVICE_BASE}
USER root
ARG SERVICE_KIND
ARG SERVICE_USER
ENV NOVA_OSS_SERVICE_KIND=${SERVICE_KIND}
COPY --from=build /build/output/ /opt/novarocks/aliyun-credentials/
COPY entrypoint.sh /opt/novarocks/aliyun-credentials/entrypoint.sh
RUN chmod 0755 /opt/novarocks/aliyun-credentials/entrypoint.sh && \
    case "$NOVA_OSS_SERVICE_KIND" in \
      spark) cp /opt/novarocks/aliyun-credentials/*.jar /opt/spark/jars/ ;; \
      rest) test -f /usr/lib/iceberg-rest/iceberg-rest-adapter.jar ;; \
      *) exit 64 ;; \
    esac
USER ${SERVICE_USER}
ENTRYPOINT ["/opt/novarocks/aliyun-credentials/entrypoint.sh"]
CMD []
