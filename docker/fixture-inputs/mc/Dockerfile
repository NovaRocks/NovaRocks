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

ARG RUNTIME_BASE
FROM ${RUNTIME_BASE} AS build
ARG TARGETARCH
ARG GOPROXY=https://goproxy.cn,https://proxy.golang.org
ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local \
    GOPATH=/go \
    GOPROXY=${GOPROXY} \
    PATH=/usr/local/go/bin:${PATH}
WORKDIR /src
COPY artifacts/go-linux-${TARGETARCH}.tar.gz /tmp/go-toolchain.tar.gz
COPY artifacts/mc-source.tar.gz /tmp/mc-source.tar.gz
RUN tar -xzf /tmp/go-toolchain.tar.gz -C /usr/local && \
    tar -xzf /tmp/mc-source.tar.gz --strip-components=1 -C /src && \
    rm /tmp/go-toolchain.tar.gz /tmp/mc-source.tar.gz && \
    test "$(go env GOVERSION)" = go1.24.8

# These are the upstream gen-ldflags.go fields for the verified source commit.
# The source archive has no .git; pass its frozen facts without modifying it.
RUN --mount=type=cache,target=/go/pkg/mod,id=novarocks-minio-go1.24.8-mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,id=novarocks-minio-go1.24.8-build-${TARGETARCH},sharing=locked \
    mkdir -p /out && \
    go build -mod=readonly -trimpath -buildvcs=false \
      -ldflags="-s -w \
        -X github.com/minio/mc/cmd.Version=2025-08-13T08:35:41Z \
        -X github.com/minio/mc/cmd.CopyrightYear=2025 \
        -X github.com/minio/mc/cmd.ReleaseTag=RELEASE.2025-08-13T08-35-41Z \
        -X github.com/minio/mc/cmd.CommitID=7394ce0dd2a80935aded936b09fa12cbb3cb8096 \
        -X github.com/minio/mc/cmd.ShortCommitID=7394ce0dd2a8" \
      -o /out/mc .

FROM ${RUNTIME_BASE}
COPY --from=build --chmod=755 /out/mc /usr/bin/mc
COPY --from=build /src/LICENSE /licenses/LICENSE
COPY --from=build /src/CREDITS /licenses/CREDITS
COPY --from=build /usr/local/go/LICENSE /licenses/go-LICENSE
ENTRYPOINT ["/usr/bin/mc"]
