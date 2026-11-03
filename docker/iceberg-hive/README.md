<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg Hive Metastore Test Environment

HMS owns one Compose project, recorded host port and Derby volume for each
fixture owner namespace + catalog ID. It does not adopt the old
`nr-iceberg-hive` project. Its container starts on its own network and joins
the catalog network through the fixture owner's exact consumer API, using
`hms` and `minio` aliases.

HMS 推荐版本、基础镜像和构建参数统一声明在 `docker/fixture-inputs/versions.json`；当前配方只声明 amd64。已有 HMS 直接使用保存配置，只有确实缺失时按推荐构建，不自动检查 digest/BOM/软件版本或整栈健康。部分创建或已有损坏报错，交人工维护。可显式提前准备缺失镜像：

```bash
docker/fixture-inputs/provision.sh --consumer derived:hive-metastore --platform linux/amd64
```

附着 REST publication，再启动或复用 HMS：

```bash
publication=$(docker/iceberg-rest/up.sh | python3 -c 'import json,sys; print(json.load(sys.stdin)["published_dir"])')
export NOVA_ENV_REST_ENV_FILE="$publication/env.sh"
source "$NOVA_ENV_REST_ENV_FILE"
hms_manifest=$(docker/iceberg-hive/up.sh)
hms_dir=$(printf '%s' "$hms_manifest" | python3 -c 'import json,sys,pathlib; print(pathlib.Path(json.load(sys.stdin)["compose_env"]).parent)')
source "$hms_dir/env.sh"
```

HMS拥有自己的项目和Derby卷，只连接准确machine外部网络，不归REST核心栈计数。它使用机器配置的HMS端口（默认28005）；不再按旧catalog版本分配或扫描端口。普通 up 复用已有记录，只有缺失的镜像才按统一配方构建。实际 HMS listener 的启动/退出仍由其原 owner 负责，不扩展成全机器环境检查。`--prepare-only`只按已固定的ready publication生成文件，不调用Docker；REST未绑定时不能猜依赖。

保存目录为 `<machine-control-root>/<machine-id>/hms/<machine-id>/`，包含manifest、env、Compose、core-site、catalog SQL和Spark defaults。机器version/generation、网络ID及准确HMS容器共同约束连接。换activation前须通过HMS自身入口退出旧记录，不能由REST owner删除其Derby卷。

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms --mode verify
printf 'SHOW NAMESPACES IN hms_catalog;\n' > /tmp/hms-check.sql
docker/iceberg-rest/spark-sql.sh /tmp/hms-check.sql
```

Spark使用显式 `NOVAROCKS_SPARK_EXTRA_DEFAULTS`。HMS产品永久只读合同继续由ADR-0169约束；供给工具自己的写入也要沿用原run identity、scratch root和预算。

`status.sh`显示保存的machine依赖、项目、端点与状态。普通down退出自身准确容器并撤销连接，保留Derby卷；`--volumes`或`--purge`确认自己的容器/卷消失后退休记录。固定machine网络是external，HMS绝不删除。清理失败保留deleting及原事实供重试。

```bash
docker/iceberg-hive/down.sh --machine-id "$NOVA_FIXTURE_MACHINE_ID" --volumes
# If the original publication is unavailable, use its saved machine binding:
docker/iceberg-hive/down.sh --root "$NOVA_FIXTURE_MACHINE_ROOT" \
  --daemon "$NOVA_FIXTURE_MACHINE_DAEMON" --machine-id "$NOVA_FIXTURE_MACHINE_ID" --volumes
```

不按旧项目名或固定端口扫描认领历史资源。机器维护与数据保留见 [fixture README](../iceberg-rest/README.md) 和 [ADR-0191](../../docs/adr/ADR-0191-machine-shared-fixture-and-bounded-scratch.md)。
