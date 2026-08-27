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

# 分布式部署

分布式部署使用 NovaRocks native 角色能力，将协调节点和计算节点拆分为不同进程。FE 角色提供 MySQL 入口、SQL 解析、优化和 fragment 调度；BE 角色提供 NovaRocks gRPC 后端服务并执行 fragment。

该模式不依赖 StarRocks FE。StarRocks 数据只通过只读 external Connector 接入：RPC 支持所有拓扑，direct 永久只支持 shared-data。

## 部署拓扑

典型拓扑如下：

```text
MySQL client
  |
  v
NovaRocks role=fe
  |  Native self-registration + FE-pull exact heartbeat verification
  v
NovaRocks role=be  +  NovaRocks role=be  +  ...
```

角色说明：

| 角色 | 作用 | 对外端口 |
| --- | --- | --- |
| `fe` | 接收 MySQL 连接，解析 SQL，优化计划，调度 fragment 到后端 | MySQL：`[standalone_server].mysql_port`；Native coordinator-report gRPC：`[server].grpc_port`；FE management HTTP：`[server].http_port` |
| `be` | 执行 FE 下发的 fragment，处理 exchange 和结果回传 | Native fragment/exchange gRPC：`[server].grpc_port`；BE management HTTP：`[server].http_port` |

## 前提条件

- FE 节点和所有 BE 节点使用同一版本的 NovaRocks。
- FE 节点可以访问每个 BE 的 `grpc_port`。
- BE 节点可以访问 FE 的 `grpc_port`，用于向 coordinator 上报执行状态。
- 所有 FE/BE 配置必须有完全相同的 `[native_trust].deployment_id`、shared
  secret 与 transport mode；Native JWT 是 mandatory，TLS 只是在其上增加的可选层。
- 所有 BE 节点都能访问相同的数据源、对象存储和 catalog。
- 如果使用对象存储，所有节点的凭据、endpoint 和 path-style 设置应保持一致。
- `role=fe` 必须显式选择 `[catalog_source]`。StaticFile 从一份挂载的完整 snapshot 启动；
  DynamicStateStore 才要求可用的 SQLite `[state_store]` 并允许 SQL catalog mutation。StateStore
  不是 backend membership source。backend desired lifecycle 属于外部 orchestrator，FE 的 observed
  registry 会由 BE announce 和 FE-pull heartbeat 在重启后重建。SQLite 只适用于恰好一个 active FE；多 FE
  fencing/takeover 尚未实现，不能由 StateStore SPI 或 SQLite 配置字段推断。

## 编译 NovaRocks

分布式部署使用 NovaRocks native runtime，不依赖 StarRocks FE。FE 角色和 BE 角色使用同一个 NovaRocks 二进制文件，建议构建 release 二进制后分发到所有节点。

推荐构建命令：

```bash
cargo build --release -p novarocks-server
```

本地伪分布式验证时，也可以使用 debug 构建加快迭代：

```bash
cargo build
```

后续启动命令中的 `./target/release/novarocks` 可相应替换为 `./target/debug/novarocks`。

## 配置 BE 节点

BE 节点使用 `server.grpc_port` 提供 Native NovaRocksGrpc 服务，使用独立的
`server.http_port` 提供 BE-scoped management metrics。Native listener 不提供任何
management route，management listener 也不提供 Native gRPC service。不同机器上的
BE 可以都使用默认端口；同一 address family 内不能让任意 Native/management
listener 复用相同端口，wildcard bind 也会与同端口具体地址冲突。

示例 `be-1.toml`：

```toml
log_level = "info"

[server]
host = "0.0.0.0"
grpc_port = 9080
http_port = 8040
# Keep this aligned with terminationGracePeriodSeconds: 360 below.
frontend_drain_timeout_ms = 300000
frontend_cleanup_timeout_ms = 30000

[native_trust]
deployment_id = "analytics-prod"
shared_secret = "${ENV:NOVAROCKS_NATIVE_SHARED_SECRET}"

[cluster]
role = "be"
advertise_host = "10.0.0.11"

[connector.object_store]
endpoint = "http://10.0.0.20:9000"
access_key_id = "${ENV:AWS_S3_ACCESS_KEY_ID}"
access_key_secret = "${ENV:AWS_S3_SECRET_ACCESS_KEY}"
enable_path_style_access = true
```

启动 BE：

```bash
NO_PROXY=127.0.0.1,localhost \
./target/release/novarocks standalone --role be --config ./be-1.toml
```

启动成功后会输出：

```text
NOVAROCKS_READY role=be grpc_port=9080 advertise_host=10.0.0.11 pid=<pid>
```

`role=be` 不提供 MySQL 端口，`--port` 参数对 BE 无效。

`[connector.object_store]` 是 native connector 读取 Iceberg/S3 数据时使用的
BE 本地启动配置。所有参与同一集群的 BE 必须使用同一组值；native fragment
只携带文件、split 和 catalog 标识，不会携带 endpoint 或凭据。运行期通过 SQL
创建但只存在于 FE 内存中的 catalog 配置不能作为 distributed native read 的
凭据来源。

Secret-bearing startup scalars accept literals or only exact `${ENV:VAR}` references. Every
FE and BE resolves its own startup snapshot once; changing a secret requires restarting the
affected process. Credentials never enter native fragments or FE-to-BE transport.

## 配置 FE 节点

FE 节点启动 `server.grpc_port` 接收 BE coordinator report 和 authenticated backend
announce，并启动独立 `server.http_port` 暴露 FE-scoped metrics 和 lifecycle observation。
Native listener 不承载 management HTTP。FE 的 catalog source 必须显式选择；StaticFile 可配合
可丢弃的本地 SQLite Accelerator carrier，DynamicStateStore 则使用 StateStore 作为 catalog
authority。无论哪种 mode，StateStore 都不持久化 backend membership：BE 由外部 orchestrator
创建，并向 FE self-register。

示例 `fe.toml`：

```toml
log_level = "info"

[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/fe-state-store.sqlite"

[catalog_source]
# This distributed example creates catalogs through SQL.
mode = "dynamic-state-store"

[server]
host = "0.0.0.0"
grpc_port = 9080
http_port = 8040

[native_trust]
deployment_id = "analytics-prod"
shared_secret = "${ENV:NOVAROCKS_NATIVE_SHARED_SECRET}"

[standalone_server]
mysql_port = 9030
user = "root"

[connector.object_store]
endpoint = "http://10.0.0.20:9000"
access_key_id = "${ENV:AWS_S3_ACCESS_KEY_ID}"
access_key_secret = "${ENV:AWS_S3_SECRET_ACCESS_KEY}"
enable_path_style_access = true

[cluster]
role = "fe"
heartbeat_interval_ms = 1000
heartbeat_timeout_retries = 3
backend_announce_lease_ttl_ms = 5000
```

`[catalog_source]` 是 catalog desired-state 的唯一 authority。上例的 `dynamic-state-store`
使 `[state_store]` 成为该 authority 的 durable carrier；StaticFile deployment 仍可配置 SQLite
作为可重建 Accelerator carrier，但必须只从 static snapshot 读取 catalog truth。membership 只是
可重建的内存投影：FE 重启后由仍在运行的 BE renew announce 重建。不得添加第二套 metadata store、seed
或内存 fallback。持久用户表属于 external Iceberg catalog；`[connector.object_store]` 只提供
connector execution 的进程本地凭据。

启动 FE：

```bash
NO_PROXY=127.0.0.1,localhost \
./target/release/novarocks standalone --role fe --config ./fe.toml
```

启动成功后会输出：

```text
NOVAROCKS_READY mysql_port=9030 pid=<pid>
```

当前 MySQL 入口绑定在 `127.0.0.1`。如果需要远程访问，请在 FE 节点上使用 SSH tunnel、反向代理或本机客户端连接。

## Native trust 与传输选择

每个 deployable FE/BE role 都必须配置相同的 `[native_trust]`。它要求
`deployment_id` 和至少 32 bytes 的 shared secret；secret 推荐以
`openssl rand -base64 32` 生成，并通过每台主机受保护的
`NOVAROCKS_NATIVE_SHARED_SECRET` 环境变量供 Server 在启动时解析。不要将 production
secret 提交到 TOML、shell history 或日志。

未写 `[native_trust.transport]` 时是 **authenticated h2c**：每个 Native RPC 都必须有
短期 HS256 deployment JWT，但 protobuf body 仍是 plaintext。它只适用于明确可信、没有
被动监听和主动中间人的内部网络。需要保密性、transport integrity 或 server endpoint
cryptographic identity 时，所有 role 必须一起切换到 `automatic` 或 `pem` TLS 1.3。精确
TLS profile、证书要求、DNS/IP identity、轮换和故障 runbook 见
[Native trust、JWT 与可选 TLS](native-trust.md)。MySQL 与 management HTTP 不受
`[native_trust]` 保护。

## 验证集群

连接 FE：

```bash
mysql -h 127.0.0.1 -P 9030 -uroot
```

查看后端：

```sql
SHOW BACKENDS;
```

期望看到每个 BE 的 `Host`、`GrpcPort`、`State`、`Alive` 等字段。至少应有一个 BE 处于可用状态后再执行查询。

执行最小查询：

```sql
SELECT 1;
```

如果集群连接了 external Iceberg catalog，再执行一条真实表查询，确认 FE 调度、BE 执行和外部存储访问均可用。

## 配置与管理 BE

每个 BE 使用自己的 deployable config，并指向同一个 FE Native endpoint：

```toml
[cluster]
role = "be"
frontend_endpoint = "fe.native.example:9080"
backend_announce_interval_ms = 1000
backend_announce_initial_backoff_ms = 100
backend_announce_max_backoff_ms = 2000
```

BE 启动后创建新的 process identity，立即通过同一受 NWT-3 保护的 Native listener announce；FE 随后反向 heartbeat 该 endpoint，二者 descriptor 精确一致才调度新查询。`SHOW BACKENDS` 只读展示 `ProcessId`、lease、identity verification、reported state、compatibility 和 derived `Eligible`。`ADD BACKEND`、`DROP BACKEND`、`[cluster].backends` 均不是产品接口。

## 启停顺序

推荐启动顺序：

1. 启动对象存储、catalog、HDFS 等外部依赖。
2. 启动 FE 节点，等待 `NOVAROCKS_READY mysql_port=...`。
3. 启动所有 BE 节点，等待 `NOVAROCKS_READY role=be` 及 `SHOW BACKENDS` 中 `Eligible=true`。
4. 连接 FE 并执行 `SHOW BACKENDS`。
5. 执行最小查询和一条真实数据查询。

推荐停止顺序：

1. 在 LB/Gateway 中 external deactivate 旧 FE，先停止把新连接路由到它。
2. 对旧 FE 发送 `SIGTERM`。它会立即拒绝新 statement/background work，保留已准入 attempt 最多
   300 秒；management `/livez` 在 drain 中仍为 200，`/readyz` 变为 503。
3. 为 Pod 配置 `terminationGracePeriodSeconds: 360`：300 秒 drain + 30 秒 cleanup，外加 30 秒
   orchestrator margin。不要用短于该总预算的 preStop sleep 代替本地 drain。
4. FE 退出后再停止其 BE，或由外部 orchestrator 按 BE 自己的 drain 协议处理。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| `SHOW BACKENDS` 为空 | 确认 BE 已启动、`frontend_endpoint` 指向 FE Native listener，且所有 role 的 Native trust 配置一致。 |
| BE 一直不 Eligible | 确认 FE 能访问 BE 的 advertised endpoint，并检查 announce、heartbeat、process identity 和 build diagnostics。 |
| `Unauthenticated` 或 native trust startup failure | 检查每个 FE/BE 的 `deployment_id`、environment-resolved secret 与 transport mode 完全一致；不要为恢复连接而删除 `[native_trust]`。 |
| TLS handshake / certificate failure | 所有 role 必须使用同一 TLS mode；检查 advertised IP/DNS reference 与 certificate SAN，PEM mode 还要检查显式 trust roots。 |
| 查询报 `role=fe: no live backend available` | 当前 FE 没有可调度的 live BE；先恢复或注册 BE。 |
| FE 启动时提示缺少 catalog source | StaticFile mode 必须提供可读、完整的 snapshot file；DynamicStateStore mode 必须配置 `[state_store]`。不要使用 core metadata 或内存 registry 作为 fallback。 |
| BE 启动时配置校验失败 | `role=be` 必须配置 `[cluster].frontend_endpoint`，且不能配置 FE heartbeat 或 lease 设置。 |
| Native 或 management endpoint 冲突 | 让 FE MySQL、FE Native gRPC、FE management HTTP、BE Native gRPC、BE management HTTP 使用不重叠的 bind endpoint；同时检查 wildcard bind。 |
| `/metrics` 在 gRPC port 不可用 | 改访问对应 role 的 `[server].http_port`；metrics 使用 role-local registry，不会跨 FE/BE 混合。 |
