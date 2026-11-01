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

# all-in-one 本地部署

all-in-one 是 `novarocks-server` 的本地组合启动方式：一个进程并发监督完整的
FE 与 BE role runner。它不是第三种 application role，也不提供单配置或 direct-call
快捷路径。它适合本地试用、功能验证和 SQL 回归；生产拓扑仍使用独立 FE 与 BE。

## 编译

```bash
cargo build -p novarocks-server
```

## 准备一对角色配置

从仓库根目录的 `novarocks-fe.toml.example` 和
`novarocks-be.toml.example` 开始。两份 deployable TOML 都必须显式写入
`[cluster].role`：前者是 `fe`，后者是 `be`。

FE 配置必须包括：

- FE 客户端端口 `[standalone_server].mysql_port` 与 `trino_port`；
- FE Native gRPC 的 `[server].grpc_port`；
- FE management HTTP 的 `[server].http_port`；
- 一个显式 `[catalog_source]`：静态部署使用随配置挂载的 snapshot；只有需要 SQL
  `CREATE/DROP CATALOG` 时才选择 `dynamic-state-store` 并配置 `[state_store]`；
- 可选 `[state_store]` Accelerator carrier（SQLite `emptyDir` 可以随 Pod 删除；它不是 StaticFile
  catalog truth）；
- 与 BE 相同的 mandatory `[native_trust]` deployment id、shared secret 和 transport mode；
- BE 会向 FE Native gRPC endpoint 自注册；`[cluster].backends` 不是 deployment 配置。

Trino 入口的 FE 配置如下，省略时分别使用 `8080` 与 `300000`：

```toml
[standalone_server]
mysql_port = 9030
trino_port = 8080
trino_client_timeout_ms = 300000
user = "root"
```

`trino_client_timeout_ms` 是客户端两次查询请求之间的放弃期限，默认五分钟；针对该查询的 `HEAD` 也刷新期限。
它不设置查询执行期限。每次 HTTP 响应另有固定 30 秒写出期限，写出失败或到期保留原页供重试；CLI 会处理分页与最后一页确认。
Trino 与 MySQL 都固定绑定 `127.0.0.1`；`[server].host` 不扩展客户端入口的监听地址。

BE 配置必须包括 Data 的 `[server].grpc_port`、显式非零 Control 的
`[server].control_grpc_port` 与 `[server].http_port`，三个端口必须独立；Control 不从
Data 端口推导。BE 使用 `[cluster].frontend_endpoint` 向 FE 原有 Native endpoint announce。
每次 announce 的完整 descriptor 都包含 Data 与 Control endpoint，FE 对准确 Control
endpoint 的 heartbeat 必须返回完全相同的 descriptor，才允许新查询调度。

Data 与 Control 使用同一个准确 advertised host；可选 `[cluster].advertise_control_port`
单独指定 Control 外部端口，未配置时等于显式的 Control bind 端口。若本地也使用 NAT，
两条端口映射必须分别正确。BE 还必须配置本地 connector object-store binding。Paimon 读取恢复后，FE 与 BE 的 binding
仍须指向同一 warehouse 访问域。两份配置在同一进程共享 logging 与 data-runtime
sizing，其他 role-local 字段各自生效。

两份正常角色配置都可包含 `[runtime.native_ingress]`：FE 只使用
`worker_threads`、`max_blocking_threads` 调整自己的 Native report listener；BE
使用同节的普通/控制运行及等待资格、消息界和控制执行器参数。all-in-one 沿用
正常 role-local listener：FE report 一条，BE Data/Control 两条，没有 all-in-one
专用的入口容量或 FE 整集群 Task 配额。
完整键与默认值见 [分布式部署](distributed.md#配置-be-节点)。

`all-in-one` 不共享或绕过 Native trust：Server 仍分别为 FE 与 BE 构造 role-scoped
trust snapshot，并在任何 listener 或 outbound connect 前拒绝两份配置的 deployment id、secret
或 transport mode 不一致。省略 `[native_trust.transport]` 是 authenticated plaintext h2c；若
启用 `automatic` 或 `pem`，两份配置必须一起使用同一 TLS 1.3 profile，JWT 仍为 mandatory。
参阅 [Native trust、JWT 与可选 TLS](native-trust.md)。

同一 address family 内，任意两个 listener 不能重叠：相同地址/端口冲突，wildcard
地址也与同端口具体地址冲突。启动会在 logging、runtime、StateStore 或 listener
创建之前拒绝冲突和不兼容的 process-owned 配置。

## 启动

```bash
NO_PROXY=127.0.0.1,localhost \
./target/debug/novarocks standalone --role all-in-one \
  --fe-config ./novarocks-fe.toml \
  --be-config ./novarocks-be.toml
```

任一 role runner 返回时，supervisor 会请求另一侧 shutdown、等待双方完成清理，并
保留 primary error。BE 两条 advertised endpoint 的启动就绪检查成功后会打印：

```text
NOVAROCKS_READY role=be grpc_port=9081 control_grpc_port=9082 advertise_host=127.0.0.1 pid=<pid>
```

FE 在两个客户端监听绑定后另行打印就绪 marker；具体端口取自本次实际绑定：

```text
NOVAROCKS_READY mysql_port=9030 pid=<pid> trino_port=8080
```

看到 marker 后再连接客户端。Trino CLI 使用当前 NovaRocks SQL 方言：

```bash
trino --server http://127.0.0.1:8080 --user root --execute 'SELECT 1'
mysql -h 127.0.0.1 -P 9030 -uroot
```

Trino HTTP 只接受 Host 为 `localhost`、`127.0.0.1` 或 `[::1]` 的请求（可带数字端口），其他名称返回 403。
`X-Trino-User` 必须与配置 `user` 相同，不匹配返回 401。该入口提供本机单用户访问，没有客户端 TLS 或非空认证；
Native trust 只保护 FE/BE 的 Native 通道。`--catalog` 与 `--schema` 由 CLI 放入每条语句的请求头，`USE` / `SET` 的会话变化随响应回写。

Trino 的 SQL 方言、时间语义、元数据查询、预编译语句和多语句事务没有因采用客户端协议而扩展。
结果中的宽整数、无符号整数、opaque 与精度大于 38 的 decimal 在 Trino 呈现规划期返回不支持的具名错误。
DDL 与会话语句返回 `updateType`；没有真实逻辑写计数时不返回 `updateCount`。

## 五个 Native／management listener surface

| Role | Native listener | Management listener |
| --- | --- | --- |
| FE | `[server].grpc_port`：coordinator-report 与 authenticated announce | `[server].http_port`：FE-scoped metrics 与现有 gated lifecycle debug |
| BE | `[server].grpc_port`：Data；`[server].control_grpc_port`：Control | `[server].http_port`：BE-scoped metrics |

BE Data 接受 task submission、status/result、普通 split/domain updates、catalog prune 和 peer
exchange/runtime-filter RPC；Control 接受 `ApplyTaskControlOperations` 与 `Heartbeat`。
错误域在认证后、读取 body 与调用 handler 前拒绝，不跨 listener 转发；BE→FE
`AnnounceBackend` 仍使用 FE 原有 Native endpoint。

BE 两条 listener 各有自己的 Tokio runtime，Control 使用 `control_worker_threads`。
Data 32 与 Control 8 个 acquisition positions 不互借，2 秒握手期限覆盖 TLS/preface/
初始 SETTINGS；成功应用 peer SETTINGS 且完成实际 SETTINGS/ACK flush 才释放握手
位置，失败和取消等实际握手 owner 退出。Data 半开连接满位不会占用 Control 资格。
正常应用 stream 不受已完成握手的 2 秒期限约束。

两份 role 配置都在 bind 前检查文件描述符 soft limit：BE 至少 1024、FE 至少 2048，
所以这个组合启动方式也需要满足 FE 基线。检查只查询 `getrlimit(RLIMIT_NOFILE)`，不
修改或预留 FD。BE 的 Native socket stock 518 Data + 20 Control + 2 listeners + 2 refusal
transients = 542，1024 基线余量为 482；它不证明其他文件或 Connector 已预留容量。
这些 listener 和资格也不宣称完整 Native owner graph 或 bounded-root/V1 支持。

Native listener 不安装 management HTTP route；management listener 不承载 Native
gRPC service。metrics 也按 role-local registry 收集，因此同进程的 all-in-one 不会
把 FE 与 BE metrics 混在一个 endpoint。

Native Task 接收状态应从 BE management listener 的 `/metrics` 或
`/metrics?type=json` 读取。`novarocks_backend_native_ingress_slots` 按 ordinary/control
与 running/waiting 报当前占用和上限；`novarocks_backend_worker_context_reservations`
是独立的 Worker Context 门。`novarocks_backend_saturation_source_available` 中尚未
接入的 Exchange slot、内存账本读数表示 unavailable，不表示空闲。各读数及
async/blocking/registry 排队归因见 [分布式部署](distributed.md#native-入口容量与观测)。

## 本地 Iceberg REST 环境

```bash
docker/fixture-inputs/provision.sh
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" \
  --be-config "$NOVAROCKS_BE_CONFIG"
```

该 fixture 为当前工作区生成一对正常 FE/BE 配置，以及 FE Native/management、BE
Data/Control/management 五个不冲突 listener port，并另外配置 MySQL 与 Trino 客户端端口。Trino 端口通过 `NOVA_ENV_TRINO_PORT` 导出；不要
猜测端口或重用旧的 standalone config 环境变量。

当前 Server 的封闭 provider manifest 保留 Iceberg 与 Paimon。Paimon catalog 配置与
投影保留，目录和读取操作返回 `Unsupported`，不进入 pure catalogue。恢复须等待
paimon-rust 及其依赖支持 Arrow 60 与 zstd 0.14，并重新满足 ADR-0138 的授权读取边界；
见 [ADR-0204](../../adr/ADR-0204-upstream-iceberg-backports-and-provider-suspension.md) 与
[Paimon Connector（读取暂停）](../connectors/paimon.md) 中保留的历史 fixture 和支持矩阵。旧
`[connector.starrocks]` 配置会在解析阶段明确失败。

## 停止与排障

前台运行时按 `Ctrl-C`。后台运行时记录 PID 并优先发送 `SIGTERM`。FE 收到信号后先
停止新 workload 准入，等待已准入工作最多 300 秒，再最多用 30 秒完成 teardown；all-in-one
会先完成 FE drain，之后才停止本地 BE。

| 现象 | 处理方式 |
| --- | --- |
| 启动前报 endpoint overlap | 为 FE MySQL、FE Trino、FE Native、BE Data、BE Control 与两侧 management HTTP 分配不同端口；也检查 wildcard bind。 |
| 启动前报 process configuration mismatch | 两份配置的 logging 与 data-runtime sizing 必须相同。 |
| FE 提示缺少 StaticFile snapshot | 复制 `novarocks-catalogs.toml.example` 到 FE config 同目录，并使 `[catalog_source].static_file_path` 指向它；文件缺失不会降级为空 snapshot。 |
| FE 提示 DynamicStateStore 缺少 StateStore | 仅在 `[catalog_source].mode = "dynamic-state-store"` 时配置 durable `[state_store]`；不要以 transient/in-memory membership 替代。 |
| native trust preflight failure | 使 FE/BE 的 `deployment_id`、resolved shared secret 与 transport mode 完全相同；完整 secret rotation 只能 homogeneous restart。 |
| 缺少 BE Control 配置或 BE 不 Eligible | 显式填写 `[server].control_grpc_port`，核对两条 advertised endpoint 可达；FE Heartbeat 使用 Control，完整 descriptor 必须一致。 |
| 启动拒绝 Native descriptor baseline | 在进程启动环境使 soft FD limit 至少 2048；Server 不会自动修改限制。 |
| Native RPC `Unauthenticated` 或 TLS handshake failure | 检查 secret/env、token clock 和双方 TLS profile；不要降级或删除 JWT 来绕过失败。 |
| 访问 `/metrics` 得到 Native 协议错误或 404 | 改访问对应 role 的 management HTTP port，而不是 Native gRPC port。 |
| 连接 MySQL 失败 | 等待 `NOVAROCKS_READY`，然后确认 FE 的 `[standalone_server].mysql_port`。 |
| 连接 Trino 失败或返回 403/401 | 等待 marker 中的实际 `trino_port`；使用本机名称，核对 CLI 的 `--user` 与 `[standalone_server].user`。 |
| Trino 查询返回 `ABANDONED_QUERY` | 客户端超过 `trino_client_timeout_ms` 没有请求该查询；检查分页消费或查询心跳，区别于原查询执行期限。 |
