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

分布式部署使用 NovaRocks native 角色能力，将协调节点和计算节点拆分为不同进程。FE 角色提供 Trino HTTP 与 MySQL 客户端入口、SQL 解析、优化和 fragment 调度；BE 角色通过独立的 Native Data 与 Control gRPC listener 执行 fragment 和处理控制请求。

该模式不依赖 StarRocks FE。当前 Server 的封闭 provider manifest 保留 Iceberg 与 Paimon；Iceberg 保留现有读写能力。Paimon 保留注册、catalog 配置与投影，目录和读取操作返回 `Unsupported`，不进入 pure catalogue。恢复须等待 paimon-rust 及其依赖支持 Arrow 60 与 zstd 0.14，并重新满足 ADR-0138 的授权读取边界，见 [ADR-0204](../../adr/ADR-0204-upstream-iceberg-backports-and-provider-suspension.md)。StarRocks 已废弃，没有 active read capability，旧 `[connector.starrocks]` 配置会在启动解析阶段明确失败。

## 部署拓扑

典型拓扑如下：

```text
Trino / MySQL client
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
| `fe` | 接收客户端语句，解析 SQL，优化计划，调度 fragment 到后端 | Trino HTTP：`[standalone_server].trino_port`；MySQL：`[standalone_server].mysql_port`；Native coordinator-report gRPC：`[server].grpc_port`；FE management HTTP：`[server].http_port` |
| `be` | 执行 FE 下发的 fragment，处理 exchange、结果回传和 lifecycle control | Native Data gRPC：`[server].grpc_port`；Native Control gRPC：`[server].control_grpc_port`；BE management HTTP：`[server].http_port` |

## 前提条件

- FE 节点和所有 BE 节点使用同一版本的 NovaRocks。
- FE 节点可以访问每个 BE 的 advertised Data 与 Control endpoint；BE 之间可以访问对方的 advertised Data endpoint。
- BE 节点可以访问 FE 原有的 `grpc_port`，用于 announce 与 coordinator report；FE 不为这些请求新增 BE Control listener。
- 所有 FE/BE 配置必须有完全相同的 `[native_trust].deployment_id`、shared
  secret 与 transport mode；Native JWT 是 mandatory，TLS 只是在其上增加的可选层。
- 所有 BE 节点都能访问相同的数据源、对象存储和 catalog。
- 如果使用静态对象存储绑定，相关 FE/BE 节点须配置覆盖各自用途的访问域、endpoint 和 path-style；REST Catalog 的 vended 执行材料由实际签请求的 BE 按需取得：
  BE 用 catalog 定义中 `DataCredentialVending` 绑定所指的本地身份认证，按 FE 冻结的公开路由上下文（完整请求地址与公开路由请求头）发出请求，因此每个 BE 都须能访问 FE 会话实际使用的 REST 地址（含 `/v1/config` 的 `overrides.uri`）与服务端给出的凭证端点，且不经重定向。
- `role=fe` 必须显式选择 `[catalog_source]`。StaticFile 从一份挂载的完整 snapshot 启动；
  DynamicStateStore 才要求可用的 SQLite `[state_store]` 并允许 SQL catalog mutation。StateStore
  不是 backend membership source。backend desired lifecycle 属于外部 orchestrator，FE 的 observed
  registry 会由 BE announce 和 FE-pull heartbeat 在重启后重建。SQLite 只适用于恰好一个 active FE；多 FE
  fencing/takeover 尚未实现，不能由 StateStore SPI 或 SQLite 配置字段推断。

## 编译 NovaRocks

分布式部署使用 NovaRocks native runtime。FE 角色和 BE 角色使用同一个 NovaRocks 二进制文件，且必须包含一致的 Iceberg/Paimon 私有 descriptor 与 revision；建议构建同一 release 二进制后分发到所有节点。

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

BE 必须显式配置非零的 `[server].control_grpc_port`，并使它与 Data 的
`[server].grpc_port`、management 的 `[server].http_port` 不同。Control 端口不会由
Data 端口加一或缺省值推导。两条 Native listener 不提供 management route，management
listener 也不提供 Native gRPC service。同一 address family 内不能让任意 listener
复用相同 bind endpoint，wildcard bind 也会与同端口具体地址冲突。

Data 与 Control 共用准确的 advertised reference host，并使用独立端口。
`[cluster].advertise_port` 指定 Data 的外部端口；可选的
`[cluster].advertise_control_port` 指定 Control 的外部端口，未配置时使用显式的
`[server].control_grpc_port`。若部署使用 NAT 或端口映射，两个外部端口都必须映射到
各自 listener，且 advertised Data 与 Control endpoint 必须不同。TLS 身份及防火墙规则
必须同时覆盖这两条连接路径。

示例 `be-1.toml`：

```toml
log_level = "info"

[server]
host = "0.0.0.0"
grpc_port = 9080
control_grpc_port = 9081
http_port = 8040
# Keep this aligned with terminationGracePeriodSeconds: 360 below.
frontend_drain_timeout_ms = 300000
frontend_cleanup_timeout_ms = 30000

[native_trust]
deployment_id = "analytics-prod"
shared_secret = "${ENV:NOVAROCKS_NATIVE_SHARED_SECRET}"

[cluster]
role = "be"
frontend_endpoint = "fe.native.example:9080"
advertise_host = "10.0.0.11"
# For NAT, set the exact externally reachable ports separately.
# advertise_port = 29080
# advertise_control_port = 29081

[runtime.native_ingress]
worker_threads = 8
max_blocking_threads = 64
ordinary_running = 8
ordinary_waiting = 8
control_worker_threads = 4
control_running = 4
control_waiting = 4
ordinary_request_max_bytes = 67108864
ordinary_response_max_bytes = 67108864
control_request_max_bytes = 1048576
control_response_max_bytes = 67108864

[connector.object_store]
endpoint = "http://10.0.0.20:9000"
access_key_id = "${ENV:AWS_S3_ACCESS_KEY_ID}"
access_key_secret = "${ENV:AWS_S3_SECRET_ACCESS_KEY}"
enable_path_style_access = true
```

`[runtime.native_ingress]` 的 BE 容量按 listener 生效，不是 FE 整集群查询配额。
Data listener 使用 `worker_threads` 与 ordinary 运行/等待资格；Control listener 有自己的
线程、Tokio runtime、`control_worker_threads` 与 control 运行/等待资格。后者的小请求界
和控制执行器不会排进 Data listener 的运行队列。

RPC endpoint 由唯一的 Native method manifest 决定，认证后在读取 request body 和调用
handler 前拒绝错误域，不转发到另一条 listener：

| BE endpoint | 接受的方法 |
| --- | --- |
| Data | `ApplyTaskOperations`、`SubscribeTaskStatus`、`FetchTaskResult`、`FetchTaskDynamicFilters`、`GetFinalTaskInfo`、`PruneCatalogs`、`ExchangeUnary`、`TransmitRuntimeFilterEnvelope` |
| Control | `ApplyTaskControlOperations`、`Heartbeat` |

`AnnounceBackend` 发到 FE 原有 Native endpoint；已退役的 `FetchResult` 和 `Exchange`
不因换一个端口而恢复。FE 每个已准入 attempt 冻结完整 BE descriptor，再按具体 RPC method
选择其中的 Data 或 Control endpoint，不重新查询 membership，也不在 Control 缺失时拨 Data。
消息尺寸门位于 protobuf 对象构造之前，Worker 的 Context reservation 仍是另一道
独立门。等待资格可以设为零；`ordinary_running` 不得超过
`max_blocking_threads`，`control_running` 不得超过 `control_worker_threads`，
control 请求/响应上限也不得超过各自 ordinary 上限。Server 在启动前验证这些关系。
上述默认组合的帧尺寸乘积估算是普通类 2048 MiB、控制类 520 MiB；它用于审查配置，
不是进程 RSS 硬上限，具体负载的驻留和放大仍需观测。

启动 BE：

```bash
NO_PROXY=127.0.0.1,localhost \
./target/release/novarocks standalone --role be --config ./be-1.toml
```

启动成功后会输出：

```text
NOVAROCKS_READY role=be grpc_port=9080 control_grpc_port=9081 advertise_host=10.0.0.11 pid=<pid>
```

BE readiness marker 在两条 advertised endpoint 的启动就绪检查成功后发布；其中两个
`*_grpc_port` 是本地 bind 端口，不能替代 NAT 后的 advertised port。`role=be` 不提供
MySQL 端口，`--port` 参数对 BE 无效。

`[connector.object_store]` 是 native connector 当前读取 Iceberg/S3 数据时使用的
role-local 静态启动配置。静态绑定须使 FE metadata 用途和 BE execution 用途分别获得
覆盖所需资源的访问权；这不要求两种用途共享 secret。REST Catalog 的 vended 材料
由签请求的 BE 本地 `StorageAuthority` 按需取得和续期。FE 随凭证租约公告取得路径：
两条路径（凭证端点、load-table）都携带 FE 从自己唯一的 catalog 会话冻结的公开路由上下文，
即完整请求地址与公开路由请求头（catalog 定义的 `header.*`，以及
`iceberg.rest.server-routing-headers` 声明过的服务端下发请求头），load-table 路径另带表身份与
期望的表 UUID；其中从不包含令牌、client secret 或任何其他认证材料。BE 用自己的
`DataCredentialVending` 身份认证，不建立自己的 catalog 会话，也不请求 `/v1/config`。
FE 无法形成路由上下文时（缺少 `DataCredentialVending` 绑定、服务端下发了未声明的请求头、
超出数量或字节上限）在派发之前拒绝查询。native fragment 只携带文件、
split 和 catalog 标识，不携带凭据；不能把只存在于 FE 内存中的
catalog 配置或 FE metadata secret 当作 BE 的执行材料。见
[ADR-0151](../../adr/ADR-0151-credential-renewal-is-driven-by-the-consumer.md) 与
[ADR-0175](../../adr/ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md)。

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

[runtime.frontend_constant_policy]
# Explicit author limits; these are not allocation grants or inferred defaults.
max_rows = 1048576
max_array_nodes = 1048576
max_logical_elements = 16777216
max_retained_buffer_bytes = 1073741824
max_type_depth = 64
max_type_nodes = 4096
max_dictionary_depth = 64
max_metadata_bytes = 1048576
max_library_validation_work = 1073741824
max_library_validation_bytes = 4294967296

[runtime.native_ingress]
worker_threads = 8
max_blocking_threads = 64

[standalone_server]
mysql_port = 9030
trino_port = 8080
trino_client_timeout_ms = 300000
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

FE 必须显式填写 `[runtime.frontend_constant_policy]` 的全部十项限额。示例值是可调整的部署配置；这些维度分别限制常量行数、数组结构、逻辑元素、保留字节、类型、字典、metadata 和库验证工作，不代表内存 grant。BE 不需要此 FE 作者配置。

FE 的 `[runtime.native_ingress]` 只设置本角色 Native report listener 的 async worker
与 blocking pool 大小；Task ordinary/control 资格和消息界只装配在 BE listener。
两种 role 使用同一正常配置模型，不能把 FE 的这些字段当成新的整集群查询配额。

`[catalog_source]` 是 catalog desired-state 的唯一 authority。上例的 `dynamic-state-store`
使 `[state_store]` 成为该 authority 的 durable carrier；StaticFile deployment 仍可配置 SQLite
作为可重建 Accelerator carrier，但必须只从 static snapshot 读取 catalog truth。membership 只是
可重建的内存投影：FE 重启后由仍在运行的 BE renew announce 重建。不得添加第二套 metadata store、seed
或内存 fallback。持久用户表属于 external Iceberg 或 Paimon catalog；Iceberg 当前支持
native 读写，Paimon 保留 catalog 配置与投影，目录和读取操作已暂停。`[connector.object_store]` 只提供 connector execution 的进程本地凭据。

启动 FE：

```bash
NO_PROXY=127.0.0.1,localhost \
./target/release/novarocks standalone --role fe --config ./fe.toml
```

启动成功后会输出：

```text
NOVAROCKS_READY mysql_port=9030 pid=<pid> trino_port=8080
```

FE 的 Trino 与 MySQL 客户端入口固定绑定 `127.0.0.1`，不读取 `[server].host` 扩展监听地址。
Trino 的 `trino_port` 默认 `8080`，复用 `[standalone_server].user`（默认 `root`）；`trino_client_timeout_ms` 默认 `300000`，
约束两次针对同一查询的客户端请求间隔，`HEAD nextUri` 也刷新它。单个 HTTP 响应另有 30 秒写出期限；期限到达只中止本次响应，原页可按同 token 重试。
查询执行继续使用原查询期限与取消。Trino 端口应与 FE 的 MySQL、Native gRPC 和 management HTTP 端口分别配置。

Trino 请求的 Host 必须是 `localhost`、`127.0.0.1` 或 `[::1]`（可带数字端口），否则返回 403；语句请求的 `X-Trino-User`
必须等于配置用户，否则返回 401。生成 `nextUri` 时保留本机名称及显式端口，并将数字端口规范化。
该入口没有客户端 TLS 或非空认证；Native trust 不保护客户端 HTTP。如果从其他主机使用客户端，可通过 SSH tunnel 连接 FE 本机入口：

```bash
ssh -L 18080:127.0.0.1:8080 user@fe-host
```

随后客户端连接 `http://127.0.0.1:18080`，请求 Host 仍为允许的本机名称；远端身份与通道保护由 SSH 提供。

## Native 握手资格与文件描述符基线

BE 进程的 Native acquisition 资格按 Data 32、Control 8 独立计数，同类的实际
accepted/dial 路径使用同一资格来源，两个类别不互借。listener 在应用 clone、connection
任务和 TLS/H2 I/O 前取得资格；不足时关闭刚 accepted 的连接，不排一个无界握手队列。
从 accept 开始的 2 秒绝对期限覆盖 TLS、preface 和初始 SETTINGS。只有 peer SETTINGS
已成功校验并应用、初始本地 SETTINGS 与 ACK 已实际 flush 后才归还握手资格；失败或取消
等待实际握手 owner 退出。成功后正常应用 stream 不再受这条握手期限约束。

独立 listener/runtime 与 Control 8 个位置，使 Data 半开连接耗尽其 32 个握手位置时不会
占用 Control 握手资格。这不是所有 CPU、socket、TLS、stream、缓存及外部 Connector
owner 的完整资源隔离证明，也不代表已经发布 bounded-root/V1 支持。

启动在 bind 前只通过 `getrlimit(RLIMIT_NOFILE)` 查询 soft/hard limit：BE 的 soft limit
至少 1024，FE 至少 2048。不满足时启动失败；NovaRocks 不调用 `setrlimit` 自动提高限制。
部署者应在进程启动环境或服务管理器中设置符合基线的限制。

当前 BE Native 物理 stock 算术是 Data 518 + Control 20 + 2 listener + 2 accepted refusal
transient，共 542 个 socket positions；1024 基线留下 482 的算术余量。查询和比较本身不
预留这些 FD，也不证明 Connector、普通文件、OS scheduler 或其他进程内使用者已取得
独立 FD headroom。FE 的 2048 是本角色 operational baseline，不由 BE 的 542 推导。

## Native 入口容量与观测

BE management HTTP 的 `/metrics` 或 `/metrics?type=json` 提供当前 Native 门的具名读数。
`novarocks_backend_native_ingress_slots{class,phase,dimension}` 可比较 ordinary/control
的 running/waiting `used` 与 `limit`；`novarocks_backend_native_response_backings`
显示仍持有资格的 body/DATA backing，
`novarocks_backend_worker_context_reservations` 显示另一道 Worker 门的已用量与上限。
`novarocks_backend_native_ingress_oldest_wait_since_unixtime_seconds`、
`novarocks_backend_native_ingress_last_progress_unixtime_seconds` 与拒绝/等待累计计数辅助
判断门是否在推进。`novarocks_backend_native_async_first_poll_lag_seconds`、
`novarocks_backend_native_blocking_queue_wait_seconds`、
`novarocks_backend_native_control_queue_wait_seconds` 和
`novarocks_backend_worker_registry_lock_observation` 分别帮助区分 async 首次调度、
普通 blocking 排队、控制执行器排队与 Worker registry 锁等待/持有。
`novarocks_backend_task_preparation_snapshot_available` 为 1 时，本次 scrape 含有
准确 preparation ledger；Worker registry 正忙或没有采样 owner 时为 0，本次响应
省略 preparation 数值，不能将它解释成零占用或沿用上次读数。采样不等待 registry
锁，因此持锁期间仍能观察真实 Control 执行器进展；registry poison 明确使 scrape
失败。其他 owner 的采样仍有各自的同步边界。
`novarocks_backend_saturation_source_available` 对尚未接入此读数面的 Exchange slot
与内存账本报告 unavailable（值为零），不能把它解释成这些资源空闲。

## 内存分配归属与残留诊断

每个 BE 的上述 management 端口导出进程本地内存读数；不要从一个 BE 推断全体 BE，FE 与 BE 的 registry 也不混合。Server 全局 allocator 已使用尺寸分段归属包装器，生产 query/R1 的资金接线尚未完成，当前 query=0 不能证明查询没有内存或已受硬限保护。

| 指标 | 口径与有界标签 |
|---|---|
| `novarocks_backend_process_counted_live_bytes{band}` | small/tagged 的存活请求字节；tagged 含 8 B token |
| `novarocks_backend_process_counted_operations_total{band,kind}` | alloc/dealloc/realloc/failure 事件，跨段是一次 realloc |
| `novarocks_backend_process_counted_requested_bytes_total{band,flow}` | 请求字节流量；含完整块跨段迁移，不是只有 process delta |
| `novarocks_backend_memory_attributed_bytes{band,class}` | tagged/r1_small × query/residual/service 的 signed 独立事实采样 |
| `novarocks_backend_memory_unattributed_bytes` | 无有效 owner 的 tagged 请求 |
| `novarocks_backend_memory_ledger_blind_spot_bytes` | small 进程请求减 R1 small；普通环境小对象无查询来源 |
| `novarocks_backend_memory_attribution_reconcile_bytes` | tagged 进程请求减全部 tagged lane（含 unattributed） |
| `novarocks_backend_memory_lane_records{class,production}` | 四类责任 × producing/sealed/stopped 的记录数 |
| `novarocks_backend_memory_lane_record_capacity`, `_high_water`, `novarocks_backend_memory_lane_records_draining` | 固定记录容量、采样扫描前缀与待回收数 |
| `novarocks_backend_memory_lane_record_segment_requested_bytes`, `novarocks_backend_memory_observation_metadata_bytes` | segment 请求 backing 与本 authority 观测控制估计；不等于 resident，不加入 S1 的资金 C |
| `novarocks_backend_memory_batch_threshold_bytes`, `_pinned_slots`, `_slot_balance_estimate_bytes` | Q=1 MiB、独立采样 pins 与 Q×pins，排除在途项 |
| `novarocks_backend_memory_attribution_faults_total{kind}` | 七类固定故障计数；观测不拒绝 SQL |
| `novarocks_backend_memory_attribution_sample_unixtime_seconds`, `_sequence_sum` | 采样时间与独立序号的 wrapping sum；不是一致快照或结清收据 |

`band`、`class`、`production`、`kind`、`flow` 均为固定枚举，不增加 query/account/origin 标签。纯 small 路径不读 TLS，≥512 B 请求的尾部保存下标/代次；free 按原来源扣回。R1 allocator-api 零尺寸没有物理事实，tagged 与 R1 small 对同一块只发布一份。

作用域完成后每槽余额 <Q，但在途分配无先验上界，Q×sampled pins 不能证明瞬时物理峰值或已结清。signed 盲区/对账可以暂时为负，不能 clamp 为零；孤立的对账非零也不能判为缺陷。停止 lane 生产仍保留执行中 Query；真正 Work teardown 才转 Residual。U=ΣC_query 不包含来源诊断，root C/N 不因重分类下降，真实释放和闲置授权归还另计。RSS/cgroup/jemalloc 读数保持独立，不能和请求/责任事实相加。

以下 Prometheus 规则是故障提示示例，阈值与路由由部署 owner 决定；先按 instance 看原值、采样进展和退出证据。残留增长只记诊断，不能据此自动 kill 已退出查询。

```yaml
groups:
  - name: novarocks-memory-attribution
    rules:
      - alert: NovaRocksResidualMemoryGrowth
        expr: increase(novarocks_backend_memory_attribution_faults_total{kind="residual_growth"}[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Residual allocation growth on {{ $labels.instance }}"
      - alert: NovaRocksAttributionCoverageFailure
        expr: increase(novarocks_backend_memory_attribution_faults_total{kind=~"binding_failure|record_exhaustion|generation_exhaustion|scope_refusal"}[5m]) > 0
        labels:
          severity: warning
        annotations:
          summary: "Allocation attribution coverage degraded on {{ $labels.instance }}"
      - alert: NovaRocksAttributionLifetimeFault
        expr: increase(novarocks_backend_memory_attribution_faults_total{kind=~"orphan|reclaim_nonzero"}[5m]) > 0
        labels:
          severity: critical
        annotations:
          summary: "Allocation attribution lifetime fault on {{ $labels.instance }}"
```

unsafe 的地址、别名和生命周期要求继续由模型/Miri 与静态复核承担；指标可读、原生场景通过或 smoke 不代替它们。当前阶段仅维护功能与数据正确性，旧内存测量入口已退出，不作 Linux 性能达标结论。详见 [memory 核心边界](../development/memory-boundary.md) 与 [memory tests](../../../novarocks/memory/tests/README.md)。

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

在 FE 主机上使用 Trino CLI 连接，SQL 仍采用 NovaRocks 当前方言：

```bash
trino --server http://127.0.0.1:8080 --user root --execute 'SELECT 1'
```

`--catalog lake --schema sales` 可把 catalog/schema 名称带入每条语句；服务端只在实际使用名称时解析存在性。
CLI 按响应头应用 `USE` / `SET` 产生的会话变化。客户端必须沿 `nextUri` 消费至结束页：最后一个数据页仍带 `nextUri`，
下一次请求才确认它；重试同一 token 得到相同响应，`DELETE nextUri` 请求取消查询。

也可使用原 MySQL 入口：

```bash
mysql -h 127.0.0.1 -P 9030 -uroot
```

HTTP 入口观察可使用以下两项；它们不证明 BE 已经获得调度资格：

```bash
curl --noproxy '*' -fsS http://127.0.0.1:8080/v1/info
curl --noproxy '*' -fsSI http://127.0.0.1:8080/v1/statement
```

采用 Trino 客户端协议不扩展 SQL 方言、时间语义、元数据查询、预编译语句或多语句事务。
Trino 呈现对宽整数、无符号整数、opaque 与精度大于 38 的 decimal 在规划期具名拒绝；DDL 与会话语句提供 `updateType`，
没有真实逻辑写计数时不填 `updateCount`。

查看后端：

```sql
SHOW BACKENDS;
```

检查每个 BE 的 `ProcessId`、`Endpoint`、`IdentityVerified` 与 `Eligible` 等字段。`Endpoint` 展示 Data endpoint；至少应有一个 BE `Eligible=true` 后再执行查询。

执行最小查询：

```sql
SELECT 1;
```

如果集群连接了 external Iceberg catalog，再执行一条真实表查询，确认 FE 调度、BE 执行和外部存储访问均可用。配置了 Paimon catalog 时，确认 catalog 投影保留、目录及读取操作返回 `Unsupported`。Paimon 的恢复条件与恢复后的外部 GC/expiration 保留前提见 [Paimon Connector（读取暂停）](../connectors/paimon.md)。

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

BE 每次启动生成新的 UUIDv7 process identity，并向 `[cluster].frontend_endpoint` 指定的
FE 原有 Native gRPC listener announce。不可变 descriptor 同时携带准确 Data endpoint、
mandatory Control endpoint、deployment/build/compatibility 与 preparation positions；
BE advertised Control 不依赖可选的 bounded-root 支持字段。

FE 按该 descriptor 的 Control endpoint 反向 `Heartbeat`，并比较完整 descriptor。
Data 或 Control endpoint 漂移都不能通过 exact heartbeat verification；同一 process
identity 的不同 descriptor announce 也会拒绝。二者完全一致才调度新查询。
`SHOW BACKENDS` 只读展示 `ProcessId`、lease、identity verification、reported state、compatibility 和 derived `Eligible`。`ADD BACKEND`、`DROP BACKEND`、`[cluster].backends` 均不是产品接口。

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
   Trino 拒绝新的 `POST`，已登记但尚未执行的查询在首次 `GET` 时同样得到 `SERVER_SHUTTING_DOWN`；
   执行中的查询继续响应取页、取消与心跳，直到结算或原排空期限到达。
3. 为 Pod 配置 `terminationGracePeriodSeconds: 360`：300 秒 drain + 30 秒 cleanup，外加 30 秒
   orchestrator margin。不要用短于该总预算的 preStop sleep 代替本地 drain。
4. FE 退出后再停止其 BE，或由外部 orchestrator 按 BE 自己的 drain 协议处理。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| `SHOW BACKENDS` 为空 | 确认 BE 已启动、`frontend_endpoint` 指向 FE Native listener，且所有 role 的 Native trust 配置一致。 |
| BE 一直不 Eligible | 确认 FE 能访问 BE 的两个 advertised endpoint，特别是 Heartbeat 的 Control 端口；检查 announce 与完整 heartbeat descriptor、process identity 和 build diagnostics。 |
| `Unauthenticated` 或 native trust startup failure | 检查每个 FE/BE 的 `deployment_id`、environment-resolved secret 与 transport mode 完全一致；不要为恢复连接而删除 `[native_trust]`。 |
| TLS handshake / certificate failure | 所有 role 必须使用同一 TLS mode；检查 advertised IP/DNS reference 与 certificate SAN，PEM mode 还要检查显式 trust roots。 |
| Trino 返回 HTTP 403/401 | 使用允许的本机 Host，核对 CLI `--user` 与 FE 配置用户；远程访问通过本机客户端或 SSH tunnel。 |
| Trino 返回 `ABANDONED_QUERY` | 客户端超过 `trino_client_timeout_ms` 没有拉取或心跳；这与查询执行超时不同。 |
| 查询报 `role=fe: no live backend available` | 当前 FE 没有可调度的 live BE；先恢复或注册 BE。 |
| FE 启动时提示缺少 catalog source | StaticFile mode 必须提供可读、完整的 snapshot file；DynamicStateStore mode 必须配置 `[state_store]`。不要使用 core metadata 或内存 registry 作为 fallback。 |
| BE 启动时配置校验失败 | `role=be` 必须配置 `[cluster].frontend_endpoint` 和非零独立 `[server].control_grpc_port`，且不能配置 FE heartbeat 或 lease 设置。 |
| Native 或 management endpoint 冲突 | 让 FE Trino、FE MySQL、FE Native gRPC、FE management HTTP、BE Data gRPC、BE Control gRPC、BE management HTTP 使用不重叠的 bind endpoint；同时检查 wildcard bind。 |
| 启动拒绝 Native descriptor baseline | 检查 BE soft `RLIMIT_NOFILE` 至少 1024、FE 至少 2048；在启动环境设置，不依赖 Server 自动提高。 |
| `/metrics` 在 gRPC port 不可用 | 改访问对应 role 的 `[server].http_port`；metrics 使用 role-local registry，不会跨 FE/BE 混合。 |

### MEM-1-M07 配置迁移

`[runtime.frontend_workload]` 中的 `control_bytes`、`per_scope_bytes` 已退役，旧键会报配置错误。FE 使用计数准入与固定结果窗口；不再从进程内存上限派生通用 LRA result budget。进程 MemoryAuthority 的控制 floor 使用 `[runtime.memory].control_bytes`，默认保持 64 MiB，FE/BE 进程启动仍在 work account 之前安装该保护。需要调整控制 floor 的部署应把原控制值移到新位置；`per_scope_bytes` 无替代 FE 配置。BE retained 保护与 M03/M04 后续接线边界保持独立。

### FE 分布式执行容量

FE 默认将每个依赖域的累计保管上限设为 12 GiB，将原 Native 进程发送队列的累计字节上限设为 1 GiB。前者包含尚未交付的结果、写入和清理责任；后者覆盖排队及已接收发送的原始字节。它们是准入上限，不是启动时预分配量，也不代表进程 RSS 上限。默认支持目标是至少 8 个完整依赖域，同时保留前台与收尾操作的保护空间；实际支持界由启动时各资源作者的完整报价和物理容量共同验证。

依赖域来自真实选中的 Provider、存储和 Native endpoint，数量不等于 BE 数。同一 BE 的 Data 与 Control endpoint 可能分别形成依赖域。计划使用的完整集合超过当前支持界时，会明确拒绝；系统不会丢弃域、猜测单机数量或自动提高显式配置。

可通过正常部署配置覆盖这两项：

```toml
[runtime]
task_backend_max_queued_bytes = 1073741824

[runtime.frontend_workload.dependency_custody]
retained_payload_bytes = 12884901888
```

较低的显式值仍按原值执行，可能减少可支持的完整依赖域数量或拒绝相应动作。部署调大上限时，应同时检查完整保管、Native 队列字节与位置、元数据、计算和未交付写入责任；只提高保管字节不能补足物理队列。新默认允许更多累计保管暴露，满载时仍由全局、每域及各执行类别的原准入保护限制。
