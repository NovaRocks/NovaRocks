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

- `[standalone_server].mysql_port`；
- FE Native gRPC 的 `[server].grpc_port`；
- FE management HTTP 的 `[server].http_port`；
- 一个显式 `[catalog_source]`：静态部署使用随配置挂载的 snapshot；只有需要 SQL
  `CREATE/DROP CATALOG` 时才选择 `dynamic-state-store` 并配置 `[state_store]`；
- 可选 `[state_store]` Accelerator carrier（SQLite `emptyDir` 可以随 Pod 删除；它不是 StaticFile
  catalog truth）；
- 与 BE 相同的 mandatory `[native_trust]` deployment id、shared secret 和 transport mode；
- BE 会向 FE Native gRPC endpoint 自注册；`[cluster].backends` 不是 deployment 配置。

BE 配置必须包括自身不同的 Native gRPC 与 management HTTP 端口，以及本地
connector object-store binding。Paimon 查询要求 FE 与 BE 的 binding 指向同一
warehouse 访问域。两份配置在同一进程共享 logging 与 data-runtime
sizing，其他 role-local 字段各自生效。

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
保留 primary error。成功启动后，FE 会打印 MySQL readiness marker：

```text
NOVAROCKS_READY mysql_port=9030 pid=<pid>
```

看到 marker 后再连接客户端：

```bash
mysql -h 127.0.0.1 -P 9030 -uroot
```

## 四个 listener surface

| Role | Native listener | Management listener |
| --- | --- | --- |
| FE | `[server].grpc_port`：仅 coordinator-report gRPC | `[server].http_port`：FE-scoped metrics 与现有 gated lifecycle debug |
| BE | `[server].grpc_port`：仅 fragment/exchange Native gRPC | `[server].http_port`：BE-scoped metrics |

Native listener 不安装 management HTTP route；management listener 不承载 Native
gRPC service。metrics 也按 role-local registry 收集，因此同进程的 all-in-one 不会
把 FE 与 BE metrics 混在一个 endpoint。

## 本地 Iceberg REST 环境

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" \
  --be-config "$NOVAROCKS_BE_CONFIG"
```

该 fixture 为当前工作区生成一对正常 FE/BE 配置和四个不冲突 listener port；不要
猜测端口或重用旧的 standalone config 环境变量。

Paimon 本地 fixture 和支持矩阵见 [Paimon 只读 Connector](../connectors/paimon.md)。
当前 Server 的封闭 provider 集合只有 Iceberg 与 Paimon；旧
`[connector.starrocks]` 配置会在解析阶段明确失败。

## 停止与排障

前台运行时按 `Ctrl-C`。后台运行时记录 PID 并优先发送 `SIGTERM`。FE 收到信号后先
停止新 workload 准入，等待已准入工作最多 300 秒，再最多用 30 秒完成 teardown；all-in-one
会先完成 FE drain，之后才停止本地 BE。

| 现象 | 处理方式 |
| --- | --- |
| 启动前报 endpoint overlap | 为 FE MySQL、FE/BE Native gRPC、FE/BE management HTTP 分配不同端口；也检查 wildcard bind。 |
| 启动前报 process configuration mismatch | 两份配置的 logging 与 data-runtime sizing 必须相同。 |
| FE 提示缺少 StaticFile snapshot | 复制 `novarocks-catalogs.toml.example` 到 FE config 同目录，并使 `[catalog_source].static_file_path` 指向它；文件缺失不会降级为空 snapshot。 |
| FE 提示 DynamicStateStore 缺少 StateStore | 仅在 `[catalog_source].mode = "dynamic-state-store"` 时配置 durable `[state_store]`；不要以 transient/in-memory membership 替代。 |
| native trust preflight failure | 使 FE/BE 的 `deployment_id`、resolved shared secret 与 transport mode 完全相同；完整 secret rotation 只能 homogeneous restart。 |
| Native RPC `Unauthenticated` 或 TLS handshake failure | 检查 secret/env、token clock 和双方 TLS profile；不要降级或删除 JWT 来绕过失败。 |
| 访问 `/metrics` 得到 Native 协议错误或 404 | 改访问对应 role 的 management HTTP port，而不是 Native gRPC port。 |
| 连接 MySQL 失败 | 等待 `NOVAROCKS_READY`，然后确认 FE 的 `[standalone_server].mysql_port`。 |
