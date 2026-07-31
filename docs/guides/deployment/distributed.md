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

分布式部署使用 NovaRocks standalone SQL 引擎的集群角色能力，将协调节点和计算节点拆分为不同进程。FE 角色提供 MySQL 入口、SQL 解析、优化和 fragment 调度；BE 角色提供 NovaRocks gRPC 后端服务并执行 fragment。

该模式不依赖 StarRocks FE。如果要接入 StarRocks FE，请使用 [StarRocks兼容部署](starrocks-compatible.md)。

## 部署拓扑

典型拓扑如下：

```text
MySQL client
  |
  v
NovaRocks role=fe
  |  StateStore durable membership + cluster.backends additive seeds
  v
NovaRocks role=be  +  NovaRocks role=be  +  ...
```

角色说明：

| 角色 | 作用 | 对外端口 |
| --- | --- | --- |
| `fe` | 接收 MySQL 连接，解析 SQL，优化计划，调度 fragment 到后端 | `[standalone_server].mysql_port`；同时使用 `[server].grpc_port` 提供 coordinator report gRPC |
| `be` | 执行 FE 下发的 fragment，处理 exchange 和结果回传 | `[server].grpc_port` |
| `all-in-one` | 默认单进程模式，适合 standalone 单机部署；进程内仍由独立 FE/BE application host 通过 NovaRocksGrpc 通信，不走 direct-call shortcut | `[standalone_server].mysql_port`；`[server].grpc_port` |

## 前提条件

- FE 节点和所有 BE 节点使用同一版本的 NovaRocks。
- FE 节点可以访问每个 BE 的 `grpc_port`。
- BE 节点可以访问 FE 的 `grpc_port`，用于向 coordinator 上报执行状态。
- 所有 BE 节点都能访问相同的数据源、对象存储和 catalog。
- 如果使用对象存储，所有节点的凭据、endpoint 和 path-style 设置应保持一致。
- `role=fe` 必须配置一个可用的 `[state_store]`。它是 backend membership 的唯一 durable authority；SQLite 只适用于恰好一个 active FE。

## 编译 NovaRocks

分布式部署仍然属于 NovaRocks standalone SQL 引擎，不依赖 StarRocks FE，也不需要 `compat` feature。FE 角色和 BE 角色使用同一个 NovaRocks 二进制文件，建议先构建一次 release package，再分发到所有节点。

推荐构建命令：

```bash
./build.sh --release --package
```

默认输出目录为 `./output/novarocks`。将该目录同步到所有 FE / BE 节点，并为不同节点准备各自的配置文件：

```text
./output/novarocks/bin/novarocks
./output/novarocks/bin/novarocksctl
./output/novarocks/conf/novarocks.toml.example
./output/novarocks/lib/
```

本地伪分布式验证时，也可以使用 debug 构建加快迭代：

```bash
cargo build
```

后续启动命令中的 `./target/release/novarocks` 可相应替换为 `./target/debug/novarocks`。只有接入 StarRocks FE 的兼容后端部署才需要 `--features compat`。

## 配置 BE 节点

BE 节点使用 `server.grpc_port` 提供 NovaRocksGrpc 服务，默认端口是 `9080`。不同机器上的 BE 可以都使用默认端口；只有多个 NovaRocks 进程部署在同一台机器并发生端口冲突时，才需要为其中的进程改成其他端口。如果节点有多张网卡，建议显式配置 `advertise_host`，端口统一使用 `server.grpc_port`。

示例 `be-1.toml`：

```toml
log_level = "info"

[server]
host = "0.0.0.0"
grpc_port = 9080

[cluster]
role = "be"
advertise_host = "10.0.0.11"

[connector.object_store]
endpoint = "http://10.0.0.20:9000"
access_key_id = "admin"
access_key_secret = "admin123"
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

## 配置 FE 节点

FE 节点也会启动 `server.grpc_port`，用于接收 BE 回报执行状态。该配置默认是 `9080`，如果没有端口冲突可以不显式配置。FE 必须配置 `[state_store]`，以持久化 backend membership；`[cluster].backends` 则是可选的 additive seeds，endpoint 应指向 BE 的 `grpc_port`。

示例 `fe.toml`：

```toml
log_level = "info"

[metadata]
provider = "sqlite"
path = "meta/fe.sqlite"

[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/fe-state-store.sqlite"
deployment_owner = "fe-a"

[server]
host = "0.0.0.0"
grpc_port = 9080

[standalone_server]
mysql_port = 9030
user = "root"

[connector.object_store]
endpoint = "http://10.0.0.20:9000"
access_key_id = "admin"
access_key_secret = "admin123"
enable_path_style_access = true

[cluster]
role = "fe"
backends = [
  "10.0.0.11:9080",
  "10.0.0.12:9080",
]
```

`[metadata]` 保存 native control metadata，不提供内部 StarRocks 表或 backend membership fallback。持久用户表属于 external Iceberg catalog；`[connector.object_store]` 只提供 connector execution 的进程本地凭据。`[state_store]` 中的 membership 会在 FE 重启后恢复。`[cluster].backends` 只会补充尚不存在的 endpoint：它不会删除 StateStore 中已有的 backend，也不会重新激活正在 decommissioning 的 backend。

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

## 管理 BE

FE 角色支持通过 SQL 动态管理后端：

```sql
ADD BACKEND '10.0.0.13:9080';
SHOW BACKENDS;
DROP BACKEND '10.0.0.13:9080';
```

如果需要立即移除后端，可以使用：

```sql
DROP BACKEND '10.0.0.13:9080' FORCE;
```

普通 `DROP BACKEND` 会让后端停止接收新查询，并等待在途 fragment 结束后移除；`FORCE` 会立即移除，可能导致在途查询失败。

ADD/DROP 的最终 desired membership 先写入 FE 的 StateStore，因此 clean FE restart 后仍会保留动态 ADD 的 backend，并保持已删除 backend 不被 seeds 静默恢复。要恢复一个已删除 endpoint，需要再次显式执行 `ADD BACKEND`。

## 启停顺序

推荐启动顺序：

1. 启动对象存储、catalog、HDFS 等外部依赖。
2. 启动所有 BE 节点，等待 `NOVAROCKS_READY role=be`。
3. 启动 FE 节点，等待 `NOVAROCKS_READY mysql_port=...`。
4. 连接 FE 并执行 `SHOW BACKENDS`。
5. 执行最小查询和一条真实数据查询。

推荐停止顺序：

1. 停止新查询入口或断开客户端。
2. 在 FE 上 `DROP BACKEND` 或等待查询结束。
3. 停止 FE 进程。
4. 停止 BE 进程。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| `SHOW BACKENDS` 为空 | 检查 StateStore 中的 durable membership、`[cluster].backends` seeds，或使用 `ADD BACKEND 'host:port'` 注册后端。 |
| BE 一直不 Alive | 确认 FE 节点能访问 BE 的 `grpc_port`，并检查 BE 的 `advertise_host`。 |
| 查询报 `role=fe: no live backend available` | 当前 FE 没有可调度的 live BE；先恢复或注册 BE。 |
| FE 启动时提示缺少 StateStore | 为 `role=fe` 配置 `[state_store]`；不要使用 core metadata 或内存 registry 作为 membership fallback。 |
| BE 启动时配置校验失败 | `role=be` 不能配置 `[cluster].backends`。 |
| all-in-one 配置校验失败 | `role=all-in-one` 不能配置 `[cluster].backends`。 |
