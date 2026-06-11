# StarRocks兼容部署

StarRocks 兼容部署用于让 NovaRocks 作为 StarRocks FE 可识别的后端运行。此模式下，用户仍然连接 StarRocks FE；StarRocks FE 负责 SQL 解析、优化和计划切分，NovaRocks 负责接收 FE 下发的 thrift 计划并执行 fragment。

如果你希望完全不依赖 StarRocks FE，请使用 [standalone部署](standalone.md) 或 [分布式部署](distributed.md)。

## 工作方式

兼容模式启动后，NovaRocks 会提供 StarRocks FE 期望的一组服务：

| 服务 | 配置项 | 用途 |
| --- | --- | --- |
| HeartbeatService | `[server].heartbeat_port` | FE 通过该端口识别和管理后端。 |
| BackendService | `[server].be_port` | StarRocks BE thrift 管理接口。 |
| brpc gateway | `[server].brpc_port` | FE 下发查询执行请求。 |
| HTTP service | `[server].http_port` | HTTP 管理和状态端口。 |
| Starlet / gRPC | `[server].starlet_port` | 内部 exchange、runtime filter、lookup 等 gRPC 通信。 |

该模式不会启动 standalone MySQL 端口；SQL 客户端应连接 StarRocks FE。

## 前提条件

- 已有可用的 StarRocks FE 集群。
- NovaRocks 构建产物包含 StarRocks 兼容所需的 brpc shim。
- FE 节点可以访问 NovaRocks 的 `heartbeat_port`、`be_port`、`brpc_port`、`http_port` 和 `starlet_port`。
- NovaRocks 节点可以访问查询所需的数据源、对象存储和网络环境。

## 编译 NovaRocks

StarRocks 兼容部署需要 C++ brpc shim 和 StarRocks 兼容协议相关依赖，必须启用 `compat` feature 编译。编译前需要准备 NovaRocks thirdparty；如果默认 `./thirdparty` 不可用，请用 `STARROCKS_THIRDPARTY` 指向已构建好的 thirdparty 根目录。

推荐生成 release package：

```bash
STARROCKS_THIRDPARTY=./thirdparty \
./build.sh --release --package --features compat
```

Linux 环境如果使用 StarRocks toolchain，还需要设置 `STARROCKS_GCC_HOME` 指向包含 `bin/gcc` 和 `bin/g++` 的工具链根目录。

默认输出目录为 `./output/novarocks`：

```text
./output/novarocks/bin/novarocks
./output/novarocks/bin/novarocksctl
./output/novarocks/conf/novarocks.toml.example
./output/novarocks/lib/
```

如果只需要生成二进制，也可以直接使用 Cargo：

```bash
cargo build --release --features compat
```

直接使用 Cargo 产物时，需要自行处理配置文件、控制脚本和运行时库。生产部署优先使用 package 输出。

## 准备配置文件

示例 `novarocks.toml`：

```toml
log_level = "info"

[server]
host = "0.0.0.0"
priority_networks = "10.0.0.0/24"
heartbeat_port = 9050
be_port = 9060
brpc_port = 8060
http_port = 8040
starlet_port = 9070

[runtime]
pipeline_scan_thread_pool_thread_num = 0
pipeline_exec_thread_pool_thread_num = 0

[starrocks]
meta_cache_ttl_ms = 0
lake_data_write_format = "native"
```

说明：

- `host` 是服务绑定地址。
- `priority_networks` 用于多网卡环境下选择对 FE 上报的地址。
- `heartbeat_port` 是在 StarRocks FE 中注册 NovaRocks 后端时使用的端口。
- `brpc_port` 是 FE 发送 fragment 执行请求的端口。
- `starlet_port` 会作为 gRPC / Starlet 端口上报给 FE。

## 启动 NovaRocks

如果使用 package 输出，先进入部署目录并准备配置：

```bash
cd ./output/novarocks
cp conf/novarocks.toml.example conf/novarocks.toml
```

前台启动：

```bash
./bin/novarocks run --config ./conf/novarocks.toml
```

使用控制脚本启动：

```bash
./bin/novarocksctl start --daemon
```

停止：

```bash
./bin/novarocksctl stop
```

如果使用的是直接 Cargo 构建的产物，可以改用：

```bash
./target/release/novarocks run --config ./novarocks.toml
```

启动成功后会输出类似：

```text
novarocksd started (bind_host=0.0.0.0, advertise_host=10.0.0.21, advertise_port=9070, heartbeat_port=9050, be_port=9060, brpc_port=8060, http_port=8040, starlet_port=9070)
```

## 在 StarRocks FE 中注册后端

连接 StarRocks FE 后，将 NovaRocks 节点按 StarRocks 后端注册。地址使用 NovaRocks 对 FE 可达的主机名或 IP，以及 `heartbeat_port`：

```sql
ALTER SYSTEM ADD BACKEND "10.0.0.21:9050";
SHOW BACKENDS;
```

确认 `SHOW BACKENDS` 中对应节点为 Alive 后，再执行查询。

如果需要移除节点：

```sql
ALTER SYSTEM DROP BACKEND "10.0.0.21:9050";
```

具体注册、下线和安全操作流程以当前 StarRocks FE 版本的后端管理文档为准。

## 验证查询

在 StarRocks FE 中执行：

```sql
SELECT 1;
SHOW BACKENDS;
```

如果 NovaRocks 节点已被 FE 调度，查询执行请求会通过 `brpc_port` 进入 NovaRocks，执行结果再按 StarRocks 协议返回给 FE。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| FE 无法添加后端 | 检查 FE 到 NovaRocks `heartbeat_port` 的网络连通性。 |
| `SHOW BACKENDS` 中节点不 Alive | 检查 `priority_networks`、上报地址、各端口是否被防火墙阻断。 |
| 查询无法下发 | 确认 `brpc_port` 可访问，并确认构建产物包含兼容 brpc shim。 |
| 误连 NovaRocks MySQL 端口 | 兼容模式没有 standalone MySQL 入口；请连接 StarRocks FE。 |
| 多网卡上报地址不正确 | 配置 `[server].priority_networks`，让 NovaRocks 选择 FE 可访问的网段。 |
