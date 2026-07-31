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

# standalone部署

standalone 部署使用一个 NovaRocks 进程提供 MySQL 兼容 SQL 入口，并在同一进程内完成 SQL 解析、优化和执行。它不依赖 StarRocks FE，适合本地试用、功能验证、SQL 回归测试和单机开发环境。

## 前提条件

- 已获取 NovaRocks 源码或构建产物。
- 机器上可以运行 NovaRocks 二进制文件。
- 已安装 MySQL 客户端，用于连接 NovaRocks 的 MySQL 协议端口。
- 如果要访问 S3、OSS、MinIO、HDFS 或 Iceberg REST Catalog，需要提前准备对应服务和访问凭据。

## 编译 NovaRocks

standalone 部署不需要 StarRocks FE 兼容 brpc shim，使用默认非 `compat` 构建即可。

本地验证或开发调试时，可以直接使用 debug 构建：

```bash
cargo build
```

启动时使用对应的 debug 产物：

```bash
NO_PROXY=127.0.0.1,localhost \
./target/debug/novarocks standalone --config ./novarocks.toml
```

生产或准生产部署建议使用 release 构建：

```bash
./build.sh --release
```

如果需要生成包含二进制、默认配置、控制脚本和运行时库的部署目录，使用 package 输出：

```bash
./build.sh --release --package
```

默认部署目录为 `./output/novarocks`，常用文件如下：

```text
./output/novarocks/bin/novarocks
./output/novarocks/bin/novarocksctl
./output/novarocks/conf/novarocks.toml.example
./output/novarocks/lib/
```

`build.sh` 会使用 `STARROCKS_THIRDPARTY` 指向的 thirdparty 目录；未显式设置时默认使用仓库下的 `./thirdparty`。

## 准备配置文件

NovaRocks 按以下顺序查找配置：

1. 命令行 `--config <path>`。
2. 环境变量 `NOVAROCKS_CONFIG=<path>`。
3. 当前目录下的 `./novarocks.toml`。

最小 standalone 配置如下：

```toml
[server]
grpc_port = 9080

[metadata]
provider = "sqlite"
path = "meta/standalone.sqlite"

[standalone_server]
mysql_port = 9030
user = "root"

[connector.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
access_key_secret = "admin123"
enable_path_style_access = true
```

说明：

- `mysql_port` 是客户端连接 NovaRocks 的端口。
- `[server].grpc_port` 是 NovaRocksGrpc 端口。`role=be` 和 `role=all-in-one` 由 BE host 提供完整 fragment/exchange 服务；`role=fe` 只提供 coordinator report 服务。`all-in-one` 仍通过该 gRPC 边界调度本机 BE，不使用 direct-call shortcut。默认值为 `9080`。
- `user` 当前只支持 `root`。
- `[metadata].path` 用于保存 native control metadata，不承载用户内表数据。
- native 持久表必须属于显式创建的 external Iceberg catalog；NovaRocks 不创建内部 StarRocks 类型表。
- `[connector.object_store]` 为 connector execution 提供进程本地对象存储凭据；它本身不创建 catalog 或内表。

## 启动服务

使用源码启动：

```bash
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --config ./novarocks.toml
```

使用已构建的二进制启动：

```bash
NO_PROXY=127.0.0.1,localhost \
./target/release/novarocks standalone --config ./novarocks.toml
```

也可以临时覆盖 MySQL 端口：

```bash
NO_PROXY=127.0.0.1,localhost \
./target/release/novarocks standalone --port 9030 --config ./novarocks.toml
```

启动成功后，标准输出会出现类似以下 readiness 标记：

```text
NOVAROCKS_READY mysql_port=9030 pid=<pid>
```

看到该标记后再连接客户端。

## 连接并验证

```bash
mysql -h 127.0.0.1 -P 9030 -uroot
```

执行基础 SQL：

```sql
SELECT 1;
SHOW DATABASES;
```

配置 external Iceberg catalog 后，可以继续创建 database 和 table。Native 不提供内部 StarRocks 表类型；未来 StarRocks 数据源也必须通过 external connector 接入。Iceberg v3 的快速验证流程见 [Iceberg v3 快速上手](../iceberg-v3/quickstart.md)。

## 使用本地 Iceberg REST 环境

仓库内置了 Iceberg REST Catalog、MinIO 和 Spark 的本地测试环境：

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --config "$NOVAROCKS_STANDALONE_CONFIG"
```

该环境会生成当前工作区专用的 NovaRocks 配置、SQL test 配置和端口。不要假设固定端口，优先使用 `docker/iceberg-rest/runtime/current/env.sh` 中导出的变量。

## 停止服务

前台运行时按 `Ctrl-C` 停止。后台运行时请记录启动进程的 PID，并优先发送 `SIGTERM`：

```bash
kill <pid>
```

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| 客户端连不上 `9030` | 确认服务已打印 `NOVAROCKS_READY`，并检查 `--port` 或 `[standalone_server].mysql_port` 是否被改过。 |
| `9080` 端口冲突 | 修改 `[server].grpc_port`。该端口不影响 MySQL 客户端连接端口。 |
| 对象存储访问失败 | 检查 endpoint、access key、secret、path-style 设置和 `NO_PROXY`。 |
| 多个工作区端口冲突 | 使用不同 `mysql_port`，或通过 `docker/iceberg-rest/runtime/current/env.sh` 获取自动分配端口。 |
| 登录失败 | 当前 standalone server 只支持 `root` 用户，默认空密码。 |
