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

# 部署NovaRocks

本文介绍 NovaRocks 的 native 角色部署。application role 只有 `fe` 和 `be`；
`all-in-one` 是 Server 读取一对正常 FE/BE 配置后的本地 supervisor，不是单独的
application role 或生产拓扑。

- [分布式部署](distributed.md)：将 NovaRocks `fe` 与一个或多个 `be` 拆开运行。
- [all-in-one 部署](standalone.md)：在一个进程中运行 native FE/BE application host，适合本地验证和功能测试。
- [Native trust、JWT 与可选 TLS](native-trust.md)：所有 deployable role 的 mandatory
  Native RPC caller authentication、TLS 1.3 选择、轮换与运维边界。

## 部署流程

生产或准生产部署前，建议先完成以下准备：

1. 确认部署模式。
2. 按部署模式编译 NovaRocks。
3. 准备 NovaRocks 二进制文件和配置文件。
4. 为所有 FE/BE 准备同一 `[native_trust]` deployment id 和 256-bit CSPRNG
   shared secret；决定 trusted-network h2c 或 TLS 1.3 profile。
5. 规划端口、主机名、对象存储、元数据路径和日志目录。
6. 启动服务并确认 readiness 输出或管理 SQL 能正常返回。
7. 运行一条最小查询，确认 SQL 入口、执行节点和存储访问都可用。

## 模式选择

| 部署模式 | 适用场景 | 编译重点 | SQL 入口 | 计算节点 | 主要配置 |
| --- | --- | --- | --- | --- | --- |
| 分布式部署 | 多节点执行、分布式查询验证、生产或准生产环境 | `cargo build --release -p novarocks-server`，将同一 native 二进制部署到 FE/BE 节点 | NovaRocks FE 的 MySQL 协议端口 | 一个或多个 NovaRocks BE 进程 | 独立 `fe.toml` 与每个 `be.toml`，均含 `[cluster].role` |
| all-in-one 部署 | 单机开发、快速试用、SQL 回归测试 | `cargo build -p novarocks-server` 用于本地验证；部署建议 release | 当前进程 FE 的 MySQL 协议端口 | 当前进程内的正常 native BE host | 一对普通 `fe.toml` / `be.toml`；命令显式传入 `--fe-config` 与 `--be-config` |

## 分布式部署

分布式部署使用 `standalone` 的 `fe` / `be` 角色拆分执行链路。FE 角色负责 MySQL 协议、SQL 解析、优化和任务调度；BE 角色负责接收 fragment、执行算子并回传结果。

阅读：[分布式部署](distributed.md)

## standalone部署

all-in-one 部署使用单个 NovaRocks 进程并发运行完整 native FE/BE role runner。它适合快速验证 external Iceberg/Paimon catalog、SQL 功能和本地测试环境；它不绕过 Native gRPC、StateStore、topology 或 listener 路径。

阅读：[standalone部署](standalone.md)

## Connector provider

当前二进制的封闭 provider 集合只有 Iceberg 与 Paimon。Iceberg 保留现有读写能力；Paimon 首期只读，支持 Filesystem Catalog 上的 append-only 与 `deduplicate` 主键表快照。StarRocks 已废弃，没有 active read capability；部署中出现旧 `[connector.starrocks]` 配置会在 Server 解析阶段明确失败，不会被静默忽略。Paimon 的详细范围与外部 snapshot 保留前提见 [Paimon 只读 Connector](../connectors/paimon.md)。
