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

本文介绍 NovaRocks 的 native 角色部署。生产角色只有 `fe` 和 `be`；
`all-in-one` 是本地验证与测试便利，不是单独的生产拓扑。

- [分布式部署](distributed.md)：将 NovaRocks `fe` 与一个或多个 `be` 拆开运行。
- [all-in-one 部署](standalone.md)：在一个进程中运行 native FE/BE application host，适合本地验证和功能测试。

## 部署流程

生产或准生产部署前，建议先完成以下准备：

1. 确认部署模式。
2. 按部署模式编译 NovaRocks。
3. 准备 NovaRocks 二进制文件和配置文件。
4. 规划端口、主机名、对象存储、元数据路径和日志目录。
5. 启动服务并确认 readiness 输出或管理 SQL 能正常返回。
6. 运行一条最小查询，确认 SQL 入口、执行节点和存储访问都可用。

## 模式选择

| 部署模式 | 适用场景 | 编译重点 | SQL 入口 | 计算节点 | 主要配置 |
| --- | --- | --- | --- | --- | --- |
| 分布式部署 | 多节点执行、分布式查询验证、生产或准生产环境 | `cargo build --release -p novarocks-server`，将同一 native 二进制部署到 FE/BE 节点 | NovaRocks FE 的 MySQL 协议端口 | 一个或多个 NovaRocks BE 进程 | `[cluster]`、`[server]`、`[standalone_server]` |
| all-in-one 部署 | 单机开发、快速试用、SQL 回归测试 | `cargo build -p novarocks-server` 用于本地验证；部署建议 release | 当前进程的 MySQL 协议端口 | 当前进程内的 native BE host | `[standalone_server]`、对象存储配置 |

## 分布式部署

分布式部署使用 `standalone` 的 `fe` / `be` 角色拆分执行链路。FE 角色负责 MySQL 协议、SQL 解析、优化和任务调度；BE 角色负责接收 fragment、执行算子并回传结果。

阅读：[分布式部署](distributed.md)

## standalone部署

all-in-one 部署使用单个 NovaRocks 进程完成 native FE/BE application host 的 SQL 接入、分析、优化和执行。它适合快速验证 external Iceberg catalog、SQL 功能和本地测试环境；它不提供内部 StarRocks 表类型。

阅读：[standalone部署](standalone.md)

## StarRocks 外部 Connector

StarRocks 不是 NovaRocks 的 server 角色。它以只读 external Connector 接入：RPC 读取支持所有 StarRocks 拓扑，direct 读取永久只支持 shared-data。Connector 的 control 与 execution binding 由 native FE/BE host 装配，不需要 StarRocks FE、BE 兼容协议或 thirdparty 工具链。
