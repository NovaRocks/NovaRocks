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

本文介绍 NovaRocks 的部署模式选择。NovaRocks 有三种常见部署方式：

- [分布式部署](distributed.md)：使用 NovaRocks 自带的 standalone SQL 入口，将协调节点和计算节点拆开运行。
- [standalone部署](standalone.md)：单进程运行 NovaRocks，适合本地验证、功能测试和小规模使用。
- [StarRocks兼容部署](starrocks-compatible.md)：作为 StarRocks FE 可识别的兼容后端，由 StarRocks FE 负责 SQL 解析和计划下发。

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
| 分布式部署 | 多节点执行、分布式查询验证、准生产环境 | 默认非 `compat` 构建；建议生成 release package 并分发到所有 FE/BE 节点 | NovaRocks FE 角色的 MySQL 协议端口 | 一个或多个 NovaRocks BE 角色进程 | `[cluster]`、`[server]`、`[standalone_server]` |
| standalone部署 | 单机开发、快速试用、SQL 回归测试 | 默认非 `compat` 构建；本地验证可用 debug，部署建议 release | 当前进程的 MySQL 协议端口 | 当前进程本地执行 | `[standalone_server]`、对象存储配置 |
| StarRocks兼容部署 | 接入已有 StarRocks FE，替换或补充 BE 执行层 | 必须启用 `--features compat`，并准备 thirdparty / brpc shim 依赖 | StarRocks FE | NovaRocks 兼容后端进程 | `[server]`、StarRocks FE 后端注册 |

## 分布式部署

分布式部署使用 `standalone` 的 `fe` / `be` 角色拆分执行链路。FE 角色负责 MySQL 协议、SQL 解析、优化和任务调度；BE 角色负责接收 fragment、执行算子并回传结果。

阅读：[分布式部署](distributed.md)

## standalone部署

standalone 部署使用单个 NovaRocks 进程完成 SQL 接入、分析、优化和执行。它不依赖 StarRocks FE，适合用来快速验证 external Iceberg catalog、SQL 功能和本地测试环境；它不提供内部 StarRocks 表类型。

阅读：[standalone部署](standalone.md)

## StarRocks兼容部署

StarRocks 兼容部署用于让 NovaRocks 作为 StarRocks 后端运行。此模式下，SQL 入口仍然是 StarRocks FE；NovaRocks 接收 FE 下发的 thrift 计划和 brpc 请求，并按 StarRocks BE 协议返回结果。

阅读：[StarRocks兼容部署](starrocks-compatible.md)
