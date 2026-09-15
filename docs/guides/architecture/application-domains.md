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

# 应用领域与执行所有权

本文是修改 NovaRocks native FE/BE 应用层时的入口指南。它说明谁拥有可变
状态、谁决定重试、谁交付结果、谁收敛资源；它不是 wire 协议或 Connector
实现指南。架构裁决以 [ADR-0144](../../adr/ADR-0144-application-domains-follow-stable-ownership-seams.md)、
[ADR-0145](../../adr/ADR-0145-freeze-query-semantics-before-attempt-access.md)、
[ADR-0146](../../adr/ADR-0146-logical-execution-owns-attempts-and-result-visibility.md) 和
[ADR-0147](../../adr/ADR-0147-process-local-work-governance-separates-responsibility-and-resources.md)
为准；Task 的 wire/lifecycle 细节仍遵循 [ADR-0135](../../adr/ADR-0135-native-distributed-work-as-tasks.md)。

## 先判定 owner，再选 crate

同一份数据被多个地方读取，不表示它有多个 owner。修改前先回答：谁能改变它、
谁决定失败是否可恢复、谁必须在终态后继续等待实际停止、谁对外宣布结果。稳定且
独立演化的 owner 才成为 crate；同一 request-local 语义内仍共同演化的观察、准备和
协调留在 Query Application 的受限模块中。

| 领域 | 当前 owner | 可以拥有 | 不可以拥有 |
| --- | --- | --- | --- |
| Execution Contract | `novarocks-execution-contract` | 不可变 identity、descriptor、operation、receipt 与纯验证 | registry、重试循环、资源 owner、transport 或执行 kernel |
| Query Application | `novarocks-query-application` | request-local 观察、准备、逻辑执行、attempt 协调、结果与恢复政策 | 产品状态、角色实现、wire codec 或 Server 配置 |
| Frontend Application | `novarocks-frontend-application` | FE 角色装配、SQL/MySQL 适配所需的 application host、产品 consumer 接入 | BE Task registry 或第二套逻辑执行 policy |
| Worker | `novarocks-worker` | BE 本地 Task/context 准入、租约、入站围栏、实际停止与收敛 | FE 观察、编译、逻辑恢复政策或产品状态 |
| Workload Control | `novarocks-workload-control` | 进程内 scope、阶段准入、受治理资源与结果信用 | MV/Statistics/Maintenance 的业务锁、外部提交或 durable truth |
| Catalog Application | `novarocks-catalog-application` | 长期 Catalog generation 与退休 | 查询局部 compiler 映射或 Connector execution instance |
| 产品应用 | MV、Statistics、Maintenance application crates | 业务状态、外部效果、发布与业务恢复决定 | Query Application 的通用协调 policy |
| Native/MySQL adapter | `novarocks-native-adapter`、`novarocks-mysql-adapter` | wire/协议 decode、encode、连接与 transport 事实 | 领域业务 policy 或全局 service locator |
| Server | `novarocks-server` | 配置验证、唯一 owner 构造、窄 handle 注入与逆序 shutdown | SQL/Task wire 的业务解释或查询执行 policy |

依赖方向向政策内侧：产品与角色 adapter 依赖通用应用契约，Server 依赖并组合它们；
Query Application 不反向依赖产品、Frontend、Backend、adapter、wire codec 或 Server。
不要以 optional port、全局 registry、万能 `AppState`、兼容 facade 或双向 callback
掩盖循环依赖。出现循环时，先重新核对 owner，或将 consumer port 定义在 policy owner
一侧。

## 一次逻辑执行不等于一次 attempt

查询的 SQL 语义和执行资源有不同生命周期。准备阶段可以在一个有界 request scope 内
交替进行观察、纯编译与 Connector negotiation；只有 exact binding、输出契约、残余责任、
恢复模式和资源需求全部确定后，才能冻结 `FrozenExecutionDescription`。冻结描述和静态
Native template 不携带 endpoint、placement、Task identity、credential、Catalog lease、split
source 或可回调的重新观察逻辑。

| 阶段 | owner 的事实 | 禁止的捷径 |
| --- | --- | --- |
| 观察与准备 | exact object binding、MV/统计候选、协商结果、FE metadata principal | 借用未来 attempt 的数据凭据或 Worker placement |
| 逻辑描述冻结 | sealed plan、输出契约、恢复政策、静态 attempt recipe | 缓存 secret、endpoint、运行 owner 或重新协商 callback |
| attempt 实例化 | 本 attempt 的 topology、admission、credential、split source、Task/RF/Exchange manifest | 改写已冻结语义、重做分析/优化/MV 发现/Connector negotiation |
| Worker 收敛 | Task 的本地状态、实际停止、资源与输出事实 | 宣布逻辑执行的恢复成功，或用协议消息替代本地终态 |

`PreparedLogicalRead` 将冻结描述、静态 template 和解析后的选项以不可拆开的
move-only carrier 交给 launcher。每次 replacement 只根据冻结 recipe 重新取得本 attempt
的运行能力，并验证它仍覆盖冻结 binding；它不重新规划，也不改变输出 schema、residual
predicate 或恢复承诺。无法证明 credential/delegation 覆盖冻结对象时必须 fail closed。

外部效果会改变观察边界。执行跨过创建、提交或发布等不可逆效果后，后续 descriptor
观察必须使用新的 request/provider scope；不得让 effect 前的 catalog/metadata cache 把
effect 后的验证伪装成同一次观察。

## 责任、结论、输出和资源分别收敛

`LogicalExecutionSupervisor` 与其 actor 拥有逻辑执行的可变状态：当前允许推进的
attempt、旧 residual attempt、恢复预算、结果 visibility、业务结论、取消与资源 obligation。
Task 的 terminal 是 Worker 自己的本地事实；它不是逻辑查询成功或所有资源已释放的同义词。

因此以下终态不能折叠为一个布尔：

- 业务结论：逻辑执行是否成功、失败、取消或不可恢复；
- 输出责任：首批、流式写出、客户端断线和成功 EOF 是否已经唯一地定案；
- Worker 收敛：每个 Task 是否已停止、状态是否可观察、残留 attempt 是否仍需处理；
- 资源责任：scope、reservation、result credit、queue entry 和 owner 是否已经归还。

业务结论确定后，actor 仍可能必须等待 Worker 的实际停止、结果 writer 的收尾或资源
obligation 的归还。不要为让测试“清理干净”而抹去尚未取得的停止事实，也不要把
`Submitted` 当作 `Completed`。客户端取消经 FE 的 query-control owner 进入；它在 coordinator
worker unwind 前保持 generation fencing，下一条语句不得与前一条取消竞态。

## 构造、关闭与验证

Server 先验证配置、secret/provider/StateStore；FE 与 BE 各自构造独立的治理和资源 owner，
再构造 Catalog、Native capability、Worker/topology、独立 candidate reader、query core、产品
服务与协议 adapter。所有 supervisor 和失败清理路径完成注册后才开放 admission。关闭顺序
相反：先关闭新 root admission，再取消并收敛控制、逻辑执行/registry、workload、decode 和
被依赖 owner。all-in-one 只是并行监督同一组正常 role runner，不能成为进程内直调路径。

验证必须分层报告：crate/contract guard 证明依赖和构造边界；focused test 证明具体 owner
行为；native `1FE+3BE` 场景才证明产品路径。all-in-one smoke 只能补充本地反馈，不能替代
分布式验收。性能比较需要 B0 与 candidate 在相同 main、工具、fixture、配置和 workload
manifest 上分别构建和运行；不同来源的 smoke 只能作为诊断证据。

## 修改前检查表

1. 写下 owner、可变状态、失败/重试决定者和最终资源责任；若答不出，先回到 ADR。
2. 区分 immutable contract、冻结语义、per-attempt resource 和 process-local runtime，避免把
   runtime object 放入 plan 或 contract。
3. 仅从 Server 的显式组合路径取得生产依赖；不添加 fallback、global registry 或 all-in-one
   shortcut。
4. 对外部效果后的观察建立新的 scope；对 attempt replacement 禁止 semantic re-planning。
5. 为变更选择最小的 focused check，并在产品主张上补 native `1FE+3BE` 证据；单独记录环境
   阻塞和未达成的性能 gate。
