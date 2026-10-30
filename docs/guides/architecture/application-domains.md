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
[ADR-0161](../../adr/ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md)、
[ADR-0198](../../adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) 和
[ADR-0147](../../adr/ADR-0147-process-local-work-governance-separates-responsibility-and-resources.md)
为准；Task 的 wire/lifecycle、创建载体、按 identity 重放与一次激活以 ADR-0161 为准。
ADR-0198 的统一执行契约已接受；源码索引反映旧同步协调路径退役后的组合，
不构成整族原生验收或最终验证完成证明。ADR 保持 implementation pending，已记录失败仍须单独处置。

当前入口可从 `novarocks/frontend-application/src/query/statement_action.rs` 的稳定语句 custody，
`read_action.rs`、`write_action.rs`、`mutation_action.rs` 和 `ctas_action.rs` 的实际 Action，以及
`statement_computation.rs`、`statement_publication.rs` 的共享接缝核对。Query Application 的
`coordination/supervisor.rs` 拥有统一 query-id 来源、逻辑驱动与恢复；Frontend 的
Provider rewrite session 保管在 `query_execution/distributed_rewrite.rs`，空的
`QueryExecutionService` 及其注入链已退役。
原 credentials、RF 与 Profile 的共享事实/作者仍由实际消费者保留。
Statistics 的现有 Action 入口在 `statistics_jobs/{service,attempt_executor}.rs`：前台 capture 实际关闭
CPU/Provider 后，把原 query permit 移交给同 job 稳定槽，再通知原 worker；后台沿共享计算 owner
推进。`query_execution/internal_result_cpu/scoped_statistics.rs` 持封闭 scoped CPU source，
`query_execution/statistics/preparation.rs` 保留原准备访问，`query_execution/internal_consumer/decoded.rs`
在 Ready payload 移交前取得原 statement retention。

维护 SQL 的实际 Action 选择位于 `query/maintenance_action.rs`；后台 Rewrite 与 Cleanup 原来源在
`query_execution/maintenance/{rewrite_job,cleanup_action}.rs`，共享产品算法仍在
`novarocks/table-maintenance/src/product.rs`。`query/mv_action.rs` 保管同 SQL Root 的多步骤 MV
编排，数据步骤借共享 Native 计算，控制/publication 由相应原 Source 推进；
`query/mv_read_action.rs` 保只读 SHOW/EXPLAIN 与原本地结果交接。MV 后台由
`mv-application/src/process_runtime.rs` 的异步任务保原事件与 Join，Frontend 的
`mv/service/background_original.rs` 保稳定调度 Source/同 Root。`MvProductOperations` 与唯一
runtime wrapper 分开，仍共用同业务闸门和调度状态，不弱化 Unknown 保管来断开强引用循环。
未触及的 CALL/Host 辅助入口不在此处承诺全域异步完成；最终原生接线、全工作区验证和
失败处置仍须逐项取证。

## 先判定 owner，再选 crate

同一份数据被多个地方读取，不表示它有多个 owner。修改前先回答：谁能改变它、
谁决定失败是否可恢复、谁必须在终态后继续等待实际停止、谁对外宣布结果。稳定且
独立演化的 owner 才成为 crate；同一 request-local 语义内仍共同演化的观察、准备和
协调留在 Query Application 的受限模块中。跨 owner 的最小纯词汇进入 Coordination Contract，
不能因为某段算法不依赖 Tokio 就把它迁出自己的 policy owner。

| 领域 | 责任归属 | 可以拥有 | 不可以拥有 |
| --- | --- | --- | --- |
| Execution Contract | `novarocks-execution-contract` | 不可变 identity、descriptor、operation、receipt 与纯验证 | registry、重试循环、资源 owner、transport 或执行 kernel |
| Coordination Contract | `novarocks-coordination-contract` | FE owner 之间实际共用的身份、时间、消息与中立事实 | Tokio、运行资源、领域载荷、任意 callback 或恢复算法 |
| Query Application | `novarocks-query-application` | request-local 观察/准备，各分布式意图的逻辑执行、attempt 恢复、可见性与计算就绪 | 产品业务结果、publication/GC 策略、角色实现、wire codec 或 Server 配置 |
| Frontend Application | `novarocks-frontend-application` | FE 装配、prepare 前语句监督、Native attempt 驱动、领域就绪证明与 SQL/MySQL 结果接线 | BE Task registry、第二套逻辑执行 policy 或第二份 Provider 提交真相 |
| Worker | `novarocks-worker` | BE 本地 Task/context 准入、租约、入站围栏、实际停止与收敛 | FE 观察、编译、逻辑恢复政策或产品状态 |
| Workload Control | `novarocks-workload-control` | 进程内 scope、责任/完整依赖事实及原结果预算授权 | MV/Statistics/Maintenance 的业务锁、外部提交或 durable truth |
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
| attempt 实例化 | 本 attempt 的 topology、admission、数据访问资格与作用域绑定、split source、Task/RF/Exchange manifest | 改写已冻结语义、重做分析/优化/MV 发现/Connector negotiation |
| Worker 收敛 | Task 的本地状态、实际停止、资源与输出事实 | 宣布逻辑执行的恢复成功，或用协议消息替代本地终态 |

`PreparedLogicalRead` 将冻结描述、静态 template 和解析后的选项以不可拆开的
move-only carrier 交给 launcher；内部计算使用 `PreparedLogicalComputation`，固定领域消费者及其
结果保管留在 Frontend，不进入共享 contract。每次 replacement 只根据冻结 recipe 重新取得本 attempt
的运行能力，并验证它仍覆盖冻结 binding；它不重新规划，也不改变输出 schema、residual
predicate 或恢复承诺。无法证明 credential/delegation 覆盖冻结对象时必须 fail closed。
纯读的 final-plan 路径在首批输出可见前保留 `RestartAttemptBeforeVisibility`；
外部效果语句不借用该恢复资格。

执行访问能力的 attempt 绑定与凭据续期的 owner 不同。attempt 验证本次对象和作用域的
执行资格；实际签名的消费节点持有进程级 `StorageAuthority`，按需取得或续期材料，并可在
等价的 catalog generation、scope 和能力路径下跨查询复用。FE 的 metadata principal
与 BE 的数据访问 principal 分开；FE 不下发执行 secret，也没有跨节点轮换屏障。见
[ADR-0151](../../adr/ADR-0151-credential-renewal-is-driven-by-the-consumer.md)。

外部效果会改变观察边界。执行跨过创建、提交或发布等不可逆效果后，后续 descriptor
观察必须使用新的 request/provider scope；不得让 effect 前的 catalog/metadata cache 把
effect 后的验证伪装成同一次观察。

## 本地程序与 Task 准备

`novarocks/local-program/src/**` 定义纯 `LocalProgram`：节点、表达式、布局、sink
和 binding requirements 都是静态事实，不持有 scan、writer、Connector 或异步运行能力。
Native 在 `novarocks/native-adapter/src/backend_task_execution/{compiled_package,execution_host}.rs`
完成 per Task 的 package 解码、Provider 验证与能力绑定，经 `novarocks/local-compiler/src/**`
产生纯程序。准确 scan/writer/exchange/sink/RF 能力与程序分别进入 Execution；
`novarocks/execution/src/exec/pipeline/builder/compiled.rs` 直接构建 compiled pipeline。
不能恢复构造用 ExecPlan 或旧解码桥，静态计划路径见 ADR-0185。

Worker 的 `Accepted` 只证明接管，昂贵准备在有界公平 FIFO 中继续；`Installed` 是本地
运行资源安装完成的独立历史事实。准备失败保留已接管实体，不能伪造 Installed。
Exchange 只有在冻结 destination 已 Installed 或准确正常撤回输入需求后才可开放；
失败不能伪装成正常撤回。Native status 生产入口只接受 covered subscription 的非零
generation，不再接入旧 cursor-only 模式。

`novarocks/worker/src/task_registry.rs` 的准备数量、bytes 和 per-context position 预算
在 job 实际退出后归还；发布 Installed 或 terminal 本身不能证明 job 已释放输入与临时
展开资源。FE 的 `novarocks/frontend-application/src/task_execution/{execution,remote_task}.rs`
另行持有每 BE 的部署窗口 W：Accepted ACK 或 RPC 未知结果不能归还位置，Installed、
terminal evidence 或永久 stand-down 才能归还。W 必须小于等于该准确 BE immutable
descriptor 广告的 per-context 准备位置 P；两端预算分别由各自 owner 负责。

这些路径说明当前所有权边界，最终验收仍须按下文分层报告实际测试证据。

## 计算就绪、语句结果与资源分别收敛

`LogicalExecutionSupervisor` 与 actor 管理当前 progressing attempt、原 residual、恢复预算、
可见性、计算结论与资源义务。`runtime_registry/health.rs` 保原首个 health、typed cause 和完整
panic packet，silent 接收完成后才在 caller 锁外通知；真实 active Native、rows 与原 ACK Join
继续由各自原记录保管，health 不是实际退出证明。语句/job owner 在 prepare 前登记并编排整个业务；
领域与 Provider 决定外部 publication。Task terminal、ComputationReady、外部提交和语句成功不能互换。

内部结果完成解码、领域 proof 与协议事实闭合、接收方保管已预留后，才能接受一次
ComputationReady。随后领域业务可立即推进；Native 诊断和收敛继续由原 owner 监督。
普通读取仍保留客户端成功 EOF 的边界。Profile 在就绪前按冻结语义恢复，就绪后不恢复；
诊断缺失必须显式标注，不能阻挡已完成计算或被当作零。

普通 Read 保留同 Root 的原 Client 结果窗口，同窗口只允许一个活计算，下一计算等待原
计算最后实际 alias 退出。成功 EOF 与空 activity 是计算退位证据的一部分，仍须原本地
producer Future 正常退出；失败、取消或 attempt 替换不能伪造成功退位。共享 Root/Stage
不把 Client 转换为 Internal。Internal 的计算位置在就绪并满足实际本地退出后归还，
仍活结果数据继续占同 Root 原预算；Native 远端收尾另由原 owner 保管。结果窗口与 worker
并非同一资源。

同 Root 的 Internal retention 共用原 `ResultCapacityConfig` 的 `all_objects_bytes`，计算 grant
使用独立的 `positions`。从 active WorkOwner 借 Root retention 只续接原预算与实际 holder 寿命，
不取得 Stage/Native/manifest 或 CPU 权限；metadata Source 也不能另建一份同 Root 字节预算。
实际分配仍须原 byte receipt，保留身份或原 Arc 不等于已支付 buffer；这些配置不证明整进程 RSS 有界。

外层语句/job 同时保存业务是否完成、实际 publication 的累计效果、剩余必要/维护义务和本地
响应交付事实。seal/NoOp 不产生虚构提交；最后一步未提交不能覆盖前一步已提交或 Unknown。
成功路径和错误路径都把这些事实传到 MySQL；业务完成后晚到取消不得改成普通中断。
响应失败不撤销业务成功，本地发送成功也不证明客户端应用已收到。已发终局不可改写。

派发授权、取消 veto 与 Provider 可能派发前沿各有证据。未派发是 KnownUncommitted；
已可能派发用独立有限结算预算观察同一 publication，不能凭取消当作可撤回或重派整条 SQL。
正确性屏障和必要领域 finalize 完成才满足对应业务成功；安全 GC 和非必要诊断可以延后，
但原对象、访问能力、删除权、容量和退出观察必须已由稳定 owner 接管。

必要业务事实在 `ConnectorWriteNeededReady` 中保留原 effect、receipt 与 finalization，
由同 canonical Arc 交给接收方和最终 completion；它不是一份深复制的提交记录。
finalization 为 Failed 仍保留真实效果，但不能宣布相应业务成功，CTAS 也不能据失败 seal
继续最终 CREATE。GC 尚未运行时为 Pending，只有真实清理观察才为 Observed。
原 Provider Future 仍留在同一个 publication Call 中，由原 Root 的残留继续推进；
会话取消或无人接成功结果也必须保留同一事实并续接原清理。NeededReady、F 实际退出与
精确库存 Disposed 分别取证，任何一个都不能代替另外两个。

## 产物保管、容量与故障域

FE 在启动写出前登记有限的未接收产物责任。BE seal 前建立稳定输出 custody 并不可回退地
撤销旧 writer 删除权；传输丢失不恢复该权力。FE 收到完整结果且 End 解码成功后，才有
完整清单并能在 FE 内交给 publication owner。这是两个本地切点，不是跨进程原子 ACK。
零结果、前缀或缺 End 时保留未决回收；即使未派发已可确定，也不能报 cleanup Complete。

原 custody 位于可丢弃 future 之外；panic 后接管原始责任，不能只剩 ID/JoinHandle 再猜造新
session。进程崩溃可能丢失清单，不承诺确定回收。扫描 GC（包括 SQL 手动、自动和旧 CTAS
根扫尾）另需 Provider 证明无未来引用；当前 REST/Hadoop 为 UNPROVEN，不能用年龄、
进程内 Unknown 记录或持久 first-observation 替代保护。精确 operation-owned cleanup
与扫描能力分开，具体支持限制见 [维护指南](../iceberg-v3/maintenance.md)。

资源只有实际 owner：CPU pool 拥有 worker 和队列，Transport 拥有 process/target 的
queued bytes/items，Workload/result owner 拥有计算位置与仍活结果字节。完整 D 与原 factory
身份保留，但不再叠加工作类别或依赖域配额，也不要求逐个 SDK 对象图预付 invoice。

普通 Full 沿原队列可取消等待，同输入保持稳定且只处理一次。出队可以归还 queue 位置；
运行、结果及析构仍按原实际退出归还对应资源，超时或取消等待不证明它们退出。临时 proposal
构造 backing 与已安装资源许可分开，真实仍借用的来源保管不能提前释放。不在占满全部 worker
后等待必须由同一批 worker 执行的后继任务，也不为控制流程另起 pool 或整段 block_on。
公平推进以资源可用和依赖能够返回为条件；全部共享 worker 永久阻塞时，健康请求与结算也
可能不可用，不承诺单坏域隔离或据此推出 worker 最低数。

实际 buffer 复制前取得 old+new 并存峰的原预算回执。move、合并和最后 alias 退出按真实
backing 续接或退款，不能把同一 buffer 重复收费，也不能因交接退掉仍活 backing。额外
schema lifetime 只保来源，不产生 byte author。多段业务的仍活数据共用同一预算，不逐段清零；
自持结果使下一次形成不足时明确 typed 拒绝，不等待自己，也不隐式 spill。不设置通用
Internal 峰值 floor 或把窗口数绑定到 worker 数。

metadata 结算沿同 Root 原能力继续；scope、alias 或 context getter 不自行授予新资源。
Unknown 保留原 journal、未决对象和仍活数据，已实际退出的物理动作则归还原运行资源。
原 `MetadataIoAdmission` 的 actual quiescence 只有在 growth、真实 I/O 与最后 cell alias
均已退出后成立，并保原 typed Source；普通资源拒绝、process health 与真实退出分别报告。
实现迁移、原生调用面和最终全工作区验证仍待完成，源码存在与局部测试不代替这些证据。

客户端取消经 query-control owner 进入，会话 generation fencing 等本语句的本地受监督编排
退出；不等远端 Unknown 责任消失。后者继续由原 owner 持有真实义务与容量。

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
