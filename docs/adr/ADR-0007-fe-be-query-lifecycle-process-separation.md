---
id: ADR-0007
title: "Keep FE Coordination and BE Query Lifecycle Process-Separated"
domain: [distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-07-27
provenance:
  - "discussion: 2026-07-27 FE/BE query lifecycle process separation"
code-anchors:
  - "novarocks/frontend/src/native/task_transport.rs (NativeTaskOperationSink)"
  - "novarocks/backend/src/task_execution/registry.rs (TaskExecutionRegistry)"
  - "novarocks-server/src/composition.rs (run_all_in_one_until)"
---

## 问题

NovaRocks 应如何划分 FE coordinator 与 BE query lifecycle 的状态、故障和代码所有权，才能让分布式部署中的网络与进程
失败被真实建模，同时避免 all-in-one 形态绕过这条边界？

## 背景与执行事实

NovaRocks 的 FE 与 BE 是可独立部署、重启和失败的进程角色，因此天然属于不同故障域。FE 需要看到全局计划、参与节点和
查询进度，BE 则需要管理本地执行、内存、算子、exchange、runtime filter 及其他 query-scoped 资源。两侧生命周期的
事实来源、超时策略和终止条件不同，不能由同一个进程内对象诚实表达。

当前 `ExecutionCoordinator` 位于 `novarocks/core/src/coordinator/execution.rs`，BE 的
`QueryContextManager` 位于 `novarocks/core/src/runtime/query_context.rs`；而
`novarocks/core/src/engine/mod.rs` 仍有直接调用 coordinator 的路径。这些结构使 all-in-one/test convenience 有机会
影响生产边界，也让 coordinator 与本地资源生命周期仍可能在同一 crate 和调用栈中互相穿透。

现有 runtime filter 流程能够传输安装和确认等 feature-specific 事实，但 feature handler 参与创建本地
`QueryContext`，因而可能反向成为通用 query lifecycle 的入口。fragment finish/report 同样只能证明已提交并执行的
fragment 事实；在 partial-submit、零 fragment 服务参与者或计划内 instance 从未抵达 BE 时，它无法判断整个 query
是否终止。

真实跨进程协作必须把重复、延迟、乱序、丢失、过期 ownership 和任一进程失败视为一等输入。共享内存调用会掩盖这些
输入，令单进程测试通过却无法证明独立 FE/BE 部署正确。

## 考虑过的选项

### 选项一：FE/BE 共享一个 lifecycle manager 或状态机

这会减少代码和测试数量，并让 all-in-one 直接调用很方便。但同一 runtime object、registry、lock、timer 和 terminal
record 会把独立进程错误地建模为共享内存并发，无法表达网络异常、独立 crash、重启或 stale ownership。否决。

### 选项二：共享 lifecycle flow，通过 adapter 区分 FE 与 BE

Adapter 可以隐藏调用方式差异，但 transition、timer、terminal owner 与错误策略仍由同一流程实现决定；在
all-in-one 中还容易退化为 adapter 直接读取对侧 registry 或触发 callback。它保留了共享状态机的核心问题，只把边界
藏得更深。否决。

### 选项三：FE/BE 分别实现状态机，只共享中立协议、值对象与纯验证

FE 与 BE 各自拥有符合本地职责的 transition、timer、record 和测试，通过版本化 wire protocol 交换事实。
all-in-one 也经过相同协议入口。两侧可以共享 immutable wire DTO、identity/value object、codec 和无 runtime state
的 pure validation。接受。

### 选项四：保留 fragment/runtime-filter-specific lifecycle

这个方案迁移量最小，但 fragment report 无法覆盖未提交 instance，runtime filter 也不应为没有该能力的 query 或其他
query-scoped 服务承担生命周期。继续让 feature-specific flow 兼任通用生命周期，会留下 query-level ownership
空洞。否决。

## 裁决

FE coordinator 拥有 query 的全局 orchestration：选择与跟踪参与方、推进全局阶段、汇聚事实并作出全局终止裁决。
BE query lifecycle 拥有本进程内的执行与资源：建立、启动、取消、回收本地 query-scoped 状态，并报告本地事实。

FE 与 BE 必须是进程和故障域分离的角色，并刻意维护彼此独立的状态机实现。它们只通过版本化 wire protocol 协作；
duplicate、delay、reorder、loss、stale ownership 和 process failure 都是协议处理必须覆盖的一等输入，而不是异常路径
之外的假设。

跨角色可共享的代码仅限 immutable wire DTO、identity/value object、codec，以及不读取或持有 runtime state 的 pure
validation。两侧不得共享 runtime object、registry、lock、callback、terminal record、timer 或 lifecycle transition
实现；相似逻辑也不得以共享 flow、基类或隐藏 adapter 的形式重新合并。

all-in-one/standalone 不构成例外。它必须使用与独立进程部署相同的协议边界，不得添加 direct-call shortcut。传输可以
在同进程中实现，但消息编解码、身份、幂等和状态机入口的语义必须与 distributed 路径一致。

Runtime filter、exchange 及未来 query-scoped capability 可以向 prepare、progress 或 terminal 协作提供 typed
protocol value，但不得拥有 generic query lifecycle。Fragment report 只承载 fragment facts，不能判断或拥有 query
terminality。

crate dependency 与 visibility 应表达上述边界：角色实现依赖中立协议/value 层而不依赖对侧 runtime internals；
`novarocks-server` 只负责组合和启动 process roles，不拥有生命周期语义，也不通过 composition 建立越界调用。

## 接受的妥协（诚实记录）

FE 与 BE 将存在概念相似但刻意独立的 transition、错误处理和测试，产生明确的代码重复。这里不以复用率为目标；重复是
为了让两侧 ownership、failure policy 和演进节奏保持独立。

query lifecycle protocol、persistent control stream、staged startup、reporting、heartbeats、retries 和小型 retained
records 会增加 RPC、状态观察、运维诊断与容量管理复杂度。all-in-one 也会失去 direct-call 的开发便利，并承担协议
路径的额外测试与调试成本。

物理拆分会扩大迁移规模，需要把当前跨层调用和 feature-specific ownership 分阶段迁出。不能用局部 fallback 或
in-process shortcut 缩小首个迁移步骤，因为它们会保留两套语义。这些成本不意味着设计更简单；接受它们是为了暴露
真实 distributed failure model，使其可测试、可观察并可独立演进。

第一版采用 fail-close：当 ownership、消息顺序或对侧状态无法被可靠证明时，不猜测成功或继续执行。第一版不承诺
active-stream recovery、coordinator takeover，也不承诺 terminal snapshot 在 FE crash 后持久化并继续交付。这些是
明确的产品边界，而不是已经由协议解决的能力。

## 何时重新评估

- 产品永久移除独立 FE/BE 部署，只支持单进程且不再保留独立 process roles 时；
- 生产测量证明 wire protocol/control-plane 是主导性能瓶颈，并且替代方案仍能保持 FE 与 BE 独立的 state 和 failure
  ownership 时；
- durable coordinator recovery 获得真实、单调的 ownership source，需要 coordinator takeover 同一 execution
  attempt 时；届时应新增 fencing protocol，而不是用没有 takeover 语义的 token 冒充 fence；
- 出现新的 query-scoped capability 时，应先评估它作为 typed protocol contribution 的边界，不应以此为理由重新共享
  状态机或 runtime state。
