---
id: ADR-0008
title: "Use Three-Phase Startup for Distributed Queries"
domain: [distributed-query-lifecycle]
status: superseded
supersedes: []
superseded-by: ADR-0135
date: 2026-07-27
provenance:
  - "discussion: 2026-07-27 three-phase distributed query startup"
code-anchors:
  - "novarocks/frontend/src/coordinator/execution.rs (FrontendDistributedQueryCoordinator::execute_request)"
  - "novarocks/core/src/query_execution/artifact.rs (StagePreparedDistributedQuery)"
  - "novarocks/backend/src/query_lifecycle/registry.rs (QueryLifecycleRegistry)"
---

## 问题

NovaRocks 的分布式查询应如何在所有参与方建立 query-level 控制面、完整准备本地 fragment 和 query-scoped 服务，并在
部分提交、RPC 结果未知及参与方启动顺序不同时避免提前执行或遗留资源？

## 背景与执行事实

`FrontendDistributedQueryCoordinator::execute_request` 位于
`novarocks/frontend/src/coordinator/execution.rs`。native production path 先由
`novarocks/core/src/query_execution/artifact.rs` 冻结每个 participant 的精确 Stage batch，再依次经过全部
`StageFragments` ACK 与全部 `StartPreparedQuery` ACK；不再通过 `FragmentDispatcher::submit_fragment` 逐个启动
fragment。BE 在同一 batch 的 dormant worker 共享 `StartGate`，因此 Stage 成功但 Start 之前 driver 不可调度。

BE 的 query-level participant entry 由
`novarocks/backend/src/query_lifecycle/registry.rs` 中的 `QueryLifecycleRegistry` 管理，本地 query-scoped 执行资源仍由
core runtime 承担。分布式查询除了 fragment executor，还可能有只承载 runtime filter、exchange 或其他 query-scoped
服务的参与方。这些 service-only participant 合法地拥有空 fragment 集合，却仍需要在任何数据面流量到达前建立 query
identity、角色、控制连接和资源所有权。Fragment submit 本身无法表达这种参与方；伪造空 fragment 又会把服务角色错误地
包装成执行实例。

Exchange 连接、result/report binding 和其他数据面接收方也必须在 driver 开始产生流量之前准备好。即使 coordinator
作出一个全局启动裁决，各 BE 收到启动请求的物理时间仍不同，因此启动协议只能建立明确的 happens-before 屏障，不能
假设所有进程在同一时刻运行。

因此，查询启动需要三个可独立重试和诊断的逻辑阶段：

```text
InitQuery
    ↓ all participants initialized and control-ready
StageFragments
    ↓ all participants staged
StartPreparedQuery
```

这三个阶段处理的是分布式执行尝试的控制面与本地资源发布，不是数据库 ACID transaction。

## 考虑过的选项

### 选项一：逐 fragment 提交并立即运行

该方案 RPC 数量直观，也沿用当前提交入口，但第一个 fragment 可以在同一 BE 的完整 instance set 到达前运行。中途失败会
形成 partial-submit；单次提交结果未知时，FE 无法区分“未创建”和“已经运行”。尚未准备好的 exchange/data-plane
receiver 可能收到提前流量，零 fragment 的 service-only participant 也没有合法的初始化与终止入口。否决。

### 选项二：先初始化 query，再逐 fragment 提交并立即运行

独立初始化可以建立 query-level identity 和 service-only participant，却没有本地原子发布边界。第一个 fragment
仍可能在后续 fragment 构建失败或结果未知时开始运行，missing、unknown 或 duplicate instance 也只能在局部状态已经
可见后发现。它解决了 query entry 缺失，却没有解决 partial-submit 和 data-plane receiver 未就绪。否决。

### 选项三：只做 Stage 与 Start，不设 query-level Init

Stage 可以原子准备本地 fragment，Start 可以延迟 driver 调度，但 Stage 之前没有稳定的 query-level participant
entry 来绑定 execution attempt、精确角色/instance manifest、typed contribution 和 liveness/control attachment。
因此无法在上传 fragment 前验证完整意图，也无法自然表示空 fragment 的 service-only participant。Stage unary
结果未知时也缺少一个已经建立的控制 owner 来执行幂等查询、补偿和失联回收。否决。

### 选项四：InitQuery、StageFragments、StartPreparedQuery 三阶段

Init 先建立每个参与方的 query-level owner 和控制可达性；Stage 以每个 BE 的完整本地 fragment 集合作为一个原子
发布单元；所有 Stage ACK 到齐后，Start 才释放全局执行屏障。每阶段都以 execution identity、digest 和显式状态转移
支持未知结果重试、冲突拒绝及补偿终止，同时覆盖 service-only participant 与提前数据面流量。接受。

## 裁决

分布式查询采用 `InitQuery → StageFragments → StartPreparedQuery` 的三阶段逻辑启动序列。FE 只有在全部参与方完成
当前阶段并返回 ACK 后，才能推进到下一阶段。

`InitQuery` 必须在任何 fragment 执行之前创建 query-level participant entry，并绑定 execution attempt、精确的
participant roles 与 instance manifest、query-scoped typed contributions，以及 Stage 前所需的 liveness/control
attachment。Init 是幂等边界：相同 execution identity 与 init digest 的重试返回既有结果；相同 identity 携带冲突
内容时拒绝。若重试改变拓扑，或移除某个 optional typed contribution，必须创建新的 execution attempt，不得修改已经
初始化的 attempt。

`StageFragments` 对每个 BE 携带其完整本地 fragment set，并把它作为一个逻辑 batch。未来可以因大型 plan 的 payload
限制将传输物理地分块或流式化，但最终 publish 语义必须保持原子。BE 在 private workspace 中构建 pipeline、
exchange/result/report binding 和其他本地资源；local commit 之前，不得让任何 driver 变为 schedulable，也不得把
不完整资源发布为可执行状态。

Stage 必须用 Init 时绑定的 expected exact instance set 验证整个 batch。任何 missing、unknown 或 duplicate instance
都拒绝整个本地 Stage，不保留局部成功。相同 execution identity 与 stage digest 的重试返回既有结果；冲突的 Stage
内容必须拒绝。每个 participant 都必须经历 Stage：service-only participant 使用显式的空 fragment list，禁止伪造
empty fragment 来占位。

只有每个 participant 都返回 Stage ACK 后，FE 才发送 `StartPreparedQuery`。Start 是从 `Staged` 到 `Running` 的
轻量、幂等转移：在 `Initialized` 或 `Staging` 收到 Start 必须拒绝；在 `Running` 或 `LocallyDrained` 收到重复 Start
返回幂等 ACK；在 `Finalizing` 或 terminal state 收到 Start 只能返回稳定终态，绝不能复活该 attempt。

Start 是全局 barrier decision，但不是物理同时发生。某个 BE 已运行时，其他 BE 可能仍处于 `Staged`；因此 Staged
receiver 与 service 必须在有界资源约束下容纳少量 early ingress，或施加 backpressure，直到本地 Start 生效。它们
不得因为接收了提前流量而自行调度 driver。

任何 Init、Stage 或 Start 失败，都使 FE 对所有参与方 abort 当前 attempt。RPC 结果未知时，通过相同 digest 的幂等
重试确定状态，并对已成功参与方执行幂等补偿；control stream 丢失或 pre-start timeout 是无法确认时的 fail-close
兜底。该机制不是 ACID distributed transaction，而是 local atomic staging、global ACK barriers、idempotent
compensation 与 liveness-based convergence 的组合。

## 接受的妥协（诚实记录）

三阶段协议增加了启动 RPC 和全局 barrier latency；在网络延迟明显或查询本身很短时，这部分固定成本会直接增加
端到端耗时。系统还会在 `Staged` 状态暂时保留已构建但尚未运行的 pipeline、binding 和其他资源，因此需要明确的
内存上限、pre-start timeout 与回收策略。

实现和运维复杂度也会提高：FE 与 BE 都需要更多状态转移、digest、幂等判断、timeout、rollback、补偿及诊断代码。
per-BE batch 对大型 plan 可能超过合适的单次 RPC 大小，未来需要 chunked upload；这种物理传输优化不能削弱完整
batch 的 logical atomicity。

Start 在真实时间中仍是偏序的，不会消除 partial-start 窗口。系统仍必须实现 early-ingress 的有界
buffering/backpressure，并在失败后让 abort 与 liveness 机制收敛所有参与方。该设计明确选择 correctness 和显式
failure visibility，而不是最少 RPC 数；这些额外成本是为消除隐式 partial-submit 语义而接受的真实代价。

## 何时重新评估

- 产品永久移除 independent distributed execution，不再需要跨进程参与方与启动屏障时；
- 生产测量证明 startup latency 已成为主导成本，并且替代方案仍能保持 exact manifest、service-only
  participant、local atomic staging、全局 ACK barrier、幂等与 fail-close 等全部不变量时；
- dynamic fragment placement 或 rescaling 需要显式、版本化的 manifest amendment，而不能再以隐式 submit 表达时；
- plan payload size 要求 chunked staging 时；传输协议可以重评，但必须保留 logical atomic commit；
- 出现更强的 distributed transaction 或 coordinator recovery protocol 时；新协议可以替换补偿与恢复策略，但不得
  削弱 local staging atomicity 或 FE/BE process separation。
