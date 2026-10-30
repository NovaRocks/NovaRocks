---
id: ADR-0111
title: "Backend self-registration and pre-ready distributed replanning"
domain: [cluster-membership, distributed-query-lifecycle]
status: active
supersedes: [ADR-0013]
superseded-by: null
partially-superseded-by:
  - id: ADR-0198
    scope: "Whole semantic replanning before ControlReady and pre-ready retries for mutating execution; external desired membership, authenticated registration, exact heartbeat and process identity remain active"
    link: "ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md"
date: 2026-08-25
provenance:
  - "discussion: 2026-08-25 serverless backend self-registration, drain, and pre-ready admission"
code-anchors:
  - "novarocks/frontend-application/src/topology.rs (ClusterBackendService)"
  - "novarocks-server/library/src/roles/backend.rs (run_until_shutdown)"
  - "novarocks/frontend-application/src/coordinator/execution.rs (FrontendDistributedQueryCoordinator)"
---

## 问题

面向外部编排器的计算存储解耦部署中，Frontend 如何只根据可丢失的 Backend 运行期观察进行新查询调度，
同时在执行尚未开始前安全处理 Backend 拓扑竞态，而不把 placement、进程身份或旧 attempt 恢复重新纳入
Frontend 权威？

## 背景与执行事实

外部 orchestrator 已经拥有 Backend 副本数、模板、创建、替换、缩容和终止的权威。将 endpoint seed、
SQL ADD/DROP 或 StateStore 中的 desired membership 同时保留为 Frontend authority，会使两套控制面可以对
同一进程作出冲突决定。

endpoint 不能表示一次进程生命期：同一 Pod/DNS/port 可以被新进程复用。Frontend 的本地整数 id 也不能
跨 Frontend 重启或 endpoint 替换表示该进程。Native query lifecycle 已有 Init + ControlReady 屏障：在所有
participant ControlReady 之前，还没有 Stage/Start 或 fragment execution；屏障之后则可能已有外部 read/write
和不可重放的本地执行状态。

topology 会影响 optimizer cost、fragment parallelism、connector split assignment、writer cohort、exchange route
和 runtime-filter participant。缩容后仅替换 backend endpoint 或复用旧 distributed artifact 会遗留指向旧
participant 的 split/write/RF 事实，可能造成静默漏读或漏写。Lake publication 以一次 statement 的固定
publication identity、exact source/base binding 和 crash-only external outcome 为边界；任何 staging、writer 或
publication dispatch 之后不能因 topology 竞态取得另一轮自动重试授权。

Native wire 的 generated DTO、validated value 和 canonical codec 由 ADR-0106 的分层拥有；本决策不能建立
第二套 registration parser、build gate、digest 或 transport authority。Native listener/security policy 仍由
独立的 listener/trust 决策提供。

## 考虑过的选项

1. Frontend 持久化 desired membership，并以 seed/SQL mutation 管理 Backend。它能在 Frontend restart 后立即
   列出旧 endpoint，但与外部 orchestrator 重复控制，且无法让 endpoint 自身证明当前进程身份。
2. 只让 Frontend 从 Kubernetes DNS/Endpoints pull discovery。它省去 BE→FE announce，但把 Frontend 绑定到
   特定编排器和 DNS 语义；NAT/非 Kubernetes 部署较差，DNS/endpoint 复用仍不能证明进程身份。
3. Backend push announce，Frontend pull exact heartbeat 验证；Frontend 维护内存 facts 并派生 eligibility。它
   让 orchestrator 保持 desired owner，且让 endpoint 只能在 process identity 被验证后参与调度。选择此方案。
4. topology 变化后复用旧 plan、只替换 participant，或在任意 query failure 上做 task retry。它改动较少，但
   无法证明 split/write/RF 完整性，也越过了 crash-only lifecycle 和 publication 边界。
5. 在 ControlReady 前丢弃整个 distributed round、保持 statement semantic binding 后完整重规划。它增加一次
   planning 延迟，但把自动动作限制在尚未执行、可证明零外部效果的 admission 候选。选择此方案。

## 裁决

外部 orchestrator 是 Backend desired lifecycle 的唯一权威。Backend 在每次进程启动时生成 UUIDv7
`BackendProcessId`，并周期性向 Frontend 现有 Native ingress announce immutable descriptor；Frontend 对
该 endpoint 发起 authenticated heartbeat，只有两条路径返回同一 process id、endpoint、deployment scope、
reported state 与 compatible build identity 后，entry 才可参与调度。

Frontend registry 只存在于内存，保存 lease validity、identity verification、reported Running/Draining、
compatibility 和 endpoint ownership 等正交 runtime facts。`eligible` 是这些 facts 的纯派生谓词；topology
revision 只在 eligible `(BackendProcessId, endpoint)` 集合改变时递增。StateStore membership、seed、ADD BACKEND
和 DROP BACKEND 不再是 membership authority。旧 StateStore key 停止读取/写入，但不在该行为切换中物理删除。

Backend 收到 SIGTERM 后单向进入 Draining：继续 announce、拒绝新的 InitQuery、允许已有 lifecycle 完成，并在
本地 active lifecycle 归零后退出。Frontend 不发送 drain command，也不提供“可以关闭”的 ack；orchestrator
负责最终 termination grace period。heartbeat/announce failure 只决定未来调度 eligibility，已有 query 的命运
仍由其 query lifecycle/control/fragment transport 决定。

statement 在没有 eligible Backend 时仅在原始 deadline 内有界等待。若所有 participant 尚未 ControlReady，且
Backend Draining、process mismatch 或 eligibility 失效导致 admission candidate 失败，Frontend 可先 Abort
已 Init participant，再用新的 protocol attempt identity 完整重新派生 distributed plan。新的 round 保持首次
冻结的 parsed input、catalog/lake exact semantic binding、DML base binding 和 publication identity；必须重算
optimizer、split、writer、schedule、exchange、runtime-filter、manifest 与 digest，禁止局部 patch。

mutating statement 只有在一个单调 effect tracker 正面证明未发生任何 staging、writer 或 publication dispatch
时才可进入该 pre-ready retry。完整 ControlReady、Stage/Start、任何已发生或未知 external effect、publication
unknown，以及 Frontend 进程死亡都会永久关闭 retry。该动作不是旧 attempt recovery、adoption、resume 或
post-ready task reschedule。

## 接受的妥协（诚实记录）

Frontend restart 后 registry 会短暂为空，需要等待 Backend renew announce 与 heartbeat 才能重新调度；我们接受
这段恢复延迟，因为它避免让过期 endpoint 或 durable desired state 冒充当前进程身份。raw diagnostic entry只在
内存中有限保留，不能提供审计级历史。

pre-ready retry 会重复 optimizer 和 connector planning，尾延迟也会进入原始 query deadline。我们接受这项成本，
因为完整重派生是防止 topology-dependent artifact 漏掉或误指向 participant 的最小正确性条件；当前没有以局部
patch 安全降低该成本的证明。

单 deployment、单 Frontend registry 和同构 build 的限制仍然存在。它们不是最通用的 serverless 平台模型，
但在没有多 Frontend convergence、compatibility epoch 或安全 mixed-version 协议时，扩大范围只会制造第二权威。

## 何时重新评估

1. 需要多 Frontend active-active、Frontend takeover 或跨进程 registry convergence 时，先定义 lease、fencing、
   replicated observation 和 query ownership，不能把内存 registry 直接复制。
2. 需要 mixed-version/rolling upgrade 时，先由独立 compatibility contract 取代 build identity exact match，
   并定义升级岛与 protocol window。
3. 真实负载显示 pre-ready replan 的 planning 成本不可接受时，可研究 admission 前稳定 placement 或更细粒度
   artifact partition；必须先证明不会复用 topology-dependent split/write/RF facts。
4. 部署必须依赖 orchestrator-specific pull discovery 或多 warehouse capacity policy 时，先定义新的 desired
   lifecycle authority，不能把 Kubernetes client 或 ManagedPlacement 塞回 Frontend registry。
5. 若 listener/trust 决策改变 Native authenticated ingress 的上下文或 all-in-one composition，保持一条明确
   supersession 链并重新验证此决策不引入 plaintext、direct-call 或第二 membership authority。
