---
id: ADR-0124
title: "Native compatibility islands and ingress admission"
domain: [cluster-membership, distributed-query-lifecycle, runtime-role]
status: active
supersedes: []
superseded-by: null
date: 2026-08-28
provenance:
  - "discussion: 2026-08-28 native compatibility identity and rolling execution"
code-anchors:
  - "novarocks/version/src/lib.rs (native_build_identity)"
  - "novarocks/frontend/src/topology.rs (ClusterBackendService::record_heartbeat_success)"
  - "novarocks/backend/src/task_execution/registry.rs (TaskExecutionRegistry::establish_query_context)"
  - "novarocks/backend/src/exchange_receiver.rs (BackendExchangeReceiverPort)"
  - "novarocks/frontend/src/workload_lifecycle.rs (FrontendServingLifecycle::base_ready)"
---

## 问题

当同一 deployment 需要滚动替换 Native FE/BE binary 时，如何区分构建来源与实际可互操作的执行契约，并在 topology、
query Init 与 Exchange 数据面同时拒绝跨版本岛执行，而不引入协议协商、持久化升级状态或 all-in-one 旁路？

## 背景与执行事实

`native_build_identity()` 当前由 build input 或 Git commit 派生，并同时参与 Backend descriptor 与 Frontend topology
admission。它适合诊断 build provenance，却不能机械表示 repository Protocol descriptor、静态内建 Provider/carrier
集合和 Native execution wire 是否保持相同。仅以 build identity 划分会把无关 planner、日志或诊断改动误作不可互操作；
仅在 Frontend topology 过滤又无法阻止陈旧 snapshot 或直接 RPC 让异构 attempt 进入 Backend。

ADR-0111 已裁决 external orchestrator 是 Backend desired lifecycle 的唯一 owner，Frontend registry 只是可重建的
announce/heartbeat observation，并且只能在完整 `ControlReady` 前做零外部效果的 full re-plan。ADR-0113 已裁决 Native
IDL/validated codec 是跨进程结构与格式边界的唯一 authority；cross-message fence 保留，但不得以 message self-attestation
或第二份 digest authority 伪造兼容性。ADR-0119 已裁决 `/readyz` 只表达 FE-local base readiness，drain 是 FE-local
单向 admission transition，而不是 external route 或 deployment policy。

当前 Exchange ingress 可以在没有 query lifecycle authority 的 receiver key 上先解码，并通过 execution registry 建立
buffer。因而即使控制面拒绝异构 Backend，陈旧或迷路的 BE-to-BE frame 仍可能触碰 payload decode 与 receiver allocation。
生产真相是独立 `role=fe` 加一个或多个 `role=be`；all-in-one 只能复用同一 Native listener/channel 路径。

## 考虑过的选项

1. **继续用 BuildIdentity exact match。** 实现最小，但构建 provenance 过度决定执行互操作性，并不能表达 descriptor
   或静态 carrier contract 的断代；拒绝。
2. **引入 wire version range、negotiation、dual decoder 或 per-Provider partial compatibility。** 它可减少某些升级窗口的
   容量压力，但把 single atomic deployment contract 扩展为必须长期维护的组合矩阵，并要求新的 authority 与失败语义；拒绝。
3. **只在 Frontend topology 分岛。** 可避免普通调度跨岛，却不能覆盖 snapshot race、直接 Init 或已认证 Native data-plane
   frame；拒绝。
4. **以精确 `NativeCompatibilityId` 建立版本岛，并在 Topology、Init 与 Exchange ingress 三处闭合。** binary 在启动副作用
   前从 descriptor、闭合 static Provider/carrier manifest 与显式 epoch 派生 fixed-width identity；同 identity 的不同 build
   可互操作，其他 identity 可观察但不可调度。成功 Init 安装 execution-scoped ingress capability，数据面只接受 capability
   精确声明的 source/destination/sender tuple。选择此方案。

## 裁决

每个 Native binary 必须拥有一个 immutable、exact 的 `NativeCompatibilityId`。它是 compatibility admission 的唯一 key；
`BuildIdentity` 只保留 build provenance、日志与 rollout diagnostics。ID 的 canonical material 是 repository Protocol
descriptor、Server composition 唯一声明的闭合 Provider/carrier revision manifest 与 `NATIVE_COMPAT_EPOCH`。缺少、未知或
非法的 material/ID 一律 fail closed；不从 runtime catalog、config、StateStore、remote registry 或当前安装实例重新推导。

Frontend 以 exact compatibility identity 计算可调度 Backend：same-island Backend 继续满足 announce、exact heartbeat、
reported Running 与 endpoint ownership 等 ADR-0111 eligibility 条件；OtherIsland 是可观察的事实，不是 deployment error，
但绝不进入 eligible snapshot 或 query participant。descriptor 与 participant manifest 都携带 required fixed-width ID，
validated Protocol parse 拒绝缺失或非法值。

Backend 在创建 lifecycle entry、安装 runtime filter、预留 Exchange route 或创建 fragment record 前，先把 manifest ID
与本地 ID 精确比较。mismatch 返回 typed Init outcome，且只能在 `ControlReady` 前、尚未发生任何 external effect 时复用
ADR-0111 的统一 bounded full re-plan；不得在 ready 后 retry、migrate、resume 或刷新 statement semantic binding。

成功 Init 从 immutable participant manifest 的完整 route 事实一次性 prepare 并 activate Backend-local Exchange ingress
capability。remote frame 在读取 payload、Arrow decode 或 receiver allocation 前必须验证 exact source/destination/sender
tuple；已准入但 operator 尚未注册的 route 可以保留既有有界 early buffer。任何 terminal path 先 revoke capability，
再取消/删除 Execution receiver，并以有界 tombstone 拒绝晚到 frame。Backend lifecycle 拥有 admission/capability，
Execution 仍拥有 Arrow、receiver、sequence、EOS 与 buffer 语义。

`/readyz`、`/livez` 和 FE-local drain 语义保持 ADR-0119 不变。新增只读 island signal 仅表达
`base_ready && compatible_eligible_backend_count >= 1`；desired replicas、required catalog、容量与 external route 仍由
orchestrator/LB 组合，不进入 FE durable state 或 fixed threshold policy。compatibility-breaking rollout 通过外部 target
island + surge/blue-green 完成；NovaRocks 不持久化 upgrade phase，也不迁移 existing attempt。

## 接受的妥协（诚实记录）

这是一份保守的 exact-island contract。descriptor 或 manifest/epoch 任一变化都会分岛，即使某个实际改动或许能互操作；
我们接受额外 surge capacity 或计划停机窗口，换取没有 range、fallback、dual decoder 和长期 matrix 的可证明边界。

island readiness 只保证存在至少一个同岛 eligible Backend，不保证 `1FE+NBE` 目标容量、catalog 完整性、缓存热度或业务
SLO。把这些事实塞进 FE 会重复 external orchestrator 的 authority；平台必须自行组合 count、catalog 与 route policy。

Ingress capability 增加 Backend ProcessRuntime 的 route ledger、capacity、tombstone 与终止排序复杂度。我们接受这项状态与
实现成本，因为在 Execution global registry 中猜 query authority，或在未 Init frame 上先解码，都会破坏 FE/BE owner
边界。该 decision 只面对同 deployment 内已认证成员的陈旧/迷路 frame；它不是对恶意已认证 BE 的 process attestation。

## 何时重新评估

- 产品需要独立发布 Models、role-specialized binary、仓库外 Provider plugin 或真正的 mixed-version compatibility
  window 时，先定义新的 version/negotiation authority 与测试矩阵，不能扩张本 decision 的 exact ID。
- 真实 workload 证明 exact descriptor/manifest boundary 造成不可接受的 surge 或维护窗口成本时，提出带明确 wire、
  failure、security 与 deprecation 证据的替代设计；不得直接添加 fallback。
- threat model 包含同 deployment 内恶意但已认证的 BE 时，设计 caller process attestation；不得把 compatibility ID
  当作 frame credential。
- 多 FE active-active、attempt migration/resume、durable upgrade workflow 或跨 deployment handoff 成为产品目标时，
  先重新裁决 query ownership、fencing 和 durable authority。
