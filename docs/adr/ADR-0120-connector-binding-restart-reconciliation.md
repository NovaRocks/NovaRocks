---
id: ADR-0120
title: "Connector Binding Restart Reconciliation"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: [ADR-0104]
superseded-by: null
date: 2026-08-27
provenance:
  - "PR: pending local implementation"
  - "discussion: 2026-08-27 connector binding recovery after frontend restart"
code-anchors:
  - "novarocks/spi/src/connector/execution_declaration.rs (ConnectorExecutionDeclaration)"
  - "novarocks/backend/src/connector/execution_host.rs (ConnectorExecutionHost)"
---

## 问题

Frontend 重启后失去本进程的 connector control generation 记录时，Backend 如何在不覆盖仍被 query 使用的精确 generation 的前提下，让同一 catalog 的新 generation 重新安装？

## 背景与执行事实

ADR-0104 将 Backend execution binding 的完整声明、digest、安装失败与 exact key lifecycle 收敛到 `ConnectorExecutionHost`。每个正常查询在 ensure 成功后取得 Backend-local query lease；fragment resolver 只能解析该 lease 中的 `(instance_id, incarnation)`，因此已运行查询不能以新 generation 继续读取。

但 connector control host 是 Frontend 的 ProcessRuntime。一次 clean Frontend restart 会从 durable catalog desired state 重建新的 connector control generation；Backend 仍是原进程，可能保留上一代已完成查询的 execution binding。新的 Frontend 不知道旧 exact key，无法用正常的精确 retire RPC 清理它。若 Backend 一律拒绝同 instance 的新 typed provider，catalog attachment 和 StateStore-backed MV 都无法在 FE restart 后恢复分布式查询。

## 考虑过的选项

1. **维持 exact retire 作为唯一替换方式。** generation ownership 表面上最简单，但新 Frontend 无法发现旧 key，正常 restart 永久卡在已完成查询遗留的 binding 上。
2. **新 ensure 直接覆盖旧 generation。** restart 可立即恢复，但仍在运行的旧 query 会失去 exact provider，或在新 provider 上误读；这破坏 query lease 与 generation fence。
3. **由 Backend 在 ensure 时回收无 lease 的旧 generation。** Backend 已是 query lease 的唯一事实 owner，因此可在同一 host lock 中证明旧 generation 没有任何活动 query；先 retire typed entry、移除旧 binding，再安装新 exact generation。Installing 或 retryable-failed cell，以及任一被 lease 的 generation，都返回 typed rejection。

## 裁决

采用选项 3，并以此 supersede ADR-0104 的“只随进程 drop 回收 generation map”限制。正常 exact-key retire 仍是 catalog drop/recreate 的优先路径；restart reconciliation 只处理同一 instance 的旧 generation 且没有 query lease 的恢复情形。

`ConnectorExecutionHost::ensure` 在创建新 cell 前枚举同 instance 的旧 key：任何旧 key 仍被 query lease 持有时，返回 `QueryIncarnationConflict`；旧 cell 正在 Installing 或 RetryableFailed 时，返回可重试的 `InternalFailure`；只有 Ready 或 TerminalFailed 且无 lease 的 key 才可从 binding map 移除，并同步 retire `TypedConnectorProviderRegistry` 中的旧 entry。随后按既有 digest/cell/install 规则安装新 generation。旧 fragment 因其 query lease 已不存在或 key 已退休而 fail closed，绝不被重绑定到新 provider。

## 接受的妥协（诚实记录）

这不是跨 Frontend failover 的 durable generation handoff：Backend 只能证明本进程当前没有 query lease，不能替代 durable operation 的 exact-generation recovery。我们选择它是因为 catalog read 与已完成 query 的遗留 execution binding 本来就是 ProcessRuntime；为它增加 durable ledger 或由新 FE 猜测旧 key，都会把短生命周期资源误做成 StateStore authority。

已退休 key 仍保留在 `retiring` set，避免迟到 old ensure 重新安装；该 set 目前随 Backend process 结束才释放。为恢复正确性接受这一小的进程级增长，而不是把 active query 置于错误的自动替换风险中。

## 何时重新评估

- Backend 持久运行且 catalog generation churn 证明 `retiring` set 或 retained completion state 带来可观内存增长时，定义有界 tombstone/eviction 与迟到 RPC 安全证明。
- 需要在 Frontend crash、不是 clean restart 的情况下保留或接管未完成的 connector write、mutation 或 maintenance operation 时，设计 durable exact-generation handoff；不得把本决策的无 lease 规则扩展到这些 operation。
- native control protocol 能安全枚举或确认旧 generation 且需要更强审计时，评估显式 restart reconciliation RPC；它仍必须以 Backend query lease 为活跃性权威。
