---
id: ADR-0129
title: "Attempt-scoped collection of vended credentials before metadata materialization"
domain: [provider-spi, distributed-query-lifecycle, native-transport-security]
status: superseded
supersedes: []
superseded-by: ADR-0145
date: 2026-08-30
provenance:
  - "discussion: 2026-08-30 CCA-1 vended credential ownership and timing"
code-anchors:
  - "novarocks/frontend/src/query_execution/compiler.rs (FrontendQueryCompiler preparation boundary)"
  - "novarocks/frontend/src/coordinator/execution.rs (FrontendDistributedQueryCoordinator::execute_request)"
  - "novarocks/connector/iceberg/src/metadata_context.rs (IcebergMetadata::load_table_classified)"
---

## 问题

当 Iceberg REST `load_table` 在 query preparation 期间返回短期 storage credentials，而 native `QueryExecutionId`
原本在其后的 coordinator 才生成时，系统应如何保持同一次 metadata observation 的 snapshot/schema 与凭据 scope 一致，
又不把 secret 暂存进 table、cache、plan 或执行 handle？

## 背景与执行事实

Iceberg REST adapter 在一次 `load_table` response 中同时得到表事实和 access delegation。connector 已把 delegation 投影为
move-only、默认脱敏的 response-local seed；当前 `IcebergMetadata::load_table_classified` 在写入 physical table cache 前拒绝
未被 query attempt 消费的 seed。这是安全失败，不是 provider 不支持 vended response。

query catalog admission 与 preparation 发生在 `FrontendDistributedQueryCoordinator::execute_request` 之前；后者才生成
`QueryExecutionId`。下游并非缺少 carrier：`QueryInitOptions::with_credential_leases` 已把机密 lease 与 manifest 分离，
Init materialization 在 TLS-only vended admission 下把 envelope 送到 BE，BE lifecycle entry 负责本 attempt 的安装和清理。

将 seed 塞入 `Table`、`FileIO`、physical cache、query binding、SQL plan、fragment 或 handle 会把一次 query 的 capability
扩展成可跨请求存活的状态；第二次 `load_table` 补取 credential 又可能观察到不同 snapshot/schema。因此二者都不能作为交接。

## 考虑过的选项

1. **把 seed 缓存在 table/cache/plan，等待 coordinator 分配 id。** 实现看似局部，但 credential 会跨 query 或错误路径保留，
   并进入现有可观察或复用对象；拒绝。
2. **保持现有时序，在分配 id 后再调用一次 `load_table`。** 不需要改 query preparation seam，但无法证明第二次 response 与
   已规划的 table facts 是同一次 observation；拒绝。
3. **将 credential 放进 scan、writer 或 fragment handle。** 可随任务下发，但违反 secret 不进 digest/handle 的闭合边界，
   并重演凭据在可观察 query JSON 泄漏的风险；拒绝。
4. **在 metadata materialization 前 mint candidate attempt identity，并用 FE-local collector 立即消费 seed。** collector 在
   成功准备后仅通过既有 lifecycle Init 出域；采纳。

## 裁决

每个将进入分布式执行的 candidate attempt 必须先 mint `QueryExecutionId` 和 FE-local、move-only
`AttemptCredentialLeaseCollector`，再开始会产生 vended response 的 metadata/table materialization。provider 只把
response-local seed 移交该 collector；collector 按 non-secret catalog owner 与规范化 scope 去重，并只持有
`QueryCredentialLeases` 与受限 refresh source。

准备成功后，coordinator 在 `QueryInitOptions::new` 与 `initialize_query` 之间将 collector 的 leases move 到
`with_credential_leases`。descriptor 仍进入 manifest digest，secret envelope 仍只走 TLS-protected Init；本 ADR 不改变
native wire、TLS gate、BE refresh ownership或 fragment submission barrier。

planning error、cancel、collector drop 和 attempt terminal 必须清空 material。Init RPC retry 可重发同一内存 lease；pre-ready
topology retry 是新 attempt，必须创建新 collector 并通过重新 planning 获得新 metadata observation，绝不复用旧 seed。
没有 candidate query attempt 的 vended catalog DDL、后台 maintenance 和 create/staged-create response 在 v1 fail closed；它们
若需要支持，必须另行定义 operation-attempt owner。

## 接受的妥协（诚实记录）

我们接受把 attempt identity 的创建前移并扩大 FE preparation-to-coordinator 的显式交接面。这不是因为它让代码更短，
而是因为安全地缓存 seed 或重新读取 metadata 都会损害 capability scope 或 observation 一致性。该选择也意味着普通 catalog
操作不能顺带获得 vended 支持，直到它们拥有等价的 operation attempt 生命周期。

## 何时重新评估

1. Iceberg REST 或底层 SDK 提供可证明的原子 metadata-plus-credential snapshot token，且可在不二次观察表的条件下续租时。
2. NovaRocks 引入有明确 owner、取消与恢复语义的 catalog DDL/maintenance operation-attempt，使其能安全消费短期凭据时。
3. vended credential collection 的额外 preparation 交接在生产 profile 中成为可测瓶颈时；可优化内部数据流，但不得放宽
   attempt-only、single-observation 或 TLS-only contract。
