---
id: ADR-0019
title: "Frontend owns the INSERT application flow"
domain: [frontend-dml]
status: superseded
supersedes: []
superseded-by: ADR-0021
date: 2026-07-31
provenance:
  - "PR: frontend query session and router ownership #763"
  - "PR: connector control and execution role separation #772"
  - "PR: atomic native query Stage/Start #777"
  - "discussion: 2026-07-31 frontend INSERT ownership and crate integration closeout"
code-anchors:
  - "novarocks/core/src/engine/insert_engine.rs (InsertEngine)"
---

## 问题

在 frontend 已拥有 statement admission 和 application routing 之后，standalone INSERT 的 conversion、dispatch、
batch shaping 与写事务编排应由哪个 crate 拥有，connector、query execution 和 external commit truth 又应如何保持在
core 内而不形成反向依赖或公共 SPI？

## 背景与执行事实

frontend query session 已按 ADR-0012 成为 statement admission 与 router owner。每个 statement 在 admission 时按
ADR-0011 冻结唯一的 `RequestContext`，其中 topology snapshot、deadline、cancellation 与 optimizer settings 必须贯穿
后续 query 和 distributed write。把 INSERT 留在 core command kernel 会使 application router 只迁了一半，并让 core
继续同时拥有 SQL command conversion、target policy、batch shaping、transaction lifecycle 与 connector I/O。

Connector catalog、table sink、distributed query execution、Iceberg writer 和 external commit 都仍是 core 的真实执行
能力。它们包含 engine-private payload，不能泄漏给 frontend，也不能因为跨 crate 调用就进入 provider SPI。ADR-0006
规定一对一 consumer port 不是产品级可替换 provider；ADR-0009 已证明 frontend application owner 通过窄 core engine
port 调用 external-system truth 的依赖方向可行。

frontend DML 已有 write transaction runner，但其 durable operation truth 必须落到 host-owned StateStore，而不是让 core
session 持有 frontend service callback，也不是继续使用 metadata bridge。frontend query router 合入后，core-held
`InsertService` callback 还会产生 frontend 与 `StandaloneState` 的 ownership cycle，并保留一条难以证明已经关闭的
dual route。

## 考虑过的选项

**frontend 拥有完整 INSERT application flow，core 暴露窄 reverse port。** frontend 负责 raw INSERT AST 到 command 的
conversion、所有 target 的 dispatch、VALUES/query batch shaping、Iceberg transaction runner 与成功后的 statistics
observation；core 通过 object-safe `InsertEngine` 提供 target resolution、query/connector execution 与 typed commit
truth。跨边界只传中立 DTO、Arrow batch 和 `Send + Sync` opaque handle。

**只迁 Iceberg INSERT。** 改动较少，但 local、StarRocks 与 Iceberg 会拥有不同 application owner；conversion、
`UNION ALL`、overwrite policy、shaping 与 statistics sequencing 继续分叉，frontend 也不能成为唯一 INSERT router。

**由 core session 持有 frontend `InsertService` callback。** 能复用 core 现有 dispatch，却反转 crate 依赖或迫使
composition 引入 service locator/ownership cycle；同时会让已建立的 frontend router 退化成转发壳。

**把 INSERT contract 放进系统 SPI。** 表面上统一了跨 crate 类型，但 `InsertEngine` 只有一个 frontend consumer，
并不表达可替换 provider 产品能力。这会违背 ADR-0006，也会把 SQL application 细节错误地固化成长期 SPI。

## 裁决

frontend 是 standalone INSERT 的唯一 application owner 和 production router。`DmlService` 识别并转换 INSERT，拥有
all-target dispatch、batch shaping、Iceberg write transaction orchestration 与 statistics sequencing；非 INSERT 才进入
core command kernel。DML operation journal 由 frontend host 以 StateStore 持久化。

core 定义 object-safe `InsertEngine`、中立 request/result DTO、纯 normalization + raw sqlparser INSERT primitive，以及
Iceberg prepared/commit opaque handles。frontend 不接触 `StandaloneState`、connector registry/backend concrete、
coordinator-private result或 Iceberg catalog/table/collector。所有会进入 query 或 distributed write 的 request 都携带
同一个 admitted `QueryExecutionContext` 的 clone；core adapter从它派生 connector context，保留同一 deadline 与
cancellation，并继续复用既有 query execution lifecycle。

`InsertEngine` 是 aggregate core 被进一步拆分前的过渡 cut seam，不进入系统 SPI，也不被视为 core 的永久 application
owner。未来 SQL kernel、provider 和 application crate 完成物理化时，可以在保持 frontend DML contract 和行为语义的
前提下移动或删除该 adapter。

## 接受的妥协（诚实记录）

这项裁决引入了一组看似重复的中立 DTO，以及 prepared write/commit opaque handle 的传递。它们增加了 adapter 和 fake
测试成本；选择它们不是因为 plumbing 本身更优，而是因为这是在不泄漏 core private payload、不反转 crate 依赖、也不
误建公共 SPI 的情况下完成 hard cut 的最低长期成本。

Iceberg INSERT 在 StateStore 未配置时必须在任何 writer side effect 前 fail-fast，而 local/StarRocks INSERT 仍可工作。
这让不同 backend 在配置缺失时的可用性不完全一致，但明确的 capability matrix 比 metadata fallback 或内存 durable
假象更诚实。

CTAS 暂时保留 core-owned composition，等待其自身 application owner 迁移。这会在过渡期保留部分 shared write helper；
接受这一点是为了保持单个变更的行为边界和回滚能力，不代表 CTAS 应长期绕过 frontend。

## 何时重新评估

- aggregate core 的 SQL kernel/provider/application closeout 已完成，`InsertEngine` 的现有物理位置不再是合理 cut
  seam，应移动或删除 adapter；
- INSERT capability 出现多个真正可替换的产品 provider，且具有稳定的跨实现 conformance 需求，此时按 ADR-0006
  重新判断是否形成系统 SPI；
- CTAS 的 application owner 迁入 frontend，可删除为其保留的 core write composition；
- StateStore 的部署契约或多 frontend fencing/takeover 模型改变，需要重新裁决无 StateStore capability matrix 或
  operation journal authority；
- immutable request context 或 native query lifecycle 被新的 ADR supersede，需要同步检查 INSERT query/write 的
  identity 透传契约。
