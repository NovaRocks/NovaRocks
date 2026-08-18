---
id: ADR-0084
title: "Durable statistics job target binding"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-18
provenance:
  - "discussion: 2026-08-18 durable statistics job target binding acceptance"
code-anchors:
  - "novarocks/frontend/src/statistics_jobs/application.rs (StatisticsTargetResolver)"
  - "novarocks/frontend/src/statistics_jobs/model.rs (StoredStatisticsJobV3)"
---

## 问题

一个可恢复的统计 job 如何持久化其表目标，使后续 attempt 既能在运行时取得所需的数据版本，又不会把同名 drop/recreate 的替换表误当成原目标，或把 provider runtime handle 写入 durable record？

## 背景与执行事实

统计 job 是 Frontend 拥有的 durable application lifecycle：提交时需要记录跨重启可解释的目标，worker 在之后的 attempt 中才执行 collection 与 publication。`StatisticsTargetResolver` 是 Frontend 在提交与 attempt 边界使用的 application port；`StoredStatisticsJobV3` 是 statistics job 的 durable schema owner。durable record 受 ADR-0074 的 canonical 编码与整记录预算约束，不能保存 reader、scan artifact、sketch 或其他可执行 runtime 对象。

逻辑 catalog、namespace、table 名只回答“现在这个名字解析到哪里”，不能区分原表被 drop 后以同名 recreate 的 ABA。ADR-0085 已定义：durable caller 经既有 exact metadata lease 捕获并重绑 opaque `ConnectorTableObjectId`，以 typed `Missing` 或 `Replaced` 拒绝对象消失或替换；该 object identity 不等同于 schema version、data version 或逻辑表名。

统计的 data version 是 attempt-time collection、publish 与逐 metric basis 判断所需的 provider 事实。它会随同一物理对象的新数据演进，因而不能代替 object identity：仅保存 data version 无法证明一次同名替换不是“恰好取得了另一张表的版本”。ADR-0022 所定义的 statistics capability 仍是可选、FE-only 的 provider capability；它不是一般表身份的 authority。

## 考虑过的选项

**A：持久化 logical target 与 submission-time data version。** 记录较少，但 data version 不证明物理对象连续性；恢复时 latest name lookup 会让 drop/recreate 静默越过 durable boundary。

**B：持久化 provider table handle、scan artifact 或 collection runtime。** attempt 可以少一次准备，却把具有 provider 生命周期、大小增长或不可跨重启语义的对象写入 Frontend durable record，也违反 ADR-0074 的有界 durable 模型。

**C：持久化 logical target、physical object ID、column intent，并在每次 attempt 重新解析当前 data version。** logical target 保留用户意图与诊断入口；opaque object ID 作为对象连续性 gate；column intent 只表达需收集的列集合，不携带执行物；attempt 在同一 exact metadata observation 中重绑对象后取得其当前 data version。

**D：将 object capture/rebind 加入 statistics capability，或为 statistics 新建平行 resolver/lease。** 这会把 ADR-0085 的 metadata identity authority 重新附着到可选统计能力，复制装配与 conformance 责任，并让无统计 consumer 无法复用相同 identity contract。

## 裁决

选择 **C**。statistics job durable record 只保存以下 target binding：

- logical target（catalog、namespace、table）；
- 经 ADR-0085 捕获的 opaque bounded physical `ConnectorTableObjectId`；
- column intent，即请求统计的列集合与其已验证的名称约束。

它不得持久化作为未来 attempt 输入的 data version、provider table handle、metadata handle、reader、scan artifact、sketch 或其他 attempt-local/executable 对象。提交时 capture logical target 和 physical object ID；每个 attempt 在既有 exact metadata lease 的同一次 observation 中 rebind expected object ID，只有成功后才 resolve 该物理对象当前的 data version，并将该版本用于本 attempt 的 collection、publication 和证据判断。成功跨过 publication boundary 时，record 可以以有界 opaque terminal evidence 保存这一次已发布的 basis data version；它只用于诊断和策略证据，绝不被恢复 attempt 复用。`Missing`、`Replaced`、`Unsupported` 必须按 typed outcome 终止或明确分类，绝不以 latest logical-name resolution、旧 data version 或 replacement handle 继续执行。

本决策收紧 ADR-0022 的 consumer usage：statistics consumer 必须把 statistics capability 用于 statistics facts，而不能用它证明表对象身份。它不改变 statistics capability 的 owner、可选性、FE-only 边界或 provider 责任，也不 supersede ADR-0022。physical object ID 的 capture/rebind 仍完全由 ADR-0085 的 metadata capability、exact lease 与 provider conformance 契约拥有；本 ADR 仅规定 statistics durable caller 如何消费该契约。

## 接受的妥协（诚实记录）

每次 attempt 都要重新取得 metadata observation 并 resolve 当前 data version，不能复用提交时的版本，因此增加了 provider 调用和失败面。选择这一成本是为了让同一物理表的数据演进仍可被正常统计，同时把同名替换 fail closed；这不是因为 attempt-time resolution 更快或实现更简单。

column intent 保存的是名称级用户请求，而非冻结 schema/field ID。列 rename、drop 或类型语义变化会在 attempt-time resolution 中得到显式失败或由 statistics owner 定义的保守分类，而不会由 durable record 猜测映射。当前决策也不解决历史 reference、branch 或 snapshot 的 target binding；这些语义若需要 durable 支持，必须另行裁决。

## 何时重新评估

- statistics job 需要绑定历史 snapshot、branch、tag 或非 `Current` selector 时；必须扩展 ADR-0085 的 selector/proof 契约，而不能把它们伪装为 logical target。
- provider 无法在有界 metadata observation 中同时 rebind physical object ID 与 resolve data version，或该 identity 超过 ADR-0074 的 durable budget 时；需要重新裁决 observation 与 durable evidence 的边界。
- 统计 collection 需要稳定 schema/field identity，而 column name intent 不能表达正确的 rename 或 evolution 语义时；应单独裁决列 identity contract。
- 多个非统计 durable consumer 需要相同的 attempt-time version resolution policy 时；可抽取 provider-neutral consumer guidance，但不得把 identity authority 从 ADR-0085 移入 statistics capability 或新增平行 lease。
