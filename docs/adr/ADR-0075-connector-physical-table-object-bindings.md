---
id: ADR-0075
title: "Connector physical table object bindings"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-16
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/909"
  - "discussion: 2026-08-15 durable statistics table identity and rebinding"
code-anchors:
  - "novarocks/spi/src/connector/metadata.rs (ConnectorMetadata::capture_table_object_binding)"
  - "novarocks/connector/iceberg/src/control_provider.rs (IcebergControlProvider::current_table_object_binding)"
---

## 问题

当一个 durable caller 需要确认逻辑表名仍指向同一个物理表对象时，Connector 边界如何防止 drop/recreate 的 ABA，而不把统计能力、第二套 lease 或 provider table 对象泄漏给调用方？

## 背景与执行事实

`ConnectorTableIdentity` 是 catalog/namespace/table 的逻辑名称；同名表被 drop 后 recreate 时它不变，不能证明持久化工作仍针对原来的物理对象。Iceberg metadata 的 UUID 能识别该替换，但该 UUID 必须和返回的 metadata handle 来自同一次 catalog observation；另一次 latest lookup 只能得到调用顺序假设。

现有 `ConnectorControlPlanningLease` 已经固定 metadata 的 instance 与 incarnation。`ConnectorMetadata` 是该 exact lease 暴露的 metadata owner；统计 capability 则是可选的、FE-only 的独立能力（ADR-0022），不能成为 generic table rebinding 的依赖。

ADR-0060 已为 MV refresh 裁决：从同一份 exact metadata 投影 UUID 与 current snapshot 的 consumer-owned observation 不能为这个单一 use case 新建 Connector capability。现在出现了跨 durable caller 的「只确认当前物理对象仍相同」需求，触发了该 ADR 列出的通用化重评估条件；本 ADR 只承接 object-ID capture/rebind，既不推翻 MV-local observation，也不把其 snapshot、schema 或 target-publication 事实扩大到通用契约。

## 考虑过的选项

**A：把 UUID 重绑附着在 statistics capability。** 这会让无统计 consumer 无法使用本来属于 metadata 的物理对象事实，并把可选统计实现误变成 catalog identity 的权威。

**B：新建 object-binding capability、resolver 或 lease。** 该方案表面更显式，却复制 exact-generation 装配、释放与 conformance 责任，并制造平行 authority。

**C：调用方持久化 logical name 后在恢复时直接再次 load latest。** 实现最少，但无法区分同一对象的新 metadata 与同名 replacement，drop/recreate 会静默越过 durable boundary。

**D：在既有 `ConnectorMetadata` 上提供可选的 capture/rebind 方法。** capture 返回 opaque、bounded `ConnectorTableObjectId` 与同次观察产生的 `ConnectorTableMetadata`；rebind 要求非 optional expected ID，并对 missing/replaced 返回 typed terminal failure。

## 裁决

采用 **D**。调用方只可经 retained exact `ConnectorControlPlanningLease` 取得 metadata 后调用此可选方法；新 API 不新增 resolver、capability 或 lease，也不进入 native FE/BE wire。

`ConnectorTableObjectId` 是 bounded opaque bytes：Core 与 Frontend 可以比较和持久化，不能解析、改写或由 logical name 合成。唯一 selector 是 `Current`，未来 selector 必须由 provider 显式处理；未知 selector fail closed。未广告此能力的 provider 返回显式 `Unsupported`。

Iceberg 在一次 metadata load 中从封存 payload 的 table UUID 生成 ID。rebind 的目标缺失必须返回 `Missing`，UUID 不同必须返回 `Replaced`；两者都不可在进度前 retry，且不得向 caller 返回 replacement 的 handle。StarRocks 不实现该可选能力，保持 `Unsupported`。

ADR-0060 的 `MvStorageObservationPort::observe_refresh_base` 继续是 MV 的同版本 UUID/snapshot 投影入口；它的 consumer-owned owner 与无额外 runtime IO 前提保持不变。这里的通用 metadata contract 只给未来 durable caller 一个 opaque identity gate，不能反向替代或扩展该 MV port。

## 接受的妥协（诚实记录）

这个边界给每个支持 provider 增加了 capture/rebind 样板、typed failure 和 conformance 测试；不选新 capability 的原因是避免重复 authority，不是因为在 metadata trait 上继续加方法天然更好。方法继续增长时，metadata trait 可能变成杂物集合，代价由 owner 负责审查。

当前只支持 `Current`，因此不能表达历史 snapshot 或 ref 的物理对象绑定。这是有意收窄：未经独立裁决的 reference 语义若被伪装成 current，会比拒绝更危险。

## 何时重新评估

- 多个无关 consumer 需要跨进程传输或跨 generation 恢复 object binding 时；届时要单独定义 versioned durable/wire evidence，不能直接复用 FE-local metadata handle。
- provider 只能用额外 runtime IO 才能取得稳定 physical identity，或 ID 超过有界 durable budget 时；需要重新裁决 observation 与预算边界。
- 历史 reference、branch 或 snapshot 需要绑定语义时；必须为 selector 和其 provider proof 增加明确契约，而非把它们静默当作 `Current`。
- `ConnectorMetadata` 的 optional use cases 持续增长到无法按 owner 解释时；应按 consumer 拆窄 port，不应回退到 service locator 或 unbounded metadata dump。
