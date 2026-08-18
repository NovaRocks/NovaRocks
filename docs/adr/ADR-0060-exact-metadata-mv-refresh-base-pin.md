---
id: ADR-0060
title: "Exact metadata projection for MV refresh base pins"
domain: [provider-spi, frontend-mv]
status: superseded
supersedes: []
superseded-by: ADR-0086
date: 2026-08-12
provenance:
  - "PR: MV production owner-cut readiness consumer migration (PR number pending merge)"
  - "discussion: 2026-08-12 exact-generation MV base UUID and snapshot pinning"
code-anchors:
  - "novarocks/spi/src/connector/mv_storage_observation.rs (MvStorageObservationPort::observe_refresh_base)"
  - "novarocks/connector/iceberg/src/storage_inspector.rs (IcebergStorageInspector::observe_refresh_base)"
---

## 问题

MV refresh 如何从同一个 exact Connector generation、同一份已封存 metadata 中取得 base table UUID 与
current snapshot，而不让 Core重新获得 provider table/catalog authority，也不把这个窄需求扩大成通用 metadata
dump？

## 背景与执行事实

MV refresh pin要同时记录 base table UUID与current snapshot。UUID用于识别drop/recreate ABA，snapshot用于定义
本次 refresh的水位；二者如果来自不同metadata version，就无法证明pin描述的是同一个外部表状态。

旧路径通过一次concrete Iceberg table load天然取得两者，但代价是Core持有catalog entry、catalog client与`Table`，
阻止provider owner cut。现有中立边界已经提供`ConnectorControlPlanningLease`、`ConnectorTableMetadata`与
composition-injected `MvStorageObservationPort`：application先在retained exact lease上加载一次metadata，只有
provider inspector能解释其opaque handle。该handle已封存纯`TableMetadata`，因此投影UUID与current snapshot
不需要FileIO、catalog reload或latest-generation lookup。

已有refresh-target observation还携带schema、partition、refs、lineage与publication marker；这些是target apply
需要的事实，不是base pin需要的事实。已有read-reference facts与schema observation则是独立调用，拼接它们会把
一次metadata load的同版本保证降为调用顺序假设。

## 考虑过的选项

**A：复用完整refresh-target observation。** 不增加port方法，但每个base pin都会解码并分配schema、partition、
refs、lineage与marker。更重要的是，它把target publication语义错误地泛化为所有base table契约，使未来字段扩张
自动进入不需要它们的热路径。

**B：组合既有reference/schema observations。** 表面上不增加类型，但UUID与snapshot来自两次独立load；并发提交、
cache invalidation或generation变化会让调用方得到一个无法证明同版本的组合值。用调用顺序或latest retry补救会削弱
drop/recreate与snapshot drift的fail-closed语义。

**C：新增Connector SPI capability。** 可以显式命名base pin，却会复制exact lease、table handle与metadata lifecycle，
并把一个application-owned纯投影误升格为所有provider都必须实现的新系统能力。该事实不需要provider runtime IO，
没有建立平行capability的收益。

**D：在既有Core-owned observation port增加一个窄use-case方法。** 调用方把同一 exact lease加载的同一个
`ConnectorTableMetadata`交给inspector；返回值只包含exact table identity、非空UUID与`Option<i64>` current
snapshot。

## 裁决

采用 **D**。

`MvStorageObservationPort::observe_refresh_base`只接收retained exact planning lease、该lease加载的
`ConnectorTableMetadata`与bounded request context。Provider inspector必须先验证lease instance、opaque handle owner、
namespace/table identity与frozen metadata identity一致，再从同一`TableMetadata`投影UUID和current snapshot。

返回的`MvRefreshBaseObservation`只包含：

- exact `ConnectorTableIdentity`；
- 非空table UUID；
- `Option<i64>` current snapshot ID。

它不携带schema、partition、refs、properties、manifest、provenance、catalog client或provider table object，也不触发
FileIO。空UUID、负snapshot identity、metadata/lease identity mismatch、corrupt opaque payload、取消与deadline均
typed fail closed。无current snapshot以`None`原样报告，由MV application按既有语义决定是否接受；不得retry latest。

该方法是Core-owned application port的扩展，不是Connector SPI capability，不进入native wire或durable schema。
Server composition负责安装真实provider inspector；未安装实现保持明确`Unsupported`。一个refresh attempt后续的
write preparation、activation与terminal control仍使用同一retained generation，遵循ADR-0051。

## 接受的妥协（诚实记录）

`MvStorageObservationPort`因此从五个use case增长到六个，继续增加了它成为“MV metadata杂物袋”的风险。这里选择
专用方法并不是因为方法更多更优，而是为了同时保住两项强约束：UUID/snapshot同一metadata version，以及base pin
不继承target apply的大载荷与语义。复用完整target observation代码更少，但会把错误的字段集合固化为长期契约；
组合两次observation表面更通用，却会真实降低一致性强度。

每个provider adapter仍要写一层看似机械的DTO映射，测试fake也必须实现新增方法。这是consumer-owned port的成本；
接受它是因为映射点让Core类型无法命名provider table，而不是为了减少样板代码。

## 何时重新评估

- 非MV consumer也需要同样的UUID/snapshot pin时；届时“MV storage observation”命名与owner可能已经过窄，应抽取
  通用的exact table identity observation，而不是复制第七个方法。
- base refresh需要schema、reference或partition事实时；不得直接向本值逐字段扩张，应重新证明为何完整target
  observation或新的有界组合值才是正确边界。
- `ConnectorTableMetadata`不再封存同一版本的UUID与current snapshot，或provider只能通过额外runtime IO取得其中
  一项时；本裁决的同版本证明将失效，需要新的版本锚点契约。
- observation port继续增长到use case无法清晰按consumer检索时；应按consumer拆port，不能改成unbounded property
  map或service locator。
- base pin必须跨进程transport或跨generation durable recovery时；届时需要单独裁决versioned wire/durable evidence，
  不能直接序列化当前FE-local observation。
