---
id: ADR-0108
title: "MV lake descriptor is the rebuild authority for desired semantics"
domain: [frontend-mv, provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-26
provenance:
  - "discussion: 2026-08-25 MV lake descriptor fidelity and wipe-start equivalence"
  - "implementation: pending PR"
code-anchors:
  - "novarocks/frontend/src/mv/domain/persistence/descriptor.rs (MvDescriptorV3)"
  - "novarocks/frontend/src/mv/domain/lake_rebuild.rs (rebuild_mv_definition_from_lake)"
  - "novarocks/frontend/src/mv/domain/stateless_rebuild.rs (clear_sqlite_and_rebuild_from_lake)"
---

## 问题

当 Frontend 的 MV durable projection 丢失或被主动清空时，哪一份外部状态能够无默认值、无旧进程身份地重建
同一份 MV desired semantics 与已发布水位？

## 背景与执行事实

MV 的用户可见定义跨越 frozen SQL source 与 resolution context、依赖与目标 schema/partition contract、
visible/hidden layout、primary key，以及 refresh policy、paused、interval 和 max staleness。它们不能从当前
catalog 名称、SQL display 或 StateStore 默认值安全猜回。尤其是 primary key 被重建为空、或 Async/paused 配置
被重建为 Manual，都会改变后续 refresh 与 apply 的行为。

Iceberg target table 的 application-owned descriptor properties 可以和 target table identity一同由 exact
Connector generation读取；current snapshot及其 provenance提供已经发布的 target snapshot、base waterline、
object identity、rows和refresh time。Provider只投影这些中立事实，Frontend保留 MV descriptor、SQL与
repository 的语义解释，遵循 ADR-0086。Catalog commit 仍是外部写入的唯一 frontier，响应不明保持
crash-only outcome，遵循 ADR-0104。

StateStore definition、target/dependency indexes、partition state和scheduler metadata服务于当前 Frontend
运行；numeric `mv_id`、refresh attempt、lease/fence、in-progress flag、scheduler error和next-run物理时间
不能成为跨进程重建的身份或语义 authority。当前 refresh attempt/recovery owner仍受 ADR-0096 与既有 lifecycle
约束，本决策不授权删除或接管它们。

## 考虑过的选项

1. **继续以 StateStore definition 为主，并把 lake descriptor当作辅助副本。** CREATE/ALTER 失败时较易沿用
   现有顺序，但 descriptor 不完整或丢失时会让重启以默认值继续；两个可写authority也无法说明哪一份赢。
2. **从现有 target schema、SHOW CREATE 或当前 catalog重新推导定义。** 省去 descriptor字段，但无法恢复用户
   effective SQL、resolution context、hidden layout、primary key与完整 refresh desired configuration；latest
   catalog事实也会重写创建时语义。
3. **以 versioned、strict lake descriptor 加 exact package observation 为 desired/published authority，
   将 StateStore definition视为可重建 Accelerator projection。** descriptor承担全部用户desired semantics，
   package current snapshot/provenance承担已发布事实；CREATE、ALTER和rebuild共享typed projector。（采纳）

## 裁决

采用选项3。当前格式只有一个 strict `MvDescriptorV3`：它以 canonical typed encoding保存完整 MV desired
semantics，并对未知版本、缺失字段、非法 primary key/refresh/schema组合 fail closed。项目开发期不保留旧
descriptor reader、migration或默认值兼容路径；旧fixture和测试StateStore直接重建。

CREATE 先完成target bootstrap和exact target observation，形成完整 semantic value后先提交lake descriptor，
再创建 StateStore projection。descriptor已known-committed后的repository或catalog registration失败属于
known-committed finalization failure，不能通过删除target伪装回滚；descriptor commit unknown仍保持unknown。
ALTER和repartition从完整semantic value重写descriptor，不能patch局部JSON并遗失未改字段。

Rebuild只消费validated descriptor、exact target identity与current snapshot/provenance observation：它一次性
建立 definition、dependencies/indexes、refresh desired configuration和published projection。它不复用旧
`mv_id`、attempt、lease/fence、in-progress state、scheduler error或next-run timestamp。published target snapshot
的timestamp是last refresh time的authority；NeverPublished不得制造watermark。

test-only wipe proof必须先拒绝active refresh，并只移除可重建projection records，保留lake状态和历史attempt
records。它在同一个逻辑时刻比较wipe前后canonical semantics、published projection及scheduler eligibility，
并在source revision变化时拒绝跨revision比较。

## 接受的妥协（诚实记录）

descriptor字段和canonical hash变大，CREATE需要在target创建后再做一次descriptor property mutation；这不是
更少写入的方案，而是为了让外部可观察的MV package在StateStore丢失时仍是完整authority。descriptor已经提交
但本地projection未完成时，用户可能看到known-committed finalize failure，必须依靠后续rebuild收敛，不能得到
传统双写事务的“全有或全无”外观。

本决策暂时保留refresh attempts、lease/fence和historical recovery records，即使wipe不再删除它们。这使
StateStore还不是纯Accelerator keyspace；选择分阶段切分是为了不在descriptor fidelity变更中同时重写运行中
attempt的故障语义，而不是因为混合状态是长期更好的模型。

## 何时重新评估

1. 若产品承诺升级前descriptor、MV definition或外部package仍可读取，必须另立版本化migration/retention
   决策；不得在current decoder中悄悄恢复dual-read。
2. 若存在无法由exact target snapshot/provenance observation无损提供的用户可见published fact，应先扩展
   provider-neutral observation contract并证明不增加latest I/O，或重新裁决该fact的authority。
3. 若Descriptor property mutation必须与target data/snapshot commit原子可见，应重新评估provider是否应拥有
   更强的application descriptor contract；Frontend不得把provider-private metadata decode带回本层。
4. 当refresh attempt/recovery keyspace正式拆分时，应以新决策收敛历史attempt的保留、GC和startup owner，
   不通过本ADR暗示它们已经删除。
