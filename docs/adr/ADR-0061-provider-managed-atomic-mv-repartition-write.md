---
id: ADR-0061
title: "MV repartition uses one provider-managed atomic write commit"
domain: [provider-spi, frontend-mv]
status: active
supersedes: []
superseded-by: null
date: 2026-08-12
provenance:
  - "discussion: 2026-08-12 MV repartition atomicity and recovery contract"
code-anchors:
  - "novarocks/spi/src/connector/write.rs (ConnectorManagedPublicationIntent)"
  - "novarocks/frontend/src/mv/refresh.rs (execute_data_refresh)"
  - "novarocks/connector/iceberg/src/commit/write_control.rs (IcebergWriteControl)"
---

## 问题

当物化视图 repartition 既要替换 default partition spec，又要写入并发布一个按新 spec 分区的 snapshot 时，
NovaRocks 应该把 spec transition 建模为 Frontend 拥有的独立 durable saga，还是让 Connector Provider 在同一个
write operation 的单次 external table commit 中原子完成 spec 与 snapshot 的切换？

## 背景与执行事实

旧 Iceberg 路径先以 expected default spec id 做 compare-and-set，安装完整 replacement spec，再写入新文件并
发布 snapshot；失败时使用精确 old/new spec guard 尝试恢复。这个流程具备明确的 caller-frozen guard，但也会在
两次 external commit 之间暴露“新 spec 已成为 default、对应 snapshot 尚未发布”的中间状态。

现有中立 catalog mutation 只表达单字段 add/drop，不能原子表示完整 replacement、expected prior default spec、
新 spec id 或精确 restore target。Frontend 的 durable refresh ledger则固定为 StagingCreate、Write、Publication、
StagingDrop 四个阶段，没有 spec-transition / restore action和相应的 durable old/new facts。把旧流程机械搬到
Provider既无法安全归因 response loss，也无法在进程重启后确定该重试 mutation还是执行 restore。

Iceberg table commit本身允许在一次 metadata transaction 中组合 AddSpec、SetDefaultSpec、AddSnapshot与
SetSnapshotRef。Provider已经拥有 exact-generation metadata、physical writer、data-file与terminal
commit/reconcile authority，因此只有 Provider能在不泄露 table-format对象的前提下冻结新 spec id、按该 spec写
partition values，并提交或归因整个 external action。

## 考虑过的选项

1. **Frontend 独立 spec-transition saga。** 新增原子 ReplaceDefaultPartitionSpec / exact restore capability，
   扩展 durable ledger保存 prior/replacement spec、mutation/restore operation id与每步结果。语义显式，但会新增
   lifecycle阶段、schema迁移与一套独立 commit-unknown恢复协议。
2. **Provider-managed 单次 atomic write。** 扩展已有 signed managed-write intent/preparation，携带有界的
   replacement facts与 expected prior spec observation；Provider在一个 table commit里同时切换 spec与snapshot，
   复用现有 Write action的 operation id、receipt、evidence与reconcile。
3. **Provider先改 spec、失败时 best-effort rollback。** 保留四阶段 ledger，但把两次 mutation藏进 write
   implementation。这个选项仍暴露中间状态，且 response loss后没有 durable authority决定 rollback，属于伪原子。

## 裁决

采用选项 2。MV repartition的 replacement必须是现有 managed-publication write intent的一部分，并参与
preparation签名或等价完整性校验。它只表达与本次 snapshot commit绑定的完整 partition replacement和 exact
expected prior spec observation，不扩展成通用 ALTER TABLE mutation语言。

Provider在 retained exact generation上验证 table identity、metadata location与 prior default spec，确定不会与
现有 spec冲突的新 spec id，并让writer使用该 sealed spec编码partition values。terminal external action必须在
一次 table commit中提交 AddSpec、SetDefaultSpec、AddSnapshot与main SetSnapshotRef。不能先把snapshot只写到
staging branch再由Frontend做第二次main fast-forward，因为那会暴露new default spec + old main snapshot，并在
第二次提交失败时重新需要restore。Frontend继续使用原有四阶段ledger；repartition的Publication phase变为验证并
持久化Write已完成原子publication的proof/finalize checkpoint，不发第二次external mutation，也不会新增
spec-transition或restore action。

external commit发出前的失败是 KnownUncommitted，可清理本 operation生成的文件；提交请求丢失或结果未知时，
必须使用同一个write operation id、receipt/evidence和Provider historical inspection进行reconcile；成功事实
同时携带canonical committed partitioning与provider-assigned spec id，供restart后的application finalize。任何路径都不得另发
“恢复旧 default spec”的补偿 mutation，因为完成态不存在可独立观察的 spec-only commit。

## 接受的妥协（诚实记录）

这个裁决扩大了 managed-write contract：它不再只描述行效果与publication facts，还允许一种受限的、与同一
snapshot原子绑定的 metadata evolution。选择它主要是为了避免 Frontend ledger/schema扩张和两阶段外部 saga，
不是因为把metadata mutation收入write天然更整洁。Provider实现也更复杂：必须在spec尚未对外发布时为writer
冻结id和partition codec，并为atomic commit、conflict与response loss提供可归因证据。

此外，该模型刻意不提供通用的partition-spec replacement capability。未来若有非MV调用方需要独立 ALTER TABLE
replacement，仍须单独设计其durable owner和恢复协议，不能复用本裁决绕过生命周期设计。

## 何时重新评估

- 任一受支持Iceberg catalog不能原子接受spec与snapshot/ref update集合；
- writer无法在不先提交spec的情况下安全、确定地编码新partition spec id；
- Provider inspection无法在metadata前进后把repartition commit归因到单个write operation；
- 需要非MV、无snapshot写入的完整partition-spec replacement；
- managed-write intent因更多metadata mutation持续膨胀，表明应拆出版本化mutation transaction能力；
- Frontend refresh ledger发生版本升级，能够以更低成本承载显式spec-transition saga及其恢复事实。
