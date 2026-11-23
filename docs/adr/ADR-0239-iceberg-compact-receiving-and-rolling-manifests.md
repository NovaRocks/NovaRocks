---
id: ADR-0239
title: "Iceberg compact receiving and operation-owned rolling manifests"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0262
    scope: "The whole-set receiving path and its use by remaining write flavors."
    link: "ADR-0262-iceberg-write-classes-and-exact-create-cleanup.md"
  - id: ADR-0246
    scope: "Rule 5 coverage for registered ADD FILES entries without receiving receipts"
    link: "ADR-0246-iceberg-add-files-uses-read-binding-and-atomic-registration.md"
  - id: ADR-0244
    scope: "Rule 6's RefUnchanged and no-retry policy for ordinary Iceberg row-level writes"
    link: "ADR-0244-iceberg-row-dml-conflict-validation.md"
partially-supersedes:
  - id: ADR-0171
    scope: "Section 1 addition representation for compact receiving; WholeSet intents, dependencies, inheritance and publication authority remain active"
    link: "ADR-0171-commit-operation-model.md"
  - id: ADR-0234
    scope: "Rules 3 and 5 for reuse and coverage of compact additions; the physical object ledger, retirement, outcome and cleanup rules remain active"
    link: "ADR-0234-operation-owned-artifacts-and-bounded-publication-facts.md"
date: 2026-11-19
implementation_status: implemented
verification_status: scoped-tracked-separately
provenance:
  - "discussion: 2026-11-18 accepted Iceberg compact receiving and publication-time manifest rolling design"
related:
  - ADR-0136
  - ADR-0198
  - ADR-0225
  - ADR-0228
  - ADR-0235
code-anchors:
  - "novarocks/frontend-application/src/query_execution/write_result.rs (RootWriteResultDecoder)"
  - "novarocks/frontend-application/src/query_execution/write_session/computation.rs (WriteReceivingPort)"
  - "novarocks/frontend-application/src/query_execution/write_production.rs (WriterCreationProof)"
  - "novarocks/connector/iceberg/src/commit/write_stack/receiving.rs (IcebergReceiving, ReceivedCompactFiles)"
  - "novarocks/connector/iceberg/src/commit/write_stack/receiving/validation.rs (FragmentAdmission, StatementIndexes)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control/publication.rs (prepare_compact_operation)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control/publication/materialized.rs (MaterializedDeleteStep)"
  - "novarocks/connector/iceberg/src/commit/model/intent.rs (OperationIntent::with_compact_stage)"
  - "novarocks/connector/iceberg/src/commit/new_manifests.rs (StageNewFiles, prepare_added_manifests)"
  - "novarocks/connector/iceberg/src/commit/model/artifacts.rs (ArtifactLedger::seal_compact_stage)"
---

> 裁决 5 的新增条目覆盖由 [ADR-0246](ADR-0246-iceberg-add-files-uses-read-binding-and-atomic-registration.md) 扩展至 ADD FILES 登记来源。登记核对受检条目消费、ADDED 计数与发布覆盖。接收写入的创建回执、物理覆盖和删除权继续按本 ADR 执行。

> [ADR-0244](ADR-0244-iceberg-row-dml-conflict-validation.md) 更新第 6 条普通 Iceberg 行级写入的依赖与重试政策。紧凑接收、滚动 manifest、按条目核对覆盖和固定 ref 的刷新政策继续适用。

## 问题

Iceberg 的 FE 如何接收大量文件事实并构造可沿用的新增 manifest，同时守住完整写集的发布屏障、结果窗口预算和物理对象的清理权？

## 背景与执行事实

BE 写出实际文件。根 BE 的单一 `TableFinish` 维持完整写集语义，等待全部发送方结束后才输出根计数和提交片段。FE 消费根记录，Provider 验证片段并构造提交。根 EOF、参与方成功及原取消与期限检查共同决定写入能否交给发布方。参见 [ADR-0136](ADR-0136-ordinary-aggregate-statistics-dataflow.md) 与 [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md)。

| 阶段与作者 | 保管的事实 |
| --- | --- |
| FE 根记录解码与写入计算 | 在原受控 CPU 范围内组装、逐记录解码、同步交给 Provider，确认后释放记录载体。FE 保留根行数、统计草稿与删除核对计数。 |
| Iceberg 写会话的 `IcebergReceiving` | 按原 query 与目标登记接收并持有回执。紧凑模式保留一份每条目的计量事实、接收索引键和删除效果。整份保留模式仍保有完整片段。 |
| `IcebergCommitOperation` 与提交意图 | 完整接收后由原发布方接管文件和紧凑条目。意图引用 operation 内的新增文件 stage，冻结精确目标、依赖及必要字段。 |
| 新增 manifest 写出与 `ArtifactLedger` | 第一轮按组滚动写出新增 manifest，逐个释放所消费的紧凑条目。operation 留下 descriptor、计数、覆盖事实和原物理对象账本。 |
| 合并、manifest-list 与发布 journal | 按已冻结的装箱政策和完整输入关系选择合并，按本轮 metadata 写 list 并分配序号与行 ID。原 journal 持有派发结论与准确发布引用。 |

## 考虑过的选项

1. **完整接收前写出 manifest，或交给 BE 构造。设计否决。** 可提前释放 FE 的文件事实，但需要在发布屏障前签发 snapshot ID、对象写入和清理授权，或把发布期的格式解释交给 BE。
2. **在 EOF 后解码整份载体，或给提交意图再保留完整新增文件。设计否决。** 可以延续整份输入接口，但会同时持有逐文件的载体、解码值或意图副本，紧凑路径无法保持一个受同一窗口计量的文件事实 owner。
3. **用物理对象数校验新增条目覆盖。设计否决。** 多个 DV 条目合法引用同一 Puffin，也可能落在不同 manifest 中。对象是否可删除与条目是否完整发布有各自的身份和核对方式。
4. **并行写多个新增 manifest。成本否决。** SDK 在调用它的任务中同步编码，分组并发能重叠部分上传，也会同时保留多份编码缓冲。先采用顺序滚动和现有 I/O 许可。
5. **用新增 manifest 替代逐对象账本，或对超大规模操作分批提交。待评估。** 前者要重新定义原对象 owner 与失败清理权限，后者要定义多次可见提交的原子性与结论。

## 裁决

1. **接收与发布屏障。** FE 按根记录顺序在原 CPU 范围内同步交付。Provider 按已登记的 query 与 sealed target 核对。创建回执查询原已登记实例的不可变索引，逐片段检查无需取得 Workload Control 的进程锁。完整写入还须消费原生产完成证明。接收期只保管对象事实和窗口回执，接管、删除权、发布资格及退出观察在完整接收之后沿原交接推进。提前拒绝、取消或接收不全时仍无新增 manifest。
2. **同一窗口计量。** 紧凑条目接过原解码回执。按该窗口追加实际所需的索引键、删除效果与转换并存峰所需字节，获准后再分配保留内存。一个记录全部通过才安装其条目与索引。补足不足沿原类型化资源失败。接管后由 operation 持有该批条目，条目与收到的回执随所属 manifest 的写出实际退出。当前紧凑表示保留 Provider 私有的编码新增事实与少量定型键，第一轮写出时在原窗口下解码，根记录线路契约保持原样。
3. **紧凑 stage 规则。** 紧凑意图引用同一 operation 的新增文件 stage。stage 写出前持有计量条目，写出后持有新增 manifest 和计数。Append、固定 v2/v3 行级写入、MV 仅元数据与普通增量刷新使用此路径。覆盖写、CTAS 封存、equality delete、维护与分布式改写、全量刷新及带重新分区的刷新沿各自原意图保留整份输入。其接收保管也位于 Provider。
4. **顺序滚动与复用。** 每个 stage 使用一个稳定 snapshot ID。每个 partition spec 与 manifest 内容类型内顺序滚动，目标为 `commit.manifest.target-size-bytes`。用编码长度下界和实际写出长度校准批次大小。该属性决定布局，超出或未达到目标都不构成容量拒绝。新增 manifest 在第一轮写出，允许重试时沿用已经完成的 descriptor 与计数，省去逐轮的文件转换与路径摘要。若允许重试的操作遇到旧 v1 新增 manifest 与刷新后的格式不兼容，只重编码本 operation 已写的 v1 manifest，再核对覆盖。合并仍按有序来源与编码依据复用，输入变化的箱重算，本轮 list 与统计产物重新准备。序号和 first-row-id 由本轮 list 分配。
5. **按条目核对覆盖。** 首次形成新增 manifest 后，逐 manifest 核对 ADDED 数等于找到已接管对象的 ADDED 数，再核对总数等于接管条目数且每个通过 manifest 发布的接管对象均被覆盖。冻结各轮请求时核对每个原新增 manifest 恰由一个最终 list 成员直接保留或合并替代，合并产物的新增条目数和可解析到接管对象的数量都须等于其新增输入的相应数目。该轮覆盖工作按 manifest 数伸缩，物理接管集合共享同一份引用。一个共享 Puffin 保持一个物理对象，多个 DV 分开计条目。覆盖不一致使请求在派发前失败。
6. **删除效果与重试。** v2 的多个 position-delete 文件可指向同一目标，必须具有相同的准确分区事实，目标在固定端点 E 上仍须存活。v3 每个目标数据文件至多一个最终删除效果。新增 DV 进入滚动条目，旧位置删除或旧 DV 替代、整文件移除和新增可见删除数留在已计量的效果中。发布观察只为 E 新读取与构造的事实预留，使用原精确来源与历史分区解释。delete manifest 的编码按每个实际批次选择，移除只重写触及的既有 manifest。行级写入与有固定 ref 依赖的刷新沿 `RefUnchanged` 执行，发生冲突即停止。
7. **物理责任、结论与退出。** 账本以物理对象记录退役与清理，写出沿 operation 原 I/O 许可及实际退出。可重试的确定冲突只退休本轮 list 与统计产物。已知未提交时按原账本清理，已提交时保留准确发布引用，Unknown 保留对象。沿用 [ADR-0234](ADR-0234-operation-owned-artifacts-and-bounded-publication-facts.md) 的稳定 ID、原清理 owner 和有界证据。 [ADR-0171](ADR-0171-commit-operation-model.md) 的依赖、字段继承、发布权与派发规则仍适用。

## 接受的妥协（诚实记录）

- 单语句仍需在结果窗口中保留每条目的一份紧凑事实及相关索引，编码形式以首轮的逐条解码换取较小的保留量。窗口继续决定单语句可接收的规模。物理账本的逐对象记录随规模增长，其内存归属由统一内存归属工作接入。
- 根 BE 保留原汇总。超大规模写入的分批提交留待后续 Iceberg 写入规模设计评估，须先定义分批后的可见性、失败及清理边界。
- SDK 编码仍发生在发布任务。把编码移至原有界 CPU owner 与并行写出属于后续工作，当前批次大小允许出现尾部小 manifest，也可能触发原合并政策。
- 整份保留的 flavor 延续其提交作者和已核对的行为，切换由各类操作的代表输入及兼容证据驱动。代码接线与定向验证的范围由实施记录说明，最终全工作区、原生及外部读回验收按各自观测结果判定。

## 何时重新评估

- 统一内存归属覆盖 operation 账本后，以实际归属与峰值判断物理对象库存是否需要不同表示。若改用 manifest 承载库存，先明确完整接收失败、部分写出及 Unknown 时怎样证明和保留删除权。
- 生产数据表明单语句的真实窗口仍限制超大规模写入时，评估分批提交与外部快照可见性协议，并区分维护类与普通 DML 的原子性。
- 编码由有界 CPU owner 执行，且存储吞吐观测表明写出重叠能带来收益时，评估有限并行。以实际峰值、取消与退出作为准入条件。
- 扩展到整份保留的 flavor 前，先找齐其全部新增文件事实消费者，并验证覆盖、封存或重写所需的原字段依赖。
