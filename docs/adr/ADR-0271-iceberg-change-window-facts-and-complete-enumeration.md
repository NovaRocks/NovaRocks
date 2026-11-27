---
id: ADR-0271
title: "Iceberg change windows freeze source facts and enumerate the complete difference"
domain: [provider-spi, frontend-mv]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0278
    scope: "Ruling 7 non-aggregate write-mode selection uses evidence, cost, and admission for multiple strategies."
    link: "ADR-0278-mv-maintains-visible-tuples-and-replays-verification.md"
partially-supersedes: [ADR-0207]
date: 2026-11-25
provenance:
  - "discussion: 2026-11-25 accepted typed Iceberg change-window facts, source volume and enumeration contract"
symptoms:
  - "基表压缩后，MV 刷新怎样判断只推进水位还是读取差集？"
  - "ConnectorChangeWindowFacts::NetZero and ConnectorChangeWindowFacts::Difference"
code-anchors:
  - "novarocks/spi/src/connector/read.rs (ConnectorChangeWindowFacts, ConnectorChangeWindowPartitionImpactBuilder)"
  - "novarocks/spi/src/connector/read_stack/runtime/source_preparation.rs (ConnectorPreparedSourceRead::window_facts)"
  - "novarocks/spi/src/connector/read_stack/split_source.rs (ConnectorSplitSource::next_batch)"
  - "novarocks/connector/iceberg/src/snapshot_history.rs (SnapshotHistory, SnapshotDataEffect)"
  - "novarocks/connector/iceberg/src/change_planning.rs (prepare_change_window, finish_change_window)"
  - "novarocks/connector/iceberg/src/typed_boundary.rs (frozen_prepared_source_relations, finish_prepared_source_splits)"
  - "novarocks/connector/iceberg/src/typed_read/split_source.rs (plan_change_window_splits, IcebergChangeWindowSplitSource)"
  - "novarocks/connector/iceberg/src/typed_read/change_window_page_source.rs (ChangeWindowRead)"
  - "novarocks/frontend-application/src/mv/domain/refresh/non_join_incremental.rs (plan_non_join_incremental_changes)"
  - "novarocks/frontend-application/src/query_execution/mv_assembly/refresh_preparation/pure_reader.rs (mv_refresh_publication_intent)"
  - "novarocks/execution/src/exec/operators/compiled_change_router.rs (ChangeRouter::begin)"
---

## 问题

MV 怎样用同一份冻结的源窗口事实判断刷新方式，并证明活动尝试实际取得了完整的源差集？

## 背景与执行事实

[ADR-0207](ADR-0207-single-prepared-change-window-and-frozen-source-endpoints.md) 确立了单次准备和冻结端点读取。
每个刷新尝试按准确的 Provider 绑定、表对象和 `(From, To]` 窗口准备一次。同一窗口的多个关系 occurrence 各自绑定，复用同一份准备回执。
每种行内容的带符号贡献之和等于 To 的可见次数减去 From 的可见次数，允许源流包含相消的新增与撤销。

| 事实 | 作者和证明 |
| --- | --- |
| 窗口能否执行差集 | Provider 从格式、父链、schema 与冻结端点得出结论。`NotIncremental` 携带原因。损坏、不支持的格式和控制或读取故障沿原类型化错误返回。 |
| 源是否净零 | Provider 给出 `NetZero` 及依据，证明整个冻结窗口的行袋差为零。差集仍可能在执行后相消。 |
| 源是否可能产生物理负权重 | `Difference` 携带 `Absent` 或 `Possible`。前者要求没有数据文件移除，且所有存续文件的逻辑删除闭包相同。源流权重、相消后的源变化和 MV 输出权重各自需要证据。 |
| 活动尝试是否完整覆盖源 | Provider 核定应读文件及范围，活动尝试观察成功枚举及拆分正常读完。交接边与关系节点分别为严格 EOS 和关系闭合提供证据。 |

## 考虑过的选项

1. **平铺能力、净零和负权重，并由消费者组合。设计否决。** 字段组合能表达不可执行窗口的净零证明，并允许各消费者各自解释同一个投影。封闭的准备结果统一限定可执行关系。
2. **用快照摘要行数验证 REPLACE。设计否决。** 相同物理行数无法证明内容等价，合并时应用旧删除也可以减少物理行数。采用 Iceberg 格式定义的 REPLACE 语义，由 Provider 共享作者解释操作。
3. **允许动态谓词直接截断变更窗口的枚举。设计否决。** 活动尝试需要自然交付完整范围的证据，关系层拥有筛选导致的零贡献证明。为显式变更窗口和准备后 Delta 采用同一个成功枚举合同。
4. **沿快照来源缩小发现范围与相消对。成本否决。** 对纯追加这样的窗口，可以降低端点发现成本。混合窗口仍需要证明删除适用性、重写搬移和完整覆盖。当前复用两端比较，给后续发现算法保留同一事实边界。

## 裁决

1. **封闭的准备投影。** `ConnectorChangeWindowFacts` 取以下三种形状。差集携带 `negative_weight`、可选提示 `partition_impact` 与 `volume`。同一个 `permits` 裁定关系投影。

   | 结论 | To | From | Delta |
   | --- | --- | --- | --- |
   | `NotIncremental(reason)` | 可读 | — | — |
   | `NetZero(basis)` | 可读 | — | 闭合的空 Delta |
   | `Difference` | 可读 | 可读 | 冻结差集 |

   不可增量的封闭原因是 `SnapshotAncestryBroken { from_snapshot_id }` 和 `SchemaChanged { snapshot_id }`。净零的封闭依据是 `SameSnapshot`、`ReplaceOnly` 与 `EqualEndpoints`。新增原因、依据或投影能力时，同步 Provider、SPI、FE 消费方和独立断言。

2. **先证祖先，再裁定内容。** `SnapshotHistory` 是窗口和统计共用的 Provider 历史作者。它把 REPLACE 解释为保持行袋，
   APPEND 为增加、DELETE 为减少、OVERWRITE 为可能增减。各消费者继续应用自身的方向、历史可读性和遍历约束。
   窗口先处理同一快照，再验证从 To 到 From 的完整父链。完整区间仅含 REPLACE 时成立 `ReplaceOnly`，包括沿途发生 schema 演进的情况。
   仅在区间含数据变更操作时，超出改名的相邻 schema 变化给出 `SchemaChanged`，报告最靠近 To 的触发快照。
   其余情形比较两端的不可变文件身份及逻辑删除闭包，两端相同可给出 `EqualEndpoints`。冻结 To 观察的原校验仍适用。
   [Iceberg 快照操作](https://iceberg.apache.org/spec/#snapshots)

3. **体量刻画相消前的源事件。** `source_event_rows_upper_bound` 按格式合同累计带权事件的行数上界。
   固定数据文件在单个快照最多有一个适用 DV，新删除合入已有 DV。记文件物理 `record_count` 为 `n`，
   From 和 To 的适用 DV 基数分别为 `a` 和 `b`，空闭包计零。逻辑闭包仅含 DV 或为空时，
   To 新增文件计 `n − b`，From 移除文件计 `n − a`，存续文件的删除闭包变化计 `b − a`。
   相关闭包含位置删除文件或等值删除时，每个文件变体以 `n` 为上界。精确计数使用的 DV 基数超出 `[0, n]`，
   或适用精确计数的存续文件出现 `b < a`，返回 `CorruptData`。`source_data_file_bytes` 按应读文件变体累加 `file_size_in_bytes`，
   与拆分布局无关。原执行证据另行度量删除文件读取、实际 I/O、Arrow 输出和执行峰值。字段用于后续策略估算，
   刷新拒绝仍归原准入及资源作者。 [Iceberg 删除格式与 DV](https://iceberg.apache.org/spec/#delete-formats) · [manifest 文件字段](https://iceberg.apache.org/spec/#data-file-fields)

4. **可选分区影响按保留量计费。** 分区 spec 身份、源字段身份、变换和值共同确定规范身份。
   新增侧与移除侧各自逐项去重，共用原请求 `max_total_payload_bytes`。按实际保留的结构、身份、列名和值先计费再收录。
   超预算、计费溢出或提示无法表达时，释放精确集合并给出 `Unavailable`。窗口事实的计算和源元数据校验继续进行。
   固定大小的准备结论、原因、依据、负权重及体量均在可选提示预算之外。整个窗口回执仅在 FE 进程内传递和使用。

5. **完整枚举与读取闭合分别取证。** Provider 以冻结的两端文件和删除闭包核定预期差异。
   每个差异文件有唯一变体、绑定域和由原范围作者给出的扫描范围。拆分组装检查完整范围及实际起点，
   两条入口共用 `IcebergChangeWindowSplitSource`，对 `all`、`none` 及其他动态谓词交出相同的范围。
   活动尝试从首批消费至自然末批后才取得枚举证据，末批可携带拆分。提前关闭、取消和 `is_finished` 只说明生命周期状态。
   空 Delta 沿冻结的净零依据闭合。后续 occurrence 级零贡献由其关系作者另证。
   后续完整覆盖核验须绑定同一尝试与 occurrence，合成枚举、逐拆分正常读取、严格 EOS 与关系闭合的证据。

6. **整文件删除撤销 From 可见行。** 移除文件按 From 读取域和删除闭包发出 `−1`，排除 From 时已被 DV、位置删除或等值删除隐藏的行。删除剩余可见行和直接移除该文件产生相同的撤销袋、次数与可用的原行身份。 [ADR-0164](ADR-0164-iceberg-delete-closures-and-execution-unions.md)

7. **选路与真实发布沿各自作者。** FE 先用源绑定、定义和目标存储事实判断重用资格，再消费已复用定义的窗口结论。
   所有相关 occurrence 净零时由原发布流程只推进冻结的源水位。有不可增量原因时沿已捕获的 To 准备全量。
   差集的负权重可能性是现有写模式的输入。仅追加要求源负权重 `Absent`，以及相应关系与目标写入的准入。
   路由在输出任何分支前核对整批效果，无接收分支的删除使本批及语句失败。FE 将选择交接给原发布所有者，
   沿原目标基准、B 根及派发证据结算。 [ADR-0267](ADR-0267-mv-publishes-bound-source-and-storage-facts.md) · [ADR-0268](ADR-0268-iceberg-mv-converges-from-the-original-commit.md)

本条部分替代 ADR-0207「只投影，不重新准入」一段的写模式、`MetadataOnly`、`FullRebuild` 与 `has_deletes` 投影词汇，以及「闭合的零贡献投影」一段对 `MetadataOnly` 的称呼。单次准备、冻结端点、真实执行观察、原资源与失败结算继续由 ADR-0207 约束。

## 接受的妥协（诚实记录）

- 两端比较复用现有文件与删除闭包解释。混合窗口里的文件搬移可同时产生撤销和新增，源体量与精确分区提示可能随重写范围增长。
- 净零可信度依赖 Iceberg 写方遵守 REPLACE 合同。写方把真实内容变化标成 REPLACE 时，源窗会漏报变化，修正责任属于写方。
- 完整枚举可为后来证明贡献为零的关系发出拆分，尤其是显式窗口处于 probe 侧且 build 侧为空时。可选分区提示降级后，依赖精确分区的策略失去这项输入。
- 纯 REPLACE 的空 Delta 仍需两个端点的冻结读域。From 过期时，即便父链可到达其 ID，目前也缺少构造该空投影所需的端点 metadata。

## 何时重新评估

- 当端点发现或混合重写的相消前体量影响刷新时，优先评估纯追加的按来源发现，再对混合窗口证明删除闭包和搬移。发现算法可以收紧源体量，仍须完整交出预期范围。
- 比较全量与增量时，用同一套优化器估算双方计划，并结合 Provider 的冻结差集体量、关系运算和目标应用代价。随计划引入关系覆盖证据时，可评估由关系作者提供的零贡献依据。
- 观测到空 build 导致显式变更窗口的额外工作值得优化时，可评估共用的类型化结束证据。自然发完与关系已证明为空各有证据。
- 当执行投影可将祖先身份证明与端点可读性分开时，评估父链仍可达、全为 REPLACE 的过期 From 窗口。需要实际读取差集的情形仍由相关读域和冻结端点约束。
