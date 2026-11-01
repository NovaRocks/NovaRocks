---
id: ADR-0207
title: "Prepare each change window once and read frozen source endpoints"
domain: [provider-spi, frontend-mv]
status: active
supersedes: []
superseded-by: null
date: 2026-10-29
provenance:
  - "discussion: 2026-10-29 typed window preparation, frozen source endpoints and execution observations"
code-anchors:
  - "novarocks/spi/src/connector/read_stack/adapter/mod.rs (BoundSourcePreparation, project_prepared_source_read)"
  - "novarocks/frontend-application/src/query_execution/mv_assembly/refresh_preparation/captured_sources.rs (CapturedRefreshSource)"
  - "novarocks/frontend-application/src/query_execution/mv_assembly/refresh_preparation/io_gate.rs (MvRefreshPreparationIo)"
  - "novarocks/frontend-application/src/mv/domain/refresh/rewrite_context.rs (project_prepared_change_window_for_table_original)"
  - "novarocks/frontend-application/src/query_execution/mv_assembly/query_local_bindings.rs (validate_source_revisions_original)"
  - "novarocks/frontend-application/src/query_execution/provider_read_facts.rs (open_relation)"
  - "novarocks/frontend-application/src/query_execution/attempt_initialization.rs (ProductionSourceOpening)"
  - "novarocks/frontend-application/src/query/mv_action.rs (MvCommandOriginal)"
  - "novarocks/frontend-application/src/query/statement_computation.rs (StatementComputation)"
  - "novarocks/connector/iceberg/src/typed_boundary.rs (project_prepared_source_read, begin_get_splits)"
  - "novarocks/connector/iceberg/src/catalog_control/statistics/scoped_prepared_source.rs (IcebergPreparedSourceRead)"
  - "novarocks/connector/iceberg/src/catalog_control/statistics/scoped_prepared_split.rs (Work, drive_prepared_split)"
  - "novarocks/connector/iceberg/src/typed_read/prepared_source_proof.rs (validate_prepared_endpoint, validate_referenced_specs)"
  - "novarocks/connector/iceberg/src/metadata_context.rs (observe_table_original_for_request_with_snapshots_async)"
  - "novarocks/connector/iceberg/src/read_snapshot.rs (observe_read_files_original)"
---

## 问题

增量刷新如何只解释一次源窗口，同时允许源继续写入，并在执行时准确拒绝对象替换或冻结端点不可读？

## 背景与执行事实

快照不可变并不表示表的 current 指针、schema、location 或外部保留策略不变。刷新要消费某个窗口的确定内容；并不需要在整个刷新期间锁住源 head。目标提交的 OCC 只保护目标提交，不能替源表提供跨表事务或 CAS。

| 实体 | 唯一职责 | 不提供的保证 |
|---|---|---|
| 原刷新 attempt / Root | 保管 pin、一次准备的事实、原始结果、实际资源与退出 | 裸 Root 身份不授予 IO 或容量 |
| typed change-window 准备作者 | 按 exact binding、对象 UUID 和 From/To 解释 lineage、To schema、delete 与分区事实 | 不代表目标提交成功，不形成外部 snapshot retention lease |
| 策略与查询局部绑定 | 从同一份冻结事实分别投影写模式、查询关系与 occurrence 绑定 | 不能各自重新准入或改选 current |
| FE lazy split / access owner | 实际重新观察对象与端点，之后规划冻结窗口的完整 split | 用旧 metadata 重建 SDK Table 不算新观察 |
| BE page-source / StorageAuthority | 按准确 split 与 scope 读取；实际请求消费者取得/续期凭据 | 凭据有效不证明端点语义有效 |
| MV publication | 发布本轮实际读取的 To 水位，承担目标提交与后续领域处置 | 不把之后的新提交计入本轮 |

现有 typed read 架构由 [ADR-0123](ADR-0123-task-update-watermark-retry-delivery.md) 及其历史谱系定义：窗口有自己的句柄与 split，不复用普通 opaque scan 准入。凭据/执行 owner 规则见 ADR-0151、ADR-0159、ADR-0198。本条补充窗口准备与冻结端点合同，不替代这些规则。

`read_snapshot::observe_read_files_original` 已核选中物理视图的 UUID，也核整份 metadata 与冻结 domain 的一致性。这个哈希可证明同一冻结解释内部一致，不能用来要求一次新观察的整表 metadata 完全不变。`IcebergMetadataContext::observe_table_original_for_request_with_snapshots_async` 是实际绕过 request/generation 缓存的观察作者；凭据续期后返回冻结 Table 的 access 重建不是替代品。

MV action 的原 `MvCommandOriginal` 所持 positive MetadataIoAdmission 是 Internal 计算可续接的实际 IO 作者。当前接线沿 `StatementComputation`、私有 launcher/factory 与原 attempt context 续同一能力；`ProductionSourceOpening` 保管实际 preparation packet、原始结果与 close。原 Source 的 progress 与实际 quiescence 仍分别取证，不能从 scope/window 或凭据另造授权。

本条记录已接受的长期设计。当前源码已接通 `CapturedRefreshSource` 的完成事实库存、Provider 单次 typed 准备、FE 同步策略/关系投影，以及实际 fresh 观察、端点证明和冻结 split 规划。`scoped_prepared_source`、`scoped_prepared_split` 与 `prepared_source_proof` 是当前 MV 路径的规范作者；原 `change_window_split_source` 留给普通 typed ChangeWindow/TVF，不是 MV 准备的 fallback。源码接线不提供 B1b plan v5 的 F/G/H、完整原生或最终验收的完成资格；这些范围仍按各自实际结果、原始失败和收尾证据独立核对，不能据 `status: active` 声称产品已满足本条。

## 考虑过的选项

1. **策略、绑定与执行各自准入。设计否决。** 各层独立解释外部窗口会增加完整发现并形成竞争的语义来源；晚调用本身也不构成线性化点。保留便于各层独立调用不是正确性理由。
2. **要求源 current 一直等于 To，且 location 不变。设计否决。** 能避免观察差异，但把持续追加变成刷新失败条件；location 也不是对象身份。只有取消“刷新可与源追加并发”的能力时，才有重新选择这种合同的前提。
3. **只核 UUID 与 snapshot 数字，或全域跳过 metadata 校验。设计否决。** 会丢失端点描述、To schema、spec/继承/delete 等解释证明，并误放宽普通 Snapshot、Statistics、Rewrite/COW 与目标 OCC。
4. **typed 单次准备，真实新观察验证端点，冻结视图执行。接受。** 所有消费者共享语义来源；保所选端点的准确合同，同时允许无关 head 推进。需要显式保管两个视图、资源与真实退出。
5. **在新观察视图上直接枚举，并以专属闭合读合同代替全表 hash。待评估。** 可以成立，但需显式区分窗口/派生 To 与其他 exact-domain 消费者；不能由 caller 任意设置 skip flag。本条选择两个视图，不以这种替代实现为当前义务。
6. **共享 manifest 差集与跨刷新缓存。待评估。** 可能减少长期完整发现成本，但需要独立的 descriptor 继承、delete 可见性、失效、保留与容量模型。本条不引入缓存平台，不把后继算法当单次准备的前置。

## 裁决

**一次规范准备。** 每个刷新 attempt 对同一 exact provider/read binding 与 incarnation、同一 UUID、同一 From/To 只准备一次 typed 窗口。原 Root/刷新业务 attempt 持有其库存，不能按表名或裸数字键跨新的刷新 attempt 共享。原恢复政策若允许内部 Native QueryAttempt/round，它们只续同一份冻结输入，每次实际读取仍按自己的 request/access scope 在同原 MIO 下观察，不重新 pin 或准备；不新增身份或重试能力，不改变 CommitUnknown/已提交不可重跑的政策。冻结事实包含源身份与端点、按 To 解释的 schema、lineage 分类与原因、完整 delete 和分区影响，以及可执行类别的 typed 句柄/端点证明。FullRebuild 只提供合法的 To 读取与原因，不伪造 Delta。

**只投影，不重新准入。** 策略投影写模式、MetadataOnly/FullRebuild 与 has_deletes；查询局部绑定投影 To 读取及 From/To 物化，保低成本 pin 修订断言。各 join occurrence 独立绑定但共享同 exact 窗口事实，同依赖只 pin 一次。删除没有消费者的准入库存，退役 opaque ConnectorScan 的 ChangeWindow 准入及旧镜像，不保 fallback。EXPLAIN 使用相同准备作者，仅展示策略，不执行 split 观察。first/full 没有 From，不准备窗口，只消费 UUID+To 的 typed snapshot；空 To 保真实 None/Empty，不造快照 0。

**读取冻结 To。** pin 后源可以继续追加。本轮读取和发布水位都是 To，之后的提交由下一轮消费。移除以 current==To、table/metadata location 相等作为窗口准入条件的检查；按 To schema 解释，不按后来的 current schema 重解释。

**定义准入与读取解释分开。** D/L 的对象、字段身份、类型和可空性校验消费同一次初始 All pin 的 schema，由原 7B7 作者逐 occurrence 裁决。pin 前发生的定义漂移不能被旧 To schema 遮蔽；实际读取继续消费准备后的 To schema。两者都从原 Root 的捕获帧投影，不为定义校验再读 Current，也不在 pin 后跟随 schema 推进。

**闭合的零贡献投影。** 混合 UNION 中一项依赖为 MetadataOnly，仍可从该 exact 窗口事实投影空 Delta。它返回零 split，不开放可执行 From，也不以相同数字端点代替 Provider 的空贡献证明。原 binding、请求与 Root scope 检查仍成立；无 fresh 观察、CPU 工作或新的 Source。该依赖真正读取 To 时继续执行原观察及数据读取。FullRebuild 和 snapshot-only 不因此获得 Delta。

**真实执行观察。** 实际启动读取时，FE 原 access/split owner 使用同 Root 已付费 IO 能力实际观察 catalog，先核 UUID。可执行 Delta 核两端仍可证明可见及合法祖先关系（含合法同端点）；first/full 与 FullRebuild 的实际 To 读取只核 To，历史 From 只保留为 FullRebuild 原因。MetadataOnly/SkipEmpty 没有实际 split 时不强加执行观察，也不冒称完成该观察。各实际读取分支验证冻结 To schema 和所依赖的 snapshot manifest-list、sequence、父链、spec/partition storage type、field/nominal domain 及所选端点实际依赖的 name mapping/default 读取政策等相关事实。新增无关 snapshot、默认 spec 或 property 不是拒绝理由。用准备时冻结的物理视图和请求期 FileIO 进行完整发现与 split 规划；一次整窗 catalog 读取/解码及 control/UUID 后，Delta 按 From 专属证明、From 完整发现、To 专属证明/关系核对、To 完整发现推进。To 专属的新验证不能遮蔽 From manifest 硬错，整窗 catalog/解码/control/identity 失败仍保持原位置。共享 observer 的全 metadata hash 仍保冻结 domain 对冻结视图的内部一致性。普通 Snapshot、Statistics、Rewrite/COW、TableExecute 的合同与目标 OCC 不被放宽。

**漂移与晚期失败。** 准备及执行均以 typed 错误拒绝对象 UUID 替换；后果交 MV application。准备时端点丢失/非祖先按既定能力产生 FullRebuild 原因或 typed 错误；执行期才发现端点不可见/解释漂移，本轮失败，不改读最新端点、不静默转 full 或空结果。后续位置变化不直接拒绝，实际旧依赖访问失败仍失败。授权在原语句准入和准确请求 scope 各自成立，权限撤销造成的 IO 失败保原原因。

**原资源与责任。** 准备仍沿原 CPU、MIO、稳定 raw receiving、retention 与完整依赖域。Internal 计算从原 Source 续同 MIO port，Source 强持并驱动、factory 弱借，不复活已关闭 prepare gate，不新增池/额度/位置。新观察的 child Future、原始结果与析构实际退出前不宣称 quiescent；取消/timeout/Ready 不代替退出。冻结事实与并存峰按原预算付费，句柄或 Arc 不自行授容量。完成的 source receipt 独立拥有不可变事实及其数据，不强持准备时的 per-request Provider、request-bound Table/FileIO 或旧请求库存；同 exact binding 与 incarnation 的新请求仅从这些事实投影。原 Root、planning lease、access/split Source 继续分别保管实际授权、请求作用域、活跃资源和真实 close，resource-free 事实不授予或替代这些能力。

**完整发现与可选复用。** 对需要端点比较并执行 Delta 的窗口，当前保留准备一次、执行一次的两端完整发现。lineage 已闭合的 MetadataOnly/FullRebuild 保原分类，不为取得 Delta 句柄强行枚举不可用的 From。物理复用可选，只限 Provider 内同 attempt/同窗口，且 descriptor 与继承上下文、完整 delete、From 先错顺序、真实容量全部成立；否则执行自行发现。不是固定两次网络请求，不跨 attempt 保存，不跳共同 manifest，不做差集或 lineage 相消。

## 接受的妥协（诚实记录）

- 完整发现仍随 manifest 数增长；消除重复准备并不使长期刷新总体成本自动成为线性，也不保证任何现有时间预算一定通过。
- 新观察增加一次必要的受管 metadata 访问；资源不足、撤权、外部保留清理都可能使本轮失败。既有预算、轮数和验证 oracle 不因设计正确而放宽。
- 真实验证与冻结读取是两个视图，需要明确的 provenance、寿命及稳定接收格。验证和 IO 之间仍可能发生清理；不承诺外部系统没有提供的 retention 或跨表事务。
- 相同 manifest 地址不意味着继承/删除解释相同；本条允许不做任何准备与执行间的物理复用，先保持可证明的完整发现。

## 何时重新评估

- 若完整发现仍成为长历史刷新瓶颈，独立评估 manifest 差集、共同 manifest 跳读、lineage 相消及其完整 delete/继承证明；不能直接把本条的 attempt-local 库存扩成全局缓存。
- 若 Provider 已有封闭的 snapshot-scoped 读域与真实 observer，可评估直接在新观察视图枚举，需保持普通 exact-domain 消费者原合同、失败顺序和真实资源。
- 若外部 catalog 提供可依赖的 snapshot retention lease 或跨表事务，再评估相应的可用性/一致性保证；本条 pin 本身不具备这些能力。
- 若产品取消并发源追加或改变一次刷新读取冻结端点的能力，重新裁决 head 稳定条件，不能靠局部谓词悄悄改变。
