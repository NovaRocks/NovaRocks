---
id: ADR-0135
title: "Statistics use ordinary aggregates, dataflow artifacts, and provider-owned publication sessions"
domain: [provider-spi]
status: active
supersedes: [ADR-0022, ADR-0082]
superseded-by: null
date: 2026-09-04
provenance:
  - "discussion: 2026-09-02 through 2026-09-04 statistics collection data plane, Trino comparison, and Iceberg OCC semantics"
  - "PR: pending — backfill the number once the statistics cutover merges"
code-anchors:
  - "novarocks/spi/src/connector/statistics.rs (ConnectorStatistics, StatisticsCollectionSession, StatisticsArtifactDraft)"
  - "novarocks/sql/src/planner/distributed/write/auxiliary.rs (plan_writer_statistics)"
  - "novarocks/execution/src/exec/operators/table_writer.rs (TableWriterOperatorFactory)"
  - "novarocks/execution/src/exec/operators/table_finish.rs (TableFinishOperatorFactory)"
  - "novarocks/frontend/src/query_execution/write_result.rs (RootWriteResultDecoder)"
  - "novarocks/connector/iceberg/src/catalog_control/statistics.rs (IcebergStatisticsCollectionSession)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control.rs (IcebergWriteSessionControl)"
---

## 问题

统计收集应当使用一条 provider 专用执行通路，还是复用普通聚合与写入数据流？统计 artifact 如何到达 FE，Iceberg 又应如何在不重读已写数据、不透明重放事务的前提下完成 ANALYZE 和 collect-on-write 发布？

## 背景与执行事实

旧设计把 statistics collection 建模成 FE-only capability 返回 opaque plan/result，再由专用
`StatisticsSink` 在 BE 收集并把 partial bytes 塞进 query lifecycle terminal。Execution 因而必须认识
statistics metric、provider payload 与专用 collector，terminal 同时承担执行结论和业务结果运输。旧的
collect-on-write 还在 writer 旁维护 per-file sketch，数据 snapshot 提交后再做一次 metadata-only
statistics commit；这既产生第二遍统计实现，也让数据与统计不是同一个原子 Iceberg transaction。

Trino 提供的成熟形状不是 per-query connector aggregate ABI。它把 Iceberg theta 函数作为隐藏的普通引擎
聚合注册；planner 选择普通 aggregation，`TableWriterOperator` 将同一 page 同时交给 page sink 与
aggregation，`TableFinishOperator` demultiplex writer rows 并完成 final aggregation。connector metadata
只拥有 collection planning 和最终 publication，不向通用执行算子导出裸聚合状态 ABI。

NovaRocks 的进程边界与 Trino 不同：Arrow pipeline 只在 BE，外部 catalog mutation 只允许 FE。因此完整
照搬 `TableFinishOperator -> metadata.finish*` 会让 BE 获得 catalog mutation 权限；让 FE 执行 final
aggregate 又会破坏 FE/BE 角色边界。已有 ADR-0133 已裁决 writer fragment 走普通数据流、单 Root BE 完成
有界汇聚、FE 以 Root EOF 与全 participant success 的合取启动唯一外部提交。本决策把统计纳入该形状。

Iceberg Rust 依赖原有 `Transaction` 是延迟 action 模型：commit 时才求值，冲突 refresh 后整体重放，重放
会重新生成 snapshot id，且 transaction 内没有可读取的已 staged snapshot。它无法表达“先 stage 数据
snapshot，读取该 snapshot id/sequence，再 stage SetStatistics”，其透明 retry 也违反 ADR-0118 的
single-dispatch 与 unknown-outcome 闭权。仅在应用层模拟 transaction-local snapshot 会形成第二套 Iceberg
transaction 语义 owner。

统计是优化证据，不是 read/write correctness。Iceberg 和跨引擎生态也没有可靠、标准的 whole-table
coverage 属性可供所有写入者共同仲裁。项目私有 coverage 排序会把外部统计默认降级，并在冲突后引入额外
re-arbitration authority，却不能证明结果真的覆盖全表。

## 考虑过的选项

**A. 保留专用 StatisticsSink、opaque provider plan/result 与 terminal partial。** 局部改动少，但会永久保留
第二套 aggregation、result transport 和 completion authority。Execution 必须理解 provider metric，terminal
既证明生命周期又运输业务结果，新增 histogram/MCV 还要扩展 opaque envelope 与专用 merge 逻辑，因此否决。

**B. 建立 provider aggregate ABI。** provider 可以按 query 注册聚合函数，但 NovaRocks 当前聚合 ABI 是
Execution arena 内的定长 state、裸 state pointer 与手工 drop。把它导出为跨 crate/provider contract 会把
内存布局和 unsafe 生命周期冻结成公共 ABI，并需要另建动态 heap footprint hook。Trino 也没有这套 ABI；
它注册的是普通全局隐藏函数。因此否决。

**C. 让 Execution 认识 statistics descriptor，并在 final aggregate 后专门整形。** 实现直接，但 descriptor
中的 field id、blob type 与 statistic identity 会泄漏到通用执行层。宽转长本质是通用 Unpivot；让 Execution
只认识 typed mapping、constant expression 和 output schema，扩展性更强。因此否决专用 shaper。

**D. collect-on-write 在数据提交后重读新 snapshot。** 可以复用 ANALYZE，但对象存储扫描、Parquet 解码与
Theta hash 的成本与刚完成的写入规模线性相关，且扩大失败窗口。writer 已经看过每一行；成熟做法是同 page
喂给 writer 与 partial aggregate。因此否决。

**E. 应用层包装 vendored 延迟 transaction，模拟 eager staged snapshot。** 可以暂时满足调用面，却让应用
层解释 Iceberg action 顺序、snapshot identity、requirements 与重放语义，形成与依赖库并列的 transaction
owner。因此否决。

**F（选中）. 普通聚合 + generic Unpivot + 统一 writer/Root relation + provider-owned single-use session。**
Execution 只执行 seal 后的普通函数与通用算子；统计 artifact 作为数据流行到 FE；Iceberg provider 使用真正
eager 的 vendored transaction，外部 effect 仍只经过 ADR-0118 frontier。

## 裁决

统计聚合函数进入 process-wide immutable engine function catalog。provider bundle只能在 startup 注册普通
隐藏函数及其 safe typed state contributor；query 只能引用已经 seal 的 exact resolved signature，不能注册
per-query function、查 connector role binding 或导出 raw arena ABI。Execution 拥有 allocation、state layout、
drop 与 retained-memory admission，provider 函数只拥有具体 state 和 typed batch 语义。

ANALYZE 的非空 collection 是普通 `Scan -> local Aggregate -> Gather -> global Aggregate -> Unpivot -> ResultSink`
计划。provider 的 single-use collection session 在 FE 冻结 table UUID、snapshot、schema field id/type、函数与
期望 artifact identity；Execution 和 Unpivot 不认识 statistics、Puffin 或 provider。Root relation 直接输出
`(input_fields, blob_type, body, properties)`，FE 只在精确 schema、identity、预算、Root EOF 与 lifecycle
all-success 全部成立后 consuming finish session。空 requirement 在建 plan、建 task、读数据前直接完成空结果。

collect-on-write 复用相同普通 aggregate。每个 `TableWriter` 收到一个 page 后，在承诺任何一侧前同时取得
writer queue 与 partial aggregate 的容量；随后同一 page 进入异步 page sink 和 partial aggregate。两侧任一
blocked 都向上游传播 backpressure，任一失败都使 attempt 失败。writer row count、commit fragment 与 sparse
partial state 以一个固定 writer relation multiplex；单 Root `TableFinish` demultiplex、完成 final aggregate、
调用 generic Unpivot，再以固定 Root relation multiplex SUMMARY、prepared fragment 与 artifact draft。Root
EOF 证明数据面完整，participant terminal set 证明执行成功；二者以及 cancel/deadline veto 构成 FE commit
barrier，互不替代。

artifact identity 是 `(query-local target ordinal, input field ids, blob type)`。query-local target set 从 SQL
的 TableFinish 冻结到 FE-only decode contract，不能用 session 的最大 ordinal、wire 反推或 coordinator 猜测。
一个 statement 驱动多个 query 时，session 对 row count、fragment、artifact 与所有 byte/count budget 做
事务性 union，最后只发出一次 provider finish request。

发布目标是 Puffin，因此跨层值直接采用与 Puffin blob 同构的 draft，而不是 Arrow union 或 provider result
envelope。NDV、未来 histogram、MCV 等以不同 `blob_type/body/properties` 扩展；generic data plane 无需增加
统计种类分支。Puffin codec、snapshot ancestry、field type 与 set union 仍由 Iceberg provider 拥有。

Iceberg transaction 改为 eager stage：每个 action 被调用时立刻、确定性地作用于 transaction-local metadata；
后续 action 能读取刚 stage 的 snapshot id 与 sequence。export 只导出完整 staged commit，不产生外部 I/O；
commit 对该导出结果只 dispatch 一次，不 refresh、不 replay、不生成新 snapshot id、不透明 retry。
collect-on-write 在同一个 eager transaction 中依次 stage data snapshot、基于精确 parent 做 field-wise statistics
union、写 attempt-owned Puffin、stage SetStatistics，最后把完整 transaction 交给 ADR-0118 frontier。

OCC 只在 provider 层对明确的 `KnownUncommitted::Conflict` 建立一个 fresh attempt；fresh attempt 重载 metadata，
复用已经计算的 data files 与 artifact body，重新生成 attempt-local manifest/Puffin 并重新 eager stage，绝不
重读输入数据。每个 attempt 最多一次外部 dispatch；`CommitUnknown` 后禁止 retry、abort cleanup 或重新发布，
只能 reconcile。ANALYZE 同样由 single-use provider session拥有 fresh-attempt policy，不调用依赖库透明 replay。

不再发布或读取私有 whole-table coverage 属性，也不按 coverage ranking 仲裁。同 snapshot 的统计是允许被
更新的优化证据；读侧按 snapshot ancestry、field id/type 与各 metric 自身可用性保守使用，缺失或陈旧只影响
优化质量。旧 `StatisticsSink`、custom collector、per-file sketch、post-commit registration 与 terminal
statistics payload 整体删除，其 proto number/name 永久 reserved。

## 接受的妥协（诚实记录）

**计划统计失败会使写入失败。** 同 page 的 writer 与 aggregation 是一个 composite operator；既然计划选择
collect-on-write，统计 OOM、函数错误或 relation 破坏都会阻止数据提交。这牺牲了“数据优先、统计随缘”的
局部可用性，换来一个清晰的原子 execution contract：不能静默提交一个少了计划分支的结果。若产品将来要
best-effort collection，必须在 planning 前显式选择空 auxiliary plan，不能在执行中吞错降级。

**单 Root 仍是汇聚瓶颈和 attempt 故障点。** sparse partial 与 byte-aware batching 降低了流量和峰值，但
final aggregate、Unpivot 与所有 prepared fragments 仍经过一个 BE。选择它是为了维持唯一 Root/FE commit
authority并复用现有 result transport，不是因为它能无限扩展。

**同一 page 同时进入 writer 与 aggregate 会增加 CPU 和内存压力。** 它避免了代价更大的二次对象存储扫描，
却不能消除 sketch hash 本身的成本；两侧合取 backpressure 还意味着较慢一侧限制整体吞吐。这是准确计算
write-time statistics 的必然成本，是否启用由 provider planning 决定。

**放弃 coverage ranking 可能让更高质量统计被同 snapshot 的后一次发布替换。** 这是有意接受的：生态没有
可验证的标准 coverage 证明，私有标记只会产生虚假的强保证和跨引擎偏见。统计不是 correctness；宁可让读侧
保守面对质量波动，也不让提交点维护一个无法证明的全局偏序。

**维护一个 vendored Iceberg transaction patch。** eager semantics 必须在真正拥有 action/snapshot 构造的
库层实现，否则应用层会成为第二 owner。代价是上游升级时必须重做 action 顺序、determinism、zero-I/O export、
single-dispatch 与 conflict tests；这不是一次性补丁成本。

**这是不兼容 hard cut。** 旧 terminal tag、provider opaque result、private coverage property 和旧 Theta
carrier 不保留 dual reader/writer。当前没有需要兼容的历史用户资产，所以选择结构清晰的一次切换；若部署前
发现真实历史数据，这一前提失效，必须另行设计迁移，而不是恢复双 authority。

## 何时重新评估

- profile 显示单 Root 的 final aggregate、Unpivot 或 fragment mux 持续成为主要瓶颈，或经常逼近已冻结的
  count/body/retained-memory 上界；届时评估分层 finish 或 spool，但必须保留唯一 completion authority；
- 出现多个独立动态 provider，确有运行期安装聚合实现的产品需求；届时可以设计稳定的 safe plugin ABI，不能
  直接暴露当前 arena pointer/state layout；
- 统计成为 read/write correctness 或 freshness invariant；届时必须设计 durable coverage/evidence protocol，
  当前 best-effort snapshot evidence 不可升级解释为强保证；
- 上游 Iceberg Rust 提供 eager staged、transaction-local snapshot 可读、zero-I/O export 与 single-dispatch
  commit 的等价 API；通过 action determinism/OCC/unknown tests 后可删除 vendor patch；
- collect-on-write 的 CPU 成本在目标 workload 上不可接受；先比较显式关闭 auxiliary plan、采样与异步独立
  ANALYZE，不得回到提交后重读同一新数据的默认路径；
- 标准化生态出现跨引擎可验证的 statistics coverage/freshness contract；只有能够独立证明而非相信私有属性时，
  才重新讨论同 snapshot 仲裁。
