# Iceberg IMV-v2: Refresh / Apply 抽象设计

日期：2026-06-03
状态：Spec / 待实现计划
范围：Iceberg-backed IMV 的物理 refresh 编排与 apply 契约抽象

---

## 0. TL;DR

本任务不实现 B 族 `UNION ALL of aggregate branches`。B 族只作为下一
个任务的预期应用方，用来校验本次抽象边界是否足够。

当前 NovaRocks 的 IMV 逻辑 rewrite 已经比较统一：`ImvDelta` /
`ImvVersion` marker 进入 pipeline 后，由 join、union、aggregate、
scan binding、action propagation、apply-key 等 rule 组合出 delta plan。
差异的主体已经在 plan / rule / operator 组合里。

真正需要重构的是 Iceberg MV 的物理 refresh 编排层。现在
`refresh_iceberg_mv_with_planned_partitions` 先 classify shape，再分派
到 projection/filter、join projection、UNION projection、single
aggregate、fan-in aggregate、join aggregate 等多个 `refresh_*` 函数。
这些函数各自重复处理 base load、snapshot pin、previous/current 判断、
first refresh、metadata-only refresh、incremental apply 和 refresh
intent。

本设计把这层收敛为：

1. 一个统一的 refresh lifecycle driver；
2. 一个 shape adapter 接口，只声明 shape 必要策略；
3. 一个一等 `ApplyKeyContract`，替代散落的 apply-key 参数；
4. first-refresh 与 incremental-apply 的统一入口；
5. B 族作为 future adapter，不在本任务接入执行。

目标是：新增 shape 时不再新增一套 bespoke refresh 编排，只补 plan
rewrite / first-refresh strategy / apply-key contract。

---

## 1. 当前问题

用户直觉是正确的：refresh 不应该因为 SQL shape 不同而复制一套完整流程。
shape 差异主要应该体现在 logical plan rewrite 和执行算子组合里。

当前代码的问题是把两类差异混在一起：

- 语义差异：例如 delta 是否穿过 union，join delta 如何展开，aggregate
  如何做 signed-state merge。这些应该由 optimizer rewrite pipeline 负责。
- 物理生命周期差异：例如哪些 base snapshot 被 pin，本次是 first refresh
  还是 incremental refresh，如何提交 staging branch，target row 用什么 key
  定位。这些是 refresh/apply 协议，不是普通 plan 算子能独自完成的。

因此本次抽象不是让 refresh 完全不知道 shape，而是把 shape-specific 的
内容压缩成清晰策略：

- base snapshot policy；
- schema contract / rebind policy；
- context builder；
- first refresh strategy；
- incremental apply strategy；
- apply key contract；
- rewrite validation evidence。

driver 负责通用生命周期，adapter 负责少量语义策略。

---

## 2. 代码现状

### 2.1 逻辑 rewrite 已经接近统一

`src/sql/optimizer/rewrite/imv/pipeline.rs` 的 stage 顺序是：

```text
logical-normalize
delta-marker
join-delta
union-delta
aggregate-state
delta-pushdown
scan-binding
action-propagation
apply-key
marker-cleanup
validation
```

这个 pipeline 已经把大部分 shape 语义下沉到 rule 组合：

- `RewriteJoinAggregateDeltaRule` 处理 join aggregate delta；
- `RewriteUnionAggregateDeltaRule` 处理 A 族 `Aggregate(UNION ALL(...))`；
- `RewriteTopLevelUnionDeltaRule` 处理 top-level union projection/filter；
- `RewriteAggregateStateRule` 处理 aggregate state；
- `BindIcebergScanRule` 基于 refresh context 和 pin 绑定各 base scan 的
  snapshot window。

这层不需要推倒重来。

### 2.2 物理 refresh 仍按 shape 分派

`src/engine/mv/iceberg_refresh.rs` 当前主要路径包括：

- single projection/filter：`refresh_iceberg_mv_with_planned_partitions`
  内联处理；
- join projection/filter：`refresh_iceberg_join_mv`；
- UNION ALL projection/filter：`refresh_iceberg_union_projection_mv`；
- single aggregate：`refresh_single_aggregate_iceberg_mv`；
- A 族 fan-in aggregate：`refresh_fan_in_aggregate_iceberg_mv`；
- join aggregate：`refresh_join_aggregate_iceberg_mv`。

这些函数的 shape 语义不同，但生命周期骨架高度重复：

1. pre-pin load base table；
2. 根据 previous/current snapshot 判断 skip、first、metadata-only、
   incremental 或 fail-fast；
3. capture `RefreshSnapshotPin`；
4. 校验 table uuid 和 schema contract；
5. 构造 `IcebergMvRefreshContext`；
6. first refresh 时创建 staged intent 并写 target；
7. unchanged 时 metadata-only finalize；
8. incremental 时构造 base changes 并进入 apply。

### 2.3 apply 核心已经半统一

`incremental_refresh_iceberg_mv_with_changes` 已经是一个共同 apply 核心：

- 接收一组 `RewriteMergeBaseChange`；
- plan base snapshot changes；
- 处理 empty delta；
- 创建 staging branch 和 refresh intent；
- 构造 IMV refresh catalog；
- 执行 rewritten query；
- 通过 `IcebergMergeSink` 写 data files / position deletes；
- publish staging branch 并 finalize metadata。

现在的问题是它的上游仍由 shape-specific 函数手工准备参数，并用
`RewriteMergeRefreshOptions` 传散装配置：

- `apply_key_column`；
- `apply_key_value_type`；
- `allow_full_rebuild_on_policy_full_refresh`；
- `rewrite_evidence`；
- `preload_locator_for_change_stream_deletes`。

这些应该被提升为一等 apply contract。

---

## 3. 设计目标

### 3.1 本次目标

本次实现完成后，应满足：

- 现有所有已支持 Iceberg IMV refresh shape 都通过统一 driver 编排；
- shape-specific refresh 函数不再拥有完整 lifecycle；
- first / metadata-only / incremental 决策集中在 driver；
- `ApplyKeyContract` 成为 refresh/apply 的显式输入；
- B 族未来可以作为 adapter 接入，不需要新增 bespoke refresh driver；
- 现有行为保持不变。

覆盖现有 shape：

- projection/filter；
- join projection/filter；
- UNION ALL projection/filter；
- single aggregate；
- A 族 fan-in aggregate；
- join aggregate。

### 3.2 非目标

本任务不做：

- B 族 logical rewrite；
- B 族 first-refresh SQL；
- B 族 row-id 编码；
- B 族 ignored test 解禁；
- PCT-style full refresh fallback；
- CREATE-time trial rewrite；
- 改变 Iceberg target 表存储模型；
- 改变 optimizer rewrite pipeline 的 rule 语义。

B 族只在 spec 和接口设计中作为未来 consumer。

---

## 4. 核心架构

### 4.1 Refresh Driver

新增统一 driver，工作名：

```text
IcebergMvRefreshDriver
```

driver 负责所有 shape 共享的生命周期：

1. 加载 target / base refresh metadata；
2. 根据 adapter 的 base ordering 预加载 base tables；
3. 应用 `BaseSnapshotPolicy` 得出 refresh decision；
4. capture `RefreshSnapshotPin`；
5. 校验 table uuid；
6. 调用 adapter 执行 schema contract / rebind；
7. 构造 `IcebergMvRefreshContext`；
8. 分发到 first refresh、metadata-only refresh 或 incremental apply；
9. 统一处理 refresh intent、staging branch、abort、publish、finalize。

driver 不负责：

- classify SQL shape；
- 重写 logical plan；
- 生成 shape-specific first-refresh SQL；
- 决定 apply key 语义；
- 判断 B 族是否支持。

### 4.2 Shape Adapter

每个 shape 提供一个 adapter。adapter 是 strategy，不是完整 refresh path。

建议接口职责：

```text
RefreshShapeAdapter {
  label()
  base_refs()
  base_snapshot_policy()
  validate_pre_pin()
  validate_after_pin_and_rebind()
  build_refresh_context()
  first_refresh_strategy()
  incremental_apply_strategy()
}
```

其中：

- `validate_pre_pin` 用于在可能 skip 的场景前做必要 contract 检查；
- `validate_after_pin_and_rebind` 返回 effective MV definition 和必要的
  reclassified shape；
- `build_refresh_context` 可选择是否携带 affected partitions；
- `first_refresh_strategy` 只描述如何产出/执行首次物化；
- `incremental_apply_strategy` 提供 `ApplyKeyContract`、rewrite evidence、
  full-rebuild fallback policy 和 locator preload policy。

### 4.3 BaseSnapshotPolicy

base snapshot policy 显式表达当前散落在各函数里的差异。

建议枚举：

```text
BaseSnapshotPolicy =
  SingleBase
  AllBasesRequired
  JoinPairPartialInitialSkip
```

语义：

- `SingleBase`：没有 previous 且 base 无 current snapshot 时 skip；有
  previous 但 current 不可达时 fail-fast。
- `AllBasesRequired`：多 base first refresh 必须所有 base 都有 current
  snapshot；如果全都没有 current snapshot 则 skip；部分有 current snapshot
  则 fail-fast。
- `JoinPairPartialInitialSkip`：join projection / join aggregate 在 first
  refresh 前，如果只有一边有 current snapshot，保持当前行为：skip initial
  refresh，而不是 capture pin 后报错。

这个策略是完整抽象的关键：差异不是藏在每个 refresh 函数里，而是被命名、
测试和集中执行。

### 4.4 RefreshDecision

driver 根据 previous metadata、pre-pin current snapshots 和 policy 生成：

```text
RefreshDecision =
  SkipEmpty
  FirstRefresh
  MetadataOnly
  Incremental
  FailFast(reason)
```

`MetadataOnly` 只在已有 previous 且所有 tracked base snapshot 不变时出现。
`Incremental` 只在 previous metadata 完整且至少一个 tracked base snapshot
变化时出现。

partial previous metadata 一律 fail-fast，要求 recreate MV。

### 4.5 ApplyKeyContract

用 `ApplyKeyContract` 替代 `RewriteMergeRefreshOptions` 里的散装 apply-key
字段。

建议结构：

```text
ApplyKeyContract {
  column_name
  value_type
  source
  rewrite_evidence
  locator_preload
  full_rebuild_fallback
}
```

当前可表达现有类型：

- projection/filter：`__nova_base_row_id` + `Int64` + no evidence；
- UNION projection/filter：`__nova_base_row_id` + `BranchInt64` + no evidence；
- aggregate：`__row_id__` + `Utf8` + aggregate evidence；
- join aggregate：`__row_id__` + `Utf8` + join aggregate evidence。

本任务不新增 B 族类型。B 族未来需要 branch-scoped aggregate identity，
但这应作为 `ApplyKeyContract` 的 future extension，而不是本次实现。

### 4.6 FirstRefreshStrategy

first refresh 统一进入 driver 的 staged intent 生命周期，但具体物化方式由
adapter 提供。

现有 first-refresh 类型：

- projection/filter：生成 pinned physical SQL 后走
  `first_refresh_iceberg_mv_with_physical_sql`；
- UNION projection/filter：生成带 branch id / hidden key 的 full-refresh SQL
  后走 physical SQL first refresh；
- aggregate / A 族 / join aggregate：走
  `first_refresh_iceberg_aggregate_mv`；
- join projection/filter：走 `first_refresh_iceberg_join_mv`。

本次不强行把所有 first-refresh SQL builder 合成一个函数。先统一 staged
intent / abort / publish / finalize 生命周期，再把 SQL 生成作为 strategy。
这样能完整抽象 lifecycle，同时避免在第一步改动过大。

### 4.7 IncrementalApplyStrategy

incremental apply 统一进入：

```text
incremental_refresh_iceberg_mv_with_changes
```

但调用方只传：

- refresh context；
- base changes；
- optional full rebuild SQL；
- `ApplyKeyContract`。

`RewriteMergeRefreshOptions` 可以保留为内部适配层，但不再由每个
shape-specific refresh 函数手写。

---

## 5. Data Flow

统一后 refresh data flow：

```text
REFRESH MV
  -> load MV definition / target
  -> classify shape
  -> build shape adapter
  -> driver.preload_bases()
  -> driver.decide(policy, previous, current_before_pin)
  -> if SkipEmpty: return Ok
  -> capture RefreshSnapshotPin
  -> validate uuid + schema contract
  -> build IcebergMvRefreshContext
  -> if FirstRefresh:
       adapter.first_refresh_strategy.execute(driver lifecycle)
     if MetadataOnly:
       finalize_iceberg_mv_metadata_only_refresh
     if Incremental:
       build RewriteMergeBaseChange[]
       incremental_apply(ctx, changes, ApplyKeyContract)
```

logical plan rewrite 仍发生在 query execution 内部：

```text
stored MV SELECT
  -> IMV rewrite pipeline
  -> delta/version scan binding through refresh ctx + pin
  -> action/apply-key columns
  -> IcebergMergeSink
  -> data files + position deletes
  -> staging branch publish
```

---

## 6. Error Handling

driver 统一保留当前 fail-fast 语义：

- target snapshot 不匹配：fail-fast；
- base table uuid 与 previous metadata 不一致：fail-fast；
- previous metadata partial：fail-fast；
- previous snapshot 不可达：fail-fast；
- schema contract incompatible：fail-fast；
- required rewrite evidence 缺失：fail-fast；
- multi-base full rebuild fallback：默认 fail-fast，除非 adapter 明确支持；
- staging branch / commit / publish 失败：沿用现有 abort / recovery flow。

skip 只允许出现在明确策略里：

- single base 没有 previous 且 base 无 snapshot；
- all-bases-required 且所有 base 都无 snapshot；
- join pair first refresh 前只有一侧有 snapshot。

任何未被策略命名的 partial 状态都不能 silent skip。

---

## 7. B 族作为未来应用方

B 族定义为：

```text
UNION ALL(
  Aggregate(base_1),
  Aggregate(base_2),
  ...
)
```

它和 A 族不同：

- A 族是 `Aggregate(UNION ALL(...))`，相同 group key 应跨 branch 合并；
- B 族是 `UNION ALL(Aggregate(...), Aggregate(...))`，相同 group key
  在不同 branch 内必须保持独立 bag semantics。

本任务不实现 B 族，但抽象必须允许未来 B adapter 表达：

- `BaseSnapshotPolicy::AllBasesRequired`；
- aggregate-like first-refresh strategy；
- branch-scoped aggregate apply key；
- B-specific rewrite evidence；
- no bespoke refresh lifecycle。

未来 B 族最可能的 apply-key 方向是让 aggregate row id 显式包含 branch
identity，并继续复用 Utf8 locator。这个 row-id policy 需要同时覆盖
first-refresh 物化和 aggregate physical row validation，否则 target 中
跨 branch 同 group key 会被错误折叠或校验失败。

本次只在接口上保留这个扩展点，不实现 row-id 编码和 B rewrite rule。

---

## 8. 迁移计划

### Phase 1: 提取公共类型与 decision 逻辑

- 新增 refresh driver / adapter / decision / policy / apply contract 类型；
- 为 base snapshot decision 写单元测试；
- 暂不大规模移动 first-refresh SQL builder；
- 行为保持不变。

### Phase 2: 接入 projection/filter 与 UNION projection/filter

- single projection/filter adapter；
- UNION projection/filter adapter；
- 移除这两类路径中重复的 pin / previous / metadata-only / incremental
  编排；
- 保留原有 physical SQL first-refresh builder。

### Phase 3: 接入 aggregate family

- single aggregate adapter；
- A 族 fan-in aggregate adapter；
- join aggregate adapter；
- 保留 schema rebind 和 affected partitions 语义；
- 保留 aggregate / join aggregate rewrite evidence 校验。

### Phase 4: 接入 join projection/filter

- join projection/filter adapter；
- 保留 `JoinPairPartialInitialSkip` 行为；
- 保留现有 first refresh / incremental join apply 语义。

### Phase 5: 收口旧入口

- `refresh_iceberg_mv_with_planned_partitions` 只负责：
  - load target / definition；
  - classify shape；
  - build adapter；
  - call driver。
- 删除或降级旧 `refresh_*` 函数为 adapter helper；
- `RewriteMergeRefreshOptions` 降为 `ApplyKeyContract` 的内部转换。

B 族不在本迁移计划内。

---

## 9. 测试计划

### Unit Tests

新增 base snapshot decision 测试：

- single base: no previous + no current => skip；
- single base: previous + no current => fail-fast；
- all bases required: all empty => skip；
- all bases required: partial current first refresh => fail-fast；
- all bases required: partial previous metadata => fail-fast；
- all bases required: unchanged => metadata-only；
- all bases required: one changed => incremental；
- join pair: one side current on first refresh => skip；
- join pair: previous + missing current => fail-fast。

新增 adapter contract 测试：

- projection/filter 产出 Int64 base-row apply key；
- UNION projection 产出 BranchInt64 apply key；
- aggregate 产出 Utf8 group-row apply key + aggregate evidence；
- join aggregate 产出 Utf8 group-row apply key + join aggregate evidence。

### Regression Tests

保持现有 `iceberg_refresh` 模块测试通过，重点覆盖：

- projection/filter first / metadata-only / incremental；
- UNION projection/filter branch apply；
- single aggregate first / incremental；
- A 族 fan-in aggregate first / incremental / delete；
- join projection/filter partial initial skip；
- join aggregate update/delete retraction。

### SQL Suite

本任务完成后至少运行相关 Iceberg IMV SQL suite。若时间允许，运行完整
Iceberg suite。B 族 ignored test 保持 ignored。

---

## 10. 验收标准

本任务完成时：

- 现有 supported shape 行为不变；
- refresh lifecycle 的重复逻辑集中到 driver；
- shape-specific code 只保留 adapter 策略和必要 helper；
- `ApplyKeyContract` 成为 apply 语义的一等结构；
- `refresh_iceberg_mv_with_planned_partitions` 不再按 shape 进入完整
  bespoke lifecycle；
- B 族仍未实现，但 spec 能说明它未来如何作为 adapter 接入；
- B 族 ignored test 不解禁、不改预期。

---

## 11. 风险与约束

- join projection / join aggregate 的 partial initial skip 是真实行为，
  不能被 all-current 通用逻辑误改。
- single projection/filter 的 full-rebuild fallback 当前允许，multi-base
  路径当前不允许；抽象后必须保留差异。
- schema rebind 在 single aggregate / join aggregate 路径较完整，fan-in
  aggregate 当前仍有限制；driver 不能把 rebind 语义抹平。
- first-refresh builder 不能在第一阶段过度统一，否则容易同时影响 target
  schema、hidden columns、branch id、aggregate state layout。
- B 族 branch-scoped row identity 需要后续单独设计和测试，不能在本任务中
  通过隐式字符串拼接偷渡。

---

## 12. 关键代码位置

NovaRocks：

- `src/engine/mv/iceberg_refresh.rs`
  - refresh dispatcher；
  - shape-specific refresh paths；
  - `incremental_refresh_iceberg_mv_with_changes`；
  - first-refresh helpers；
  - metadata-only finalize。
- `src/engine/mv/refresh_context.rs`
  - `IcebergMvRefreshContext`。
- `src/engine/mv/iceberg_merge_sink.rs`
  - `ApplyKeyValueType` and merge sink plan。
- `src/engine/mv/iceberg_target_apply.rs`
  - target row locator helpers。
- `src/sql/optimizer/rewrite/imv/pipeline.rs`
  - IMV rewrite pipeline。
- `src/sql/optimizer/rewrite/imv/union_delta.rs`
  - A 族 and top-level union delta rewrite。
- `src/sql/optimizer/rewrite/imv/join_delta.rs`
  - join aggregate delta rewrite。
- `src/connector/starrocks/table/mv_agg_state.rs`
  - aggregate row-id materialization and validation。

StarRocks reference remains useful for conceptual comparison, but this spec is
based on current NovaRocks code and does not copy StarRocks refresh architecture.
