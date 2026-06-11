# Iceberg IMV B 族 UNION ALL Aggregate 设计

日期：2026-06-04
状态：Spec / 待实现计划
范围：Iceberg-backed IMV 的 B 族 `UNION ALL` aggregate branches 增量刷新

---

## 0. TL;DR

本任务实现 B 族 Iceberg IMV：

```sql
SELECT k, agg(...) FROM t1 GROUP BY k
UNION ALL
SELECT k, agg(...) FROM t2 GROUP BY k
```

B 族和已经支持的 A 族 `Aggregate(UNION ALL(...))` 不同。A 族在 union
之后再 aggregate，相同 group key 会跨 branch 合并。B 族是多个 aggregate
结果做 `UNION ALL`，相同 group key 在不同 branch 必须保持独立行。

因此 B 族 target row identity 不能只使用 aggregate group row id。它必须由
branch identity 和 branch 内部 group row id 共同决定：

```text
(__branch_id__, __row_id__)
```

本设计把现有 `UnsupportedBranchUnionAggregate` 升级为可执行的
`BranchUnionAggregate`。差异只进入 refresh contract、logical rewrite、
target-state read 和 apply locator；refresh lifecycle 继续复用当前统一
refresh driver / apply contract 抽象，不新增 bespoke refresh 大分叉。

---

## 1. 当前状态

`origin/main` 已经具备本任务的主要前置条件：

- `ImvRefreshContract` 从 analyzed query 派生；
- `RefreshStrategy` 已经区分 projection/filter、join、A 族 fan-in aggregate、
  join aggregate 和 B 族 unsupported contract；
- CREATE 阶段能识别 B 族，但当前 fail-fast；
- A 族 `Aggregate(UNION ALL(...))` 已通过 `RewriteUnionAggregateDeltaRule`
  接入 IMV rewrite pipeline；
- top-level union projection/filter 已有 `RewriteTopLevelUnionDeltaRule`；
- target schema / schema contract 已有 `__branch_id__`、`BranchUnionContract`
  和 `BranchInt64` projection union locator 的基础设施。

仍缺失的是 B 族 aggregate branches 的执行语义：

- `RefreshStrategy::BranchUnionAggregate`；
- branch-scoped aggregate target-state read；
- B 族 `RewriteBranchUnionRule`；
- aggregate branch 的 composite locator；
- B 族 refresh path 放行和端到端 SQL fixture。

---

## 2. 语义边界

### 2.1 支持的形态

本任务只支持所有 branch 都是 aggregate 的 top-level `UNION ALL`：

```sql
SELECT g1, ..., agg1(...), agg2(...)
FROM base_1
WHERE ...
GROUP BY g1, ...
UNION ALL
SELECT g1, ..., agg1(...), agg2(...)
FROM base_2
WHERE ...
GROUP BY g1, ...
```

所有 branch 必须满足：

- branch kind 都是 aggregate；
- group key 数量、aggregate 数量、visible output layout 兼容；
- output column 数量和类型兼容；
- 每个 branch 绑定到一个 Iceberg base table；
- aggregate 能被现有 aggregate-state merge 框架表达。

### 2.2 非目标

本任务不支持：

- 混合 projection/filter branch 和 aggregate branch；
- `UNION` distinct；
- empty group by aggregate；
- `SELECT DISTINCT`；
- window / CTE / HAVING / grouping sets；
- branch 内 join aggregate；
- B 族 projection/filter path 的重新设计；
- 通过字符串拼接把 branch id 塞进 row id。

Projection/filter union 已经有自己的 `BranchInt64` path。本任务只把 aggregate
B 族打通。

---

## 3. 核心架构

### 3.1 RefreshStrategy

将现有：

```text
UnsupportedBranchUnionAggregate
```

升级为：

```text
BranchUnionAggregate
```

`derive_imv_refresh_contract` 在看到
`UnionAll(Aggregate(...), Aggregate(...))` 时派生：

```text
ImvRefreshContract {
  strategy: BranchUnionAggregate,
  aggregate: Some(...),
  branch: Some(...),
  apply_key: branch_scoped_aggregate_group_row,
  ...
}
```

`branch_scoped_aggregate_group_row` 的概念含义是：inner apply key 是 aggregate
group row id，target row locator 额外按 `__branch_id__` 限定。它不需要把
branch id 编码进 `__row_id__` 字符串。

### 3.2 BranchScope

在 `IcebergMvTargetStateRowFilter::DeltaInputRowIds` 中增加 branch scope：

```text
DeltaInputRowIds {
  row_id_column_name,
  branch_scope: Option<BranchScope>,
}

BranchScope {
  branch_id_column_name,
  branch_id,
}
```

普通 aggregate、A 族 fan-in aggregate 和 join aggregate 使用 `None`。
B 族每个 branch 的 aggregate state merge 使用 `Some(branch_id)`，从而 old
target-state read 只读取当前 branch 的 target rows。

### 3.3 AggregateStateMerge Builder

从 `RewriteAggregateStateRule` 中抽出：

```text
build_aggregate_state_merge(aggregate, action_column, branch_scope, ext)
```

调用约定：

- ordinary aggregate / A 族：`branch_scope = None`；
- B 族：每个 branch 使用对应 `branch_scope = Some(i)`。

这个 builder 必须保留现有 aggregate rewrite 的所有校验，包括 non-empty
group by、distinct aggregate 拒绝、aggregate state layout 校验和 target
state schema contract 校验。

### 3.4 RewriteBranchUnionRule

新增 IMV structural rewrite rule：

```text
Delta(UnionAll(Aggregate(base_1), Aggregate(base_2), ...))
  ->
UnionAll(
  Project(AggregateStateMerge(branch=0), __branch_id__ = 0),
  Project(AggregateStateMerge(branch=1), __branch_id__ = 1),
  ...
)
```

rule 放在 pipeline 的 structural rewrite 阶段，位于 root delta marker 之后、
generic delta pushdown 之前。它只处理 top-level B 族 union，不处理 A 族
`Delta(Aggregate(Union(...)))`。

每个 branch 的 visible outputs 继续来自 aggregate-state merge；额外 hidden
output 是 `__branch_id__`，类型为 `Int32`。

### 3.5 Action Propagation / Validation

现有 action propagation 和 validation 会对 unsupported union fail-fast。
B 族 rewrite 后的 top-level union 应被识别为 supported branch union：

```text
UnionAll(Project(AggregateStateMerge(...), __branch_id__), ...)
```

因此需要新增 predicate，允许这种 rewritten union 通过 action propagation
和 validation。

### 3.6 Branch-Scoped Apply Locator

aggregate B 族的 target row locator 使用 composite identity：

```text
branch id column == branch_id
AND inner group row id in requested row ids
```

实现上新增 branch-scoped string locator，复用现有 target apply scan 框架，
但 scan projection 必须包含：

- `_file`
- `_pos`
- inner aggregate apply key column
- `__branch_id__`

匹配时同一个 `__row_id__` 在不同 branch 之间不能互相命中。

---

## 4. CREATE / REFRESH 数据流

### 4.1 CREATE MV

CREATE 阶段流程：

```text
SQL
  -> parse/analyze
  -> derive_imv_refresh_contract
  -> BranchUnionAggregate contract
  -> create target schema with hidden __branch_id__ + aggregate hidden state
  -> persist MvSchemaContract { aggregate, branch, target, ... }
```

目标表必须包含 hidden `__branch_id__`。schema contract 中记录：

- `branch.branch_count`；
- `branch.branch_id_column`；
- `branch.inner_apply_key_source = GroupRowId`；
- aggregate state layout；
- target hidden apply key contract。

CREATE 不再因 B 族 contract unsupported 而拒绝，但仍对不兼容 branch layout
fail-fast。

### 4.2 REFRESH MV

REFRESH 阶段流程：

```text
stored SQL
  -> analyze
  -> derive current BranchUnionAggregate contract
  -> validate/rebind persisted contract
  -> AllBasesRequired snapshot policy
  -> first / metadata-only / incremental decision
```

first refresh：

- 使用 branch-aware full materialization；
- 每个 branch 输出对应 `__branch_id__`；
- 同 group key 跨 branch 保持独立 target rows。

incremental refresh：

- IMV rewrite 生成 branch union of aggregate-state merges；
- 每个 branch 的 target-state read 使用 branch scope；
- apply sink 用 `(__branch_id__, __row_id__)` 删除旧 row 并插入新 row；
- empty delta branch 不影响其他 branch；
- 所有 branch 都 empty 时走 metadata-only / no-op 语义。

---

## 5. 错误处理

必须保留 fail-fast 语义：

- 非 `UNION ALL`：拒绝；
- branch kind 混合：拒绝；
- branch output arity 或类型不兼容：拒绝；
- aggregate layout 不兼容：拒绝；
- missing `__branch_id__` target field：拒绝；
- branch count 与 schema contract 不一致：拒绝；
- previous metadata partial：拒绝；
- previous base snapshot 不可达：拒绝；
- schema rebind 不兼容：拒绝；
- target snapshot 不匹配：拒绝；
- 用户显式输出 `__branch_id__`：拒绝。

skip 只允许由统一 refresh decision policy 产生，不允许 B 族单独 silent skip。

---

## 6. 测试计划

### 6.1 Unit Tests

覆盖：

- contract deriver 将 B 族派生为 `BranchUnionAggregate`；
- incompatible B 族 branch layout fail-fast；
- `BranchScope` 能进入 target-state row filter；
- `build_aggregate_state_merge` 在 `branch_scope = Some(i)` 时生成 branch-scoped
  target-state scan；
- `RewriteBranchUnionRule` 将 top-level union of aggregate branches 改写为
  branch-scoped aggregate-state merges；
- pipeline stage order：branch union rewrite 在 generic delta pushdown 前；
- action propagation / validation 接受 rewritten branch union；
- branch-scoped locator 中，同 row id 不同 branch 不互相匹配。

### 6.2 SQL Fixtures

新增或解禁：

- `iceberg_ivm_union_of_aggregates_basic.sql`
  - 两个 branch 有相同 group key；
  - first refresh 后应保留两行；
  - incremental update/delete 后与 full recompute 一致。
- `iceberg_ivm_union_of_aggregates_branch_empty.sql`
  - 某个 branch 初始为空或增量为空；
  - 其他 branch 正常刷新；
  - empty branch 不删除或合并其他 branch rows。

### 6.3 Verification

实现完成后至少运行：

```bash
cargo test --lib branch_union::tests aggregate_rewrite::tests iceberg_target_apply::tests
cargo build --lib
```

SQL 验证使用 `iceberg-ivm` suite 的 B 族 case。若环境允许，运行完整相关
Iceberg IMV suite。

---

## 7. 验收标准

任务完成时：

- B 族 CREATE MV 不再因 unsupported strategy 失败；
- B 族 REFRESH MV 能完成 first refresh、metadata-only refresh 和 incremental
  refresh；
- 同 group key 跨 branch 保持独立 rows；
- B 族 incremental result 与 full recompute 一致；
- A 族 fan-in aggregate 行为不变；
- projection/filter union 行为不变；
- refresh lifecycle 没有新增 bespoke 大分叉；
- `__branch_id__` 只作为 hidden branch identity 使用。

---

## 8. 关键代码位置

- `src/engine/mv/refresh_contract.rs`
  - `RefreshStrategy`；
  - `ApplyKeyContract`；
  - `derive_imv_refresh_contract`。
- `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`
  - aggregate-state merge rewrite；
  - branch-parameterized builder。
- `src/sql/optimizer/rewrite/imv/branch_union.rs`
  - 新增 B 族 rewrite rule。
- `src/sql/optimizer/rewrite/imv/pipeline.rs`
  - branch union rewrite stage。
- `src/sql/optimizer/rewrite/imv/action_propagation.rs`
  - branch union action propagation predicate。
- `src/sql/optimizer/rewrite/imv/action_column.rs`
  - branch union validation predicate。
- `src/sql/catalog.rs`
  - `IcebergMvTargetStateRowFilter`；
  - `BranchScope`。
- `src/sql/optimizer/rewrite/imv/target_state.rs`
  - target-state scan source construction。
- `src/engine/mv/iceberg_target_apply.rs`
  - branch-scoped target row locator。
- `src/engine/mv/iceberg_refresh.rs`
  - CREATE schema/contract；
  - B 族 first/incremental refresh dispatch。
- `src/meta/repository/mv_contract.rs`
  - `BranchUnionContract`；
  - `BRANCH_ID_COLUMN_NAME` self-check。
