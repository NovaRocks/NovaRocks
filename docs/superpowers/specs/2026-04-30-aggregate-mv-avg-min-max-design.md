# Aggregate MV 增量维护：AVG / MIN / MAX 算子覆盖

**Status:** Draft for user review
**Date:** 2026-04-30
**Builds on:**
- `docs/superpowers/specs/2026-04-26-mv-on-iceberg-aggregate-ivm-design.md`
- `docs/superpowers/specs/2026-04-29-iceberg-ivm-phase2-design.md`

**Related roadmap:**
- `/Users/harbor/Documents/Obsidian/NovaRocks IVM Internal Table Roadmap.md` §2.3

---

## 1. 目标

扩展现有 Aggregate MV 增量维护对聚合函数的覆盖：

- **AVG** 输入 = `Int8` / `Int16` / `Int32` / `Int64` / `Decimal128`，输出类型对齐 StarRocks 类型推导（Int → Double，Decimal → Decimal with promoted scale）
- **MIN / MAX** 输入 = `AggScalarValue` 已知类型 + `Float32` / `Float64`，**不接** `Bool` 和复合类型

**不在本阶段范围**：

- `COUNT(DISTINCT)`
- `AVG(FLOAT)` / `AVG(DOUBLE)`（浮点 AVG 累加误差需要 Kahan 求和等正确性补丁，单独立项）
- `MIN(BOOL)` / `MAX(BOOL)`
- `STDDEV` / `VARIANCE` / `PERCENTILE` 等复杂 state 聚合

---

## 2. 范围

延续 `2026-04-26-mv-on-iceberg-aggregate-ivm-design.md` 已确立的 MV 形状约束（单 base 表 / append-only base lineage / 顶层简单聚合表达式 / 手动 REFRESH），增量在此基础上支持：

```sql
SELECT
  <group keys>,
  count(*) | count(col) | sum(col)
  | avg(col)              -- 新
  | min(col) | max(col)   -- 新
FROM iceberg_catalog.namespace.table
[WHERE <deterministic predicate>]
GROUP BY <group keys>
```

**MIN/MAX 在 base 含 DELETE 的 incremental refresh 行为**：整个 MV 退化为 full refresh（详见 §6）。

---

## 3. 关键决策回顾

| 维度 | 决策 |
|---|---|
| 范围 | AVG + MIN + MAX；COUNT(DISTINCT) 单独立项 |
| MIN/MAX DELETE 行为 | 整个 MV fall-back 到 full refresh（per-MV 粒度，不做 per-group） |
| Layout 重构 | `AggregateStateColumn` 增加 `aggregate_index` 和 `state_role` 字段，1 个 aggregate 可对应多个 state 列 |
| AVG 输出类型 | 完全对齐 StarRocks 类型推导（不统一为 DOUBLE） |
| AVG 输入类型 | Int8 / Int16 / Int32 / Int64 / Decimal128 |
| MIN/MAX 输入类型 | AggScalarValue 全类型（Int64 / Utf8 / Date32 / Timestamp / Decimal128）+ Float32 / Float64；拒 Bool 和复合 |
| 兼容性 | NovaRocks 无历史用户，layout 字段直接改 / 加 / 重排，不留 compat shim |

---

## 4. 架构改动

### 4.1 `AggregateFunctionKind` 扩展

```rust
pub(crate) enum AggregateFunctionKind {
    Count,
    Sum,
    Avg,   // 新
    Min,   // 新
    Max,   // 新
}
```

### 4.2 `AggregateStateColumn` 增加字段

```rust
pub(crate) struct AggregateStateColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) sql_type: SqlType,
    pub(crate) nullable: bool,
    pub(crate) visible_source_index: usize,
    pub(crate) aggregate_index: usize,            // 新：指向 shape.aggregates[i]
    pub(crate) function: AggregateFunctionKind,
    pub(crate) state_role: AggregateStateRole,    // 新
    pub(crate) count_star: bool,
}

pub(crate) enum AggregateStateRole {
    /// 单 state 列：state 本身 = 聚合结果（COUNT / SUM / MIN / MAX）
    Single,
    /// AVG 拆出来的 sum 子状态
    AvgSum,
    /// AVG 拆出来的 count 子状态
    AvgCount,
}
```

**为什么 `function` 和 `state_role` 双字段**：

- merge 逻辑按 `(function, state_role)` 派发，直接对应每条 state 列的合并语义
- AvgSum 复用 SUM 的 merge 数学；AvgCount 复用 COUNT 的 merge 数学
- 派生 visible 时按 `aggregate_index` 收集所有相关 state 列

### 4.3 物理列命名

| 函数 | 物理列 |
|---|---|
| COUNT / SUM | `__agg_state_<sanitized_output_name>`（保持现状）|
| MIN / MAX | `__agg_state_<sanitized_output_name>`（同 Single 模式）|
| AVG | `__agg_state_<sanitized_output_name>__sum` + `__agg_state_<sanitized_output_name>__count` |

**碰撞处理**：`sanitize_state_column_name` 仅做小写 / 非 alnum 转 `_`，不主动 escape `__` 序列。理论上若用户给两个 aggregate 起别名为 `x_avg__sum` 和 `x_avg`（前者是 SUM，后者是 AVG），会产生物理列名冲突 `__agg_state_x_avg__sum`。

**解决方式**：`build_aggregate_mv_layout` 在最终物理列列表上做 uniqueness 检查；冲突时返回错误：

```
aggregate MV state column name collision: `__agg_state_<n>` produced by multiple aggregates
```

这是 DDL 阶段拒绝，业务上极少触发。本 spec 不引入新 escape 规则，保持 sanitize 实现稳定。

### 4.4 Visible 派生函数

新增 `materialize_visible_value`：

```rust
fn materialize_visible_value(
    aggregate: &AggregateCallShape,
    state_values_for_aggregate: &[(AggregateStateRole, Option<AggScalarValue>)],
    visible_data_type: &DataType,
) -> Result<Option<AggScalarValue>, String>
```

派发：

- `Count` / `Sum` / `Min` / `Max` → 取 `Single` state 值，直接返回（visible == state）
- `Avg` → 取 `AvgSum` 和 `AvgCount` 两个 state 值，按 visible_data_type 做除法 cast：
  - Int 输入 → Float64 visible：`(sum as f64) / (count as f64)`
  - Decimal128 输入 → Decimal128 visible：使用 `arrow::compute::kernels::numeric::div`，结果 scale = visible_data_type 的 scale
  - count == 0 → 返回 `None`（NULL）
  - sum NULL + count > 0 → 返回 `None`（NULL）—— 整组全 NULL 输入

### 4.5 Layout 构建（`build_aggregate_mv_layout`）

按 `shape.aggregates` 遍历，每个 aggregate 按 function 推不同数量的 state 列：

```text
COUNT / SUM / MIN / MAX → 1 条 Single state 列
AVG                     → 2 条 state 列：AvgSum + AvgCount
```

物理列顺序：`__row_id__`，所有 visible 列，所有 state 列（按 aggregate_index 升序，每个 aggregate 内部 AvgSum 在 AvgCount 之前）。

### 4.6 metadata 持久化

`StoredMaterializedView` **不增加新字段**。`shape.aggregates` 已经能完整反推 layout。

---

## 5. 数据流

### 5.1 CREATE MV（DDL）

入口：`mv_ddl::create_mv` → `mv_shape::classify_aggregate_call`

变化：

- `classify_aggregate_call` 在 `match function_name.as_str()` 中加 `"avg"` / `"min"` / `"max"` 三个 case
- AVG 复用 `classify_sum_input`：必须是 expr，不能是 `*`
- MIN/MAX 引入 `classify_min_max_input`：必须是 expr，不能是 `*`，类型校验在 `validate_state_column_type` 阶段
- `validate_state_column_type` 按 `(function, state_role, data_type)` 完整派发：
  - `(Avg, AvgSum, Int8/16/32/64)` ✓
  - `(Avg, AvgSum, Decimal128)` ✓
  - `(Avg, AvgSum, _)` ✗
  - `(Avg, AvgCount, Int64)` ✓（永远 Int64）
  - `(Min | Max, Single, Bool)` ✗
  - `(Min | Max, Single, Int8/16/32/64 / Float32/Float64 / Decimal128 / Utf8 / Date32 / Timestamp)` ✓

### 5.2 REFRESH — Incremental（仅 INSERT）

```text
plan_changes(prev_snap, cur_snap)
  → IcebergChangeBatch { inserts, deletes }

choose_refresh_strategy(layout, batch)
  → if !batch.deletes.is_empty() && layout_has_min_or_max(layout):
        return Strategy::Full   // ← 新（fall-back）
    else if batch.inserts.is_empty():
        return Strategy::NoOp
    else:
        return Strategy::Incremental

[Incremental 分支]
  materialize_changes(batch.inserts)
    → SELECT k, count(*), sum(v), avg_sum(v), avg_count(v), min(v), max(v) FROM inserts GROUP BY k
    → 物化为带 layout 物理列的 chunks

  merge_aggregate_state_batches(old_rows, delta_chunks)
    → 按 state 列 merge（不变）
    → 派生 visible 列从「按 state 列写一次」改成「按 aggregate 写一次」：
        for each aggregate in shape.aggregates:
            collect (state_role, state_value) tuples for this aggregate
            visible[aggregate.visible_source_index] = materialize_visible_value(aggregate, tuples, visible_data_type)

  写回 MV partition（走现有 upsert 路径）
```

### 5.3 REFRESH — Full（DELETE fall-back 触发）

走现有 Full 路径：重跑整个 MV SELECT，全量 overwrite 当前 partition。新增聚合函数在 SQL 执行层已支持，物化阶段通过 §4.5 layout 自动 work。

### 5.4 NULL 语义（确认 SQL 标准）

| 函数 | 整组全 NULL | 整组混合 NULL |
|---|---|---|
| AVG(x) | visible=NULL，AvgSum=NULL，AvgCount=0 | visible=sum_non_null/count_non_null，AvgCount 不计 NULL 行 |
| MIN(x) / MAX(x) | visible=NULL，state=NULL | visible=min/max(non_null)，state=同 |
| COUNT(*) | count=group 总行数（含 NULL 行）| 同左 |
| COUNT(x) | count=0 | count=non-NULL 数 |

MIN/MAX merge NULL 处理：

- `(Some(a), Some(b))` → `Some(min/max(a, b))`
- `(Some(a), None)` 或 `(None, Some(a))` → `Some(a)`
- `(None, None)` → `None`

跟现有 SUM 的 NULL 合并语义一致（`mv_agg_state.rs::merge_sum_state_value`）。

### 5.5 Float NaN 处理（MIN / MAX）

IEEE 754 下 `NaN cmp x` 返回 false，naive `cmp::min` / `cmp::max` 行为不定。本阶段固定行为：

- 显式调用 `f64::is_nan` / `f32::is_nan` 检查
- 如果两边都 NaN：`Some(NaN)`（保留 NaN）
- 如果一边 NaN：返回非 NaN 边

单测覆盖此 4 个 case 作为契约。如未来发现与 StarRocks 不一致，另立 issue 调整；本 spec 不引入对齐工作。

---

## 6. fall-back 触发

### 6.1 触发位置

`mv_refresh::choose_refresh_strategy` 现有的 `Full | NoOp | Incremental` 三分支判断之前，插入 fall-back 检查：

```rust
fn layout_has_min_or_max(layout: &AggregateMvLayout) -> bool {
    layout.state_columns.iter().any(|col| {
        matches!(col.function, AggregateFunctionKind::Min | AggregateFunctionKind::Max)
    })
}

// 在 Incremental 决策前：
if base_has_deletes_in_range(prev, cur) && layout_has_min_or_max(layout) {
    log::info!(
        target: "mv_refresh",
        "mv={db}.{name} strategy=Full reason=min_max_with_deletes \
         base={base_fqn} snapshot_from={prev} snapshot_to={cur}"
    );
    return Strategy::Full;
}
```

### 6.2 触发位置（更新）

代码盘点后发现 `mv_refresh.rs:152-161` 现有的 Incremental closure 已经在 `plan_changes` 之后立刻有一个 `if !batch.deletes.is_empty()` 报错分支（当前所有 DELETE 都直接 error，PR-3 时会扩展支持）。

fall-back 检查最自然的位置就是这个分支：

```rust
if !batch.deletes.is_empty() {
    if layout_has_min_or_max(&layout) {
        log::info!(...);
        return refresh_mv_full_with_executor(...);  // fall-back to Full
    }
    return Err(format!(...));  // existing error
}
```

不需要重构 `dispatch_mv_refresh_strategy` / `choose_refresh_strategy` 的签名；改动局部化。

### 6.3 OVERWRITE 与 fall-back 的关系

OVERWRITE snapshot 当前在 `plan_changes` 阶段直接报错（`changes.rs:62-74`）。本阶段 **不修复** OVERWRITE 行为（属于 roadmap §2.2 单独立项），AVG / MIN / MAX 在 OVERWRITE 场景下仍报错而非自动 fall-back。

---

## 7. 错误处理

### 7.1 CREATE MV 阶段

| 场景 | 错误信息 |
|---|---|
| `AVG(*)` | `AVG aggregate requires a column expression argument` |
| `AVG(bool_col)` | `AVG state type is unsupported for column \`<n>\`: Boolean` |
| `AVG(string_col)` | `AVG state type is unsupported for column \`<n>\`: Utf8` |
| `AVG(float_col)` / `AVG(double_col)` | `AVG over FLOAT/DOUBLE is not supported in this version; use DECIMAL` |
| `MIN(*)` / `MAX(*)` | `MIN/MAX aggregate requires a column expression argument` |
| `MIN(bool_col)` / `MAX(bool_col)` | `MIN/MAX state type is unsupported for column \`<n>\`: Boolean` |
| `MIN(struct_col)` 等复合类型 | `MIN/MAX state type is unsupported for column \`<n>\`: <DataType>` |

错误信息直接面向用户，不含 stack trace。测试用 `expect_error=` 字符串前缀匹配。

### 7.2 REFRESH 阶段

| 场景 | 行为 |
|---|---|
| AVG 除法溢出 | `AVG visible derivation overflow for column \`<n>\` row id \`<rowid>\`` |
| AVG count_state == 0 但 sum_state != NULL | state corruption error |
| MIN/MAX state 进入 `negate_aggregate_state_chunks` | panic：`internal: MIN/MAX state should not enter negate path` |
| Decimal128 cast 失败 | `AVG decimal cast failed for column \`<n>\` from <from_scale> to <to_scale>` |
| Full fall-back 时执行失败 | 现有 Full 路径错误传递不变 |

### 7.3 Fall-back INFO 日志

```
[mv_refresh] mv=<db>.<name> strategy=Full reason=min_max_with_deletes
              base=<base_fqn> snapshot_from=<X> snapshot_to=<Y>
```

未来 Roadmap §4.4（可观测性）扩展时，这条日志接到 metrics。

### 7.4 边界场景

- **AVG 整组全 NULL 输入**：AvgCount=0，visible=NULL；merge 路径不做除法
- **MIN/MAX 整组全 NULL 输入**：state=NULL，visible=NULL；group 不被 drop（INSERT-only 路径下也无 retract）

---

## 8. 测试

### 8.1 单元测试

放在 `mv_agg_state.rs` 文件末（跟现有单测同位置）+ `mv_shape.rs`。

**Layout / DDL**：

- `classify_aggregate_call_avg_*`：`AVG(x)` / `AVG(*)` 拒绝 / `AVG(distinct x)` 拒绝
- `classify_aggregate_call_min_max_*`：`MIN(x)` / `MAX(x)` / `MIN(bool_col)` 拒绝 / `MIN(*)` 拒绝
- `build_aggregate_mv_layout_avg`：AVG 推 2 条 state 列，命名为 `__agg_state_<n>__sum` / `__agg_state_<n>__count`
- `build_aggregate_mv_layout_min_max`：MIN/MAX 推 1 条 Single state 列
- `build_aggregate_mv_layout_mixed`：COUNT + SUM + AVG + MIN + MAX 同一 MV，验证物理列顺序、aggregate_index 正确
- `validate_state_column_type_*`：拒绝场景全覆盖

**Merge / state**：

- `merge_state_value_avg_sum`：Int8/16/32/64 + Decimal128 各类型，含 NULL 合并、溢出
- `merge_state_value_avg_count`：Int64 累加 + 溢出
- `merge_state_value_min`：各类型 cmp::min 正确，NULL 处理 4 个 case，Float NaN 处理
- `merge_state_value_max`：同上
- `materialize_visible_value_avg`：Int → Double 除法、Decimal → Decimal 除法（含 scale 对齐）、count==0 → NULL
- `negate_aggregate_state_chunks_avg`：AvgSum / AvgCount 都翻号
- `negate_aggregate_state_chunks_min_max_panics`：invariant
- `merge_aggregate_state_batches_mixed`：end-to-end，old + delta(INSERT batch) → 正确 merged

**Refresh strategy**：

- `choose_refresh_strategy_min_max_no_deletes` → Incremental
- `choose_refresh_strategy_min_max_with_deletes` → Full（fall-back）
- `choose_refresh_strategy_avg_with_deletes` → Incremental（AVG 不触发 fall-back）
- `choose_refresh_strategy_count_sum_only_with_deletes` → Incremental（不变）

### 8.2 SQL 集成测试

新增文件：`sql-tests/write-path/sql/managed_lake_mv_aggregate_avg_min_max.sql`

参考现有 `managed_lake_mv_aggregate_ivm.sql` 模式，每个查询步骤用 `-- query N` 标号。

**覆盖场景**：

1. **基础**：base 表（k INT，v BIGINT，d DECIMAL(20,4)，s STRING，t DATETIME），MV `SELECT k, COUNT(*), SUM(v), AVG(v), AVG(d), MIN(v), MAX(v), MIN(s), MAX(t) FROM base GROUP BY k`，初始 INSERT 多行包含 NULL，REFRESH（first refresh = full），SELECT 验证
2. **增量 INSERT**：再 INSERT（已有 group + 新 group），REFRESH（incremental），SELECT 验证 MIN/MAX 增量、AVG 数学正确
3. **整组全 NULL**：INSERT 一组 v 全 NULL，REFRESH，验证 AVG/MIN/MAX = NULL，COUNT(*) > 0
4. **DDL 拒绝**：`AVG(*)` / `AVG(bool)` / `AVG(float)` / `MIN(*)` / `MIN(bool)` 都用 `@expect_error` 验证
5. **类型覆盖**：单独 MV 用 MIN/MAX 跑 STRING / DATE / DATETIME / DECIMAL，验证比较语义

**不在 SQL 集成测试**：DELETE → fall-back（靠 §8.1 的 Rust strategy 单测覆盖）

### 8.3 验收

- 所有新 / 旧单测通过：`cargo test`
- `cargo clippy --all-targets -- -D warnings` 零 warning
- `cargo fmt --check` 通过
- 新 SQL 集成测试通过：`sql-tests --suite write-path --only managed_lake_mv_aggregate_avg_min_max`
- 已有 SQL 测试不回归：`sql-tests --suite write-path`
- 手工跑过一遍 fall-back 场景，确认日志输出正确

---

## 9. 落地切片

单 PR，内部 5 个逻辑 commit：

```
commit 1: refactor: introduce AggregateStateRole and aggregate_index
  - AggregateFunctionKind 暂不加新值
  - AggregateStateColumn 加 state_role + aggregate_index 字段
  - 现有 SUM/COUNT 走 state_role=Single, aggregate_index 自然推导
  - visible 派生从「按 state 列」改成「按 aggregate」：对 Single = 直接复制
  - 所有现有测试保持通过

commit 2: feat: support AVG aggregate in incremental MV
  - AggregateFunctionKind::Avg
  - classify_aggregate_call("avg") + DDL 拒绝
  - layout 推 AvgSum + AvgCount 两条 state 列
  - merge_state_value 加 Avg 分支
  - materialize_visible_value 加 Avg 派生（含 Decimal scale 对齐）
  - validate_state_column_type AVG 类型范围
  - negate_aggregate_state_chunks AvgSum/AvgCount 翻号
  - 单测

commit 3: feat: support MIN/MAX aggregate in incremental MV (INSERT-only)
  - AggregateFunctionKind::Min, Max
  - classify_aggregate_call("min"/"max") + DDL 拒绝（拒 *、Bool）
  - layout 推 Single state 列
  - merge_state_value Min/Max 分支（NULL 4 case + Float NaN）
  - validate_state_column_type Min/Max 类型范围（含 Float64/Float32）
  - negate_aggregate_state_chunks Min/Max → panic invariant
  - 单测

commit 4: feat: full-refresh fall-back when MV uses MIN/MAX and base has DELETE
  - choose_refresh_strategy 接 plan_changes 输出
  - 检测 layout_has_min_or_max + batch.deletes 非空 → Strategy::Full
  - INFO 日志 reason=min_max_with_deletes
  - 单测覆盖 4 个 strategy case

commit 5: test: add SQL integration test for AVG/MIN/MAX aggregate IVM
  - sql-tests/write-path/sql/managed_lake_mv_aggregate_avg_min_max.sql
  - 覆盖场景 1/2/3/4/5（DELETE fall-back 走 Rust 单测）
```

每个 commit 单独可编译可测，rebase 友好。

---

## 10. 文件改动清单

| 文件 | 改动 |
|---|---|
| `src/connector/starrocks/managed/mv_shape.rs` | `AggregateFunctionKind` 加 3 值；`classify_aggregate_call` 加 3 case + 错误信息；DDL 单测 |
| `src/connector/starrocks/managed/mv_agg_state.rs` | `AggregateStateRole` enum；`AggregateStateColumn` 加字段；`build_aggregate_mv_layout` AVG 拆 2 列；`merge_state_value` / `negate_aggregate_state_chunks` / `validate_loaded_physical_row` / `validate_state_column_type` 重排；新增 `materialize_visible_value`；大量单测 |
| `src/connector/starrocks/managed/mv_ddl.rs` | `build_mv_storage_layout` 物理列名生成（含 `__sum` / `__count` 后缀）；hidden column 推导微调 |
| `src/connector/starrocks/managed/mv_refresh.rs` | `choose_refresh_strategy` 加 fall-back 分支；INFO 日志；调用顺序微调以提前拿到 plan_changes 输出 |
| `src/connector/starrocks/managed/txn.rs` | upsert 写路径如按 state 列遍历，按 aggregate 重组 visible 派生 |
| `sql-tests/write-path/sql/managed_lake_mv_aggregate_avg_min_max.sql` | 新增 |

---

## 11. 风险与坑点

### 11.1 编译风险（中）

`AggregateStateColumn` 加字段会让所有构造点编译错。`grep -n "AggregateStateColumn {" src/` 列出全部点逐一修。

### 11.2 Decimal scale 推导（中）

- `arrow::compute::kernels::numeric::div` 对 Decimal128 的 scale 处理需要测试覆盖
- 实施步骤：commit 2 实现前先编一个最小 SELECT AVG(decimal_col) 跑一遍，从执行层 OutputColumn 拿到 visible_data_type 的实际 scale（记入 commit 2 PR description）
- 风险：如果 SQL analyzer 推的 visible scale 跟我们除法的输出 scale 不一致，需要显式 cast
- 缓解：`materialize_visible_value` 在 Decimal 分支强制按 visible_data_type 的 scale 做最终 cast，不假设 div kernel 的输出 scale 已经对

### 11.3 Float NaN（低）

- IEEE 754 cmp 的 NaN 行为
- 缓解：MIN/MAX merge 时显式 `is_nan()` 处理；单测明确行为
- 跟 StarRocks 完全对齐工作未来跟进

### 11.4 fall-back 调用顺序（低）

代码盘点后确认现有 `mv_refresh.rs:155` 的 DELETE error 分支天然就是 fall-back 注入点；不需要重构 `choose_refresh_strategy` 签名。Risk 从「中」降到「低」。

---

## 变更记录

- 2026-04-30 — 初版。基于 commit `0bd945e`（PR #67 合入后）的代码盘点。
