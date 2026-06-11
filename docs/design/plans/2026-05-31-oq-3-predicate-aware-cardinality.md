# OQ-3 Predicate-Aware Cardinality Propagation 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让有真实统计的 Iceberg v3 表在优化器内部与 `EXPLAIN COSTS` 中显示反映谓词 selectivity 的 post-filter 基数，并把 per-column 统计完整传播到物理 plan，作为 OQ-4 的前置。

**Architecture:** selectivity 函数（`estimate_selectivity`）与逐算子基数估算（`derive_scan` / `derive_join` / aggregate NDV 连乘）**已存在且接好**。真正的根因是 `LogicalProperties` 只携带 `row_count`、不带列统计，导致 derivation 期的 `child_statistics`（`stats.rs:651`）与 extraction 期的 `group_statistics`（`extract.rs:123`）都返回**空列统计** → 上层算子的 selectivity 退默认、`EXPLAIN COSTS` 无 per-column。核心修复是让 `LogicalProperties` 携带列统计，一处修复同时治好两处。其余为：无统计 fallback 应用 selectivity、Between 覆盖、EXPLAIN COSTS per-column 显示、Iceberg golden。

**Tech Stack:** Rust（`src/sql/optimizer/**`、`src/sql/explain.rs`）；unit test 用 `#[cfg(test)] mod tests`（`make_table_stats` / `scan_plan` / `Memo` harness）；SQL golden 用 `tests/sql-test-runner`（iceberg-rest 套件 + `-- @explain_contains`）。Debug build（CLAUDE.md §8.2）。

**语言**：本计划散文用中文，代码标识符/路径/命令用英文（CLAUDE.md §8.1）。

**关键实现决定（偏离 spec 处，已记录理由）：**
- spec §4.2.1 建议 `LogicalProperties.column_statistics` 按 `ColumnId` 键。**本计划改为按 lowercased name 键**，与现有 `Statistics.column_statistics`、`derive_scan`（按列名）、`LogicalProject`（按 `output_name`）整条 derivation 管线一致，消除 spec §8 标注的「name↔ColumnId 映射」风险。ColumnId 键作为未来 hardening，不在本任务。
- 传播深度 = scan+filter + 基础 join/agg：join（`derive_join`）/agg（NDV 连乘）基数**已实现**，本计划只需让它们吃到真实列统计（Task 1 解锁），不重写。

---

## 文件结构（创建 / 修改）

- Modify `src/sql/optimizer/memo.rs` — `LogicalProperties` 增 `column_statistics: HashMap<String, ColumnStatistic>` 字段 + `new()` 初始化。
- Modify `src/sql/optimizer/logical_props.rs` — `derive_for_group` / `derive_for_expr` 增列统计参数并写入 props。
- Modify `src/sql/optimizer/stats.rs` — `derive_group_statistics` 传列统计；`child_statistics` 返回真实列统计；无统计 scan fallback（`derive_scan` else + `PhysicalScan` else）应用 selectivity；`estimate_selectivity` 增 `Between`；更新/新增 unit tests。
- Modify `src/sql/optimizer/extract.rs` — `group_statistics` 返回真实列统计。
- Modify `src/sql/explain.rs` — 新增 `format_column_stats_costs` 并在 `format_physical_node` 的 Costs 分支输出 per-column 统计。
- Create `sql-tests/iceberg-rest/sql/iceberg_rest_cardinality.sql` + `sql-tests/iceberg-rest/result/iceberg_rest_cardinality.result` — post-filter 基数 golden。

每个 Task 独立可编译、可测、可提交。

---

## 执行前基线（重要 — 执行的 subagent 必读）

开始 Task 1 前先抓一次测试基线，**只把「新增」失败当回归**：

```bash
cargo test --lib 2>&1 | tail -5
```

已知 pre-existing lib-test 失败（来自 main commit `45f6e676`「correctness regressions documented」，**与本任务无关**）：
- `connector::starrocks::table::mv_shape::tests::*`（2 个）
- `exec::pipeline::builder::tests::*`（3 个）

基线 = **3320 passed / 5 failed**（仅上述 5 个）。本计划各 Task 的目标单测都在 `sql::optimizer` / `explain` 模块下（不在这 5 个失败模块内），所以 `cargo test --lib sql::optimizer` 与 `cargo test --lib explain` 应全绿；全量 `cargo test --lib` 应保持「仅这 5 个 pre-existing 失败、无新增」。

套件已由独立工作迁至 **Iceberg v3**（filter/limit/project/sort/join/cte/set-op/table-function/runtime-filter；`low-cardinality` 故意留在 native，原因见迁移 spec）。迁移用 hadoop catalog + `-- @catalog=` 指令 + 每套件 `init.sql`/`cleanup.sql`，建表带 `TBLPROPERTIES("format-version"="3")`。详见 `docs/design/specs/2026-05-31-stable-sql-suites-iceberg-v3-migration-design.md`。Task 5 Step 6 的标杆复核即在这些 Iceberg 套件上做。

---

## Task 1: 让 LogicalProperties 携带列统计（核心修复）

**Files:**
- Modify: `src/sql/optimizer/memo.rs:106-120`（`LogicalProperties` + `new`）
- Modify: `src/sql/optimizer/logical_props.rs:10-32`（`derive_for_group` / `derive_for_expr`）
- Modify: `src/sql/optimizer/stats.rs:634-639`（`derive_group_statistics`）、`stats.rs:651-668`（`child_statistics`）
- Modify: `src/sql/optimizer/extract.rs:123-135`（`group_statistics`）
- Test: `src/sql/optimizer/stats.rs`（`mod tests`：更新 `filter_group_stats`、`aggregate_group_stats`）

- [ ] **Step 1: 更新两个现有测试到「修复后」的期望值（先失败）**

把 `stats.rs` 中 `filter_group_stats` 的断言段（当前 `stats.rs:1577-1582`）替换为：

```rust
        // Filter group (1): with column stats now flowing through
        // child_statistics, `a = 42` uses real NDV(a)=100 -> selectivity
        // 1/100 = 0.01 -> 10000 * 0.01 = 100 rows.
        let filter_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!((filter_props.row_count - 100.0).abs() < 1.0);
```

把 `aggregate_group_stats` 的断言段（当前 `stats.rs:1642-1648`）替换为：

```rust
        // Agg group: real NDV(status)=5 now flows through child_statistics,
        // so output = min(5, 100000*0.75) = 5.
        let agg_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!((agg_props.row_count - 5.0).abs() < 1.0);
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `cargo test --lib filter_group_stats aggregate_group_stats`
Expected: FAIL — `filter_group_stats` 实得 2500（0.25 默认 selectivity），`aggregate_group_stats` 实得 ~10（默认 NDV）。

- [ ] **Step 3: `LogicalProperties` 增列统计字段**

在 `src/sql/optimizer/memo.rs` 顶部确保 `use std::collections::HashMap;` 与 `use super::statistics::ColumnStatistic;` 存在（缺则补）。把 `LogicalProperties`（`memo.rs:106`）改为：

```rust
pub(crate) struct LogicalProperties {
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) row_count: f64,
    pub(crate) column_statistics: HashMap<String, ColumnStatistic>,
    pub(crate) equivalence_classes: EquivalenceClasses,
    pub(crate) unique_columns: Vec<ColumnIdSet>,
}
```

把 `LogicalProperties::new`（`memo.rs:114`）改为初始化空 map：

```rust
    pub(crate) fn new(output_columns: Vec<OutputColumn>, row_count: f64) -> Self {
        Self {
            output_columns,
            row_count,
            column_statistics: HashMap::new(),
            equivalence_classes: EquivalenceClasses::default(),
            unique_columns: Vec::new(),
        }
    }
```

- [ ] **Step 4: `derive_for_group` / `derive_for_expr` 接收并写入列统计**

在 `src/sql/optimizer/logical_props.rs` 顶部补 `use std::collections::HashMap;` 与 `use super::statistics::ColumnStatistic;`。把 `derive_for_group`（`logical_props.rs:10`）签名与转发改为：

```rust
pub(crate) fn derive_for_group(
    memo: &Memo,
    group_idx: GroupId,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
    column_statistics: HashMap<String, ColumnStatistic>,
) -> LogicalProperties {
    let group = &memo.groups[group_idx];
    let expr = group.logical_exprs.first().or(group.physical_exprs.first());
    let Some(expr) = expr else {
        let mut props = LogicalProperties::new(output_columns, row_count);
        props.column_statistics = column_statistics;
        return props;
    };
    derive_for_expr(expr, memo, output_columns, row_count, column_statistics)
}
```

把 `derive_for_expr`（`logical_props.rs:24`）签名与开头改为：

```rust
pub(crate) fn derive_for_expr(
    expr: &MExpr,
    memo: &Memo,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
    column_statistics: HashMap<String, ColumnStatistic>,
) -> LogicalProperties {
    let output_ids = output_id_set(&output_columns);
    let mut props = LogicalProperties::new(output_columns, row_count);
    props.column_statistics = column_statistics;
```

（函数其余 match 体不变。）

- [ ] **Step 5: `derive_group_statistics` 把列统计传下去**

把 `src/sql/optimizer/stats.rs:634-639` 的 `derive_for_group` 调用改为（注意 `stats.output_row_count` 是 `f64`，Copy，可在 move `stats.column_statistics` 之前读取）：

```rust
        memo.groups[group_idx].logical_props = Some(super::logical_props::derive_for_group(
            memo,
            group_idx,
            output_columns,
            stats.output_row_count,
            stats.column_statistics,
        ));
```

- [ ] **Step 6: `child_statistics` 返回真实列统计**

把 `src/sql/optimizer/stats.rs:654-661`（`if let Some(ref props)` 分支）改为：

```rust
    if let Some(ref props) = group.logical_props {
        // Column statistics now travel on LogicalProperties, so propagate
        // them so parent operators estimate real selectivity / join NDV.
        Statistics {
            output_row_count: props.row_count,
            column_statistics: props.column_statistics.clone(),
        }
    } else {
```

- [ ] **Step 7: `group_statistics`（extraction）返回真实列统计**

把 `src/sql/optimizer/extract.rs:124-128`（`if let Some(ref lp)` 分支）改为：

```rust
    if let Some(ref lp) = group.logical_props {
        Statistics {
            output_row_count: lp.row_count,
            column_statistics: lp.column_statistics.clone(),
        }
    } else {
```

- [ ] **Step 8: 更新其它 `derive_for_group` / `derive_for_expr` 调用点**

Run: `grep -rn "derive_for_group\|derive_for_expr" src/`
对每个调用点补上第 5 个参数。若调用方手头没有列统计，传 `std::collections::HashMap::new()`。预期只有 `stats.rs:634`（已在 Step 5 改）与可能的 IMV/test 调用点。

- [ ] **Step 9: 运行目标测试 + 全 lib 测试，确认通过**

Run: `cargo test --lib filter_group_stats aggregate_group_stats`
Expected: PASS（filter→100，agg→5）。
Run: `cargo test --lib sql::optimizer`
Expected: PASS（无回归；`scan_group_stats` / `join_group_stats` / `limit_group_stats` 等仍绿）。

- [ ] **Step 10: 提交**

```bash
git add src/sql/optimizer/memo.rs src/sql/optimizer/logical_props.rs src/sql/optimizer/stats.rs src/sql/optimizer/extract.rs
git commit -m "feat(optimizer): carry column statistics through LogicalProperties

LogicalProperties only carried row_count, so child_statistics (derivation)
and group_statistics (extraction) both returned empty column stats. This
starved estimate_selectivity / derive_join of real NDV/min-max on every
parent-child boundary. Thread column_statistics through LogicalProperties so
filters, joins and aggregates estimate real cardinality and EXPLAIN can show
per-column stats. Keyed by lowercased column name for consistency with the
existing name-keyed derivation pipeline.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: 无统计 scan fallback 应用 selectivity

**Files:**
- Modify: `src/sql/optimizer/stats.rs:273-284`（`PhysicalScan` else 分支）、`stats.rs:728-741`（`derive_scan` else 分支）
- Test: `src/sql/optimizer/stats.rs`（`mod tests`：新增 `fallback_scan_applies_predicate_selectivity` + helper `scan_plan_with_predicates`）

- [ ] **Step 1: 新增 helper + 失败测试**

在 `stats.rs` 的 `mod tests` 内、`scan_plan` 之后新增一个带谓词的 scan 构造器。它复制 `scan_plan`（`stats.rs:1472-1503`）但把 `predicates` 设为入参：

```rust
    fn scan_plan_with_predicates(
        name: &str,
        cols: &[&str],
        predicates: Vec<TypedExpr>,
    ) -> LogicalPlan {
        let LogicalPlan::Scan(mut node) = scan_plan(name, cols) else {
            unreachable!("scan_plan always returns a Scan");
        };
        node.predicates = predicates;
        LogicalPlan::Scan(node)
    }

    #[test]
    fn fallback_scan_applies_predicate_selectivity() {
        // No table stats registered -> derive_scan takes the heuristic
        // fallback. With the fix, the predicate still reduces the row count.
        let table_stats: HashMap<String, TableStatistics> = HashMap::new();
        let pred = eq_expr(col_ref("a"), int_lit(42));
        let plan = scan_plan_with_predicates("unknown_tbl", &["a"], vec![pred]);

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // default_rows("unknown_tbl") = 100000; unknown-column eq selectivity
        // = PREDICATE_UNKNOWN_FILTER (0.25) -> 100000 * 0.25 = 25000.
        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 25_000.0).abs() < 1.0);
    }
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `cargo test --lib fallback_scan_applies_predicate_selectivity`
Expected: FAIL — 实得 100000（fallback 当前不乘 selectivity）。

- [ ] **Step 3: 在 `derive_scan` 的 else 分支应用 selectivity**

把 `src/sql/optimizer/stats.rs:728-741`（`derive_scan` 的 `} else {` 块）改为：

```rust
    } else {
        // No table stats available: use heuristic defaults based on table name.
        let default_rows = estimate_default_row_count(&scan.table.name);
        let column_statistics: HashMap<String, ColumnStatistic> = scan
            .columns
            .iter()
            .map(|c| (c.name.to_lowercase(), ColumnStatistic::unknown()))
            .collect();
        let mut selectivity = 1.0;
        for pred in &scan.predicates {
            selectivity *= estimate_selectivity(pred, &column_statistics);
        }
        Statistics {
            output_row_count: (default_rows * selectivity).max(1.0),
            column_statistics,
        }
    }
```

- [ ] **Step 4: 在 `PhysicalScan` 的 else 分支做同样修改**

把 `src/sql/optimizer/stats.rs:273-284`（`PhysicalScan` 的 `} else {` 块）改为同样形式：

```rust
            } else {
                let default_rows = estimate_default_row_count(&scan.table.name);
                let column_statistics: HashMap<String, ColumnStatistic> = scan
                    .columns
                    .iter()
                    .map(|c| (c.name.to_lowercase(), ColumnStatistic::unknown()))
                    .collect();
                let mut selectivity = 1.0;
                for pred in &scan.predicates {
                    selectivity *= estimate_selectivity(pred, &column_statistics);
                }
                Statistics {
                    output_row_count: (default_rows * selectivity).max(1.0),
                    column_statistics,
                }
            }
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `cargo test --lib fallback_scan_applies_predicate_selectivity`
Expected: PASS（25000）。
Run: `cargo test --lib sql::optimizer`
Expected: PASS（无回归）。

- [ ] **Step 6: 提交**

```bash
git add src/sql/optimizer/stats.rs
git commit -m "feat(optimizer): apply predicate selectivity in no-stats scan fallback

The heuristic fallback (no table stats) returned the default row count
without applying any predicate selectivity, so predicates were ignored
entirely on un-analyzed tables. Multiply by estimate_selectivity so a
filtered scan always reduces the estimate.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: selectivity 覆盖 — `BETWEEN` 走 range

**Files:**
- Modify: `src/sql/optimizer/stats.rs:1247-1250`（`estimate_selectivity` 的 `Between` 分支）
- Test: `src/sql/optimizer/stats.rs`（`mod tests`：新增 `between_uses_range_selectivity`）

- [ ] **Step 1: 确认 `ExprKind::Between` 字段名**

Run: `grep -n "Between" src/sql/analysis*.rs src/sql/analysis/*.rs 2>/dev/null | head`
确认变体字段（预期 `Between { expr, low, high, negated }`）。下面代码按此字段名编写；若实际不同，按实际调整 `low` / `high` 名称。

- [ ] **Step 2: 新增失败测试（直接测 `estimate_selectivity`）**

在 `stats.rs` 的 `mod tests` 内新增。该测试直接构造带 finite min/max 的列统计，绕开 `make_table_stats`：

```rust
    fn col_stat(min: f64, max: f64, ndv: f64) -> ColumnStatistic {
        ColumnStatistic {
            min_value: min,
            max_value: max,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            distinct_values_count: ndv,
        }
    }

    fn between_expr(expr: TypedExpr, low: TypedExpr, high: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::Between {
                expr: Box::new(expr),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            },
        }
    }

    #[test]
    fn between_uses_range_selectivity() {
        let mut cs = HashMap::new();
        cs.insert("a".to_string(), col_stat(0.0, 100.0, 100.0));
        // a BETWEEN 0 AND 50 over [0,100] -> roughly half the range.
        let pred = between_expr(col_ref("a"), int_lit(0), int_lit(50));
        let sel = estimate_selectivity(&pred, &cs);
        assert!(sel > 0.3 && sel < 0.7, "between selectivity was {sel}");
    }
```

- [ ] **Step 3: 运行测试，确认失败**

Run: `cargo test --lib between_uses_range_selectivity`
Expected: FAIL — 当前 `Between` 返回 `PREDICATE_UNKNOWN_FILTER`（0.25），不在 (0.3, 0.7)。

- [ ] **Step 4: 实现 `Between` 拆成两个 range 估算**

把 `src/sql/optimizer/stats.rs:1247-1250` 的 `Between` 分支改为（复用已有的 `estimate_range_selectivity`）：

```rust
        ExprKind::Between {
            negated,
            expr,
            low,
            high,
        } => {
            // a BETWEEN low AND high  ==  a >= low AND a <= high
            let ge = estimate_range_selectivity(expr, low, BinOp::Ge, column_stats);
            let le = estimate_range_selectivity(expr, high, BinOp::Le, column_stats);
            let sel = ge * le;
            if *negated { 1.0 - sel } else { sel }
        }
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `cargo test --lib between_uses_range_selectivity`
Expected: PASS（sel ≈ 0.99 × 0.51 ≈ 0.5）。
Run: `cargo test --lib sql::optimizer`
Expected: PASS（无回归）。

- [ ] **Step 6: 提交**

```bash
git add src/sql/optimizer/stats.rs
git commit -m "feat(optimizer): estimate BETWEEN via range selectivity

BETWEEN fell through to the unknown-predicate default (0.25). Decompose it
into >= low AND <= high and reuse estimate_range_selectivity so it benefits
from real column min/max.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: EXPLAIN COSTS 显示 per-column 统计

**Files:**
- Modify: `src/sql/explain.rs`（新增 `format_column_stats_costs`；在 `format_physical_node` 的 Costs 分支调用）
- Test: `src/sql/explain.rs`（`mod tests`：新增 `costs_column_stats_formatting`）

- [ ] **Step 1: 新增失败测试（测纯格式化 helper）**

在 `src/sql/explain.rs` 末尾的 `#[cfg(test)] mod tests`（若无则新建）内新增：

```rust
#[cfg(test)]
mod oq3_tests {
    use super::*;
    use crate::sql::optimizer::statistics::{ColumnStatistic, Statistics};
    use std::collections::HashMap;

    #[test]
    fn costs_column_stats_formatting() {
        let mut cs = HashMap::new();
        cs.insert(
            "k1".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 1000.0,
            },
        );
        let stats = Statistics {
            output_row_count: 10.0,
            column_statistics: cs,
        };
        let s = format_column_stats_costs(&stats);
        assert!(s.contains("k1"), "missing column name: {s}");
        assert!(s.contains("ndv=1000"), "missing ndv: {s}");
        assert!(s.contains("min=0"), "missing min: {s}");
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `cargo test --lib costs_column_stats_formatting`
Expected: FAIL — `format_column_stats_costs` 未定义（编译错）。

- [ ] **Step 3: 实现 `format_column_stats_costs`**

在 `src/sql/explain.rs` 的 `format_stats_trailer`（`explain.rs:18`）之后新增。列按名字排序保证 golden 稳定：

```rust
/// Costs-only per-column statistics block. Kept separate from
/// `format_stats_trailer` so Verbose/Analyze output (and existing golden
/// files) stay unchanged — only `EXPLAIN COSTS` shows column stats.
/// Unknown-stat columns (ColumnStatistic::unknown) render as min=-inf max=+inf ndv=1 null_frac=0.
pub(crate) fn format_column_stats_costs(
    stats: &crate::sql::optimizer::statistics::Statistics,
) -> String {
    if stats.column_statistics.is_empty() {
        return String::new();
    }
    let mut names: Vec<&String> = stats.column_statistics.keys().collect();
    names.sort();
    let parts: Vec<String> = names
        .into_iter()
        .map(|name| {
            let c = &stats.column_statistics[name];
            let ndv = if c.distinct_values_count.is_finite() {
                (c.distinct_values_count.round() as i64).to_string()
            } else {
                "?".to_string()
            };
            format!(
                "{name}[min={} max={} ndv={ndv} null_frac={}]",
                fmt_f64(c.min_value),
                fmt_f64(c.max_value),
                fmt_f64(c.nulls_fraction),
            )
        })
        .collect();
    format!("colstats={{{}}}", parts.join(", "))
}

fn fmt_f64(v: f64) -> String {
    if v.is_nan() {
        "?".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+inf".to_string() } else { "-inf".to_string() }
    } else if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v:.4}")
    }
}
```

- [ ] **Step 4: 在 `format_physical_node` 的 Costs 分支输出该块**

在 `src/sql/explain.rs` 的 `format_physical_node`（`explain.rs:296-308`），把 `costs_suffix` 的构造扩展为同时追加 colstats（仅 Costs）：

```rust
    let costs_suffix = if matches!(level, ExplainLevel::Costs) {
        let colstats = format_column_stats_costs(&node.stats);
        if colstats.is_empty() {
            format!(" (rows={:.0})", node.stats.output_row_count)
        } else {
            format!(" (rows={:.0}) {colstats}", node.stats.output_row_count)
        }
    } else {
        String::new()
    };
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `cargo test --lib costs_column_stats_formatting`
Expected: PASS。
Run: `cargo test --lib explain`
Expected: PASS（Verbose/Normal 输出不变 —— colstats 只进 Costs）。

- [ ] **Step 6: 提交**

```bash
git add src/sql/explain.rs
git commit -m "feat(explain): show per-column stats in EXPLAIN COSTS

Add a Costs-only colstats={...} block (min/max/ndv/nulls per output column),
sorted by name for stable goldens. Verbose/Analyze trailer stays rows-only so
existing golden files are unaffected.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Iceberg 基础验证 + iceberg-rest cardinality golden + 标杆复核

> 本 Task 需要运行 standalone-server 与 iceberg-rest Docker 环境。务必用 worktree 生成环境（见 CLAUDE.md §7.3 / §8.4），不要硬编码端口。

- [ ] **Step 1: 起环境与 server**

```bash
cd /Users/harbor/project/NovaRocks/.claude/worktrees/serene-mendeleev-9b5915
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build
LOG=/tmp/oq3-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { echo "server died"; tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout"; kill -9 "$SRV_PID"; exit 1; }
```

- [ ] **Step 2: 手工验证 Iceberg 列统计真到位（基础前提）**

对一个 iceberg-rest 表插已知数据，用 `EXPLAIN COSTS` 确认 scan 节点出现非空 `colstats=` 且 `k1<...` 后 `stats={rows=N}` 已 < 全表行数。例（端口用 `$NOVA_ENV_MYSQL_PORT`）：

```bash
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -uroot <<'SQL'
CREATE DATABASE IF NOT EXISTS iceberg_rest_card_probe;
CREATE TABLE iceberg_rest_card_probe.t (k1 INT, v INT);
INSERT INTO iceberg_rest_card_probe.t
  SELECT number, number FROM TABLE(generate_series(1, 1000));
EXPLAIN COSTS SELECT k1 FROM iceberg_rest_card_probe.t WHERE k1 < 100;
SQL
```

预期：SCAN 行带 `colstats={k1[min=1 max=1000 ndv=... null_frac=0], ...}`，且 `stats={rows=~100}`（而非 1000）。
- 若 `colstats` 为空或 min/max 为 `+inf/-inf`：说明 Iceberg manifest 的 min/max 未进 `ColumnStatistic`。此时先修 `build_table_statistics_with_columns`（`src/sql/optimizer/statistics.rs:123`）/`collect_scan_stats`（`src/engine/mod.rs:2952`）让 INT 列 min/max/NDV 真正填充，再继续。把该修复单独提交。
- 若 `stats={rows=1000}` 不变但 `colstats` 有真实值：检查谓词是否下推进 scan（`scan.predicates`）；未下推则由 Task 1 修好的 `child_statistics` 在 Filter 节点上体现，`EXPLAIN COSTS` 看 FILTER 行的 rows。

- [ ] **Step 3: 写 golden 用例（record 模式生成）**

创建 `sql-tests/iceberg-rest/sql/iceberg_rest_cardinality.sql`，沿用套件命名（`iceberg_rest_${suite_uuid0}` / `${uuid0}`）与 `-- @explain_contains`（注意：`@explain_contains` 跑 `EXPLAIN VERBOSE`，断言 `stats={rows=N}` 的 VERBOSE trailer）：

```sql
-- @tags=iceberg-rest,optimizer,cardinality
-- Test Objective:
-- Lock post-filter cardinality on Iceberg scans: predicate selectivity must
-- reduce the scan/filter row-count estimate (OQ-3). Uses @explain_contains
-- (EXPLAIN VERBOSE) to assert the stats={rows=N} trailer.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS iceberg_rest_${suite_uuid0}.card_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.card_${uuid0}.t (k1 INT, v INT);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.card_${uuid0}.t
  SELECT number, number FROM TABLE(generate_series(1, 1000));

-- query 4
-- @skip_result_check=true
-- @explain_contains=stats={rows=
SELECT k1 FROM iceberg_rest_${suite_uuid0}.card_${uuid0}.t WHERE k1 < 100;

-- query 5
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.card_${uuid0};
```

> 注：上面 query 4 先只断言 trailer 存在。生成 result 后（Step 4），把 `@explain_contains` 收紧成实际 post-filter 值，例如 `-- @explain_contains=stats={rows=100}`（取 record 跑出的真实数字，确认 < 1000 且与 StarRocks 同数量级）。

用 record 模式生成 `.result`：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_cardinality --mode record
```

- [ ] **Step 4: 收紧断言并 verify**

把 query 4 的 `-- @explain_contains=stats={rows=` 改成 record 跑出的真实值（如 `stats={rows=100}`）。再 verify：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_cardinality --mode verify
```
Expected: PASS（post-filter rows 远小于 1000）。

- [ ] **Step 5: 回归 — 全 lib 测试 + 相关套件**

```bash
cargo test --lib
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-rest --mode verify
```
Expected: lib = **3320 passed / 5 failed**（仅「执行前基线」列出的 5 个 pre-existing 失败，**无新增**）；iceberg-rest 套件无回归。
> 若 optimizer 套件的 golden 因 Task 2 的 fallback selectivity 而 `rows=` 变化：用 `--suite optimizer --mode record` 重生成，逐条人工确认「变小了是因为谓词现在生效」而非误伤，再提交。

- [ ] **Step 6: 标杆复核（套件转 Iceberg 后，由用户准备）**

对 `join_one_key` q22、`join_linear_chained` q31、一个简单 INNER `count(*)` 跑 `EXPLAIN COSTS`，与 StarRocks plan 写 cardinality diff（FE 在 9030，见 roadmap §可复用资产）。确认 scan 后基数同数量级。把 diff quote 进 PR 描述（roadmap PR checklist 第 3 条）。

- [ ] **Step 7: 关 server + 提交**

```bash
kill "$SRV_PID" 2>/dev/null
git add sql-tests/iceberg-rest/sql/iceberg_rest_cardinality.sql sql-tests/iceberg-rest/result/iceberg_rest_cardinality.result
git commit -m "test(iceberg-rest): golden for post-filter cardinality (OQ-3)

Assert that a predicate (k1 < 100) reduces the Iceberg scan row-count
estimate via @explain_contains on the stats={rows=N} trailer.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage（逐条对 spec §4 / §9）：**
- §4.1 Iceberg 统计补强 → Task 5 Step 2（验证 + 必要时修 `build_table_statistics`）。✓
- §4.2.1 extraction 不丢列统计 → Task 1（同时修 `child_statistics` 与 `group_statistics`）。✓ 且发现 derivation 期也丢，一并修。
- §4.2.2 无统计 fallback 应用 selectivity → Task 2。✓
- §4.2.3 基础 join/agg 基数 → 已存在（`derive_join`/agg NDV），Task 1 解锁真实输入；无新算子。✓（深度决定见头部）
- §4.3 selectivity 覆盖（Between）→ Task 3。range 公式 `+1.0` polish 列为可选，未单列 task（YAGNI；如需，追加一个直接测 `estimate_range_selectivity` 的 task）。✓（部分，polish 显式延后）
- §4.4 EXPLAIN COSTS per-column → Task 4。✓
- §7 验证（iceberg golden + 标杆）→ Task 5。✓

**2. Placeholder scan：** 无 TBD/TODO。唯一外部依赖确认点是 Task 3 Step 1（`ExprKind::Between` 字段名）与 Task 5 Step 2（Iceberg min/max 是否已填充）——都写成显式「确认/分支」步骤，非占位符。

**3. Type consistency：** `LogicalProperties.column_statistics`、`Statistics.column_statistics` 均为 `HashMap<String, ColumnStatistic>`（name-keyed，一致）。`derive_for_group`/`derive_for_expr` 新参数类型一致。`format_column_stats_costs(&Statistics) -> String` 在 Task 4 定义并调用。`ColumnStatistic` 字段（min_value/max_value/nulls_fraction/average_row_size/distinct_values_count）在所有 test 构造处一致。

**4. 范围：** 单条 OQ-3 主线，5 个 task 顺序无环（Task 1 是 2/4 的语义前提但各自可独立编译/提交；Task 5 依赖 1-4）。

---

## 执行交接

详见交接消息。
