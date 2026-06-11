# OQ-12 Stats 与 NDV Robustness 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 optimizer 的 row count / NDV / selectivity 从"能展示"提升为"能支撑 CBO 决策"的可靠输入,根除 overflow（q72 的 `i64::MAX`）、rows=1 collapse（q85）、多键 join 低估（q9/q20）。

**Architecture:** 方向 B——新建纯函数内核 `src/sql/optimizer/estimate/`（饱和算术 + `Confidence` 模型 + 统一 join 基数 / 选择率 / NDV 公式）。Cascades 的 `stats.rs`（Logical+Physical 双套 match 臂）与 join-reorder 的 `cardinality.rs`（LogicalPlan 树）全部委托给内核,消除 4 处 join 基数 + 2 处 filter/agg 的数值漂移。Confidence 随统计贯穿,fallback 全部可观测（EXPLAIN `conf=` + `>=` overflow 前缀,供 OQ-16 解析）。

**Tech Stack:** Rust（crate `novarocks`）、Arrow、sql-test-runner（`tests/sql-test-runner`）、sql-tests/optimizer 合成表 golden。

**Spec:** `docs/design/specs/2026-06-03-oq-12-stats-and-ndv-robustness-design.md`

---

## File Structure

新建：
- `src/sql/optimizer/estimate/mod.rs` — 模块根 + 再导出。
- `src/sql/optimizer/estimate/arith.rs` — `MAX_ROW_COUNT`、`sat_mul/sat_add/sat_div`、`damped_conjunction`。
- `src/sql/optimizer/estimate/cardinality.rs` — `JoinCardInput`、`estimate_join_cardinality`、set-op 行数。
- `src/sql/optimizer/estimate/selectivity.rs` — `estimate_selectivity` 及其 helper（从 `stats.rs` 迁出）。
- `src/sql/optimizer/estimate/ndv.rs` — `get_expr_ndv`、filter/agg/join NDV 传播 helper。
- `sql-tests/optimizer/sql/stats_*.sql` — 7 个合成 golden。

修改：
- `src/sql/optimizer/statistics.rs` — `Confidence` 枚举、两类型加字段、`Default`、常量集中。
- `src/sql/optimizer/mod.rs` — 注册 `pub(crate) mod estimate;`。
- `src/sql/optimizer/stats.rs` — 逐算子 derivation 委托内核。
- `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs` — `estimate_join` 委托内核。
- `src/sql/explain.rs` — 渲染兜底 + COSTS/ANALYZE 的 `conf=`。
- `tests/sql-test-runner/src/{parser.rs,types.rs,results.rs}` — `@explain_not_contains`。

**通用约定**
- 单测命令：`cargo test --lib <filter>`（先 `cargo build` 验证编译）。
- SQL golden：先按 CLAUDE.md §7 启动 standalone-server（等 `NOVAROCKS_READY`），再用 runner `--suite optimizer --only <case> --mode record|verify`。
- 每个 Task 末尾 commit；commit message 用英文。

---

## Phase P0 — 内核地基（Confidence / 饱和算术 / 字段 / 渲染兜底）

### Task 0.1: `Confidence` 模型

**Files:**
- Modify: `src/sql/optimizer/statistics.rs`（在 `ColumnStatistic` 定义之前插入）

- [ ] **Step 1: 写失败测试**（追加到 `statistics.rs` 的 `mod tests`）

```rust
#[test]
fn confidence_ordering_and_combine() {
    use Confidence::*;
    assert!(Exact > Estimated && Estimated > Fallback);
    // combine = 最不可信者胜
    assert_eq!(Exact.combine(Fallback), Fallback);
    assert_eq!(Exact.combine(Estimated), Estimated);
    // derive：公式结果最高 Estimated；任一 Fallback 输入 → Fallback
    assert_eq!(Confidence::derive(&[Exact, Exact], false), Estimated);
    assert_eq!(Confidence::derive(&[Exact, Fallback], false), Fallback);
    assert_eq!(Confidence::derive(&[Exact, Exact], true), Fallback);
    assert_eq!(Confidence::default(), Fallback);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib confidence_ordering_and_combine`
Expected: 编译失败 `cannot find type Confidence`.

- [ ] **Step 3: 实现**（插到 `statistics.rs` 顶部 `use` 之后）

```rust
/// Trustworthiness of a statistic. Variant order is meaningful: derived
/// `Ord` makes `Exact > Estimated > Fallback`, so `min` yields the
/// least-confident input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Confidence {
    #[default]
    Fallback,  // relied on a heuristic/default (name-based rows, default selectivity/NDV)
    Estimated, // derived via formula from at-least-partially-real inputs
    Exact,     // sourced from real catalog/Iceberg stats (Puffin NDV, metadata row_count)
}

impl Confidence {
    /// Least-confident of two confidences.
    pub fn combine(self, other: Confidence) -> Confidence {
        self.min(other)
    }

    /// Confidence of a value produced by applying a formula to `inputs`.
    /// A formula result is never better than `Estimated`; any `Fallback`
    /// input — or `used_default` — degrades the result to `Fallback`.
    pub fn derive(inputs: &[Confidence], used_default: bool) -> Confidence {
        if used_default {
            return Confidence::Fallback;
        }
        let least = inputs.iter().copied().min().unwrap_or(Confidence::Estimated);
        least.min(Confidence::Estimated)
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib confidence_ordering_and_combine`
Expected: `test result: ok. 1 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/statistics.rs
git commit -m "feat(optimizer): add Confidence model for stats robustness (OQ-12 P0)"
```

---

### Task 0.2: 饱和算术原语 + `estimate/` 模块骨架

**Files:**
- Create: `src/sql/optimizer/estimate/mod.rs`
- Create: `src/sql/optimizer/estimate/arith.rs`
- Modify: `src/sql/optimizer/mod.rs`（加 `pub(crate) mod estimate;`，与现有 `mod stats; mod statistics;` 同处）

- [ ] **Step 1: 写失败测试**（`arith.rs` 内 `mod tests`）

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sat_mul_caps_and_flags() {
        let (v, sat) = sat_mul(1e10, 1e10); // 1e20 > MAX_ROW_COUNT
        assert_eq!(v, MAX_ROW_COUNT);
        assert!(sat);
        let (v2, sat2) = sat_mul(1000.0, 1000.0);
        assert_eq!(v2, 1_000_000.0);
        assert!(!sat2);
        // infinity input saturates, never NaN
        let (v3, sat3) = sat_mul(f64::INFINITY, 2.0);
        assert_eq!(v3, MAX_ROW_COUNT);
        assert!(sat3);
    }

    #[test]
    fn sat_div_guards_zero() {
        let (v, sat) = sat_div(100.0, 0.0);
        assert_eq!(v, 100.0); // numerator returned unchanged
        assert!(sat);
        let (v2, sat2) = sat_div(100.0, 4.0);
        assert_eq!(v2, 25.0);
        assert!(!sat2);
    }

    #[test]
    fn damped_conjunction_never_collapses() {
        // 5 个 0.25 谓词：朴素连乘 = 0.25^5 ≈ 0.000977
        let naive = 0.25_f64.powi(5);
        let damped = damped_conjunction(&[0.25, 0.25, 0.25, 0.25, 0.25]);
        assert!(damped > naive * 10.0, "damped {damped} should be >> naive {naive}");
        assert!(damped <= 0.25, "damped must not exceed the strongest selectivity");
        // 单个 selectivity 原样返回
        assert!((damped_conjunction(&[0.3]) - 0.3).abs() < 1e-9);
        // 空 → 1.0（无谓词不缩减）
        assert_eq!(damped_conjunction(&[]), 1.0);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib estimate::arith`
Expected: 编译失败（模块不存在）。

- [ ] **Step 3: 实现**

`src/sql/optimizer/estimate/mod.rs`：

```rust
//! Pure-function statistics kernel: a single source of truth for saturating
//! arithmetic, join cardinality, predicate selectivity and NDV propagation.
//! Both the Cascades `stats` derivation and the join-reorder `cardinality`
//! walker delegate here so they never drift numerically.

pub(crate) mod arith;
// cardinality / selectivity / ndv added in later phases.
```

`src/sql/optimizer/estimate/arith.rs`：

```rust
//! Saturating arithmetic for row-count estimation.
//!
//! Row counts are `f64` but must never reach the magnitudes that the EXPLAIN
//! renderer would saturate into `i64::MAX`. Every product/sum/quotient that
//! feeds a row count goes through these helpers, which clamp to
//! [`MAX_ROW_COUNT`] and report whether the cap was hit (so callers can
//! downgrade confidence to `Fallback`).

/// Upper bound for any estimated row count. Far below `i64::MAX / 2` and any
/// realistic table size, so it both prevents f64 overflow in downstream
/// products and renders cleanly instead of `9223372036854775807`.
pub const MAX_ROW_COUNT: f64 = 1e15;

/// `a * b`, clamped to `[0, MAX_ROW_COUNT]`. Returns `(value, saturated)`.
/// Non-finite results saturate (never propagate NaN/inf).
pub fn sat_mul(a: f64, b: f64) -> (f64, bool) {
    clamp_row_count(a * b)
}

/// `a + b`, clamped to `[0, MAX_ROW_COUNT]`.
pub fn sat_add(a: f64, b: f64) -> (f64, bool) {
    clamp_row_count(a + b)
}

/// `a / b`. Guards `b <= 0` (returns the numerator + saturated=true rather
/// than NaN/inf).
pub fn sat_div(a: f64, b: f64) -> (f64, bool) {
    if !(b > 0.0) {
        return (a, true);
    }
    clamp_row_count(a / b)
}

fn clamp_row_count(v: f64) -> (f64, bool) {
    if v.is_nan() {
        (0.0, true)
    } else if v >= MAX_ROW_COUNT || v.is_infinite() {
        (MAX_ROW_COUNT, true)
    } else if v < 0.0 {
        (0.0, true)
    } else {
        (v, false)
    }
}

/// Combine independent selectivities with exponential backoff so a conjunction
/// never collapses toward zero. Sorts ascending (strongest/smallest first),
/// then `s1 * s2^(1/2) * s3^(1/4) * ...`. Empty slice → 1.0 (no reduction).
pub fn damped_conjunction(selectivities: &[f64]) -> f64 {
    let mut s: Vec<f64> = selectivities
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .map(|v| v.clamp(0.0, 1.0))
        .collect();
    if s.is_empty() {
        return 1.0;
    }
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mut combined = 1.0f64;
    let mut exp = 1.0f64;
    for sel in s {
        combined *= sel.powf(exp);
        exp *= 0.5;
    }
    combined
}
```

在 `src/sql/optimizer/mod.rs` 加模块声明（紧邻现有 `mod stats;`）：

```rust
pub(crate) mod estimate;
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib estimate::arith`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/estimate/ src/sql/optimizer/mod.rs
git commit -m "feat(optimizer): add saturating arith + damped_conjunction kernel (OQ-12 P0)"
```

---

### Task 0.3: 给 `Statistics`/`ColumnStatistic` 加 confidence 字段（含全量构造点修复）

**Files:**
- Modify: `src/sql/optimizer/statistics.rs`
- Modify: 全部构造 `Statistics {`（16 文件）/ `ColumnStatistic {`（9 文件）的站点

- [ ] **Step 1: 改类型 + Default + unknown()**

```rust
#[derive(Clone, Debug, Default)]
pub struct ColumnStatistic {
    pub min_value: f64,
    pub max_value: f64,
    pub nulls_fraction: f64,
    pub average_row_size: f64,
    pub distinct_values_count: f64,
    pub confidence: Confidence,   // 新增；Default = Fallback
}
```
`ColumnStatistic::unknown()` 末尾加 `confidence: Confidence::Fallback,`。

```rust
#[derive(Clone, Debug, Default)]
pub struct Statistics {
    pub output_row_count: f64,
    pub row_count_confidence: Confidence,   // 新增
    pub column_statistics: HashMap<String, ColumnStatistic>,
}
```
> `f64`/`HashMap`/`Confidence` 都实现 `Default`，故可 `#[derive(Default)]`。

- [ ] **Step 2: 枚举所有构造点**

Run:
```bash
grep -rn "Statistics {" src/ | grep -v "TableStatistics\|CostEstimate"
grep -rn "ColumnStatistic {" src/
```
Expected: 列出全部站点（约 25 处，多在 `stats.rs`/`cardinality.rs`/`explain.rs` 测试与 `build_table_statistics_with_ndv`）。

- [ ] **Step 3: 逐站点补字段**

规则：
- `build_table_statistics_with_ndv`（statistics.rs:230-260）构造 `ColumnStatistic` → `confidence: Confidence::Exact`（来自真实文件元数据）。
- 非测试的 derivation 站点暂统一补 `row_count_confidence: Confidence::Estimated` / `confidence: Confidence::Estimated`（后续 Phase 会按来源细化；先让编译通过、行为不变）。
- 测试站点补 `..Default::default()`（位于字段列表末尾）或显式 `Confidence::Estimated`。

例（`stats.rs` 的 `LogicalValues` 臂）：
```rust
Operator::LogicalValues(vals) => Statistics {
    output_row_count: vals.rows.len() as f64,
    row_count_confidence: Confidence::Exact, // literal row count is exact
    column_statistics: HashMap::new(),
},
```

- [ ] **Step 4: 编译 + 跑现有测试**

Run: `cargo build && cargo test --lib statistics::`
Expected: 编译通过；statistics.rs 现有测试全过（含 `column_statistic_unknown`、`build_table_statistics_*`）。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(optimizer): thread Confidence through Statistics/ColumnStatistic (OQ-12 P0)"
```

---

### Task 0.4: 渲染兜底（overflow 不再 i64::MAX）

**Files:**
- Modify: `src/sql/explain.rs:18-28`（`format_stats_trailer`）
- Modify: `src/sql/explain.rs` 测试模块（补字段 + 新 case）

- [ ] **Step 1: 写失败测试**（追加到 explain.rs 第一个 stats 测试模块，约 1605 行处）

```rust
#[test]
fn stats_trailer_caps_overflow_instead_of_i64_max() {
    let inf = Statistics { output_row_count: f64::INFINITY, ..Default::default() };
    assert_eq!(format_stats_trailer(&inf), "stats={rows=>=1e15}");
    let huge = Statistics { output_row_count: 9.5e18, ..Default::default() };
    assert_eq!(format_stats_trailer(&huge), "stats={rows=>=1e15}");
    // 阈值以下仍正常
    let ok = Statistics { output_row_count: 1234.0, ..Default::default() };
    assert_eq!(format_stats_trailer(&ok), "stats={rows=1234}");
}
```
> 同时把该模块内既有 `Statistics { output_row_count: .., column_statistics: .. }` 字面量改为补 `..Default::default()`（Task 0.3 若已统一处理则跳过）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib stats_trailer_caps_overflow`
Expected: FAIL（当前 `9.5e18` → `9223372036854775807`，`inf` → `i64::MAX`）。

- [ ] **Step 3: 实现**（替换 explain.rs:21-26）

```rust
    let rows = stats.output_row_count;
    let rows_str: String = if rows.is_nan() || rows <= 0.0 {
        "?".to_string()
    } else if rows.is_infinite()
        || rows >= crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT
    {
        // Saturated estimate — render a clear overflow marker, never i64::MAX.
        // OQ-16 keys off the ">=" prefix.
        ">=1e15".to_string()
    } else {
        (rows.round() as i64).to_string()
    };
```
> 需 `estimate::arith` 为 `pub(crate)`（已是）。`format!("{:.0e}", 1e15)` 在 Rust 下为 `"1e15"`；此处直接写字面 `">=1e15"` 与 `MAX_ROW_COUNT` 对齐,避免格式歧义。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib stats_trailer`
Expected: 新 case + 4 个既有 case（`?`/124/NaN/negative）全过。

- [ ] **Step 5: Commit**

```bash
git add src/sql/explain.rs
git commit -m "fix(explain): cap saturated row counts to >=1e15 instead of i64::MAX (OQ-12 q72)"
```

---

## Phase P1 — 统一 join 基数内核 + 三路径委托

### Task 1.1: `estimate/cardinality.rs` — `estimate_join_cardinality`

**Files:**
- Create: `src/sql/optimizer/estimate/cardinality.rs`
- Modify: `src/sql/optimizer/estimate/mod.rs`（加 `pub(crate) mod cardinality;`）
- Modify: `src/sql/optimizer/statistics.rs`（把 semi 常量集中：新增 `pub const SEMI_JOIN_SELECTIVITY: f64 = 0.3;`）

- [ ] **Step 1: 写失败测试**（`cardinality.rs` 内 `mod tests`）

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::JoinKind;
    use crate::sql::optimizer::statistics::Confidence;

    fn inp(kind: JoinKind, l: f64, r: f64, keys: Vec<(f64, f64)>) -> JoinCardInput {
        JoinCardInput {
            left: (l, Confidence::Estimated),
            right: (r, Confidence::Estimated),
            kind,
            eq_key_ndvs: keys.into_iter().map(|(a, b)| (a, b, Confidence::Estimated)).collect(),
            non_equi_selectivity: None,
        }
    }

    #[test]
    fn single_key_inner_matches_containment() {
        // 1000 x 800 / max(ndv) ; ndv=100 → 8000
        let (rows, _) = estimate_join_cardinality(&inp(JoinKind::Inner, 1000.0, 800.0, vec![(100.0, 50.0)]));
        assert!((rows - 8000.0).abs() < 1.0, "got {rows}");
    }

    #[test]
    fn multikey_inner_does_not_collapse_or_inflate() {
        // 两键，均 ndv=100。max-NDV 旧法 = 1000*1000/100 = 10000（低估反而偏大）。
        // 阻尼连乘：sel = (1/100) * (1/100)^0.5 = 0.01 * 0.1 = 0.001 → 1000 行。
        let (rows, _) = estimate_join_cardinality(&inp(JoinKind::Inner, 1000.0, 1000.0, vec![(100.0, 100.0), (100.0, 100.0)]));
        assert!(rows < 10000.0 && rows > 1.0, "multikey should reduce below single-key but not collapse: {rows}");
        assert!((rows - 1000.0).abs() < 50.0, "got {rows}");
    }

    #[test]
    fn outer_join_at_least_preserved_side() {
        let (rows, _) = estimate_join_cardinality(&inp(JoinKind::LeftOuter, 5000.0, 10.0, vec![(1e6, 1e6)]));
        assert!(rows >= 5000.0, "left outer must keep >= left rows: {rows}");
    }

    #[test]
    fn cross_join_saturates_with_fallback() {
        let (rows, conf) = estimate_join_cardinality(&inp(JoinKind::Cross, 1e9, 1e9, vec![]));
        assert_eq!(rows, crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT);
        assert_eq!(conf, Confidence::Fallback);
    }

    #[test]
    fn semi_and_anti_bounded_by_left() {
        let (semi, _) = estimate_join_cardinality(&inp(JoinKind::LeftSemi, 1000.0, 50.0, vec![(10.0, 10.0)]));
        assert!(semi <= 1000.0 && semi >= 1.0);
        let (anti, _) = estimate_join_cardinality(&inp(JoinKind::LeftAnti, 1000.0, 50.0, vec![(10.0, 10.0)]));
        assert!(anti <= 1000.0 && anti >= 1.0);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib estimate::cardinality`
Expected: 编译失败（模块不存在）。

- [ ] **Step 3: 实现**

`src/sql/optimizer/estimate/mod.rs` 增 `pub(crate) mod cardinality;`。`statistics.rs` 增 `pub const SEMI_JOIN_SELECTIVITY: f64 = 0.3;`。

`src/sql/optimizer/estimate/cardinality.rs`：

```rust
//! Single source of truth for join output cardinality. Both the Cascades
//! `stats` derivation and the join-reorder `cardinality` walker build a
//! `JoinCardInput` from their own representation and call here.

use crate::sql::analysis::JoinKind;
use crate::sql::optimizer::statistics::{Confidence, ANTI_JOIN_SELECTIVITY, SEMI_JOIN_SELECTIVITY};

use super::arith::{damped_conjunction, sat_mul, MAX_ROW_COUNT};

/// Representation-independent inputs to join cardinality estimation.
pub struct JoinCardInput {
    pub left: (f64, Confidence),
    pub right: (f64, Confidence),
    pub kind: JoinKind,
    /// One entry per equi-join key: `(left_ndv, right_ndv, confidence)`.
    pub eq_key_ndvs: Vec<(f64, f64, Confidence)>,
    /// Selectivity of any non-equi residual predicate.
    pub non_equi_selectivity: Option<(f64, Confidence)>,
}

/// Estimate join output rows + confidence. Never overflows (saturates at
/// `MAX_ROW_COUNT`) and never collapses below 1.0 for a non-empty join.
pub fn estimate_join_cardinality(input: &JoinCardInput) -> (f64, Confidence) {
    let l = input.left.0.max(1.0);
    let r = input.right.0.max(1.0);
    let mut conf = input.left.1.combine(input.right.1);

    // Combined equi-key selectivity = damped product of per-key 1/max(ndv).
    let mut key_sels: Vec<f64> = Vec::with_capacity(input.eq_key_ndvs.len());
    for (lndv, rndv, c) in &input.eq_key_ndvs {
        let ndv = lndv.max(*rndv).max(1.0);
        key_sels.push(1.0 / ndv);
        conf = conf.combine(*c);
    }
    if let Some((_, c)) = input.non_equi_selectivity {
        conf = conf.combine(c);
    }
    let non_equi = input.non_equi_selectivity.map(|(s, _)| s).unwrap_or(1.0);

    let (lr, sat_lr) = sat_mul(l, r);

    let (rows, saturated) = match input.kind {
        JoinKind::Cross => (lr, sat_lr),
        JoinKind::Inner => {
            if key_sels.is_empty() {
                let (v, s) = sat_mul(lr, non_equi);
                (v, s || sat_lr)
            } else {
                let sel = damped_conjunction(&key_sels) * non_equi;
                let (v, s) = sat_mul(lr, sel);
                (v.max(1.0), s || sat_lr)
            }
        }
        JoinKind::LeftOuter => {
            let inner = inner_rows(lr, &key_sels, non_equi);
            (inner.0.max(l), inner.1 || sat_lr)
        }
        JoinKind::RightOuter => {
            let inner = inner_rows(lr, &key_sels, non_equi);
            (inner.0.max(r), inner.1 || sat_lr)
        }
        JoinKind::FullOuter => {
            let inner = inner_rows(lr, &key_sels, non_equi);
            (inner.0.max(l).max(r), inner.1 || sat_lr)
        }
        JoinKind::LeftSemi => {
            let sel = input.non_equi_selectivity.map(|(s, _)| s).unwrap_or(SEMI_JOIN_SELECTIVITY);
            ((l * sel).max(1.0), false)
        }
        JoinKind::RightSemi => {
            let sel = input.non_equi_selectivity.map(|(s, _)| s).unwrap_or(SEMI_JOIN_SELECTIVITY);
            ((r * sel).max(1.0), false)
        }
        JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
            ((l * ANTI_JOIN_SELECTIVITY).max(1.0), false)
        }
        JoinKind::RightAnti => ((r * ANTI_JOIN_SELECTIVITY).max(1.0), false),
    };

    let rows = rows.min(MAX_ROW_COUNT).max(1.0);
    let out_conf = if saturated {
        Confidence::Fallback
    } else {
        Confidence::derive(&[conf], false)
    };
    (rows, out_conf)
}

/// Inner-join rows for equi (damped) or cross (no keys), with non-equi factor.
fn inner_rows(lr: f64, key_sels: &[f64], non_equi: f64) -> (f64, bool) {
    if key_sels.is_empty() {
        sat_mul(lr, non_equi)
    } else {
        let sel = damped_conjunction(key_sels) * non_equi;
        let (v, s) = sat_mul(lr, sel);
        (v.max(1.0), s)
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib estimate::cardinality`
Expected: `test result: ok. 5 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/estimate/ src/sql/optimizer/statistics.rs
git commit -m "feat(optimizer): unified estimate_join_cardinality kernel (OQ-12 P1)"
```

---

### Task 1.2: 委托 `PhysicalHashJoin`（stats.rs:348-412）

**Files:**
- Modify: `src/sql/optimizer/stats.rs:348-412`

- [ ] **Step 1: 实现委托**（替换该臂 364-404 的 `match join.join_type { .. }` 计算）

```rust
        Operator::PhysicalHashJoin(join) => {
            use crate::sql::optimizer::estimate::cardinality::{
                estimate_join_cardinality, JoinCardInput,
            };
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);

            let eq_key_ndvs = join
                .eq_conditions
                .iter()
                .map(|eq| {
                    let l_ndv = get_expr_ndv(&eq.left, &left_stats.column_statistics)
                        .max(get_expr_ndv(&eq.left, &right_stats.column_statistics));
                    let r_ndv = get_expr_ndv(&eq.right, &left_stats.column_statistics)
                        .max(get_expr_ndv(&eq.right, &right_stats.column_statistics));
                    (l_ndv, r_ndv, Confidence::Estimated)
                })
                .collect();

            let (output_rows, row_count_confidence) = estimate_join_cardinality(&JoinCardInput {
                left: (left_stats.output_row_count, left_stats.row_count_confidence),
                right: (right_stats.output_row_count, right_stats.row_count_confidence),
                kind: join.join_type,
                eq_key_ndvs,
                non_equi_selectivity: None,
            });

            let mut column_statistics = left_stats.column_statistics;
            column_statistics.extend(right_stats.column_statistics);
            Statistics { output_row_count: output_rows, row_count_confidence, column_statistics }
        }
```
> 删除旧 `max_ndv` 循环与 `DEFAULT_FILTER_SELECTIVITY` 引用（该常量若仅此处用，移除其 `use`/定义在 Task 1.4 后统一清理）。`Confidence` 需在 `stats.rs` 顶部 `use crate::sql::optimizer::statistics::*;` 已覆盖（含枚举）。

- [ ] **Step 2: 编译**

Run: `cargo build`
Expected: 通过（若报 `DEFAULT_FILTER_SELECTIVITY` 未使用，保留至 1.4 清理）。

- [ ] **Step 3: 回归既有 join 单测**

Run: `cargo test --lib stats::`
Expected: 通过（行为等价或更优；如某 stats 单测 pin 了旧 max-NDV 多键值，更新为内核值并在 commit 注明）。

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/stats.rs
git commit -m "refactor(optimizer): PhysicalHashJoin delegates to cardinality kernel (OQ-12 P1)"
```

---

### Task 1.3: 委托 `PhysicalNestLoopJoin`（stats.rs:414-467）

**Files:**
- Modify: `src/sql/optimizer/stats.rs:414-467`

- [ ] **Step 1: 实现委托**

```rust
        Operator::PhysicalNestLoopJoin(join) => {
            use crate::sql::optimizer::estimate::cardinality::{
                estimate_join_cardinality, JoinCardInput,
            };
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);

            let non_equi_selectivity = join.condition.as_ref().map(|cond| {
                (estimate_selectivity(cond, &left_stats.column_statistics), Confidence::Estimated)
            });

            let (output_rows, row_count_confidence) = estimate_join_cardinality(&JoinCardInput {
                left: (left_stats.output_row_count, left_stats.row_count_confidence),
                right: (right_stats.output_row_count, right_stats.row_count_confidence),
                kind: join.join_type,
                eq_key_ndvs: Vec::new(), // nest-loop has no equi keys
                non_equi_selectivity,
            });

            let mut column_statistics = left_stats.column_statistics;
            column_statistics.extend(right_stats.column_statistics);
            Statistics { output_row_count: output_rows, row_count_confidence, column_statistics }
        }
```

- [ ] **Step 2: 编译 + 测试**

Run: `cargo build && cargo test --lib stats::`
Expected: 通过。

- [ ] **Step 3: Commit**

```bash
git add src/sql/optimizer/stats.rs
git commit -m "refactor(optimizer): PhysicalNestLoopJoin delegates to cardinality kernel (OQ-12 P1)"
```

---

### Task 1.4: 委托 Logical join `derive_join`（stats.rs:852-944）

**Files:**
- Modify: `src/sql/optimizer/stats.rs`（`derive_join` 函数；先 Read 该函数确认其用 `get_join_key_ndv` 还是 eq_conditions）

- [ ] **Step 1: Read 当前实现**

Run: `sed -n '850,945p' src/sql/optimizer/stats.rs`（或 Read 工具）
确认其参数与 join 条件结构（与 `PhysicalHashJoin` 的 `eq_conditions` 一致则照 1.2 构造 `eq_key_ndvs`；若走单一 `condition` 则照 cardinality.rs 的 `get_join_key_ndv` 拆键）。

- [ ] **Step 2: 实现委托**

按 1.2 的模式构造 `JoinCardInput` 并调用 `estimate_join_cardinality`，输出 `output_row_count` + `row_count_confidence`。删除本函数内重复的逐 join-kind 算式。若此处是清理 `DEFAULT_FILTER_SELECTIVITY` 的最后一处，移除该常量定义（stats.rs:18）。

- [ ] **Step 3: 编译 + 测试**

Run: `cargo build && cargo test --lib stats::`
Expected: 通过。

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/stats.rs
git commit -m "refactor(optimizer): logical derive_join delegates to cardinality kernel (OQ-12 P1)"
```

---

### Task 1.5: 委托 join-reorder `estimate_join`（cardinality.rs:203-294）

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs:203-294`
- 保留 `get_join_key_ndv`（用于拆键）但其结果改喂内核；或改造为产出 `eq_key_ndvs`。

- [ ] **Step 1: 实现委托**

把 `estimate_join` 改为：递归算 `left_stats`/`right_stats` 后，用现有 `get_join_key_ndv` 的拆键逻辑产出每键 `(l_ndv, r_ndv, Confidence::Estimated)`（若 `get_join_key_ndv` 只回单值,则按"单键 = 该值"塞一条 `(ndv, ndv, _)`），semi 用 `estimate_selectivity` 作 `non_equi_selectivity`，调用 `estimate_join_cardinality`。

```rust
fn estimate_join(join: &JoinNode, table_stats: &HashMap<String, TableStatistics>) -> Statistics {
    use crate::sql::optimizer::estimate::cardinality::{estimate_join_cardinality, JoinCardInput};
    use crate::sql::optimizer::statistics::Confidence;

    let left_stats = estimate_statistics(&join.left, table_stats);
    let right_stats = estimate_statistics(&join.right, table_stats);

    let eq_key_ndvs = match &join.condition {
        Some(cond) => {
            let ndv = get_join_key_ndv(cond, &left_stats.column_statistics, &right_stats.column_statistics);
            vec![(ndv, ndv, Confidence::Estimated)]
        }
        None => Vec::new(),
    };
    let non_equi_selectivity = join.condition.as_ref().map(|c| {
        (estimate_selectivity(c, &left_stats.column_statistics), Confidence::Estimated)
    });

    let (output_row_count, row_count_confidence) = estimate_join_cardinality(&JoinCardInput {
        left: (left_stats.output_row_count, left_stats.row_count_confidence),
        right: (right_stats.output_row_count, right_stats.row_count_confidence),
        kind: join.join_type,
        eq_key_ndvs,
        non_equi_selectivity,
    });

    let mut column_statistics = left_stats.column_statistics;
    column_statistics.extend(right_stats.column_statistics);
    Statistics { output_row_count, row_count_confidence, column_statistics }
}
```
> 注：semi/anti 在内核里走 `non_equi_selectivity`/常量，与旧 `estimate_join` 行为对齐。

- [ ] **Step 2: 编译 + join-reorder 测试**

Run: `cargo build && cargo test --lib join_reorder`
Expected: 通过；若某 plan-shape 单测因 join 顺序改善而变化,确认是改进后更新。

- [ ] **Step 3: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs
git commit -m "refactor(optimizer): join-reorder estimate_join delegates to kernel (OQ-12 P1)"
```

---

### Task 1.6: 漂移守卫单测

**Files:**
- Modify: `src/sql/optimizer/estimate/cardinality.rs`（`mod tests` 追加）

- [ ] **Step 1: 写测试**

```rust
#[test]
fn single_key_inner_equals_legacy_containment_formula() {
    // 守卫：单键 inner 必须恒等于 L*R/max(ndv)，确保所有委托路径与历史公式一致。
    for &(l, r, ndv) in &[(1000.0, 500.0, 50.0), (10.0, 8.0, 10.0), (1e6, 1e3, 1e4)] {
        let (rows, _) = estimate_join_cardinality(&inp(JoinKind::Inner, l, r, vec![(ndv, ndv)]));
        let expected = (l * r / ndv).min(crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT).max(1.0);
        assert!((rows - expected).abs() <= expected * 1e-9 + 1.0, "l={l} r={r} ndv={ndv}: {rows} vs {expected}");
    }
}
```

- [ ] **Step 2: 跑测试**

Run: `cargo test --lib estimate::cardinality::tests::single_key_inner_equals_legacy`
Expected: PASS（单键无阻尼,等于历史公式 → 四路径数值一致）。

- [ ] **Step 3: Commit**

```bash
git add src/sql/optimizer/estimate/cardinality.rs
git commit -m "test(optimizer): drift guard pins single-key join cardinality (OQ-12 P1)"
```

---

## Phase P2 — 谓词选择率（q85 collapse 主修复）

### Task 2.1: 把 `estimate_selectivity` 及 helper 迁入 `estimate/selectivity.rs`（纯搬迁，行为不变）

**Files:**
- Create: `src/sql/optimizer/estimate/selectivity.rs`
- Modify: `src/sql/optimizer/estimate/mod.rs`（`pub(crate) mod selectivity;`）
- Modify: `src/sql/optimizer/stats.rs`（删除迁出的函数；改 `pub(crate) use estimate::selectivity::{estimate_selectivity, extract_column_name};` 或在调用处改路径）
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs:10`（`use` 改指向新位置）

- [ ] **Step 1: 搬迁**

把 `stats.rs` 的 `estimate_selectivity`（1195-1280）、`estimate_eq_selectivity`（1282-1297）、`estimate_range_selectivity`（1299-1326）、`extract_column_name` 整体移入 `selectivity.rs`，签名不变（`pub(crate)`）。`stats.rs` 改为 `pub(crate) use super::estimate::selectivity::{estimate_selectivity, extract_column_name};` 以维持现有 `use crate::sql::optimizer::stats::{estimate_selectivity, extract_column_name}` 的外部引用（cardinality.rs:10）。

- [ ] **Step 2: 编译 + 全量选择率测试**

Run: `cargo build && cargo test --lib selectivity`
Expected: 现有选择率相关测试全过（纯搬迁,零行为变化）。

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor(optimizer): move estimate_selectivity into estimate/selectivity.rs (OQ-12 P2)"
```

---

### Task 2.2: AND 指数阻尼（替换朴素连乘）

**Files:**
- Modify: `src/sql/optimizer/estimate/selectivity.rs`（`BinOp::And` 臂）

- [ ] **Step 1: 写失败测试**（selectivity.rs `mod tests`）

```rust
#[test]
fn and_chain_does_not_collapse() {
    use std::collections::HashMap;
    // 构造 a=? AND b=? AND c=? AND d=? AND e=?，列无 stats → 每个 0.25。
    let preds = and_of_unknown_eq(5); // helper: 见实现说明
    let sel = estimate_selectivity(&preds, &HashMap::new());
    assert!(sel > 0.01, "5x0.25 AND must not collapse to ~0.001: {sel}");
    assert!(sel <= 0.25, "must not exceed strongest conjunct");
}
```
> `and_of_unknown_eq(n)` 在测试模块内构造 n 个未知列等值谓词的 AND 树（用 `TypedExpr`/`BinOp::And`/`BinOp::Eq`，列名互不相同以避免被合并）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib and_chain_does_not_collapse`
Expected: FAIL（当前 `l * r` 连乘 → ≈0.000977）。

- [ ] **Step 3: 实现**

新增 conjunct 扁平化 + 阻尼：

```rust
/// Flatten a left/right-nested AND tree into its leaf conjuncts.
fn flatten_and<'a>(expr: &'a TypedExpr, out: &mut Vec<&'a TypedExpr>) {
    if let ExprKind::BinaryOp { op: BinOp::And, left, right } = &expr.kind {
        flatten_and(left, out);
        flatten_and(right, out);
    } else {
        out.push(expr);
    }
}
```
把 `BinOp::And` 臂从 `l * r` 改为：
```rust
        BinOp::And => {
            let mut conjuncts = Vec::new();
            flatten_and(expr, &mut conjuncts);
            let sels: Vec<f64> = conjuncts
                .iter()
                .map(|c| estimate_selectivity(c, column_stats))
                .collect();
            crate::sql::optimizer::estimate::arith::damped_conjunction(&sels)
        }
```
> `ExprKind`/`BinOp` 的实际变体名以 `crate::sql::analysis` 为准；若 AND 是 `ExprKind::BinaryOp { op, left, right }` 之外的形态（如 `And(Vec)`），`flatten_and` 相应调整。

- [ ] **Step 4: 跑测试确认通过 + 回归**

Run: `cargo test --lib selectivity`
Expected: 新 case 过；既有 OR/range/eq 测试不变。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/estimate/selectivity.rs
git commit -m "fix(optimizer): damp conjunctive selectivity to prevent rows=1 collapse (OQ-12 q85)"
```

---

### Task 2.3: selectivity floor + filter 降 confidence

**Files:**
- Modify: `src/sql/optimizer/stats.rs`（`LogicalFilter`/`PhysicalFilter` 两臂，71-80 与 296-305）

- [ ] **Step 1: 写失败测试**（stats.rs `mod tests`，需要构造 1000 行 child + 5×0.25 谓词的 filter MExpr——若构造成本高,改为内核级测试 `selectivity.rs`:断言 `apply_filter(1000.0, Confidence::Exact, sel_tiny)` 返回 rows≥1 且 conf 被降级）

```rust
// estimate/selectivity.rs 内新增可测的纯函数 apply_filter：
#[test]
fn tiny_selectivity_floors_and_downgrades() {
    let (rows, conf) = apply_filter(1000.0, crate::sql::optimizer::statistics::Confidence::Exact, 1e-6);
    assert!(rows >= 1.0);
    assert_eq!(conf, crate::sql::optimizer::statistics::Confidence::Fallback); // floor 触发 → 可观测
}
```

- [ ] **Step 2: 实现** `apply_filter`（selectivity.rs）

```rust
use crate::sql::optimizer::statistics::Confidence;

/// Apply a filter selectivity to a child row count. Floors at 1.0 for a
/// non-empty input and downgrades confidence to Fallback when the floor binds
/// (so collapse risk stays observable for OQ-16).
pub fn apply_filter(child_rows: f64, child_conf: Confidence, selectivity: f64) -> (f64, Confidence) {
    let raw = child_rows * selectivity;
    if raw < 1.0 && child_rows >= 1.0 {
        (1.0, Confidence::Fallback)
    } else {
        (raw.max(0.0), Confidence::derive(&[child_conf], false))
    }
}
```
把 `stats.rs` 两个 filter 臂的 `let output_rows = (child_stats.output_row_count * selectivity).max(1.0);` 改为：
```rust
            let (output_rows, row_count_confidence) = crate::sql::optimizer::estimate::selectivity::apply_filter(
                child_stats.output_row_count, child_stats.row_count_confidence, selectivity);
```
并把返回的 `Statistics` 带上 `row_count_confidence`。

- [ ] **Step 3: 测试 + 回归**

Run: `cargo test --lib estimate::selectivity && cargo build`
Expected: 通过。

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(optimizer): selectivity floor downgrades confidence on collapse (OQ-12 P2)"
```

---

## Phase P3 — NDV 传播

### Task 3.1: `get_expr_ndv` 迁入 `estimate/ndv.rs` + filter NDV 封顶

**Files:**
- Create: `src/sql/optimizer/estimate/ndv.rs`
- Modify: `mod.rs`（`pub(crate) mod ndv;`）、`stats.rs`、`selectivity.rs`（引用方）

- [ ] **Step 1: 搬迁 + 写测试**

把 `get_expr_ndv`（stats.rs:1344-1359）、`get_join_key_ndv`（1361-1388）移入 `ndv.rs`。新增 filter NDV 封顶 helper + 测试：

```rust
#[test]
fn filter_ndv_capped_at_output_rows() {
    // NDV 不能超过存活行数。
    assert_eq!(cap_ndv_at_rows(1000.0, 50.0), 50.0);
    assert_eq!(cap_ndv_at_rows(30.0, 50.0), 30.0);
}
```

- [ ] **Step 2: 实现**

```rust
/// A column's NDV can never exceed the number of surviving rows.
pub fn cap_ndv_at_rows(ndv: f64, rows: f64) -> f64 {
    ndv.min(rows).max(1.0)
}
```
在 `stats.rs` 两个 filter 臂里,对透传的 `column_statistics` 各列 `distinct_values_count = cap_ndv_at_rows(ndv, output_rows)`。

- [ ] **Step 3: 测试 + 提交**

Run: `cargo test --lib estimate::ndv && cargo build`
```bash
git add -A
git commit -m "refactor(optimizer): move NDV helpers to estimate/ndv.rs, cap filter NDV at rows (OQ-12 P3)"
```

---

### Task 3.2: aggregate group NDV 阻尼连乘

**Files:**
- Modify: `src/sql/optimizer/stats.rs`（`LogicalAggregate` 102-121、`PhysicalHashAggregate` 327-346）

- [ ] **Step 1: 写测试**（内核级,放 ndv.rs）

```rust
#[test]
fn agg_group_rows_damped_and_capped() {
    // 3 个 group key 各 ndv=100；朴素连乘 = 1e6，应被 child*0.75 与阻尼共同收敛。
    let rows = agg_group_rows(&[100.0, 100.0, 100.0], 10_000.0);
    assert!(rows <= 10_000.0 * 0.75 + 1.0);
    assert!(rows > 100.0, "must exceed single key");
}
```

- [ ] **Step 2: 实现** `agg_group_rows`（ndv.rs）

```rust
use super::arith::damped_conjunction;
use crate::sql::optimizer::statistics::UNKNOWN_GROUP_BY_CORRELATION;

/// Estimate grouped-aggregate output rows from group-key NDVs. Uses a damped
/// product (so many keys don't explode) capped at child_rows * correlation.
pub fn agg_group_rows(group_key_ndvs: &[f64], child_rows: f64) -> f64 {
    if group_key_ndvs.is_empty() {
        return 1.0;
    }
    // Damped product of NDVs (largest gets full weight, rest exponentially
    // discounted) so many group keys don't explode toward the cross product.
    let combined_ndv: f64 = {
        let mut sorted: Vec<f64> = group_key_ndvs.iter().copied().map(|n| n.max(1.0)).collect();
        sorted.sort_by(|a, b| b.partial_cmp(a).unwrap()); // largest first gets full weight
        let mut product = 1.0;
        let mut exp = 1.0;
        for ndv in sorted {
            product *= ndv.powf(exp);
            exp *= 0.5;
        }
        product
    };
    let capped = child_rows * UNKNOWN_GROUP_BY_CORRELATION;
    combined_ndv.min(capped).max(1.0)
}
```
把两个 aggregate 臂的 `ndv_product` 循环替换为 `let output_rows = agg_group_rows(&group_key_ndvs, child_stats.output_row_count);`，并带 `row_count_confidence: Confidence::derive(&[child_stats.row_count_confidence], false)`。

- [ ] **Step 3: 测试 + 提交**

Run: `cargo test --lib estimate::ndv && cargo build`
```bash
git add -A
git commit -m "fix(optimizer): damp aggregate group-key NDV product (OQ-12 P3)"
```

---

### Task 3.3: set-op 行数公式 + 列 stats merge

**Files:**
- Modify: `src/sql/optimizer/stats.rs`（set-op 臂,约 540-588）
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs`（`estimate_union/intersect/except`，若存在重复亦统一）

- [ ] **Step 1: Read 当前 set-op 臂**

Run: `sed -n '530,600p' src/sql/optimizer/stats.rs`
确认 union/intersect/except 当前公式与列 stats 处理（agent 报告：union all=sum、union distinct=sum*0.75、intersect=min*0.5、except=first*0.5，列 stats 取第一个输入）。

- [ ] **Step 2: 实现内核 helper**（cardinality.rs 内新增 set-op 函数,带 saturation + confidence；列 stats merge 在 stats.rs 内做）

```rust
pub fn union_all_rows(inputs: &[f64]) -> (f64, bool) {
    let mut acc = 0.0; let mut sat = false;
    for &r in inputs { let (v, s) = super::arith::sat_add(acc, r); acc = v; sat |= s; }
    (acc, sat)
}
```
- union distinct = `union_all_rows * UNKNOWN_GROUP_BY_CORRELATION`；
- intersect = `min(inputs) * 0.5`；except = `first * 0.5`。
- 列 stats：对相同输出列,`min_value=min(mins)`、`max_value=max(maxs)`、`distinct_values_count` 取 union 时 `sum.min(rows)`、intersect/except 取 `min`，`confidence = combine` 各输入。

- [ ] **Step 3: 测试**（ndv.rs/cardinality.rs 单测 union_all_rows 饱和；stats.rs 行为）

Run: `cargo test --lib estimate:: && cargo build`

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(optimizer): saturating set-op rows + column-stats merge (OQ-12 P3)"
```

---

### Task 3.4: join key 等价类 NDV 合并 + 输出列 NDV 封顶

**Files:**
- Modify: `src/sql/optimizer/stats.rs`（两个 join 臂的 `column_statistics` 合并处）

- [ ] **Step 1: 写测试**（ndv.rs）

```rust
#[test]
fn join_output_ndv_capped_at_output_rows() {
    assert_eq!(cap_ndv_at_rows(1e6, 8.0), 8.0); // 复用 3.1 的 helper
}
```

- [ ] **Step 2: 实现**

在 join 臂合并 `column_statistics` 后,对每列 `distinct_values_count = cap_ndv_at_rows(ndv, output_rows)`；对等值键列,令两侧 `distinct_values_count = min(left_ndv, right_ndv)`（containment）。

**同时收尾 window 输出列 NDV（spec §5.6）**：window 算子行数已透传（passthrough,正确）；新增窗口函数输出列的 `ColumnStatistic.confidence = Fallback`（rank/sum 分布未知）。定位 `LogicalWindow`/`PhysicalWindow` derivation 臂,对窗口新产出列标 `Confidence::Fallback`、`distinct_values_count` 不强加真值。

- [ ] **Step 3: 测试 + 提交**

Run: `cargo test --lib estimate::ndv && cargo build`
```bash
git add -A
git commit -m "feat(optimizer): merge join-key NDV equivalence, cap output NDV at rows (OQ-12 P3)"
```

---

## Phase P4 — Scan / Iceberg stats 接入

### Task 4.1: scan derivation 标注 confidence（真实 stats → Exact，heuristic → Fallback）

**Files:**
- Modify: `src/sql/optimizer/stats.rs`（`derive_scan` 703-763、`estimate_default_row_count` 772-849）
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs`（`estimate_scan`，同步）

- [ ] **Step 1: Read `derive_scan`**

Run: `sed -n '700,850p' src/sql/optimizer/stats.rs`
确认 `table_stats.get(name)` 命中时读 `ts.row_count`/`ts.column_stats`，未命中走 `estimate_default_row_count`。

- [ ] **Step 2: 实现**

- 命中 `TableStatistics` 分支：`row_count_confidence = Confidence::Exact`；每个有真实 `ColumnStatistic` 的列 `confidence = Confidence::Exact`（`build_table_statistics_with_ndv` 已填,故 Task 0.3 把那里设为 Exact 即生效）。
- 未命中(name-heuristic)：`row_count_confidence = Confidence::Fallback`；`ColumnStatistic::unknown()`（已是 Fallback）。
- scan 谓词经 `apply_filter` 组合 confidence（沿用 Task 2.3）。

- [ ] **Step 3: 写测试**（stats.rs：构造带 `TableStatistics` 的 `table_stats` map → derive_scan → 断言 `row_count_confidence == Exact`；空 map → `Fallback`）

Run: `cargo test --lib stats:: && cargo build`

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(optimizer): scan marks Exact for real table stats, Fallback for heuristics (OQ-12 P4)"
```

---

## Phase P5 — 可观测 + golden

### Task 5.1: COSTS/ANALYZE 渲染 `conf=`

**Files:**
- Modify: `src/sql/explain.rs`（`format_stats_trailer` 调用处 + 函数签名）

- [ ] **Step 1: Read 调用处**

Run: `grep -n "format_stats_trailer\|ExplainLevel" src/sql/explain.rs | head -40`
确认 trailer 在何处按 `ExplainLevel`（Verbose/Costs/Analyze）拼接（约 370-385）。

- [ ] **Step 2: 写失败测试**

```rust
#[test]
fn costs_trailer_shows_conf_for_non_exact() {
    let s = Statistics { output_row_count: 8.0, row_count_confidence: Confidence::Fallback, ..Default::default() };
    assert_eq!(format_stats_trailer_with_conf(&s, true), "stats={rows=8 conf=fallback}");
    // Exact 不显示 conf
    let e = Statistics { output_row_count: 8.0, row_count_confidence: Confidence::Exact, ..Default::default() };
    assert_eq!(format_stats_trailer_with_conf(&e, true), "stats={rows=8}");
    // 非 COSTS（show_conf=false，即 Verbose）永不显示
    assert_eq!(format_stats_trailer_with_conf(&s, false), "stats={rows=8}");
}
```

- [ ] **Step 3: 实现**

新增 `format_stats_trailer_with_conf(stats, show_conf: bool)`，`format_stats_trailer` 改为 `format_stats_trailer_with_conf(stats, false)` 的包装（保持 Verbose 文本完全不变）。Costs/Analyze 的调用处改调 `..._with_conf(stats, true)`。conf 文本映射：`Estimated→"estimated"`、`Fallback→"fallback"`、`Exact→`（不输出）。

```rust
pub(crate) fn format_stats_trailer_with_conf(stats: &Statistics, show_conf: bool) -> String {
    let rows_str = /* 同 Task 0.4 的分支 */;
    let conf_suffix = if show_conf {
        match stats.row_count_confidence {
            Confidence::Estimated => " conf=estimated",
            Confidence::Fallback => " conf=fallback",
            Confidence::Exact => "",
        }
    } else { "" };
    format!("stats={{rows={rows_str}{conf_suffix}}}")
}
```

- [ ] **Step 4: 测试 + 回归现有 explain golden**

Run: `cargo test --lib explain` 然后 SQL：`--suite optimizer --mode verify`（Verbose golden 文本应不变）。

- [ ] **Step 5: Commit**

```bash
git add src/sql/explain.rs
git commit -m "feat(explain): show conf= in COSTS/ANALYZE trailers only (OQ-12 P5)"
```

---

### Task 5.2: runner 新增 `@explain_not_contains`

**Files:**
- Modify: `tests/sql-test-runner/src/types.rs`（case 结构加 `explain_not_contains: Vec<String>`）
- Modify: `tests/sql-test-runner/src/parser.rs`（解析 `-- @explain_not_contains=`）
- Modify: `tests/sql-test-runner/src/results.rs`（断言 EXPLAIN 输出 **不含** 子串）

- [ ] **Step 1: Read 现有 `@explain_contains` 三处实现**

Run: `grep -n "explain_contains" tests/sql-test-runner/src/*.rs`

- [ ] **Step 2: 镜像实现**

按 `explain_contains` 的同款写法新增 `explain_not_contains`：parser 收集多条；results 对每条断言 `!explain_text.contains(substr)`，失败时报告含该子串的行。

- [ ] **Step 3: 测试**

Run: `cargo test --manifest-path tests/sql-test-runner/Cargo.toml`
Expected: runner 自身单测通过。

- [ ] **Step 4: Commit**

```bash
git add tests/sql-test-runner/
git commit -m "feat(sql-test-runner): add @explain_not_contains directive (OQ-12 P5)"
```

---

### Task 5.3: 7 个合成 golden

**Files:**
- Create: `sql-tests/optimizer/sql/stats_multikey_join_ndv.sql`
- Create: `sql-tests/optimizer/sql/stats_or_selectivity.sql`
- Create: `sql-tests/optimizer/sql/stats_outer_semi_anti_card.sql`
- Create: `sql-tests/optimizer/sql/stats_aggregate_group_ndv.sql`
- Create: `sql-tests/optimizer/sql/stats_setop_rowcount.sql`
- Create: `sql-tests/optimizer/sql/stats_no_collapse_and_chain.sql`
- Create: `sql-tests/optimizer/sql/stats_overflow_saturation.sql`

> 先 Read 一个现有 golden（如 `sql-tests/optimizer/sql/baseline_inner_join.sql`）确认 `${case_db}` 占位、header 指令、record 产物路径（`expected/`）的约定,照抄结构。下面给关键断言;每个文件用 `EXPLAIN VERBOSE`（除需要 `conf=` 的用 `EXPLAIN COSTS`），断言一律带闭合 `}`。

- [ ] **Step 1: 启动 standalone-server**（CLAUDE.md §7，等 `NOVAROCKS_READY`）

- [ ] **Step 2: 写 golden（示例 `stats_no_collapse_and_chain.sql`，q85 代理）**

```sql
-- @tags=optimizer,stats,oq12
-- q85 collapse proxy: many ANDed predicates must NOT drive the scan/filter
-- estimate down to rows=1 (exponential damping keeps it sane).
DROP TABLE IF EXISTS ${case_db}.t_and_chain;
CREATE TABLE ${case_db}.t_and_chain (a INT, b INT, c INT, d INT, e INT);
INSERT INTO ${case_db}.t_and_chain
SELECT x, x, x, x, x FROM TABLE(generate_series(1, 1000));
-- @explain_not_contains=stats={rows=1}
EXPLAIN VERBOSE
SELECT * FROM ${case_db}.t_and_chain
WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5;
DROP TABLE ${case_db}.t_and_chain;
```

- [ ] **Step 3: 写 golden（`stats_overflow_saturation.sql`，q72 渲染代理）**

```sql
-- @tags=optimizer,stats,oq12
-- Cross join must render a finite, non-overflow estimate (no i64::MAX, no >=).
DROP TABLE IF EXISTS ${case_db}.t_x; DROP TABLE IF EXISTS ${case_db}.t_y;
CREATE TABLE ${case_db}.t_x (k INT);
CREATE TABLE ${case_db}.t_y (k INT);
INSERT INTO ${case_db}.t_x SELECT x FROM TABLE(generate_series(1, 100));
INSERT INTO ${case_db}.t_y SELECT x FROM TABLE(generate_series(1, 100));
-- @explain_not_contains=rows=>=
-- @explain_not_contains=9223372036854775807
EXPLAIN VERBOSE
SELECT * FROM ${case_db}.t_x, ${case_db}.t_y;
DROP TABLE ${case_db}.t_x; DROP TABLE ${case_db}.t_y;
```

- [ ] **Step 4: 写其余 5 个 golden**

- `stats_multikey_join_ndv.sql`：两表两键 join（`ON x.a=y.a AND x.b=y.b`），用确定数据（如各 1000 行、a/b 各 ndv≈100）；`-- @explain_contains=stats={rows=<阻尼后 pin 值>}`（先 record 取实际值再 pin）。
- `stats_or_selectivity.sql`：`WHERE a=1 OR a=2`，断言行数符合 inclusion-exclusion（pin）。
- `stats_outer_semi_anti_card.sql`：left outer / left semi / left anti 三条,断言 outer `>=` 左表行数;semi/anti 有界（`@explain_not_contains=stats={rows=1}` + pin）。
- `stats_aggregate_group_ndv.sql`：`GROUP BY a, b`,断言行数 `<= child*0.75` 且 `> 单键`（pin）。
- `stats_setop_rowcount.sql`：union all / union distinct / intersect / except 各一,pin 行数。

- [ ] **Step 5: record + verify**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer \
  --only stats_multikey_join_ndv,stats_or_selectivity,stats_outer_semi_anti_card,stats_aggregate_group_ndv,stats_setop_rowcount,stats_no_collapse_and_chain,stats_overflow_saturation \
  --mode record
# 人工核对 record 产物的行数确为合理值（非 1、非 >=1e15），再：
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --only <同上> --mode verify
```
Expected: 7 个 case verify 全过。

- [ ] **Step 6: Commit**

```bash
git add sql-tests/optimizer/
git commit -m "test(optimizer): synthetic goldens for stats/NDV robustness (OQ-12 P5)"
```

---

### Task 5.4: 全量回归 + 收尾

- [ ] **Step 1: 全量 optimizer 套件**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify
```
Expected: 全过。任何因 join 顺序/基数改善而变化的 plan-golden,逐个确认是改进后 `--mode record` 更新,并在 commit message 说明缘由。

- [ ] **Step 2: fmt + clippy + 单测**

Run: `cargo fmt && cargo clippy --all-targets && cargo test --lib estimate:: stats:: selectivity`
Expected: 无警告;全过。

- [ ] **Step 3: 清理**

确认 `DEFAULT_FILTER_SELECTIVITY`（stats.rs:18）已移除或集中到 `statistics.rs`;`estimate/mod.rs` 导出齐全;无 dead code。

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore(optimizer): regen plan goldens + cleanup after OQ-12 stats kernel"
```

---

## 验收对照（spec §7）

| spec 验收 | 对应 Task |
|---|---|
| q72 不再 i64::MAX | Task 0.2（sat_mul）+ 0.4（渲染）+ 1.x（join 饱和）+ 5.3 overflow golden |
| q85 不再 rows=1 collapse | Task 2.2（AND 阻尼）+ 2.3（floor 降 conf）+ 5.3 and_chain golden |
| q9/q20 量级（公式） | Task 1.1 多键阻尼 + 1.2–1.5 委托 + 4.1 scan stats + 5.3 multikey golden |
| 6 类 golden | Task 5.3（multikey/OR/outer-semi-anti/agg-group/setop/overflow）+ 单测 |
| confidence 可观测 | Task 0.1 + 0.3（贯穿）+ 5.1（EXPLAIN conf=）|
| `@explain_not_contains` | Task 5.2 |
| 漂移根除 | Task 1.1 内核 + 1.2–1.5 委托 + 1.6 漂移守卫 |
