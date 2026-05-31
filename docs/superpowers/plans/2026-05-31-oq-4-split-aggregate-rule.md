# OQ-4 SplitAggregateRule Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将普通非 DISTINCT 聚合的两阶段 SplitAgg 从 implementation rule 抽成独立 Cascades transformation,覆盖 grouped aggregate 与 scalar aggregate,让 CBO 在 Single 与 Local->Global 备选之间按 cost 选择。

**Architecture:** 新增逻辑聚合阶段元数据 `AggStage` 与 `SplitAggregateRule`:explore 阶段把符合条件的 `LogicalAggregate(Single)` 扩展为 `LogicalAggregate(Global) -> LogicalAggregate(Local) -> child` 逻辑备选,保留原始 Single 备选。`AggToHashAgg` 只做 stage-to-physical lowering;物理 property derivation 修正 Local scalar 输出分布,让 Global scalar 的 Gather requirement 正确触发 enforcer。

**Tech Stack:** Rust;Cascades optimizer (`src/sql/optimizer/cascades_rules/`、`operator.rs`、`derive/hash_aggregate.rs`);SQL golden (`sql-tests/optimizer/`);验证命令使用 `cargo test`、`cargo fmt`、optimizer SQL suite。

**Spec:** `docs/superpowers/specs/2026-05-31-oq-4-split-aggregate-rule-design.md`

---

## File Structure

- **Modify** `src/sql/optimizer/operator.rs` — 新增 `AggStage`;给 `LogicalAggregateOp` 增加 `stage`、`is_merge`、`is_split`;提供 `single` / `staged` 构造函数与 stage 到 `AggMode` 的映射。
- **Modify** `src/sql/optimizer/convert.rs` — analyzer plan 转 optimizer memo 时,所有普通聚合默认生成 `LogicalAggregateOp::single(...)`。
- **Create** `src/sql/optimizer/cascades_rules/split_aggregate.rs` — 独立 transformation rule,负责 eligible check、group key output-ref 重写、Local/Global 逻辑节点构造和规则单元测试。
- **Modify** `src/sql/optimizer/cascades_rules/mod.rs` — 注册 `split_aggregate` 模块,把 `SplitAggregateRule` 加入 transformation rules;保留 `SplitDistinctAgg` 在 implementation rules。
- **Modify** `src/sql/optimizer/cascades_rules/implement.rs` — 删除普通 SplitAgg physical alternative 生成逻辑;`AggToHashAgg` 只把当前 logical stage lowering 到一个 `PhysicalHashAggregateOp`。
- **Modify** `src/sql/optimizer/cascades_rules/split_distinct_agg.rs` — 所有测试与 helper 使用新的 `LogicalAggregateOp::single(...)`;不改变 DISTINCT 多阶段 implementation 语义。
- **Modify** `src/sql/optimizer/derive/hash_aggregate.rs` — `AggMode::Local` / `AggMode::DistinctLocal` 在 empty group key 时输出 `Any`,Single/Global/DistinctGlobal scalar 仍输出/要求 Gather。
- **Modify** `src/sql/optimizer/stats.rs` — 保持现有聚合 row-count 推导,增加 stage 不改变统计语义的单元测试覆盖。
- **Modify** `src/sql/optimizer/cost.rs` — 更新测试构造体以适配新增字段;保留 local cheaper than single 的 cost invariant。
- **Create** optimizer SQL cases:
  - `sql-tests/optimizer/sql/split_aggregate_grouped.sql`
  - `sql-tests/optimizer/sql/split_aggregate_scalar.sql`
  - `sql-tests/optimizer/sql/split_aggregate_disabled.sql`
  - corresponding generated files under `sql-tests/optimizer/result/`

---

## Task 1: Add logical aggregate stage metadata

**Files:**
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/convert.rs`
- Modify: all existing `LogicalAggregateOp { ... }` test literals in `src/sql/optimizer/cascades_rules/implement.rs` and `src/sql/optimizer/cascades_rules/split_distinct_agg.rs`

- [ ] **Step 1: Write failing tests for aggregate stage constructors**

Add this test module at the end of `src/sql/optimizer/operator.rs`:

```rust
#[cfg(test)]
mod aggregate_stage_tests {
    use super::*;
    use crate::sql::analysis::{AggregateCall, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn count_call() -> AggregateCall {
        AggregateCall {
            func: "count".to_string(),
            args: vec![col_ref(2, "v")],
            distinct: false,
            output_name: "count(v)".to_string(),
            output_column_id: ColumnId(3),
            return_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    #[test]
    fn single_constructor_sets_unsplit_single_metadata() {
        let op = LogicalAggregateOp::single(
            vec![col_ref(1, "k")],
            vec![count_call()],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
        );
        assert_eq!(op.stage, AggStage::Single);
        assert_eq!(op.is_merge, vec![false]);
        assert!(!op.is_split);
    }

    #[test]
    fn staged_constructor_preserves_merge_flags_and_split_marker() {
        let op = LogicalAggregateOp::staged(
            AggStage::Global,
            vec![col_ref(1, "k")],
            vec![count_call()],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
            vec![true],
            true,
        );
        assert_eq!(op.stage, AggStage::Global);
        assert_eq!(op.stage.to_physical_mode(), AggMode::Global);
        assert_eq!(op.is_merge, vec![true]);
        assert!(op.is_split);
    }
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test -q sql::optimizer::operator::aggregate_stage_tests
```

Expected: compilation fails with missing `AggStage`, `LogicalAggregateOp::single`, `LogicalAggregateOp::staged`, and `to_physical_mode`.

- [ ] **Step 3: Add `AggStage` and constructors**

In `src/sql/optimizer/operator.rs`, replace the existing `LogicalAggregateOp` definition with:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggStage {
    Single,
    Local,
    Global,
}

impl AggStage {
    pub(crate) fn to_physical_mode(self) -> AggMode {
        match self {
            AggStage::Single => AggMode::Single,
            AggStage::Local => AggMode::Local,
            AggStage::Global => AggMode::Global,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalAggregateOp {
    pub(crate) stage: AggStage,
    pub(crate) group_by: Vec<TypedExpr>,
    pub(crate) aggregates: Vec<AggregateCall>,
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) is_merge: Vec<bool>,
    pub(crate) is_split: bool,
}

impl LogicalAggregateOp {
    pub(crate) fn single(
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
    ) -> Self {
        let is_merge = vec![false; aggregates.len()];
        Self {
            stage: AggStage::Single,
            group_by,
            aggregates,
            output_columns,
            is_merge,
            is_split: false,
        }
    }

    pub(crate) fn staged(
        stage: AggStage,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
        is_merge: Vec<bool>,
        is_split: bool,
    ) -> Self {
        debug_assert_eq!(aggregates.len(), is_merge.len());
        Self {
            stage,
            group_by,
            aggregates,
            output_columns,
            is_merge,
            is_split,
        }
    }
}
```

- [ ] **Step 4: Update conversion from analyzer logical plan**

In `src/sql/optimizer/convert.rs`, replace the aggregate operator construction:

```rust
let op = Operator::LogicalAggregate(LogicalAggregateOp::single(
    agg.group_by,
    agg.aggr_exprs,
    agg.output_columns,
));
```

- [ ] **Step 5: Update existing test literals**

Replace every test-only literal shaped as:

```rust
Operator::LogicalAggregate(LogicalAggregateOp {
    group_by,
    aggregates,
    output_columns,
})
```

with:

```rust
Operator::LogicalAggregate(LogicalAggregateOp::single(
    group_by,
    aggregates,
    output_columns,
))
```

Apply this to `src/sql/optimizer/cascades_rules/implement.rs` and `src/sql/optimizer/cascades_rules/split_distinct_agg.rs`. Do not change production DISTINCT rule behavior in this task.

- [ ] **Step 6: Run stage metadata tests**

Run:

```bash
cargo test -q sql::optimizer::operator::aggregate_stage_tests
```

Expected: tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/operator.rs src/sql/optimizer/convert.rs src/sql/optimizer/cascades_rules/implement.rs src/sql/optimizer/cascades_rules/split_distinct_agg.rs
git commit -m "OQ-4: add aggregate stage metadata"
```

---

## Task 2: Implement `SplitAggregateRule` as a transformation rule

**Files:**
- Create: `src/sql/optimizer/cascades_rules/split_aggregate.rs`
- Modify: `src/sql/optimizer/cascades_rules/mod.rs`

- [ ] **Step 1: Write failing unit tests for grouped, scalar, and guarded split**

Create `src/sql/optimizer/cascades_rules/split_aggregate.rs` with this initial test-focused skeleton:

```rust
use crate::sql::optimizer::cascades_rules::{Rule, RuleContext};
use crate::sql::optimizer::memo::NewExpr;
use crate::sql::optimizer::operator::Operator;

pub(crate) struct SplitAggregateRule;

impl Rule for SplitAggregateRule {
    fn name(&self) -> &'static str {
        "SplitAggregateRule"
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(_))
    }

    fn apply(&self, _expr: &NewExpr, _ctx: &RuleContext) -> Vec<NewExpr> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{AggregateCall, ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp};

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn count_call(distinct: bool) -> AggregateCall {
        AggregateCall {
            func: "count".to_string(),
            args: vec![col_ref(2, "v")],
            distinct,
            output_name: "count(v)".to_string(),
            output_column_id: ColumnId(3),
            return_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn single_grouped_expr() -> NewExpr {
        NewExpr {
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col_ref(1, "k")],
                vec![count_call(false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![0],
        }
    }

    fn single_scalar_expr() -> NewExpr {
        NewExpr {
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![],
                vec![count_call(false)],
                vec![output_column(3, "count(v)")],
            )),
            children: vec![0],
        }
    }

    #[test]
    fn splits_grouped_aggregate_into_global_over_local() {
        let out = SplitAggregateRule.apply(&single_grouped_expr(), &RuleContext::default());
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert_eq!(global.is_merge, vec![true]);
        assert!(global.is_split);
        assert_eq!(out[0].children.len(), 1);
        let Operator::LogicalAggregate(local) = &out[0].children[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert_eq!(local.is_merge, vec![false]);
        assert!(local.is_split);
    }

    #[test]
    fn splits_scalar_aggregate() {
        let out = SplitAggregateRule.apply(&single_scalar_expr(), &RuleContext::default());
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert!(global.group_by.is_empty());
        let Operator::LogicalAggregate(local) = &out[0].children[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert!(local.group_by.is_empty());
    }

    #[test]
    fn rejects_distinct_and_already_split_aggregate() {
        let distinct = NewExpr {
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col_ref(1, "k")],
                vec![count_call(true)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![0],
        };
        assert!(SplitAggregateRule
            .apply(&distinct, &RuleContext::default())
            .is_empty());

        let already_split = NewExpr {
            op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Local,
                vec![col_ref(1, "k")],
                vec![count_call(false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
                vec![false],
                true,
            )),
            children: vec![0],
        };
        assert!(SplitAggregateRule
            .apply(&already_split, &RuleContext::default())
            .is_empty());
    }
}
```

- [ ] **Step 2: Run the failing split rule tests**

Run:

```bash
cargo test -q sql::optimizer::cascades_rules::split_aggregate
```

Expected: compilation fails until the module is registered in `mod.rs`; after registration, the first two tests fail because `apply` returns no alternatives.

- [ ] **Step 3: Register the new module**

In `src/sql/optimizer/cascades_rules/mod.rs`, add:

```rust
pub(crate) mod split_aggregate;
```

Add the rule to `all_transformation_rules()`:

```rust
Box::new(split_aggregate::SplitAggregateRule),
```

Do not add it to `all_implementation_rules()`.

- [ ] **Step 4: Implement the transformation**

Replace the skeleton body in `src/sql/optimizer/cascades_rules/split_aggregate.rs` with:

```rust
use crate::sql::analysis::{AggregateCall, ExprKind, OutputColumn, TypedExpr};
use crate::sql::optimizer::cascades_rules::{Rule, RuleContext};
use crate::sql::optimizer::memo::NewExpr;
use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, Operator};

pub(crate) struct SplitAggregateRule;

impl Rule for SplitAggregateRule {
    fn name(&self) -> &'static str {
        "SplitAggregateRule"
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(_))
    }

    fn apply(&self, expr: &NewExpr, _ctx: &RuleContext) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(agg) = &expr.op else {
            return Vec::new();
        };
        if !is_eligible(agg) {
            return Vec::new();
        }

        let local = LogicalAggregateOp::staged(
            AggStage::Local,
            agg.group_by.clone(),
            agg.aggregates.clone(),
            agg.output_columns.clone(),
            vec![false; agg.aggregates.len()],
            true,
        );
        let global_group_by = aggregate_group_key_output_ref(&agg.group_by, &agg.output_columns);
        let global = LogicalAggregateOp::staged(
            AggStage::Global,
            global_group_by,
            agg.aggregates.clone(),
            agg.output_columns.clone(),
            vec![true; agg.aggregates.len()],
            true,
        );

        vec![NewExpr {
            op: Operator::LogicalAggregate(global),
            children: vec![NewExpr {
                op: Operator::LogicalAggregate(local),
                children: expr.children.clone(),
            }],
        }]
    }
}

fn is_eligible(agg: &LogicalAggregateOp) -> bool {
    agg.stage == AggStage::Single
        && !agg.is_split
        && agg.is_merge.iter().all(|flag| !*flag)
        && !agg.aggregates.is_empty()
        && agg.aggregates.iter().all(is_splittable_aggregate)
}

fn is_splittable_aggregate(call: &AggregateCall) -> bool {
    !call.distinct
        && matches!(
            call.func.to_ascii_lowercase().as_str(),
            "sum" | "min" | "max" | "count"
        )
}

fn aggregate_group_key_output_ref(
    group_by: &[TypedExpr],
    output_columns: &[OutputColumn],
) -> Vec<TypedExpr> {
    group_by
        .iter()
        .zip(output_columns.iter())
        .map(|(expr, output)| TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: output.column_id,
                qualifier: None,
                column: output.name.clone(),
            },
            data_type: output.data_type.clone(),
            nullable: expr.nullable,
        })
        .collect()
}
```

- [ ] **Step 5: Run split rule tests**

Run:

```bash
cargo test -q sql::optimizer::cascades_rules::split_aggregate
```

Expected: grouped/scalar split tests pass; DISTINCT and already-split guards pass.

- [ ] **Step 6: Verify rule-name disable registry recognizes it**

Run:

```bash
cargo test -q sql::optimizer::cascades_rules::tests::known_rule_names_include_all_registered_rules
```

Expected: pass. If this exact test name does not exist, run:

```bash
cargo test -q sql::optimizer::cascades_rules
```

Expected: pass, including rule registration tests.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/cascades_rules/mod.rs src/sql/optimizer/cascades_rules/split_aggregate.rs
git commit -m "OQ-4: add split aggregate transformation rule"
```

---

## Task 3: Simplify `AggToHashAgg` to stage-to-physical lowering

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/optimizer/cascades_rules/split_distinct_agg.rs` only when tests need constructor updates from Task 1

- [ ] **Step 1: Write failing lowering tests**

In `src/sql/optimizer/cascades_rules/implement.rs`, add these tests to the existing `#[cfg(test)]` module:

```rust
#[test]
fn agg_to_hash_agg_lowers_single_to_one_physical_single() {
    let expr = NewExpr {
        op: Operator::LogicalAggregate(LogicalAggregateOp::single(
            vec![col_ref(1, "k")],
            vec![count_call(false)],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
        )),
        children: vec![0],
    };

    let out = AggToHashAgg.apply(&expr, &RuleContext::default());
    assert_eq!(out.len(), 1);
    let Operator::PhysicalHashAggregate(op) = &out[0].op else {
        panic!("expected physical hash aggregate");
    };
    assert_eq!(op.mode, AggMode::Single);
    assert_eq!(op.is_merge, vec![false]);
    assert_eq!(out[0].children, vec![0]);
}

#[test]
fn agg_to_hash_agg_lowers_split_stages_without_creating_extra_alternatives() {
    let local_expr = NewExpr {
        op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
            AggStage::Local,
            vec![col_ref(1, "k")],
            vec![count_call(false)],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
            vec![false],
            true,
        )),
        children: vec![0],
    };
    let local_out = AggToHashAgg.apply(&local_expr, &RuleContext::default());
    assert_eq!(local_out.len(), 1);
    let Operator::PhysicalHashAggregate(local) = &local_out[0].op else {
        panic!("expected local physical aggregate");
    };
    assert_eq!(local.mode, AggMode::Local);
    assert_eq!(local.is_merge, vec![false]);

    let global_expr = NewExpr {
        op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
            AggStage::Global,
            vec![col_ref(1, "k")],
            vec![count_call(false)],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
            vec![true],
            true,
        )),
        children: vec![1],
    };
    let global_out = AggToHashAgg.apply(&global_expr, &RuleContext::default());
    assert_eq!(global_out.len(), 1);
    let Operator::PhysicalHashAggregate(global) = &global_out[0].op else {
        panic!("expected global physical aggregate");
    };
    assert_eq!(global.mode, AggMode::Global);
    assert_eq!(global.is_merge, vec![true]);
    assert_eq!(global_out[0].children, vec![1]);
}
```

The helper functions `col_ref`, `count_call`, and `output_column` should already exist in the module from current aggregate implementation tests. If any helper name differs, add the same helper definitions from Task 2 test code inside the implement.rs test module.

- [ ] **Step 2: Run failing lowering tests**

Run:

```bash
cargo test -q sql::optimizer::cascades_rules::implement::tests::agg_to_hash_agg
```

Expected: the single test fails because current `AggToHashAgg` still emits Local/Global alternatives for grouped non-DISTINCT aggregate.

- [ ] **Step 3: Replace `AggToHashAgg::apply`**

In `src/sql/optimizer/cascades_rules/implement.rs`, replace the current `AggToHashAgg::apply` body with:

```rust
fn apply(&self, expr: &NewExpr, _ctx: &RuleContext) -> Vec<NewExpr> {
    let Operator::LogicalAggregate(op) = &expr.op else {
        return Vec::new();
    };
    vec![NewExpr {
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: op.stage.to_physical_mode(),
            group_by: op.group_by.clone(),
            aggregates: op.aggregates.clone(),
            output_columns: op.output_columns.clone(),
            is_merge: op.is_merge.clone(),
        }),
        children: expr.children.clone(),
    }]
}
```

Delete the old `aggregate_group_key_output_ref` helper from `implement.rs` after `AggToHashAgg` no longer uses it.

- [ ] **Step 4: Keep DISTINCT implementation rule independent**

Run:

```bash
cargo test -q sql::optimizer::cascades_rules::split_distinct_agg
```

Expected: pass. `SplitDistinctAgg` still emits its existing physical DISTINCT chains and is not converted into a transformation in OQ-4.

- [ ] **Step 5: Run lowering tests**

Run:

```bash
cargo test -q sql::optimizer::cascades_rules::implement::tests::agg_to_hash_agg_lowers_single_to_one_physical_single
cargo test -q sql::optimizer::cascades_rules::implement::tests::agg_to_hash_agg_lowers_split_stages_without_creating_extra_alternatives
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/cascades_rules/implement.rs src/sql/optimizer/cascades_rules/split_distinct_agg.rs
git commit -m "OQ-4: lower aggregate stages to physical hash aggregate"
```

---

## Task 4: Fix Local scalar aggregate physical properties

**Files:**
- Modify: `src/sql/optimizer/derive/hash_aggregate.rs`

- [ ] **Step 1: Write failing property tests**

Add these tests to `src/sql/optimizer/derive/hash_aggregate.rs` test module:

```rust
#[test]
fn local_scalar_aggregate_outputs_any_distribution() {
    let op = PhysicalHashAggregateOp {
        mode: AggMode::Local,
        group_by: vec![],
        aggregates: vec![],
        output_columns: vec![],
        is_merge: vec![],
    };
    let props = op.derive_output(&[]);
    assert!(matches!(props.distribution, DistributionSpec::Any));
}

#[test]
fn global_scalar_aggregate_outputs_and_requires_gather() {
    let op = PhysicalHashAggregateOp {
        mode: AggMode::Global,
        group_by: vec![],
        aggregates: vec![],
        output_columns: vec![],
        is_merge: vec![],
    };
    let out = op.derive_output(&[]);
    assert!(matches!(out.distribution, DistributionSpec::Gather));

    let reqs = op.derive_required(&PhysicalPropertySet::any(), 1);
    assert_eq!(reqs.len(), 1);
    assert!(matches!(reqs[0].distribution, DistributionSpec::Gather));
}

#[test]
fn single_scalar_aggregate_keeps_gather_output_and_requirement() {
    let op = PhysicalHashAggregateOp {
        mode: AggMode::Single,
        group_by: vec![],
        aggregates: vec![],
        output_columns: vec![],
        is_merge: vec![],
    };
    let out = op.derive_output(&[]);
    assert!(matches!(out.distribution, DistributionSpec::Gather));

    let reqs = op.derive_required(&PhysicalPropertySet::any(), 1);
    assert_eq!(reqs.len(), 1);
    assert!(matches!(reqs[0].distribution, DistributionSpec::Gather));
}
```

- [ ] **Step 2: Run failing property tests**

Run:

```bash
cargo test -q sql::optimizer::derive::hash_aggregate::tests::local_scalar_aggregate_outputs_any_distribution
```

Expected: fails because current `derive_output` returns Gather when `cols.is_empty()`.

- [ ] **Step 3: Update `derive_output`**

In `src/sql/optimizer/derive/hash_aggregate.rs`, replace:

```rust
if cols.is_empty() {
    PhysicalPropertySet::gather()
} else {
    PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_agg(cols),
        ordering: OrderingSpec::Any,
    }
}
```

with:

```rust
if cols.is_empty() {
    match self.mode {
        AggMode::Local | AggMode::DistinctLocal => PhysicalPropertySet::any(),
        AggMode::Single | AggMode::Global | AggMode::DistinctGlobal => {
            PhysicalPropertySet::gather()
        }
    }
} else {
    PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_agg(cols),
        ordering: OrderingSpec::Any,
    }
}
```

Do not change `derive_required`: Local and DistinctLocal already require Any; Global and DistinctGlobal scalar already require Gather.

- [ ] **Step 4: Run property tests**

Run:

```bash
cargo test -q sql::optimizer::derive::hash_aggregate
```

Expected: all hash aggregate property tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/derive/hash_aggregate.rs
git commit -m "OQ-4: keep local scalar aggregate distributed"
```

---

## Task 5: Preserve stats and cost invariants for split alternatives

**Files:**
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/cost.rs`

- [ ] **Step 1: Add stats stage-invariance test**

In `src/sql/optimizer/stats.rs`, add this test to the existing `#[cfg(test)]` module:

```rust
#[test]
fn aggregate_stats_are_independent_of_split_stage_metadata() {
    use crate::sql::analysis::{AggregateCall, ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::{Memo, NewExpr};
    use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, Operator};

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn count_call() -> AggregateCall {
        AggregateCall {
            func: "count".to_string(),
            args: vec![col_ref(2, "v")],
            distinct: false,
            output_name: "count(v)".to_string(),
            output_column_id: ColumnId(3),
            return_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    let child_group = 0;
    let single = NewExpr {
        op: Operator::LogicalAggregate(LogicalAggregateOp::single(
            vec![col_ref(1, "k")],
            vec![count_call()],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
        )),
        children: vec![child_group],
    };
    let local = NewExpr {
        op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
            AggStage::Local,
            vec![col_ref(1, "k")],
            vec![count_call()],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
            vec![false],
            true,
        )),
        children: vec![child_group],
    };

    let mut memo = Memo::new_for_test();
    memo.groups[child_group].statistics.output_row_count = 10_000.0;
    memo.groups[child_group]
        .statistics
        .column_statistics
        .insert("k".to_string(), ColumnStatistic {
            ndv: Some(100.0),
            null_count: Some(0.0),
            min_value: None,
            max_value: None,
            avg_size: None,
        });

    let single_stats = derive_statistics_for_expr(&memo, &single);
    let local_stats = derive_statistics_for_expr(&memo, &local);
    assert_eq!(single_stats.output_row_count, local_stats.output_row_count);
}
```

If helper constructors such as `Memo::new_for_test` or `derive_statistics_for_expr` have different names, use the equivalent test helpers already present in `stats.rs`; keep the assertion that Single and Local with the same group keys derive identical row counts.

- [ ] **Step 2: Update cost tests for struct changes**

In `src/sql/optimizer/cost.rs`, keep the existing `local_agg_cheaper_than_single` test and update any `PhysicalHashAggregateOp` literal to include all fields required after Task 1. The expected invariant remains:

```rust
assert!(compute_cost(&single, &own, &cs) > compute_cost(&local, &own, &cs));
```

- [ ] **Step 3: Run stats and cost tests**

Run:

```bash
cargo test -q sql::optimizer::stats::tests::aggregate_stats_are_independent_of_split_stage_metadata
cargo test -q sql::optimizer::cost::tests::local_agg_cheaper_than_single
```

Expected: both pass.

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/stats.rs src/sql/optimizer/cost.rs
git commit -m "OQ-4: preserve split aggregate stats and cost invariants"
```

---

## Task 6: Add optimizer SQL golden coverage

**Files:**
- Create: `sql-tests/optimizer/sql/split_aggregate_grouped.sql`
- Create: `sql-tests/optimizer/sql/split_aggregate_scalar.sql`
- Create: `sql-tests/optimizer/sql/split_aggregate_disabled.sql`
- Create or update generated result files under `sql-tests/optimizer/result/`

- [ ] **Step 1: Add grouped SplitAgg SQL case**

Create `sql-tests/optimizer/sql/split_aggregate_grouped.sql`:

```sql
-- OQ-4: grouped non-DISTINCT aggregate can use two-phase hash aggregate.

CREATE TABLE ${case_db}.t_split_agg_grouped (k INT, v INT);
INSERT INTO ${case_db}.t_split_agg_grouped VALUES
    (1, 10), (1, 20), (1, 30),
    (2, 5),  (2, 15), (2, 25),
    (3, 7),  (3, 11), (3, 13),
    (4, 1),  (4, 2),  (4, 3);
ANALYZE TABLE ${case_db}.t_split_agg_grouped;

-- @explain_contains=PhysicalHashAggregate[LOCAL]
-- @explain_contains=PhysicalHashAggregate[GLOBAL]
SELECT k, SUM(v) AS s
FROM ${case_db}.t_split_agg_grouped
GROUP BY k
ORDER BY k;
```

- [ ] **Step 2: Add scalar SplitAgg SQL case**

Create `sql-tests/optimizer/sql/split_aggregate_scalar.sql`:

```sql
-- OQ-4: scalar aggregate over join can use Local->Global aggregate.

CREATE TABLE ${case_db}.t_split_agg_l (k INT, payload INT);
CREATE TABLE ${case_db}.t_split_agg_r (k INT, payload INT);
INSERT INTO ${case_db}.t_split_agg_l VALUES
    (1, 10), (1, 11), (2, 20), (2, 21), (3, 30), (4, 40);
INSERT INTO ${case_db}.t_split_agg_r VALUES
    (1, 100), (1, 101), (2, 200), (5, 500);
ANALYZE TABLE ${case_db}.t_split_agg_l;
ANALYZE TABLE ${case_db}.t_split_agg_r;

-- @explain_contains=PhysicalHashAggregate[LOCAL]
-- @explain_contains=PhysicalHashAggregate[GLOBAL]
SELECT COUNT(r.payload) AS cnt
FROM ${case_db}.t_split_agg_l AS l
JOIN ${case_db}.t_split_agg_r AS r
  ON l.k = r.k;
```

- [ ] **Step 3: Add disabled-rule SQL case**

Create `sql-tests/optimizer/sql/split_aggregate_disabled.sql`:

```sql
-- OQ-4: disabling SplitAggregateRule keeps ordinary aggregate lowering single-phase.

CREATE TABLE ${case_db}.t_split_agg_disabled (k INT, v INT);
INSERT INTO ${case_db}.t_split_agg_disabled VALUES
    (1, 10), (1, 20), (2, 30), (2, 40), (3, 50), (3, 60);
ANALYZE TABLE ${case_db}.t_split_agg_disabled;

SET disable_optimizer_rules = 'SplitAggregateRule';

-- @result_not_contains=PhysicalHashAggregate[LOCAL]
-- @result_not_contains=PhysicalHashAggregate[GLOBAL]
EXPLAIN VERBOSE
SELECT k, SUM(v) AS s
FROM ${case_db}.t_split_agg_disabled
GROUP BY k
ORDER BY k;

SET disable_optimizer_rules = '';
```

- [ ] **Step 4: Prepare local SQL test environment**

Run:

```bash
docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
cargo build --profile dev-opt
```

Expected: runtime env exists and build succeeds.

- [ ] **Step 5: Start standalone server for SQL tests**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
LOG=/tmp/novarocks-oq4-splitagg.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timed out waiting for NOVAROCKS_READY" >&2; kill -9 "$SRV_PID"; exit 1; }
```

Expected: log contains `NOVAROCKS_READY mysql_port=...`.

- [ ] **Step 6: Record optimizer golden output for the three cases**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only split_aggregate_grouped,split_aggregate_scalar,split_aggregate_disabled \
  --mode record
```

Expected: runner succeeds and writes corresponding result files under `sql-tests/optimizer/result/`.

- [ ] **Step 7: Verify optimizer golden output**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only split_aggregate_grouped,split_aggregate_scalar,split_aggregate_disabled \
  --mode verify
```

Expected: all three cases pass. If the disabled case fails because `EXPLAIN VERBOSE` text uses a different physical aggregate label, update only the exact `@result_not_contains` substrings to match `src/sql/explain.rs` formatting and rerun record + verify.

- [ ] **Step 8: Stop standalone server**

Run:

```bash
kill "$SRV_PID"
wait "$SRV_PID" 2>/dev/null || true
```

Expected: the process exits.

- [ ] **Step 9: Commit**

```bash
git add sql-tests/optimizer/sql/split_aggregate_grouped.sql sql-tests/optimizer/sql/split_aggregate_scalar.sql sql-tests/optimizer/sql/split_aggregate_disabled.sql sql-tests/optimizer/result/
git commit -m "OQ-4: add split aggregate optimizer coverage"
```

---

## Task 7: Full validation and roadmap handoff

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md` if the implementation branch is complete and validated

- [ ] **Step 1: Run Rust formatting**

Run:

```bash
cargo fmt
```

Expected: completes with no output. If files changed, include them in the final validation commit.

- [ ] **Step 2: Run targeted optimizer tests**

Run:

```bash
cargo test -q sql::optimizer::operator::aggregate_stage_tests
cargo test -q sql::optimizer::cascades_rules::split_aggregate
cargo test -q sql::optimizer::cascades_rules::implement::tests::agg_to_hash_agg_lowers_single_to_one_physical_single
cargo test -q sql::optimizer::cascades_rules::implement::tests::agg_to_hash_agg_lowers_split_stages_without_creating_extra_alternatives
cargo test -q sql::optimizer::derive::hash_aggregate
cargo test -q sql::optimizer::cost::tests::local_agg_cheaper_than_single
```

Expected: all commands pass.

- [ ] **Step 3: Run full Rust optimizer test module**

Run:

```bash
cargo test -q sql::optimizer
```

Expected: pass.

- [ ] **Step 4: Run full optimizer SQL suite**

With standalone server from Task 6 running, run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --mode verify
```

Expected: optimizer suite passes.

- [ ] **Step 5: Inspect final diff**

Run:

```bash
git --no-pager diff --stat origin/main...HEAD
git --no-pager diff --check
```

Expected: diff contains only OQ-4 design/implementation/test files; `diff --check` reports no whitespace errors.

- [ ] **Step 6: Update Roadmap OQ-4 status**

In `/Users/harbor/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md`, update the OQ-4 entry from pending/in-progress wording to completed wording with this evidence sentence:

```markdown
OQ-4 SplitAgg implemented as a Cascades transformation (`SplitAggregateRule`), with grouped/scalar Local->Global coverage, rule-disable coverage, and optimizer SQL golden verification.
```

Do not copy repository code into the Obsidian document.

- [ ] **Step 7: Commit validation cleanup and roadmap note if repository files changed**

If `cargo fmt` changed repository files, commit them:

```bash
git add src sql-tests
git commit -m "OQ-4: finalize split aggregate validation"
```

If only the external Roadmap changed, do not commit it from the repository worktree.

---

## Definition of Done

- `SplitAggregateRule` is a transformation rule and appears in `all_transformation_rules()`.
- `AggToHashAgg` emits exactly one physical hash aggregate per logical aggregate stage.
- Ordinary non-DISTINCT grouped aggregate can choose `PhysicalHashAggregate[LOCAL] -> PhysicalHashAggregate[GLOBAL]`.
- Ordinary non-DISTINCT scalar aggregate can choose `PhysicalHashAggregate[LOCAL] -> Gather enforcer -> PhysicalHashAggregate[GLOBAL]`.
- DISTINCT aggregation continues to use existing `SplitDistinctAgg` implementation path.
- `SET disable_optimizer_rules = 'SplitAggregateRule'` prevents the ordinary split alternative while leaving single-phase aggregate available.
- Targeted Rust tests, optimizer module tests, and optimizer SQL suite pass.
