# Distributed TopN via MERGING-EXCHANGE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit two-stage distributed TopN (`PARTIAL` per fragment → MERGING-EXCHANGE → `FINAL` on coordinator) as a cascades alternative to single-stage `PhysicalTopN`, matching StarRocks FE's `SplitTopNRule` + `PlanFragmentBuilder.buildFinalTopNFragment` pattern.

**Architecture:** New `TopNPhase` enum + `is_split` flag on `LogicalTopNOp`/`PhysicalTopNOp`. Transformation rule `SplitTopN` emits the two-stage alternative. Fragment builder's `visit_physical_top_n` splits into three branches: single-stage (today), PARTIAL (just a SORT_NODE), and FINAL+split (creates fragment boundary, coordinator fragment contains an EXCHANGE_NODE with `sort_info` + `offset` + `limit` — no separate final SORT_NODE). Runtime already supports merging-receive via `src/lower/node/exchange.rs:105-140`.

**Tech Stack:** Rust, Cascades optimizer, Thrift plan nodes, StarRocks execution layer.

**Spec:** `docs/design/specs/2026-04-17-distributed-top-n-merge-exchange-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/sql/optimizer/operator.rs` | Modify (lines 17-22, 81-85, 209-213, 299, 321) | Add `TopNPhase` enum; extend `LogicalTopNOp` and `PhysicalTopNOp` with `phase` + `is_split` |
| `src/sql/optimizer/rules/sort_limit_to_top_n.rs:57` | Modify | Set defaults on LogicalTopNOp constructor |
| `src/sql/optimizer/rules/split_top_n.rs` | Create | `SplitTopN` transformation rule |
| `src/sql/optimizer/rules/mod.rs:36-44` | Modify | Register `SplitTopN` in `all_transformation_rules()`; add module declaration |
| `src/sql/optimizer/rules/implement.rs:794,1191` | Modify | `TopNToPhysical` passes phase/is_split through; test update |
| `src/sql/optimizer/search.rs:350-371, 496` | Modify | Distribution contracts for PARTIAL and FINAL+split |
| `src/sql/optimizer/search.rs:967, 980` | Modify | Update test constructors |
| `src/sql/optimizer/cost.rs:313, 333, 348` | Modify | Update test constructors |
| `src/sql/codegen/nodes.rs:475-497` | Modify | Extend `build_exchange_node` to accept optional `sort_info` + `offset` |
| `src/sql/codegen/fragment_builder.rs:994-1069` | Modify | Three-branch `visit_physical_top_n` |

---

### Task 1: Add `TopNPhase` enum and extend op structs

**Files:**
- Modify: `src/sql/optimizer/operator.rs`

- [ ] **Step 1: Add `TopNPhase` enum after `AggMode`**

Insert after line 30 in `src/sql/optimizer/operator.rs`:

```rust
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TopNPhase {
    Partial,
    Final,
}

impl Default for TopNPhase {
    fn default() -> Self {
        TopNPhase::Final
    }
}
```

- [ ] **Step 2: Extend `LogicalTopNOp` (line 80-85)**

Replace:

```rust
#[derive(Clone, Debug)]
pub(crate) struct LogicalTopNOp {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}
```

With:

```rust
#[derive(Clone, Debug)]
pub(crate) struct LogicalTopNOp {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}
```

- [ ] **Step 3: Extend `PhysicalTopNOp` (line 208-213)**

Replace:

```rust
#[derive(Clone, Debug)]
pub(crate) struct PhysicalTopNOp {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}
```

With:

```rust
#[derive(Clone, Debug)]
pub(crate) struct PhysicalTopNOp {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}
```

- [ ] **Step 4: Update constructor at `src/sql/optimizer/rules/sort_limit_to_top_n.rs:57`**

Replace:

```rust
                op: Operator::LogicalTopN(LogicalTopNOp {
                    items: sort_op.items.clone(),
                    limit: limit_op.limit,
                    offset: limit_op.offset,
                }),
```

With:

```rust
                op: Operator::LogicalTopN(LogicalTopNOp {
                    items: sort_op.items.clone(),
                    limit: limit_op.limit,
                    offset: limit_op.offset,
                    phase: crate::sql::optimizer::operator::TopNPhase::Final,
                    is_split: false,
                }),
```

- [ ] **Step 5: Update `TopNToPhysical` at `src/sql/optimizer/rules/implement.rs:793-799`**

Replace:

```rust
        vec![NewExpr {
            op: Operator::PhysicalTopN(PhysicalTopNOp {
                items: op.items.clone(),
                limit: op.limit,
                offset: op.offset,
            }),
            children: expr.children.clone(),
        }]
```

With:

```rust
        vec![NewExpr {
            op: Operator::PhysicalTopN(PhysicalTopNOp {
                items: op.items.clone(),
                limit: op.limit,
                offset: op.offset,
                phase: op.phase,
                is_split: op.is_split,
            }),
            children: expr.children.clone(),
        }]
```

- [ ] **Step 6: Update test constructor in `implement.rs` around line 1191**

Find the `LogicalTopNOp { ... }` in the test module. Replace with the same struct adding `phase: TopNPhase::Final, is_split: false`. Add `use crate::sql::optimizer::operator::TopNPhase;` to the test's imports if not present.

- [ ] **Step 7: Update test constructors in `search.rs` lines 967 and 980**

Each has `PhysicalTopNOp { items: ..., limit: ..., offset: ... }`. Add `phase: TopNPhase::Final, is_split: false,` to each. Add `TopNPhase` import if needed.

- [ ] **Step 8: Update test constructors in `cost.rs` lines 313, 333, 348**

Same treatment: add `phase: TopNPhase::Final, is_split: false,` to each `PhysicalTopNOp { ... }`. Add `TopNPhase` import if needed.

- [ ] **Step 9: Verify build**

Run: `cargo build 2>&1 | tail -10`
Expected: no errors.

- [ ] **Step 10: Run all tests**

Run: `cargo test --lib 2>&1 | tail -15`
Expected: all previously-passing tests still pass (field additions don't change behavior).

- [ ] **Step 11: Commit**

```bash
git add src/sql/optimizer/operator.rs src/sql/optimizer/rules/sort_limit_to_top_n.rs \
        src/sql/optimizer/rules/implement.rs src/sql/optimizer/search.rs \
        src/sql/optimizer/cost.rs
git commit -m "refactor: add TopNPhase + is_split to LogicalTopNOp/PhysicalTopNOp"
```

---

### Task 2: `SplitTopN` transformation rule

**Files:**
- Create: `src/sql/optimizer/rules/split_top_n.rs`
- Modify: `src/sql/optimizer/rules/mod.rs`

- [ ] **Step 1: Write failing tests**

Create `src/sql/optimizer/rules/split_top_n.rs`:

```rust
//! Transformation rule: LogicalTopN(FINAL, !split) -> LogicalTopN(FINAL, split=true) over LogicalTopN(PARTIAL).
//!
//! Mirrors StarRocks's SplitTopNRule.java. Cost search picks between the
//! single-stage TopN (original) and this two-stage alternative.

use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalTopNOp, Operator, TopNPhase};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};

pub(crate) struct SplitTopN;

impl Rule for SplitTopN {
    fn name(&self) -> &str {
        "SplitTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalTopN(t) if t.phase == TopNPhase::Final && !t.is_split
        )
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(src) = &expr.op else {
            return vec![];
        };
        // Finite limit required; plain ORDER BY without LIMIT is out of scope.
        let limit = match src.limit {
            Some(l) if l >= 0 => l,
            _ => return vec![],
        };
        let offset = src.offset.unwrap_or(0).max(0);
        // Saturating add: if L+O would overflow, cap at i64::MAX (effectively
        // means "partial passes everything through"; cost search will prefer
        // single-stage in that corner case).
        let partial_limit = limit.saturating_add(offset);

        // PARTIAL child: same sort items, larger limit, zero offset.
        let partial_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: src.items.clone(),
                limit: Some(partial_limit),
                offset: Some(0),
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: expr.children.clone(),
        };
        let partial_group = memo.new_group(partial_mexpr);

        // FINAL with split flag, original limit/offset. Child = new partial group.
        let final_expr = NewExpr {
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: src.items.clone(),
                limit: src.limit,
                offset: src.offset,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![partial_group],
        };

        vec![final_expr]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::optimizer::operator::{LogicalScanOp, LogicalTopNOp};

    fn mk_scan_group(memo: &mut Memo) -> usize {
        let m = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
            }),
            children: vec![],
        };
        memo.new_group(m)
    }

    #[test]
    fn fires_on_final_unsplit_with_limit() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(100),
                offset: Some(10),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN.apply(&topn_mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one split alternative");
        match &out[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.phase, TopNPhase::Final);
                assert!(t.is_split);
                assert_eq!(t.limit, Some(100));
                assert_eq!(t.offset, Some(10));
            }
            other => panic!("expected LogicalTopN final+split, got {:?}", other),
        }
        // FINAL's child is a new group containing the PARTIAL TopN.
        assert_eq!(out[0].children.len(), 1);
        let partial_group = &memo.groups[out[0].children[0]];
        match &partial_group.logical_exprs[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.phase, TopNPhase::Partial);
                assert!(!t.is_split);
                assert_eq!(t.limit, Some(110), "partial limit must be L+O = 100+10");
                assert_eq!(t.offset, Some(0));
            }
            other => panic!("expected LogicalTopN partial, got {:?}", other),
        }
        assert_eq!(partial_group.logical_exprs[0].children, vec![scan_group]);
    }

    #[test]
    fn does_not_fire_on_partial() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN.apply(&topn_mexpr, &mut memo);
        assert!(out.is_empty());
    }

    #[test]
    fn does_not_fire_when_already_split() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN.apply(&topn_mexpr, &mut memo);
        assert!(out.is_empty());
    }

    #[test]
    fn does_not_fire_without_limit() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: None,
                offset: Some(5),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN.apply(&topn_mexpr, &mut memo);
        assert!(out.is_empty(), "no limit => out of scope");
    }
}
```

- [ ] **Step 2: Register the module in `src/sql/optimizer/rules/mod.rs`**

In `src/sql/optimizer/rules/mod.rs`, add after the other `pub(crate) mod` lines near the top:

```rust
pub(crate) mod split_top_n;
```

Then in `all_transformation_rules()` (line ~36-44), add the rule:

```rust
pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),
        Box::new(split_top_n::SplitTopN),
    ]
}
```

- [ ] **Step 3: Run the new tests to confirm they pass**

Run: `cargo test --lib split_top_n 2>&1 | tail -15`
Expected: 4 tests pass.

- [ ] **Step 4: Run full test suite**

Run: `cargo test --lib 2>&1 | tail -15`
Expected: all tests pass (including the 4 new ones).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rules/split_top_n.rs src/sql/optimizer/rules/mod.rs
git commit -m "feat: add SplitTopN transformation rule for two-stage TopN"
```

---

### Task 3: Update distribution contracts in `search.rs`

**Files:**
- Modify: `src/sql/optimizer/search.rs` (provided-props at line ~350, required-child-props at line ~496)

- [ ] **Step 1: Update TopN provided-properties (inside `output_properties`, match arm at lines 350-371)**

Replace the existing `Operator::PhysicalTopN(t) => { ... }` arm with:

```rust
        // TopN provided properties depend on phase/split:
        //   - Partial: Any distribution (preserves child layout). Ordering = Required if
        //     sort keys present (each partial's output is sorted).
        //   - Final split / Final !split: Gather (sort output is serialized to one instance).
        Operator::PhysicalTopN(t) => {
            let sort_keys: Vec<SortKey> = t
                .items
                .iter()
                .filter_map(|item| {
                    typed_expr_to_column_ref(&item.expr).map(|col| SortKey {
                        column: col,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect();
            let ordering = if sort_keys.is_empty() {
                OrderingSpec::Any
            } else {
                OrderingSpec::Required(sort_keys)
            };
            let distribution = match t.phase {
                crate::sql::optimizer::operator::TopNPhase::Partial => DistributionSpec::Any,
                crate::sql::optimizer::operator::TopNPhase::Final => DistributionSpec::Gather,
            };
            PhysicalPropertySet { distribution, ordering }
        }
```

- [ ] **Step 2: Update TopN required-child-properties (inside `required_input_properties`, match arm at line 496)**

Replace:

```rust
        // TopN: child must be Gather (same as Sort).
        Operator::PhysicalTopN(_) => vec![PhysicalPropertySet::gather()],
```

With:

```rust
        // TopN child requirement depends on phase/split:
        //   - Partial: child is Any (don't force gather; we run per-instance).
        //   - Final + split=true: child is PARTIAL with Any distribution already;
        //     the fragment builder materializes the merging-exchange, so no
        //     enforcer should insert a Gather between FINAL(split) and PARTIAL.
        //   - Final + !split (single-stage, today's behavior): child must be Gather.
        Operator::PhysicalTopN(t) => {
            use crate::sql::optimizer::operator::TopNPhase;
            let req = match (t.phase, t.is_split) {
                (TopNPhase::Partial, _) => PhysicalPropertySet::any(),
                (TopNPhase::Final, true) => PhysicalPropertySet::any(),
                (TopNPhase::Final, false) => PhysicalPropertySet::gather(),
            };
            vec![req]
        }
```

- [ ] **Step 3: Run existing search tests**

Run: `cargo test --lib search:: 2>&1 | tail -20`
Expected: the two existing TopN tests at lines 967 and 980 still pass (they use `phase=Final, is_split=false`, preserving old behavior).

- [ ] **Step 4: Add tests for PARTIAL and FINAL(split) distribution contracts**

Append inside the `mod top_n_property_tests` module in `src/sql/optimizer/search.rs` (around line 960):

```rust
    use crate::sql::optimizer::operator::TopNPhase;

    #[test]
    fn top_n_partial_requires_any_and_provides_any() {
        let op = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Partial,
            is_split: false,
        });
        let out = output_properties(&op);
        assert!(matches!(out.distribution, DistributionSpec::Any));

        let reqs = required_input_properties(&op, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }

    #[test]
    fn top_n_final_split_requires_any_and_provides_gather() {
        let op = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: true,
        });
        let out = output_properties(&op);
        assert!(matches!(out.distribution, DistributionSpec::Gather));

        let reqs = required_input_properties(&op, &PhysicalPropertySet::gather(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }
```

Note: one existing `use crate::sql::optimizer::operator::PhysicalTopNOp;` line is already in that module; the `TopNPhase` import is the only addition.

- [ ] **Step 5: Run the new tests**

Run: `cargo test --lib search:: 2>&1 | tail -20`
Expected: two new tests pass plus existing ones.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/search.rs
git commit -m "feat: phase-aware distribution contracts for PhysicalTopN"
```

---

### Task 4: Extend `build_exchange_node` to accept sort_info + offset

**Files:**
- Modify: `src/sql/codegen/nodes.rs:475-497`

- [ ] **Step 1: Add a `build_merging_exchange_node` helper**

Append after the existing `build_exchange_node` function (line ~497) in `src/sql/codegen/nodes.rs`:

```rust
/// Build a merging EXCHANGE_NODE. The receive side performs k-way merge
/// over sorted input streams using `sort_info`, then applies offset/limit.
/// Used for distributed TopN FINAL(split) and global ORDER BY.
pub(crate) fn build_merging_exchange_node(
    node_id: i32,
    input_row_tuples: Vec<i32>,
    partition_type: partitions::TPartitionType,
    sort_info: plan_nodes::TSortInfo,
    limit: Option<i64>,
    offset: Option<i64>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::EXCHANGE_NODE;
    node.num_children = 0;
    node.limit = limit.unwrap_or(-1);
    node.row_tuples = input_row_tuples.clone();
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.exchange_node = Some(plan_nodes::TExchangeNode::new(
        input_row_tuples,
        Some(sort_info),
        offset,
        Some(partition_type),
        None::<bool>,
        None::<plan_nodes::TLateMaterializeMode>,
    ));
    node
}
```

- [ ] **Step 2: Verify build**

Run: `cargo build 2>&1 | tail -5`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add src/sql/codegen/nodes.rs
git commit -m "feat: add build_merging_exchange_node helper"
```

---

### Task 5: Fragment builder — PARTIAL branch

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs:994-1069` (`visit_physical_top_n`)

- [ ] **Step 1: Rewrite `visit_physical_top_n` with three-branch dispatch (PARTIAL only; FINAL+split handled in Task 6)**

Replace the entire `visit_physical_top_n` body (lines 994-1069) with:

```rust
    fn visit_physical_top_n(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        use crate::sql::optimizer::operator::TopNPhase;
        match (op.phase, op.is_split) {
            // Single-stage (today's behavior) and PARTIAL both emit a single
            // SORT_NODE and return. PARTIAL's output is consumed by the FINAL
            // split visitor without a fragment boundary.
            (TopNPhase::Final, false) | (TopNPhase::Partial, _) => {
                self.visit_physical_top_n_single_or_partial(op, node)
            }
            // FINAL+split: add fragment boundary + merging EXCHANGE_NODE.
            (TopNPhase::Final, true) => self.visit_physical_top_n_final_split(op, node),
        }
    }

    fn visit_physical_top_n_single_or_partial(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let sort_node_id = self.alloc_node();
        let sort_tuple_id = *child.tuple_ids.last().unwrap();

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            None::<Vec<exprs::TExpr>>,
        );

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = op.limit.unwrap_or(-1);
        sort_plan_node.row_tuples = vec![sort_tuple_id];
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: true,
            offset: op.offset,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs: None,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs: None,
            partition_exprs: None,
            partition_limit: None,
            topn_type: None,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });

        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    fn visit_physical_top_n_final_split(
        &mut self,
        _op: &PhysicalTopNOp,
        _node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // Implemented in Task 6.
        Err("FINAL+split TopN not yet implemented".into())
    }
```

- [ ] **Step 2: Verify build**

Run: `cargo build 2>&1 | tail -5`
Expected: no errors (the FINAL+split branch is a stub returning Err, but no query currently produces FINAL+split because the `SplitTopN` alternative is only a cost-search candidate; cost should prefer single-stage until cost model is tuned, so this stub should not fire in tpc-ds suite yet).

- [ ] **Step 3: Run TPC-DS suite to confirm stub does not fire**

Assumes standalone-server is running on 9030 and minio on 9000 (previously started). If not:
```bash
NO_PROXY=127.0.0.1,localhost ./target/release/novarocks standalone-server --port 9030 &
MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=admin123 minio server ~/minio-data --address :9000 &
sleep 5
```

Then:
```bash
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite tpc-ds --mode verify --query-timeout 120 -j 1
```

Expected: 99/99 pass. If any query fails with "FINAL+split TopN not yet implemented", the cost model is picking split before Task 6 lands — stop and investigate (likely cost for two-stage is underestimated).

- [ ] **Step 4: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "refactor: three-branch visit_physical_top_n dispatch"
```

---

### Task 6: Fragment builder — FINAL+split branch

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs` (replace `visit_physical_top_n_final_split` stub)

- [ ] **Step 1: Implement the FINAL+split branch**

Replace the stub function with:

```rust
    fn visit_physical_top_n_final_split(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // Visit child. The child is a PARTIAL TopN that emitted a SORT_NODE
        // at the head of its plan_nodes. We create a fragment boundary here
        // (no PhysicalDistribution was inserted because FINAL(split)'s
        // required child distribution is Any).
        let parent_fragment_id = self.current_fragment_id()?;
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(&node.children[0]);
        self.fragment_stack.pop();
        let child = child_result?;
        let VisitResult {
            plan_nodes: child_plan_nodes,
            scope: child_scope,
            tuple_ids: child_tuple_ids,
            cte_exchange_nodes,
        } = child;

        // Assert the PARTIAL emitted a SORT_NODE at the head.
        let partial_sort_info = child_plan_nodes
            .first()
            .and_then(|n| n.sort_node.as_ref())
            .map(|s| s.sort_info.clone())
            .ok_or_else(|| {
                "FINAL+split TopN: expected PARTIAL child's root to be SORT_NODE".to_string()
            })?;

        // Close the partial fragment. Unpartitioned sender → merging exchange.
        let gather_spec = crate::sql::optimizer::property::DistributionSpec::Gather;
        let output_partition = self.build_output_partition(&gather_spec, &child_scope)?;
        let exchange_partition_type = output_partition.type_.clone();

        self.completed_fragments.push(FragmentBuildResult {
            fragment_id: child_fragment_id,
            plan: plan_nodes::TPlan::new(child_plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(&[])?,
            output_sink: build_noop_sink(),
            output_columns: node.children[0]
                .output_columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id: None,
            cte_exchange_nodes,
        });

        // Coordinator fragment root: merging EXCHANGE_NODE.
        let exchange_node_id = self.alloc_node();
        let exchange_node = nodes::build_merging_exchange_node(
            exchange_node_id,
            child_tuple_ids.clone(),
            exchange_partition_type,
            partial_sort_info,
            op.limit,
            op.offset,
        );

        self.completed_edges.push(FragmentEdge {
            source_fragment_id: child_fragment_id,
            target_fragment_id: parent_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition,
            edge_kind: FragmentEdgeKind::Stream,
        });

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope: child_scope,
            tuple_ids: child_tuple_ids,
            cte_exchange_nodes: Vec::new(),
        })
    }
```

- [ ] **Step 2: Verify build**

Run: `cargo build 2>&1 | tail -5`
Expected: no errors.

- [ ] **Step 3: EXPLAIN spot-check**

Ensure standalone-server is running (port 9030) and minio (port 9000). Then:

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P 9030 -u root --table -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_ref PROPERTIES (
    'type'='iceberg','iceberg.catalog.type'='hadoop',
    'iceberg.catalog.warehouse'='oss://novarocks/iceberg-catalog/',
    'aws.s3.access_key'='admin','aws.s3.secret_key'='admin123',
    'aws.s3.endpoint'='http://127.0.0.1:9000',
    'aws.s3.enable_path_style_access'='true');
USE iceberg_ref.tpcds;
EXPLAIN SELECT d_year FROM date_dim ORDER BY d_year LIMIT 5;
"
```

Expected: the plan contains an EXCHANGE_NODE whose sort_info is set (shown in EXPLAIN as some merging-exchange marker if available, or at minimum the plan has only one SORT_NODE rather than a separate final SORT_NODE above gather). If cost picks the single-stage form, note it and move to next spot-check with a larger-cardinality query like `EXPLAIN SELECT ss_item_sk FROM store_sales ORDER BY ss_item_sk LIMIT 100;`.

- [ ] **Step 4: Run TPC-DS verify**

```bash
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite tpc-ds --mode verify --query-timeout 120 -j 1
```

Expected: 99/99 pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "feat: FINAL+split TopN emits merging exchange"
```

---

### Task 7: Baseline diff + performance spot-check

**Files:** None. Verification only.

- [ ] **Step 1: Capture new EXPLAIN snapshots**

```bash
mkdir -p /tmp/novarocks-plan-compare/standalone-merge-topn
for f in sql-tests/tpc-ds/sql/q*.sql; do
  name=$(basename "$f" .sql)
  {
    echo "USE iceberg_ref.tpcds;"
    echo "EXPLAIN"
    cat "$f"
  } | NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P 9030 -u root --table \
    > "/tmp/novarocks-plan-compare/standalone-merge-topn/$name.txt"
done
```

- [ ] **Step 2: Diff against baseline**

```bash
diff -q /tmp/novarocks-plan-compare/standalone-current/ \
        /tmp/novarocks-plan-compare/standalone-merge-topn/ \
  | tee /tmp/topn-diff-summary.txt
wc -l /tmp/topn-diff-summary.txt
```

Expected: a subset of the ~110 top-level TopN queries (q1, q3, q10, q12, q15, ...) show differences. Non-TopN queries should be byte-identical.

- [ ] **Step 3: Classify a sample of diffs**

Pick 3-5 differing queries. For each, open both EXPLAINs and confirm:
- Before: `TOP-N (...) → GATHER EXCHANGE → ...`
- After: either `EXCHANGE_NODE (with sort_info marker) → TOP-N (partial, limit=L+O) → ...` or still single-stage (cost picked it) — both are acceptable.
- The overall plan shape otherwise matches.

If any non-TopN query differs, stop and investigate.

- [ ] **Step 4: Record counts**

```bash
echo "Queries that switched to two-stage:"
grep -L "GATHER EXCHANGE$" /tmp/novarocks-plan-compare/standalone-merge-topn/*.txt | wc -l
# Alternative: count queries whose EXPLAIN contains an EXCHANGE_NODE with sort info.
# Since current EXPLAIN formatter does not surface sort_info, this is a textual
# heuristic — record the result and move on.
```

- [ ] **Step 5: Performance spot-check (optional but recommended)**

Pick 2-3 queries where a large sort is visible: e.g., a synthesized `SELECT ss_item_sk FROM store_sales ORDER BY ss_item_sk LIMIT 100`. Measure wall time before and after (run 3× each, median):

```bash
time NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P 9030 -u root -e "
USE iceberg_ref.tpcds;
SELECT ss_item_sk FROM store_sales ORDER BY ss_item_sk LIMIT 100;
" > /dev/null
```

Record before/after in the landing note if measurable.

- [ ] **Step 6: Append landing note**

Append to `docs/design/specs/2026-04-17-distributed-top-n-merge-exchange-design.md`:

```markdown
---

**Landed.** Date: YYYY-MM-DD. HEAD: <commit sha>. SplitTopN transformation + phase-aware distribution contracts + fragment-builder FINAL+split branch all live. TPC-DS standalone suite verify: 99/99 pass. EXPLAIN diffs: N of 110 top-level TopN queries switched to merging-exchange form; remainder kept single-stage (cost preference, acceptable). Non-TopN queries: byte-identical.
```

- [ ] **Step 7: Commit landing note**

```bash
git add docs/design/specs/2026-04-17-distributed-top-n-merge-exchange-design.md
git commit -m "docs: landing note for distributed TopN merge exchange"
```

---

## Risks Recap

- Task 5 step 3 is the **safety gate** that catches cost-model over-eagerness: if cost picks FINAL+split before Task 6 lands, the stub fires and the suite fails. Keep Task 5 and Task 6 in separate commits so bisection is clean.
- Task 3's distribution contract changes can cascade: inspect any non-TopN EXPLAIN diffs carefully in Task 7.
- If Task 7 shows <30% of TopN queries switched, follow up with a cost-penalty tune in a separate plan (out of scope here).
