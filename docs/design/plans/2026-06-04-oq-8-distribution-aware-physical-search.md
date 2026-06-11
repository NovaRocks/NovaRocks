# OQ-8 Distribution-Aware Physical Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make hash join broadcast/shuffle selection property-driven: one physical hash join expression enumerates child-property alternatives, search costs candidates after seeing child output properties, and extracted execution metadata drives EXPLAIN, runtime filters, and fragment codegen.

**Architecture:** Adopt the StarRocks-style shape from the approved OQ-8 spec, but keep the first implementation scoped to NovaRocks' current distribution model. `DistributionSpec::Broadcast` becomes a real physical property, hash join alternatives move into `derive_required_alternatives`, `SearchContext` records the chosen alternative and child properties in `Winner`, and extraction attaches final `PlanExecutionProps` so execution code stops relying on memo-time operator candidates.

**Tech Stack:** Rust crate `novarocks`, Cascades optimizer under `src/sql/optimizer`, standalone SQL golden tests under `sql-tests/optimizer`, thrift fragment codegen under `src/sql/codegen`.

**Spec:** `docs/design/specs/2026-06-04-oq-8-distribution-aware-physical-search-design.md`

---

## File Structure

Create:
- `sql-tests/optimizer/sql/distribution_join_broadcast_gate.sql` - golden for rejecting risky broadcast on large/fallback build sides.
- `sql-tests/optimizer/sql/distribution_join_shuffle_reuse.sql` - golden for reusing a child hash distribution instead of duplicating exchange.
- `sql-tests/optimizer/sql/distribution_join_downstream_reuse.sql` - golden for downstream hash requirement reuse from partitioned join output.
- `sql-tests/optimizer/sql/distribution_join_fallback_stats_gate.sql` - golden for conservative broadcast selection when row-count confidence is fallback.

Modify:
- `src/sql/optimizer/property.rs` - add `DistributionSpec::Broadcast`, `PhysicalPropertySet::broadcast`, and satisfies tests.
- `src/sql/optimizer/operator.rs` - add `JoinDistribution::Unknown` as memo-time placeholder and keep `Broadcast/Shuffle/Colocate` as extracted execution labels during the compatibility phase.
- `src/sql/optimizer/physical_plan.rs` - add `JoinExecutionDistribution` and `PlanExecutionProps` to extracted nodes.
- `src/sql/optimizer/derive/mod.rs` - add `PropertyAlternativeKind`, `ChildRequirementAlternative`, `derive_required_alternatives`, and compatibility wrappers for existing per-operator derivers.
- `src/sql/optimizer/derive/hash_join.rs` - implement hash join broadcast/shuffle alternatives, parent-key alignment, alternative-specific output derivation, and join execution distribution derivation.
- `src/sql/optimizer/cascades_rules/implement.rs` - make `JoinToHashJoin` emit one memo physical expression instead of separate broadcast/shuffle expressions.
- `src/sql/optimizer/search.rs` - enumerate alternatives, record chosen alternative/child properties/child outputs, apply broadcast gate, and call child-output-aware cost.
- `src/sql/optimizer/cost.rs` - add `CostOptions`, `compute_cost_with_properties`, broadcast gate helper, and child-output-aware hash join formulas.
- `src/sql/optimizer/extract.rs` - extract children with `winner.child_props`, attach `PlanExecutionProps`, and keep enforcer wrapping stable.
- `src/sql/explain.rs` - render hash join distribution from extracted execution metadata and add `BROADCAST EXCHANGE` label.
- `src/sql/optimizer/runtime_filter_pass.rs` - read join execution distribution from `PhysicalPlanNode.execution_props`.
- `src/sql/codegen/nodes.rs` - let hash join thrift builder accept final join distribution mode.
- `src/sql/codegen/fragment_builder.rs` - map `DistributionSpec::Broadcast` to a broadcast edge and lower hash join/RF layout from `PlanExecutionProps`.

Common commands:
- Format: `cargo fmt`
- Compile: `cargo build`
- Focused Rust tests are listed with exact filters in each task.
- SQL runner after starting standalone server from the generated environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG"
```

Task 12 lists the exact optimizer SQL runner command for the OQ-8 cases.

Commit message language is English.

---

## Phase P1 - Property Model And Metadata Scaffolding

### Task 1: Add Broadcast Distribution Property

**Files:**
- Modify: `src/sql/optimizer/property.rs`

- [ ] **Step 1: Write failing tests**

Append these tests inside `property.rs` `#[cfg(test)] mod tests`:

```rust
#[test]
fn broadcast_distribution_satisfies_only_broadcast_and_any() {
    let provided = DistributionSpec::Broadcast;

    assert!(provided.satisfies(&DistributionSpec::Any));
    assert!(provided.satisfies(&DistributionSpec::Broadcast));
    assert!(!provided.satisfies(&DistributionSpec::Gather));
    assert!(!provided.satisfies(&DistributionSpec::shuffle_join([ColumnId(1)])));
    assert!(!DistributionSpec::Gather.satisfies(&DistributionSpec::Broadcast));
    assert!(!DistributionSpec::shuffle_join([ColumnId(1)]).satisfies(&DistributionSpec::Broadcast));
}

#[test]
fn physical_property_set_broadcast_uses_any_ordering() {
    let props = PhysicalPropertySet::broadcast();
    assert_eq!(props.distribution, DistributionSpec::Broadcast);
    assert_eq!(props.ordering, OrderingSpec::Any);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --lib broadcast_distribution_satisfies_only_broadcast_and_any`

Expected: compile failure mentioning `no variant or associated item named Broadcast`.

- [ ] **Step 3: Implement `Broadcast`**

In `PhysicalPropertySet`:

```rust
    pub fn broadcast() -> Self {
        Self {
            distribution: DistributionSpec::Broadcast,
            ordering: OrderingSpec::Any,
        }
    }
```

In `DistributionSpec`:

```rust
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    Broadcast,
    HashPartitioned {
        cols: Vec<ColumnId>,
        source: HashSource,
    },
}
```

In `DistributionSpec::satisfies`:

```rust
            DistributionSpec::Any => true,
            DistributionSpec::Gather => matches!(self, DistributionSpec::Gather),
            DistributionSpec::Broadcast => matches!(self, DistributionSpec::Broadcast),
            DistributionSpec::HashPartitioned {
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
cargo test --lib broadcast_distribution_satisfies_only_broadcast_and_any
cargo test --lib physical_property_set_broadcast_uses_any_ordering
```

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/property.rs
git commit -m "feat(optimizer): add broadcast distribution property"
```

### Task 2: Add Extracted Plan Execution Metadata

**Files:**
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/physical_plan.rs`
- Modify: every existing `PhysicalPlanNode { ... }` literal reported by `rg -n "PhysicalPlanNode \\{" src/sql`

- [ ] **Step 1: Write failing tests**

Add to `physical_plan.rs` test module:

```rust
#[test]
fn physical_node_carries_execution_properties() {
    let node = PhysicalPlanNode {
        op: make_test_op(),
        children: vec![],
        stats: Statistics {
            output_row_count: 1.0,
            column_statistics: Default::default(),
            ..Default::default()
        },
        output_columns: vec![],
        execution_props: PlanExecutionProps {
            output_property: crate::sql::optimizer::property::PhysicalPropertySet::broadcast(),
            child_output_properties: vec![crate::sql::optimizer::property::PhysicalPropertySet::any()],
            join_distribution: Some(JoinExecutionDistribution::Broadcast),
        },
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    };

    assert_eq!(
        node.execution_props.join_distribution,
        Some(JoinExecutionDistribution::Broadcast)
    );
    assert_eq!(
        node.execution_props.output_property.distribution,
        crate::sql::optimizer::property::DistributionSpec::Broadcast
    );
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib physical_node_carries_execution_properties`

Expected: compile failure for missing `PlanExecutionProps` and `JoinExecutionDistribution`.

- [ ] **Step 3: Add metadata types**

In `operator.rs`, extend the existing enum:

```rust
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JoinDistribution {
    Unknown,
    Shuffle,
    Broadcast,
    Colocate,
}
```

In `physical_plan.rs`:

```rust
use crate::sql::optimizer::property::PhysicalPropertySet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinExecutionDistribution {
    Broadcast,
    Partitioned,
    Colocate,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanExecutionProps {
    pub output_property: PhysicalPropertySet,
    pub child_output_properties: Vec<PhysicalPropertySet>,
    pub join_distribution: Option<JoinExecutionDistribution>,
}

impl Default for PlanExecutionProps {
    fn default() -> Self {
        Self {
            output_property: PhysicalPropertySet::any(),
            child_output_properties: Vec::new(),
            join_distribution: None,
        }
    }
}
```

Add the field to `PhysicalPlanNode`:

```rust
    pub execution_props: PlanExecutionProps,
```

For every existing literal produced by `rg -n "PhysicalPlanNode \\{" src/sql`, add:

```rust
            execution_props: PlanExecutionProps::default(),
```

If the file has not imported `PlanExecutionProps`, use the fully qualified path:

```rust
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test --lib physical_node_carries_execution_properties`

Expected: test passes.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/operator.rs src/sql/optimizer/physical_plan.rs src/sql/explain.rs src/sql/optimizer/runtime_filter_pass.rs src/sql/codegen/fragment_builder.rs src/sql/optimizer/extract.rs
git commit -m "feat(optimizer): add extracted plan execution metadata"
```

### Task 3: Add Required-Property Alternatives Dispatcher

**Files:**
- Modify: `src/sql/optimizer/derive/mod.rs`

- [ ] **Step 1: Write failing tests**

Append inside `derive/mod.rs` tests:

```rust
#[test]
fn default_required_alternative_wraps_legacy_deriver() {
    let op = Operator::PhysicalLimit(PhysicalLimitOp {
        limit: Some(10),
        offset: None,
    });
    let parent = PhysicalPropertySet::gather();

    let legacy = derive_required(&op, &parent, 1);
    let alternatives = derive_required_alternatives(&op, &parent, 1);

    assert_eq!(alternatives.len(), 1);
    assert_eq!(alternatives[0].kind, PropertyAlternativeKind::Default);
    assert_eq!(alternatives[0].child_props, legacy);
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib default_required_alternative_wraps_legacy_deriver`

Expected: compile failure for missing `derive_required_alternatives`.

- [ ] **Step 3: Implement alternatives**

Add near the trait declarations:

```rust
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum PropertyAlternativeKind {
    Default,
    BroadcastJoin,
    ShuffleJoin,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ChildRequirementAlternative {
    pub kind: PropertyAlternativeKind,
    pub child_props: Vec<PhysicalPropertySet>,
}

impl ChildRequirementAlternative {
    pub(crate) fn default(child_props: Vec<PhysicalPropertySet>) -> Self {
        Self {
            kind: PropertyAlternativeKind::Default,
            child_props,
        }
    }
}
```

Add dispatcher after `derive_required`:

```rust
pub(crate) fn derive_required_alternatives(
    op: &Operator,
    parent_required: &PhysicalPropertySet,
    num_children: usize,
) -> Vec<ChildRequirementAlternative> {
    match op {
        Operator::PhysicalHashJoin(o) => o.derive_required_alternatives(parent_required, num_children),
        _ => vec![ChildRequirementAlternative::default(derive_required(
            op,
            parent_required,
            num_children,
        ))],
    }
}
```

Before Task 4 replaces the implementation with hash-join-specific alternatives, add this compatibility method in `derive/hash_join.rs`:

```rust
impl PhysicalHashJoinOp {
    pub(crate) fn derive_required_alternatives(
        &self,
        parent_required: &PhysicalPropertySet,
        num_children: usize,
    ) -> Vec<super::ChildRequirementAlternative> {
        vec![super::ChildRequirementAlternative::default(
            self.derive_required(parent_required, num_children),
        )]
    }
}
```

- [ ] **Step 4: Run test to verify pass**

Run: `cargo test --lib default_required_alternative_wraps_legacy_deriver`

Expected: test passes.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/derive/mod.rs src/sql/optimizer/derive/hash_join.rs
git commit -m "feat(optimizer): add required property alternatives"
```

---

## Phase P2 - Hash Join Alternatives

### Task 4: Implement Hash Join Broadcast And Shuffle Alternatives

**Files:**
- Modify: `src/sql/optimizer/derive/hash_join.rs`
- Modify: `src/sql/optimizer/derive/mod.rs`

- [ ] **Step 1: Write failing tests**

Add inside `derive/hash_join.rs` tests:

```rust
#[test]
fn hash_join_required_alternatives_include_broadcast_and_shuffle() {
    let op = PhysicalHashJoinOp {
        join_type: crate::sql::analysis::JoinKind::Inner,
        eq_conditions: vec![PhysicalHashJoinEqCondition {
            left: col(10),
            right: col(20),
            null_safe: false,
        }],
        other_condition: None,
        distribution: JoinDistribution::Unknown,
    };

    let alternatives = op.derive_required_alternatives(&PhysicalPropertySet::any(), 2);
    assert_eq!(alternatives.len(), 2);
    assert_eq!(alternatives[0].kind, super::PropertyAlternativeKind::BroadcastJoin);
    assert_eq!(alternatives[0].child_props[0].distribution, DistributionSpec::Any);
    assert_eq!(alternatives[0].child_props[1].distribution, DistributionSpec::Broadcast);
    assert_eq!(alternatives[1].kind, super::PropertyAlternativeKind::ShuffleJoin);
    assert_eq!(alternatives[1].child_props[0].distribution, DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]));
    assert_eq!(alternatives[1].child_props[1].distribution, DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]));
}

#[test]
fn hash_join_shuffle_alternative_aligns_with_parent_required_order() {
    let op = PhysicalHashJoinOp {
        join_type: crate::sql::analysis::JoinKind::Inner,
        eq_conditions: vec![
            PhysicalHashJoinEqCondition {
                left: col(10),
                right: col(20),
                null_safe: false,
            },
            PhysicalHashJoinEqCondition {
                left: col(11),
                right: col(21),
                null_safe: false,
            },
        ],
        other_condition: None,
        distribution: JoinDistribution::Unknown,
    };
    let parent = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_join([ColumnId(11), ColumnId(10)]),
        ordering: OrderingSpec::Any,
    };

    let alternatives = op.derive_required_alternatives(&parent, 2);
    let shuffle = alternatives
        .iter()
        .find(|alt| alt.kind == super::PropertyAlternativeKind::ShuffleJoin)
        .expect("shuffle alternative");

    assert_eq!(shuffle.child_props[0].distribution, DistributionSpec::shuffle_join([ColumnId(11), ColumnId(10)]));
    assert_eq!(shuffle.child_props[1].distribution, DistributionSpec::shuffle_join([ColumnId(21), ColumnId(20)]));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib hash_join_required_alternatives_include_broadcast_and_shuffle
cargo test --lib hash_join_shuffle_alternative_aligns_with_parent_required_order
```

Expected: first test fails because current compatibility method returns one default alternative.

- [ ] **Step 3: Implement helper functions**

Add imports:

```rust
use crate::sql::optimizer::derive::{
    ChildRequirementAlternative, PropertyAlternativeKind,
};
```

Add helper functions above `impl DeriveOutput`:

```rust
fn left_key_ids(eq_conditions: &[PhysicalHashJoinEqCondition]) -> Vec<ColumnId> {
    eq_conditions
        .iter()
        .filter_map(|eq| typed_expr_to_column_id(&eq.left))
        .collect()
}

fn right_key_ids(eq_conditions: &[PhysicalHashJoinEqCondition]) -> Vec<ColumnId> {
    eq_conditions
        .iter()
        .filter_map(|eq| typed_expr_to_column_id(&eq.right))
        .collect()
}

fn aligned_shuffle_keys(
    eq_conditions: &[PhysicalHashJoinEqCondition],
    parent_required: &PhysicalPropertySet,
) -> (Vec<ColumnId>, Vec<ColumnId>) {
    let left = left_key_ids(eq_conditions);
    let right = right_key_ids(eq_conditions);
    let DistributionSpec::HashPartitioned {
        cols: parent_cols,
        source: HashSource::ShuffleJoin,
    } = &parent_required.distribution
    else {
        return (shuffle_join_column_ids(eq_conditions), shuffle_join_column_ids(eq_conditions));
    };

    let mut aligned_left = Vec::new();
    let mut aligned_right = Vec::new();
    for parent_col in parent_cols {
        if let Some(pos) = left.iter().position(|left_col| left_col == parent_col) {
            aligned_left.push(left[pos]);
            aligned_right.push(right[pos]);
        } else if let Some(pos) = right.iter().position(|right_col| right_col == parent_col) {
            aligned_left.push(left[pos]);
            aligned_right.push(right[pos]);
        } else {
            return (shuffle_join_column_ids(eq_conditions), shuffle_join_column_ids(eq_conditions));
        }
    }
    if aligned_left.len() == left.len() && aligned_right.len() == right.len() {
        (aligned_left, aligned_right)
    } else {
        (shuffle_join_column_ids(eq_conditions), shuffle_join_column_ids(eq_conditions))
    }
}

fn hash_join_only_shuffle(join_type: crate::sql::analysis::JoinKind) -> bool {
    use crate::sql::analysis::JoinKind::*;
    matches!(join_type, RightOuter | RightSemi | RightAnti | FullOuter)
}
```

- [ ] **Step 4: Replace compatibility method**

Replace the temporary `derive_required_alternatives` method with:

```rust
impl PhysicalHashJoinOp {
    pub(crate) fn derive_required_alternatives(
        &self,
        parent_required: &PhysicalPropertySet,
        num_children: usize,
    ) -> Vec<ChildRequirementAlternative> {
        if num_children != 2 {
            return vec![ChildRequirementAlternative::default(vec![
                PhysicalPropertySet::any();
                num_children
            ])];
        }

        let mut alternatives = Vec::new();
        if !hash_join_only_shuffle(self.join_type) {
            alternatives.push(ChildRequirementAlternative {
                kind: PropertyAlternativeKind::BroadcastJoin,
                child_props: vec![PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()],
            });
        }

        let (left_keys, right_keys) = aligned_shuffle_keys(&self.eq_conditions, parent_required);
        alternatives.push(ChildRequirementAlternative {
            kind: PropertyAlternativeKind::ShuffleJoin,
            child_props: vec![
                PhysicalPropertySet {
                    distribution: DistributionSpec::shuffle_join(left_keys),
                    ordering: OrderingSpec::Any,
                },
                PhysicalPropertySet {
                    distribution: DistributionSpec::shuffle_join(right_keys),
                    ordering: OrderingSpec::Any,
                },
            ],
        });
        alternatives
    }
}
```

- [ ] **Step 5: Run tests to verify pass**

Run:

```bash
cargo test --lib hash_join_required_alternatives_include_broadcast_and_shuffle
cargo test --lib hash_join_shuffle_alternative_aligns_with_parent_required_order
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/derive/hash_join.rs src/sql/optimizer/derive/mod.rs
git commit -m "feat(optimizer): derive hash join distribution alternatives"
```

### Task 5: Make JoinToHashJoin Emit One Physical Expression

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: tests in `src/sql/optimizer/cascades_rules/implement.rs`

- [ ] **Step 1: Write failing test**

In the existing `join_demotion_tests` module, add this test after `null_safe_join_pair_stays_hash_join_key`:

```rust
#[test]
fn join_to_hash_join_emits_one_property_driven_hash_join() {
    let mut memo = Memo::new();
    let left_group = mk_scan_group(&mut memo, &["a_id"]);
    let right_group = mk_scan_group(&mut memo, &["b_id"]);
    let condition = bin(col("a_id"), BinOp::Eq, col("b_id"));
    let expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalJoin(LogicalJoinOp {
            join_type: JoinKind::Inner,
            condition: Some(condition),
        }),
        children: vec![left_group, right_group],
    };
    let rule = JoinToHashJoin;
    let alternatives = rule.apply(&expr, &mut memo);

    assert_eq!(alternatives.len(), 1);
    let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
        panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
    };
    assert!(matches!(phys.distribution, JoinDistribution::Unknown));
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib join_to_hash_join_emits_one_property_driven_hash_join`

Expected: test fails because the rule currently emits two alternatives.

- [ ] **Step 3: Implement single expression**

Replace the current `vec![shuffle, broadcast]` block in `JoinToHashJoin::apply` with:

```rust
        vec![NewExpr {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: op.join_type,
                eq_conditions: eq_conds,
                other_condition: other,
                distribution: JoinDistribution::Unknown,
            }),
            children: expr.children.clone(),
        }]
```

Update old tests that expected two hash join alternatives so they assert:

```rust
assert_eq!(alternatives.len(), 1);
```

and remove checks that depend on separate `JoinDistribution::Shuffle` and `JoinDistribution::Broadcast` memo expressions.

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test --lib demoted_single_side_pair_ends_in_other_condition
cargo test --lib null_safe_join_pair_stays_hash_join_key
cargo test --lib join_to_hash_join_emits_one_property_driven_hash_join
```

Expected: all `JoinToHashJoin` tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cascades_rules/implement.rs
git commit -m "feat(optimizer): emit one property-driven hash join"
```

---

## Phase P3 - Child-Output-Aware Search And Cost

### Task 6: Record Alternative Metadata In Winners

**Files:**
- Modify: `src/sql/optimizer/search.rs`
- Modify: `src/sql/optimizer/extract.rs` only enough to compile by reading `winner.child_props`

- [ ] **Step 1: Write failing test**

In `search.rs` tests, add:

```rust
fn test_col(id: u32) -> crate::sql::analysis::TypedExpr {
    crate::sql::analysis::TypedExpr {
        kind: crate::sql::analysis::ExprKind::ColumnRef {
            column_id: ColumnId(id),
            qualifier: None,
            column: format!("c{id}"),
        },
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
    }
}

fn make_two_table_inner_join_memo_for_test() -> (Memo, GroupId) {
    let mut memo = Memo::new();
    let left = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalValues(PhysicalValuesOp {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
    });
    let right = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalValues(PhysicalValuesOp {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
    });
    let root = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: crate::sql::analysis::JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: test_col(10),
                right: test_col(20),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        }),
        children: vec![left, right],
    });
    (memo, root)
}

#[test]
fn winner_records_hash_join_alternative_and_child_properties() {
    let (memo, root) = make_two_table_inner_join_memo_for_test();
    let mut ctx = SearchContext::new(Default::default());
    let required = PhysicalPropertySet::gather();

    ctx.optimize_group(&memo, root, &required).expect("search");
    let winner = ctx.winners.get(&(root, required)).expect("winner");

    assert!(
        matches!(
            winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::BroadcastJoin
                | crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin
        ),
        "expected hash join alternative, got {:?}",
        winner.alt_kind
    );
    assert_eq!(winner.child_props.len(), 2);
    assert_eq!(winner.child_outputs.len(), 2);
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib winner_records_hash_join_alternative_and_child_properties`

Expected: compile failure for missing `Winner.alt_kind`, `Winner.child_props`, and `Winner.child_outputs`.

- [ ] **Step 3: Extend `Winner`**

In `search.rs` imports:

```rust
use super::derive::{ChildRequirementAlternative, PropertyAlternativeKind};
```

Extend `Winner`:

```rust
    pub(crate) alt_kind: PropertyAlternativeKind,
    pub(crate) child_props: Vec<PhysicalPropertySet>,
    pub(crate) child_outputs: Vec<PhysicalPropertySet>,
```

Initialize best fields before the expression loop:

```rust
        let mut best_alt_kind = PropertyAlternativeKind::Default;
        let mut best_child_props = Vec::new();
        let mut best_child_outputs = Vec::new();
```

When building an infinite winner, set:

```rust
            alt_kind: best_alt_kind,
            child_props: best_child_props,
            child_outputs: best_child_outputs,
```

- [ ] **Step 4: Loop over alternatives**

Replace the single `child_reqs = derive_required(...)` block with:

```rust
            let alternatives =
                super::derive::derive_required_alternatives(&expr.op, required, expr.children.len());

            for alt in alternatives {
                let child_reqs = alt.child_props.clone();
                if child_reqs.len() != expr.children.len() {
                    continue;
                }

                let own_stats = derive_statistics(expr, memo, &self.table_stats);
                let child_stats_vec: Vec<_> = expr
                    .children
                    .iter()
                    .map(|&cg| stats_for_group(&memo.groups[cg], memo, &self.table_stats))
                    .collect();
                let child_stats_refs: Vec<&_> = child_stats_vec.iter().collect();

                let mut child_total = 0.0;
                let mut child_outputs: Vec<PhysicalPropertySet> =
                    Vec::with_capacity(expr.children.len());
                let mut feasible = true;
                for (i, &cg) in expr.children.iter().enumerate() {
                    let child_cost = self.optimize_group(memo, cg, &child_reqs[i])?;
                    if child_cost.is_infinite() {
                        feasible = false;
                        break;
                    }
                    child_total += child_cost;
                    let cw = self
                        .winners
                        .get(&(cg, child_reqs[i].clone()))
                        .expect("child just optimized - winner must be in cache");
                    child_outputs.push(cw.output.clone());
                }
                if !feasible {
                    continue;
                }

                let child_output_refs: Vec<&PhysicalPropertySet> = child_outputs.iter().collect();
                let own_cost = compute_cost_with_properties(
                    &expr.op,
                    &own_stats,
                    &child_stats_refs,
                    &child_output_refs,
                    &alt.kind,
                    &CostOptions::default(),
                );
                let total = own_cost + child_total;
                let provided = super::derive::derive_output_for_alternative(
                    &expr.op,
                    required,
                    &child_output_refs,
                    &alt.kind,
                );

                // Keep the existing provided -> required enforcer block here.
            }
```

Add temporary imports for Task 7:

```rust
use super::cost::{compute_cost_with_properties, CostOptions};
```

Task 7 defines these symbols. To keep this task compiling before Task 7, add compatibility wrappers in `cost.rs` as part of this task:

```rust
pub(crate) struct CostOptions {
    pub backend_factor: f64,
}

impl Default for CostOptions {
    fn default() -> Self {
        Self { backend_factor: 3.0 }
    }
}

pub(crate) fn compute_cost_with_properties(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    _child_outputs: &[&PhysicalPropertySet],
    _alt_kind: &PropertyAlternativeKind,
    _options: &CostOptions,
) -> Cost {
    compute_cost(op, own_stats, child_stats)
}
```

Add imports in `cost.rs`:

```rust
use super::derive::PropertyAlternativeKind;
use super::property::PhysicalPropertySet;
```

- [ ] **Step 5: Update best winner assignment**

Inside `if candidate_cost < best_cost`:

```rust
                best_alt_kind = alt.kind.clone();
                best_child_props = child_reqs.clone();
                best_child_outputs = child_outputs.clone();
```

When creating `Winner`:

```rust
            alt_kind: best_alt_kind,
            child_props: best_child_props,
            child_outputs: best_child_outputs,
```

- [ ] **Step 6: Add compatibility output dispatcher**

In `derive/mod.rs`, add:

```rust
pub(crate) fn derive_output_for_alternative(
    op: &Operator,
    parent_required: &PhysicalPropertySet,
    children_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
) -> PhysicalPropertySet {
    match op {
        Operator::PhysicalHashJoin(o) => {
            o.derive_output_for_alternative(parent_required, children_outputs, alt_kind)
        }
        _ => derive_output(op, children_outputs),
    }
}
```

In `derive/hash_join.rs`, add:

```rust
impl PhysicalHashJoinOp {
    pub(crate) fn derive_output_for_alternative(
        &self,
        _parent_required: &PhysicalPropertySet,
        children: &[&PhysicalPropertySet],
        alt_kind: &PropertyAlternativeKind,
    ) -> PhysicalPropertySet {
        match alt_kind {
            PropertyAlternativeKind::BroadcastJoin => {
                derive_broadcast_output(self, children)
            }
            PropertyAlternativeKind::ShuffleJoin => {
                derive_shuffle_output(self)
            }
            PropertyAlternativeKind::Default => self.derive_output(children),
        }
    }
}
```

Move the existing broadcast branch body into `derive_broadcast_output` and the existing shuffle branch body into `derive_shuffle_output`. Keep `DeriveOutput for PhysicalHashJoinOp` as:

```rust
impl DeriveOutput for PhysicalHashJoinOp {
    fn derive_output(&self, children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        match self.distribution {
            JoinDistribution::Shuffle => derive_shuffle_output(self),
            JoinDistribution::Broadcast | JoinDistribution::Colocate => {
                derive_broadcast_output(self, children)
            }
            JoinDistribution::Unknown => PhysicalPropertySet::any(),
        }
    }
}
```

- [ ] **Step 7: Run tests**

Run: `cargo test --lib winner_records_hash_join_alternative_and_child_properties`

Expected: test passes.

- [ ] **Step 8: Commit**

```bash
git add src/sql/optimizer/search.rs src/sql/optimizer/extract.rs src/sql/optimizer/cost.rs src/sql/optimizer/derive/mod.rs src/sql/optimizer/derive/hash_join.rs
git commit -m "feat(optimizer): record property alternative winners"
```

### Task 7: Implement Child-Output-Aware Cost And Broadcast Gate

**Files:**
- Modify: `src/sql/optimizer/cost.rs`
- Modify: `src/sql/optimizer/search.rs`

- [ ] **Step 1: Write failing cost tests**

Add inside `cost.rs` tests:

```rust
#[test]
fn child_output_aware_shuffle_join_does_not_charge_network_exchange_twice() {
    let probe = stats(100_000.0, 100.0);
    let build = stats(10_000.0, 100.0);
    let own = stats(100_000.0, 200.0);
    let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
        join_type: JoinKind::Inner,
        eq_conditions: vec![],
        other_condition: None,
        distribution: JoinDistribution::Unknown,
    });
    let child_stats = [&probe, &build];
    let left_output = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_join([ColumnId(1)]),
        ordering: OrderingSpec::Any,
    };
    let right_output = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_join([ColumnId(2)]),
        ordering: OrderingSpec::Any,
    };
    let child_outputs = [&left_output, &right_output];

    let cost = compute_cost_with_properties(
        &op,
        &own,
        &child_stats,
        &child_outputs,
        &PropertyAlternativeKind::ShuffleJoin,
        &CostOptions::default(),
    );

    let probe_size = probe.compute_size();
    let build_size = build.compute_size();
    assert!(cost < (probe_size + build_size) * NETWORK_COST + probe_size);
    assert!(cost >= probe_size);
}

#[test]
fn broadcast_gate_rejects_fallback_build_above_fallback_limit() {
    let mut build = stats(600_000.0, 100.0);
    build.row_count_confidence = crate::sql::optimizer::statistics::Confidence::Fallback;
    let probe = stats(700_000.0, 100.0);
    let options = CostOptions::default();

    assert!(!broadcast_gate_passes(&probe, &build, &options));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib child_output_aware_shuffle_join_does_not_charge_network_exchange_twice
cargo test --lib broadcast_gate_rejects_fallback_build_above_fallback_limit
```

Expected: cost test fails because the compatibility wrapper still delegates to old `compute_cost`.

- [ ] **Step 3: Expand `CostOptions`**

Replace the temporary `CostOptions` with:

```rust
#[derive(Clone, Debug)]
pub(crate) struct CostOptions {
    pub backend_factor: f64,
    pub broadcast_row_limit: f64,
    pub broadcast_byte_limit: f64,
    pub broadcast_right_table_scale_factor: f64,
    pub fallback_broadcast_row_limit: f64,
    pub network_cost: f64,
    pub memory_cost_weight: f64,
}

impl Default for CostOptions {
    fn default() -> Self {
        Self {
            backend_factor: 3.0,
            broadcast_row_limit: 15_000_000.0,
            broadcast_byte_limit: 512.0 * 1024.0 * 1024.0,
            broadcast_right_table_scale_factor: 10.0,
            fallback_broadcast_row_limit: 500_000.0,
            network_cost: NETWORK_COST,
            memory_cost_weight: 0.25,
        }
    }
}
```

- [ ] **Step 4: Implement broadcast gate**

Add:

```rust
pub(crate) fn broadcast_gate_passes(
    probe_stats: &Statistics,
    build_stats: &Statistics,
    options: &CostOptions,
) -> bool {
    let build_rows = build_stats.output_row_count;
    let build_bytes = build_stats.compute_size();
    let probe_bytes = probe_stats.compute_size();

    if build_bytes > options.broadcast_byte_limit {
        return false;
    }

    if build_stats.row_count_confidence
        == crate::sql::optimizer::statistics::Confidence::Fallback
        && build_rows > options.fallback_broadcast_row_limit
    {
        return false;
    }

    let build_is_obviously_tiny =
        probe_bytes >= build_bytes * options.backend_factor * options.broadcast_right_table_scale_factor;
    if build_rows > options.broadcast_row_limit && !build_is_obviously_tiny {
        return false;
    }

    true
}
```

- [ ] **Step 5: Implement child-output-aware hash join cost**

Replace `compute_cost_with_properties` with:

```rust
pub(crate) fn compute_cost_with_properties(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    _child_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
    options: &CostOptions,
) -> Cost {
    match op {
        Operator::PhysicalHashJoin(j) => {
            let probe_stats = child_stats.first().copied();
            let build_stats = child_stats.get(1).copied();
            let probe_size = probe_stats.map(|s| s.compute_size()).unwrap_or(0.0);
            let build_size = build_stats.map(|s| s.compute_size()).unwrap_or(0.0);

            let base_cost = match alt_kind {
                PropertyAlternativeKind::BroadcastJoin => {
                    probe_size
                        + build_size * options.network_cost * options.backend_factor
                        + build_size * options.memory_cost_weight * options.backend_factor
                }
                PropertyAlternativeKind::ShuffleJoin => {
                    probe_size + build_size / options.backend_factor.max(1.0)
                }
                PropertyAlternativeKind::Default => {
                    compute_cost(op, own_stats, child_stats)
                }
            };

            let cost_after_cross = if j.join_type == crate::sql::analysis::JoinKind::Cross {
                base_cost * CROSS_JOIN_COST_PENALTY
            } else {
                base_cost
            };
            if j.other_condition.is_some() {
                cost_after_cross * NON_EQUI_JOIN_COST_PENALTY
            } else {
                cost_after_cross
            }
        }
        _ => compute_cost(op, own_stats, child_stats),
    }
}
```

- [ ] **Step 6: Integrate gate into search**

Remove `BROADCAST_ROW_COUNT_LIMIT` from `search.rs`.

Before optimizing children for an alternative, add:

```rust
                if alt.kind == PropertyAlternativeKind::BroadcastJoin {
                    if let (Some(&probe_group_id), Some(&build_group_id)) =
                        (expr.children.first(), expr.children.get(1))
                    {
                        let probe_stats =
                            stats_for_group(&memo.groups[probe_group_id], memo, &self.table_stats);
                        let build_stats =
                            stats_for_group(&memo.groups[build_group_id], memo, &self.table_stats);
                        if !super::cost::broadcast_gate_passes(
                            &probe_stats,
                            &build_stats,
                            &CostOptions::default(),
                        ) {
                            continue;
                        }
                    }
                }
```

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test --lib child_output_aware_shuffle_join_does_not_charge_network_exchange_twice
cargo test --lib broadcast_gate_rejects_fallback_build_above_fallback_limit
```

Expected: both tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/sql/optimizer/cost.rs src/sql/optimizer/search.rs
git commit -m "feat(optimizer): cost hash join alternatives from child properties"
```

### Task 8: Validate Search Chooses Shuffle For Risky Build And Reuses Hash Output

**Files:**
- Modify: `src/sql/optimizer/search.rs`

- [ ] **Step 1: Write failing search tests**

Add to `search.rs` tests:

```rust
fn make_large_build_inner_join_memo_for_test(
    build_rows: f64,
    confidence: crate::sql::optimizer::statistics::Confidence,
) -> (Memo, GroupId, HashMap<String, TableStatistics>) {
    let (mut memo, root) = make_two_table_inner_join_memo_for_test();
    let build_group = memo.groups[root].physical_exprs[0].children[1];
    memo.groups[build_group].logical_props = Some(crate::sql::optimizer::memo::LogicalProperties {
        output_columns: vec![],
        row_count: build_rows,
        row_count_confidence: confidence,
        column_statistics: HashMap::new(),
        equivalence_classes: Default::default(),
        unique_columns: vec![],
    });
    (memo, root, HashMap::new())
}

fn make_join_over_prepartitioned_children_for_test() -> (Memo, GroupId) {
    let mut memo = Memo::new();
    let left_scan = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalValues(PhysicalValuesOp {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
    });
    let right_scan = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalValues(PhysicalValuesOp {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
    });
    let left = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::shuffle_join([ColumnId(10)]),
        }),
        children: vec![left_scan],
    });
    let right = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::shuffle_join([ColumnId(20)]),
        }),
        children: vec![right_scan],
    });
    let root = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: crate::sql::analysis::JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: test_col(10),
                right: test_col(20),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        }),
        children: vec![left, right],
    });
    (memo, root)
}

#[test]
fn search_rejects_broadcast_for_fallback_large_build() {
    let (memo, root, table_stats) = make_large_build_inner_join_memo_for_test(
        1_000_000.0,
        crate::sql::optimizer::statistics::Confidence::Fallback,
    );
    let mut ctx = SearchContext::new(table_stats);
    let required = PhysicalPropertySet::gather();

    ctx.optimize_group(&memo, root, &required).expect("search");
    let winner = ctx.winners.get(&(root, required)).expect("winner");

    assert_eq!(winner.alt_kind, crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin);
}

#[test]
fn search_reuses_child_shuffle_output_without_top_hash_enforcer() {
    let (memo, root) = make_join_over_prepartitioned_children_for_test();
    let mut ctx = SearchContext::new(Default::default());
    let required = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]),
        ordering: OrderingSpec::Any,
    };

    ctx.optimize_group(&memo, root, &required).expect("search");
    let winner = ctx.winners.get(&(root, required)).expect("winner");

    assert_eq!(winner.alt_kind, crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin);
    assert!(winner.enforcer.is_none(), "partitioned join output should satisfy parent directly");
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib search_rejects_broadcast_for_fallback_large_build
cargo test --lib search_reuses_child_shuffle_output_without_top_hash_enforcer
```

Expected: at least one test fails until search uses the new gate and alternative output consistently.

- [ ] **Step 3: Fix search integration**

Make sure the alternative loop has this order:

```rust
// 1. Gate broadcast from group statistics.
// 2. Optimize children with alt.child_props.
// 3. Collect child winner output properties.
// 4. Compute own cost with child output properties.
// 5. Derive provided output for alt.kind.
// 6. Add only the parent enforcer needed for provided -> required.
```

The code block from Task 6 and Task 7 is the required final shape. Do not call old `derive_required` anywhere in `optimize_group`.

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test --lib search_rejects_broadcast_for_fallback_large_build
cargo test --lib search_reuses_child_shuffle_output_without_top_hash_enforcer
```

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/search.rs
git commit -m "test(optimizer): cover property-driven hash join search"
```

---

## Phase P4 - Extract, EXPLAIN, Runtime Filter, Fragment Builder Cutover

### Task 9: Extract With Recorded Child Properties And Final Execution Props

**Files:**
- Modify: `src/sql/optimizer/extract.rs`
- Modify: `src/sql/optimizer/derive/hash_join.rs`
- Modify: `src/sql/optimizer/physical_plan.rs`

- [ ] **Step 1: Write failing extraction test**

Add to `extract.rs` tests:

```rust
#[test]
fn extract_uses_winner_child_props_instead_of_rederiving() {
    let (memo, root, winners, required) = make_hash_join_winner_with_shuffle_child_props_for_test();

    let plan = extract_best(&memo, root, &required, &winners).expect("extract");

    assert_eq!(
        plan.execution_props.join_distribution,
        Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned)
    );
    assert_eq!(plan.execution_props.child_output_properties.len(), 2);
}
```

Add this private helper in `extract.rs` tests:

```rust
fn make_hash_join_winner_with_shuffle_child_props_for_test()
    -> (
        Memo,
        GroupId,
        HashMap<(GroupId, PhysicalPropertySet), Winner>,
        PhysicalPropertySet,
    )
{
    let mut memo = Memo::new();
    let left = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalValues(PhysicalValuesOp {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
    });
    let right = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalValues(PhysicalValuesOp {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
    });
    let root = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: test_col(10),
                right: test_col(20),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        }),
        children: vec![left, right],
    });

    let required = PhysicalPropertySet::gather();
    let left_req = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_join([ColumnId(10)]),
        ordering: OrderingSpec::Any,
    };
    let right_req = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_join([ColumnId(20)]),
        ordering: OrderingSpec::Any,
    };
    let root_output = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]),
        ordering: OrderingSpec::Any,
    };

    let mut winners = HashMap::new();
    winners.insert(
        (left, left_req.clone()),
        Winner {
            group_id: left,
            expr_index: 0,
            cost: 1.0,
            enforcer: None,
            output: left_req.clone(),
            alt_kind: PropertyAlternativeKind::Default,
            child_props: vec![],
            child_outputs: vec![],
        },
    );
    winners.insert(
        (right, right_req.clone()),
        Winner {
            group_id: right,
            expr_index: 0,
            cost: 1.0,
            enforcer: None,
            output: right_req.clone(),
            alt_kind: PropertyAlternativeKind::Default,
            child_props: vec![],
            child_outputs: vec![],
        },
    );
    winners.insert(
        (root, required.clone()),
        Winner {
            group_id: root,
            expr_index: 0,
            cost: 3.0,
            enforcer: None,
            output: root_output,
            alt_kind: PropertyAlternativeKind::ShuffleJoin,
            child_props: vec![left_req.clone(), right_req.clone()],
            child_outputs: vec![left_req, right_req],
        },
    );

    (memo, root, winners, required)
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib extract_uses_winner_child_props_instead_of_rederiving`

Expected: extraction fails because current code re-derives child requirements.

- [ ] **Step 3: Add execution distribution helper**

In `derive/hash_join.rs`:

```rust
pub(crate) fn join_execution_distribution_for_alternative(
    alt_kind: &PropertyAlternativeKind,
) -> Option<crate::sql::optimizer::physical_plan::JoinExecutionDistribution> {
    match alt_kind {
        PropertyAlternativeKind::BroadcastJoin => {
            Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Broadcast)
        }
        PropertyAlternativeKind::ShuffleJoin => {
            Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned)
        }
        PropertyAlternativeKind::Default => None,
    }
}
```

- [ ] **Step 4: Update extraction**

Replace:

```rust
    let child_reqs = derive_required(&expr.op, required, expr.children.len());
```

with:

```rust
    let child_reqs = winner.child_props.clone();
```

When cloning the operator, patch hash join compatibility metadata:

```rust
    let mut op = match &expr.op {
        Operator::PhysicalCTEAnchor(op) => Operator::PhysicalCTEAnchor(op.clone()),
        other => other.clone(),
    };
    let join_distribution =
        if matches!(op, Operator::PhysicalHashJoin(_)) {
            crate::sql::optimizer::derive::hash_join::join_execution_distribution_for_alternative(
                &winner.alt_kind,
            )
        } else {
            None
        };
    if let (Operator::PhysicalHashJoin(ref mut join), Some(distribution)) =
        (&mut op, join_distribution)
    {
        join.distribution = match distribution {
            crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Broadcast => {
                crate::sql::optimizer::operator::JoinDistribution::Broadcast
            }
            crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned => {
                crate::sql::optimizer::operator::JoinDistribution::Shuffle
            }
            crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Colocate => {
                crate::sql::optimizer::operator::JoinDistribution::Colocate
            }
        };
    }
```

When creating `inner_node`, set:

```rust
        execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps {
            output_property: winner.output.clone(),
            child_output_properties: winner.child_outputs.clone(),
            join_distribution,
        },
```

For enforcer nodes, set:

```rust
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps {
                output_property: required.clone(),
                child_output_properties: vec![winner.output.clone()],
                join_distribution: None,
            },
```

- [ ] **Step 5: Run tests**

Run: `cargo test --lib extract_uses_winner_child_props_instead_of_rederiving`

Expected: test passes.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/extract.rs src/sql/optimizer/derive/hash_join.rs src/sql/optimizer/physical_plan.rs
git commit -m "feat(optimizer): extract property-driven join metadata"
```

### Task 10: Render And Annotate From Execution Metadata

**Files:**
- Modify: `src/sql/explain.rs`
- Modify: `src/sql/optimizer/runtime_filter_pass.rs`

- [ ] **Step 1: Write failing EXPLAIN test**

Add in `explain.rs` tests:

```rust
#[test]
fn explain_hash_join_uses_execution_distribution_metadata() {
    let plan = PhysicalPlanNode {
        op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        }),
        children: vec![],
        stats: Statistics::default(),
        output_columns: vec![],
        execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps {
            output_property: PhysicalPropertySet::any(),
            child_output_properties: vec![],
            join_distribution: Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned),
        },
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    };

    let text = explain_physical(&plan, ExplainLevel::Verbose).join("\n");
    assert!(text.contains("HASH JOIN (PARTITIONED, INNER"));
    assert!(!text.contains("UNKNOWN"));
}
```

Add in `runtime_filter_pass.rs` tests:

```rust
#[test]
fn runtime_filter_uses_execution_distribution_metadata() {
    let mut plan = test_plans::inner_join_two_scans();
    plan.execution_props.join_distribution =
        Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned);
    if let Operator::PhysicalHashJoin(ref mut join) = plan.op {
        join.distribution = JoinDistribution::Unknown;
    }

    annotate(&mut plan, &OptimizerOptions::default());

    assert!(
        plan.build_runtime_filters
            .iter()
            .all(|rf| matches!(rf.distribution, JoinDistribution::Shuffle))
    );
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib explain_hash_join_uses_execution_distribution_metadata
cargo test --lib runtime_filter_uses_execution_distribution_metadata
```

Expected: tests fail because both paths still read `PhysicalHashJoinOp.distribution`.

- [ ] **Step 3: Update EXPLAIN**

Add helper in `explain.rs`:

```rust
fn join_distribution_label(
    node: &crate::sql::optimizer::physical_plan::PhysicalPlanNode,
    fallback: &JoinDistribution,
) -> &'static str {
    use crate::sql::optimizer::physical_plan::JoinExecutionDistribution;
    match node.execution_props.join_distribution {
        Some(JoinExecutionDistribution::Broadcast) => "BROADCAST",
        Some(JoinExecutionDistribution::Partitioned) => "PARTITIONED",
        Some(JoinExecutionDistribution::Colocate) => "COLOCATE",
        None => match fallback {
            JoinDistribution::Broadcast => "BROADCAST",
            JoinDistribution::Shuffle => "PARTITIONED",
            JoinDistribution::Colocate => "COLOCATE",
            JoinDistribution::Unknown => "UNKNOWN",
        },
    }
}
```

Replace the hash join `dist` match with:

```rust
            let dist = join_distribution_label(node, &op.distribution);
```

In the `PhysicalDistribution` EXPLAIN match, add:

```rust
                DistributionSpec::Broadcast => "BROADCAST EXCHANGE".to_string(),
```

- [ ] **Step 4: Update runtime filter distribution helper**

In `runtime_filter_pass.rs`, add:

```rust
fn join_distribution_for_runtime_filter(
    node: &PhysicalPlanNode,
    fallback: &JoinDistribution,
) -> JoinDistribution {
    use crate::sql::optimizer::physical_plan::JoinExecutionDistribution;
    match node.execution_props.join_distribution {
        Some(JoinExecutionDistribution::Broadcast) => JoinDistribution::Broadcast,
        Some(JoinExecutionDistribution::Partitioned) => JoinDistribution::Shuffle,
        Some(JoinExecutionDistribution::Colocate) => JoinDistribution::Colocate,
        None => fallback.clone(),
    }
}
```

Replace:

```rust
    let distribution = join.distribution.clone();
```

with:

```rust
    let distribution = join_distribution_for_runtime_filter(node, &join.distribution);
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test --lib explain_hash_join_uses_execution_distribution_metadata
cargo test --lib runtime_filter_uses_execution_distribution_metadata
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/sql/explain.rs src/sql/optimizer/runtime_filter_pass.rs
git commit -m "feat(optimizer): render join distribution from execution metadata"
```

### Task 11: Cut Fragment Builder To Execution Metadata And Broadcast Edge

**Files:**
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Write failing fragment builder tests**

Add to `fragment_builder.rs` tests:

```rust
#[test]
fn build_broadcast_distribution_edge_uses_unpartitioned_stream_partition() {
    let plan = PhysicalPlanNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::Broadcast,
        }),
        children: vec![values_plan_for_test(vec![OutputColumn {
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        }])],
        stats: Statistics::default(),
        output_columns: vec![OutputColumn {
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        }],
        execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    };

    let built = PlanFragmentBuilder::new_for_test().build_multi_fragment(&plan).expect("build");
    assert_eq!(built.edges.len(), 1);
    assert_eq!(
        built.edges[0].output_partition.type_,
        crate::partitions::TPartitionType::UNPARTITIONED
    );
}

#[test]
fn hash_join_thrift_distribution_uses_execution_metadata_partitioned() {
    let mut plan = mixed_starrocks_iceberg_join_plan();
    plan.execution_props.join_distribution =
        Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned);
    if let Operator::PhysicalHashJoin(ref mut join) = plan.op {
        join.distribution = JoinDistribution::Unknown;
    }

    let built = PlanFragmentBuilder::new_for_test().build_multi_fragment(&plan).expect("build");
    let root = built
        .fragment_results
        .iter()
        .find(|fragment| fragment.fragment_id == built.root_fragment_id)
        .expect("root fragment");
    let join = root
        .plan
        .nodes
        .iter()
        .find_map(|node| node.hash_join_node.as_ref())
        .expect("hash join node");

    assert_eq!(
        join.distribution_mode,
        Some(crate::plan_nodes::TJoinDistributionMode::PARTITIONED)
    );
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib build_broadcast_distribution_edge_uses_unpartitioned_stream_partition
cargo test --lib hash_join_thrift_distribution_uses_execution_metadata_partitioned
```

Expected: broadcast edge test fails because `DistributionSpec::Broadcast` is not handled; join thrift test fails because `nodes::build_hash_join_node` defaults to `BROADCAST`.

- [ ] **Step 3: Make hash join node builder accept distribution mode**

In `nodes.rs`, change signature:

```rust
pub(crate) fn build_hash_join_node(
    node_id: i32,
    left_tuple_ids: &[i32],
    right_tuple_ids: &[i32],
    join_op: plan_nodes::TJoinOp,
    eq_join_conjuncts: Vec<plan_nodes::TEqJoinCondition>,
    other_join_conjuncts: Vec<exprs::TExpr>,
    distribution_mode: plan_nodes::TJoinDistributionMode,
) -> plan_nodes::TPlanNode {
```

Replace the hard-coded field:

```rust
        distribution_mode: Some(distribution_mode),
```

Update the call in `fragment_builder.rs`:

```rust
        let distribution_mode = join_distribution_mode(node, &op.distribution);
        let mut join_plan_node = nodes::build_hash_join_node(
            join_node_id,
            &left.tuple_ids,
            &right.tuple_ids,
            join_op,
            eq_join_conjuncts,
            other_join_conjuncts,
            distribution_mode,
        );
```

Add helper:

```rust
fn join_distribution_mode(
    node: &PhysicalPlanNode,
    fallback: &JoinDistribution,
) -> plan_nodes::TJoinDistributionMode {
    use crate::sql::optimizer::physical_plan::JoinExecutionDistribution;
    match node.execution_props.join_distribution {
        Some(JoinExecutionDistribution::Broadcast) => plan_nodes::TJoinDistributionMode::BROADCAST,
        Some(JoinExecutionDistribution::Partitioned) => {
            plan_nodes::TJoinDistributionMode::PARTITIONED
        }
        Some(JoinExecutionDistribution::Colocate) => plan_nodes::TJoinDistributionMode::COLOCATE,
        None => match fallback {
            JoinDistribution::Broadcast => plan_nodes::TJoinDistributionMode::BROADCAST,
            JoinDistribution::Shuffle => plan_nodes::TJoinDistributionMode::PARTITIONED,
            JoinDistribution::Colocate => plan_nodes::TJoinDistributionMode::COLOCATE,
            JoinDistribution::Unknown => plan_nodes::TJoinDistributionMode::BROADCAST,
        },
    }
}
```

- [ ] **Step 4: Map broadcast distribution edge**

In `build_output_partition`, add:

```rust
            crate::sql::optimizer::property::DistributionSpec::Broadcast => {
                Ok(unpartitioned_stream_partition())
            }
```

This maps optimizer broadcast property to thrift `UNPARTITIONED`; NovaRocks' `DataStreamSink` broadcasts `UNPARTITIONED` chunks to all destinations.

- [ ] **Step 5: Update RF layout lowering**

Change `rf_layout_for_distribution` to accept `JoinExecutionDistribution`:

```rust
fn rf_layout_for_execution_distribution(
    distribution: crate::sql::optimizer::physical_plan::JoinExecutionDistribution,
) -> (
    crate::runtime_filter::TRuntimeFilterBuildJoinMode,
    crate::runtime_filter::TRuntimeFilterLayoutMode,
    crate::runtime_filter::TRuntimeFilterLayoutMode,
) {
    use crate::runtime_filter::{TRuntimeFilterBuildJoinMode, TRuntimeFilterLayoutMode};
    use crate::sql::optimizer::physical_plan::JoinExecutionDistribution;
    match distribution {
        JoinExecutionDistribution::Broadcast => (
            TRuntimeFilterBuildJoinMode::BORADCAST,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::SINGLETON,
        ),
        JoinExecutionDistribution::Partitioned => (
            TRuntimeFilterBuildJoinMode::PARTITIONED,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L,
        ),
        JoinExecutionDistribution::Colocate => (
            TRuntimeFilterBuildJoinMode::COLOCATE,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L,
        ),
    }
}
```

Where RF descriptors are built, derive the distribution from `node.execution_props.join_distribution.expect("hash join execution distribution")`; if absent, map the legacy `RuntimeFilterDesc.distribution` for compatibility:

```rust
fn legacy_rf_distribution_to_execution(
    distribution: &JoinDistribution,
) -> crate::sql::optimizer::physical_plan::JoinExecutionDistribution {
    match distribution {
        JoinDistribution::Broadcast | JoinDistribution::Unknown => {
            crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Broadcast
        }
        JoinDistribution::Shuffle => {
            crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned
        }
        JoinDistribution::Colocate => {
            crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Colocate
        }
    }
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test --lib build_broadcast_distribution_edge_uses_unpartitioned_stream_partition
cargo test --lib hash_join_thrift_distribution_uses_execution_metadata_partitioned
```

Expected: both tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/sql/codegen/nodes.rs src/sql/codegen/fragment_builder.rs
git commit -m "feat(optimizer): lower join distribution from execution metadata"
```

---

## Phase P5 - SQL Goldens And Validation

### Task 12: Add Optimizer SQL Goldens

**Files:**
- Create: `sql-tests/optimizer/sql/distribution_join_broadcast_gate.sql`
- Create: `sql-tests/optimizer/sql/distribution_join_shuffle_reuse.sql`
- Create: `sql-tests/optimizer/sql/distribution_join_downstream_reuse.sql`
- Create: `sql-tests/optimizer/sql/distribution_join_fallback_stats_gate.sql`
- Create/update generated `.result` files under `sql-tests/optimizer/result/`

- [ ] **Step 1: Add broadcast gate SQL case**

Create `sql-tests/optimizer/sql/distribution_join_broadcast_gate.sql`:

```sql
-- @tags=optimizer,oq8,distribution
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=HASH JOIN (BROADCAST
DROP TABLE IF EXISTS ${case_db}.oq8_probe_big;
DROP TABLE IF EXISTS ${case_db}.oq8_build_big;
CREATE TABLE ${case_db}.oq8_probe_big (k INT, v INT);
CREATE TABLE ${case_db}.oq8_build_big (k INT, v INT);
INSERT INTO ${case_db}.oq8_probe_big
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq8_build_big
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 1000));

EXPLAIN VERBOSE
SELECT p.k, b.v
FROM ${case_db}.oq8_probe_big p
INNER JOIN ${case_db}.oq8_build_big b ON p.k = b.k;
```

- [ ] **Step 2: Add shuffle reuse SQL case**

Create `sql-tests/optimizer/sql/distribution_join_shuffle_reuse.sql`:

```sql
-- @tags=optimizer,oq8,distribution
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=HASH EXCHANGE (source: ShuffleJoin
DROP TABLE IF EXISTS ${case_db}.oq8_reuse_l;
DROP TABLE IF EXISTS ${case_db}.oq8_reuse_r;
CREATE TABLE ${case_db}.oq8_reuse_l (k INT, v INT);
CREATE TABLE ${case_db}.oq8_reuse_r (k INT, v INT);
INSERT INTO ${case_db}.oq8_reuse_l VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.oq8_reuse_r VALUES (1, 100), (2, 200), (3, 300);

SET disable_optimizer_rules = 'JoinCommutativity';

EXPLAIN VERBOSE
SELECT l.k, SUM(r.v) AS total_v
FROM ${case_db}.oq8_reuse_l l
INNER JOIN ${case_db}.oq8_reuse_r r ON l.k = r.k
GROUP BY l.k;

SET disable_optimizer_rules = '';
```

- [ ] **Step 3: Add downstream reuse SQL case**

Create `sql-tests/optimizer/sql/distribution_join_downstream_reuse.sql`:

```sql
-- @tags=optimizer,oq8,distribution
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=HASH AGGREGATE (GLOBAL
DROP TABLE IF EXISTS ${case_db}.oq8_down_l;
DROP TABLE IF EXISTS ${case_db}.oq8_down_r;
CREATE TABLE ${case_db}.oq8_down_l (k INT, v INT);
CREATE TABLE ${case_db}.oq8_down_r (k INT, w INT);
INSERT INTO ${case_db}.oq8_down_l VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.oq8_down_r VALUES (1, 100), (2, 200), (3, 300);

EXPLAIN VERBOSE
SELECT l.k, COUNT(*) AS cnt
FROM ${case_db}.oq8_down_l l
INNER JOIN ${case_db}.oq8_down_r r ON l.k = r.k
GROUP BY l.k;
```

- [ ] **Step 4: Add fallback stats gate SQL case**

Create `sql-tests/optimizer/sql/distribution_join_fallback_stats_gate.sql`:

```sql
-- @tags=optimizer,oq8,distribution
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=HASH JOIN (BROADCAST
DROP TABLE IF EXISTS ${case_db}.lineitem;
DROP TABLE IF EXISTS ${case_db}.orders;
CREATE TABLE ${case_db}.lineitem (k INT, v INT);
CREATE TABLE ${case_db}.orders (k INT, w INT);
INSERT INTO ${case_db}.lineitem VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.orders VALUES (1, 100), (2, 200);

SET disable_optimizer_rules = 'JoinCommutativity';

EXPLAIN VERBOSE
SELECT l.k, r.w
FROM ${case_db}.lineitem l
INNER JOIN ${case_db}.orders r ON l.k = r.k;

SET disable_optimizer_rules = '';
```

- [ ] **Step 5: Record and verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only distribution_join_broadcast_gate,distribution_join_shuffle_reuse,distribution_join_downstream_reuse,distribution_join_fallback_stats_gate \
  --mode record
```

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only distribution_join_broadcast_gate,distribution_join_shuffle_reuse,distribution_join_downstream_reuse,distribution_join_fallback_stats_gate \
  --mode verify
```

Expected: runner reports all four cases pass.

- [ ] **Step 6: Commit**

```bash
git add sql-tests/optimizer/sql/distribution_join_broadcast_gate.sql sql-tests/optimizer/sql/distribution_join_shuffle_reuse.sql sql-tests/optimizer/sql/distribution_join_downstream_reuse.sql sql-tests/optimizer/sql/distribution_join_fallback_stats_gate.sql sql-tests/optimizer/result/distribution_join_broadcast_gate.result sql-tests/optimizer/result/distribution_join_shuffle_reuse.result sql-tests/optimizer/result/distribution_join_downstream_reuse.result sql-tests/optimizer/result/distribution_join_fallback_stats_gate.result
git commit -m "test(optimizer): add OQ-8 distribution join goldens"
```

### Task 13: Final Verification And Plan-Shape Audit

**Files:**
- Modify: files changed by `cargo fmt` or SQL golden record if verification produces deterministic formatting/result updates.

- [ ] **Step 1: Format**

Run: `cargo fmt`

Expected: exits 0.

- [ ] **Step 2: Build**

Run: `cargo build`

Expected: exits 0.

- [ ] **Step 3: Focused Rust test sweep**

Run:

```bash
cargo test --lib broadcast_distribution_satisfies_only_broadcast_and_any
cargo test --lib default_required_alternative_wraps_legacy_deriver
cargo test --lib hash_join_required_alternatives_include_broadcast_and_shuffle
cargo test --lib winner_records_hash_join_alternative_and_child_properties
cargo test --lib child_output_aware_shuffle_join_does_not_charge_network_exchange_twice
cargo test --lib extract_uses_winner_child_props_instead_of_rederiving
cargo test --lib explain_hash_join_uses_execution_distribution_metadata
cargo test --lib hash_join_thrift_distribution_uses_execution_metadata_partitioned
```

Expected: all listed tests pass.

- [ ] **Step 4: Optimizer SQL suite focused verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
```

Expected: optimizer suite passes.

- [ ] **Step 5: Representative query plan audit**

Run the existing suites with EXPLAIN-focused cases or manually compare plans for:

```text
tpc-h/q9
tpc-h/q20
tpc-ds/q4
tpc-ds/q64
tpc-ds/q85
ssb sanity queries
```

Expected direction:
- TPC-H/TPC-DS representatives show fewer large `HASH JOIN (BROADCAST` labels and more `HASH JOIN (PARTITIONED` labels where the build side is not safely small.
- SSB small-dimension joins keep broadcast when the build side remains clearly tiny.
- No plan contains `HASH JOIN (UNKNOWN`.

- [ ] **Step 6: Final commit if verification-only edits occurred**

If Step 1 changed formatting or Step 4 required golden normalization:

```bash
git add src/sql/optimizer src/sql/codegen src/sql/explain.rs sql-tests/optimizer/result/distribution_join_broadcast_gate.result sql-tests/optimizer/result/distribution_join_shuffle_reuse.result sql-tests/optimizer/result/distribution_join_downstream_reuse.result sql-tests/optimizer/result/distribution_join_fallback_stats_gate.result
git commit -m "chore(optimizer): finalize OQ-8 verification artifacts"
```

If no files changed, do not create an empty commit.

---

## Acceptance Checklist

- [ ] `DistributionSpec::Broadcast` exists and does not satisfy `Gather` or hash requirements.
- [ ] `JoinToHashJoin` emits exactly one `PhysicalHashJoin` memo expression.
- [ ] Hash join broadcast/shuffle are enumerated through `ChildRequirementAlternative`.
- [ ] `SearchContext` stores `Winner.alt_kind`, `Winner.child_props`, and `Winner.child_outputs`.
- [ ] `extract_best` uses recorded `winner.child_props`; it does not re-derive child requirements.
- [ ] Hash join cost reads `PropertyAlternativeKind` and child output properties.
- [ ] Broadcast gate uses row count, byte size, and OQ-12 confidence.
- [ ] EXPLAIN, runtime filter pass, and fragment builder read final extracted execution metadata.
- [ ] `PhysicalDistribution(Broadcast)` creates a broadcast exchange edge represented as thrift `UNPARTITIONED`.
- [ ] Focused Rust tests, `cargo build`, and `sql-tests/optimizer` verification pass.
