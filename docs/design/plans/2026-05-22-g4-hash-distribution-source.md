# G4 Hash Distribution Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add source-aware hash distribution properties for `ShuffleAgg` and `ShuffleJoin`, fixing the current over-broad hash satisfies behavior without introducing colocate or bucket semantics.

**Architecture:** `DistributionSpec::HashPartitioned` becomes a struct-like variant carrying `cols` and `HashSource`. Aggregate/window/partitioned-sort requirements use `ShuffleAgg`; shuffle hash join requirements/output use `ShuffleJoin`; generic distribution enforcers output the required source unchanged. Satisfies logic is centralized in `src/sql/optimizer/property.rs`, while codegen continues to use only the hash columns to build exchange partition expressions.

**Tech Stack:** Rust, NovaRocks Cascades optimizer, SQL test runner, optimizer golden SQL suite.

---

## File Structure

- Modify `src/sql/optimizer/property.rs`: define `HashSource`, change `DistributionSpec::HashPartitioned`, add constructors/accessors, and replace the global contains rule with source-aware satisfies tests.
- Modify `src/sql/optimizer/derive/hash_aggregate.rs`: aggregate output and Global/DistinctGlobal required properties use `ShuffleAgg`.
- Modify `src/sql/optimizer/derive/window.rs`: window partition output/required properties use `ShuffleAgg`.
- Modify `src/sql/optimizer/derive/sort.rs`: analytic precursor sort output/required properties use `ShuffleAgg`; top-level sort remains `Gather`.
- Modify `src/sql/optimizer/derive/hash_join.rs`: shuffle hash join output/required properties use `ShuffleJoin`; broadcast/colocate legacy paths preserve the child source when enriching eq-equivalent columns.
- Modify `src/sql/optimizer/derive/enforcer.rs`: retain current generic enforcer behavior; tests assert output source equals required source.
- Modify `src/sql/explain.rs`: display hash source in `PhysicalDistribution` labels.
- Modify `src/sql/codegen/fragment_builder.rs`: update pattern matching to ignore `source` while building partition expressions from `cols`.
- Modify `src/sql/optimizer/search.rs`, `src/sql/optimizer/cost.rs`, `src/sql/optimizer/extract.rs`, and any compile-reported tests using direct `HashPartitioned` patterns.
- Add `sql-tests/optimizer/sql/g4_hash_distribution_source.sql`: plan-shape coverage for `ShuffleJoin` not satisfying narrower `ShuffleAgg`.
- Add `sql-tests/optimizer/result/g4_hash_distribution_source.result`: recorded explain/result output for the new optimizer SQL case.

---

### Task 1: Source-Aware Property Model

**Files:**
- Modify: `src/sql/optimizer/property.rs`

- [ ] **Step 1: Write failing property tests**

Add these tests inside `#[cfg(test)] mod tests` in `src/sql/optimizer/property.rs`:

```rust
#[test]
fn shuffle_agg_subset_satisfies_finer_shuffle_agg_requirement() {
    let provided = DistributionSpec::shuffle_agg([ColumnId(1)]);
    let required = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]);
    assert!(provided.satisfies(&required));
}

#[test]
fn shuffle_agg_superset_does_not_satisfy_coarser_shuffle_agg_requirement() {
    let provided = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]);
    let required = DistributionSpec::shuffle_agg([ColumnId(1)]);
    assert!(!provided.satisfies(&required));
}

#[test]
fn shuffle_join_requires_exact_ordered_keys() {
    let provided = DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]);
    let exact = DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]);
    let reordered = DistributionSpec::shuffle_join([ColumnId(2), ColumnId(1)]);
    let prefix = DistributionSpec::shuffle_join([ColumnId(1)]);

    assert!(provided.satisfies(&exact));
    assert!(!provided.satisfies(&reordered));
    assert!(!provided.satisfies(&prefix));
}

#[test]
fn shuffle_join_does_not_satisfy_narrower_shuffle_agg_requirement() {
    let provided = DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]);
    let required = DistributionSpec::shuffle_agg([ColumnId(10)]);
    assert!(!provided.satisfies(&required));
}

#[test]
fn cross_source_rules_are_conservative() {
    let agg_exact = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]);
    let join_exact = DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]);
    let join_finer = DistributionSpec::shuffle_join([ColumnId(1)]);
    let agg_finer_required = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2), ColumnId(3)]);

    assert!(agg_exact.satisfies(&join_exact));
    assert!(join_finer.satisfies(&agg_finer_required));
    assert!(!join_exact.satisfies(&DistributionSpec::shuffle_agg([ColumnId(1)])));
}

#[test]
fn hash_constructors_drop_unset_and_dedup_preserving_first_seen_order() {
    let spec = DistributionSpec::shuffle_agg([
        ColumnId(3),
        ColumnId::UNSET,
        ColumnId(1),
        ColumnId(3),
        ColumnId(2),
    ]);

    match spec {
        DistributionSpec::HashPartitioned { cols, source } => {
            assert_eq!(source, HashSource::ShuffleAgg);
            assert_eq!(cols, vec![ColumnId(3), ColumnId(1), ColumnId(2)]);
        }
        other => panic!("expected hash distribution, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run the failing test command**

Run:

```bash
cargo test property::tests::shuffle_agg_subset_satisfies_finer_shuffle_agg_requirement --lib
```

Expected: compile fails with errors mentioning missing `DistributionSpec::shuffle_agg` or missing `HashSource`.

- [ ] **Step 3: Add `HashSource`, constructors, accessors, and source-aware satisfies**

Replace the current `DistributionSpec` definition and `impl DistributionSpec` in `src/sql/optimizer/property.rs` with:

```rust
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub(crate) enum HashSource {
    ShuffleAgg,
    ShuffleJoin,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    HashPartitioned {
        cols: Vec<ColumnId>,
        source: HashSource,
    },
}

impl DistributionSpec {
    pub(crate) fn shuffle_agg<I>(cols: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        Self::hash_partitioned(cols, HashSource::ShuffleAgg)
    }

    pub(crate) fn shuffle_join<I>(cols: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        Self::hash_partitioned(cols, HashSource::ShuffleJoin)
    }

    pub(crate) fn hash_partitioned<I>(cols: I, source: HashSource) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let mut normalized = Vec::new();
        for col in cols {
            if col == ColumnId::UNSET || normalized.contains(&col) {
                continue;
            }
            normalized.push(col);
        }
        if normalized.is_empty() {
            DistributionSpec::Any
        } else {
            DistributionSpec::HashPartitioned {
                cols: normalized,
                source,
            }
        }
    }

    pub(crate) fn hash_cols(&self) -> Option<&[ColumnId]> {
        match self {
            DistributionSpec::HashPartitioned { cols, .. } => Some(cols.as_slice()),
            _ => None,
        }
    }

    pub(crate) fn hash_source(&self) -> Option<HashSource> {
        match self {
            DistributionSpec::HashPartitioned { source, .. } => Some(*source),
            _ => None,
        }
    }

    pub fn satisfies(&self, required: &DistributionSpec) -> bool {
        match required {
            DistributionSpec::Any => true,
            DistributionSpec::Gather => matches!(self, DistributionSpec::Gather),
            DistributionSpec::HashPartitioned {
                cols: required_cols,
                source: required_source,
            } => {
                let DistributionSpec::HashPartitioned {
                    cols: provided_cols,
                    source: provided_source,
                } = self
                else {
                    return false;
                };
                match (*provided_source, *required_source) {
                    (HashSource::ShuffleAgg, HashSource::ShuffleAgg) => {
                        hash_cols_subset(provided_cols, required_cols)
                    }
                    (HashSource::ShuffleJoin, HashSource::ShuffleJoin) => {
                        provided_cols == required_cols
                    }
                    (HashSource::ShuffleAgg, HashSource::ShuffleJoin) => {
                        provided_cols == required_cols
                    }
                    (HashSource::ShuffleJoin, HashSource::ShuffleAgg) => {
                        hash_cols_subset(provided_cols, required_cols)
                    }
                }
            }
        }
    }
}

fn hash_cols_subset(left: &[ColumnId], right: &[ColumnId]) -> bool {
    left.iter().all(|col| right.contains(col))
}
```

- [ ] **Step 4: Update existing property tests to use constructors**

Replace old direct constructions in `src/sql/optimizer/property.rs` tests:

```rust
DistributionSpec::HashPartitioned(vec![ColumnId(1), ColumnId(2)])
```

with source-specific constructors. Use `shuffle_agg` for aggregate-like tests and `shuffle_join` for join strictness tests. Remove or rewrite the old test named `hash_partitioned_satisfies_when_provider_has_superset`, because it encodes the bug G4 is fixing.

- [ ] **Step 5: Run property tests**

Run:

```bash
cargo test property::tests --lib
```

Expected: property tests compile and pass after downstream direct match sites are updated in Task 2.

Do not commit at the end of Task 1 if the crate does not compile yet. Continue directly to Task 2.

---

### Task 2: Mechanical Call-Site Migration

**Files:**
- Modify: `src/sql/optimizer/derive/hash_aggregate.rs`
- Modify: `src/sql/optimizer/derive/window.rs`
- Modify: `src/sql/optimizer/derive/sort.rs`
- Modify: `src/sql/optimizer/derive/hash_join.rs`
- Modify: `src/sql/optimizer/derive/enforcer.rs`
- Modify: `src/sql/optimizer/search.rs`
- Modify: `src/sql/explain.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/optimizer/cost.rs`
- Modify: `src/sql/optimizer/extract.rs`

- [ ] **Step 1: Find every direct `HashPartitioned` construction and match**

Run:

```bash
rg -n "HashPartitioned" src/sql/optimizer src/sql/codegen src/sql/explain.rs
```

Expected: each match is in one of the files listed above or a compile-reported test module.

- [ ] **Step 2: Update aggregate/window/sort constructions to `ShuffleAgg`**

Use this pattern in `src/sql/optimizer/derive/hash_aggregate.rs`, `src/sql/optimizer/derive/window.rs`, and `src/sql/optimizer/derive/sort.rs`:

```rust
PhysicalPropertySet {
    distribution: DistributionSpec::shuffle_agg(cols),
    ordering: OrderingSpec::Any,
}
```

For sort output, keep the top-level sort path unchanged:

```rust
let distribution = if self.analytic_partition_exprs.is_empty() {
    DistributionSpec::Gather
} else {
    let partition_cols = typed_exprs_to_column_ids(&self.analytic_partition_exprs);
    if partition_cols.len() == self.analytic_partition_exprs.len() {
        DistributionSpec::shuffle_agg(partition_cols)
    } else {
        DistributionSpec::Gather
    }
};
```

- [ ] **Step 3: Update shuffle join constructions to `ShuffleJoin`**

In `src/sql/optimizer/derive/hash_join.rs`, replace shuffle join output and required construction with:

```rust
let distribution = if cols.is_empty() {
    DistributionSpec::Any
} else {
    DistributionSpec::shuffle_join(cols)
};
```

For the child required properties, keep the current all-eq-cols contract for this PR:

```rust
let distribution = if all_cols.is_empty() {
    DistributionSpec::Any
} else {
    DistributionSpec::shuffle_join(all_cols.clone())
};
```

Use the non-cloned `all_cols` for the second child.

- [ ] **Step 4: Preserve source when enriching broadcast/legacy colocate output**

In `src/sql/optimizer/derive/hash_join.rs`, update the broadcast/colocate branch from a tuple match to:

```rust
let distribution = match left.distribution {
    DistributionSpec::HashPartitioned { cols, source } => DistributionSpec::hash_partitioned(
        expand_with_eq_equivalents(&cols, &self.eq_conditions),
        source,
    ),
    other => other,
};
```

- [ ] **Step 5: Update pattern matches that only need hash columns**

In `src/sql/explain.rs`, use:

```rust
DistributionSpec::HashPartitioned { cols, source } => {
    let col_names: Vec<String> = cols.iter().map(|c| format!("{}", c)).collect();
    format!("HASH EXCHANGE (source: {:?}, hash: [{}])", source, col_names.join(", "))
}
```

In `src/sql/codegen/fragment_builder.rs`, use:

```rust
crate::sql::optimizer::property::DistributionSpec::HashPartitioned { cols, .. } => {
    let mut partition_exprs = Vec::new();
    let mut used_ids = std::collections::HashSet::new();
    let mut used_names = std::collections::HashSet::new();
    for col_id in cols.iter() {
        // keep the existing body unchanged
    }
    // keep the existing trailing validation and partition construction unchanged
}
```

In tests that assert only columns, use:

```rust
match &props.distribution {
    DistributionSpec::HashPartitioned { cols, source } => {
        assert_eq!(*source, HashSource::ShuffleAgg);
        assert_eq!(cols.as_slice(), &[ColumnId(3)]);
    }
    other => panic!("expected ShuffleAgg hash distribution, got {other:?}"),
}
```

- [ ] **Step 6: Run the compile-focused test command**

Run:

```bash
cargo test property::tests --lib
```

Expected: PASS. If compile errors remain, update the reported direct `HashPartitioned(...)` construction or tuple pattern to the struct-like variant or helper constructor.

- [ ] **Step 7: Commit property model and mechanical migration**

Run:

```bash
git add src/sql/optimizer/property.rs \
  src/sql/optimizer/derive/hash_aggregate.rs \
  src/sql/optimizer/derive/window.rs \
  src/sql/optimizer/derive/sort.rs \
  src/sql/optimizer/derive/hash_join.rs \
  src/sql/optimizer/derive/enforcer.rs \
  src/sql/optimizer/search.rs \
  src/sql/explain.rs \
  src/sql/codegen/fragment_builder.rs \
  src/sql/optimizer/cost.rs \
  src/sql/optimizer/extract.rs
git commit -m "feat(optimizer): add hash distribution source"
```

Expected: commit succeeds with only optimizer/codegen/explain files staged.

---

### Task 3: Derive Tests for Producer and Enforcer Semantics

**Files:**
- Modify: `src/sql/optimizer/derive/hash_aggregate.rs`
- Modify: `src/sql/optimizer/derive/window.rs`
- Modify: `src/sql/optimizer/derive/sort.rs`
- Modify: `src/sql/optimizer/derive/hash_join.rs`
- Modify: `src/sql/optimizer/derive/enforcer.rs`

- [ ] **Step 1: Add aggregate source assertions**

Update aggregate tests so every hash distribution match checks `HashSource::ShuffleAgg`:

```rust
match &props.distribution {
    DistributionSpec::HashPartitioned { cols, source } => {
        assert_eq!(*source, HashSource::ShuffleAgg);
        assert_eq!(cols.as_slice(), &[ColumnId(3)]);
    }
    other => panic!("expected ShuffleAgg([c3]), got {other:?}"),
}
```

Also update the DistinctGlobal required test:

```rust
match &reqs[0].distribution {
    DistributionSpec::HashPartitioned { cols, source } => {
        assert_eq!(*source, HashSource::ShuffleAgg);
        assert_eq!(cols.len(), 2, "Hash on both g and x");
    }
    other => panic!("expected ShuffleAgg hash distribution, got {other:?}"),
}
```

- [ ] **Step 2: Add window and sort source assertions**

In `src/sql/optimizer/derive/window.rs`, update the partition output test:

```rust
match &props.distribution {
    DistributionSpec::HashPartitioned { cols, source } => {
        assert_eq!(*source, HashSource::ShuffleAgg);
        assert_eq!(cols.as_slice(), &[ColumnId(2)]);
    }
    other => panic!("expected ShuffleAgg([c2]), got {other:?}"),
}
```

In `src/sql/optimizer/derive/sort.rs`, add a test:

```rust
#[test]
fn analytic_sort_requires_shuffle_agg_on_partition_columns() {
    let partition = crate::sql::analysis::TypedExpr {
        kind: crate::sql::analysis::ExprKind::ColumnRef {
            column_id: ColumnId(7),
            qualifier: None,
            column: "k".into(),
        },
        data_type: arrow::datatypes::DataType::Int32,
        nullable: false,
    };
    let op = PhysicalSortOp {
        items: vec![],
        analytic_partition_exprs: vec![partition],
    };

    let reqs = op.derive_required(&PhysicalPropertySet::any(), 1);
    match &reqs[0].distribution {
        DistributionSpec::HashPartitioned { cols, source } => {
            assert_eq!(*source, HashSource::ShuffleAgg);
            assert_eq!(cols.as_slice(), &[ColumnId(7)]);
        }
        other => panic!("expected ShuffleAgg([c7]), got {other:?}"),
    }
}
```

- [ ] **Step 3: Add shuffle join source assertions**

In `src/sql/optimizer/derive/hash_join.rs`, update shuffle join tests to assert `HashSource::ShuffleJoin`:

```rust
match &out.distribution {
    DistributionSpec::HashPartitioned { cols, source } => {
        assert_eq!(*source, HashSource::ShuffleJoin);
        assert_eq!(cols.as_slice(), &[ColumnId(10), ColumnId(20)]);
    }
    other => panic!("expected ShuffleJoin([10, 20]), got {other:?}"),
}
```

For broadcast preserves-left tests, assert the source is preserved:

```rust
let left = PhysicalPropertySet {
    distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
    ordering: OrderingSpec::Any,
};
let right = PhysicalPropertySet {
    distribution: DistributionSpec::shuffle_agg([ColumnId(20)]),
    ordering: OrderingSpec::Any,
};
let out = op.derive_output(&[&left, &right]);
match &out.distribution {
    DistributionSpec::HashPartitioned { cols, source } => {
        assert_eq!(*source, HashSource::ShuffleAgg);
        assert_eq!(cols.as_slice(), &[ColumnId(10), ColumnId(20)]);
    }
    other => panic!("expected left ShuffleAgg source to be preserved, got {other:?}"),
}
```

- [ ] **Step 4: Add enforcer source assertion**

In `src/sql/optimizer/derive/enforcer.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::PhysicalDistributionOp;
    use crate::sql::optimizer::property::{DistributionSpec, HashSource};

    #[test]
    fn distribution_enforcer_outputs_required_source() {
        let op = PhysicalDistributionOp {
            spec: DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]),
        };
        let props = op.derive_output(&[]);
        match props.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(source, HashSource::ShuffleJoin);
                assert_eq!(cols, vec![ColumnId(1), ColumnId(2)]);
            }
            other => panic!("expected ShuffleJoin enforcer output, got {other:?}"),
        }
    }
}
```

- [ ] **Step 5: Run derive tests**

Run:

```bash
cargo test optimizer::derive --lib
```

Expected: PASS.

- [ ] **Step 6: Commit derive semantics tests**

Run:

```bash
git add src/sql/optimizer/derive/hash_aggregate.rs \
  src/sql/optimizer/derive/window.rs \
  src/sql/optimizer/derive/sort.rs \
  src/sql/optimizer/derive/hash_join.rs \
  src/sql/optimizer/derive/enforcer.rs
git commit -m "test(optimizer): cover hash distribution source derivation"
```

Expected: commit succeeds.

---

### Task 4: Explain and Search Regression Coverage

**Files:**
- Modify: `src/sql/explain.rs`
- Modify: `src/sql/optimizer/search.rs`
- Modify: `src/sql/optimizer/derive/mod.rs`

- [ ] **Step 1: Add source-aware explain assertion**

In `src/sql/explain.rs` tests, update any expected physical distribution label to include source. Add this focused unit test if no equivalent exists:

```rust
#[test]
fn physical_distribution_explain_prints_hash_source() {
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{Operator, PhysicalDistributionOp};
    use crate::sql::optimizer::property::DistributionSpec;

    let node = crate::sql::optimizer::extract::PhysicalPlanNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::shuffle_agg([ColumnId(1)]),
        }),
        children: vec![],
        output: crate::sql::optimizer::property::PhysicalPropertySet::any(),
        cost: 0.0,
        stats: None,
    };

    let mut lines = Vec::new();
    super::format_physical_node(&node, false, 0, &mut lines);
    assert!(
        lines.join("\n").contains("HASH EXCHANGE (source: ShuffleAgg, hash: [c1])"),
        "explain output was:\n{}",
        lines.join("\n")
    );
}
```

If `format_physical_node` is private to the module and the test is in the same module, call it directly. If the helper signature differs, use the existing explain test helper and assert the same substring.

- [ ] **Step 2: Add search test for join output not satisfying narrower agg requirement**

In `src/sql/optimizer/derive/mod.rs` tests, add:

```rust
#[test]
fn shuffle_join_output_needs_enforcer_for_narrower_shuffle_agg_requirement() {
    let required = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
        ordering: OrderingSpec::Any,
    };
    let provided = PhysicalPropertySet {
        distribution: DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]),
        ordering: OrderingSpec::Any,
    };

    let enforcers = needed_enforcers(&required, &provided);
    assert_eq!(enforcers.len(), 1);
    assert!(matches!(
        enforcers[0],
        EnforcerKind::Distribution(DistributionSpec::HashPartitioned {
            source: HashSource::ShuffleAgg,
            ..
        })
    ));
}
```

- [ ] **Step 3: Update imports for tests**

Add `HashSource` to relevant test imports:

```rust
use crate::sql::optimizer::property::{DistributionSpec, HashSource, OrderingSpec, PhysicalPropertySet};
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test explain::tests::physical_distribution_explain_prints_hash_source --lib
cargo test optimizer::derive::tests::shuffle_join_output_needs_enforcer_for_narrower_shuffle_agg_requirement --lib
```

Expected: both PASS.

- [ ] **Step 5: Commit explain/search coverage**

Run:

```bash
git add src/sql/explain.rs src/sql/optimizer/derive/mod.rs src/sql/optimizer/search.rs
git commit -m "test(optimizer): guard source-aware exchange enforcement"
```

Expected: commit succeeds.

---

### Task 5: Optimizer SQL Golden

**Files:**
- Create: `sql-tests/optimizer/sql/g4_hash_distribution_source.sql`
- Create: `sql-tests/optimizer/result/g4_hash_distribution_source.result`

- [ ] **Step 1: Add the SQL test case**

Create `sql-tests/optimizer/sql/g4_hash_distribution_source.sql` with:

```sql
-- @tags=optimizer,g4
-- Test Objective:
-- 1. ShuffleJoin output carries both join eq columns, but it must not satisfy
--    a narrower ShuffleAgg requirement for GROUP BY or Window PARTITION BY.
-- 2. EXPLAIN output exposes source-aware hash exchange labels.
DROP TABLE IF EXISTS ${case_db}.g4_src_a;
DROP TABLE IF EXISTS ${case_db}.g4_src_b;
CREATE TABLE ${case_db}.g4_src_a (k INT, v INT);
CREATE TABLE ${case_db}.g4_src_b (k INT, w INT);
INSERT INTO ${case_db}.g4_src_a VALUES (1, 10), (1, 11), (2, 20);
INSERT INTO ${case_db}.g4_src_b VALUES (1, 100), (2, 200);

-- @explain_contains=HASH EXCHANGE (source: ShuffleAgg
-- @explain_contains=HASH EXCHANGE (source: ShuffleJoin
SELECT a.k, SUM(a.v + b.w) AS s
FROM ${case_db}.g4_src_a a
INNER JOIN ${case_db}.g4_src_b b ON a.k = b.k
GROUP BY a.k
ORDER BY a.k;

-- @explain_contains=HASH EXCHANGE (source: ShuffleAgg
-- @explain_contains=HASH EXCHANGE (source: ShuffleJoin
SELECT a.k,
       MAX(b.w) OVER (PARTITION BY a.k ORDER BY b.w) AS max_w
FROM ${case_db}.g4_src_a a
INNER JOIN ${case_db}.g4_src_b b ON a.k = b.k
ORDER BY a.k, max_w;
```

- [ ] **Step 2: Start or reuse the standalone SQL test environment**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
if [ -n "${NOVAROCKS_SQL_TEST_CONFIG:-}" ]; then
  docker/iceberg-rest/up.sh
fi
```

Expected: if the generated environment exists, Docker services start or are reused. If the generated environment does not exist, continue with the repository default SQL runner configuration.

- [ ] **Step 3: Record the optimizer suite case**

Run one of these commands, preferring the generated config when available:

```bash
if [ -n "${NOVAROCKS_SQL_TEST_CONFIG:-}" ]; then
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite optimizer --only g4_hash_distribution_source --mode record
else
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --only g4_hash_distribution_source --mode record
fi
```

Expected: `sql-tests/optimizer/result/g4_hash_distribution_source.result` is created and both `@explain_contains` assertions pass during record.

- [ ] **Step 4: Verify the recorded case**

Run:

```bash
if [ -n "${NOVAROCKS_SQL_TEST_CONFIG:-}" ]; then
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite optimizer --only g4_hash_distribution_source --mode verify
else
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --only g4_hash_distribution_source --mode verify
fi
```

Expected: PASS.

- [ ] **Step 5: Commit SQL golden coverage**

Run:

```bash
git add sql-tests/optimizer/sql/g4_hash_distribution_source.sql \
  sql-tests/optimizer/result/g4_hash_distribution_source.result
git commit -m "test(optimizer): cover hash distribution source plans"
```

Expected: commit succeeds.

---

### Task 6: Full Validation and Cleanup

**Files:**
- Modify only files touched by earlier tasks if validation reveals exact failures.

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt --check
```

Expected: PASS. If it fails, run `cargo fmt`, then re-run `cargo fmt --check`.

- [ ] **Step 2: Run focused Rust tests**

Run:

```bash
cargo test property::tests --lib
cargo test optimizer::derive --lib
cargo test optimizer::search --lib
```

Expected: PASS.

- [ ] **Step 3: Run optimizer suite verify**

Run one of these commands:

```bash
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
if [ -n "${NOVAROCKS_SQL_TEST_CONFIG:-}" ]; then
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite optimizer --mode verify
else
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --mode verify
fi
```

Expected: PASS for the optimizer suite.

- [ ] **Step 4: Inspect final diff**

Run:

```bash
git status --short
git diff --stat HEAD
rg -n "HashPartitioned\\(" src/sql/optimizer src/sql/codegen src/sql/explain.rs
```

Expected:
- `git status --short` shows only intentional files.
- `rg "HashPartitioned\\("` returns no direct tuple-style constructor uses.

- [ ] **Step 5: Commit validation fixes if any were needed**

If Step 1-4 required changes after the previous commits, run:

```bash
git add src sql-tests
git commit -m "fix(optimizer): finalize hash distribution source migration"
```

Expected: commit is created only if validation changed files.

---

## Implementation Notes

- Keep code comments and errors in English.
- Do not add `Local`, `Bucket`, or `ShuffleEnforce` in this implementation.
- Do not treat Iceberg `bucket(...)` as execution colocate distribution.
- Do not use G7 logical equivalence classes inside physical distribution satisfies.
- Preserve first-seen column order in hash constructors because join shuffle key order matters.
- For `ShuffleAgg`, subset direction is intentional: `hash(a)` satisfies `hash(a, b)`, while `hash(a, b)` does not satisfy `hash(a)`.

## Self-Review Checklist

- Spec §4 data model is covered by Task 1.
- Spec §5 satisfies semantics is covered by Task 1 property tests and implementation.
- Spec §6 producer/consumer mapping is covered by Tasks 2 and 3.
- Spec §7 explain/codegen behavior is covered by Tasks 2 and 4.
- Spec §8 SQL golden coverage and verification commands are covered by Tasks 5 and 6.
- Non-goals are enforced by the implementation notes and by the absence of `Local`, `Bucket`, and `ShuffleEnforce` constructors.
