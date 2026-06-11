# Iceberg IMV UNION ALL Delta Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add incremental refresh for three `UNION ALL` Iceberg MV shapes — aggregate-over-UNION-ALL (task 8), UNION-ALL-of-aggregate-branches (task 9), and projection/filter UNION ALL — in one PR.

**Architecture:** Two union families. **A 族** (`Aggregate(Union(..))`, union below aggregate) merges by group key and reuses the existing `AggregateStateMerge` machinery unchanged. **B 族** (`Union(branches)`, union at root) keeps branches independent via a new hidden `__branch_id__` column, with composite apply key `(__branch_id__, inner_row_id)`. The logical rewrite mirrors the proven `RewriteJoinAggregateDeltaRule` pattern (new dedicated rules in the structural phase, then existing aggregate-state / scan-binding / action-propagation stages consume the result).

**Tech Stack:** Rust; `sqlparser` AST; NovaRocks IMV logical rewrite pipeline (`src/sql/optimizer/rewrite/imv/`); Iceberg target apply (`src/engine/mv/`); MV schema contract (`src/meta/repository/mv_contract.rs`); `sql-tests/iceberg-ivm` SQL regression suite.

**Spec:** `docs/design/specs/2026-06-02-iceberg-imv-union-all-delta-rewrite-design.md`

**Reference (read before starting):** `RewriteJoinAggregateDeltaRule` in `src/sql/optimizer/rewrite/imv/join_delta.rs` is the closest precedent — it already synthesizes `Aggregate(Union(branches))` and reuses `mark_delta_scan`/`mark_version_scan`/`normalize_branch_output`/`plan_output_columns`. The A-family rule is a simpler sibling of it; reuse those helpers (they are `pub(crate)` or sibling-accessible in the same module).

**Build/test commands:**
- Focused unit tests: `cargo test -p novarocks --lib <module>::tests -- --nocapture` (debug build, fastest).
- Full lib tests: `cargo test -p novarocks --lib`.
- `cargo fmt && cargo clippy --all-targets` before each commit.
- SQL suite (needs a running standalone-server; see CLAUDE.md §7.3): `source docker/iceberg-rest/runtime/current/env.sh` then run the `iceberg-ivm` suite via the sql-test-runner.

> NOTE on crate name: the workspace package name may not be `novarocks`. If `-p novarocks` errors, run `cargo test --lib <path>` from the repo root (the lib target is unambiguous in a single-crate workspace).

---

## File Structure

**New files:**
- `src/sql/optimizer/rewrite/imv/union_delta.rs` — A 族 rule `RewriteUnionAggregateDeltaRule` (`Delta(Aggregate(Union)) → Delta(Aggregate(Union(Δbranches)))`).
- `src/sql/optimizer/rewrite/imv/branch_union.rs` — B 族 rule `RewriteBranchUnionRule` (`Delta(Union(branches)) → Union(branch-scoped merges / delta projections)` + `__branch_id__` injection) and the `branch_id` flatten/allocation helpers.

**Modified files:**
- `src/connector/starrocks/table/mv_shape.rs` — `IncrementalMvShape::UnionAll` variant; `AggregateMvShape.fan_in_bases`; `classify_union_all_mv_query`; A-family FROM-union acceptance in `classify_aggregate_mv_query`; dispatch in `classify_incremental_mv_query`.
- `src/meta/repository/mv_contract.rs` — `BranchUnionContract`; `MvSchemaContract.branch`; self-check branch validation; `BRANCH_ID_COLUMN_NAME` const.
- `src/engine/mv/iceberg_target_apply.rs` — `ICEBERG_MV_BRANCH_ID_COLUMN` const; `branch_id_table_column()`; composite branch-scoped row locator.
- `src/engine/mv/iceberg_refresh.rs` — `create_iceberg_mv` adds `__branch_id__` for B 族 + writes branch contract; `plan_iceberg_mv_refresh` dispatch for union shapes; per-branch apply orchestration.
- `src/sql/catalog.rs` — `IcebergMvTargetStateRowFilter` gains optional `branch_id` scoping.
- `src/sql/optimizer/rewrite/imv/target_state.rs` — `build_target_state_scan_source` gains `branch_id` param (or a sibling builder).
- `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs` — extract a branch-parameterized `AggregateStateMerge` builder reused by `branch_union.rs`.
- `src/sql/optimizer/rewrite/imv/action_propagation.rs` — generalize union acceptance (fan-in delta union + top branch union).
- `src/sql/optimizer/rewrite/imv/action_column.rs` — same generalization in `ActionColumnValidationRule`.
- `src/sql/optimizer/rewrite/imv/pipeline.rs` — register the two new stages.
- `src/sql/optimizer/rewrite/imv/mod.rs` — `mod union_delta; mod branch_union;`.

**New test fixtures:** under `sql-tests/iceberg-ivm/sql/` (Stage 4).

---

## Stage 1 — Classifier + contract + target scaffolding

**Stage goal:** A union MV can be CREATEd (classifier accepts it, target table gets `__branch_id__` when B 族, contract describes branches). Refresh execution is NOT wired yet — a union-shape REFRESH returns a clear "not yet supported in this build" error until Stage 2/3. All Stage 1 tests are classification/contract/create-level.

### Task 1.1: Add `UnionAll` shape variant + extend `AggregateMvShape` with fan-in bases

**Files:**
- Modify: `src/connector/starrocks/table/mv_shape.rs:1-73` (enum + structs + `base_tables`)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module of `mv_shape.rs`:

```rust
#[test]
fn union_all_shape_reports_all_branch_base_tables() {
    let agg = AggregateMvShape {
        base_table: name("ice.ns.t1"),
        fan_in_bases: Vec::new(),
        group_keys: Vec::new(),
        aggregates: Vec::new(),
        visible_outputs: Vec::new(),
    };
    let agg2 = AggregateMvShape {
        base_table: name("ice.ns.t2"),
        fan_in_bases: Vec::new(),
        group_keys: Vec::new(),
        aggregates: Vec::new(),
        visible_outputs: Vec::new(),
    };
    let shape = IncrementalMvShape::UnionAll(UnionAllMvShape {
        branch_kind: UnionBranchKind::Aggregate,
        branches: vec![
            IncrementalMvShape::Aggregate(agg),
            IncrementalMvShape::Aggregate(agg2),
        ],
    });
    let bases: Vec<String> = shape.base_tables().iter().map(|n| n.to_string()).collect();
    assert_eq!(bases, vec!["ice.ns.t1".to_string(), "ice.ns.t2".to_string()]);
}

#[test]
fn aggregate_shape_fan_in_bases_drive_base_tables() {
    let agg = AggregateMvShape {
        base_table: name("ice.ns.t1"),
        fan_in_bases: vec![name("ice.ns.t1"), name("ice.ns.t2")],
        group_keys: Vec::new(),
        aggregates: Vec::new(),
        visible_outputs: Vec::new(),
    };
    let shape = IncrementalMvShape::Aggregate(agg);
    let bases: Vec<String> = shape.base_tables().iter().map(|n| n.to_string()).collect();
    assert_eq!(bases, vec!["ice.ns.t1".to_string(), "ice.ns.t2".to_string()]);
}
```

Add this test helper to the test module if no equivalent exists:

```rust
fn name(s: &str) -> sqlparser::ast::ObjectName {
    // `classify_sql` parsing already exists in this test module; reuse the
    // parser to build a three-part ObjectName from a dotted string.
    let parts = s.split('.').map(sqlparser::ast::Ident::new).collect::<Vec<_>>();
    sqlparser::ast::ObjectName(parts.into_iter().map(sqlparser::ast::ObjectNamePart::Identifier).collect())
}
```

> If `ObjectName` construction differs in this `sqlparser` version, mirror however an existing test in this file builds an `ObjectName` (search the test module for `ObjectName`). The behavior asserted is what matters.

- [ ] **Step 2: Run test — verify it fails to compile**

Run: `cargo test --lib mv_shape::tests::union_all_shape_reports_all_branch_base_tables`
Expected: compile error — `UnionAllMvShape`, `UnionBranchKind`, and field `fan_in_bases` do not exist.

- [ ] **Step 3: Add the types and field**

In `mv_shape.rs`, add the variant to `IncrementalMvShape`:

```rust
pub(crate) enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
    JoinProjectionFilter(JoinProjectionFilterMvShape),
    JoinAggregate(JoinAggregateMvShape),
    UnionAll(UnionAllMvShape),
}
```

Add `fan_in_bases` to `AggregateMvShape`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateMvShape {
    pub(crate) base_table: sqlparser::ast::ObjectName,
    /// Non-empty only for A-family aggregate-over-UNION-ALL fan-in. Each entry
    /// is one base table feeding the union below the aggregate. Empty for the
    /// ordinary single-base aggregate, where `base_table` is the only base.
    pub(crate) fan_in_bases: Vec<sqlparser::ast::ObjectName>,
    pub(crate) group_keys: Vec<GroupKeyShape>,
    pub(crate) aggregates: Vec<AggregateCallShape>,
    pub(crate) visible_outputs: Vec<VisibleAggregateOutput>,
}
```

Add the new shape types (place near the other shape structs):

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnionBranchKind {
    ProjectionFilter,
    Aggregate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UnionAllMvShape {
    pub(crate) branch_kind: UnionBranchKind,
    /// Flattened left-to-right branches. Index = stable branch_id.
    pub(crate) branches: Vec<IncrementalMvShape>,
}
```

Update `base_tables()` (and `base_table()` panic arm) to cover the new cases:

```rust
pub(crate) fn base_tables(&self) -> Vec<&sqlparser::ast::ObjectName> {
    match self {
        IncrementalMvShape::ProjectionFilter(shape) => vec![&shape.base_table],
        IncrementalMvShape::Aggregate(shape) => {
            if shape.fan_in_bases.is_empty() {
                vec![&shape.base_table]
            } else {
                shape.fan_in_bases.iter().collect()
            }
        }
        IncrementalMvShape::JoinProjectionFilter(shape) => {
            vec![&shape.left_table, &shape.right_table]
        }
        IncrementalMvShape::JoinAggregate(shape) => {
            vec![&shape.join.left_table, &shape.join.right_table]
        }
        IncrementalMvShape::UnionAll(shape) => {
            shape.branches.iter().flat_map(|b| b.base_tables()).collect()
        }
    }
}
```

In `base_table()`, extend the panic arm to include `UnionAll`:

```rust
IncrementalMvShape::JoinProjectionFilter(_)
| IncrementalMvShape::JoinAggregate(_)
| IncrementalMvShape::UnionAll(_) => {
    panic!("base_table() is only valid for single-base MV shapes")
}
```

Find every existing `AggregateMvShape { .. }` literal in the file and in `iceberg_refresh.rs` / `aggregate_rewrite.rs` tests and add `fan_in_bases: Vec::new(),` (the compiler will list them). The production constructors of `AggregateMvShape` live in `classify_aggregate_select_outputs`/`classify_aggregate_mv_query` — add `fan_in_bases: Vec::new()` there too.

- [ ] **Step 4: Run tests — verify they pass**

Run: `cargo test --lib mv_shape::tests::union_all_shape_reports_all_branch_base_tables mv_shape::tests::aggregate_shape_fan_in_bases_drive_base_tables`
Expected: PASS. Then `cargo build --lib` to confirm all `AggregateMvShape` literals updated.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/connector/starrocks/table/mv_shape.rs
git commit -m "feat(imv): add UnionAll MV shape variant and aggregate fan-in bases"
```

### Task 1.2: Classify top-level UNION ALL of aggregate branches (B 族)

**Files:**
- Modify: `src/connector/starrocks/table/mv_shape.rs` (new `classify_union_all_mv_query` + error helper)

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn accepts_top_level_union_all_of_aggregate_branches() {
    let shape = classify_sql(
        "select k1, sum(v2) as s from ice.ns.t1 group by k1 \
         union all \
         select k1, sum(v2) as s from ice.ns.t2 group by k1",
    )
    .expect("union all of aggregates should be accepted");
    let IncrementalMvShape::UnionAll(u) = shape else {
        panic!("expected UnionAll shape");
    };
    assert_eq!(u.branch_kind, UnionBranchKind::Aggregate);
    assert_eq!(u.branches.len(), 2);
    assert!(matches!(u.branches[0], IncrementalMvShape::Aggregate(_)));
    assert!(matches!(u.branches[1], IncrementalMvShape::Aggregate(_)));
    assert_eq!(
        u.branches.iter().flat_map(|b| b.base_tables()).map(|n| n.to_string()).collect::<Vec<_>>(),
        vec!["ice.ns.t1".to_string(), "ice.ns.t2".to_string()]
    );
}
```

- [ ] **Step 2: Run test — verify it fails**

Run: `cargo test --lib mv_shape::tests::accepts_top_level_union_all_of_aggregate_branches`
Expected: FAIL — current `classify_incremental_mv_query` rejects `SetExpr::SetOperation`.

- [ ] **Step 3: Implement `classify_union_all_mv_query`**

Add to `mv_shape.rs`:

```rust
/// Classify a top-level `UNION ALL` (B-family). Flattens nested UNION ALL into
/// a single left-to-right branch list, classifies each branch with the existing
/// single-branch classifiers, and enforces homogeneous branch kind + compatible
/// outputs. Rejects UNION (distinct) / INTERSECT / EXCEPT and mixed shapes.
fn classify_union_all_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<UnionAllMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| union_all_error())?;
    let mut branch_queries = Vec::new();
    flatten_union_all(query.body.as_ref(), &mut branch_queries)?;
    if branch_queries.len() < 2 {
        return Err(union_all_error());
    }

    let mut branches = Vec::with_capacity(branch_queries.len());
    for body in &branch_queries {
        // Wrap each branch SetExpr back into a Query so the single-branch
        // classifiers (which take &Query) can run unchanged.
        let branch_query = wrap_setexpr_as_query(body);
        let branch_shape = classify_single_union_branch(&branch_query)?;
        branches.push(branch_shape);
    }

    let branch_kind = union_branch_kind(&branches[0])?;
    for branch in &branches {
        if union_branch_kind(branch)? != branch_kind {
            return Err(union_all_mixed_shape_error());
        }
    }
    validate_union_branch_outputs_compatible(&branches)?;
    Ok(UnionAllMvShape { branch_kind, branches })
}

/// Flatten nested `UNION ALL` into leaf SetExprs. Any non-ALL set op fails.
fn flatten_union_all<'a>(
    body: &'a sqlparser::ast::SetExpr,
    out: &mut Vec<&'a sqlparser::ast::SetExpr>,
) -> Result<(), String> {
    match body {
        sqlparser::ast::SetExpr::SetOperation {
            op: sqlparser::ast::SetOperator::Union,
            set_quantifier:
                sqlparser::ast::SetQuantifier::All | sqlparser::ast::SetQuantifier::AllByName,
            left,
            right,
        } => {
            flatten_union_all(left, out)?;
            flatten_union_all(right, out)
        }
        sqlparser::ast::SetExpr::SetOperation { .. } => Err(union_all_non_all_error()),
        sqlparser::ast::SetExpr::Select(_) => {
            out.push(body);
            Ok(())
        }
        // Parenthesized subquery body: descend.
        sqlparser::ast::SetExpr::Query(inner) => flatten_union_all(inner.body.as_ref(), out),
        _ => Err(union_all_error()),
    }
}

fn wrap_setexpr_as_query(body: &sqlparser::ast::SetExpr) -> sqlparser::ast::Query {
    sqlparser::ast::Query {
        with: None,
        body: Box::new(body.clone()),
        order_by: None,
        limit: None,
        limit_by: Vec::new(),
        offset: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
    }
}
```

> The exact field list of `sqlparser::ast::Query` is version-specific. Build `branch_query` by mirroring however `wrap_setexpr_as_query` must look — the simplest robust approach is to keep the original `Query` and only swap its `body`: clone `query` and set `body = Box::new(branch_body.clone())`. Rewrite `wrap_setexpr_as_query(query, body)` to take the outer `&Query` and clone-with-body-swapped if the literal struct fields are awkward.

```rust
fn classify_single_union_branch(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    // A branch is either an aggregate or a projection/filter over a single base.
    // Join branches are out of scope for v1 (fail-fast).
    if is_probably_join_query(query) {
        return Err(union_all_branch_join_unsupported_error());
    }
    if is_probably_aggregate_query(query) {
        return classify_aggregate_mv_query(query).map(IncrementalMvShape::Aggregate);
    }
    classify_projection_filter_mv_query(query).map(IncrementalMvShape::ProjectionFilter)
}

fn union_branch_kind(shape: &IncrementalMvShape) -> Result<UnionBranchKind, String> {
    match shape {
        IncrementalMvShape::Aggregate(_) => Ok(UnionBranchKind::Aggregate),
        IncrementalMvShape::ProjectionFilter(_) => Ok(UnionBranchKind::ProjectionFilter),
        _ => Err(union_all_mixed_shape_error()),
    }
}
```

Add error helpers near the other `*_error()` fns:

```rust
fn union_all_error() -> String {
    "incremental UNION ALL MV query must be a UNION ALL of two or more compatible branches"
        .to_string()
}
fn union_all_non_all_error() -> String {
    "incremental UNION ALL MV supports only UNION ALL; UNION (distinct) / INTERSECT / EXCEPT are not supported"
        .to_string()
}
fn union_all_mixed_shape_error() -> String {
    "incremental UNION ALL MV requires all branches to be the same shape (all aggregate or all projection/filter)"
        .to_string()
}
fn union_all_branch_join_unsupported_error() -> String {
    "incremental UNION ALL MV branches may not contain joins in this version".to_string()
}
fn union_all_branch_output_mismatch_error() -> String {
    "incremental UNION ALL MV branches must have identical output arity, types, and nullability (no implicit cast)"
        .to_string()
}
```

Add a placeholder for the compatibility checker (filled in Task 1.3):

```rust
fn validate_union_branch_outputs_compatible(
    _branches: &[IncrementalMvShape],
) -> Result<(), String> {
    Ok(())
}
```

- [ ] **Step 4: Wire into `classify_incremental_mv_query`**

Replace the body of `classify_incremental_mv_query` so a top-level UNION ALL routes to the new classifier before the single-shape paths:

```rust
pub(crate) fn classify_incremental_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    if matches!(
        query.body.as_ref(),
        sqlparser::ast::SetExpr::SetOperation { .. }
    ) {
        return classify_union_all_mv_query(query).map(IncrementalMvShape::UnionAll);
    }

    if is_probably_aggregate_query(query) {
        if is_probably_join_query(query) {
            return classify_join_aggregate_mv_query(query).map(IncrementalMvShape::JoinAggregate);
        }
        return classify_aggregate_mv_query(query).map(IncrementalMvShape::Aggregate);
    }
    match classify_join_projection_filter_mv_query(query) {
        Ok(shape) => return Ok(IncrementalMvShape::JoinProjectionFilter(shape)),
        Err(err) if is_probably_join_query(query) => return Err(err),
        Err(_) => {}
    }
    classify_projection_filter_mv_query(query).map(IncrementalMvShape::ProjectionFilter)
}
```

- [ ] **Step 5: Run test — verify it passes**

Run: `cargo test --lib mv_shape::tests::accepts_top_level_union_all_of_aggregate_branches`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/connector/starrocks/table/mv_shape.rs
git commit -m "feat(imv): classify top-level UNION ALL of aggregate branches"
```

### Task 1.3: Branch compatibility checks + projection branches + reject negatives

**Files:**
- Modify: `src/connector/starrocks/table/mv_shape.rs` (`validate_union_branch_outputs_compatible`, tests)

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn accepts_top_level_union_all_of_projection_branches() {
    let shape = classify_sql(
        "select k1, v2 from ice.ns.t1 where v2 > 0 \
         union all \
         select k1, v2 from ice.ns.t2 where v2 < 0",
    )
    .expect("union all of projection/filter should be accepted");
    let IncrementalMvShape::UnionAll(u) = shape else { panic!("expected UnionAll"); };
    assert_eq!(u.branch_kind, UnionBranchKind::ProjectionFilter);
    assert_eq!(u.branches.len(), 2);
}

#[test]
fn flattens_three_branch_union_all() {
    let shape = classify_sql(
        "select k1, sum(v2) s from ice.ns.t1 group by k1 \
         union all select k1, sum(v2) s from ice.ns.t2 group by k1 \
         union all select k1, sum(v2) s from ice.ns.t3 group by k1",
    )
    .expect("three-branch union all should flatten");
    let IncrementalMvShape::UnionAll(u) = shape else { panic!("expected UnionAll"); };
    assert_eq!(u.branches.len(), 3);
}

#[test]
fn rejects_union_distinct() {
    let err = classify_sql(
        "select k1 from ice.ns.t1 union select k1 from ice.ns.t2",
    )
    .expect_err("UNION distinct must be rejected");
    assert!(err.contains("UNION ALL"), "unexpected: {err}");
}

#[test]
fn rejects_intersect() {
    let err = classify_sql(
        "select k1 from ice.ns.t1 intersect select k1 from ice.ns.t2",
    )
    .expect_err("INTERSECT must be rejected");
    assert!(err.contains("not supported") || err.contains("UNION ALL"), "unexpected: {err}");
}

#[test]
fn rejects_mixed_aggregate_and_projection_branches() {
    let err = classify_sql(
        "select k1, sum(v2) s from ice.ns.t1 group by k1 \
         union all select k1, v2 from ice.ns.t2",
    )
    .expect_err("mixed shapes must be rejected");
    assert!(err.contains("same shape"), "unexpected: {err}");
}

#[test]
fn rejects_branch_arity_mismatch() {
    let err = classify_sql(
        "select k1, sum(v2) s from ice.ns.t1 group by k1 \
         union all select k1, sum(v2) s, count(*) c from ice.ns.t2 group by k1",
    )
    .expect_err("arity mismatch must be rejected");
    assert!(err.contains("arity") || err.contains("identical output"), "unexpected: {err}");
}
```

- [ ] **Step 2: Run tests — verify projection/flatten/reject behavior fails where expected**

Run: `cargo test --lib mv_shape::tests::rejects_branch_arity_mismatch mv_shape::tests::accepts_top_level_union_all_of_projection_branches`
Expected: `rejects_branch_arity_mismatch` FAILS (compat checker is a stub that always returns Ok); others may already pass from Task 1.2. Note which fail.

- [ ] **Step 3: Implement `validate_union_branch_outputs_compatible`**

Replace the stub. Compare each branch's visible output count and (where determinable from the shape) names. Arity is the v1 guarantee; deep type/nullability checks happen at analysis time when the logical plans are built, but arity + output-name count is checked here for an early, clear error.

```rust
fn validate_union_branch_outputs_compatible(
    branches: &[IncrementalMvShape],
) -> Result<(), String> {
    let arity = |shape: &IncrementalMvShape| -> usize {
        match shape {
            IncrementalMvShape::Aggregate(a) => a.visible_outputs.len(),
            // Projection/filter arity is the SELECT projection length; recover
            // it from the branch query during classification instead. For the
            // shape-level check we compare aggregate visible-output counts and
            // rely on analysis-time type checks for projection branches.
            _ => usize::MAX,
        }
    };
    let first = arity(&branches[0]);
    if first != usize::MAX {
        for branch in &branches[1..] {
            if arity(branch) != first {
                return Err(union_all_branch_output_mismatch_error());
            }
        }
    }
    Ok(())
}
```

> Projection-branch arity is not captured in `ProjectionFilterMvShape` today (it only stores `base_table`). For a robust arity check on projection branches, either (a) add a `projection_arity: usize` field to `ProjectionFilterMvShape` and populate it in `classify_projection_filter_mv_query`, or (b) defer projection-branch arity/type checks to the analyzer (it already errors on UNION arity/type mismatch when building the `UnionNode`). Choose (b) for v1 to keep `ProjectionFilterMvShape` unchanged; the analyzer's `UnionNode` builder is the authoritative arity/type gate for projection branches, and `rejects_branch_arity_mismatch` (aggregate branches) is covered here.

- [ ] **Step 4: Run tests — verify they pass**

Run: `cargo test --lib mv_shape::tests`
Expected: all new union tests PASS. If `rejects_intersect` parsing differs (INTERSECT may parse as a different `SetOperator`), confirm `flatten_union_all`'s non-ALL arm catches it.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/connector/starrocks/table/mv_shape.rs
git commit -m "feat(imv): UNION ALL branch compatibility checks and negative cases"
```

### Task 1.4: A 族 — accept UNION ALL derived table in aggregate FROM

**Files:**
- Modify: `src/connector/starrocks/table/mv_shape.rs` (`classify_aggregate_mv_query` / `extract_single_base_table`)

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn accepts_aggregate_over_union_all_fan_in() {
    let shape = classify_sql(
        "select k, sum(v) as s from ( \
            select k, v from ice.ns.t1 union all select k, v from ice.ns.t2 \
         ) u group by k",
    )
    .expect("aggregate over UNION ALL should be accepted");
    let IncrementalMvShape::Aggregate(a) = shape else {
        panic!("expected Aggregate shape (A-family)");
    };
    assert_eq!(
        a.fan_in_bases.iter().map(|n| n.to_string()).collect::<Vec<_>>(),
        vec!["ice.ns.t1".to_string(), "ice.ns.t2".to_string()]
    );
    assert_eq!(a.group_keys.len(), 1);
    assert_eq!(a.aggregates.len(), 1);
}
```

- [ ] **Step 2: Run test — verify it fails**

Run: `cargo test --lib mv_shape::tests::accepts_aggregate_over_union_all_fan_in`
Expected: FAIL — `extract_single_base_table` rejects a derived-table FROM.

- [ ] **Step 3: Implement fan-in extraction in `classify_aggregate_mv_query`**

In `classify_aggregate_mv_query`, before calling `extract_single_base_table`, detect a single derived-table FROM whose subquery is a UNION ALL of single-base selects, and collect the fan-in bases:

```rust
fn classify_aggregate_mv_query(query: &sqlparser::ast::Query) -> Result<AggregateMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| aggregate_error())?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(aggregate_error());
    };
    reject_unsupported_aggregate_select_clauses(select)?;

    let fan_in_bases = extract_union_all_fan_in_bases(select)?;
    let base_table = match fan_in_bases.first() {
        Some(first) => first.clone(),
        None => extract_single_base_table(select, aggregate_error, aggregate_error)?,
    };
    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection).map_err(aggregate_expr_error)?;
    }

    let (group_keys, aggregates, visible_outputs) = classify_aggregate_select_outputs(select)?;
    Ok(AggregateMvShape {
        base_table,
        fan_in_bases,
        group_keys,
        aggregates,
        visible_outputs,
    })
}

/// If the aggregate's single FROM item is a derived table whose body is a
/// `UNION ALL` of single-base `SELECT * / SELECT cols` branches, return the
/// ordered list of base tables. Returns empty for the ordinary single-base case.
fn extract_union_all_fan_in_bases(
    select: &sqlparser::ast::Select,
) -> Result<Vec<sqlparser::ast::ObjectName>, String> {
    let [from] = select.from.as_slice() else {
        return Ok(Vec::new());
    };
    if !from.joins.is_empty() {
        return Ok(Vec::new());
    }
    let sqlparser::ast::TableFactor::Derived { subquery, .. } = &from.relation else {
        return Ok(Vec::new());
    };
    // Only a UNION ALL body is a fan-in; a plain derived SELECT is not handled here.
    if !matches!(subquery.body.as_ref(), sqlparser::ast::SetExpr::SetOperation { .. }) {
        return Ok(Vec::new());
    }
    let mut branch_bodies = Vec::new();
    flatten_union_all(subquery.body.as_ref(), &mut branch_bodies)?;
    if branch_bodies.len() < 2 {
        return Err(aggregate_error());
    }
    let mut bases = Vec::with_capacity(branch_bodies.len());
    for body in branch_bodies {
        let sqlparser::ast::SetExpr::Select(branch_select) = body else {
            return Err(aggregate_error());
        };
        let base = extract_single_base_table(branch_select, aggregate_error, aggregate_error)?;
        bases.push(base);
    }
    Ok(bases)
}
```

> `is_probably_aggregate_query` must still return true for the fan-in form (the outer SELECT has `GROUP BY` + aggregate calls), so `classify_incremental_mv_query` routes here. Verify by reading `is_probably_aggregate_query` — it inspects the outer `select.group_by`/projection, which is present. The top-level body is `SetExpr::Select` (not a SetOperation), so the union-dispatch in Task 1.2 does not intercept it. Good.

- [ ] **Step 4: Run test — verify it passes**

Run: `cargo test --lib mv_shape::tests::accepts_aggregate_over_union_all_fan_in`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/connector/starrocks/table/mv_shape.rs
git commit -m "feat(imv): classify aggregate over UNION ALL fan-in (A-family)"
```

### Task 1.5: Contract — `__branch_id__` column + `BranchUnionContract` + self-check

**Files:**
- Modify: `src/meta/repository/mv_contract.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn branch_union_contract_self_check_requires_branch_id_column() {
    let mut contract = sample_aggregate_contract(); // GroupRowId apply key + aggregate state
    contract.branch = Some(BranchUnionContract {
        branch_id_column: BranchIdColumnContract {
            column_name: BRANCH_ID_COLUMN_NAME.to_string(),
            target_field_id: 4242,
        },
        branch_count: 2,
        inner_apply_key_source: ApplyKeySource::GroupRowId,
    });
    // With a correct branch_id column name it must pass.
    contract.ensure_self_consistent().expect("valid branch contract");

    // A wrong branch_id column name must fail.
    contract.branch.as_mut().unwrap().branch_id_column.column_name = "wrong".to_string();
    let err = contract.ensure_self_consistent().expect_err("wrong branch id col must fail");
    assert!(matches!(err, ContractSelfCheckError::BranchIdColumnNameWrong { .. }), "got {err:?}");
}
```

> Reuse / create `sample_aggregate_contract()` from the existing test helpers in this file (there is already a `sample_contract()` and an aggregate variant used by `group_row_id_*` tests — mirror it).

- [ ] **Step 2: Run test — verify it fails to compile**

Run: `cargo test --lib mv_contract::tests::branch_union_contract_self_check_requires_branch_id_column`
Expected: compile error — `BranchUnionContract`, `BranchIdColumnContract`, `BRANCH_ID_COLUMN_NAME`, field `branch`, and `ContractSelfCheckError::BranchIdColumnNameWrong` do not exist.

- [ ] **Step 3: Add contract types + field + const + self-check**

Add the const near the other apply-key column-name consts:

```rust
pub const BRANCH_ID_COLUMN_NAME: &str = "__branch_id__";
```

Add the structs:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchIdColumnContract {
    pub column_name: String,
    pub target_field_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchUnionContract {
    pub branch_id_column: BranchIdColumnContract,
    pub branch_count: u32,
    /// The per-branch inner apply key combined with branch_id to form the
    /// composite identity. `GroupRowId` for UNION ALL of aggregates;
    /// `BaseRowId` for projection/filter UNION ALL.
    pub inner_apply_key_source: ApplyKeySource,
}
```

Add the field to `MvSchemaContract` (backward compatible — existing contracts deserialize with `branch: None`):

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvSchemaContract {
    pub contract_version: u16,
    pub base: BaseContract,
    #[serde(default)]
    pub bases: Vec<BaseContract>,
    pub output: OutputContract,
    #[serde(default)]
    pub join: Option<JoinContract>,
    #[serde(default)]
    pub aggregate: Option<AggregateStateContract>,
    #[serde(default)]
    pub branch: Option<BranchUnionContract>,
    pub target: TargetContract,
}
```

Add the error variant to `ContractSelfCheckError`:

```rust
    BranchIdColumnNameWrong {
        expected: String,
        actual: String,
    },
    BranchInnerApplyKeyMismatch {
        branch_source: ApplyKeySource,
        hidden_apply_key_source: ApplyKeySource,
    },
```

In `ensure_self_consistent`, after the existing `hidden_apply_key` source `match`, add branch validation:

```rust
    if let Some(branch) = &self.branch {
        if branch.branch_id_column.column_name != BRANCH_ID_COLUMN_NAME {
            return Err(ContractSelfCheckError::BranchIdColumnNameWrong {
                expected: BRANCH_ID_COLUMN_NAME.to_string(),
                actual: branch.branch_id_column.column_name.clone(),
            });
        }
        // The composite key's inner source must agree with the hidden apply key
        // source recorded on the target.
        if branch.inner_apply_key_source != self.target.hidden_apply_key.source {
            return Err(ContractSelfCheckError::BranchInnerApplyKeyMismatch {
                branch_source: branch.inner_apply_key_source,
                hidden_apply_key_source: self.target.hidden_apply_key.source,
            });
        }
    }
```

Add a `Display`/`Debug` arm for the new errors if `ContractSelfCheckError` has a manual `Display` impl (search for `impl std::fmt::Display for ContractSelfCheckError`); if it derives Debug only, no extra work.

- [ ] **Step 4: Run test — verify it passes**

Run: `cargo test --lib mv_contract::tests::branch_union_contract_self_check_requires_branch_id_column`
Expected: PASS. Also run `cargo test --lib mv_contract::tests` to confirm the new `#[serde(default)] branch` did not break the round-trip test (it shouldn't — old JSON has no `branch` key → `None`).

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/meta/repository/mv_contract.rs
git commit -m "feat(imv): add BranchUnionContract and __branch_id__ contract self-check"
```

### Task 1.6: `__branch_id__` target column constant + builder

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`

- [ ] **Step 1: Write the failing test**

Add to the test module of `iceberg_target_apply.rs` (create one if absent):

```rust
#[test]
fn branch_id_table_column_is_required_int() {
    let col = branch_id_table_column();
    assert_eq!(col.name, ICEBERG_MV_BRANCH_ID_COLUMN);
    assert_eq!(col.name, "__branch_id__");
    assert!(!col.nullable);
    assert!(matches!(col.data_type, crate::sql::parser::ast::SqlType::Int));
}
```

> Check the exact `SqlType` integer variant name in `src/sql/parser/ast.rs` (it may be `Int`, `Integer`, or `Int32`). Use whichever the codebase uses for a 32-bit int; `apply_key_table_column` uses `BigInt`, so a sibling variant exists.

- [ ] **Step 2: Run — verify it fails**

Run: `cargo test --lib iceberg_target_apply::tests::branch_id_table_column_is_required_int`
Expected: compile error — symbol missing.

- [ ] **Step 3: Add the constant + builder**

Near the other apply-key consts/builders in `iceberg_target_apply.rs`:

```rust
pub(crate) const ICEBERG_MV_BRANCH_ID_COLUMN: &str = "__branch_id__";

pub(crate) fn branch_id_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        data_type: crate::sql::parser::ast::SqlType::Int,
        nullable: false,
        aggregation: None,
        default: None,
    }
}
```

- [ ] **Step 4: Run — verify it passes**

Run: `cargo test --lib iceberg_target_apply::tests::branch_id_table_column_is_required_int`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/engine/mv/iceberg_target_apply.rs
git commit -m "feat(imv): add __branch_id__ target column builder"
```

### Task 1.7: `create_iceberg_mv` — add `__branch_id__` + write branch contract for B 族

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs` (`create_iceberg_mv`, ~lines 71-316)

- [ ] **Step 1: Read the create path**

Read `create_iceberg_mv` fully (the verbatim slice in the spec covers the apply-key-column dispatch, the `columns` assembly, the `expected_apply_key_field_id` computation, and `target_properties`). Identify: (a) the `apply_key_column_name`/`apply_key_source_property` match (needs a `UnionAll` arm), (b) where `columns` is built (B 族 must append `branch_id_table_column()`), (c) where the contract is written (the branch contract must be populated).

- [ ] **Step 2: Write a targeted integration-style assertion (SQL fixture in Stage 4 is the real gate)**

Stage 1 cannot fully exercise create end-to-end without a server, so gate this task with a focused unit test on a small helper. Extract the apply-key-column selection into a pure helper and test it:

```rust
// in iceberg_refresh.rs (or a small new pure fn), add:
pub(crate) fn union_branch_inner_apply_key(branch_kind: UnionBranchKind) -> ApplyKeySource {
    match branch_kind {
        UnionBranchKind::Aggregate => ApplyKeySource::GroupRowId,
        UnionBranchKind::ProjectionFilter => ApplyKeySource::BaseRowId,
    }
}
```

Test:

```rust
#[test]
fn union_branch_inner_apply_key_maps_kind_to_source() {
    use crate::meta::repository::mv_contract::ApplyKeySource;
    assert_eq!(union_branch_inner_apply_key(UnionBranchKind::Aggregate), ApplyKeySource::GroupRowId);
    assert_eq!(union_branch_inner_apply_key(UnionBranchKind::ProjectionFilter), ApplyKeySource::BaseRowId);
}
```

- [ ] **Step 3: Run — verify it fails**

Run: `cargo test --lib iceberg_refresh` (filter to the new test name).
Expected: compile error until the helper exists.

- [ ] **Step 4: Implement the create-path changes**

In `create_iceberg_mv`:

1. Add the helper above.
2. Add a `UnionAll` arm to the `apply_key_column_name` / `apply_key_source_property` match. For B 族, the *inner* apply key column is the per-branch one (group: `__row_id__`, projection: `__nova_base_row_id`), but the composite identity also needs `__branch_id__`. Keep the inner column for `apply_key_column_name`:

```rust
    let apply_key_column_name = match &shape {
        IncrementalMvShape::ProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_COLUMN,
        IncrementalMvShape::JoinProjectionFilter(_) => ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
        IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
            ICEBERG_MV_GROUP_APPLY_KEY_COLUMN
        }
        IncrementalMvShape::UnionAll(u) => match u.branch_kind {
            UnionBranchKind::Aggregate => ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            UnionBranchKind::ProjectionFilter => ICEBERG_MV_APPLY_KEY_COLUMN,
        },
    };
    let apply_key_source_property = match &shape {
        IncrementalMvShape::ProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID,
        IncrementalMvShape::JoinProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY,
        IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
            ICEBERG_MV_APPLY_KEY_SOURCE_GROUP_ROW_ID
        }
        IncrementalMvShape::UnionAll(u) => match u.branch_kind {
            UnionBranchKind::Aggregate => ICEBERG_MV_APPLY_KEY_SOURCE_GROUP_ROW_ID,
            UnionBranchKind::ProjectionFilter => ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID,
        },
    };
```

3. For B 族, when building `columns`: aggregate branches reuse `iceberg_aggregate_target_columns` (run it on the first branch's `AggregateMvShape` — all branches share layout); projection branches reuse the projection column mapping; then **append `branch_id_table_column()`** and (for aggregate branches) the inner `__row_id__` column already produced by the aggregate layout, or (for projection branches) `apply_key_table_column()`. The `__branch_id__` column is appended for both B-family kinds.

4. After `create_table`, when the schema contract is constructed and stored, populate `contract.branch = Some(BranchUnionContract { branch_id_column: BranchIdColumnContract { column_name: BRANCH_ID_COLUMN_NAME.into(), target_field_id: <field id of __branch_id__ in the created table> }, branch_count: u.branches.len() as u32, inner_apply_key_source: union_branch_inner_apply_key(u.branch_kind) })`. Find where the contract is built for aggregate/projection MVs in this function and add the branch population in the `UnionAll` case.

> The contract construction for existing shapes is in this same function (or a helper it calls — search for `MvSchemaContract {` / `schema_contract`). Mirror that, set `branch`, and keep `hidden_apply_key.source` = the inner source so the self-check (Task 1.5) passes.

- [ ] **Step 5: Run — verify the helper test passes + lib builds**

Run: `cargo test --lib iceberg_refresh::` (the helper test) and `cargo build --lib`.
Expected: PASS + clean build. (Full create correctness is verified by Stage 4 fixtures.)

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(imv): create UNION ALL MV target with __branch_id__ and branch contract"
```

### Task 1.8: Refuse union refresh with a clear error (until Stage 2/3 wire execution)

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs` (`plan_iceberg_mv_refresh` dispatch, ~line 2499)

- [ ] **Step 1: Add an explicit not-yet-executable arm**

In `plan_iceberg_mv_refresh`, after `classify_incremental_mv_query`, add — BEFORE the `aggregate_shape_for_layout(&shape).is_some()` check — a guard so a B-family union and an A-family fan-in refresh fail fast with a clear message in this Stage-1 commit. (Stage 2 replaces the A-family guard; Stage 3 replaces the B-family guard.)

```rust
    if let IncrementalMvShape::UnionAll(_) = &shape {
        return Err(RefreshError::user(
            "incremental UNION ALL MV refresh is not yet supported in this build",
        ));
    }
    if matches!(&shape, IncrementalMvShape::Aggregate(a) if !a.fan_in_bases.is_empty()) {
        return Err(RefreshError::user(
            "incremental aggregate-over-UNION-ALL refresh is not yet supported in this build",
        ));
    }
```

- [ ] **Step 2: Run lib build**

Run: `cargo build --lib`
Expected: clean. (This guard is removed/replaced in later stages; it keeps the tree consistent at the Stage-1 commit so a created union MV fails clearly rather than mis-routing.)

- [ ] **Step 3: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/engine/mv/iceberg_refresh.rs
git commit -m "chore(imv): fail-fast UNION ALL refresh until rewrite stages land"
```

---

## Stage 2 — A 族 rewrite (aggregate over UNION ALL)

**Stage goal:** `Delta(Aggregate(Union(b₁..bₙ)))` rewrites to `Delta(Aggregate(Union(Δb₁..Δbₙ)))` (each branch's scan delta-marked with a shared action column), then the existing aggregate-state stage produces an `AggregateStateMerge` exactly as for join-aggregate. Group keys merge across branches; no `__branch_id__`. End-to-end correctness is gated by the Stage 4 A-family fixture.

### Task 2.1: `RewriteUnionAggregateDeltaRule` + pipeline stage

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/union_delta.rs`
- Modify: `src/sql/optimizer/rewrite/imv/join_delta.rs` (make `mark_delta_scan` + `normalize_branch_output` `pub(crate)`)
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs` (`mod union_delta;`)
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs` (new stage)

- [ ] **Step 1: Make join_delta helpers reusable**

In `join_delta.rs`, change the visibility of the two helpers (leave bodies unchanged):

```rust
pub(crate) fn mark_delta_scan(plan: LogicalPlan, action_column: ColumnId) -> Result<LogicalPlan, String> {
```
```rust
pub(crate) fn normalize_branch_output(input: LogicalPlan, output_columns: &[OutputColumn]) -> LogicalPlan {
```
(`plan_output_columns` is already `pub(crate)`.)

- [ ] **Step 2: Write the failing test (new file `union_delta.rs`, test module)**

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode;
    use crate::sql::planner::plan::{AggregateNode, ProjectNode, ScanNode, UnionNode};

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: vec![
                    ColumnDef { name: "k".into(), data_type: DataType::Int64, nullable: false, write_default: None, logical_type: None },
                    ColumnDef { name: "v".into(), data_type: DataType::Int64, nullable: true, write_default: None, logical_type: None },
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: IcebergTableInfo {
                        catalog: "ice".into(), namespace: "db".into(), table: name.into(),
                        table_uuid: Some(format!("uuid-{name}")), current_snapshot_id: Some(22),
                        schema_id: 7, location: format!("file:///tmp/ice/db/{name}"),
                        schema: IcebergSchemaDef { fields: Vec::new() }, serialized_metadata: None,
                    },
                    files: Vec::new(), cloud_properties: BTreeMap::new(),
                },
            },
            alias: None,
            columns: vec![
                OutputColumn { column_id: ColumnId(first_id), name: "k".into(), data_type: DataType::Int64, nullable: false, is_internal: false },
                OutputColumn { column_id: ColumnId(first_id + 1), name: "v".into(), data_type: DataType::Int64, nullable: true, is_internal: false },
            ],
            predicates: Vec::new(), required_columns: None, dict_columns: Vec::new(), required_output_columns: None,
        })
    }

    fn out_col(id: u32, name: &str, nullable: bool) -> OutputColumn {
        OutputColumn { column_id: ColumnId(id), name: name.into(), data_type: DataType::Int64, nullable, is_internal: false }
    }

    fn two_branch_union() -> LogicalPlan {
        LogicalPlan::Union(UnionNode {
            inputs: vec![scan("t1", 1), scan("t2", 10)],
            all: true,
            output_columns: vec![out_col(1, "k", false), out_col(2, "v", true)],
            required_output_columns: None,
        })
    }

    fn aggregate_over(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by: vec![TypedExpr { kind: ExprKind::ColumnRef { column_id: ColumnId(1), qualifier: None, column: "k".into() }, data_type: DataType::Int64, nullable: false }],
            aggregates: Vec::new(),
            output_columns: vec![out_col(1, "k", false)],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    fn root_delta(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::ImvDelta(ImvDeltaNode { input: Box::new(input), is_root: true, action_column: None })
    }

    #[test]
    fn matches_root_delta_over_aggregate_over_source_union() {
        let rule = RewriteUnionAggregateDeltaRule;
        let ctx = build_ctx();
        let plan = root_delta(aggregate_over(two_branch_union()));
        assert!(rule.matches(&plan, &ctx));
    }

    #[test]
    fn does_not_match_union_already_marked() {
        // After this rule runs, the union branches carry markers; the rule must
        // not re-match (mirrors join-delta marker guard).
        let rule = RewriteUnionAggregateDeltaRule;
        let mut ctx = build_ctx();
        let plan = root_delta(aggregate_over(two_branch_union()));
        let RewriteResult::Changed(rewritten) = rule.apply(plan, &mut ctx).expect("rewrite") else {
            panic!("expected Changed");
        };
        assert!(!rule.matches(&rewritten, &ctx), "rule must not re-match its own marked output");
    }

    #[test]
    fn rewrite_marks_each_branch_with_shared_action_column() {
        let rule = RewriteUnionAggregateDeltaRule;
        let mut ctx = build_ctx();
        let plan = root_delta(aggregate_over(two_branch_union()));
        let RewriteResult::Changed(LogicalPlan::ImvDelta(root)) = rule.apply(plan, &mut ctx).expect("rewrite") else {
            panic!("expected Changed(ImvDelta)");
        };
        let action = root.action_column.expect("root carries action column");
        let LogicalPlan::Aggregate(agg) = root.input.as_ref() else { panic!("expected Aggregate"); };
        let LogicalPlan::Union(u) = agg.input.as_ref() else { panic!("expected Union"); };
        assert!(u.all);
        assert_eq!(u.inputs.len(), 2);
        // Union output carries the shared action column as its last column.
        assert_eq!(u.output_columns.last().unwrap().column_id, action);
        assert!(u.output_columns.last().unwrap().name.eq_ignore_ascii_case("__change_op"));
        // Each branch is a normalized Project over an ImvDelta(scan) carrying the same action id.
        for branch in &u.inputs {
            let LogicalPlan::Project(p) = branch else { panic!("expected normalized branch Project"); };
            assert!(p.items.iter().any(|i| i.output_name.eq_ignore_ascii_case("__change_op") && i.output_column_id == action));
            let LogicalPlan::ImvDelta(d) = p.input.as_ref() else { panic!("expected Project(ImvDelta(..))"); };
            assert!(!d.is_root);
            assert_eq!(d.action_column, Some(action));
            assert!(matches!(d.input.as_ref(), LogicalPlan::Scan(_)));
        }
    }
}
```

- [ ] **Step 3: Run — verify it fails to compile**

Run: `cargo test --lib union_delta::tests`
Expected: compile error — `RewriteUnionAggregateDeltaRule` does not exist.

- [ ] **Step 4: Implement the rule (top of `union_delta.rs`)**

```rust
//! A-family IMV rewrite: `Delta(Aggregate(UNION ALL))`.
//!
//! Implements `Delta(UNION ALL children) = UNION ALL(Delta(child))` for the
//! aggregate-over-union fan-in shape. Mirrors `RewriteJoinAggregateDeltaRule`
//! (join_delta.rs): mark each branch's scan as Delta with a single shared
//! action column, normalize each branch's output, and re-emit the root
//! `Delta(Aggregate(Union(..)))` so the existing `RewriteAggregateStateRule`
//! consumes it unchanged. Unlike join-delta there is no version side: each
//! branch is fully delta because UNION ALL is row concatenation.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::join_delta::{
    mark_delta_scan, normalize_branch_output, plan_output_columns,
};
use crate::sql::optimizer::rewrite::imv::marker::{ImvDeltaNode, plan_contains_imv_marker};
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, UnionNode};

pub(crate) struct RewriteUnionAggregateDeltaRule;

impl LogicalRewriteRule for RewriteUnionAggregateDeltaRule {
    fn name(&self) -> &'static str {
        "RewriteUnionAggregateDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(delta)
                if delta.is_root
                    && matches!(
                        delta.input.as_ref(),
                        LogicalPlan::Aggregate(aggregate)
                            if matches!(aggregate.input.as_ref(), LogicalPlan::Union(u)
                                // Marker guard: only the SOURCE union (no markers
                                // yet). After this rule (or join-delta) runs, the
                                // union's branches carry markers and we must not
                                // re-match.
                                if u.all && !plan_contains_imv_marker(aggregate.input.as_ref()))
                    )
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else { return Ok(RewriteResult::Unchanged); };
        if !delta.is_root { return Ok(RewriteResult::Unchanged); }
        let LogicalPlan::Aggregate(mut aggregate) = *delta.input else { return Ok(RewriteResult::Unchanged); };
        let LogicalPlan::Union(union) = *aggregate.input else { return Ok(RewriteResult::Unchanged); };
        if !union.all {
            return Err("Iceberg IMV aggregate-over-union rewrite supports UNION ALL only".to_string());
        }
        if plan_contains_imv_marker(&LogicalPlan::Union(union.clone())) {
            // Already-marked union (e.g. join-delta output) is not ours.
            aggregate.input = Box::new(LogicalPlan::Union(union));
            return Ok(RewriteResult::Unchanged);
        }

        let action_column = match delta.action_column {
            Some(column) => column,
            None => ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| {
                    "RewriteUnionAggregateDelta requires ImvExtension in RewriteContext".to_string()
                })?
                .allocate_column_id(),
        };

        let UnionNode { inputs, all: _, output_columns, required_output_columns } = union;

        // Union output = first branch's user columns + shared action column.
        let mut union_output = output_columns;
        union_output.push(ImvActionColumn::output_column(action_column));

        let mut new_inputs = Vec::with_capacity(inputs.len());
        for branch in inputs {
            // Per-branch output: that branch's own column ids + the shared
            // action column id. Position-aligned to `union_output`.
            let mut branch_output = plan_output_columns(&branch)?;
            branch_output.push(ImvActionColumn::output_column(action_column));
            let marked = mark_delta_scan(branch, action_column)?;
            new_inputs.push(normalize_branch_output(marked, &branch_output));
        }

        aggregate.input = Box::new(LogicalPlan::Union(UnionNode {
            inputs: new_inputs,
            all: true,
            output_columns: union_output,
            required_output_columns,
        }));

        Ok(RewriteResult::Changed(LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(LogicalPlan::Aggregate(aggregate)),
            is_root: true,
            action_column: Some(action_column),
        })))
    }
}
```

Add `mod union_delta;` to `src/sql/optimizer/rewrite/imv/mod.rs`.

- [ ] **Step 5: Register the pipeline stage**

In `pipeline.rs`, add a stage between `imv-join-delta` and `imv-aggregate-state`. Import the rule and insert:

```rust
use crate::sql::optimizer::rewrite::imv::union_delta::RewriteUnionAggregateDeltaRule;
```
```rust
        RewriteStage::new(
            "imv-union-delta",
            RewritePhase::StructuralRewrite,
            vec![Box::new(RewriteUnionAggregateDeltaRule) as Box<dyn LogicalRewriteRule>],
        ),
```
(place the `RewriteStage::new("imv-union-delta", ..)` immediately after the `imv-join-delta` stage and before `imv-aggregate-state`).

Update the pipeline order test `pipeline_runs_join_and_aggregate_rewrite_before_generic_delta_pushdown` to also assert `union < agg`:

```rust
        let union = names.iter().position(|n| *n == "imv-union-delta").expect("union delta stage");
        assert!(join < union, "stage order: {names:?}");
        assert!(union < agg, "stage order: {names:?}");
```

- [ ] **Step 6: Run — verify tests pass**

Run: `cargo test --lib union_delta::tests pipeline::tests`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/sql/optimizer/rewrite/imv/
git commit -m "feat(imv): A-family RewriteUnionAggregateDelta rule and pipeline stage"
```

### Task 2.2: Generalize action propagation for fan-in delta union

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/action_propagation.rs`

- [ ] **Step 1: Write the failing test**

Add to `action_propagation.rs` tests:

```rust
#[test]
fn accepts_fan_in_delta_union_above_delta_scans() {
    // Union whose branches are normalized projections over delta scans, all
    // sharing one action column, must be accepted (not fail-fast).
    let rule = PropagateActionColumnRule;
    let ctx = build_ctx();
    let mk_branch = || {
        let scan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: TypedExpr { kind: ExprKind::ColumnRef { column_id: ColumnId(100), qualifier: None, column: "__change_op".into() }, data_type: DataType::Int8, nullable: false },
                output_name: "__change_op".into(),
                output_column_id: ColumnId(100),
            }],
            required_output_columns: None,
        })
    };
    let union = LogicalPlan::Union(UnionNode {
        inputs: vec![mk_branch(), mk_branch()],
        all: true,
        output_columns: Vec::new(),
        required_output_columns: None,
    });
    // The generalized predicate must NOT flag this union for fail-fast.
    assert!(!rule.matches(&union, &ctx), "fan-in delta union must be accepted, not fail-fast");
}
```

- [ ] **Step 2: Run — verify it fails**

Run: `cargo test --lib action_propagation::tests::accepts_fan_in_delta_union_above_delta_scans`
Expected: FAIL — `is_supported_join_delta_union(u)` is false for this shape, so `matches()` returns true (fail-fast path).

- [ ] **Step 3: Generalize the union predicate**

In `action_propagation.rs`, change the `Union` arm of `PropagateActionColumnRule::matches` to also accept fan-in delta unions:

```rust
            LogicalPlan::Union(u) => {
                u.inputs.iter().any(subtree_has_action_column)
                    && !is_supported_join_delta_union(u)
                    && !is_supported_fan_in_delta_union(u)
            }
```

Add the new predicate (a sibling of `is_supported_join_delta_union`):

```rust
/// A-family fan-in union: every branch is a delta-scan subtree (Project/Filter
/// over an `IcebergDeltaTable` scan) with NO version side. These come from
/// `RewriteUnionAggregateDelta`. Action column is shared across branches.
fn is_supported_fan_in_delta_union(node: &crate::sql::planner::plan::UnionNode) -> bool {
    node.all
        && !node.inputs.is_empty()
        && node.inputs.iter().all(|branch| {
            subtree_has_delta_scan(branch) && !subtree_has_version_scan(branch)
        })
}
```

(`subtree_has_delta_scan` and `subtree_has_version_scan` already exist in this file.)

- [ ] **Step 4: Run — verify it passes + existing tests still pass**

Run: `cargo test --lib action_propagation::tests`
Expected: PASS (new test green; `propagate_rejects_union` still rejects the bare single-scan union because that branch is a raw delta scan with no version — wait, that test's union branch IS a delta scan, so it would now be accepted). 

> IMPORTANT: the existing `propagate_rejects_union` test builds `Union { inputs: vec![Scan(delta_scan_with_action)] }` — a single bare delta scan. Under the generalized predicate this is now a *valid* fan-in union (accepted), so that test's expectation changes. UPDATE `propagate_rejects_union` to assert the new behavior: a fan-in delta union is accepted (rule does not match). Replace its body to assert `!rule.matches(&plan, &ctx)`, OR repoint it to a genuinely-unsupported union (e.g. a branch mixing a delta scan with an aggregate that is not signed-state). Prefer: rename to `accepts_bare_fan_in_delta_union` and assert `!rule.matches`.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/sql/optimizer/rewrite/imv/action_propagation.rs
git commit -m "feat(imv): accept fan-in delta union in action propagation"
```

### Task 2.3: Generalize action-column validation for fan-in delta union

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/action_column.rs`

- [ ] **Step 1: Write the failing test**

`ActionColumnValidationRule` currently rejects general unions above delta (action_column.rs:167). Add a test that a fan-in delta union passes validation. Mirror the style of existing validation tests in `action_column.rs` (build a `Union` of delta-scan projections sharing an action column, run the rule, expect `RewriteResult::Unchanged` / not `Rejected`).

```rust
#[test]
fn validation_accepts_fan_in_delta_union() {
    // Build Aggregate-free fan-in union of two delta-scan projections sharing
    // an action column; ActionColumnValidationRule must not reject it.
    // (Use the same fixtures the other validation tests in this module use.)
    // ... assert apply() returns Ok(RewriteResult::Unchanged), not Rejected.
}
```

> Fill the body by mirroring an existing `action_column.rs` validation test that constructs a delta-scan union; the assertion is "not Rejected".

- [ ] **Step 2: Run — verify it fails**

Run: `cargo test --lib action_column::tests::validation_accepts_fan_in_delta_union`
Expected: FAIL — the `Union` arm at action_column.rs:167 rejects.

- [ ] **Step 3: Generalize the validation union arm**

In `action_column.rs`, the arm that rejects general union above delta (around line 167) must also accept fan-in delta unions. Reuse the same structural check used in propagation. Either import `is_supported_fan_in_delta_union` (make it `pub(crate)` in `action_propagation.rs`) or replicate the check. Prefer making the propagation predicate `pub(crate)` and importing it:

```rust
// action_propagation.rs
pub(crate) fn is_supported_fan_in_delta_union(node: &crate::sql::planner::plan::UnionNode) -> bool { /* as above */ }
```

```rust
// action_column.rs — change the Union rejection arm:
LogicalPlan::Union(u)
    if subtree_has_delta(plan)
        && !is_supported_join_delta_union(u)
        && !is_supported_fan_in_delta_union(u) =>
{
    let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
    Err(format!(
        "Iceberg IMV rewrite does not support this union shape above delta-bound scan {fqn}"
    ))
}
```

> Check whether `is_supported_join_delta_union` is referenced from `action_column.rs` already (the spec noted both rules share union acceptance). If `action_column.rs` validates signed-delta input via a different helper, generalize that helper instead. The behavior: fan-in delta unions pass validation.

- [ ] **Step 4: Run — verify it passes**

Run: `cargo test --lib action_column::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/sql/optimizer/rewrite/imv/action_column.rs src/sql/optimizer/rewrite/imv/action_propagation.rs
git commit -m "feat(imv): accept fan-in delta union in action-column validation"
```

### Task 2.4: Route A-family through the aggregate refresh path

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs` (`plan_iceberg_mv_refresh`)

- [ ] **Step 1: Remove the Stage-1 A-family fail-fast guard**

Delete the `matches!(&shape, IcebergMvShape::Aggregate(a) if !a.fan_in_bases.is_empty())` guard added in Task 1.8. The existing `aggregate_shape_for_layout(&shape).is_some()` branch already routes `Aggregate` shapes (including fan-in) to `plan_iceberg_aggregate_mv_refresh`.

- [ ] **Step 2: Ensure multi-base pin for fan-in**

In `plan_iceberg_aggregate_mv_refresh`, the base refs come from the shape's `base_tables()` (now returns the fan-in list for A-family — Task 1.1). Confirm `RefreshSnapshotPin::capture(state, &base_refs)` is called with all fan-in bases (it iterates `base_refs`). Read the aggregate refresh path to confirm `base_refs` is derived from `shape.base_tables()` / `mv_definition` and includes all fan-in bases. If it currently assumes a single base ref, generalize the base-ref collection to use `shape.base_tables()`.

- [ ] **Step 3: Verify (gated by Stage 4 fixture)**

A-family end-to-end correctness is verified by `iceberg_ivm_union_all_aggregate_basic.sql` (Stage 4). For this task, gate with: `cargo build --lib` clean, and a focused check that `plan_iceberg_aggregate_mv_refresh` receives all bases. Add a debug-log or a unit assertion if a pure seam exists; otherwise rely on the Stage 4 fixture and note it here.

- [ ] **Step 4: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(imv): route aggregate-over-UNION-ALL through aggregate refresh path"
```

---

## Stage 3 — B 族 rewrite + `__branch_id__` apply side

**Stage goal:** `Delta(Union(branch₁..branchₙ))` (top-level union; aggregate or projection branches) rewrites to `Union(branch-scoped merge / delta-projection)` with each branch tagged `__branch_id__ = i`; the apply side locates/merges target rows by the composite `(__branch_id__, inner_row_id)` so same-key rows across branches stay independent. End-to-end correctness is gated by the Stage 4 B-family fixtures.

### Task 3.1: Extract a branch-parameterized `AggregateStateMerge` builder

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn build_aggregate_state_merge_threads_branch_scope() {
    let mut ctx = build_ctx();
    let ext = ctx.extension::<ImvExtension>().unwrap().clone();
    let LogicalPlan::Aggregate(agg) = aggregate_over(leaf_scan()) else { unreachable!() };
    let merge = build_aggregate_state_merge(agg, None, Some(1), &ext)
        .expect("branch-scoped merge builds");
    let LogicalPlan::AggregateStateMerge(node) = &merge else {
        panic!("expected AggregateStateMerge");
    };
    let LogicalPlan::Scan(old_scan) = node.old_input.as_ref() else { panic!("target-state scan"); };
    let crate::sql::catalog::ScanSource::IcebergMvTargetState(ts) = &old_scan.table.source else {
        panic!("IcebergMvTargetState");
    };
    // Branch scope must be recorded on the target-state row filter.
    assert!(matches!(
        &ts.row_filter,
        crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds { branch_scope: Some(b), .. }
            if b.branch_id == 1
    ));
}
```

- [ ] **Step 2: Run — verify it fails to compile**

Run: `cargo test --lib aggregate_rewrite::tests::build_aggregate_state_merge_threads_branch_scope`
Expected: compile error — `build_aggregate_state_merge` and `branch_scope` field do not exist.

- [ ] **Step 3: Extract the builder**

Refactor `RewriteAggregateStateRule::apply` so its body (everything after the `Aggregate` extraction and the empty-group-by / distinct guards) moves into a reusable function. The single-aggregate path calls it with `branch_id = None`; the B-family rule (Task 3.3) calls it per branch with `branch_id = Some(i)`.

```rust
/// Build an `AggregateStateMerge` from a delta-marked or unmarked aggregate.
/// `branch_id = Some(i)` scopes the target-state read to `__branch_id__ = i`
/// (B-family UNION ALL of aggregates); `None` is the ordinary single-base path.
pub(crate) fn build_aggregate_state_merge(
    aggregate: AggregateNode,
    action_column: Option<ColumnId>,
    branch_id: Option<i32>,
    ext: &ImvExtension,
) -> Result<LogicalPlan, String> {
    // (move the existing apply() body here: group_key_names, aggregate_state_names,
    //  row_id_column_name, target_columns, partition_constraint, old_source, etc.)
    // The ONLY behavioral change vs the current apply(): build the row filter as
    //   IcebergMvTargetStateRowFilter::DeltaInputRowIds {
    //       row_id_column_name: row_id_column_name.clone(),
    //       branch_scope: branch_id.map(|id| BranchScope {
    //           branch_id_column_name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
    //           branch_id: id,
    //       }),
    //   }
    // and pass `branch_id` through to build_target_state_scan_source (Task 3.2).
    // The action column default is `action_column.unwrap_or_else(|| ext.allocate_column_id())`.
    // Returns LogicalPlan::AggregateStateMerge(AggregateStateMergeNode { .. }) exactly as today.
}
```

Then make `RewriteAggregateStateRule::apply` a thin wrapper:

```rust
    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else { return Ok(RewriteResult::Unchanged); };
        if !delta.is_root { return Ok(RewriteResult::Unchanged); }
        let LogicalPlan::Aggregate(aggregate) = *delta.input else { return Ok(RewriteResult::Unchanged); };
        if aggregate.group_by.is_empty() {
            return Err("Iceberg IMV aggregate rewrite requires at least one GROUP BY key".to_string());
        }
        if aggregate.aggregates.iter().any(|call| call.distinct) {
            return Err("Iceberg IMV aggregate rewrite does not support SELECT DISTINCT".to_string());
        }
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| "RewriteAggregateState requires ImvExtension in RewriteContext".to_string())?
            .clone();
        let merge = build_aggregate_state_merge(aggregate, delta.action_column, None, &ext)?;
        Ok(RewriteResult::Changed(merge))
    }
```

> `ImvExtension` is `Clone` (verified). Clone it out of `ctx` to drop the borrow before building. Move the group-by/distinct guards into `build_aggregate_state_merge` if the B-family path should also enforce them (it should — keep them in the builder and have the wrapper rely on them).

- [ ] **Step 4: Run — verify it passes + existing aggregate tests still pass**

Run: `cargo test --lib aggregate_rewrite::tests`
Expected: all PASS (the refactor is behavior-preserving for `branch_id = None`; existing tests like `rewrite_aggregate_state_builds_state_merge_with_signed_delta` still pass).

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs
git commit -m "refactor(imv): extract branch-parameterized AggregateStateMerge builder"
```

### Task 3.2: Branch scope on the target-state row filter

**Files:**
- Modify: `src/sql/catalog.rs` (`IcebergMvTargetStateRowFilter`, add `BranchScope`)
- Modify: `src/sql/optimizer/rewrite/imv/target_state.rs` (`build_target_state_scan_source` signature)

- [ ] **Step 1: Write the failing test**

In `catalog.rs` (or wherever `IcebergMvTargetStateRowFilter` lives) add a small construction test, or rely on Task 3.1's test which already references `branch_scope`. Add a focused type test:

```rust
#[test]
fn target_state_row_filter_carries_branch_scope() {
    let f = IcebergMvTargetStateRowFilter::DeltaInputRowIds {
        row_id_column_name: "__row_id__".to_string(),
        branch_scope: Some(BranchScope { branch_id_column_name: "__branch_id__".to_string(), branch_id: 2 }),
    };
    let IcebergMvTargetStateRowFilter::DeltaInputRowIds { branch_scope: Some(b), .. } = f else {
        panic!("expected branch scope");
    };
    assert_eq!(b.branch_id, 2);
}
```

- [ ] **Step 2: Run — verify it fails to compile**

Run: `cargo test --lib <module>::target_state_row_filter_carries_branch_scope` (module = wherever the enum is, e.g. `catalog::tests`).
Expected: compile error — `branch_scope` field and `BranchScope` missing.

- [ ] **Step 3: Add `BranchScope` + extend the row filter**

In `src/sql/catalog.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchScope {
    pub(crate) branch_id_column_name: String,
    pub(crate) branch_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergMvTargetStateRowFilter {
    DeltaInputRowIds {
        row_id_column_name: String,
        /// `Some` for B-family UNION ALL of aggregates: restrict the target-state
        /// read to one branch so same-group-key rows across branches stay
        /// independent. `None` for single-base / fan-in aggregates.
        branch_scope: Option<BranchScope>,
    },
}
```

Update `build_target_state_scan_source` in `target_state.rs` — the `row_filter` param already carries the variant, so callers construct the new field. No signature change needed IF callers build the full `IcebergMvTargetStateRowFilter`. Confirm `build_target_state_scan_source` takes `row_filter: IcebergMvTargetStateRowFilter` (it does, per the extracted signature) — then only the call sites change.

Update EVERY existing construction of `DeltaInputRowIds { row_id_column_name }` to add `branch_scope: None` (the compiler lists them: `aggregate_rewrite.rs` and its tests, plus any lowering code in `src/lower/` that matches the variant — see Task 3.6).

- [ ] **Step 4: Run — verify it passes + build**

Run: `cargo test --lib` (the new test) and `cargo build --lib`.
Expected: PASS + clean build once all `DeltaInputRowIds { .. }` sites updated.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/sql/catalog.rs src/sql/optimizer/rewrite/imv/target_state.rs src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs
git commit -m "feat(imv): add branch scope to IcebergMvTargetState row filter"
```

### Task 3.3: `RewriteBranchUnionRule` (B-family) + pipeline stage

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/branch_union.rs`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`, `pipeline.rs`

- [ ] **Step 1: Write the failing test (aggregate branches)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    // ... imports mirroring union_delta.rs tests (build_ctx, scan, aggregate_over, root_delta) ...

    #[test]
    fn rewrites_top_union_of_aggregates_into_union_of_branch_merges() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx_with_aggregate_contract(); // needs aggregate state contract, like aggregate_rewrite tests
        // Delta[root]( Union( Aggregate(scan t1) GROUP BY k, Aggregate(scan t2) GROUP BY k ) )
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![aggregate_over(scan("t1", 1)), aggregate_over(scan("t2", 10))],
            all: true,
            output_columns: vec![out_col(1, "k", false)],
            required_output_columns: None,
        }));
        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::Union(u)) = rule.apply(plan, &mut ctx).expect("rewrite") else {
            panic!("expected Changed(Union)");
        };
        assert_eq!(u.inputs.len(), 2);
        // Each branch: Project that appends __branch_id__ = literal(i) over an AggregateStateMerge.
        for (i, branch) in u.inputs.iter().enumerate() {
            let LogicalPlan::Project(p) = branch else { panic!("expected Project per branch"); };
            let branch_item = p.items.iter().find(|it| it.output_name.eq_ignore_ascii_case("__branch_id__")).expect("__branch_id__ item");
            assert!(matches!(&branch_item.expr.kind, ExprKind::Literal(LiteralValue::Int(n)) if *n == i as i64));
            assert!(matches!(p.input.as_ref(), LogicalPlan::AggregateStateMerge(_)));
        }
    }
}
```

- [ ] **Step 2: Run — verify it fails**

Run: `cargo test --lib branch_union::tests::rewrites_top_union_of_aggregates_into_union_of_branch_merges`
Expected: compile error — `RewriteBranchUnionRule` missing.

- [ ] **Step 3: Implement `RewriteBranchUnionRule`**

```rust
//! B-family IMV rewrite: top-level `Delta(UNION ALL(branches))`.
//!
//! Each branch (all aggregate, or all projection/filter) stays independent via
//! a hidden `__branch_id__` literal column, so same-group-key / same-base-row-id
//! rows across branches do not collide on the target apply key. Produces
//! `Union(Project(branch_plan, __branch_id__=i))`. Aggregate branches reuse
//! `build_aggregate_state_merge(.., branch_id=Some(i))`; projection branches push
//! Delta to the leaf scan and append the apply key + `__branch_id__`.

use arrow::datatypes::DataType;

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN;
use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::aggregate_rewrite::build_aggregate_state_merge;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::join_delta::plan_output_columns;
use crate::sql::optimizer::rewrite::imv::marker::{ImvDeltaNode, plan_contains_imv_marker};
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ProjectNode, UnionNode};

pub(crate) struct RewriteBranchUnionRule;

impl LogicalRewriteRule for RewriteBranchUnionRule {
    fn name(&self) -> &'static str { "RewriteBranchUnion" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }
    fn traversal(&self) -> RewriteTraversal { RewriteTraversal::TopDown }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(delta)
                if delta.is_root
                    && matches!(delta.input.as_ref(), LogicalPlan::Union(u)
                        // Top union whose branches are NOT already an A-family
                        // marked union (guard against re-matching). Branch kind
                        // (aggregate vs projection) is validated below.
                        if u.all && !plan_contains_imv_marker(delta.input.as_ref()))
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else { return Ok(RewriteResult::Unchanged); };
        if !delta.is_root { return Ok(RewriteResult::Unchanged); }
        let LogicalPlan::Union(union) = *delta.input else { return Ok(RewriteResult::Unchanged); };
        if !union.all {
            return Err("Iceberg IMV UNION ALL rewrite supports UNION ALL only".to_string());
        }
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| "RewriteBranchUnion requires ImvExtension in RewriteContext".to_string())?
            .clone();

        let UnionNode { inputs, all: _, output_columns, required_output_columns } = union;
        let branch_count = inputs.len();
        if branch_count < 2 {
            return Err("Iceberg IMV UNION ALL rewrite requires at least two branches".to_string());
        }

        let mut new_inputs = Vec::with_capacity(branch_count);
        for (i, branch) in inputs.into_iter().enumerate() {
            let branch_id = i32::try_from(i)
                .map_err(|_| "Iceberg IMV UNION ALL branch index overflow".to_string())?;
            let branch_plan = match branch {
                // Aggregate branch -> branch-scoped AggregateStateMerge.
                LogicalPlan::Aggregate(agg) => {
                    build_aggregate_state_merge(agg, None, Some(branch_id), &ext)?
                }
                // Projection/filter branch -> mark scan as Delta; the existing
                // pushdown / scan-binding / action-propagation / apply-key stages
                // finish it. Wrap so Delta sinks to the leaf scan.
                other => LogicalPlan::ImvDelta(ImvDeltaNode {
                    input: Box::new(other),
                    is_root: false,
                    action_column: None,
                }),
            };
            new_inputs.push(append_branch_id_projection(branch_plan, branch_id, &ext)?);
        }

        Ok(RewriteResult::Changed(LogicalPlan::Union(UnionNode {
            inputs: new_inputs,
            all: true,
            output_columns: branch_union_output_columns(output_columns, &ext),
            required_output_columns,
        })))
    }
}

/// Wrap `branch_plan` in a Project that re-exposes its columns and appends a
/// constant `__branch_id__ = branch_id` internal column.
fn append_branch_id_projection(
    branch_plan: LogicalPlan,
    branch_id: i32,
    ext: &ImvExtension,
) -> Result<LogicalPlan, String> {
    let mut items: Vec<ProjectItem> = plan_output_columns(&branch_plan)?
        .into_iter()
        .map(|c| ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef { column_id: c.column_id, qualifier: None, column: c.name.clone() },
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            },
            output_name: c.name,
            output_column_id: c.column_id,
        })
        .collect();
    items.push(ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(branch_id as i64)),
            data_type: DataType::Int32,
            nullable: false,
        },
        output_name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        output_column_id: ext.allocate_column_id(),
    });
    Ok(LogicalPlan::Project(ProjectNode {
        input: Box::new(branch_plan),
        items,
        required_output_columns: None,
    }))
}

fn branch_union_output_columns(
    mut first_branch_output: Vec<OutputColumn>,
    ext: &ImvExtension,
) -> Vec<OutputColumn> {
    first_branch_output.push(OutputColumn {
        column_id: ext.allocate_column_id(),
        name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_internal: true,
    });
    first_branch_output
}
```

> `LiteralValue::Int` carries an `i64` in this codebase (see `signed_value_arg` in aggregate_rewrite.rs which uses `LiteralValue::Int(1)` for a count literal of `DataType::Int64`). The `__branch_id__` column type is `Int32`; emit the literal with `data_type: DataType::Int32` and value `branch_id as i64`. If `LiteralValue` has a dedicated 32-bit variant, prefer it. Confirm against `src/sql/analysis` `LiteralValue`.

Add `mod branch_union;` to `imv/mod.rs`.

- [ ] **Step 4: Register the pipeline stage**

In `pipeline.rs`, add `imv-branch-union` in the StructuralRewrite phase, BEFORE `imv-delta-pushdown` and BEFORE `imv-union-delta`/`imv-aggregate-state` is fine since match conditions are disjoint (`Delta(Union)` vs `Delta(Aggregate(..))`). Place it right after `imv-delta-marker`:

```rust
use crate::sql::optimizer::rewrite::imv::branch_union::RewriteBranchUnionRule;
```
```rust
        RewriteStage::new(
            "imv-branch-union",
            RewritePhase::StructuralRewrite,
            vec![Box::new(RewriteBranchUnionRule) as Box<dyn LogicalRewriteRule>],
        ),
```

Add a stage-order assertion: `imv-branch-union` before `imv-delta-pushdown`.

- [ ] **Step 5: Run — verify the aggregate-branch test passes**

Run: `cargo test --lib branch_union::tests pipeline::tests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/sql/optimizer/rewrite/imv/
git commit -m "feat(imv): B-family RewriteBranchUnion rule with __branch_id__ injection"
```

### Task 3.4: Action propagation + validation for top branch union

**Files:**
- Modify: `action_propagation.rs`, `action_column.rs`

- [ ] **Step 1: Write the failing tests**

A top-level `Union(Project(AggregateStateMerge, __branch_id__=i), ..)` and `Union(Project(delta-scan, apply-key, __branch_id__=i), ..)` must pass propagation/validation (not fail-fast). Add tests mirroring Task 2.2/2.3 style asserting `!rule.matches` (propagation) and not-`Rejected` (validation) for a top branch union of `AggregateStateMerge` branches and a top branch union of delta-projection branches.

- [ ] **Step 2: Run — verify fail**

Run: `cargo test --lib action_propagation::tests action_column::tests` (new test names)
Expected: FAIL — current Union arms fail-fast.

- [ ] **Step 3: Generalize predicates**

Add `is_supported_branch_union` to `action_propagation.rs` (and export `pub(crate)`):

```rust
/// B-family top branch union: every branch is a Project that exposes a
/// `__branch_id__` column over either an AggregateStateMerge or a delta-scan
/// subtree. Produced by RewriteBranchUnion.
pub(crate) fn is_supported_branch_union(node: &crate::sql::planner::plan::UnionNode) -> bool {
    node.all
        && !node.inputs.is_empty()
        && node.inputs.iter().all(|branch| match branch {
            LogicalPlan::Project(p) => {
                p.items.iter().any(|it| {
                    it.output_name.eq_ignore_ascii_case(
                        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                    )
                }) && matches!(
                    p.input.as_ref(),
                    LogicalPlan::AggregateStateMerge(_) | LogicalPlan::Project(_)
                        | LogicalPlan::Filter(_) | LogicalPlan::ImvDelta(_)
                )
            }
            _ => false,
        })
}
```

Wire `&& !is_supported_branch_union(u)` into both the `PropagateActionColumnRule` Union arm (action_propagation.rs) and the `ActionColumnValidationRule` Union rejection arm (action_column.rs), alongside the join/fan-in predicates.

- [ ] **Step 4: Run — verify pass**

Run: `cargo test --lib action_propagation::tests action_column::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/sql/optimizer/rewrite/imv/action_propagation.rs src/sql/optimizer/rewrite/imv/action_column.rs
git commit -m "feat(imv): accept top branch union in action propagation/validation"
```

### Task 3.5: Composite branch-scoped target-row locator

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`

- [ ] **Step 1: Write the failing test**

The locator must restrict matches to one branch. Add a unit test for the scoping predicate. Extract the branch+key matching into a pure helper and test it directly (the full async scan is integration-tested by Stage 4):

```rust
#[test]
fn branch_scoped_key_matches_only_same_branch() {
    // requested: branch 1, keys {"k=1"}.
    // a target row (branch=0, key="k=1") must NOT match; (branch=1, key="k=1") must match.
    assert!(!branch_scoped_apply_key_matches(0, "k=1", 1, &requested(&["k=1"])));
    assert!(branch_scoped_apply_key_matches(1, "k=1", 1, &requested(&["k=1"])));
    assert!(!branch_scoped_apply_key_matches(1, "k=2", 1, &requested(&["k=1"])));
}
```

(Define a small `requested(&[&str]) -> HashSet<...>` test helper mirroring `requested_apply_key_values`.)

- [ ] **Step 2: Run — verify it fails**

Run: `cargo test --lib iceberg_target_apply::tests::branch_scoped_key_matches_only_same_branch`
Expected: compile error — helper missing.

- [ ] **Step 3: Add a branch-scoped locator**

Add `locate_target_rows_by_branch_scoped_apply_key` next to `locate_target_rows_by_string_apply_key`. It scans the target table selecting `_file`, `_pos`, the inner apply-key column, AND `__branch_id__`; a row matches iff `__branch_id__ == requested_branch_id` and the inner key is in the requested set. Mirror `locate_target_rows_by_apply_key_impl` (iceberg_target_apply.rs:453) but add `ICEBERG_MV_BRANCH_ID_COLUMN` to the projected columns and add the branch equality check inside `process_apply_key_locator_batch` (or a branch-aware sibling). Factor the matching predicate into the testable `branch_scoped_apply_key_matches(row_branch_id, row_key, requested_branch_id, requested_keys)`.

```rust
pub(crate) async fn locate_target_rows_by_branch_scoped_apply_key(
    target_table: &iceberg::table::Table,
    inner_apply_key_column: &str,
    branch_id: i32,
    requested_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    // ... mirror locate_target_rows_by_apply_key_impl, projecting
    //     ICEBERG_MV_BRANCH_ID_COLUMN as well, and keeping only rows whose
    //     branch id == `branch_id` AND inner key ∈ requested.
}
```

- [ ] **Step 4: Run — verify pass**

Run: `cargo test --lib iceberg_target_apply::tests::branch_scoped_key_matches_only_same_branch`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/engine/mv/iceberg_target_apply.rs
git commit -m "feat(imv): branch-scoped composite target-row locator"
```

### Task 3.6: Lower the branch-scoped target-state read

**Files:**
- Modify: the lowering for `ScanSource::IcebergMvTargetState` (search: `rg -n "IcebergMvTargetState" src/lower/ src/exec/ src/connector/`)

- [ ] **Step 1: Find the target-state scan lowering**

`ScanSource::IcebergMvTargetState` is lowered/executed somewhere that reads the target table filtered by `DeltaInputRowIds`. Locate it (`rg -n "IcebergMvTargetState|DeltaInputRowIds" src/lower src/exec src/runtime src/connector`). Read how it currently turns `DeltaInputRowIds { row_id_column_name }` into a target-table scan filtered by delta row ids.

- [ ] **Step 2: Write a failing test mirroring the existing target-state lowering test**

If a lowering/exec test exists for `IcebergMvTargetState` (look near the lowering code), add a sibling that sets `branch_scope: Some(BranchScope { .. })` and asserts the produced scan/filter restricts to `__branch_id__ = branch_id`. If no unit seam exists, this behavior is gated by the Stage 4 B-family fixture; document that and add a `// TODO(test): covered by iceberg_ivm_union_of_aggregates_basic.sql` note only if no seam is reachable.

- [ ] **Step 3: Implement branch scoping in the lowering**

Where the lowering builds the target-table read predicate from `DeltaInputRowIds`, when `branch_scope` is `Some`, AND a `__branch_id__ = branch_id` equality predicate to the scan filter (in addition to the row-id-in-set filter). This ensures branch i's merge only reads branch i's target rows — the isolation shown in spec §5.2 Step 3.

- [ ] **Step 4: Run — verify**

Run the lowering/exec unit test if added; otherwise `cargo build --lib` and rely on Stage 4.
Expected: clean build; branch predicate applied.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --lib
git add -A
git commit -m "feat(imv): apply branch scope when reading target aggregate state"
```

### Task 3.7: B-family refresh apply orchestration

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Study the single-aggregate apply orchestration**

Read `plan_iceberg_aggregate_mv_refresh` and the function that executes its plan + commits (the path that calls `merge_aggregate_target_state` and `locate_target_rows_by_*` then commits position-deletes + inserts to the target). This is the template to generalize per-branch.

- [ ] **Step 2: Remove the Stage-1 B-family guard + add the union refresh path**

Delete the `IncrementalMvShape::UnionAll(_)` fail-fast guard from Task 1.8. Add a `UnionAll` dispatch arm in `plan_iceberg_mv_refresh` that:
1. Captures a multi-base pin over all branches' bases (`RefreshSnapshotPin::capture(state, &base_refs)` with `shape.base_tables()`).
2. Runs the IMV rewrite (which now yields `Union(branch Projects)` via Task 3.3) through the same `execute_query_with_options(.., mv_refresh_ctx=Some(ctx))` seam the aggregate/PF cutover uses.
3. Applies the result to the single target table. Because each branch's rows carry `__branch_id__` and (for aggregate branches) the merge is branch-scoped (Tasks 3.1/3.2/3.6), the apply is: for each output row, locate target rows by the composite `(__branch_id__, inner_key)` (Task 3.5) and commit position-deletes + inserts. Aggregate branches reuse `merge_aggregate_target_state` per branch (the branch-scoped target-state read already isolates branch i); projection branches reuse the PF apply with the composite locator.

> The cleanest structure: keep ONE target commit per refresh, accumulating per-branch delete groups + insert chunks, then commit once. Mirror how the join/aggregate path accumulates and commits. Empty-delta branches contribute nothing; if ALL branches are empty, take the metadata-only refresh path (`record_iceberg_mv_metadata_only_publish`).

- [ ] **Step 3: Verify — gated by Stage 4 fixtures**

This task's correctness is verified by the Stage 4 B-family fixtures (`iceberg_ivm_union_of_aggregates_basic.sql` etc.). Gate the commit on `cargo build --lib` clean + the fixtures passing in Stage 4.

- [ ] **Step 4: Commit**

```bash
cargo fmt && cargo clippy --lib
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(imv): B-family UNION ALL refresh apply orchestration"
```

---

## Stage 4 — SQL fixtures + plan-shape goldens

**Stage goal:** End-to-end regression coverage in the `iceberg-ivm` suite for all three shapes, plus negative cases. These fixtures are the authoritative correctness gate for Stages 2–3.

**How to run the suite** (needs a standalone-server; see CLAUDE.md §7.3):

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# build + start server (debug is fine for correctness)
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" >/tmp/nr.log 2>&1 &
# wait for: grep -q '^NOVAROCKS_READY ' /tmp/nr.log
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode verify \
  --only iceberg_ivm_union_all_aggregate_basic
```

(Use `--mode record` once to generate the golden `result/` file, review it, then `--mode verify`.)

### Task 4.1: A-family fixture — aggregate over UNION ALL (2 branch, INSERT + DELETE)

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_all_aggregate_basic.sql`
- Create (via `--mode record`): `sql-tests/iceberg-ivm/result/iceberg_ivm_union_all_aggregate_basic.result`

- [ ] **Step 1: Write the fixture**

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,union_all,aggregate,fan_in
-- Test Point: aggregate OVER UNION ALL (A-family). Two Iceberg bases fan into
-- one GROUP BY; same group key across branches MUST merge into one row.
-- Incremental refresh after INSERT and DELETE on each base equals full recompute.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_uaa_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_uaa_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_uaa_${uuid0}.ns_${uuid0};
CREATE TABLE ice_uaa_${uuid0}.ns_${uuid0}.t1 (k BIGINT, v BIGINT)
  TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE ice_uaa_${uuid0}.ns_${uuid0}.t2 (k BIGINT, v BIGINT)
  TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
SET CATALOG ice_uaa_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW uaa_mv_${uuid0}
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT k, SUM(v) AS s, COUNT(*) AS c
FROM (
  SELECT k, v FROM ice_uaa_${uuid0}.ns_${uuid0}.t1
  UNION ALL
  SELECT k, v FROM ice_uaa_${uuid0}.ns_${uuid0}.t2
) u
GROUP BY k;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_uaa_${uuid0}.ns_${uuid0}.t1 VALUES (1, 10), (1, 20), (2, 5);
INSERT INTO ice_uaa_${uuid0}.ns_${uuid0}.t2 VALUES (1, 100), (3, 7);

-- query 3
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW uaa_mv_${uuid0};

-- query 4
-- k=1 merges across both branches: 10+20+100 = 130, count 3.
SELECT k, s, c FROM uaa_mv_${uuid0} ORDER BY k;

-- query 5
-- Cross-check against full recompute of the same query.
SELECT k, SUM(v) AS s, COUNT(*) AS c
FROM (
  SELECT k, v FROM ice_uaa_${uuid0}.ns_${uuid0}.t1
  UNION ALL
  SELECT k, v FROM ice_uaa_${uuid0}.ns_${uuid0}.t2
) u
GROUP BY k ORDER BY k;

-- query 6
-- @skip_result_check=true
DELETE FROM ice_uaa_${uuid0}.ns_${uuid0}.t1 WHERE v = 10;
INSERT INTO ice_uaa_${uuid0}.ns_${uuid0}.t2 VALUES (2, 50);
REFRESH MATERIALIZED VIEW uaa_mv_${uuid0};

-- query 7
SELECT k, s, c FROM uaa_mv_${uuid0} ORDER BY k;

-- query 8
SELECT k, SUM(v) AS s, COUNT(*) AS c
FROM (
  SELECT k, v FROM ice_uaa_${uuid0}.ns_${uuid0}.t1
  UNION ALL
  SELECT k, v FROM ice_uaa_${uuid0}.ns_${uuid0}.t2
) u
GROUP BY k ORDER BY k;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW uaa_mv_${uuid0};
DROP TABLE ice_uaa_${uuid0}.ns_${uuid0}.t1 FORCE;
DROP TABLE ice_uaa_${uuid0}.ns_${uuid0}.t2 FORCE;
DROP DATABASE ice_uaa_${uuid0}.ns_${uuid0};
DROP CATALOG ice_uaa_${uuid0};
```

- [ ] **Step 2: Record + review the golden**

Run the suite with `--mode record --only iceberg_ivm_union_all_aggregate_basic`. Open the generated `result/iceberg_ivm_union_all_aggregate_basic.result` and verify query 4 == query 5 and query 7 == query 8 (incremental == full recompute). Expected query 4: `1\t130\t3`, `2\t5\t1`, `3\t7\t1`.

- [ ] **Step 3: Verify**

Run with `--mode verify --only iceberg_ivm_union_all_aggregate_basic`. Expected: PASS, and the `@explain_contains` assertions confirm the refresh used the logical-rewrite path (not full rebuild).

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_union_all_aggregate_basic.sql sql-tests/iceberg-ivm/result/iceberg_ivm_union_all_aggregate_basic.result
git commit -m "test(imv): aggregate-over-UNION-ALL incremental refresh fixture"
```

### Task 4.2: A-family fixture — three branches + nested flatten

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_all_aggregate_three_branch.sql` (+ result)

- [ ] **Step 1: Write the fixture**

Same structure as Task 4.1 but three bases `t1,t2,t3` and a nested union to exercise flatten: `((SELECT .. t1 UNION ALL SELECT .. t2) UNION ALL SELECT .. t3)`. Insert disjoint + overlapping keys across all three, refresh, and cross-check against full recompute. Include one DELETE that empties one branch's contribution to a key.

- [ ] **Step 2-4: Record, verify, commit** (same commands as Task 4.1).

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_union_all_aggregate_three_branch.sql sql-tests/iceberg-ivm/result/iceberg_ivm_union_all_aggregate_three_branch.result
git commit -m "test(imv): three-branch nested aggregate-over-UNION-ALL fixture"
```

### Task 4.3: B-family fixture — UNION ALL of aggregates (headline: same key not merged)

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_basic.sql` (+ result)

- [ ] **Step 1: Write the fixture**

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,union_all,aggregate,branch
-- Test Point (headline task 9): UNION ALL of two aggregate branches. Same
-- group key across branches MUST stay as two separate rows (bag semantics).
-- Mutating one branch must NOT touch the other branch's same-key row.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_uoa_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_uoa_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_uoa_${uuid0}.ns_${uuid0};
CREATE TABLE ice_uoa_${uuid0}.ns_${uuid0}.t1 (k BIGINT, v BIGINT)
  TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE ice_uoa_${uuid0}.ns_${uuid0}.t2 (k BIGINT, v BIGINT)
  TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
SET CATALOG ice_uoa_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW uoa_mv_${uuid0}
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT k, SUM(v) AS s FROM ice_uoa_${uuid0}.ns_${uuid0}.t1 GROUP BY k
UNION ALL
SELECT k, SUM(v) AS s FROM ice_uoa_${uuid0}.ns_${uuid0}.t2 GROUP BY k;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_uoa_${uuid0}.ns_${uuid0}.t1 VALUES (1, 10), (1, 20), (2, 5);
INSERT INTO ice_uoa_${uuid0}.ns_${uuid0}.t2 VALUES (1, 100), (3, 7);

-- query 3
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW uoa_mv_${uuid0};

-- query 4
-- Two rows with k=1 (one per branch): (1,30) from t1 and (1,100) from t2.
SELECT k, s FROM uoa_mv_${uuid0} ORDER BY k, s;

-- query 5
-- Cross-check against full recompute.
SELECT k, s FROM (
  SELECT k, SUM(v) AS s FROM ice_uoa_${uuid0}.ns_${uuid0}.t1 GROUP BY k
  UNION ALL
  SELECT k, SUM(v) AS s FROM ice_uoa_${uuid0}.ns_${uuid0}.t2 GROUP BY k
) u ORDER BY k, s;

-- query 6
-- @skip_result_check=true
-- Mutate ONLY branch t2's k=1 (insert 50 -> branch t2 k=1 becomes 150).
INSERT INTO ice_uoa_${uuid0}.ns_${uuid0}.t2 VALUES (1, 50);
REFRESH MATERIALIZED VIEW uoa_mv_${uuid0};

-- query 7
-- Branch t1's (1,30) MUST be untouched; branch t2's k=1 MUST be 150.
SELECT k, s FROM uoa_mv_${uuid0} ORDER BY k, s;

-- query 8
SELECT k, s FROM (
  SELECT k, SUM(v) AS s FROM ice_uoa_${uuid0}.ns_${uuid0}.t1 GROUP BY k
  UNION ALL
  SELECT k, SUM(v) AS s FROM ice_uoa_${uuid0}.ns_${uuid0}.t2 GROUP BY k
) u ORDER BY k, s;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW uoa_mv_${uuid0};
DROP TABLE ice_uoa_${uuid0}.ns_${uuid0}.t1 FORCE;
DROP TABLE ice_uoa_${uuid0}.ns_${uuid0}.t2 FORCE;
DROP DATABASE ice_uoa_${uuid0}.ns_${uuid0};
DROP CATALOG ice_uoa_${uuid0};
```

- [ ] **Step 2: Record + review**

`--mode record`. Verify query 4: `1\t30`, `1\t100`, `2\t5`, `3\t7` (TWO k=1 rows). Verify query 7: `1\t30`, `1\t150`, `2\t5`, `3\t7` — the `1\t30` row is unchanged (proves branch isolation via `__branch_id__`). Confirm query 4==5 and query 7==8.

- [ ] **Step 3: Verify** — `--mode verify`.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_basic.sql sql-tests/iceberg-ivm/result/iceberg_ivm_union_of_aggregates_basic.result
git commit -m "test(imv): UNION ALL of aggregate branches headline fixture (bag semantics)"
```

### Task 4.4: B-family fixture — branch with empty delta + branch group DELETE

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_branch_empty.sql` (+ result)

- [ ] **Step 1: Write the fixture**

Same setup as Task 4.3. Scenario: after initial refresh, mutate ONLY t1, refresh (t2's branch has empty delta → its rows untouched, no current-snapshot fallback). Then DELETE all of one group in t1 and refresh — assert that group's t1 row is removed while t2's same-key row remains. Cross-check against full recompute at each step.

- [ ] **Step 2-4: Record, verify, commit.**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregates_branch_empty.sql sql-tests/iceberg-ivm/result/iceberg_ivm_union_of_aggregates_branch_empty.result
git commit -m "test(imv): UNION ALL of aggregates empty-branch + branch-delete fixture"
```

### Task 4.5: B-family fixture — projection/filter UNION ALL

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_projection_basic.sql` (+ result)

- [ ] **Step 1: Write the fixture**

Two bases `t1(id,name)`, `t2(id,name)`; MV is `SELECT id, name FROM t1 WHERE id > 0 UNION ALL SELECT id, name FROM t2 WHERE id < 100` (no aggregate). Insert overlapping `id` values into both bases (so the same base `_row_id`/id appears in both branches), refresh, and verify ALL rows are present (bag semantics — `(branch_id, base_row_id)` keeps them distinct). Then DELETE a row from t1 and refresh — verify only the t1-branch row is removed, the t2-branch row with the same id remains. Cross-check against full recompute. Assert `@result_not_contains=__branch_id__` and `@result_not_contains=__nova_base_row_id` on the SELECT to confirm internal columns are not user-visible.

- [ ] **Step 2-4: Record, verify, commit.**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_union_projection_basic.sql sql-tests/iceberg-ivm/result/iceberg_ivm_union_projection_basic.result
git commit -m "test(imv): projection/filter UNION ALL incremental refresh fixture"
```

### Task 4.6: Negative fixture — unsupported UNION shapes fail at CREATE

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_reject_unsupported.sql` (+ result)

- [ ] **Step 1: Write the fixture**

Mirror `iceberg_ivm_join_reject_unsupported.sql`. Each rejection is a `CREATE MATERIALIZED VIEW` with `@expect_error=<substring>`:

```sql
-- @sequential=true
-- @tags=mv,iceberg,ivm,union_all,reject
-- (catalog/db/tables setup as query 1, @skip_result_check=true; two v3 bases t1,t2)

-- UNION (distinct) rejected
-- @expect_error=UNION ALL
CREATE MATERIALIZED VIEW rej_distinct_${uuid0}
DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k FROM ice_rej_${uuid0}.ns_${uuid0}.t1
   UNION SELECT k FROM ice_rej_${uuid0}.ns_${uuid0}.t2;

-- INTERSECT rejected
-- @expect_error=not supported
CREATE MATERIALIZED VIEW rej_intersect_${uuid0}
DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k FROM ice_rej_${uuid0}.ns_${uuid0}.t1
   INTERSECT SELECT k FROM ice_rej_${uuid0}.ns_${uuid0}.t2;

-- mixed aggregate + projection branches rejected
-- @expect_error=same shape
CREATE MATERIALIZED VIEW rej_mixed_${uuid0}
DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, SUM(v) s FROM ice_rej_${uuid0}.ns_${uuid0}.t1 GROUP BY k
   UNION ALL SELECT k, v FROM ice_rej_${uuid0}.ns_${uuid0}.t2;

-- branch arity mismatch rejected
-- @expect_error=identical output
CREATE MATERIALIZED VIEW rej_arity_${uuid0}
DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, SUM(v) s FROM ice_rej_${uuid0}.ns_${uuid0}.t1 GROUP BY k
   UNION ALL SELECT k, SUM(v) s, COUNT(*) c FROM ice_rej_${uuid0}.ns_${uuid0}.t2 GROUP BY k;
```

> Match each `@expect_error` substring to the actual error strings from Task 1.2/1.3 (`union_all_non_all_error`, `union_all_mixed_shape_error`, `union_all_branch_output_mismatch_error`). If a partitioned-target rejection is reachable at CREATE for union MVs, add a `PARTITION BY` case with `@expect_error` matching the unpartitioned-only guard; otherwise cover partitioned-target rejection at refresh in a separate query.

- [ ] **Step 2-4: Record (errors captured), verify, commit.**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_union_reject_unsupported.sql sql-tests/iceberg-ivm/result/iceberg_ivm_union_reject_unsupported.result
git commit -m "test(imv): reject unsupported UNION shapes at CREATE"
```

### Task 4.7: Full suite regression + roadmap update

- [ ] **Step 1: Run the whole `iceberg-ivm` suite**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode verify`
Expected: ALL fixtures PASS (new union fixtures + no regression on existing ones).

- [ ] **Step 2: Run focused lib tests once more**

Run: `cargo test --lib mv_shape mv_contract sql::optimizer::rewrite::imv iceberg_target_apply`
Expected: PASS. Then `cargo fmt && cargo clippy --all-targets`.

- [ ] **Step 3: Update the roadmap**

Mark tasks 8 and 9 done in `NovaRocks Roadmap.md` (the Obsidian roadmap path in CLAUDE.md / spec §14), noting the projection-union extension and the PR.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs(imv): mark roadmap tasks 8/9 (UNION ALL delta rewrite) done"
```

---

## Self-Review

**Spec coverage** (spec §1–§13 → tasks):
- Three shapes (§1): A-family → Stage 2 + Task 4.1/4.2; task-9 B-family → Tasks 3.1–3.7 + 4.3/4.4; projection union → Tasks 3.3/3.5/3.7 + 4.5. ✓
- Non-goals / fail-fast (§2, §9): Task 1.2/1.3 (non-ALL, mixed, arity), Task 1.8/3.7 (unpartitioned-only via refresh guard + reject fixture 4.6). ✓
- Two union families + `__branch_id__` (§5): Stage 2 (A, no branch id) vs Stage 3 (B, `__branch_id__`). ✓
- Rewrite (§6): Tasks 2.1 (A), 3.3 (B); marker guards in both `matches()`. ✓
- Classifier/dispatch (§7.1): Tasks 1.1–1.4, 2.4, 3.7. ✓
- Contract + `__branch_id__` column (§7.2/7.3): Tasks 1.5, 1.6, 1.7. ✓
- Action propagation/validation (§7.4): Tasks 2.2, 2.3, 3.4. ✓
- Apply side (§7.5): Tasks 3.2, 3.5, 3.6, 3.7. ✓
- Pin/empty-delta (§8): Task 2.4 (multi-base pin), Task 4.4 (empty branch), Task 3.7 (all-empty → metadata-only). ✓
- Tests (§10): Stage 4. ✓

**Placeholder scan:** Runtime-execution tasks (3.6 lowering, 3.7 apply orchestration, 2.4 base-ref wiring) are specified as "generalize named existing function X, gated by fixture Y" rather than literal code, because they extend execution paths in `iceberg_refresh.rs` / `src/lower` that must be read at implementation time — each names the exact function to study and the fixture that proves it. Every other task has literal code + a failing test. No `TODO`/`TBD`/"implement later".

**Type consistency:** `__branch_id__` = `ICEBERG_MV_BRANCH_ID_COLUMN` (`iceberg_target_apply.rs`) / `BRANCH_ID_COLUMN_NAME` (`mv_contract.rs`) — two consts with the same `"__branch_id__"` value, matching the existing pattern (apply-key column has parallel consts in both modules; verified `mv_contract.rs:385` vs `iceberg_target_apply.rs:3`). `BranchScope` (catalog.rs) used by `build_aggregate_state_merge` (3.1) and the row filter (3.2) and the lowering (3.6) — consistent. `build_aggregate_state_merge(aggregate, action_column, branch_id, ext)` signature is identical in Task 3.1 (definition) and Task 3.3 (call). `UnionBranchKind` / `UnionAllMvShape` consistent across Tasks 1.1–1.4, 1.7. `is_supported_fan_in_delta_union` / `is_supported_branch_union` defined in `action_propagation.rs` (`pub(crate)`) and reused in `action_column.rs` (Tasks 2.2/2.3/3.4). ✓

> Known soft spots the implementer should validate early (flagged, not hidden): (a) exact `sqlparser` `Query`/`SetExpr`/`SetOperator`/`SetQuantifier` field/variant names for the target version (Task 1.2 `wrap_setexpr_as_query`, `flatten_union_all`); (b) `LiteralValue` integer variant + width for `__branch_id__` (Task 3.3); (c) `SqlType` 32-bit int variant name (Task 1.6); (d) whether the union-branch row_id/arity interaction in the A-family normalize needs the same treatment join-delta already uses (Task 2.1 mirrors join-delta exactly, so it inherits the working behavior). Each is a small, local confirmation against the cited reference code.

---
