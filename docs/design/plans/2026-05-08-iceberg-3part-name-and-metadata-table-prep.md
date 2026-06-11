# Iceberg 3-part Name & Metadata-Table Query-Prep Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two long-failing iceberg SQL test cases (`iceberg_in_list_predicate`, `iceberg_metadata_snapshots`) by teaching the standalone-server query-prep stage to walk projection/WHERE/HAVING subqueries and to recognize the parser-rewritten 4-part metadata-table form `cat.db.tbl.__nr_meta_*__`.

**Architecture:** Surgical edits to `src/sql/parser/query_refs.rs` (extractor + stripper helpers and an `Expr` recursion mirroring `extract_table_names_from_expr`) and `src/engine/query_prep.rs::query_table_names` (merge 1-part and 3-part extractor outputs). Reuse `split_metadata_suffix` from `src/sql/analyzer/iceberg_metadata.rs`. No analyzer or catalog-dispatcher changes.

**Tech Stack:** Rust 1.x, sqlparser-rs, NovaRocks standalone server, `docker/iceberg-rest/` fixture (Iceberg REST + MinIO + Spark), `tests/sql-test-runner`.

**Working directory for all commands:** `/Users/harbor/worktree/NovaRocks/fix-iceberg-3part-name-meta-table`

**Spec:** [`docs/design/specs/2026-05-08-iceberg-3part-name-and-metadata-table-prep-design.md`](../specs/2026-05-08-iceberg-3part-name-and-metadata-table-prep-design.md)

---

## File map

| File | Change |
|---|---|
| `src/sql/parser/mod.rs` | Re-export `parse_normalized_sql_raw` for tests if not already accessible (verify). |
| `src/sql/parser/query_refs.rs` | Modify extractor + stripper for metadata-suffix awareness, expression recursion, and CTE walking. Add unit tests. |
| `src/engine/query_prep.rs` | Modify `query_table_names` to merge 1-part and 3-part extractor outputs. |
| `docs/design/plans/2026-05-08-iceberg-3part-name-and-metadata-table-prep.md` | This plan. |

No new files. All edits are inside existing modules.

---

## Task 1: 3-part extractor recognizes 4-part `__nr_meta_*__` factors

**Files:**
- Modify: `src/sql/parser/query_refs.rs:148-173` (`extract_three_part_refs_from_factor`)
- Modify: `src/sql/parser/query_refs.rs:1` (top-of-file imports)
- Test: `src/sql/parser/query_refs.rs::tests`

- [ ] **Step 1.1: Write the failing tests**

Add these tests to the `tests` module at the bottom of `src/sql/parser/query_refs.rs`. Place them after the existing tests, before the closing `}` of `mod tests`.

```rust
#[test]
fn extracts_three_part_refs_for_4part_metadata_table_factor() {
    let query = parse_query(
        "SELECT * FROM ice.db.t.__nr_meta_snapshots__"
    );

    assert_eq!(
        query_refs::extract_three_part_table_refs(&query),
        vec![("ice".to_string(), "db".to_string(), "t".to_string())],
    );
}

#[test]
fn ignores_3part_metadata_table_factor_in_three_part_extractor() {
    // base.len() == 2 — not a fully-qualified 3-part ref, must not be emitted.
    let query = parse_query(
        "SELECT * FROM db.t.__nr_meta_history__"
    );

    assert_eq!(
        query_refs::extract_three_part_table_refs(&query),
        Vec::<(String, String, String)>::new(),
    );
}
```

- [ ] **Step 1.2: Run tests to verify they fail**

```
cargo test -p novarocks --lib sql::parser::query_refs::tests::extracts_three_part_refs_for_4part_metadata_table_factor sql::parser::query_refs::tests::ignores_3part_metadata_table_factor_in_three_part_extractor 2>&1 | tail -30
```

Expected: both tests FAIL with `assertion `left == right` failed`. The first finds `[]` instead of `[(ice, db, t)]`; the second incidentally also passes today (no metadata recognition at all means base[0..3] is read literally for 4-part). Confirm the failure message says the 4-part case returns empty.

- [ ] **Step 1.3: Modify the import block**

Add the metadata-suffix import. At the top of `src/sql/parser/query_refs.rs`, add:

```rust
use crate::sql::analyzer::iceberg_metadata::split_metadata_suffix;
```

If a `use` block does not yet exist (the file currently starts with the doc comment then `pub(crate) fn`), insert the `use` immediately after the top doc comment.

- [ ] **Step 1.4: Replace `extract_three_part_refs_from_factor` body**

Locate `fn extract_three_part_refs_from_factor` at `src/sql/parser/query_refs.rs:148`. Replace its body with the metadata-aware version:

```rust
fn extract_three_part_refs_from_factor(
    factor: &sqlparser::ast::TableFactor,
    refs: &mut Vec<(String, String, String)>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();
            // Strip a trailing `__nr_meta_*__` suffix so a 4-part metadata-table
            // reference (cat.db.tbl.__nr_meta_*__) is treated as a fully-qualified
            // 3-part base reference for registration.
            let (base_parts, _) = split_metadata_suffix(&parts);
            if base_parts.len() == 3 {
                refs.push((
                    base_parts[0].clone(),
                    base_parts[1].clone(),
                    base_parts[2].clone(),
                ));
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            extract_three_part_refs_from_set_expr(subquery.body.as_ref(), refs);
        }
        _ => {}
    }
}
```

- [ ] **Step 1.5: Run tests to verify they pass**

```
cargo test -p novarocks --lib sql::parser::query_refs::tests 2>&1 | tail -30
```

Expected: all `query_refs::tests` pass, including the two new ones and the existing `extracts_three_part_table_refs_from_joins_and_subqueries`, `strips_catalog_from_three_part_table_refs`, `extracts_table_names_from_ctes_and_having_subqueries`.

- [ ] **Step 1.6: Commit**

```bash
git add src/sql/parser/query_refs.rs
git commit -m "fix(iceberg): recognize 4-part metadata-table factor in 3-part extractor

extract_three_part_refs_from_factor now strips a trailing
__nr_meta_*__ suffix via split_metadata_suffix so a parser-rewritten
cat.db.tbl.__nr_meta_*__ form is registered as the fully-qualified
3-part base table reference."
```

---

## Task 2: 3-part extractor recurses into projection / WHERE / HAVING / Function args / CTE bodies

**Files:**
- Modify: `src/sql/parser/query_refs.rs:114-122` (`extract_three_part_table_refs`)
- Modify: `src/sql/parser/query_refs.rs:124-146` (`extract_three_part_refs_from_set_expr`)
- Modify: `src/sql/parser/query_refs.rs` (add new helper `extract_three_part_refs_from_expr`)
- Test: `src/sql/parser/query_refs.rs::tests`

- [ ] **Step 2.1: Write the failing tests**

Add these tests at the end of the `tests` module:

```rust
#[test]
fn extracts_three_part_refs_from_projection_subquery() {
    // Mirrors the iceberg_in_list_predicate failure pattern: outer SELECT has
    // no FROM; the 3-part reference lives inside a COALESCE(SELECT ...) item.
    let query = parse_query(
        "SELECT COALESCE((SELECT count(*) FROM c1.db1.t1), 0) AS a"
    );

    assert_eq!(
        query_refs::extract_three_part_table_refs(&query),
        vec![("c1".to_string(), "db1".to_string(), "t1".to_string())],
    );
}

#[test]
fn extracts_three_part_refs_from_where_in_subquery() {
    let query = parse_query(
        "SELECT 1 FROM dual WHERE x IN (SELECT y FROM c2.db2.t2)"
    );

    assert_eq!(
        query_refs::extract_three_part_table_refs(&query),
        vec![("c2".to_string(), "db2".to_string(), "t2".to_string())],
    );
}

#[test]
fn extracts_three_part_refs_from_exists_subquery() {
    let query = parse_query(
        "SELECT 1 FROM dual WHERE EXISTS (SELECT * FROM c3.db3.t3)"
    );

    assert_eq!(
        query_refs::extract_three_part_table_refs(&query),
        vec![("c3".to_string(), "db3".to_string(), "t3".to_string())],
    );
}

#[test]
fn extracts_three_part_refs_from_having_subquery() {
    let query = parse_query(
        "SELECT k, count(*) FROM dual GROUP BY k \
         HAVING count(*) > (SELECT avg(v) FROM c4.db4.t4)"
    );

    assert_eq!(
        query_refs::extract_three_part_table_refs(&query),
        vec![("c4".to_string(), "db4".to_string(), "t4".to_string())],
    );
}

#[test]
fn extracts_three_part_refs_from_cte_body() {
    let query = parse_query(
        "WITH x AS (SELECT * FROM c5.db5.t5) SELECT * FROM x"
    );

    assert_eq!(
        query_refs::extract_three_part_table_refs(&query),
        vec![("c5".to_string(), "db5".to_string(), "t5".to_string())],
    );
}
```

Note: the test SQL uses `FROM dual` so the outer SELECT has a valid FROM (sqlparser-rs requires it for some constructs). `dual` is a 1-part name and won't be returned by the 3-part extractor.

- [ ] **Step 2.2: Run tests to verify they fail**

```
cargo test -p novarocks --lib sql::parser::query_refs::tests 2>&1 | tail -50
```

Expected: the five new tests FAIL (they all return empty list because the existing `extract_three_part_refs_from_set_expr` does not walk projections, WHERE, HAVING, or CTEs).

- [ ] **Step 2.3: Add `extract_three_part_refs_from_expr` helper**

Insert the new helper immediately after `extract_three_part_refs_from_factor` (before the test module):

```rust
fn extract_three_part_refs_from_expr(
    expr: &sqlparser::ast::Expr,
    refs: &mut Vec<(String, String, String)>,
) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(query) | Expr::Exists { subquery: query, .. } => {
            extract_three_part_refs_from_set_expr(query.body.as_ref(), refs);
        }
        Expr::InSubquery { subquery, expr, .. } => {
            extract_three_part_refs_from_set_expr(subquery.body.as_ref(), refs);
            extract_three_part_refs_from_expr(expr, refs);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_three_part_refs_from_expr(left, refs);
            extract_three_part_refs_from_expr(right, refs);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            extract_three_part_refs_from_expr(expr, refs);
        }
        Expr::Between { expr, low, high, .. } => {
            extract_three_part_refs_from_expr(expr, refs);
            extract_three_part_refs_from_expr(low, refs);
            extract_three_part_refs_from_expr(high, refs);
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &function.args {
                for arg in &arg_list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    )
                    | sqlparser::ast::FunctionArg::Named {
                        arg: sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ..
                    } = arg
                    {
                        extract_three_part_refs_from_expr(inner, refs);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                extract_three_part_refs_from_expr(op, refs);
            }
            for case_when in conditions {
                extract_three_part_refs_from_expr(&case_when.condition, refs);
                extract_three_part_refs_from_expr(&case_when.result, refs);
            }
            if let Some(else_expr) = else_result {
                extract_three_part_refs_from_expr(else_expr, refs);
            }
        }
        Expr::Cast { expr, .. } => {
            extract_three_part_refs_from_expr(expr, refs);
        }
        _ => {}
    }
}
```

If your sqlparser-rs version exposes `Expr::Case` with a different shape (e.g., `Vec<Expr>` for conditions/results instead of `Vec<CaseWhen>`), adapt the `Case` arm to compile. Do not change the other arms.

- [ ] **Step 2.4: Modify `extract_three_part_refs_from_set_expr` to walk projection / WHERE / HAVING**

Replace the function body with:

```rust
fn extract_three_part_refs_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    refs: &mut Vec<(String, String, String)>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                extract_three_part_refs_from_factor(&from.relation, refs);
                for join in &from.joins {
                    extract_three_part_refs_from_factor(&join.relation, refs);
                }
            }
            if let Some(selection) = &select.selection {
                extract_three_part_refs_from_expr(selection, refs);
            }
            if let Some(having) = &select.having {
                extract_three_part_refs_from_expr(having, refs);
            }
            for projection in &select.projection {
                match projection {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                        extract_three_part_refs_from_expr(expr, refs);
                    }
                    _ => {}
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_three_part_refs_from_set_expr(left, refs);
            extract_three_part_refs_from_set_expr(right, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            extract_three_part_refs_from_set_expr(query.body.as_ref(), refs);
        }
        _ => {}
    }
}
```

- [ ] **Step 2.5: Modify `extract_three_part_table_refs` to walk CTE bodies**

Replace the function body with:

```rust
pub(crate) fn extract_three_part_table_refs(
    query: &sqlparser::ast::Query,
) -> Vec<(String, String, String)> {
    let mut refs = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_three_part_refs_from_set_expr(cte.query.body.as_ref(), &mut refs);
        }
    }
    extract_three_part_refs_from_set_expr(query.body.as_ref(), &mut refs);
    refs.sort();
    refs.dedup();
    refs
}
```

- [ ] **Step 2.6: Run tests to verify all pass**

```
cargo test -p novarocks --lib sql::parser::query_refs::tests 2>&1 | tail -50
```

Expected: all tests pass — original tests, the two from Task 1, and the five from Task 2.

If the `Expr::Case` arm fails to compile due to an sqlparser-rs version mismatch, drop that arm temporarily (the failing cases do not exercise CASE) and capture the issue inline; CASE is not required for case 1/case 2 fixes.

- [ ] **Step 2.7: Commit**

```bash
git add src/sql/parser/query_refs.rs
git commit -m "fix(iceberg): walk projection/WHERE/HAVING/Function/CTE in 3-part extractor

extract_three_part_table_refs now mirrors extract_table_names_from_query's
recursion: it walks SELECT projection items (including Function args),
WHERE, HAVING, and CTE bodies via a new extract_three_part_refs_from_expr
helper. Without this, 3-part references nested inside COALESCE(SELECT ...)
projection items or inside subquery predicates were invisible to the
query-prep stage."
```

---

## Task 3: Stripper recognizes 4-part metadata + walks expressions / CTEs (mirror of Tasks 1+2)

**Files:**
- Modify: `src/sql/parser/query_refs.rs:178-180` (`strip_catalog_from_three_part_names`)
- Modify: `src/sql/parser/query_refs.rs:182-201` (`strip_catalog_in_set_expr`)
- Modify: `src/sql/parser/query_refs.rs:203-215` (`strip_catalog_in_factor`)
- Modify: `src/sql/parser/query_refs.rs` (add new helper `strip_catalog_in_expr`)
- Test: `src/sql/parser/query_refs.rs::tests`

- [ ] **Step 3.1: Write the failing tests**

Add to the `tests` module:

```rust
#[test]
fn strips_catalog_from_4part_metadata_table_factor() {
    let mut query = parse_query(
        "SELECT * FROM ice.db.t.__nr_meta_snapshots__"
    );

    query_refs::strip_catalog_from_three_part_names(&mut query);

    assert_eq!(
        query.to_string(),
        "SELECT * FROM db.t.__nr_meta_snapshots__"
    );
}

#[test]
fn leaves_3part_metadata_table_factor_alone() {
    // base.len() == 2 — already not catalog-qualified; must not be touched.
    let mut query = parse_query(
        "SELECT * FROM db.t.__nr_meta_history__"
    );

    query_refs::strip_catalog_from_three_part_names(&mut query);

    assert_eq!(
        query.to_string(),
        "SELECT * FROM db.t.__nr_meta_history__"
    );
}

#[test]
fn strips_catalog_inside_projection_subquery() {
    let mut query = parse_query(
        "SELECT COALESCE((SELECT count(*) FROM c1.db1.t1), 0) AS a"
    );

    query_refs::strip_catalog_from_three_part_names(&mut query);

    assert_eq!(
        query.to_string(),
        "SELECT COALESCE((SELECT count(*) FROM db1.t1), 0) AS a"
    );
}

#[test]
fn strips_catalog_inside_where_in_subquery() {
    let mut query = parse_query(
        "SELECT 1 FROM dual WHERE x IN (SELECT y FROM c2.db2.t2)"
    );

    query_refs::strip_catalog_from_three_part_names(&mut query);

    assert_eq!(
        query.to_string(),
        "SELECT 1 FROM dual WHERE x IN (SELECT y FROM db2.t2)"
    );
}

#[test]
fn strips_catalog_inside_cte_body() {
    let mut query = parse_query(
        "WITH x AS (SELECT * FROM c5.db5.t5) SELECT * FROM x"
    );

    query_refs::strip_catalog_from_three_part_names(&mut query);

    assert_eq!(
        query.to_string(),
        "WITH x AS (SELECT * FROM db5.t5) SELECT * FROM x"
    );
}
```

- [ ] **Step 3.2: Run tests to verify they fail**

```
cargo test -p novarocks --lib sql::parser::query_refs::tests 2>&1 | tail -50
```

Expected: the five new strip tests FAIL.

- [ ] **Step 3.3: Modify `strip_catalog_in_factor`**

Replace its body with the metadata-aware version:

```rust
fn strip_catalog_in_factor(factor: &mut sqlparser::ast::TableFactor) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            // Count parts logically (excluding any trailing __nr_meta_*__).
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();
            let (base_parts, _) = split_metadata_suffix(&parts);
            if base_parts.len() == 3 {
                // Drop the leading catalog identifier; keep the remaining
                // 2-part base name plus the (preserved) metadata suffix part.
                name.0.remove(0);
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            strip_catalog_in_set_expr(subquery.body.as_mut());
        }
        _ => {}
    }
}
```

- [ ] **Step 3.4: Add `strip_catalog_in_expr` helper**

Insert immediately after `strip_catalog_in_factor`:

```rust
fn strip_catalog_in_expr(expr: &mut sqlparser::ast::Expr) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(query) | Expr::Exists { subquery: query, .. } => {
            strip_catalog_in_set_expr(query.body.as_mut());
        }
        Expr::InSubquery { subquery, expr, .. } => {
            strip_catalog_in_set_expr(subquery.body.as_mut());
            strip_catalog_in_expr(expr);
        }
        Expr::BinaryOp { left, right, .. } => {
            strip_catalog_in_expr(left);
            strip_catalog_in_expr(right);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            strip_catalog_in_expr(expr);
        }
        Expr::Between { expr, low, high, .. } => {
            strip_catalog_in_expr(expr);
            strip_catalog_in_expr(low);
            strip_catalog_in_expr(high);
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut function.args {
                for arg in &mut arg_list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    )
                    | sqlparser::ast::FunctionArg::Named {
                        arg: sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ..
                    } = arg
                    {
                        strip_catalog_in_expr(inner);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                strip_catalog_in_expr(op);
            }
            for case_when in conditions {
                strip_catalog_in_expr(&mut case_when.condition);
                strip_catalog_in_expr(&mut case_when.result);
            }
            if let Some(else_expr) = else_result {
                strip_catalog_in_expr(else_expr);
            }
        }
        Expr::Cast { expr, .. } => {
            strip_catalog_in_expr(expr);
        }
        _ => {}
    }
}
```

- [ ] **Step 3.5: Modify `strip_catalog_in_set_expr` to walk projection / WHERE / HAVING**

Replace body with:

```rust
fn strip_catalog_in_set_expr(expr: &mut sqlparser::ast::SetExpr) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &mut select.from {
                strip_catalog_in_factor(&mut from.relation);
                for join in &mut from.joins {
                    strip_catalog_in_factor(&mut join.relation);
                }
            }
            if let Some(selection) = &mut select.selection {
                strip_catalog_in_expr(selection);
            }
            if let Some(having) = &mut select.having {
                strip_catalog_in_expr(having);
            }
            for projection in &mut select.projection {
                match projection {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                        strip_catalog_in_expr(expr);
                    }
                    _ => {}
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            strip_catalog_in_set_expr(left.as_mut());
            strip_catalog_in_set_expr(right.as_mut());
        }
        sqlparser::ast::SetExpr::Query(query) => {
            strip_catalog_in_set_expr(query.body.as_mut());
        }
        _ => {}
    }
}
```

- [ ] **Step 3.6: Modify `strip_catalog_from_three_part_names` to walk CTEs**

Replace body with:

```rust
pub(crate) fn strip_catalog_from_three_part_names(query: &mut sqlparser::ast::Query) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            strip_catalog_in_set_expr(cte.query.body.as_mut());
        }
    }
    strip_catalog_in_set_expr(query.body.as_mut());
}
```

- [ ] **Step 3.7: Run tests to verify they pass**

```
cargo test -p novarocks --lib sql::parser::query_refs::tests 2>&1 | tail -50
```

Expected: all `query_refs::tests` pass.

- [ ] **Step 3.8: Commit**

```bash
git add src/sql/parser/query_refs.rs
git commit -m "fix(iceberg): symmetric strip walks expressions/CTE and 4-part metadata

strip_catalog_from_three_part_names now mirrors the extractor: it walks
projection items (including Function args), WHERE, HAVING, and CTE bodies
via a new strip_catalog_in_expr helper, and strip_catalog_in_factor uses
split_metadata_suffix so a 4-part cat.db.tbl.__nr_meta_*__ form becomes a
3-part db.tbl.__nr_meta_*__ form (catalog stripped, metadata suffix
preserved)."
```

---

## Task 4: 1-part extractor skips `__nr_meta_*__` last parts

**Files:**
- Modify: `src/sql/parser/query_refs.rs:48-69` (`extract_table_names_from_table_factor`)
- Test: `src/sql/parser/query_refs.rs::tests`

- [ ] **Step 4.1: Write the failing test**

Add to the `tests` module:

```rust
#[test]
fn one_part_extractor_skips_nr_meta_last_part() {
    // 4-part metadata factor must not surface as a 1-part name (the last part
    // is a synthetic suffix, not a real table name). The 3-part extractor
    // handles the real registration.
    let query = parse_query(
        "SELECT * FROM ice.db.t.__nr_meta_snapshots__"
    );

    assert_eq!(
        query_refs::extract_table_names_from_query(&query),
        Vec::<String>::new(),
    );
}
```

- [ ] **Step 4.2: Run test to verify it fails**

```
cargo test -p novarocks --lib sql::parser::query_refs::tests::one_part_extractor_skips_nr_meta_last_part 2>&1 | tail -20
```

Expected: FAIL — the existing extractor returns `["__nr_meta_snapshots__"]`.

- [ ] **Step 4.3: Modify `extract_table_names_from_table_factor`**

Replace its body with:

```rust
fn extract_table_names_from_table_factor(
    factor: &sqlparser::ast::TableFactor,
    names: &mut Vec<String>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();
            // Skip synthetic iceberg-metadata factors; they are handled by
            // extract_three_part_refs (which strips the trailing
            // __nr_meta_*__ suffix and routes to the iceberg backend).
            let (_, metadata_suffix) = split_metadata_suffix(&parts);
            if metadata_suffix.is_some() {
                return;
            }
            if let Some(last) = parts.last() {
                names.push(last.clone());
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            extract_table_names_from_set_expr(subquery.body.as_ref(), names);
        }
        _ => {}
    }
}
```

- [ ] **Step 4.4: Run tests to verify they pass**

```
cargo test -p novarocks --lib sql::parser::query_refs::tests 2>&1 | tail -30
```

Expected: all tests pass.

- [ ] **Step 4.5: Commit**

```bash
git add src/sql/parser/query_refs.rs
git commit -m "fix(iceberg): 1-part extractor skips __nr_meta_*__ table factors

Synthetic iceberg-metadata factors (rewritten from <tbl>\$<metatype>) must
not surface as 1-part real table names; they are owned by the 3-part
extractor which strips the suffix and registers the underlying base
table."
```

---

## Task 5: `query_table_names` merges 1-part and 3-part extractor outputs

**Files:**
- Modify: `src/engine/query_prep.rs:468-485` (`query_table_names`)

- [ ] **Step 5.1: Read the existing function**

```
grep -n "fn query_table_names" /Users/harbor/worktree/NovaRocks/fix-iceberg-3part-name-meta-table/src/engine/query_prep.rs
```

Confirm the body matches (current `origin/main`):

```rust
fn query_table_names(
    current_catalog: Option<&str>,
    query: &sqlparser::ast::Query,
) -> Vec<ObjectName> {
    if current_catalog.is_some() {
        extract_table_names_from_query(query)
            .into_iter()
            .map(|table| ObjectName { parts: vec![table] })
            .collect()
    } else {
        extract_three_part_table_refs(query)
            .into_iter()
            .map(|(catalog, namespace, table)| ObjectName {
                parts: vec![catalog, namespace, table],
            })
            .collect()
    }
}
```

If the body has been changed by an earlier task, halt and reconcile before continuing.

- [ ] **Step 5.2: Replace the function body**

```rust
fn query_table_names(
    current_catalog: Option<&str>,
    query: &sqlparser::ast::Query,
) -> Vec<ObjectName> {
    // Always collect fully-qualified 3-part references (including 4-part
    // __nr_meta_*__ forms reduced to 3-part). They register against the
    // catalog encoded in the name regardless of session catalog.
    let mut names: Vec<ObjectName> = extract_three_part_table_refs(query)
        .into_iter()
        .map(|(catalog, namespace, table)| ObjectName {
            parts: vec![catalog, namespace, table],
        })
        .collect();

    // When the session has a current catalog, also collect 1-part names so
    // that unqualified references in the query register through the session
    // catalog + current database.
    if current_catalog.is_some() {
        for table in extract_table_names_from_query(query) {
            names.push(ObjectName { parts: vec![table] });
        }
    }

    // Stable de-duplication on (parts) so the downstream registration loop
    // does not redundantly hit the iceberg backend for the same target.
    names.sort_by(|a, b| a.parts.cmp(&b.parts));
    names.dedup_by(|a, b| a.parts == b.parts);
    names
}
```

- [ ] **Step 5.3: Build to verify the change compiles**

```
cargo build -p novarocks 2>&1 | tail -20
```

Expected: build succeeds with no warnings introduced by this change.

- [ ] **Step 5.4: Run the full library test suite for `engine` and `sql::parser`**

```
cargo test -p novarocks --lib engine sql::parser 2>&1 | tail -50
```

Expected: tests pass (unit-level coverage of `query_table_names` is exercised indirectly via integration tests; the SQL regression in Task 6 is the primary verification gate).

- [ ] **Step 5.5: Commit**

```bash
git add src/engine/query_prep.rs
git commit -m "fix(iceberg): query_table_names always runs 3-part extractor and merges

Previously query_table_names selected one extractor or the other based on
current_catalog; under current_catalog=Some(cat) a 4-part metadata factor
was reduced to its synthetic suffix as a 1-part name and failed to load.
Always collecting fully-qualified 3-part references (including 4-part
__nr_meta_*__ forms) ensures the underlying base table registers
correctly regardless of the session catalog."
```

---

## Task 5b (added after Task 6 verification revealed the gap): widen the 3-part strip path in `src/engine/mod.rs` to fire under `current_catalog=Some`

**Why this task exists:** Task 6's verification found that the `iceberg` suite runs with `-- @catalog=iceberg_cat_${suite_uuid0}` (`sql-tests/iceberg/init.sql:1`), so every query reaches the engine with `current_catalog=Some(iceberg_cat_*)`. Three call sites in `src/engine/mod.rs` gate the catalog-strip-and-rewrite branch on `current_catalog.is_none()`; under `is_some` mode the original 3-part / 4-part name reaches the analyzer, which cannot resolve `cat.db.tbl`. Tasks 1–5 set up registration correctly, but the analyzer still trips because the strip never fires.

The plan's spec accepted "other 3-part cases under `current_catalog=Some`" as out-of-scope, but the failing test cases are exactly under that mode, so the strip must now fire in both modes. Registration is already covered (Task 5 makes the 3-part extractor always run; the existing `is_some` branch above each strip-gate has already called `register_iceberg_tables_for_query`).

**Files:**
- Modify: `src/engine/mod.rs` — three call sites (Query branch, time-travel branch, EXPLAIN branch).

- [ ] **Step 5b.1: Modify the plain Query branch (around lines 724-758)**

Replace this block:

```rust
// When current_catalog is an Iceberg catalog, materialize
// referenced Iceberg tables into the local catalog first.
if current_catalog.is_some() {
    register_iceberg_tables_for_query(
        &self.inner,
        current_catalog,
        current_database,
        query,
    )?;
}

// Handle 3-part table names (catalog.database.table) when no
// current catalog context is set.  Clone the query, strip the
// catalog prefix so the analyzer sees 2-part names, and register
// the referenced Iceberg tables in the local catalog.
let three_parts = extract_three_part_table_refs(query);
if !three_parts.is_empty() && current_catalog.is_none() {
    register_iceberg_tables_for_query(&self.inner, None, current_database, query)?;
    let mut rewritten = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut rewritten);
    let catalog = self
        .inner
        .catalog
        .read()
        .expect("standalone catalog read lock");
    let result = execute_query(
        &rewritten,
        &catalog,
        current_database,
        self.inner.exchange_port,
        query_opts.clone(),
    )?;
    drop(catalog);
    return Ok(StatementResult::Query(result));
}
```

with:

```rust
// When current_catalog is an Iceberg catalog, materialize
// referenced Iceberg tables into the local catalog first.
if current_catalog.is_some() {
    register_iceberg_tables_for_query(
        &self.inner,
        current_catalog,
        current_database,
        query,
    )?;
}

// Handle fully-qualified 3-part table names (catalog.database.table).
// Strip the leading catalog so the analyzer sees a 2-part name; the
// registration above (or the explicit one below in current_catalog=None
// mode) has already materialized the iceberg base table into the local
// catalog. The strip also turns 4-part `cat.db.tbl.__nr_meta_*__`
// metadata references into the 3-part form the analyzer's metadata
// path expects.
let three_parts = extract_three_part_table_refs(query);
if !three_parts.is_empty() {
    if current_catalog.is_none() {
        register_iceberg_tables_for_query(&self.inner, None, current_database, query)?;
    }
    let mut rewritten = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut rewritten);
    let catalog = self
        .inner
        .catalog
        .read()
        .expect("standalone catalog read lock");
    let result = execute_query(
        &rewritten,
        &catalog,
        current_database,
        self.inner.exchange_port,
        query_opts.clone(),
    )?;
    drop(catalog);
    return Ok(StatementResult::Query(result));
}
```

- [ ] **Step 5b.2: Modify the time-travel branch (around lines 681-722)**

Find the existing block (inside `if has_time_travel_refs(query)`):

```rust
let three_parts = extract_three_part_table_refs(&rewritten);
if !three_parts.is_empty() && current_catalog.is_none() {
    register_iceberg_tables_for_query(
        &self.inner,
        None,
        current_database,
        &rewritten,
    )?;
    strip_catalog_from_three_part_names(&mut rewritten);
}
```

Replace with:

```rust
let three_parts = extract_three_part_table_refs(&rewritten);
if !three_parts.is_empty() {
    if current_catalog.is_none() {
        register_iceberg_tables_for_query(
            &self.inner,
            None,
            current_database,
            &rewritten,
        )?;
    }
    strip_catalog_from_three_part_names(&mut rewritten);
}
```

- [ ] **Step 5b.3: Modify the EXPLAIN branch (around lines 634-652)**

Find:

```rust
let mut rewritten_three_part_query;
let query = if current_catalog.is_none() {
    let three_parts = extract_three_part_table_refs(query);
    if !three_parts.is_empty() {
        register_iceberg_tables_for_query(
            &self.inner,
            None,
            current_database,
            query,
        )?;
        rewritten_three_part_query = query.clone();
        strip_catalog_from_three_part_names(&mut rewritten_three_part_query);
        &rewritten_three_part_query
    } else {
        query
    }
} else {
    query
};
```

Replace with:

```rust
let mut rewritten_three_part_query;
let three_parts = extract_three_part_table_refs(query);
let query = if !three_parts.is_empty() {
    if current_catalog.is_none() {
        register_iceberg_tables_for_query(
            &self.inner,
            None,
            current_database,
            query,
        )?;
    }
    rewritten_three_part_query = query.clone();
    strip_catalog_from_three_part_names(&mut rewritten_three_part_query);
    &rewritten_three_part_query
} else {
    query
};
```

- [ ] **Step 5b.4: Build and run lib tests**

```
cargo build 2>&1 | tail -10
cargo test -p novarocks --lib engine sql::parser 2>&1 | tail -50
```

Expected: build clean. Lib tests pass (modulo the pre-existing `iceberg_refresh_load_failure_removes_stale_local_catalog_entry` env-related failure that was present before this branch).

- [ ] **Step 5b.5: cargo fmt**

```
cargo fmt
cargo fmt --check
```

Clean. Fold any fmt changes into the same commit.

- [ ] **Step 5b.6: Commit**

```bash
git add src/engine/mod.rs
git commit -m "fix(iceberg): strip 3-part catalog prefix in current_catalog=Some mode too

The three call sites that catch 3-part table refs in src/engine/mod.rs
(Query branch, time-travel branch, EXPLAIN branch) previously gated the
strip-and-rewrite step on current_catalog.is_none(). Under is_some mode
the original 3-part name (or 4-part __nr_meta_*__ metadata form) reached
the analyzer, which only resolves 1- or 2-part names. Now strip in both
modes; registration in is_some mode was already handled by the
preceding register_iceberg_tables_for_query call."
```

---

## Task 6: SQL regression — verify both failing cases pass and no iceberg suite regression

This task runs the live `docker/iceberg-rest/` fixture, builds the binary, runs the standalone server in the background, and exercises the SQL test runner. It produces no code changes; the only deliverable is verification.

**Files:** none (verification only).

- [ ] **Step 6.1: Bring up (or confirm) the local fixture**

```
ls docker/iceberg-rest/runtime/current/ 2>/dev/null && echo "fixture-up" || docker/iceberg-rest/up.sh
```

Expected: either `fixture-up` followed by listing, or `up.sh` completes successfully and creates `docker/iceberg-rest/runtime/current/`.

- [ ] **Step 6.2: Source environment and confirm endpoints**

```
source docker/iceberg-rest/runtime/current/env.sh
echo "MYSQL_PORT=$NOVA_ENV_MYSQL_PORT REST_URI=$NOVAROCKS_ICEBERG_REST_URI"
```

Expected: both variables are non-empty.

- [ ] **Step 6.3: Build the binary (debug)**

```
cargo build 2>&1 | tail -10
```

Expected: `Compiling novarocks ... Finished` (debug profile), no errors.

- [ ] **Step 6.4: Start standalone-server in the background**

```
NO_PROXY=127.0.0.1,localhost \
  cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" \
  > /tmp/novarocks-server.log 2>&1 &
echo $! > /tmp/novarocks-server.pid
sleep 5
mysql --protocol=tcp -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -u root -e "SELECT 1" 2>&1 | tail -5
```

Expected: the `SELECT 1` query returns `1`. If the server is not yet listening, wait an additional ~5 seconds and retry.

- [ ] **Step 6.5: Run the two target cases in `verify` mode**

```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg \
  --only iceberg_in_list_predicate,iceberg_metadata_snapshots \
  --mode verify 2>&1 | tail -30
```

Expected: both cases report `OK` (or the runner's equivalent success indicator). If a case fails, capture the error, stop the standalone-server (Step 6.8), and revisit the implementation tasks.

- [ ] **Step 6.6: Run the full `iceberg` suite in `verify` mode**

```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg \
  --mode verify 2>&1 | tail -50
```

Expected: total pass count increases by 2 versus pre-fix; no previously-passing case regresses. Capture the pass/fail summary line.

- [ ] **Step 6.7: Run the `iceberg-compatibility` suite (cross-engine)**

```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility \
  --mode verify 2>&1 | tail -30
```

Expected: no regression versus pre-fix.

- [ ] **Step 6.8: Stop standalone-server**

```
kill "$(cat /tmp/novarocks-server.pid)" 2>/dev/null
rm -f /tmp/novarocks-server.pid
```

- [ ] **Step 6.9: Run `cargo fmt` and `cargo clippy`**

```
cargo fmt
cargo clippy -- -D warnings 2>&1 | tail -30
```

Expected: `cargo fmt` produces no diff; `cargo clippy` reports no warnings introduced by this change.

- [ ] **Step 6.10: If `cargo fmt` introduced changes, commit them**

```
git diff --quiet || (git add -A && git commit -m "style: cargo fmt after iceberg query-prep fix")
```

If no diff, skip.

- [ ] **Step 6.11: Final summary**

After Step 6.6 / 6.7 finish, report:

- Number of `iceberg` suite cases passing pre-fix vs post-fix.
- Whether `iceberg_in_list_predicate` and `iceberg_metadata_snapshots` flipped from FAIL to PASS.
- Whether `iceberg-compatibility` suite remained green.
- Any `clippy` or `fmt` outcomes.

---

## Self-review

**Spec coverage:**

- Recurse projection / WHERE / HAVING / Function / CTE in 3-part extractor → Task 2.
- 4-part metadata factor in 3-part extractor → Task 1.
- Symmetric stripper changes → Task 3.
- 1-part extractor skips `__nr_meta_*__` → Task 4.
- `query_table_names` merges both extractors → Task 5.
- Unit tests for all of the above → Tasks 1–4 (each `Step N.1` adds tests).
- SQL regression: both target cases + full iceberg + iceberg-compatibility → Task 6.
- `cargo fmt` / `cargo clippy` hygiene → Task 6 (Step 6.9).
- No backwards-compat shim, no analyzer changes, no catalog-dispatcher changes → enforced by file map.

**Placeholder scan:** none ("TBD"/"TODO" not present; every step has concrete code/commands).

**Type consistency:**

- `split_metadata_suffix(parts: &[String])` signature is consistent across Tasks 1, 3, and 4.
- The new helpers `extract_three_part_refs_from_expr(&Expr, &mut Vec<(...)>)` and `strip_catalog_in_expr(&mut Expr)` are referenced only in their respective task and in the matching set-expr / factor functions; signatures match.
- `query_table_names` retains its public signature (`Option<&str>, &Query) -> Vec<ObjectName>`); only the body changes.

If `Expr::Case` or function-arg shape mismatch the local sqlparser-rs version, Tasks 2 / 3 instruct dropping that arm — case 1 / case 2 do not depend on CASE.
