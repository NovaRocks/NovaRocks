# Iceberg Target MV Base Evolution Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden Iceberg target MV refresh so base schema evolution and base partition evolution are correctly supported for single aggregate and two-table join aggregate IMV.

**Architecture:** Extend the persisted schema-contract decision model from single-base name rebinds to per-base rebind columns, then move SQL rebind into a focused AST helper that can rewrite aggregate and join expressions safely. Join-family refresh planning and execution will accept referenced renames by rewriting the effective MV definition before shape-specific planning, while base partition evolution remains transparent to MV contracts and is proven with SQL regression coverage.

**Tech Stack:** Rust, sqlparser 0.61 AST, Iceberg v3 row-lineage, NovaRocks MV schema contracts, NovaRocks standalone SQL runner, `sql-tests/iceberg-ivm`, Spark SQL through `docker/iceberg-rest/spark-sql.sh`.

---

## File Structure

- Modify `src/engine/mv/schema_contract.rs`
  - Add `RebindColumn`.
  - Add `SchemaEvolutionError::BaseFieldNullabilityChanged`.
  - Return rebind columns with `base_table_fqn`.
  - Check single-base referenced nullability drift.
  - Add unit coverage for nullability drift and single-base rebind payload.
- Create `src/engine/mv/rebind.rs`
  - Move and extend `rewrite_select_sql_for_rebind`.
  - Rewrite projection, WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY, function arguments, and nested expressions.
  - Resolve join qualifiers through table aliases and persisted base FQNs.
  - Reject ambiguous unqualified multi-base rebinds.
- Modify `src/engine/mv/mod.rs`
  - Register the new `rebind` module.
- Modify `src/engine/mv/iceberg_refresh.rs`
  - Import `schema_contract::RebindColumn` and `mv::rebind::rewrite_select_sql_for_rebind`.
  - Change join schema validation to return a rebind decision.
  - Apply effective rewritten definitions in join projection and join aggregate refresh execution.
  - Keep refresh planning permissive for compatible rebind decisions.
- Add SQL tests and result files under `sql-tests/iceberg-ivm/sql` and `sql-tests/iceberg-ivm/result`
  - `iceberg_ivm_aggregate_a11_base_rename_group_key`
  - `iceberg_ivm_aggregate_a11_base_rename_measure`
  - `iceberg_ivm_join_aggregate_a11_base_rename_join_key`
  - `iceberg_ivm_join_aggregate_a11_base_rename_group_key`
  - `iceberg_ivm_aggregate_a11_base_nullability_change_referenced`
  - `iceberg_ivm_aggregate_base_partition_evolution`
  - `iceberg_ivm_join_aggregate_base_partition_evolution`
- Modify docs already created for this work
  - `docs/design/specs/2026-05-18-iceberg-target-mv-base-evolution-hardening-design.md`
  - `docs/design/plans/2026-05-18-iceberg-target-mv-base-evolution-hardening.md`

## Task 1: Baseline And Branch Sanity

**Files:**
- Read: `docs/design/specs/2026-05-18-iceberg-target-mv-base-evolution-hardening-design.md`
- Read: `src/engine/mv/schema_contract.rs`
- Read: `src/engine/mv/iceberg_refresh.rs`
- Read: `sql-tests/iceberg-ivm/README.md`

- [ ] **Step 1: Confirm branch base and dirty state**

Run:

```bash
git status --short --branch
git log --oneline -1 --decorate
```

Expected:

```text
## codex/iceberg-mv-base-evolution-hardening...origin/main
?? docs/design/specs/2026-05-18-iceberg-target-mv-base-evolution-hardening-design.md
?? docs/design/plans/2026-05-18-iceberg-target-mv-base-evolution-hardening.md
94e4b325 ... [codex] Support Iceberg target join aggregate IMV (#143)
```

- [ ] **Step 2: Build the library before changing code**

Run:

```bash
cargo test --lib engine::mv::schema_contract::tests::schema_evolution_error_messages_are_action_oriented -- --exact
```

Expected: PASS. This proves the current Rust test harness compiles before the feature changes.

- [ ] **Step 3: Verify the Iceberg REST test environment entry**

Run:

```bash
test -f docker/iceberg-rest/runtime/current/env.sh && source docker/iceberg-rest/runtime/current/env.sh && printf '%s\n' "$NOVAROCKS_SQL_TEST_CONFIG"
```

Expected: prints the generated SQL-test config path. If the file is absent, run:

```bash
docker/iceberg-rest/up.sh --prepare-only
```

Then re-run the `test -f ...` command.

## Task 2: Contract Rebind Payload And Nullability Guard

**Files:**
- Modify: `src/engine/mv/schema_contract.rs`

- [ ] **Step 1: Add failing tests for single-base nullability drift and rebind payload**

Add these tests inside `#[cfg(test)] mod tests` in `src/engine/mv/schema_contract.rs`, near `supplied_base_schema_drives_base_rebind_decision`:

```rust
#[test]
fn supplied_base_schema_rejects_referenced_nullability_drift() {
    let base_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
    let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
    let base_schema = iceberg::spec::Schema::builder()
        .with_schema_id(7)
        .with_fields(vec![Arc::new(iceberg::spec::NestedField::optional(
            1,
            "id",
            base_type.clone(),
        ))])
        .build()
        .expect("base schema");
    let target_schema = iceberg::spec::Schema::builder()
        .with_schema_id(11)
        .with_fields(vec![
            Arc::new(iceberg::spec::NestedField::required(
                1,
                "id",
                target_type.clone(),
            )),
            Arc::new(iceberg::spec::NestedField::required(
                2,
                HIDDEN_APPLY_KEY_COLUMN_NAME,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )),
        ])
        .build()
        .expect("target schema");
    let contract = minimal_base_row_id_contract();

    let decision =
        validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

    match decision {
        ContractDecision::Incompatible(SchemaEvolutionError::BaseFieldNullabilityChanged {
            field_id,
            name_at_create,
            from_required,
            to_required,
        }) => {
            assert_eq!(field_id, 1);
            assert_eq!(name_at_create, "id");
            assert!(from_required);
            assert!(!to_required);
        }
        other => panic!("unexpected decision: {other:?}"),
    }
}

#[test]
fn supplied_base_schema_rebind_payload_includes_base_fqn() {
    let base_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
    let target_type = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int);
    let base_schema = iceberg::spec::Schema::builder()
        .with_schema_id(7)
        .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
            1,
            "renamed_id",
            base_type.clone(),
        ))])
        .build()
        .expect("base schema");
    let target_schema = iceberg::spec::Schema::builder()
        .with_schema_id(11)
        .with_fields(vec![
            Arc::new(iceberg::spec::NestedField::required(
                1,
                "id",
                target_type.clone(),
            )),
            Arc::new(iceberg::spec::NestedField::required(
                2,
                HIDDEN_APPLY_KEY_COLUMN_NAME,
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )),
        ])
        .build()
        .expect("target schema");
    let contract = minimal_base_row_id_contract();

    let decision =
        validate_schema_contract_after_identity(&contract, &base_schema, &target_schema);

    assert_eq!(
        decision,
        ContractDecision::CompatibleSafeWithRebind {
            rebound_columns: vec![RebindColumn {
                base_table_fqn: "ice.db.orders".to_string(),
                field_id: 1,
                name_at_create: "id".to_string(),
                current_name: "renamed_id".to_string(),
            }],
        }
    );
}
```

- [ ] **Step 2: Run the focused tests and confirm failure**

Run:

```bash
cargo test --lib engine::mv::schema_contract::tests::supplied_base_schema_ -- --nocapture
```

Expected: fails because `RebindColumn` and `BaseFieldNullabilityChanged` do not exist, and existing `rebound_columns` still uses tuple payloads.

- [ ] **Step 3: Add `RebindColumn` and update `ContractDecision`**

In `src/engine/mv/schema_contract.rs`, add this near `ContractDecision`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RebindColumn {
    pub(crate) base_table_fqn: String,
    pub(crate) field_id: i32,
    pub(crate) name_at_create: String,
    pub(crate) current_name: String,
}
```

Change the decision payload:

```rust
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ContractDecision {
    CompatibleSafe,
    CompatibleSafeWithRebind {
        rebound_columns: Vec<RebindColumn>,
    },
    Incompatible(SchemaEvolutionError),
}
```

- [ ] **Step 4: Add the nullability error**

Add the enum variant:

```rust
BaseFieldNullabilityChanged {
    field_id: i32,
    name_at_create: String,
    from_required: bool,
    to_required: bool,
},
```

Add the display arm:

```rust
Self::BaseFieldNullabilityChanged {
    field_id,
    name_at_create,
    from_required,
    to_required,
} => write!(
    f,
    "iceberg MV refresh blocked: base column \"{name_at_create}\" (field id {field_id}) changed nullability from required={from_required} to required={to_required}; run REFRESH FULL or recreate the MV"
),
```

- [ ] **Step 5: Return `RebindColumn` from single-base validation**

Change the signature and body of `check_base_referenced_fields`:

```rust
fn check_base_referenced_fields(
    contract: &MvSchemaContract,
    base_schema: &iceberg::spec::Schema,
) -> Result<Vec<RebindColumn>, SchemaEvolutionError> {
    let current = base_schema.as_struct();
    let mut rebound = Vec::new();
    for record in &contract.base.schema_at_create.fields {
        let Some(field) = current.fields().iter().find(|f| f.id == record.field_id) else {
            return Err(SchemaEvolutionError::BaseFieldDropped {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
            });
        };
        let current_signature = format!("{}", field.field_type);
        if current_signature != record.type_signature {
            return Err(SchemaEvolutionError::BaseFieldTypeChanged {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from: record.type_signature.clone(),
                to: current_signature,
            });
        }
        if field.required != record.required {
            return Err(SchemaEvolutionError::BaseFieldNullabilityChanged {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from_required: record.required,
                to_required: field.required,
            });
        }
        if !field.name.eq_ignore_ascii_case(&record.name_at_create) {
            rebound.push(RebindColumn {
                base_table_fqn: contract.base.table_fqn.clone(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                current_name: field.name.clone(),
            });
        }
    }
    Ok(rebound)
}
```

- [ ] **Step 6: Update old tuple-based assertions**

Change `supplied_base_schema_drives_base_rebind_decision` to expect:

```rust
ContractDecision::CompatibleSafeWithRebind {
    rebound_columns: vec![RebindColumn {
        base_table_fqn: "ice.db.orders".to_string(),
        field_id: 1,
        name_at_create: "id".to_string(),
        current_name: "renamed_id".to_string(),
    }],
}
```

- [ ] **Step 7: Run focused tests**

Run:

```bash
cargo test --lib engine::mv::schema_contract::tests::supplied_base_schema_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit the contract change**

Run:

```bash
git add src/engine/mv/schema_contract.rs
git commit -m "feat: carry base identity in MV schema rebinds"
```

## Task 3: AST Rebind Helper For Aggregate And Join SQL

**Files:**
- Create: `src/engine/mv/rebind.rs`
- Modify: `src/engine/mv/mod.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Create failing rebind tests in the new module**

Create `src/engine/mv/rebind.rs` with test-first content:

```rust
use crate::engine::mv::schema_contract::RebindColumn;

pub(crate) fn rewrite_select_sql_for_rebind(
    stored_sql: &str,
    rebound_columns: &[RebindColumn],
) -> Result<String, String> {
    let _ = rebound_columns;
    Ok(stored_sql.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(old: &str, new: &str) -> Vec<RebindColumn> {
        vec![RebindColumn {
            base_table_fqn: "ice.db.orders".to_string(),
            field_id: 2,
            name_at_create: old.to_string(),
            current_name: new.to_string(),
        }]
    }

    fn join_rebinds() -> Vec<RebindColumn> {
        vec![
            RebindColumn {
                base_table_fqn: "ice.db.fact".to_string(),
                field_id: 2,
                name_at_create: "dim_id".to_string(),
                current_name: "new_dim_id".to_string(),
            },
            RebindColumn {
                base_table_fqn: "ice.db.dim".to_string(),
                field_id: 1,
                name_at_create: "id".to_string(),
                current_name: "new_id".to_string(),
            },
            RebindColumn {
                base_table_fqn: "ice.db.dim".to_string(),
                field_id: 3,
                name_at_create: "region".to_string(),
                current_name: "area".to_string(),
            },
        ]
    }

    #[test]
    fn rewrites_group_by_and_having_for_single_aggregate() {
        let sql = "SELECT region, COUNT(*) AS c FROM ice.db.orders GROUP BY region HAVING region IS NOT NULL ORDER BY region";
        let rewritten = rewrite_select_sql_for_rebind(sql, &single("region", "area")).unwrap();
        assert!(rewritten.contains("area"), "rewritten={rewritten}");
        assert!(!rewritten.to_ascii_lowercase().contains("region"), "rewritten={rewritten}");
    }

    #[test]
    fn rewrites_aggregate_function_argument() {
        let sql = "SELECT region, SUM(amount) AS total_amount FROM ice.db.orders GROUP BY region";
        let rewritten = rewrite_select_sql_for_rebind(sql, &single("amount", "gross_amount")).unwrap();
        assert!(rewritten.contains("SUM(gross_amount)") || rewritten.contains("SUM(gross_amount)"));
        assert!(rewritten.contains("total_amount"), "alias must stay unchanged: {rewritten}");
    }

    #[test]
    fn rewrites_join_on_and_group_key_with_qualifiers() {
        let sql = "SELECT d.region, COUNT(*) AS c FROM ice.db.fact AS f JOIN ice.db.dim AS d ON f.dim_id = d.id GROUP BY d.region ORDER BY d.region";
        let rewritten = rewrite_select_sql_for_rebind(sql, &join_rebinds()).unwrap();
        assert!(rewritten.contains("f.new_dim_id"), "rewritten={rewritten}");
        assert!(rewritten.contains("d.new_id"), "rewritten={rewritten}");
        assert!(rewritten.contains("d.area"), "rewritten={rewritten}");
        assert!(!rewritten.contains("f.dim_id"), "rewritten={rewritten}");
        assert!(!rewritten.contains("d.region"), "rewritten={rewritten}");
    }

    #[test]
    fn preserves_string_literals_and_aliases() {
        let sql = "SELECT region AS region_label FROM ice.db.orders WHERE region = 'region'";
        let rewritten = rewrite_select_sql_for_rebind(sql, &single("region", "area")).unwrap();
        assert!(rewritten.contains("area"), "rewritten={rewritten}");
        assert!(rewritten.contains("region_label"), "rewritten={rewritten}");
        assert!(rewritten.contains("'region'"), "rewritten={rewritten}");
    }

    #[test]
    fn rejects_ambiguous_unqualified_join_rebind() {
        let sql = "SELECT id FROM ice.db.fact AS f JOIN ice.db.dim AS d ON f.id = d.id";
        let err = rewrite_select_sql_for_rebind(sql, &vec![
            RebindColumn {
                base_table_fqn: "ice.db.fact".to_string(),
                field_id: 1,
                name_at_create: "id".to_string(),
                current_name: "fact_id".to_string(),
            },
            RebindColumn {
                base_table_fqn: "ice.db.dim".to_string(),
                field_id: 1,
                name_at_create: "id".to_string(),
                current_name: "dim_id".to_string(),
            },
        ])
        .expect_err("ambiguous unqualified id rejected");
        assert!(err.contains("ambiguous unqualified column"), "err={err}");
    }
}
```

- [ ] **Step 2: Register the module and run failing tests**

Add to `src/engine/mv/mod.rs`:

```rust
pub(crate) mod rebind;
```

Run:

```bash
cargo test --lib engine::mv::rebind::tests:: -- --nocapture
```

Expected: tests fail because the helper returns the input SQL unchanged.

- [ ] **Step 3: Implement the parser entrypoint**

Replace the stub with:

```rust
pub(crate) fn rewrite_select_sql_for_rebind(
    stored_sql: &str,
    rebound_columns: &[RebindColumn],
) -> Result<String, String> {
    if rebound_columns.is_empty() {
        return Ok(stored_sql.to_string());
    }
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(stored_sql)
        .map_err(|e| format!("rebind rewrite: normalize_for_raw_parse: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rebind rewrite: parse: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("rebind rewrite: expected SELECT query".to_string());
    };
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rebind rewrite: expected SELECT body".to_string());
    };
    let ctx = RebindRewriteContext::new(select, rebound_columns)?;
    rewrite_select(select, query, &ctx)?;
    Ok(stmt.to_string())
}
```

- [ ] **Step 4: Add the rewrite context**

Add this helper model below the entrypoint:

```rust
#[derive(Clone, Debug)]
struct RebindRule {
    base_table_fqn: String,
    old_name_lower: String,
    current_name: String,
}

#[derive(Debug)]
struct RebindRewriteContext {
    rules_by_old_name: std::collections::HashMap<String, Vec<RebindRule>>,
    qualifier_to_base: std::collections::HashMap<String, String>,
}

impl RebindRewriteContext {
    fn new(
        select: &sqlparser::ast::Select,
        rebound_columns: &[RebindColumn],
    ) -> Result<Self, String> {
        let mut rules_by_old_name: std::collections::HashMap<String, Vec<RebindRule>> =
            std::collections::HashMap::new();
        for col in rebound_columns {
            rules_by_old_name
                .entry(col.name_at_create.to_ascii_lowercase())
                .or_default()
                .push(RebindRule {
                    base_table_fqn: col.base_table_fqn.to_ascii_lowercase(),
                    old_name_lower: col.name_at_create.to_ascii_lowercase(),
                    current_name: col.current_name.clone(),
                });
        }
        let qualifier_to_base = collect_select_qualifiers(select);
        Ok(Self {
            rules_by_old_name,
            qualifier_to_base,
        })
    }

    fn rewrite_unqualified(&self, ident: &mut sqlparser::ast::Ident) -> Result<(), String> {
        let key = ident.value.to_ascii_lowercase();
        let Some(rules) = self.rules_by_old_name.get(&key) else {
            return Ok(());
        };
        if rules.len() != 1 {
            return Err(format!(
                "rebind rewrite: ambiguous unqualified column {} matches {} base tables; qualify the MV SELECT",
                ident.value,
                rules.len()
            ));
        }
        ident.value = rules[0].current_name.clone();
        Ok(())
    }

    fn rewrite_qualified(&self, parts: &mut [sqlparser::ast::Ident]) -> Result<(), String> {
        let Some(last) = parts.last_mut() else {
            return Ok(());
        };
        let old_name = last.value.to_ascii_lowercase();
        let Some(rules) = self.rules_by_old_name.get(&old_name) else {
            return Ok(());
        };
        let qualifier = parts[..parts.len() - 1]
            .iter()
            .map(|p| p.value.as_str())
            .collect::<Vec<_>>()
            .join(".")
            .to_ascii_lowercase();
        let qualifier_tail = parts
            .get(parts.len().saturating_sub(2))
            .map(|p| p.value.to_ascii_lowercase())
            .unwrap_or_default();
        let mut matches = rules
            .iter()
            .filter(|rule| {
                self.qualifier_to_base
                    .get(&qualifier)
                    .or_else(|| self.qualifier_to_base.get(&qualifier_tail))
                    .is_some_and(|base| base == &rule.base_table_fqn)
                    || rule.base_table_fqn == qualifier
            })
            .collect::<Vec<_>>();
        if matches.len() == 1 {
            last.value = matches.remove(0).current_name.clone();
            return Ok(());
        }
        Err(format!(
            "rebind rewrite: qualifier {} for column {} does not uniquely match a renamed base column",
            qualifier, old_name
        ))
    }
}
```

- [ ] **Step 5: Collect aliases from FROM and JOIN factors**

Add:

```rust
fn collect_select_qualifiers(
    select: &sqlparser::ast::Select,
) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for table_with_joins in &select.from {
        collect_table_factor_qualifier(&table_with_joins.relation, &mut out);
        for join in &table_with_joins.joins {
            collect_table_factor_qualifier(&join.relation, &mut out);
        }
    }
    out
}

fn collect_table_factor_qualifier(
    relation: &sqlparser::ast::TableFactor,
    out: &mut std::collections::HashMap<String, String>,
) {
    if let sqlparser::ast::TableFactor::Table { name, alias, .. } = relation {
        let fqn = name.to_string().to_ascii_lowercase();
        out.insert(fqn.clone(), fqn.clone());
        if let Some(last) = name.0.last() {
            out.insert(last.value.to_ascii_lowercase(), fqn.clone());
        }
        if let Some(alias) = alias {
            out.insert(alias.name.value.to_ascii_lowercase(), fqn);
        }
    }
}
```

- [ ] **Step 6: Rewrite SELECT, JOIN constraints, GROUP BY, HAVING, and ORDER BY**

Add:

```rust
fn rewrite_select(
    select: &mut sqlparser::ast::Select,
    query: &mut sqlparser::ast::Query,
    ctx: &RebindRewriteContext,
) -> Result<(), String> {
    for item in &mut select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                rewrite_expr_idents(e, ctx)?;
            }
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
    if let Some(filter) = &mut select.selection {
        rewrite_expr_idents(filter, ctx)?;
    }
    for table_with_joins in &mut select.from {
        for join in &mut table_with_joins.joins {
            rewrite_join_constraint(&mut join.join_operator, ctx)?;
        }
    }
    match &mut select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                rewrite_expr_idents(expr, ctx)?;
            }
        }
        sqlparser::ast::GroupByExpr::All(_) => {}
    }
    if let Some(having) = &mut select.having {
        rewrite_expr_idents(having, ctx)?;
    }
    if let Some(order_by) = &mut query.order_by {
        for item in &mut order_by.exprs {
            rewrite_expr_idents(&mut item.expr, ctx)?;
        }
    }
    Ok(())
}

fn rewrite_join_constraint(
    op: &mut sqlparser::ast::JoinOperator,
    ctx: &RebindRewriteContext,
) -> Result<(), String> {
    use sqlparser::ast::JoinConstraint;
    let constraint = match op {
        sqlparser::ast::JoinOperator::Inner(c)
        | sqlparser::ast::JoinOperator::LeftOuter(c)
        | sqlparser::ast::JoinOperator::RightOuter(c)
        | sqlparser::ast::JoinOperator::FullOuter(c)
        | sqlparser::ast::JoinOperator::LeftSemi(c)
        | sqlparser::ast::JoinOperator::RightSemi(c)
        | sqlparser::ast::JoinOperator::LeftAnti(c)
        | sqlparser::ast::JoinOperator::RightAnti(c) => c,
        sqlparser::ast::JoinOperator::CrossJoin
        | sqlparser::ast::JoinOperator::CrossApply
        | sqlparser::ast::JoinOperator::OuterApply => return Ok(()),
    };
    if let JoinConstraint::On(expr) = constraint {
        rewrite_expr_idents(expr, ctx)?;
    }
    Ok(())
}
```

- [ ] **Step 7: Rewrite expression identifiers recursively**

Add a `rewrite_expr_idents` function based on the old helper, returning `Result<(), String>` and adding function-order-by support:

```rust
fn rewrite_expr_idents(
    expr: &mut sqlparser::ast::Expr,
    ctx: &RebindRewriteContext,
) -> Result<(), String> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(ident) => ctx.rewrite_unqualified(ident)?,
        Expr::CompoundIdentifier(parts) => ctx.rewrite_qualified(parts)?,
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr_idents(left, ctx)?;
            rewrite_expr_idents(right, ctx)?;
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::Nested(expr) => {
            rewrite_expr_idents(expr, ctx)?;
        }
        Expr::Function(func) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut func.args {
                for arg in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = arg
                    {
                        rewrite_expr_idents(inner, ctx)?;
                    }
                }
                for clause in &mut list.clauses {
                    if let sqlparser::ast::FunctionArgumentClause::OrderBy(order_by) = clause {
                        for item in order_by {
                            rewrite_expr_idents(&mut item.expr, ctx)?;
                        }
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
                rewrite_expr_idents(op, ctx)?;
            }
            for c in conditions {
                rewrite_expr_idents(&mut c.condition, ctx)?;
                rewrite_expr_idents(&mut c.result, ctx)?;
            }
            if let Some(e) = else_result {
                rewrite_expr_idents(e, ctx)?;
            }
        }
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner) => rewrite_expr_idents(inner, ctx)?,
        Expr::InList { expr, list, .. } => {
            rewrite_expr_idents(expr, ctx)?;
            for e in list {
                rewrite_expr_idents(e, ctx)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_expr_idents(expr, ctx)?;
            rewrite_expr_idents(low, ctx)?;
            rewrite_expr_idents(high, ctx)?;
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            rewrite_expr_idents(expr, ctx)?;
            rewrite_expr_idents(pattern, ctx)?;
        }
        Expr::Subquery(_) | Expr::Exists { .. } => {
            return Err("rebind rewrite: subqueries are not supported in Iceberg MV definitions".to_string());
        }
        _ => {}
    }
    Ok(())
}
```

- [ ] **Step 8: Remove the old helper from `iceberg_refresh.rs`**

Delete the old `rewrite_select_sql_for_rebind`, `rewrite_expr_idents`, and `mod rebind_tests` block from `src/engine/mv/iceberg_refresh.rs`.

Add this import near other MV module imports:

```rust
use crate::engine::mv::rebind::rewrite_select_sql_for_rebind;
```

- [ ] **Step 9: Run focused rebind tests**

Run:

```bash
cargo test --lib engine::mv::rebind::tests:: -- --nocapture
```

Expected: PASS.

- [ ] **Step 10: Run existing old rebind coverage through compile**

Run:

```bash
cargo test --lib engine::mv::schema_contract::tests::supplied_base_schema_ engine::mv::rebind::tests:: -- --nocapture
```

Expected: PASS.

- [ ] **Step 11: Commit the rebind helper**

Run:

```bash
git add src/engine/mv/mod.rs src/engine/mv/rebind.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat: rewrite Iceberg MV aggregate rebind SQL"
```

## Task 4: Join-Family Contract Decisions And Effective Definition Rebind

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/schema_contract.rs`

- [ ] **Step 1: Add join validator unit tests**

Because constructing full `iceberg::table::Table` instances is heavy, add a pure helper in `src/engine/mv/iceberg_refresh.rs` near `validate_join_schema_contract`:

```rust
fn validate_join_base_schema_contract_for_rebind(
    base_fqn: &str,
    base_contract: &crate::meta::repository::mv_contract::BaseContract,
    current_schema: &iceberg::spec::Schema,
) -> Result<Vec<crate::engine::mv::schema_contract::RebindColumn>, String> {
    let current_schema = current_schema.as_struct();
    let mut rebound = Vec::new();
    for record in &base_contract.schema_at_create.fields {
        let Some(field) = current_schema
            .fields()
            .iter()
            .find(|field| field.id == record.field_id)
        else {
            return Err(format!(
                "iceberg join MV refresh blocked: base column \"{}\" (field id {}) was dropped from {}; recreate the MV",
                record.name_at_create, record.field_id, base_fqn
            ));
        };
        if format!("{}", field.field_type) != record.type_signature {
            return Err(format!(
                "iceberg join MV refresh blocked: base column \"{}\" (field id {}) changed type from {} to {}; recreate the MV",
                record.name_at_create, record.field_id, record.type_signature, field.field_type
            ));
        }
        if field.required != record.required {
            return Err(format!(
                "iceberg join MV refresh blocked: base column \"{}\" (field id {}) changed nullability; recreate the MV",
                record.name_at_create, record.field_id
            ));
        }
        if !field.name.eq_ignore_ascii_case(&record.name_at_create) {
            rebound.push(crate::engine::mv::schema_contract::RebindColumn {
                base_table_fqn: base_fqn.to_string(),
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                current_name: field.name.clone(),
            });
        }
    }
    Ok(rebound)
}
```

Then add tests under the existing `#[cfg(test)]` module in `src/engine/mv/iceberg_refresh.rs`:

```rust
#[test]
fn join_base_schema_contract_returns_rebind_for_rename() {
    let ty = iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long);
    let base_contract = crate::meta::repository::mv_contract::BaseContract {
        table_fqn: "ice.db.fact".to_string(),
        table_uuid: "uuid".to_string(),
        alias_at_create: Some("f".to_string()),
        schema_id_at_create: 1,
        schema_at_create: crate::meta::repository::mv_contract::BaseSchemaSnapshot {
            fields: vec![crate::meta::repository::mv_contract::BaseFieldRecord {
                field_id: 2,
                name_at_create: "dim_id".to_string(),
                type_signature: format!("{ty}"),
                required: false,
            }],
        },
    };
    let current_schema = iceberg::spec::Schema::builder()
        .with_schema_id(2)
        .with_fields(vec![std::sync::Arc::new(iceberg::spec::NestedField::optional(
            2,
            "new_dim_id",
            ty,
        ))])
        .build()
        .expect("schema");

    let rebound = validate_join_base_schema_contract_for_rebind(
        "ice.db.fact",
        &base_contract,
        &current_schema,
    )
    .expect("compatible");

    assert_eq!(rebound.len(), 1);
    assert_eq!(rebound[0].base_table_fqn, "ice.db.fact");
    assert_eq!(rebound[0].name_at_create, "dim_id");
    assert_eq!(rebound[0].current_name, "new_dim_id");
}
```

- [ ] **Step 2: Run the test and confirm failure or compile drift**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::join_base_schema_contract_returns_rebind_for_rename -- --exact --nocapture
```

Expected: PASS after adding the helper. This helper is intentionally introduced before changing the main validator so the rename behavior is testable without table fixtures.

- [ ] **Step 3: Introduce `JoinSchemaContractDecision`**

Add near `validate_join_schema_contract`:

```rust
#[derive(Debug, PartialEq, Eq)]
enum JoinSchemaContractDecision {
    CompatibleSafe,
    CompatibleSafeWithRebind {
        rebound_columns: Vec<crate::engine::mv::schema_contract::RebindColumn>,
    },
}

impl JoinSchemaContractDecision {
    fn into_definition(
        self,
        mv_definition: &StoredMvDefinition,
    ) -> Result<StoredMvDefinition, String> {
        match self {
            Self::CompatibleSafe => Ok(mv_definition.clone()),
            Self::CompatibleSafeWithRebind { rebound_columns } => {
                let rewritten_sql =
                    rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
                let mut def = mv_definition.clone();
                def.select_sql = rewritten_sql;
                Ok(def)
            }
        }
    }
}
```

- [ ] **Step 4: Change `validate_join_schema_contract` to return the decision**

Change the signature:

```rust
fn validate_join_schema_contract(
    contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    bases: &[(&IcebergTableRef, &iceberg::table::Table); 2],
    target_table: &iceberg::table::Table,
) -> Result<JoinSchemaContractDecision, String>
```

Inside the loop, replace the inline per-field checks with:

```rust
let mut rebound_columns = Vec::new();
```

Before the loop, declare that vector. In each base iteration, call:

```rust
rebound_columns.extend(validate_join_base_schema_contract_for_rebind(
    &base_ref.fqn(),
    base_contract,
    table.metadata().current_schema(),
)?);
```

After target validation, return:

```rust
if rebound_columns.is_empty() {
    Ok(JoinSchemaContractDecision::CompatibleSafe)
} else {
    Ok(JoinSchemaContractDecision::CompatibleSafeWithRebind { rebound_columns })
}
```

- [ ] **Step 5: Update planning call sites to accept either compatible decision**

In `plan_iceberg_mv_refresh`, replace:

```rust
validate_join_schema_contract(schema_contract, &join_bases, &target_loaded.table)
    .map_err(RefreshError::user)?;
```

with:

```rust
match validate_join_schema_contract(schema_contract, &join_bases, &target_loaded.table)
    .map_err(RefreshError::user)?
{
    JoinSchemaContractDecision::CompatibleSafe
    | JoinSchemaContractDecision::CompatibleSafeWithRebind { .. } => {}
}
```

Make the same change in `plan_iceberg_aggregate_mv_refresh` for `IncrementalMvShape::JoinAggregate`.

- [ ] **Step 6: Update join projection refresh execution to use effective definitions**

In `refresh_iceberg_join_mv`, replace the pre-pin validation with:

```rust
validate_join_schema_contract(schema_contract, &pre_pin_join_bases, target_table)?;
```

This pre-pin call remains a guard only.

After reloading pinned tables, replace the second validation with:

```rust
let effective_definition = validate_join_schema_contract(
    schema_contract,
    &[
        (left_ref, &left_loaded.table),
        (right_ref, &right_loaded.table),
    ],
    target_table,
)?
.into_definition(mv_definition)?;
let mv_definition = &effective_definition;
```

Keep all downstream calls using this shadowed `mv_definition`.

- [ ] **Step 7: Update join aggregate refresh execution to use effective definitions**

In `refresh_join_aggregate_iceberg_mv`, replace the current validation after loading left/right tables with:

```rust
let effective_definition = validate_join_schema_contract(
    schema_contract,
    &[
        (left_ref, &left_loaded.table),
        (right_ref, &right_loaded.table),
    ],
    target_table,
)?
.into_definition(mv_definition)?;
let mv_definition = &effective_definition;
```

Ensure the later calls to `first_refresh_iceberg_aggregate_mv`, `finalize_iceberg_mv_metadata_only_refresh`, and `incremental_refresh_iceberg_join_aggregate_mv` pass the shadowed `mv_definition`.

- [ ] **Step 8: Update single-base rebind call sites for `RebindColumn`**

The existing calls in projection/filter and single aggregate refresh keep the same function name:

```rust
let rewritten_sql =
    rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
```

No tuple conversion should remain. Verify with:

```bash
rg -n "Vec<\\(i32, String, String\\)|rebound_columns: vec!\\[\\(" src/engine/mv
```

Expected: no matches.

- [ ] **Step 9: Run focused Rust tests**

Run:

```bash
cargo test --lib engine::mv::schema_contract::tests::supplied_base_schema_ engine::mv::rebind::tests:: engine::mv::iceberg_refresh::tests::join_base_schema_contract_returns_rebind_for_rename -- --nocapture
```

Expected: PASS.

- [ ] **Step 10: Commit join decision plumbing**

Run:

```bash
git add src/engine/mv/schema_contract.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat: allow Iceberg join MV schema rebinds"
```

## Task 5: Aggregate Schema Evolution SQL Coverage

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_rename_group_key.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_rename_group_key.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_rename_measure.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_rename_measure.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_nullability_change_referenced.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_nullability_change_referenced.result`

- [ ] **Step 1: Write the group-key rename SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_rename_group_key.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,aggregate,base_rename
-- Test Point: Iceberg aggregate MV refresh rebinds a renamed referenced GROUP BY key.
-- Method: Rename base column region -> area through Spark, insert/delete rows through NovaRocks, refresh, and compare the MV with the rewritten base aggregate.
-- Scope: Iceberg target MV, single-base aggregate, schema evolution, field-id rebind.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_agg_a11_group_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_agg_a11_group_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_a11_group_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_agg_a11_group_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 10),
  ('east', 20),
  ('west', 5);
SET CATALOG ice_ivm_agg_a11_group_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
SELECT region, c, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-agg-group-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders RENAME COLUMN region TO area;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_agg_a11_group_${uuid0}.ns_${uuid0}.orders VALUES ('east', 30), ('north', 7);
DELETE FROM ice_ivm_agg_a11_group_${uuid0}.ns_${uuid0}.orders WHERE area = 'west';
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 6
SELECT region, c, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 7
SELECT area AS region, COUNT(*) AS c, SUM(amount) AS s
FROM orders
GROUP BY area
ORDER BY area;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_a11_group_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_agg_a11_group_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_a11_group_${uuid0};
```

- [ ] **Step 2: Write expected group-key result file**

Create `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_rename_group_key.result`:

```text
-- query 3
region	c	s
east	2	30
west	1	5
-- query 4
SPARK_SQL_OK
-- query 6
region	c	s
east	3	60
north	1	7
-- query 7
region	c	s
east	3	60
north	1	7
```

- [ ] **Step 3: Write the measure rename SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_rename_measure.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,aggregate,base_rename
-- Test Point: Iceberg aggregate MV refresh rebinds a renamed referenced aggregate input.
-- Method: Rename base column amount -> gross_amount through Spark, insert/delete rows through NovaRocks, refresh, and compare the MV with the rewritten base aggregate.
-- Scope: Iceberg target MV, single-base aggregate, schema evolution, field-id rebind.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_agg_a11_measure_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 10),
  ('east', 20),
  ('west', 5);
SET CATALOG ice_ivm_agg_a11_measure_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(amount) AS c_amount, SUM(amount) AS s
FROM orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
SELECT region, c_amount, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-agg-measure-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders RENAME COLUMN amount TO gross_amount;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders VALUES ('east', 30), ('north', 7);
DELETE FROM ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders WHERE region = 'west';
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 6
SELECT region, c_amount, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 7
SELECT region, COUNT(gross_amount) AS c_amount, SUM(gross_amount) AS s
FROM orders
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_a11_measure_${uuid0};
```

- [ ] **Step 4: Write expected measure result file**

Create `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_rename_measure.result`:

```text
-- query 3
region	c_amount	s
east	2	30
west	1	5
-- query 4
SPARK_SQL_OK
-- query 6
region	c_amount	s
east	3	60
north	1	7
-- query 7
region	c_amount	s
east	3	60
north	1	7
```

- [ ] **Step 5: Write nullability fail-fast SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_nullability_change_referenced.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,aggregate,nullability,error
-- Test Point: Iceberg aggregate MV refresh rejects referenced base nullability drift.
-- Method: Create an aggregate MV over a required group key, relax the field to optional through Spark, and verify refresh fails fast.
-- Scope: Iceberg target MV, single-base aggregate, schema contract.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_agg_a11_null_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders (
  region STRING NOT NULL,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders VALUES ('east', 10), ('west', 5);
SET CATALOG ice_ivm_agg_a11_null_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c
FROM orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-agg-null-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders ALTER COLUMN region DROP NOT NULL;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 4
-- @expect_error=changed nullability
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_a11_null_${uuid0};
```

- [ ] **Step 6: Write expected nullability result file**

Create `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_nullability_change_referenced.result`:

```text
-- query 3
SPARK_SQL_OK
```

- [ ] **Step 7: Run aggregate evolution SQL tests**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_a11_base_rename_group_key,iceberg_ivm_aggregate_a11_base_rename_measure,iceberg_ivm_aggregate_a11_base_nullability_change_referenced \
  --mode verify
```

Expected: `total=3 pass=3 fail=0`.

- [ ] **Step 8: Commit aggregate evolution coverage**

Run:

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_rename_group_key.sql \
  sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_rename_group_key.result \
  sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_rename_measure.sql \
  sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_rename_measure.result \
  sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_a11_base_nullability_change_referenced.sql \
  sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_a11_base_nullability_change_referenced.result
git commit -m "test: cover Iceberg aggregate MV base schema evolution"
```

## Task 6: Join Aggregate Schema Evolution SQL Coverage

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_a11_base_rename_join_key.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_a11_base_rename_join_key.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_a11_base_rename_group_key.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_a11_base_rename_group_key.result`

- [ ] **Step 1: Write join-key rename SQL**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_a11_base_rename_join_key.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,join,aggregate,base_rename
-- Test Point: Iceberg join aggregate MV refresh rebinds a renamed fact-side join key.
-- Method: Rename fact.dim_id -> new_dim_id through Spark, mutate fact rows through NovaRocks, refresh, and compare the MV with the rewritten base query.
-- Scope: Iceberg target MV, join aggregate, schema evolution, field-id rebind.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_agg_a11_key_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_agg_a11_key_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_agg_a11_key_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_agg_a11_key_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_agg_a11_key_${uuid0};
USE ns_${uuid0};
INSERT INTO dim VALUES (10, 'east'), (20, 'west');
INSERT INTO fact VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50);
CREATE MATERIALIZED VIEW join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 3
SELECT region, c, s FROM join_agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-join-key-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.fact RENAME COLUMN dim_id TO new_dim_id;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
INSERT INTO fact VALUES (4, 20, 80);
DELETE FROM fact WHERE id = 1;
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 6
SELECT region, c, s FROM join_agg_mv_${uuid0} ORDER BY region;

-- query 7
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.new_dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_agg_mv_${uuid0};
DROP TABLE ice_ivm_join_agg_a11_key_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_join_agg_a11_key_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_join_agg_a11_key_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_agg_a11_key_${uuid0};
```

- [ ] **Step 2: Write join-key expected result**

Create `sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_a11_base_rename_join_key.result`:

```text
-- query 3
region	c	s
east	2	300
west	1	50
-- query 4
SPARK_SQL_OK
-- query 6
region	c	s
east	1	200
west	2	130
-- query 7
region	c	s
east	1	200
west	2	130
```

- [ ] **Step 3: Write group-key rename SQL**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_a11_base_rename_group_key.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,join,aggregate,base_rename
-- Test Point: Iceberg join aggregate MV refresh rebinds a renamed dim-side GROUP BY key.
-- Method: Rename dim.region -> area through Spark, update the dim-side group value, refresh, and compare the MV with the rewritten base query.
-- Scope: Iceberg target MV, join aggregate, schema evolution, field-id rebind.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_agg_a11_group_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_agg_a11_group_${uuid0};
USE ns_${uuid0};
INSERT INTO dim VALUES (10, 'east'), (20, 'west');
INSERT INTO fact VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50);
CREATE MATERIALIZED VIEW join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 3
SELECT region, c, s FROM join_agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-join-group-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.dim RENAME COLUMN region TO area;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
UPDATE dim SET area = 'north' WHERE id = 10;
INSERT INTO fact VALUES (4, 20, 80);
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 6
SELECT region, c, s FROM join_agg_mv_${uuid0} ORDER BY region;

-- query 7
SELECT d.area AS region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
GROUP BY d.area
ORDER BY d.area;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_agg_mv_${uuid0};
DROP TABLE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_agg_a11_group_${uuid0};
```

- [ ] **Step 4: Write group-key expected result**

Create `sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_a11_base_rename_group_key.result`:

```text
-- query 3
region	c	s
east	2	300
west	1	50
-- query 4
SPARK_SQL_OK
-- query 6
region	c	s
north	2	300
west	2	130
-- query 7
region	c	s
north	2	300
west	2	130
```

- [ ] **Step 5: Run join aggregate schema evolution SQL tests**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_join_aggregate_a11_base_rename_join_key,iceberg_ivm_join_aggregate_a11_base_rename_group_key \
  --mode verify
```

Expected: `total=2 pass=2 fail=0`.

- [ ] **Step 6: Commit join aggregate evolution coverage**

Run:

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_a11_base_rename_join_key.sql \
  sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_a11_base_rename_join_key.result \
  sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_a11_base_rename_group_key.sql \
  sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_a11_base_rename_group_key.result
git commit -m "test: cover Iceberg join aggregate MV schema rebinds"
```

## Task 7: Base Partition Evolution SQL Coverage

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_base_partition_evolution.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_base_partition_evolution.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_base_partition_evolution.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_base_partition_evolution.result`

- [ ] **Step 1: Write single aggregate base partition evolution SQL**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_base_partition_evolution.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,partition_evolution
-- Test Point: Iceberg aggregate MV refresh treats base partition evolution as transparent when schema contract remains compatible.
-- Method: Create an unpartitioned base, refresh aggregate MV, evolve base to PARTITION BY region through Spark, write new rows, refresh, and compare with base aggregate.
-- Scope: Iceberg target MV, single-base aggregate, base partition evolution.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_agg_part_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_agg_part_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_part_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_agg_part_${uuid0};
USE ns_${uuid0};
INSERT INTO orders VALUES ('east', 10), ('west', 5);
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
SELECT region, c, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-agg-part-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders ADD PARTITION FIELD region;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
INSERT INTO orders VALUES ('east', 20), ('north', 7);
DELETE FROM orders WHERE region = 'west';
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 6
SELECT region, c, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 7
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM orders
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_part_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_agg_part_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_part_${uuid0};
```

- [ ] **Step 2: Write expected single aggregate partition result**

Create `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_base_partition_evolution.result`:

```text
-- query 3
region	c	s
east	1	10
west	1	5
-- query 4
SPARK_SQL_OK
-- query 6
region	c	s
east	2	30
north	1	7
-- query 7
region	c	s
east	2	30
north	1	7
```

- [ ] **Step 3: Write join aggregate base partition evolution SQL**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_base_partition_evolution.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,aggregate,partition_evolution
-- Test Point: Iceberg join aggregate MV refresh treats one-side base partition evolution as transparent when schema contract remains compatible.
-- Method: Create unpartitioned fact/dim bases, refresh join aggregate MV, evolve fact partition spec through Spark, write new fact rows, refresh, and compare with base query.
-- Scope: Iceberg target MV, join aggregate, base partition evolution.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_agg_part_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_agg_part_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_agg_part_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_agg_part_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_agg_part_${uuid0};
USE ns_${uuid0};
INSERT INTO dim VALUES (10, 'east'), (20, 'west');
INSERT INTO fact VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50);
CREATE MATERIALIZED VIEW join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 3
SELECT region, c, s FROM join_agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-join-part-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.fact ADD PARTITION FIELD bucket(4, dim_id);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
INSERT INTO fact VALUES (4, 20, 80);
DELETE FROM fact WHERE id = 1;
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 6
SELECT region, c, s FROM join_agg_mv_${uuid0} ORDER BY region;

-- query 7
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_agg_mv_${uuid0};
DROP TABLE ice_ivm_join_agg_part_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_join_agg_part_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_join_agg_part_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_agg_part_${uuid0};
```

- [ ] **Step 4: Write expected join aggregate partition result**

Create `sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_base_partition_evolution.result`:

```text
-- query 3
region	c	s
east	2	300
west	1	50
-- query 4
SPARK_SQL_OK
-- query 6
region	c	s
east	1	200
west	2	130
-- query 7
region	c	s
east	1	200
west	2	130
```

- [ ] **Step 5: Run partition evolution SQL tests**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_base_partition_evolution,iceberg_ivm_join_aggregate_base_partition_evolution \
  --mode verify
```

Expected: `total=2 pass=2 fail=0`.

- [ ] **Step 6: Fix any explicit single-spec assumption exposed by these tests**

When failure output contains `partition evolution`, `partition specs`, or `spec_id`, inspect these paths:

```bash
rg -n "specs\\(\\)|default_partition_spec|partition_spec_id|single partition|partition evolution" src/connector/iceberg src/engine
```

Allowed fix shape:

```rust
let spec_id = data_file.partition_spec_id.ok_or_else(|| {
    format!("Iceberg data file {} is missing partition spec id", data_file.file_path)
})?;
let spec = metadata
    .partition_specs()
    .get(&spec_id)
    .ok_or_else(|| format!("Iceberg partition spec id {spec_id} not found"))?;
```

Do not add an MV-level guard like `metadata.specs().len() > 1`; base partition evolution is supported when schema contract remains compatible.

- [ ] **Step 7: Re-run partition evolution SQL tests**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_base_partition_evolution,iceberg_ivm_join_aggregate_base_partition_evolution \
  --mode verify
```

Expected: `total=2 pass=2 fail=0`.

- [ ] **Step 8: Commit partition evolution coverage**

Run:

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_base_partition_evolution.sql \
  sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_base_partition_evolution.result \
  sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate_base_partition_evolution.sql \
  sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate_base_partition_evolution.result \
  src/connector/iceberg src/engine
git commit -m "test: cover Iceberg MV base partition evolution"
```

## Task 8: Full Verification And Documentation Commit

**Files:**
- Modify: `docs/design/specs/2026-05-18-iceberg-target-mv-base-evolution-hardening-design.md`
- Modify: `docs/design/plans/2026-05-18-iceberg-target-mv-base-evolution-hardening.md`

- [ ] **Step 1: Format Rust code**

Run:

```bash
cargo fmt
```

Expected: exits 0.

- [ ] **Step 2: Run focused Rust tests**

Run:

```bash
cargo test --lib engine::mv::schema_contract::tests:: engine::mv::rebind::tests:: engine::mv::iceberg_refresh::tests::join_base_schema_contract_returns_rebind_for_rename -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Run the complete new SQL set**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_a11_base_rename_group_key,iceberg_ivm_aggregate_a11_base_rename_measure,iceberg_ivm_join_aggregate_a11_base_rename_join_key,iceberg_ivm_join_aggregate_a11_base_rename_group_key,iceberg_ivm_aggregate_a11_base_nullability_change_referenced,iceberg_ivm_aggregate_base_partition_evolution,iceberg_ivm_join_aggregate_base_partition_evolution \
  --mode verify
```

Expected: `total=7 pass=7 fail=0`.

- [ ] **Step 4: Run baseline A11 and aggregate/join aggregate cases**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_a11_base_rename_referenced,iceberg_ivm_a11_base_drop_referenced,iceberg_ivm_a11_base_type_change_referenced,iceberg_ivm_join_a11_base_drop_referenced,iceberg_ivm_join_a11_base_type_change_referenced,iceberg_ivm_aggregate_target,iceberg_ivm_join_aggregate \
  --mode verify
```

Expected: `total=7 pass=7 fail=0`.

- [ ] **Step 5: Run whitespace and status checks**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; status contains only intended source, SQL, result, spec, and plan files.

- [ ] **Step 6: Commit docs if they are not already committed**

Run:

```bash
git add docs/design/specs/2026-05-18-iceberg-target-mv-base-evolution-hardening-design.md \
  docs/design/plans/2026-05-18-iceberg-target-mv-base-evolution-hardening.md
git commit -m "docs: plan Iceberg MV base evolution hardening"
```

- [ ] **Step 7: Produce final branch summary**

Run:

```bash
git log --oneline --decorate origin/main..HEAD
git status --short --branch
```

Expected: branch contains only this feature's commits, with a clean working tree.

## Self-Review

- Spec coverage:
  - Base schema matrix is covered by Tasks 2, 4, 5, and 6.
  - Rebind rewrite coverage is covered by Task 3.
  - Base partition evolution coverage is covered by Task 7.
  - Target partition drift stays in existing `check_target_partition_spec` and is protected by existing tests plus Task 8 baseline verification.
  - Managed-lake target remains untouched because all files are under Iceberg MV refresh, contract, and `sql-tests/iceberg-ivm`.
- Placeholder scan:
  - The plan has no placeholder markers, no incomplete file names, and every new SQL/result file has a concrete path and content shape.
- Type consistency:
  - `RebindColumn` is defined in `schema_contract.rs` and reused by `rebind.rs` and `iceberg_refresh.rs`.
  - `JoinSchemaContractDecision` stays local to `iceberg_refresh.rs`, matching the existing join validator location.
  - SQL test names match the `--only` verification commands.
