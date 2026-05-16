# Iceberg Join IMV Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Iceberg-backed two-table inner equi-join projection/filter IMV with correct telescoping delta refresh when both base tables change in the same refresh.

**Architecture:** Extend the Iceberg-backed MV path with a `JoinProjectionFilter` shape, multi-base schema contract, branch planner, branch AST rewriter, and a refresh-scoped join coalescer. Each branch runs through the existing SQL pipeline with at most one `__nr_ivm_delta` source; branch outputs are coalesced by a composite join row key before writing target Iceberg data/delete files.

**Tech Stack:** Rust, sqlparser AST, Arrow `RecordBatch`/`Chunk`, NovaRocks standalone SQL planner/pipeline, Iceberg v3 row-lineage, existing A1 `IcebergDeltaScan`, A2/A3 `RefreshSnapshotPin`, A11 schema contract, `sql-tests/iceberg-ivm`.

---

## File Structure

- Modify `src/connector/starrocks/managed/mv_shape.rs`
  - Add `JoinProjectionFilterMvShape`, join key shape, and classifier tests.
  - Keep existing projection/filter and aggregate behaviour stable.
- Modify `src/meta/repository/mv_contract.rs`
  - Upgrade persisted contract model to version 2 with multi-base fields.
  - Preserve serde compatibility for existing version 1 records via defaulted fields or conversion helpers.
- Modify `src/sql/analyzer/mv_lineage.rs`
  - Add qualified multi-base lineage collection for join MV projection, filter, and join condition.
- Modify `src/engine/mv/schema_contract.rs`
  - Validate all base contracts, target hidden key source, and join lineage.
- Modify `src/engine/mv/iceberg_target_apply.rs`
  - Add `JoinRowKey` apply-key source constants and target hidden key helper.
  - Keep existing `BaseRowId` path unchanged.
- Create `src/engine/mv/iceberg_join_branch.rs`
  - Own telescoping branch planning and AST rewrite for join refresh.
- Create `src/engine/mv/iceberg_join_coalesce.rs`
  - Own `IcebergJoinCoalesceSinkFactory`, `JoinDeltaCoalescer`, composite key generation, and flush into `IcebergCommitCollector`.
- Modify `src/engine/mv/mod.rs`
  - Register the two new modules.
- Modify `src/engine/mv/iceberg_refresh.rs`
  - Open Iceberg-backed CREATE gate for two-table join shape.
  - Dispatch refresh to single-base existing path or join branch/coalesce path.
- Add SQL tests under `sql-tests/iceberg-ivm/sql` and expected results under `sql-tests/iceberg-ivm/result`.

## Task 1: Shape Classifier

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs`

- [ ] **Step 1: Write failing shape tests**

Append these tests to `mod tests` in `src/connector/starrocks/managed/mv_shape.rs`. If the file has no helper for raw parsing, add the helper inside the test module.

```rust
fn parse_shape(sql: &str) -> Result<IncrementalMvShape, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .expect("normalize");
    let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .expect("parse");
    let sqlparser::ast::Statement::Query(query) = stmt else {
        panic!("expected query");
    };
    classify_incremental_mv_query(&query)
}

#[test]
fn join_projection_filter_accepts_two_table_inner_equi_join() {
    let shape = parse_shape(
        "select l.id, r.label \
         from ice.ns.orders l join ice.ns.dim r on l.dim_id = r.id \
         where l.amount > 10",
    )
    .expect("join shape");
    match shape {
        IncrementalMvShape::JoinProjectionFilter(join) => {
            assert_eq!(join.left_alias, "l");
            assert_eq!(join.right_alias, "r");
            assert_eq!(join.join_keys.len(), 1);
            assert_eq!(join.left_table.to_string(), "ice.ns.orders");
            assert_eq!(join.right_table.to_string(), "ice.ns.dim");
        }
        other => panic!("expected join shape, got {other:?}"),
    }
}

#[test]
fn join_projection_filter_rejects_outer_join() {
    let err = parse_shape(
        "select l.id, r.label \
         from ice.ns.orders l left join ice.ns.dim r on l.dim_id = r.id",
    )
    .expect_err("outer join rejected");
    assert!(err.contains("two-table inner equi-join"), "err={err}");
}

#[test]
fn join_projection_filter_rejects_non_equi_join() {
    let err = parse_shape(
        "select l.id, r.label \
         from ice.ns.orders l join ice.ns.dim r on l.dim_id > r.id",
    )
    .expect_err("non-equi join rejected");
    assert!(err.contains("equi-join"), "err={err}");
}

#[test]
fn join_projection_filter_rejects_three_table_join() {
    let err = parse_shape(
        "select l.id, r.label, x.name \
         from ice.ns.orders l \
         join ice.ns.dim r on l.dim_id = r.id \
         join ice.ns.extra x on x.id = r.id",
    )
    .expect_err("three table join rejected");
    assert!(err.contains("exactly two"), "err={err}");
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_shape::tests::join_projection_filter -- --nocapture
```

Expected: compile failure or test failure because `JoinProjectionFilter` types do not exist.

- [ ] **Step 3: Add join shape types and classifier**

In `src/connector/starrocks/managed/mv_shape.rs`, extend the enum and add the new structs:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
    JoinProjectionFilter(JoinProjectionFilterMvShape),
}

impl IncrementalMvShape {
    pub(crate) fn base_table(&self) -> &sqlparser::ast::ObjectName {
        match self {
            IncrementalMvShape::ProjectionFilter(shape) => &shape.base_table,
            IncrementalMvShape::Aggregate(shape) => &shape.base_table,
            IncrementalMvShape::JoinProjectionFilter(_) => {
                panic!("base_table() is only valid for single-base MV shapes")
            }
        }
    }

    pub(crate) fn base_tables(&self) -> Vec<&sqlparser::ast::ObjectName> {
        match self {
            IncrementalMvShape::ProjectionFilter(shape) => vec![&shape.base_table],
            IncrementalMvShape::Aggregate(shape) => vec![&shape.base_table],
            IncrementalMvShape::JoinProjectionFilter(shape) => {
                vec![&shape.left_table, &shape.right_table]
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinProjectionFilterMvShape {
    pub(crate) left_table: sqlparser::ast::ObjectName,
    pub(crate) left_alias: String,
    pub(crate) right_table: sqlparser::ast::ObjectName,
    pub(crate) right_alias: String,
    pub(crate) join_keys: Vec<JoinKeyPairShape>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinKeyPairShape {
    pub(crate) left_expr: sqlparser::ast::Expr,
    pub(crate) right_expr: sqlparser::ast::Expr,
}
```

Update `classify_incremental_mv_query` so join classification runs before single-table projection/filter:

```rust
pub(crate) fn classify_incremental_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    match classify_aggregate_mv_query(query) {
        Ok(shape) => return Ok(IncrementalMvShape::Aggregate(shape)),
        Err(err) if is_probably_aggregate_query(query) => return Err(err),
        Err(_) => {}
    }

    match classify_join_projection_filter_mv_query(query) {
        Ok(shape) => return Ok(IncrementalMvShape::JoinProjectionFilter(shape)),
        Err(err) if is_probably_join_query(query) => return Err(err),
        Err(_) => {}
    }

    classify_projection_filter_mv_query(query).map(IncrementalMvShape::ProjectionFilter)
}
```

Add these helpers in the same file:

```rust
fn classify_join_projection_filter_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<JoinProjectionFilterMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| join_projection_filter_error())?;
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(join_projection_filter_error());
    };
    reject_unsupported_select_clauses(select).map_err(|_| join_projection_filter_error())?;
    reject_match_against_before_from_shape_check(select).map_err(|_| join_projection_filter_error())?;
    reject_unsupported_projection_filter_exprs(select).map_err(|_| join_projection_filter_error())?;

    let [from] = select.from.as_slice() else {
        return Err("incremental join MV requires exactly one FROM item with exactly two tables".to_string());
    };
    let [join] = from.joins.as_slice() else {
        return Err("incremental join MV requires exactly two Iceberg base tables".to_string());
    };
    if !matches!(join.join_operator, sqlparser::ast::JoinOperator::Inner(_)) {
        return Err("incremental join MV supports only two-table inner equi-join".to_string());
    }
    let (left_table, left_alias) = table_factor_name_and_alias(&from.relation)?;
    let (right_table, right_alias) = table_factor_name_and_alias(&join.relation)?;
    let condition = match &join.join_operator {
        sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr)) => expr,
        _ => return Err("incremental join MV requires JOIN ... ON equi predicates".to_string()),
    };
    let mut join_keys = Vec::new();
    collect_equi_join_keys(condition, &left_alias, &right_alias, &mut join_keys)?;
    if join_keys.is_empty() {
        return Err("incremental join MV requires at least one equi-join predicate".to_string());
    }
    Ok(JoinProjectionFilterMvShape {
        left_table,
        left_alias,
        right_table,
        right_alias,
        join_keys,
    })
}

fn table_factor_name_and_alias(
    factor: &sqlparser::ast::TableFactor,
) -> Result<(sqlparser::ast::ObjectName, String), String> {
    let sqlparser::ast::TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = factor else {
        return Err("incremental join MV base relation must be a table".to_string());
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || !is_three_part_object_name(name)
    {
        return Err("incremental join MV base relation must be a plain 3-part Iceberg table".to_string());
    }
    let fallback = name
        .0
        .last()
        .and_then(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .ok_or_else(|| "incremental join MV table name has no identifier".to_string())?;
    let alias = alias
        .as_ref()
        .map(|a| a.name.value.clone())
        .unwrap_or(fallback);
    Ok((name.clone(), alias))
}

fn collect_equi_join_keys(
    expr: &sqlparser::ast::Expr,
    left_alias: &str,
    right_alias: &str,
    out: &mut Vec<JoinKeyPairShape>,
) -> Result<(), String> {
    match expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right }
            if matches!(op, sqlparser::ast::BinaryOperator::And) =>
        {
            collect_equi_join_keys(left, left_alias, right_alias, out)?;
            collect_equi_join_keys(right, left_alias, right_alias, out)
        }
        sqlparser::ast::Expr::BinaryOp { left, op, right }
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) =>
        {
            let left_q = qualified_column_alias(left)?;
            let right_q = qualified_column_alias(right)?;
            if left_q.eq_ignore_ascii_case(left_alias) && right_q.eq_ignore_ascii_case(right_alias) {
                out.push(JoinKeyPairShape {
                    left_expr: left.as_ref().clone(),
                    right_expr: right.as_ref().clone(),
                });
                Ok(())
            } else if left_q.eq_ignore_ascii_case(right_alias)
                && right_q.eq_ignore_ascii_case(left_alias)
            {
                out.push(JoinKeyPairShape {
                    left_expr: right.as_ref().clone(),
                    right_expr: left.as_ref().clone(),
                });
                Ok(())
            } else {
                Err("incremental join MV equi predicate must compare the two join aliases".to_string())
            }
        }
        _ => Err("incremental join MV supports only AND-combined equi-join predicates".to_string()),
    }
}

fn qualified_column_alias(expr: &sqlparser::ast::Expr) -> Result<String, String> {
    let sqlparser::ast::Expr::CompoundIdentifier(parts) = expr else {
        return Err("incremental join MV join key must be a qualified column reference".to_string());
    };
    let [alias, _column] = parts.as_slice() else {
        return Err("incremental join MV join key must be <alias>.<column>".to_string());
    };
    Ok(alias.value.clone())
}

fn is_probably_join_query(query: &sqlparser::ast::Query) -> bool {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.iter().any(|from| !from.joins.is_empty())
}

fn join_projection_filter_error() -> String {
    "incremental join MV supports only two-table inner equi-join projection/filter shapes".to_string()
}
```

- [ ] **Step 4: Run shape tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_shape::tests::join_projection_filter -- --nocapture
```

Expected: all join shape tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/mv_shape.rs
git commit -m "feat: classify iceberg join imv shape"
```

## Task 2: Multi-Base Contract Types

**Files:**
- Modify: `src/meta/repository/mv_contract.rs`

- [ ] **Step 1: Write failing contract tests**

Add tests that require two base contracts and join hidden key self-check:

```rust
#[test]
fn contract_v2_accepts_two_base_join_contract() {
    let contract = sample_join_contract();
    contract.ensure_self_consistent().expect("self check");
    assert_eq!(contract.contract_version, 2);
    assert_eq!(contract.bases.len(), 2);
    assert_eq!(
        contract.target.hidden_apply_key.source,
        ApplyKeySource::JoinRowKey
    );
}

#[test]
fn contract_v2_rejects_output_reference_to_unknown_base() {
    let mut contract = sample_join_contract();
    contract.output.columns[0]
        .expression
        .referenced_base_fields
        .push(QualifiedFieldLineage {
            table_fqn: "ice.ns.missing".to_string(),
            qualifier_at_create: "m".to_string(),
            field_id: 99,
        });
    let err = contract.ensure_self_consistent().expect_err("unknown base");
    assert!(err.to_string().contains("unknown base field"), "err={err}");
}

fn sample_join_contract() -> MvSchemaContract {
    MvSchemaContract {
        contract_version: 2,
        base: BaseContract {
            table_fqn: "ice.ns.left".to_string(),
            table_uuid: "left-uuid".to_string(),
            schema_id_at_create: 0,
            schema_at_create: BaseSchemaSnapshot { fields: vec![] },
        },
        bases: vec![
            BaseContract {
                table_fqn: "ice.ns.left".to_string(),
                table_uuid: "left-uuid".to_string(),
                alias_at_create: Some("l".to_string()),
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    }],
                },
            },
            BaseContract {
                table_fqn: "ice.ns.right".to_string(),
                table_uuid: "right-uuid".to_string(),
                alias_at_create: Some("r".to_string()),
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 2,
                        name_at_create: "id".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    }],
                },
            },
        ],
        output: OutputContract {
            columns: vec![OutputColumnLineage {
                expression: ExpressionLineage {
                    kind: ExpressionKind::Column,
                    referenced_base_field_ids: vec![],
                    referenced_base_fields: vec![QualifiedFieldLineage {
                        table_fqn: "ice.ns.left".to_string(),
                        qualifier_at_create: "l".to_string(),
                        field_id: 1,
                    }],
                },
            }],
            filter: None,
        },
        join: Some(JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates: vec![JoinPredicateLineage {
                left: QualifiedFieldLineage {
                    table_fqn: "ice.ns.left".to_string(),
                    qualifier_at_create: "l".to_string(),
                    field_id: 1,
                },
                right: QualifiedFieldLineage {
                    table_fqn: "ice.ns.right".to_string(),
                    qualifier_at_create: "r".to_string(),
                    field_id: 2,
                },
            }],
        }),
        target: TargetContract {
            table_fqn: "ice.ns.mv".to_string(),
            table_uuid: "target-uuid".to_string(),
            schema_id_at_create: 0,
            visible_columns: vec![TargetVisibleColumn {
                output_name: "id".to_string(),
                target_field_id: 1,
                type_signature: "long".to_string(),
                nullable: false,
            }],
            hidden_apply_key: HiddenApplyKeyContract {
                column_name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                target_field_id: 2,
                source: ApplyKeySource::JoinRowKey,
            },
        },
    }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test --lib meta::repository::mv_contract::tests::contract_v2 -- --nocapture
```

Expected: compile failure because `bases`, `join`, `QualifiedFieldLineage`, and `JoinRowKey` do not exist.

- [ ] **Step 3: Extend contract structs**

Update the structs in `src/meta/repository/mv_contract.rs`:

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
    pub target: TargetContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseContract {
    pub table_fqn: String,
    pub table_uuid: String,
    #[serde(default)]
    pub alias_at_create: Option<String>,
    pub schema_id_at_create: i32,
    pub schema_at_create: BaseSchemaSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualifiedFieldLineage {
    pub table_fqn: String,
    pub qualifier_at_create: String,
    pub field_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinContract {
    pub kind: JoinContractKind,
    pub predicates: Vec<JoinPredicateLineage>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JoinContractKind {
    InnerEquiJoin,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinPredicateLineage {
    pub left: QualifiedFieldLineage,
    pub right: QualifiedFieldLineage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpressionLineage {
    pub kind: ExpressionKind,
    pub referenced_base_field_ids: Vec<i32>,
    #[serde(default)]
    pub referenced_base_fields: Vec<QualifiedFieldLineage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterLineage {
    pub referenced_base_field_ids: Vec<i32>,
    #[serde(default)]
    pub referenced_base_fields: Vec<QualifiedFieldLineage>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplyKeySource {
    BaseRowId,
    JoinRowKey,
}

pub const HIDDEN_APPLY_KEY_COLUMN_NAME: &str = "__nova_base_row_id";
pub const JOIN_APPLY_KEY_COLUMN_NAME: &str = "__nova_join_row_key";
```

- [ ] **Step 4: Update self-check**

Add this helper and call it from `ensure_self_consistent()` after existing single-base checks:

```rust
impl MvSchemaContract {
    fn effective_bases(&self) -> Vec<&BaseContract> {
        if self.bases.is_empty() {
            vec![&self.base]
        } else {
            self.bases.iter().collect()
        }
    }
}

fn qualified_field_known(
    bases: &[&BaseContract],
    field: &QualifiedFieldLineage,
) -> bool {
    bases.iter().any(|base| {
        base.table_fqn == field.table_fqn
            && base
                .schema_at_create
                .fields
                .iter()
                .any(|record| record.field_id == field.field_id)
    })
}
```

Inside `ensure_self_consistent()` add:

```rust
let bases = self.effective_bases();
for (i, col) in self.output.columns.iter().enumerate() {
    for field in &col.expression.referenced_base_fields {
        if !qualified_field_known(&bases, field) {
            return Err(ContractSelfCheckError::OutputReferencesUnknownQualifiedBaseField {
                output_index: i,
                table_fqn: field.table_fqn.clone(),
                field_id: field.field_id,
            });
        }
    }
}
if let Some(filter) = &self.output.filter {
    for field in &filter.referenced_base_fields {
        if !qualified_field_known(&bases, field) {
            return Err(ContractSelfCheckError::FilterReferencesUnknownQualifiedBaseField {
                table_fqn: field.table_fqn.clone(),
                field_id: field.field_id,
            });
        }
    }
}
if let Some(join) = &self.join {
    for pred in &join.predicates {
        for field in [&pred.left, &pred.right] {
            if !qualified_field_known(&bases, field) {
                return Err(ContractSelfCheckError::JoinReferencesUnknownQualifiedBaseField {
                    table_fqn: field.table_fqn.clone(),
                    field_id: field.field_id,
                });
            }
        }
    }
}
if matches!(self.target.hidden_apply_key.source, ApplyKeySource::JoinRowKey)
    && self.target.hidden_apply_key.column_name != JOIN_APPLY_KEY_COLUMN_NAME
{
    return Err(ContractSelfCheckError::HiddenApplyKeyColumnNameWrong {
        expected: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
        actual: self.target.hidden_apply_key.column_name.clone(),
    });
}
```

Add the three new `ContractSelfCheckError` variants and `Display` arms:

```rust
OutputReferencesUnknownQualifiedBaseField {
    output_index: usize,
    table_fqn: String,
    field_id: i32,
},
FilterReferencesUnknownQualifiedBaseField {
    table_fqn: String,
    field_id: i32,
},
JoinReferencesUnknownQualifiedBaseField {
    table_fqn: String,
    field_id: i32,
},
```

- [ ] **Step 5: Run contract tests**

Run:

```bash
cargo test --lib meta::repository::mv_contract::tests -- --nocapture
```

Expected: all mv contract tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/meta/repository/mv_contract.rs
git commit -m "feat: extend mv schema contract for join imv"
```

## Task 3: Qualified Join Lineage

**Files:**
- Modify: `src/sql/analyzer/mv_lineage.rs`

- [ ] **Step 1: Write failing lineage tests**

Add tests that construct a resolved join query through the analyzer and require qualified lineage:

```rust
#[test]
fn join_lineage_distinguishes_same_named_columns_by_alias() {
    let sql = "select l.id as left_id, r.id as right_id \
               from ice.ns.left_tbl l join ice.ns.right_tbl r on l.id = r.id \
               where l.id > 0";
    let fixture = JoinLineageFixture::new();
    let resolved = fixture.analyze(sql);
    let result = build_join_projection_filter_lineage(
        &resolved,
        &[
            ("ice.ns.left_tbl", "l", &fixture.left_schema),
            ("ice.ns.right_tbl", "r", &fixture.right_schema),
        ],
    )
    .expect("join lineage");
    assert_eq!(result.output_columns.len(), 2);
    assert_eq!(
        result.output_columns[0].expression.referenced_base_fields[0].table_fqn,
        "ice.ns.left_tbl"
    );
    assert_eq!(
        result.output_columns[1].expression.referenced_base_fields[0].table_fqn,
        "ice.ns.right_tbl"
    );
    assert_eq!(result.join.as_ref().unwrap().predicates.len(), 1);
}
```

The fixture must register two `TableDef` entries whose aliases are `l` and `r`; do not replace this with a name-only unit test because the bug class is qualifier binding after analyzer resolution.

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test --lib sql::analyzer::mv_lineage::tests::join_lineage -- --nocapture
```

Expected: compile failure because `build_join_projection_filter_lineage` does not exist.

- [ ] **Step 3: Add lineage result types**

Extend `LineageResult`:

```rust
pub(crate) struct JoinLineageResult {
    pub base_fields_by_table:
        std::collections::BTreeMap<String, Vec<BaseFieldRecord>>,
    pub output_columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
    pub join: crate::meta::repository::mv_contract::JoinContract,
}
```

Add the public entry point:

```rust
pub(crate) fn build_join_projection_filter_lineage(
    resolved: &ResolvedQuery,
    base_schemas: &[(&str, &str, &iceberg::spec::Schema)],
) -> Result<JoinLineageResult, String> {
    let select = match &resolved.body {
        QueryBody::Select(s) => s,
        _ => return Err("join lineage builder requires a SELECT query".to_string()),
    };
    let join = match select.from.as_ref() {
        Some(Relation::Join(join)) => join,
        Some(_) => return Err("join lineage builder requires a join relation".to_string()),
        None => return Err("join lineage builder requires a FROM clause".to_string()),
    };
    let mut collector = QualifiedLineageCollector::new(base_schemas);
    let output_columns = select
        .projection
        .iter()
        .map(|item| collector.output_lineage(&item.expr))
        .collect::<Result<Vec<_>, _>>()?;
    let filter = select
        .filter
        .as_ref()
        .map(|expr| collector.filter_lineage(expr))
        .transpose()?;
    let join_contract = collector.join_contract(join)?;
    Ok(JoinLineageResult {
        base_fields_by_table: collector.into_base_fields_by_table(),
        output_columns,
        filter,
        join: join_contract,
    })
}
```

- [ ] **Step 4: Implement qualified collector**

Add a local collector that resolves `ExprKind::ColumnRef { qualifier, column }` by qualifier:

```rust
struct QualifiedLineageCollector<'a> {
    schemas: std::collections::BTreeMap<String, (&'a str, &'a iceberg::spec::Schema)>,
    base_fields_by_table: std::collections::BTreeMap<String, std::collections::BTreeMap<i32, BaseFieldRecord>>,
}

impl<'a> QualifiedLineageCollector<'a> {
    fn new(base_schemas: &[(&'a str, &'a str, &'a iceberg::spec::Schema)]) -> Self {
        let mut schemas = std::collections::BTreeMap::new();
        for (table_fqn, alias, schema) in base_schemas {
            schemas.insert(alias.to_ascii_lowercase(), (*table_fqn, *schema));
        }
        Self {
            schemas,
            base_fields_by_table: std::collections::BTreeMap::new(),
        }
    }

    fn output_lineage(&mut self, expr: &TypedExpr) -> Result<OutputColumnLineage, String> {
        let mut refs = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        self.collect_qualified_refs(expr, &mut refs, &mut kind_hint)?;
        Ok(OutputColumnLineage {
            expression: ExpressionLineage {
                kind: kind_hint.into_kind(),
                referenced_base_field_ids: Vec::new(),
                referenced_base_fields: refs,
            },
        })
    }

    fn filter_lineage(&mut self, expr: &TypedExpr) -> Result<FilterLineage, String> {
        let mut refs = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        self.collect_qualified_refs(expr, &mut refs, &mut kind_hint)?;
        let _ = kind_hint;
        Ok(FilterLineage {
            referenced_base_field_ids: Vec::new(),
            referenced_base_fields: refs,
        })
    }

    fn collect_qualified_refs(
        &mut self,
        expr: &TypedExpr,
        out: &mut Vec<QualifiedFieldLineage>,
        kind: &mut ExpressionKindHint,
    ) -> Result<(), String> {
        match &expr.kind {
            ExprKind::ColumnRef { qualifier, column } => {
                kind.saw_column();
                let qualifier = qualifier.as_ref().ok_or_else(|| {
                    format!("join MV column `{column}` must be qualified")
                })?;
                let resolved = self.resolve_field(qualifier, column)?;
                out.push(resolved);
                Ok(())
            }
            _ => {
                collect_column_refs(expr, &mut Vec::new(), kind);
                for child in typed_expr_children(expr) {
                    self.collect_qualified_refs(child, out, kind)?;
                }
                out.sort_by(|a, b| {
                    (a.table_fqn.as_str(), a.field_id).cmp(&(b.table_fqn.as_str(), b.field_id))
                });
                out.dedup_by(|a, b| a.table_fqn == b.table_fqn && a.field_id == b.field_id);
                Ok(())
            }
        }
    }

    fn resolve_field(
        &mut self,
        qualifier: &str,
        column: &str,
    ) -> Result<QualifiedFieldLineage, String> {
        let key = qualifier.to_ascii_lowercase();
        let (table_fqn, schema) = self.schemas.get(&key).ok_or_else(|| {
            format!("join MV qualifier `{qualifier}` does not match a base table alias")
        })?;
        let field = resolve_field(schema, column)?;
        self.base_fields_by_table
            .entry((*table_fqn).to_string())
            .or_default()
            .entry(field.id)
            .or_insert_with(|| BaseFieldRecord {
                field_id: field.id,
                name_at_create: field.name.clone(),
                type_signature: format!("{}", field.field_type),
                required: field.required,
            });
        Ok(QualifiedFieldLineage {
            table_fqn: (*table_fqn).to_string(),
            qualifier_at_create: qualifier.to_string(),
            field_id: field.id,
        })
    }

    fn join_contract(&mut self, join: &JoinRelation) -> Result<JoinContract, String> {
        let condition = join
            .condition
            .as_ref()
            .ok_or_else(|| "join MV requires ON condition".to_string())?;
        let mut predicates = Vec::new();
        self.collect_join_predicates(condition, &mut predicates)?;
        Ok(JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates,
        })
    }

    fn into_base_fields_by_table(self) -> std::collections::BTreeMap<String, Vec<BaseFieldRecord>> {
        self.base_fields_by_table
            .into_iter()
            .map(|(table, fields)| (table, fields.into_values().collect()))
            .collect()
    }
}
```

Add these helpers next to `QualifiedLineageCollector`:

```rust
fn typed_expr_children(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::Lambda { body: expr, .. } => vec![expr.as_ref()],
        ExprKind::FunctionCall { args, .. }
        | ExprKind::AggregateCall { args, .. }
        | ExprKind::WindowCall { args, .. } => args.iter().collect(),
        ExprKind::LambdaFunction { body, .. } => vec![body.as_ref()],
        ExprKind::InList { expr, list, .. } => {
            let mut out = Vec::with_capacity(1 + list.len());
            out.push(expr.as_ref());
            out.extend(list.iter());
            out
        }
        ExprKind::Between {
            expr, low, high, ..
        } => vec![expr.as_ref(), low.as_ref(), high.as_ref()],
        ExprKind::Like { expr, pattern, .. } => vec![expr.as_ref(), pattern.as_ref()],
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut out = Vec::new();
            if let Some(operand) = operand {
                out.push(operand.as_ref());
            }
            for (when, then) in when_then {
                out.push(when);
                out.push(then);
            }
            if let Some(else_expr) = else_expr {
                out.push(else_expr.as_ref());
            }
            out
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => Vec::new(),
    }
}

impl<'a> QualifiedLineageCollector<'a> {
    fn collect_join_predicates(
        &mut self,
        expr: &TypedExpr,
        out: &mut Vec<JoinPredicateLineage>,
    ) -> Result<(), String> {
        match &expr.kind {
            ExprKind::BinaryOp {
                left,
                op: crate::sql::analysis::BinOp::And,
                right,
            } => {
                self.collect_join_predicates(left, out)?;
                self.collect_join_predicates(right, out)
            }
            ExprKind::BinaryOp {
                left,
                op: crate::sql::analysis::BinOp::Eq,
                right,
            } => {
                let left_ref = self.single_qualified_column(left)?;
                let right_ref = self.single_qualified_column(right)?;
                out.push(JoinPredicateLineage {
                    left: left_ref,
                    right: right_ref,
                });
                Ok(())
            }
            _ => Err(
                "incremental join MV supports only AND-combined equi-join predicates"
                    .to_string(),
            ),
        }
    }

    fn single_qualified_column(&mut self, expr: &TypedExpr) -> Result<QualifiedFieldLineage, String> {
        let ExprKind::ColumnRef { qualifier, column } = &expr.kind else {
            return Err(
                "incremental join MV join key must be a qualified column reference".to_string(),
            );
        };
        let qualifier = qualifier.as_ref().ok_or_else(|| {
            "incremental join MV join key must be <alias>.<column>".to_string()
        })?;
        self.resolve_field(qualifier, column)
    }
}
```

- [ ] **Step 5: Run lineage tests**

Run:

```bash
cargo test --lib sql::analyzer::mv_lineage::tests -- --nocapture
```

Expected: lineage tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/sql/analyzer/mv_lineage.rs
git commit -m "feat: collect qualified join mv lineage"
```

## Task 4: Branch Planner

**Files:**
- Create: `src/engine/mv/iceberg_join_branch.rs`
- Modify: `src/engine/mv/mod.rs`

- [ ] **Step 1: Write branch planner tests**

Create `src/engine/mv/iceberg_join_branch.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn base(name: &str) -> crate::connector::starrocks::managed::model::IcebergTableRef {
        crate::connector::starrocks::managed::model::IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: name.to_string(),
        }
    }

    #[test]
    fn both_changed_uses_telescoping_order() {
        let left = base("left");
        let right = base("right");
        let plans = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].left, BranchSide::Delta(SnapshotWindow { from: 10, to: 11 }));
        assert_eq!(plans[0].right, BranchSide::Snapshot(20));
        assert_eq!(plans[1].left, BranchSide::Snapshot(11));
        assert_eq!(plans[1].right, BranchSide::Delta(SnapshotWindow { from: 20, to: 21 }));
    }

    #[test]
    fn only_left_changed_has_one_branch() {
        let left = base("left");
        let right = base("right");
        let plans = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 20 },
            true,
            false,
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].left, BranchSide::Delta(SnapshotWindow { from: 10, to: 11 }));
        assert_eq!(plans[0].right, BranchSide::Snapshot(20));
    }
}
```

- [ ] **Step 2: Implement branch planner**

Add this module content above the tests:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotWindow {
    pub(crate) from: i64,
    pub(crate) to: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BranchSide {
    Delta(SnapshotWindow),
    Snapshot(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinDeltaBranchPlan {
    pub(crate) left_base: crate::connector::starrocks::managed::model::IcebergTableRef,
    pub(crate) right_base: crate::connector::starrocks::managed::model::IcebergTableRef,
    pub(crate) left: BranchSide,
    pub(crate) right: BranchSide,
}

pub(crate) fn plan_join_delta_branches(
    left_base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    right_base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    left_window: SnapshotWindow,
    right_window: SnapshotWindow,
    left_has_changes: bool,
    right_has_changes: bool,
) -> Vec<JoinDeltaBranchPlan> {
    let mut plans = Vec::new();
    if left_has_changes {
        plans.push(JoinDeltaBranchPlan {
            left_base: left_base.clone(),
            right_base: right_base.clone(),
            left: BranchSide::Delta(left_window),
            right: BranchSide::Snapshot(right_window.from),
        });
    }
    if right_has_changes {
        plans.push(JoinDeltaBranchPlan {
            left_base: left_base.clone(),
            right_base: right_base.clone(),
            left: BranchSide::Snapshot(left_window.to),
            right: BranchSide::Delta(right_window),
        });
    }
    plans
}
```

Register the module in `src/engine/mv/mod.rs`:

```rust
pub(crate) mod iceberg_join_branch;
```

- [ ] **Step 3: Run branch planner tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_join_branch::tests -- --nocapture
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/mod.rs src/engine/mv/iceberg_join_branch.rs
git commit -m "feat: plan telescoping join mv delta branches"
```

## Task 5: Join Branch AST Rewrite

**Files:**
- Modify: `src/engine/mv/iceberg_join_branch.rs`

- [ ] **Step 1: Write AST rewrite tests**

Add tests:

```rust
#[test]
fn branch_rewrite_delta_left_snapshot_right() {
    let query = parse_query(
        "select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
    );
    let left = base("left");
    let right = base("right");
    let plan = JoinDeltaBranchPlan {
        left_base: left,
        right_base: right,
        left: BranchSide::Delta(SnapshotWindow { from: 10, to: 11 }),
        right: BranchSide::Snapshot(20),
    };
    let rewritten = rewrite_join_branch_query(&query, &plan, "l", "r").expect("rewrite");
    let rendered = rewritten.to_string();
    assert!(rendered.contains("__nr_ivm_delta"), "sql={rendered}");
    assert!(rendered.contains("right__at_20"), "sql={rendered}");
    assert!(rendered.contains("__nova_left_row_id"), "sql={rendered}");
    assert!(rendered.contains("__nova_right_row_id"), "sql={rendered}");
}
```

Add helper:

```rust
fn parse_query(sql: &str) -> sqlparser::ast::Query {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
    let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
    let sqlparser::ast::Statement::Query(query) = stmt else {
        panic!("expected query");
    };
    *query
}
```

- [ ] **Step 2: Implement rewrite function**

Add:

```rust
pub(crate) const JOIN_LEFT_ROW_ID_COLUMN: &str = "__nova_left_row_id";
pub(crate) const JOIN_RIGHT_ROW_ID_COLUMN: &str = "__nova_right_row_id";

pub(crate) fn rewrite_join_branch_query(
    query: &sqlparser::ast::Query,
    plan: &JoinDeltaBranchPlan,
    left_alias: &str,
    right_alias: &str,
) -> Result<sqlparser::ast::Query, String> {
    let mut query = query.clone();
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("join branch rewrite requires SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join branch rewrite requires one FROM item".to_string());
    };
    let [join] = from.joins.as_mut_slice() else {
        return Err("join branch rewrite requires one JOIN".to_string());
    };
    rewrite_branch_factor(
        &mut from.relation,
        &plan.left_base,
        plan.left,
        left_alias,
    )?;
    rewrite_branch_factor(
        &mut join.relation,
        &plan.right_base,
        plan.right,
        right_alias,
    )?;
    append_join_hidden_projection(select, left_alias, right_alias);
    Ok(query)
}

fn rewrite_branch_factor(
    factor: &mut sqlparser::ast::TableFactor,
    base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    side: BranchSide,
    alias: &str,
) -> Result<(), String> {
    match side {
        BranchSide::Delta(window) => {
            *factor = build_nr_ivm_delta_table_factor_for_join(base, window, alias);
        }
        BranchSide::Snapshot(snapshot_id) => {
            let sqlparser::ast::TableFactor::Table {
                name,
                version,
                alias: factor_alias,
                ..
            } = factor else {
                return Err("join branch snapshot side must be a table".to_string());
            };
            *name = snapshot_table_object_name(base, snapshot_id);
            *version = None;
            if factor_alias.is_none() {
                *factor_alias = Some(sqlparser::ast::TableAlias {
                    explicit: true,
                    name: sqlparser::ast::Ident::new(alias),
                    columns: Vec::new(),
                });
            }
        }
    }
    Ok(())
}

fn snapshot_table_object_name(
    base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    snapshot_id: i64,
) -> sqlparser::ast::ObjectName {
    sqlparser::ast::ObjectName(vec![
        sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(&base.namespace)),
        sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(format!(
            "{}__at_{}",
            base.table, snapshot_id
        ))),
    ])
}

fn build_nr_ivm_delta_table_factor_for_join(
    base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    window: SnapshotWindow,
    alias: &str,
) -> sqlparser::ast::TableFactor {
    use sqlparser::ast as sqlast;
    let make_string_arg = |s: String| -> sqlast::FunctionArg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(sqlast::Expr::Value(
            sqlast::Value::SingleQuotedString(s).into(),
        )))
    };
    let make_number_arg = |n: i64| -> sqlast::FunctionArg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(sqlast::Expr::Value(
            sqlast::Value::Number(n.to_string(), false).into(),
        )))
    };
    sqlast::TableFactor::Table {
        name: sqlast::ObjectName(vec![sqlast::ObjectNamePart::Identifier(
            sqlast::Ident::new("__nr_ivm_delta"),
        )]),
        alias: Some(sqlast::TableAlias {
            explicit: true,
            name: sqlast::Ident::new(alias),
            columns: Vec::new(),
        }),
        args: Some(sqlast::TableFunctionArgs {
            args: vec![
                make_string_arg(base.fqn()),
                make_number_arg(window.from),
                make_number_arg(window.to),
            ],
            settings: None,
        }),
        with_hints: Vec::new(),
        version: None,
        with_ordinality: false,
        partitions: Vec::new(),
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    }
}

fn append_join_hidden_projection(
    select: &mut sqlparser::ast::Select,
    left_alias: &str,
    right_alias: &str,
) {
    select.projection.push(sqlparser::ast::SelectItem::UnnamedExpr(
        sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new(
            crate::exec::change_op::CHANGE_OP_COLUMN,
        )),
    ));
    select.projection.push(row_id_alias(left_alias, JOIN_LEFT_ROW_ID_COLUMN));
    select.projection.push(row_id_alias(right_alias, JOIN_RIGHT_ROW_ID_COLUMN));
}

fn row_id_alias(alias: &str, output: &str) -> sqlparser::ast::SelectItem {
    sqlparser::ast::SelectItem::ExprWithAlias {
        expr: sqlparser::ast::Expr::CompoundIdentifier(vec![
            sqlparser::ast::Ident::new(alias),
            sqlparser::ast::Ident::new("_row_id"),
        ]),
        alias: sqlparser::ast::Ident::new(output),
    }
}
```

- [ ] **Step 3: Run rewrite tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_join_branch::tests::branch_rewrite -- --nocapture
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/iceberg_join_branch.rs
git commit -m "feat: rewrite join mv delta branch queries"
```

## Task 6: Join Delta Coalescer

**Files:**
- Create: `src/engine/mv/iceberg_join_coalesce.rs`
- Modify: `src/engine/mv/mod.rs`

- [ ] **Step 1: Write coalesce unit tests**

Create the file with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int8Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch(op: i8, left: i64, right: i64, value: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new(crate::exec::change_op::CHANGE_OP_COLUMN, DataType::Int8, false),
                Field::new(crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN, DataType::Int64, false),
                Field::new(crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![value])),
                Arc::new(Int8Array::from(vec![op])),
                Arc::new(Int64Array::from(vec![left])),
                Arc::new(Int64Array::from(vec![right])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn coalescer_cancels_insert_and_delete() {
        let coalescer = JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer.push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a")).unwrap();
        coalescer.push_batch(batch(crate::exec::change_op::CHANGE_OP_DELETE, 1, 2, "a")).unwrap();
        let rows = coalescer.finish_for_test().unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn coalescer_rejects_abs_net_greater_than_one() {
        let coalescer = JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer.push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a")).unwrap();
        coalescer.push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a")).unwrap();
        let err = coalescer.finish_for_test().expect_err("net > 1");
        assert!(err.contains("net change_op"), "err={err}");
    }
}
```

- [ ] **Step 2: Implement coalescer core**

Add:

```rust
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int8Array, Int64Array, UInt32Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug)]
struct CoalescedRow {
    net: i32,
    payload: Option<RecordBatch>,
}

pub(crate) struct JoinDeltaCoalescer {
    left_table_uuid: String,
    right_table_uuid: String,
    max_keys: usize,
    rows: Mutex<BTreeMap<String, CoalescedRow>>,
}

impl JoinDeltaCoalescer {
    pub(crate) fn new(left_table_uuid: String, right_table_uuid: String, max_keys: usize) -> Arc<Self> {
        Arc::new(Self {
            left_table_uuid,
            right_table_uuid,
            max_keys,
            rows: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) fn push_batch(&self, batch: RecordBatch) -> Result<(), String> {
        let op_idx = batch.schema().index_of(crate::exec::change_op::CHANGE_OP_COLUMN)
            .map_err(|_| "join coalesce batch missing __change_op".to_string())?;
        let left_idx = batch.schema().index_of(crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN)
            .map_err(|_| "join coalesce batch missing left row id".to_string())?;
        let right_idx = batch.schema().index_of(crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN)
            .map_err(|_| "join coalesce batch missing right row id".to_string())?;
        let ops = batch.column(op_idx).as_any().downcast_ref::<Int8Array>()
            .ok_or_else(|| "join coalesce __change_op must be Int8".to_string())?;
        let left_ids = batch.column(left_idx).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| "join coalesce left row id must be Int64".to_string())?;
        let right_ids = batch.column(right_idx).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| "join coalesce right row id must be Int64".to_string())?;

        let mut rows = self.rows.lock().expect("join coalescer lock");
        for row in 0..batch.num_rows() {
            let op = ops.value(row);
            let delta = match op {
                crate::exec::change_op::CHANGE_OP_INSERT => 1,
                crate::exec::change_op::CHANGE_OP_DELETE => -1,
                other => return Err(format!("join coalesce unexpected __change_op {other}")),
            };
            let key = stable_join_row_key(
                &self.left_table_uuid,
                left_ids.value(row),
                &self.right_table_uuid,
                right_ids.value(row),
            );
            let payload = take_one_row_without_hidden_columns(&batch, row)?;
            let entry = rows.entry(key).or_insert(CoalescedRow { net: 0, payload: None });
            entry.net += delta;
            if delta > 0 {
                if let Some(existing) = &entry.payload
                    && !record_batch_single_row_equal(existing, &payload)?
                {
                    return Err("join coalesce payload mismatch for the same join row key".to_string());
                }
                entry.payload = Some(payload);
            }
            if rows.len() > self.max_keys {
                return Err(format!(
                    "join coalesce exceeded max key budget {}; use full refresh or split the delta",
                    self.max_keys
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn finish_for_test(&self) -> Result<Vec<(String, i32)>, String> {
        let rows = self.rows.lock().expect("join coalescer lock");
        let mut out = Vec::new();
        for (key, row) in rows.iter() {
            if row.net.abs() > 1 {
                return Err(format!("join coalesce net change_op {} for key {key}", row.net));
            }
            if row.net != 0 {
                out.push((key.clone(), row.net));
            }
        }
        Ok(out)
    }
}

pub(crate) fn stable_join_row_key(
    left_uuid: &str,
    left_row_id: i64,
    right_uuid: &str,
    right_row_id: i64,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(left_uuid.as_bytes());
    hasher.update([0]);
    hasher.update(left_row_id.to_be_bytes());
    hasher.update([0]);
    hasher.update(right_uuid.as_bytes());
    hasher.update([0]);
    hasher.update(right_row_id.to_be_bytes());
    let digest = hasher.finalize();
    format!("v1:{}", hex::encode(&digest[..16]))
}

fn take_one_row_without_hidden_columns(batch: &RecordBatch, row: usize) -> Result<RecordBatch, String> {
    let hidden = [
        crate::exec::change_op::CHANGE_OP_COLUMN,
        crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN,
        crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN,
    ];
    let row_u32 = u32::try_from(row).map_err(|_| format!("row index {row} exceeds u32"))?;
    let indices = UInt32Array::from(vec![row_u32]);
    let schema = batch.schema();
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if hidden.iter().any(|name| field.name().eq_ignore_ascii_case(name)) {
            continue;
        }
        fields.push(field.as_ref().clone());
        let taken = arrow::compute::take(batch.column(idx).as_ref(), &indices, None)
            .map_err(|e| format!("join coalesce take one row: {e}"))?;
        columns.push(taken);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("join coalesce rebuild one-row batch: {e}"))
}

fn record_batch_single_row_equal(left: &RecordBatch, right: &RecordBatch) -> Result<bool, String> {
    if left.num_rows() != 1 || right.num_rows() != 1 {
        return Err("join coalesce payload comparison requires single-row batches".to_string());
    }
    if left.schema() != right.schema() {
        return Ok(false);
    }
    for idx in 0..left.num_columns() {
        if !arrow::array::equal(left.column(idx).as_ref(), right.column(idx).as_ref()) {
            return Ok(false);
        }
    }
    Ok(true)
}
```

`sha2 = "0.10"` and `hex = "0.4"` are already present in the workspace `Cargo.toml`; do not add new hashing dependencies for this task.

- [ ] **Step 3: Implement sink factory**

Add a terminal sink that only feeds chunks into the shared coalescer:

```rust
pub(crate) struct IcebergJoinCoalesceSinkFactory {
    name: String,
    coalescer: Arc<JoinDeltaCoalescer>,
}

impl IcebergJoinCoalesceSinkFactory {
    pub(crate) fn new(coalescer: Arc<JoinDeltaCoalescer>) -> Self {
        Self {
            name: "IcebergJoinCoalesceSink".to_string(),
            coalescer,
        }
    }
}

impl crate::exec::pipeline::operator_factory::OperatorFactory for IcebergJoinCoalesceSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn crate::exec::pipeline::operator::Operator> {
        Box::new(IcebergJoinCoalesceSinkOperator {
            name: self.name.clone(),
            coalescer: Arc::clone(&self.coalescer),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct IcebergJoinCoalesceSinkOperator {
    name: String,
    coalescer: Arc<JoinDeltaCoalescer>,
    finished: bool,
}

impl crate::exec::pipeline::operator::Operator for IcebergJoinCoalesceSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn crate::exec::pipeline::operator::ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn crate::exec::pipeline::operator::ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl crate::exec::pipeline::operator::ProcessorOperator for IcebergJoinCoalesceSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(
        &mut self,
        _state: &crate::runtime::runtime_state::RuntimeState,
        chunk: crate::exec::chunk::Chunk,
    ) -> Result<(), String> {
        self.coalescer.push_batch(chunk.batch)
    }

    fn pull_chunk(
        &mut self,
        _state: &crate::runtime::runtime_state::RuntimeState,
    ) -> Result<Option<crate::exec::chunk::Chunk>, String> {
        Err("join coalesce sink does not produce output".to_string())
    }

    fn set_finishing(&mut self, _state: &crate::runtime::runtime_state::RuntimeState) -> Result<(), String> {
        self.finished = true;
        Ok(())
    }
}
```

The target-commit flush API is added in Task 8 after the string apply-key locator exists, so this task remains independently buildable.

- [ ] **Step 4: Register module and run tests**

Register in `src/engine/mv/mod.rs`:

```rust
pub(crate) mod iceberg_join_coalesce;
```

Run:

```bash
cargo test --lib engine::mv::iceberg_join_coalesce::tests -- --nocapture
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/mod.rs src/engine/mv/iceberg_join_coalesce.rs
git commit -m "feat: coalesce join mv row deltas"
```

## Task 7: CREATE Path Integration

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add apply-key constants and helper**

In `iceberg_target_apply.rs`, add:

```rust
pub(crate) const ICEBERG_MV_JOIN_APPLY_KEY_COLUMN: &str = "__nova_join_row_key";
pub(crate) const ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY: &str = "JoinRowKey";

pub(crate) fn join_apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: ICEBERG_MV_JOIN_APPLY_KEY_COLUMN.to_string(),
        data_type: crate::sql::parser::ast::SqlType::String,
        nullable: false,
        aggregation: None,
        default: None,
    }
}
```

- [ ] **Step 2: Add string apply-key target locator**

In `iceberg_target_apply.rs`, add a string variant next to the existing `locate_target_rows_by_apply_key`. It is intentionally separate so the single-base `BIGINT` path remains unchanged:

```rust
pub(crate) async fn locate_target_rows_by_apply_key_string(
    target_table: &iceberg::table::Table,
    join_row_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    use arrow::array::{Array, Int64Array, StringArray};
    use futures::StreamExt;
    use iceberg::arrow::ArrowReaderBuilder;

    if join_row_keys.is_empty() {
        return Ok(Vec::new());
    }

    let requested = join_row_keys.iter().cloned().collect::<std::collections::HashSet<_>>();
    let scan = target_table
        .scan()
        .select(vec![
            "_file".to_string(),
            "_pos".to_string(),
            ICEBERG_MV_JOIN_APPLY_KEY_COLUMN.to_string(),
        ])
        .build()
        .map_err(|e| format!("build iceberg join MV target locator scan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("plan iceberg join MV target locator files failed: {e}"))?;
    let cleaned_tasks = task_stream.map(|task_result| {
        task_result.map(|mut task| {
            task.deletes.clear();
            task.predicate = None;
            task
        })
    });
    let arrow_reader = ArrowReaderBuilder::new(target_table.file_io().clone())
        .with_row_group_filtering_enabled(false)
        .with_row_selection_enabled(false)
        .build();
    let mut stream = arrow_reader
        .read(Box::pin(cleaned_tasks))
        .map_err(|e| format!("read iceberg join MV target locator scan failed: {e}"))?;

    let mut matches = std::collections::HashMap::<String, (String, i64)>::new();
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| format!("iceberg join MV target locator scan error: {e}"))?;
        let schema = batch.schema();
        let file_idx = schema
            .index_of("_file")
            .map_err(|e| format!("iceberg join MV target locator scan missing _file: {e}"))?;
        let pos_idx = schema
            .index_of("_pos")
            .map_err(|e| format!("iceberg join MV target locator scan missing _pos: {e}"))?;
        let key_idx = schema.index_of(ICEBERG_MV_JOIN_APPLY_KEY_COLUMN).map_err(|e| {
            format!("iceberg join MV target locator scan missing {ICEBERG_MV_JOIN_APPLY_KEY_COLUMN}: {e}")
        })?;
        let file_col =
            arrow::compute::cast(batch.column(file_idx), &arrow::datatypes::DataType::Utf8)
                .map_err(|e| format!("cast join target _file to STRING failed: {e}"))?;
        let pos_col =
            arrow::compute::cast(batch.column(pos_idx), &arrow::datatypes::DataType::Int64)
                .map_err(|e| format!("cast join target _pos to BIGINT failed: {e}"))?;
        let key_col =
            arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Utf8)
                .map_err(|e| {
                    format!("cast join target {ICEBERG_MV_JOIN_APPLY_KEY_COLUMN} to STRING failed: {e}")
                })?;
        let files = file_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "join target _file is not STRING after cast".to_string())?;
        let positions = pos_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "join target _pos is not BIGINT after cast".to_string())?;
        let keys = key_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!("join target {ICEBERG_MV_JOIN_APPLY_KEY_COLUMN} is not STRING after cast")
            })?;
        for row in 0..batch.num_rows() {
            if files.is_null(row) || positions.is_null(row) || keys.is_null(row) {
                continue;
            }
            let key = keys.value(row);
            if !requested.contains(key) {
                continue;
            }
            let file = files.value(row);
            let pos = positions.value(row);
            if !crate::engine::delete_flow::data_file_row_is_visible(
                &batch,
                row,
                file,
                pos,
                existing_deletes_by_file,
            )? {
                continue;
            }
            if matches.insert(key.to_string(), (file.to_string(), pos)).is_some() {
                return Err(format!("iceberg join MV target has duplicate rows for join row key {key}"));
            }
        }
    }

    for key in &requested {
        if !matches.contains_key(key) {
            return Err(format!("iceberg join MV target row not found for join row key {key}"));
        }
    }

    let mut by_file = std::collections::BTreeMap::<String, Vec<i64>>::new();
    for (_key, (file, pos)) in matches {
        by_file.entry(file).or_default().push(pos);
    }
    by_file
        .into_iter()
        .map(|(referenced_data_file, mut positions)| {
            positions.sort_unstable();
            let partition = referenced_data_file_partitions
                .get(&referenced_data_file)
                .ok_or_else(|| {
                    format!(
                        "matched iceberg join MV target data file `{referenced_data_file}` is missing partition metadata"
                    )
                })?;
            Ok(crate::connector::iceberg::commit::PositionDeleteGroup {
                referenced_data_file,
                partition_spec_id: partition.partition_spec_id,
                partition_values: partition.partition_values.clone(),
                positions,
            })
        })
        .collect()
}
```

- [ ] **Step 3: Write failing CREATE-path test**

In `iceberg_refresh.rs` tests, add a focused unit around target column selection:

```rust
#[test]
fn iceberg_join_mv_uses_join_apply_key_column() {
    let column = crate::engine::mv::iceberg_target_apply::join_apply_key_table_column();
    assert_eq!(
        column.name,
        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN
    );
}
```

- [ ] **Step 4: Modify `create_iceberg_mv` shape dispatch**

In `create_iceberg_mv`, replace the current projection-only gate with:

```rust
let shape = classify_incremental_mv_query(&canonical_select_query)?;
match &shape {
    IncrementalMvShape::ProjectionFilter(_) => {
        let [base_ref] = base_refs.as_slice() else {
            return Err(
                "iceberg-backed projection/filter materialized views require exactly one iceberg base table"
                    .to_string(),
            );
        };
        let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
        ensure_base_row_lineage_contract(&loaded_base.table, &base_ref.fqn())?;
    }
    IncrementalMvShape::JoinProjectionFilter(join_shape) => {
        if base_refs.len() != 2 {
            return Err(
                "iceberg-backed join materialized views require exactly two iceberg base tables"
                    .to_string(),
            );
        }
        for base_ref in &base_refs {
            let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
            ensure_base_row_lineage_contract(&loaded_base.table, &base_ref.fqn())?;
        }
        validate_join_shape_base_refs(join_shape, &base_refs)?;
    }
    IncrementalMvShape::Aggregate(_) => {
        return Err(
            "iceberg-backed materialized views do not support aggregate shapes in this phase"
                .to_string(),
        );
    }
}
```

Add helper:

```rust
fn validate_join_shape_base_refs(
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
    base_refs: &[IcebergTableRef],
) -> Result<(), String> {
    let expected = [
        shape.left_table.to_string().to_ascii_lowercase(),
        shape.right_table.to_string().to_ascii_lowercase(),
    ];
    for name in expected {
        if !base_refs.iter().any(|base| base.fqn().eq_ignore_ascii_case(&name)) {
            return Err(format!("join MV shape references base {name} but analyzer resolved {base_refs:?}"));
        }
    }
    Ok(())
}
```

When building target columns, choose the hidden key by shape:

```rust
let hidden_apply_key_column = match &shape {
    IncrementalMvShape::JoinProjectionFilter(_) => join_apply_key_table_column(),
    _ => apply_key_table_column(),
};
columns.push(hidden_apply_key_column);
```

Set target properties:

```rust
let apply_key_column_name = match &shape {
    IncrementalMvShape::JoinProjectionFilter(_) => ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
    _ => ICEBERG_MV_APPLY_KEY_COLUMN,
};
let apply_key_source = match &shape {
    IncrementalMvShape::JoinProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY,
    _ => ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID,
};
```

- [ ] **Step 5: Run focused CREATE tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::iceberg_join_mv_uses_join_apply_key_column -- --nocapture
cargo test --lib connector::starrocks::managed::mv_shape::tests -- --nocapture
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/iceberg_target_apply.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat: create iceberg join imv target contract"
```

## Task 8: REFRESH Path Integration

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/iceberg_join_coalesce.rs`

- [ ] **Step 1: Add refresh dispatcher test seam**

Add a pure helper in `iceberg_refresh.rs`:

```rust
fn is_join_projection_filter_mv(shape: &IncrementalMvShape) -> bool {
    matches!(shape, IncrementalMvShape::JoinProjectionFilter(_))
}
```

Add test:

```rust
#[test]
fn refresh_dispatch_identifies_join_shape() {
    let query = crate::sql::parser::parse_sql(
        "select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id",
    )
    .expect("parse")
    .remove(0);
    let crate::sql::parser::ast::Statement::Query(query) = query else {
        panic!("query");
    };
    let shape = classify_incremental_mv_query(&query).expect("shape");
    assert!(is_join_projection_filter_mv(&shape));
}
```

- [ ] **Step 2: Implement join refresh planning entry**

Add a private planning entry that performs all snapshot and change-batch decisions before any branch query runs:

```rust
#[allow(clippy::too_many_arguments)]
fn incremental_refresh_iceberg_join_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    expected_main_snapshot_id: Option<i64>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
    target_table: &iceberg::table::Table,
) -> Result<StatementResult, String> {
    if base_refs.len() != 2 {
        return Err("iceberg join MV refresh requires exactly two base tables".to_string());
    }
    let left_ref = &base_refs[0];
    let right_ref = &base_refs[1];
    let left_to = pin.get(left_ref).ok_or_else(|| format!("missing pin for {}", left_ref.fqn()))?;
    let right_to = pin.get(right_ref).ok_or_else(|| format!("missing pin for {}", right_ref.fqn()))?;
    let left_from = mv_definition.last_refresh_snapshots.get(&left_ref.fqn()).copied()
        .ok_or_else(|| format!("join MV {} missing previous snapshot for {}", mv_definition.mv_id, left_ref.fqn()))?;
    let right_from = mv_definition.last_refresh_snapshots.get(&right_ref.fqn()).copied()
        .ok_or_else(|| format!("join MV {} missing previous snapshot for {}", mv_definition.mv_id, right_ref.fqn()))?;
    let left_loaded = load_current_iceberg_base_table(state, left_ref)?;
    let right_loaded = load_current_iceberg_base_table(state, right_ref)?;
    let left_batch = plan_changes(&left_loaded.table, left_from, Some(left_to), &[])
        .map_err(|e| format!("join MV left change planning failed: {e}"))?;
    let right_batch = plan_changes(&right_loaded.table, right_from, Some(right_to), &[])
        .map_err(|e| format!("join MV right change planning failed: {e}"))?;
    let left_has_changes =
        !left_batch.inserts.is_empty() || iceberg_change_batch_has_row_deletes(&left_batch);
    let right_has_changes =
        !right_batch.inserts.is_empty() || iceberg_change_batch_has_row_deletes(&right_batch);
    let branches = crate::engine::mv::iceberg_join_branch::plan_join_delta_branches(
        left_ref,
        right_ref,
        crate::engine::mv::iceberg_join_branch::SnapshotWindow { from: left_from, to: left_to },
        crate::engine::mv::iceberg_join_branch::SnapshotWindow { from: right_from, to: right_to },
        left_has_changes,
        right_has_changes,
    );
    if branches.is_empty() {
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }
    execute_join_delta_branches(
        state,
        target,
        target_entry,
        iceberg_catalog,
        expected_main_snapshot_id,
        current_database,
        mv_definition,
        shape,
        target_table,
        pin,
        branches,
    )
}
```

Add this local helper in `iceberg_refresh.rs` if there is no visible helper with this exact behaviour:

```rust
fn iceberg_change_batch_has_row_deletes(batch: &crate::connector::iceberg::changes::IcebergChangeBatch) -> bool {
    !batch.position_deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty()
}
```

Add the metadata-only finalizer used when both pinned base snapshots advance but neither base contributes rows:

```rust
fn finalize_iceberg_mv_metadata_only_refresh(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    mv_definition: &StoredMvDefinition,
    snapshots: std::collections::BTreeMap<String, i64>,
    table_uuids: std::collections::BTreeMap<String, String>,
) -> Result<StatementResult, String> {
    let target_snapshot_id = recorded_target_snapshot_id(target, mv_definition)?;
    let refresh_id = begin_iceberg_mv_refresh_intent(state, mv_definition.mv_id, snapshots.clone())?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        mv_definition.last_refresh_rows.unwrap_or(0),
        snapshots,
        table_uuids,
        target_snapshot_id,
    )?;
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 3: Implement branch execution**

Add `execute_join_delta_branches` that uses the same staged branch lifecycle as `incremental_refresh_iceberg_mv`, but uses `IcebergJoinCoalesceSinkFactory` for branch executions. The function must:

1. Begin staged refresh intent with `pin.to_snapshot_map()`.
2. Create one shared `JoinDeltaCoalescer`.
3. For each branch, call `rewrite_join_branch_query`.
4. Execute each branch with `execute_query_with_options(..., Some(Box::new(join_sink)), Some(&*catalogs_guard))`.
5. After all branches pass, call `coalescer.flush_to_iceberg_commit_collector(...)`.
6. Commit, publish, and finalize using the same helper path as single-base incremental refresh.

In `iceberg_join_coalesce.rs`, add the flush API before wiring `execute_join_delta_branches`:

```rust
pub(crate) struct JoinCoalesceFlushOutcome {
    pub(crate) added_rows: i64,
    pub(crate) deleted_rows: i64,
}

impl JoinDeltaCoalescer {
    pub(crate) fn flush_to_iceberg_commit_collector(
        &self,
        target_table: &iceberg::table::Table,
        collector: Arc<crate::connector::iceberg::commit::IcebergCommitCollector>,
        locator_inputs: Option<(
            crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
            crate::engine::delete_flow::ReferencedDataFilePartitions,
        )>,
    ) -> Result<JoinCoalesceFlushOutcome, String> {
        let rows = self.rows.lock().expect("join coalescer lock");
        let mut insert_batches = Vec::new();
        let mut delete_keys = Vec::new();
        for (key, row) in rows.iter() {
            if row.net.abs() > 1 {
                return Err(format!("join coalesce net change_op {} for key {key}", row.net));
            }
            match row.net {
                1 => {
                    let payload = row.payload.as_ref().ok_or_else(|| {
                        format!("join coalesce missing INSERT payload for key {key}")
                    })?;
                    insert_batches.push(append_join_apply_key(payload, key)?);
                }
                -1 => delete_keys.push(key.clone()),
                0 => {}
                other => return Err(format!("join coalesce unsupported net change_op {other}")),
            }
        }
        drop(rows);

        let added_rows = insert_batches.iter().map(|batch| batch.num_rows() as i64).sum();
        let data_files = crate::runtime::global_async_runtime::data_block_on(
            crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
                target_table,
                insert_batches,
            ),
        )??;
        let partition_spec_id = target_table.metadata().default_partition_spec_id();
        for data_file in data_files {
            let written = crate::engine::iceberg_writer::data_file_to_written_file(
                &data_file,
                partition_spec_id,
            )?;
            collector.inject_written_file(written);
        }

        let deleted_rows = i64::try_from(delete_keys.len())
            .map_err(|_| "join coalesce delete key count exceeds i64".to_string())?;
        if !delete_keys.is_empty() {
            let (existing_deletes_by_file, referenced_data_file_partitions) =
                locator_inputs.ok_or_else(|| {
                    "join coalesce needs target locator inputs for DELETE rows".to_string()
                })?;
            let groups = crate::runtime::global_async_runtime::data_block_on(
                crate::engine::mv::iceberg_target_apply::locate_target_rows_by_apply_key_string(
                    target_table,
                    &delete_keys,
                    &existing_deletes_by_file,
                    &referenced_data_file_partitions,
                ),
            )??;
            for group in groups {
                collector.inject_delete_group(group);
            }
        }
        Ok(JoinCoalesceFlushOutcome { added_rows, deleted_rows })
    }
}

fn append_join_apply_key(batch: &RecordBatch, key: &str) -> Result<RecordBatch, String> {
    if batch.schema().fields().iter().any(|field| {
        field
            .name()
            .eq_ignore_ascii_case(crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN)
    }) {
        return Err(format!(
            "join coalesce payload already contains reserved column {}",
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN
        ));
    }
    let mut fields: Vec<arrow::datatypes::Field> =
        batch.schema().fields().iter().map(|field| field.as_ref().clone()).collect();
    fields.push(arrow::datatypes::Field::new(
        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
        arrow::datatypes::DataType::Utf8,
        false,
    ));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(arrow::array::StringArray::from(vec![key.to_string(); batch.num_rows()])));
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)
        .map_err(|e| format!("join coalesce append apply key: {e}"))
}
```

Use this structure and keep every listed operation in the final function. Add the helper functions first so branch execution does not depend on `StandaloneSession` time-travel rewrites:

```rust
fn parse_mv_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|err| format!("sql parser error: {err}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

fn build_join_branch_catalog(
    state: &Arc<StandaloneState>,
    branch: &crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan,
) -> Result<crate::engine::catalog::InMemoryCatalog, String> {
    let mut catalog = crate::engine::catalog::InMemoryCatalog::default();
    register_join_branch_side(&mut catalog, state, &branch.left_base, branch.left)?;
    register_join_branch_side(&mut catalog, state, &branch.right_base, branch.right)?;
    Ok(catalog)
}

fn register_join_branch_side(
    catalog: &mut crate::engine::catalog::InMemoryCatalog,
    state: &Arc<StandaloneState>,
    base: &IcebergTableRef,
    side: crate::engine::mv::iceberg_join_branch::BranchSide,
) -> Result<(), String> {
    catalog.create_database(&base.namespace)?;
    let table_def = match side {
        crate::engine::mv::iceberg_join_branch::BranchSide::Delta(_) => {
            crate::engine::query_prep::build_iceberg_table_def_for_delta_scan(
                state,
                &base.catalog,
                &base.namespace,
                &base.table,
            )?
        }
        crate::engine::mv::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) => {
            build_iceberg_table_def_for_snapshot_scan(state, base, snapshot_id)?
        }
    };
    catalog
        .register(&base.namespace, table_def)
        .map_err(|e| format!("register join branch table {}: {e}", base.fqn()))
}

fn build_iceberg_table_def_for_snapshot_scan(
    state: &Arc<StandaloneState>,
    base: &IcebergTableRef,
    snapshot_id: i64,
) -> Result<crate::sql::catalog::TableDef, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&base.catalog)?
    };
    let loaded = crate::connector::iceberg::catalog::load_table(
        &entry,
        &base.namespace,
        &base.table,
    )?;
    let data_files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
            &loaded.table,
            snapshot_id,
        )?;
    let synthetic_name = format!("{}__at_{}", base.table, snapshot_id);
    crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        &entry,
        &base.namespace,
        &synthetic_name,
        loaded,
        data_files,
    )
}

fn execute_join_delta_branches(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    expected_main_snapshot_id: Option<i64>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    shape: &crate::connector::starrocks::managed::mv_shape::JoinProjectionFilterMvShape,
    target_table: &iceberg::table::Table,
    pin: &crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin,
    branches: Vec<crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan>,
) -> Result<StatementResult, String> {
    let base_query = parse_mv_select_query(&mv_definition.select_sql)?;
    let left_uuid = pin.uuid(&branches[0].left_base)
        .ok_or_else(|| format!("missing uuid for {}", branches[0].left_base.fqn()))?
        .to_string();
    let right_uuid = pin.uuid(&branches[0].right_base)
        .ok_or_else(|| format!("missing uuid for {}", branches[0].right_base.fqn()))?
        .to_string();
    let coalescer = crate::engine::mv::iceberg_join_coalesce::JoinDeltaCoalescer::new(
        left_uuid,
        right_uuid,
        1_000_000,
    );
    let ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let staging_branch = format!(
        "__nova_mv_refresh_{}_{}",
        mv_definition.mv_id,
        uuid::Uuid::new_v4().simple()
    );
    let refresh_id = begin_staged_iceberg_mv_refresh_intent(
        state,
        target,
        mv_definition.mv_id,
        expected_main_snapshot_id,
        pin.to_snapshot_map(),
        &staging_branch,
    )?;
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        &staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let op_kind = CommitOpKind::RowDeltaDv;
    let collector = new_iceberg_mv_commit_collector(target_table, &ident, &staging_branch, op_kind);
    for branch in branches {
        let branch_query = crate::engine::mv::iceberg_join_branch::rewrite_join_branch_query(
            &base_query,
            &branch,
            &shape.left_alias,
            &shape.right_alias,
        )
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?;
        let sink = crate::engine::mv::iceberg_join_coalesce::IcebergJoinCoalesceSinkFactory::new(
            Arc::clone(&coalescer),
        );
        let branch_catalog = build_join_branch_catalog(state, &branch).map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?;
        let catalogs_guard = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        if let Err(err) = crate::engine::execute_query_with_options(
            &branch_query,
            &branch_catalog,
            current_database,
            state.exchange_port,
            None,
            Some(Box::new(sink)),
            Some(&*catalogs_guard),
        ) {
            drop(catalogs_guard);
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
        drop(catalogs_guard);
    }
    let locator_inputs = load_target_apply_locator_inputs(target_entry, target_table)
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?;
    let flush_outcome = coalescer
        .flush_to_iceberg_commit_collector(
            target_table,
            Arc::clone(&collector),
            Some(locator_inputs),
        )
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            )
        })?;
    if flush_outcome.added_rows == 0 && flush_outcome.deleted_rows == 0 {
        drop_iceberg_mv_staging_branch(state, target, target_entry, &staging_branch)?;
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return finalize_iceberg_mv_metadata_only_refresh(
            state,
            target,
            mv_definition,
            pin.to_snapshot_map(),
            pin.to_table_uuid_map(),
        );
    }

    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
        .to_summary_properties();
    let commit_outcome = match crate::runtime::global_async_runtime::data_block_on(
        commit_iceberg_mv_with_populated_collector(
            target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            Arc::clone(&collector),
            &staging_branch,
            marker,
        ),
    ) {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                &staging_branch,
                refresh_id,
                err,
            ));
        }
    };

    let new_total_rows = mv_definition
        .last_refresh_rows
        .unwrap_or(0)
        .checked_add(flush_outcome.added_rows)
        .and_then(|rows| rows.checked_sub(flush_outcome.deleted_rows))
        .ok_or_else(|| {
            format!(
                "iceberg join MV row-count delta overflow: current={:?}, inserts={}, deletes={}",
                mv_definition.last_refresh_rows,
                flush_outcome.added_rows,
                flush_outcome.deleted_rows
            )
        })?;
    let snapshots = pin.to_snapshot_map();
    let table_uuids = pin.to_table_uuid_map();
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        commit_outcome.new_snapshot_id,
        new_total_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        &staging_branch,
        expected_main_snapshot_id,
        commit_outcome.new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, &staging_branch)?;
    finalize_iceberg_mv_refresh(
        state,
        refresh_id,
        new_total_rows,
        snapshots,
        table_uuids,
        published_snapshot_id,
    )?;
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 4: Run focused refresh tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::refresh_dispatch_identifies_join_shape -- --nocapture
cargo test --lib engine::mv::iceberg_join_branch::tests -- --nocapture
cargo test --lib engine::mv::iceberg_join_coalesce::tests -- --nocapture
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs src/engine/mv/iceberg_join_coalesce.rs
git commit -m "feat: execute iceberg join imv refresh branches"
```

## Task 9: SQL Regression Tests

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_two_base_delta.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_join_two_base_delta.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_reject_unsupported.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_join_reject_unsupported.result`

- [ ] **Step 1: Add positive SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_two_base_delta.sql`:

```sql
-- name: iceberg_ivm_join_two_base_delta
-- Test Point: Iceberg-backed join IMV supports two-table inner equi-join with both bases changing in one refresh.
-- Method: Create two Iceberg v3 row-lineage base tables, create join MV, mutate both bases, refresh, and compare MV result with base join.
-- Scope: Iceberg v3 row-lineage, two-table inner equi-join, telescoping delta, composite join row key.

DROP MATERIALIZED VIEW IF EXISTS join_mv;
DROP TABLE IF EXISTS join_left;
DROP TABLE IF EXISTS join_right;

CREATE TABLE join_left (
  id BIGINT,
  rid BIGINT,
  amount INT
) ENGINE=ICEBERG
PROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);

CREATE TABLE join_right (
  rid BIGINT,
  label STRING
) ENGINE=ICEBERG
PROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);

INSERT INTO join_left VALUES (1, 10, 100), (2, 20, 200);
INSERT INTO join_right VALUES (10, 'old-a'), (20, 'old-b');

CREATE MATERIALIZED VIEW join_mv
PROPERTIES ("storage_engine" = "iceberg")
AS
SELECT l.id, l.amount, r.label
FROM iceberg.default.join_left AS l
JOIN iceberg.default.join_right AS r ON l.rid = r.rid
WHERE l.amount >= 100;

REFRESH MATERIALIZED VIEW join_mv;

SELECT id, amount, label FROM join_mv ORDER BY id, label;

INSERT INTO join_left VALUES (3, 30, 300);
INSERT INTO join_right VALUES (30, 'new-c');
DELETE FROM join_left WHERE id = 1;
UPDATE join_right SET label = 'new-b' WHERE rid = 20;

REFRESH MATERIALIZED VIEW join_mv;

SELECT id, amount, label FROM join_mv ORDER BY id, label;
SELECT l.id, l.amount, r.label
FROM join_left AS l
JOIN join_right AS r ON l.rid = r.rid
WHERE l.amount >= 100
ORDER BY l.id, r.label;

DROP MATERIALIZED VIEW join_mv;
DROP TABLE join_left;
DROP TABLE join_right;
```

Record expected output after implementation with the SQL runner record mode, then manually inspect that both final SELECTs match.

- [ ] **Step 2: Add negative SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_reject_unsupported.sql`:

```sql
-- name: iceberg_ivm_join_reject_unsupported
-- Test Point: Unsupported join IMV shapes fail at CREATE time.
-- Method: Try outer join, non-equi join, and three-table join.
-- Scope: Iceberg-backed join IMV shape validation.

DROP TABLE IF EXISTS reject_left;
DROP TABLE IF EXISTS reject_right;
DROP TABLE IF EXISTS reject_extra;

CREATE TABLE reject_left (id BIGINT, rid BIGINT) ENGINE=ICEBERG
PROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE reject_right (id BIGINT, rid BIGINT) ENGINE=ICEBERG
PROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE reject_extra (id BIGINT, rid BIGINT) ENGINE=ICEBERG
PROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- error
CREATE MATERIALIZED VIEW reject_outer
PROPERTIES ("storage_engine" = "iceberg")
AS SELECT l.id FROM iceberg.default.reject_left l
LEFT JOIN iceberg.default.reject_right r ON l.rid = r.rid;

-- error
CREATE MATERIALIZED VIEW reject_nonequi
PROPERTIES ("storage_engine" = "iceberg")
AS SELECT l.id FROM iceberg.default.reject_left l
JOIN iceberg.default.reject_right r ON l.rid > r.rid;

-- error
CREATE MATERIALIZED VIEW reject_three
PROPERTIES ("storage_engine" = "iceberg")
AS SELECT l.id FROM iceberg.default.reject_left l
JOIN iceberg.default.reject_right r ON l.rid = r.rid
JOIN iceberg.default.reject_extra x ON x.rid = r.rid;

DROP TABLE reject_left;
DROP TABLE reject_right;
DROP TABLE reject_extra;
```

Record expected errors after implementation. Each error must contain: `incremental join MV supports only two-table inner equi-join`.

- [ ] **Step 3: Run SQL tests**

Start the local Iceberg REST environment and runner per `AGENTS.md`:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_join_two_base_delta,iceberg_ivm_join_reject_unsupported \
  --mode verify \
  --query-timeout 120
```

Expected:

```text
total=2 pass=2 fail=0
```

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_join_two_base_delta.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_join_two_base_delta.result \
        sql-tests/iceberg-ivm/sql/iceberg_ivm_join_reject_unsupported.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_join_reject_unsupported.result
git commit -m "test: cover iceberg join imv refresh"
```

## Task 10: Final Verification

**Files:**
- Verify all touched files.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt
```

Expected: no output and exit 0.

- [ ] **Step 2: Run focused Rust tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_shape::tests -- --nocapture
cargo test --lib meta::repository::mv_contract::tests -- --nocapture
cargo test --lib sql::analyzer::mv_lineage::tests -- --nocapture
cargo test --lib engine::mv::iceberg_join_branch::tests -- --nocapture
cargo test --lib engine::mv::iceberg_join_coalesce::tests -- --nocapture
cargo test --lib engine::mv::iceberg_refresh::tests -- --nocapture
```

Expected: all pass.

- [ ] **Step 3: Run SQL regression**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_join_two_base_delta,iceberg_ivm_join_reject_unsupported \
  --mode verify \
  --query-timeout 120
```

Expected:

```text
total=2 pass=2 fail=0
```

- [ ] **Step 4: Run existing Iceberg IVM smoke subset**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_a1_update_only,iceberg_ivm_a1_large_delta_mixed,iceberg_ivm_base_delete_row_lineage \
  --mode verify \
  --query-timeout 120
```

Expected:

```text
total=3 pass=3 fail=0
```

- [ ] **Step 5: Check diff hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` exits 0. `git status --short` shows only intentional files.

- [ ] **Step 6: Commit final fixes**

If formatting or verification changed files, commit them:

```bash
git add src/connector/starrocks/managed/mv_shape.rs \
        src/meta/repository/mv_contract.rs \
        src/sql/analyzer/mv_lineage.rs \
        src/engine/mv/mod.rs \
        src/engine/mv/iceberg_join_branch.rs \
        src/engine/mv/iceberg_join_coalesce.rs \
        src/engine/mv/iceberg_target_apply.rs \
        src/engine/mv/iceberg_refresh.rs \
        sql-tests/iceberg-ivm/sql/iceberg_ivm_join_two_base_delta.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_join_two_base_delta.result \
        sql-tests/iceberg-ivm/sql/iceberg_ivm_join_reject_unsupported.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_join_reject_unsupported.result
git commit -m "chore: finalize iceberg join imv"
```

## Self-Review Notes

- Spec coverage: shape, two-base scope, telescoping branch correctness, composite key, coalesce, target apply, failure semantics, and SQL verification are each covered by tasks.
- Placeholder scan: this plan contains no deferred implementation markers. Task 8 lists the concrete staged-refresh operations that must be present in the final function.
- Type consistency: `JoinProjectionFilter`, `JoinRowKey`, `__nova_join_row_key`, `JoinDeltaBranchPlan`, and `JoinDeltaCoalescer` are introduced before later tasks consume them.
