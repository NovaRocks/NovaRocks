//! DDL/DML statement handlers for the standalone engine.
//!
//! Top-level dispatchers (`execute_create_database_statement`,
//! `execute_insert_statement`, etc.) route statements to the in-memory
//! catalog, the iceberg registry, or the managed lake based on the parsed name
//! and current catalog/database session context.

use std::sync::Arc;

use crate::connector::truncate_managed_table as truncate_managed_lake_table;
use crate::engine::catalog::normalize_identifier;
use crate::engine::name_resolve::resolve_local_table_name;
use crate::engine::{
    StandaloneState, StatementResult, delete_iceberg_catalog_if_needed,
    delete_iceberg_namespace_if_needed, delete_iceberg_table_if_needed,
    persist_iceberg_namespace_if_needed, persist_iceberg_table_if_needed,
};
use crate::sql::catalog::LegacyRangePartition;
use crate::sql::parser::ast::{
    CreateTableKind, DefaultLiteral, InsertSource, Literal, ObjectName, SqlType,
};
use crate::sql::parser::dialect::StarRocksDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::sql_expr::sqlparser_expr_to_literal;

fn convert_set_expr_to_insert_source(
    body: &sqlparser::ast::SetExpr,
) -> Result<InsertSource, String> {
    use sqlparser::ast as sqlast;
    match body {
        sqlast::SetExpr::Values(values) => {
            let mut rows = Vec::new();
            for row in &values.rows {
                let literal_row: Vec<Literal> = row
                    .iter()
                    .map(sqlparser_expr_to_literal)
                    .collect::<Result<_, _>>()?;
                rows.push(literal_row);
            }
            Ok(InsertSource::Values(rows))
        }
        sqlast::SetExpr::Select(select) => {
            if select.from.is_empty() {
                let row: Vec<Literal> = select
                    .projection
                    .iter()
                    .map(select_item_expr)
                    .map(|expr| expr.and_then(sqlparser_expr_to_literal))
                    .collect::<Result<_, _>>()?;
                Ok(InsertSource::SelectLiteralRow(row))
            } else {
                Err("INSERT SELECT with FROM must use the query pipeline".into())
            }
        }
        sqlast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            // Only UNION ALL is handled at this layer. UNION (distinct) would
            // need output-level dedup with the target schema's types, which we
            // don't have at parse time.
            if !matches!(op, sqlast::SetOperator::Union) {
                return Err("INSERT SELECT set operation is only UNION ALL here".into());
            }
            if !matches!(
                set_quantifier,
                sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName
            ) {
                return Err(
                    "INSERT SELECT UNION requires UNION ALL (UNION/UNION DISTINCT unsupported)"
                        .into(),
                );
            }
            let mut parts = Vec::new();
            flatten_union_all(left, &mut parts)?;
            flatten_union_all(right, &mut parts)?;
            Ok(InsertSource::UnionAll(parts))
        }
        sqlast::SetExpr::Query(query) => convert_set_expr_to_insert_source(query.body.as_ref()),
        _ => Err("unsupported INSERT source".into()),
    }
}

fn select_item_expr(item: &sqlparser::ast::SelectItem) -> Result<&sqlparser::ast::Expr, String> {
    use sqlparser::ast as sqlast;
    match item {
        sqlast::SelectItem::UnnamedExpr(expr) | sqlast::SelectItem::ExprWithAlias { expr, .. } => {
            Ok(expr)
        }
        _ => Err("INSERT SELECT source only supports expressions".into()),
    }
}

fn flatten_union_all(
    body: &sqlparser::ast::SetExpr,
    out: &mut Vec<InsertSource>,
) -> Result<(), String> {
    use sqlparser::ast as sqlast;
    if let sqlast::SetExpr::SetOperation {
        op: sqlast::SetOperator::Union,
        set_quantifier: sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName,
        left,
        right,
    } = body
    {
        flatten_union_all(left, out)?;
        flatten_union_all(right, out)?;
        Ok(())
    } else {
        out.push(convert_set_expr_to_insert_source(body)?);
        Ok(())
    }
}

/// Decide whether an INSERT's source Query should be executed via the full
/// plan pipeline (returning `InsertSource::FromQuery`) instead of being
/// collapsed into literal rows.
///
/// We route via the full pipeline whenever the Query carries clauses the
/// literal fast path can't represent (WITH/ORDER BY/LIMIT/FETCH/locks), the
/// body reads from a relation (incl. table functions like
/// `TABLE(generate_series(...))`), or any SELECT projection item is an
/// expression the constant folder cannot reduce to a `Literal`.
fn should_route_insert_via_from_query(query: &sqlparser::ast::Query) -> bool {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
    {
        return true;
    }
    body_requires_pipeline(query.body.as_ref())
}

fn body_requires_pipeline(body: &sqlparser::ast::SetExpr) -> bool {
    use sqlparser::ast as sqlast;
    match body {
        sqlast::SetExpr::Select(select) => {
            !select.from.is_empty() || select_projection_requires_pipeline(select)
        }
        sqlast::SetExpr::Query(inner) => should_route_insert_via_from_query(inner.as_ref()),
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            body_requires_pipeline(left) || body_requires_pipeline(right)
        }
        _ => false,
    }
}

fn select_projection_requires_pipeline(select: &sqlparser::ast::Select) -> bool {
    use sqlparser::ast as sqlast;
    select.projection.iter().any(|item| match item {
        sqlast::SelectItem::UnnamedExpr(expr) | sqlast::SelectItem::ExprWithAlias { expr, .. } => {
            sqlparser_expr_to_literal(expr).is_err()
        }
        // Wildcards have no expression to fold; if we ever see one without a
        // FROM the analyzer will reject it anyway, so it doesn't matter which
        // path picks it up.
        _ => false,
    })
}

/// Convert a sqlparser INSERT AST to our custom InsertStmt.
/// Used for Iceberg tables which need the custom AST's InsertSource types.
pub(crate) fn convert_sqlparser_insert_to_custom(
    insert: &sqlparser::ast::Insert,
) -> Result<crate::sql::parser::ast::InsertStmt, String> {
    use sqlparser::ast as sqlast;

    let table = match &insert.table {
        sqlast::TableObject::TableName(name) => {
            crate::sql::parser::dialect::convert_object_name(name.clone())?
        }
        other => return Err(format!("unsupported INSERT target: {other}")),
    };
    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
    let source_query = insert
        .source
        .as_ref()
        .ok_or_else(|| "INSERT requires a source".to_string())?;
    // If the source reads from a relation, including a table function, or
    // carries clauses the literal fast path can't express, hand the whole
    // Query to the analyzer/planner/pipeline stack via `FromQuery`. This keeps
    // the INSERT entry point aligned with how StarRocks wraps INSERT ... SELECT
    // as a normal plan with a sink, rather than evaluating SELECT here.
    let source = if should_route_insert_via_from_query(source_query) {
        crate::sql::parser::ast::InsertSource::FromQuery(source_query.clone())
    } else {
        convert_set_expr_to_insert_source(source_query.body.as_ref())?
    };
    use crate::sql::parser::ast::OverwriteMode;
    // The normaliser may have prepended the magic marker `__nr_op_dyn` as the
    // first name segment when it detected `INSERT OVERWRITE PARTITIONS`. Peel
    // that marker here and record the corresponding overwrite mode.
    //
    // `__nr_op_dyn` is a NovaRocks reserved identifier; it should never appear
    // as a real table-name segment in user SQL.
    let (table, overwrite_mode) = if !table.parts.is_empty() && table.parts[0] == "__nr_op_dyn" {
        if !insert.overwrite {
            // Defensive: the marker must only appear when sqlparser parsed an
            // OVERWRITE statement.  A mismatch indicates a normaliser bug.
            return Err(
                "internal: __nr_op_dyn marker present without INSERT OVERWRITE \
                 (parser/normalizer mismatch)"
                    .to_string(),
            );
        }
        let stripped = crate::sql::parser::ast::ObjectName {
            parts: table.parts.into_iter().skip(1).collect(),
        };
        (stripped, OverwriteMode::DynamicPartitions)
    } else {
        let mode = if insert.overwrite {
            OverwriteMode::FullTable
        } else {
            OverwriteMode::None
        };
        (table, mode)
    };
    Ok(crate::sql::parser::ast::InsertStmt {
        table,
        columns,
        source,
        overwrite_mode,
    })
}

/// Convert a sqlparser DELETE AST to our custom DeleteStmt.
///
/// Phase 1 restrictions:
/// - Exactly one table in `FROM`; `USING` clauses are rejected.
/// - `WHERE` is mandatory. `DELETE FROM t` (no filter) is rejected — the
///   spec recommends `INSERT OVERWRITE t SELECT * FROM t WHERE FALSE` instead.
/// - `LIMIT` and `ORDER BY` are rejected.
pub(crate) fn convert_sqlparser_delete_to_custom(
    delete: &sqlparser::ast::Delete,
) -> Result<crate::sql::parser::ast::DeleteStmt, String> {
    use sqlparser::ast as sqlast;

    let tables = match &delete.from {
        sqlast::FromTable::WithFromKeyword(tables) => tables,
        sqlast::FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(format!(
            "phase 1 DELETE supports exactly one table in FROM, got {}",
            tables.len()
        ));
    }
    if !tables[0].joins.is_empty() {
        return Err("phase 1 DELETE does not support JOIN in FROM".to_string());
    }
    let table = match &tables[0].relation {
        sqlast::TableFactor::Table { name, .. } => {
            crate::sql::parser::dialect::convert_object_name(name.clone())?
        }
        other => {
            return Err(format!(
                "phase 1 DELETE source must be a table, got {other:?}"
            ));
        }
    };
    if delete.using.as_ref().is_some_and(|u| !u.is_empty()) {
        return Err("phase 1 DELETE does not support USING".to_string());
    }
    if delete.limit.is_some() {
        return Err("phase 1 DELETE does not support LIMIT".to_string());
    }
    if !delete.order_by.is_empty() {
        return Err("phase 1 DELETE does not support ORDER BY".to_string());
    }
    let where_clause = delete.selection.clone().ok_or_else(|| {
        "DELETE requires a WHERE clause; for full table replacement use \
         INSERT OVERWRITE t SELECT * FROM t WHERE FALSE"
            .to_string()
    })?;
    Ok(crate::sql::parser::ast::DeleteStmt {
        table,
        where_clause,
    })
}

pub(crate) fn convert_sqlparser_update_to_custom(
    statement: &sqlparser::ast::Statement,
) -> Result<crate::sql::parser::ast::UpdateStmt, String> {
    use crate::sql::parser::ast::{UpdateAssignment, UpdateStmt};
    use sqlparser::ast as sqlast;

    let sqlast::Statement::Update(update) = statement else {
        return Err("expected UPDATE statement".to_string());
    };
    let sqlast::Update {
        update_token,
        optimizer_hint,
        table,
        assignments,
        from,
        selection,
        returning,
        or,
        limit,
    } = update;
    let _ = update_token;
    if optimizer_hint.is_some() {
        return Err("UPDATE optimizer hints are not supported".to_string());
    }
    if or.is_some() {
        return Err("UPDATE conflict clauses are not supported".to_string());
    }
    if returning.is_some() {
        return Err("UPDATE RETURNING is not supported".to_string());
    }
    if limit.is_some() {
        return Err("UPDATE LIMIT is not supported".to_string());
    }
    if !table.joins.is_empty() {
        return Err(
            "UPDATE target joins are not supported; use UPDATE ... FROM with a single source relation"
                .to_string(),
        );
    }

    let (target_name, target_alias) = match &table.relation {
        sqlast::TableFactor::Table {
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
        } => {
            reject_update_table_modifiers(
                args,
                with_hints,
                version,
                *with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                "UPDATE target",
            )?;
            (
                crate::sql::parser::dialect::convert_object_name(name.clone())?,
                update_alias_name(alias, "UPDATE target")?,
            )
        }
        sqlast::TableFactor::Pivot { .. } | sqlast::TableFactor::Unpivot { .. } => {
            return Err("UPDATE target pivot/unpivot are not supported".to_string());
        }
        other => return Err(format!("UPDATE target must be a table, got {other:?}")),
    };

    let mut out_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let sqlast::AssignmentTarget::ColumnName(column_name) = &assignment.target else {
            return Err("only single-column UPDATE assignments are supported".to_string());
        };
        let column = crate::sql::parser::dialect::convert_object_name(column_name.clone())?;
        if column.parts.len() != 1 {
            return Err(format!(
                "UPDATE assignment must reference an unqualified target column, got `{column_name}`"
            ));
        }
        out_assignments.push(UpdateAssignment {
            column: column.parts[0].clone(),
            value: assignment.value.clone(),
        });
    }
    if out_assignments.is_empty() {
        return Err("UPDATE requires at least one assignment".to_string());
    }

    let source = convert_update_from_source(from)?;
    Ok(UpdateStmt {
        table: target_name,
        alias: target_alias,
        assignments: out_assignments,
        source,
        where_clause: selection.clone(),
    })
}

pub(crate) fn convert_sqlparser_merge_to_custom(
    statement: &sqlparser::ast::Statement,
) -> Result<crate::sql::parser::ast::MergeStmt, String> {
    use crate::sql::parser::ast::{
        MergeMatchedAction, MergeNotMatchedAction, MergeStmt, MergeWhenClause, MutationSource,
        UpdateAssignment,
    };
    use sqlparser::ast as sqlast;

    let sqlast::Statement::Merge(merge) = statement else {
        return Err("expected MERGE statement".to_string());
    };
    let sqlast::Merge {
        merge_token,
        optimizer_hint,
        into: _,
        table,
        source,
        on,
        clauses,
        output,
    } = merge;
    let _ = merge_token;
    if optimizer_hint.is_some() {
        return Err("MERGE optimizer hints are not supported".to_string());
    }
    if output.is_some() {
        return Err("MERGE OUTPUT is not supported".to_string());
    }

    let (target_name, target_alias) = match table {
        sqlast::TableFactor::Table {
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
        } => {
            reject_update_table_modifiers(
                args,
                with_hints,
                version,
                *with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                "MERGE target",
            )?;
            (
                crate::sql::parser::dialect::convert_object_name(name.clone())?,
                update_alias_name(alias, "MERGE target")?,
            )
        }
        sqlast::TableFactor::Pivot { .. } | sqlast::TableFactor::Unpivot { .. } => {
            return Err("MERGE target pivot/unpivot are not supported".to_string());
        }
        other => return Err(format!("MERGE target must be a table, got {other:?}")),
    };

    let source = match source {
        sqlast::TableFactor::Table {
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
        } => {
            reject_update_table_modifiers(
                args,
                with_hints,
                version,
                *with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                "MERGE source",
            )?;
            MutationSource::Table {
                name: crate::sql::parser::dialect::convert_object_name(name.clone())?,
                alias: update_alias_name(alias, "MERGE source")?,
            }
        }
        sqlast::TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral {
                return Err("MERGE source lateral subqueries are not supported".to_string());
            }
            if sample.is_some() {
                return Err("MERGE source samples are not supported".to_string());
            }
            MutationSource::Query {
                query: subquery.clone(),
                alias: update_alias_name(alias, "MERGE source")?,
            }
        }
        sqlast::TableFactor::Pivot { .. } | sqlast::TableFactor::Unpivot { .. } => {
            return Err("MERGE source pivot/unpivot are not supported".to_string());
        }
        other => return Err(format!("unsupported MERGE source: {other:?}")),
    };

    let mut matched: Option<MergeWhenClause<MergeMatchedAction>> = None;
    let mut not_matched: Option<MergeWhenClause<MergeNotMatchedAction>> = None;
    for clause in clauses {
        let sqlast::MergeClause {
            when_token,
            clause_kind,
            predicate,
            action,
        } = clause;
        let _ = when_token;
        match clause_kind {
            sqlast::MergeClauseKind::Matched => {
                if matched.is_some() {
                    return Err(
                        "MERGE supports at most one WHEN MATCHED clause in this implementation"
                            .to_string(),
                    );
                }
                let action = match action {
                    sqlast::MergeAction::Update(update) => {
                        let sqlast::MergeUpdateExpr {
                            update_token,
                            assignments,
                            update_predicate,
                            delete_predicate,
                        } = update;
                        let _ = update_token;
                        if update_predicate.is_some() || delete_predicate.is_some() {
                            return Err(
                                "MERGE WHEN MATCHED UPDATE WHERE / DELETE WHERE clauses are not supported"
                                    .to_string(),
                            );
                        }
                        let mut out = Vec::with_capacity(assignments.len());
                        for assignment in assignments {
                            let sqlast::AssignmentTarget::ColumnName(column_name) =
                                &assignment.target
                            else {
                                return Err(
                                    "only single-column MERGE UPDATE assignments are supported"
                                        .to_string(),
                                );
                            };
                            let column = crate::sql::parser::dialect::convert_object_name(
                                column_name.clone(),
                            )?;
                            if column.parts.len() != 1 {
                                return Err(format!(
                                    "MERGE UPDATE assignment must reference an unqualified target column, got `{column_name}`"
                                ));
                            }
                            out.push(UpdateAssignment {
                                column: column.parts[0].clone(),
                                value: assignment.value.clone(),
                            });
                        }
                        if out.is_empty() {
                            return Err(
                                "MERGE WHEN MATCHED UPDATE requires at least one assignment"
                                    .to_string(),
                            );
                        }
                        MergeMatchedAction::Update { assignments: out }
                    }
                    sqlast::MergeAction::Delete { .. } => MergeMatchedAction::Delete,
                    sqlast::MergeAction::Insert(_) => {
                        return Err(
                            "MERGE WHEN MATCHED INSERT is not valid; use UPDATE or DELETE"
                                .to_string(),
                        );
                    }
                };
                matched = Some(MergeWhenClause {
                    predicate: predicate.clone(),
                    action,
                });
            }
            sqlast::MergeClauseKind::NotMatched | sqlast::MergeClauseKind::NotMatchedByTarget => {
                if not_matched.is_some() {
                    return Err(
                        "MERGE supports at most one WHEN NOT MATCHED clause in this implementation"
                            .to_string(),
                    );
                }
                let action = match action {
                    sqlast::MergeAction::Insert(insert) => {
                        let sqlast::MergeInsertExpr {
                            insert_token,
                            columns,
                            kind_token,
                            kind,
                            insert_predicate,
                        } = insert;
                        let _ = (insert_token, kind_token);
                        if insert_predicate.is_some() {
                            return Err(
                                "MERGE WHEN NOT MATCHED INSERT WHERE clauses are not supported"
                                    .to_string(),
                            );
                        }
                        let columns_out: Vec<String> = columns
                            .iter()
                            .map(|name| {
                                let parts =
                                    crate::sql::parser::dialect::convert_object_name(name.clone())?;
                                if parts.parts.len() != 1 {
                                    return Err(format!(
                                        "MERGE INSERT column must be unqualified, got `{name}`"
                                    ));
                                }
                                Ok::<_, String>(parts.parts[0].clone())
                            })
                            .collect::<Result<_, _>>()?;
                        let values = match kind {
                            sqlast::MergeInsertKind::Values(values) => {
                                if values.rows.len() != 1 {
                                    return Err(format!(
                                        "MERGE WHEN NOT MATCHED INSERT VALUES requires exactly one row tuple, got {}",
                                        values.rows.len()
                                    ));
                                }
                                values.rows[0].clone()
                            }
                            sqlast::MergeInsertKind::Row => {
                                return Err(
                                    "MERGE WHEN NOT MATCHED INSERT ROW shorthand is not supported; \
                                     spell out VALUES (...) explicitly"
                                        .to_string(),
                                );
                            }
                        };
                        if !columns_out.is_empty() && columns_out.len() != values.len() {
                            return Err(format!(
                                "MERGE INSERT column count {} does not match VALUES count {}",
                                columns_out.len(),
                                values.len()
                            ));
                        }
                        MergeNotMatchedAction {
                            columns: columns_out,
                            values,
                        }
                    }
                    sqlast::MergeAction::Update(_) | sqlast::MergeAction::Delete { .. } => {
                        return Err("MERGE WHEN NOT MATCHED action must be INSERT".to_string());
                    }
                };
                not_matched = Some(MergeWhenClause {
                    predicate: predicate.clone(),
                    action,
                });
            }
            sqlast::MergeClauseKind::NotMatchedBySource => {
                return Err(
                    "MERGE WHEN NOT MATCHED BY SOURCE is not supported in this implementation"
                        .to_string(),
                );
            }
        }
    }

    if matched.is_none() && not_matched.is_none() {
        return Err("MERGE requires at least one WHEN clause".to_string());
    }

    Ok(MergeStmt {
        table: target_name,
        target_alias,
        source,
        on: (**on).clone(),
        matched,
        not_matched,
    })
}

fn convert_update_from_source(
    from: &Option<sqlparser::ast::UpdateTableFromKind>,
) -> Result<Option<crate::sql::parser::ast::MutationSource>, String> {
    use crate::sql::parser::ast::MutationSource;
    use sqlparser::ast as sqlast;

    let Some(from) = from else {
        return Ok(None);
    };
    let tables = match from {
        sqlast::UpdateTableFromKind::BeforeSet(tables)
        | sqlast::UpdateTableFromKind::AfterSet(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(format!(
            "UPDATE ... FROM supports exactly one source relation, got {}",
            tables.len()
        ));
    }
    if !tables[0].joins.is_empty() {
        return Err("UPDATE ... FROM joins must be wrapped in a subquery".to_string());
    }
    match &tables[0].relation {
        sqlast::TableFactor::Table {
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
        } => {
            reject_update_table_modifiers(
                args,
                with_hints,
                version,
                *with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                "UPDATE ... FROM source",
            )?;
            Ok(Some(MutationSource::Table {
                name: crate::sql::parser::dialect::convert_object_name(name.clone())?,
                alias: update_alias_name(alias, "UPDATE ... FROM source")?,
            }))
        }
        sqlast::TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral {
                return Err(
                    "UPDATE ... FROM source lateral subqueries are not supported".to_string(),
                );
            }
            if sample.is_some() {
                return Err("UPDATE ... FROM source samples are not supported".to_string());
            }
            Ok(Some(MutationSource::Query {
                query: subquery.clone(),
                alias: update_alias_name(alias, "UPDATE ... FROM source")?,
            }))
        }
        sqlast::TableFactor::Pivot { .. } | sqlast::TableFactor::Unpivot { .. } => {
            Err("UPDATE ... FROM source pivot/unpivot are not supported".to_string())
        }
        other => Err(format!("unsupported UPDATE ... FROM source: {other:?}")),
    }
}

fn reject_update_table_modifiers(
    args: &Option<sqlparser::ast::TableFunctionArgs>,
    with_hints: &[sqlparser::ast::Expr],
    version: &Option<sqlparser::ast::TableVersion>,
    with_ordinality: bool,
    partitions: &[sqlparser::ast::Ident],
    json_path: &Option<sqlparser::ast::JsonPath>,
    sample: &Option<sqlparser::ast::TableSampleKind>,
    index_hints: &[sqlparser::ast::TableIndexHints],
    context: &str,
) -> Result<(), String> {
    if args.is_some() {
        return Err(format!("{context} table arguments are not supported"));
    }
    if !with_hints.is_empty() {
        return Err(format!("{context} table hints are not supported"));
    }
    if version.is_some() {
        return Err(format!("{context} time travel is not supported"));
    }
    if with_ordinality {
        return Err(format!("{context} WITH ORDINALITY is not supported"));
    }
    if !partitions.is_empty() {
        return Err(format!("{context} partitions are not supported"));
    }
    if json_path.is_some() {
        return Err(format!("{context} JSON paths are not supported"));
    }
    if sample.is_some() {
        return Err(format!("{context} samples are not supported"));
    }
    if !index_hints.is_empty() {
        return Err(format!("{context} index hints are not supported"));
    }
    Ok(())
}

fn update_alias_name(
    alias: &Option<sqlparser::ast::TableAlias>,
    context: &str,
) -> Result<Option<String>, String> {
    let Some(alias) = alias else {
        return Ok(None);
    };
    if !alias.columns.is_empty() {
        return Err(format!("{context} alias column lists are not supported"));
    }
    Ok(Some(alias.name.value.clone()))
}

// ---------------------------------------------------------------------------
// DDL handlers
// ---------------------------------------------------------------------------

pub(crate) fn execute_create_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    if_not_exists: bool,
    current_catalog: Option<&str>,
) -> Result<StatementResult, String> {
    let target =
        crate::engine::backend_resolver::resolve_namespace_target(state, name, current_catalog)?;
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    // When IF NOT EXISTS is specified, skip creation if the namespace already exists.
    if if_not_exists && backend.namespace_exists(&target.catalog, &target.namespace)? {
        return Ok(StatementResult::Ok);
    }
    backend.create_namespace(&target.catalog, &target.namespace)?;
    if target.backend_name == "iceberg" {
        persist_iceberg_namespace_if_needed(state, &target.catalog, &target.namespace)?;
    }
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_create_table_statement(
    state: &Arc<StandaloneState>,
    stmt: crate::sql::parser::ast::CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let legacy_range_partitions = stmt.legacy_range_partitions.clone();
    // CTAS dispatch: when the statement carries an AS SELECT clause, route
    // managed-lake targets to the managed CTAS helper and iceberg targets
    // to the iceberg helper. The parser already rejected
    // non-iceberg-compatible CTAS forms (branch target / format-version=2 /
    // explicit columns / etc.).
    if stmt.as_select.is_some() {
        let target = crate::engine::backend_resolver::resolve_table_target(
            state,
            &stmt.name,
            current_catalog,
            current_database,
        )?;
        if target.backend_name == "managed" {
            return crate::engine::managed_ctas::execute_managed_ctas(
                state,
                stmt,
                current_catalog,
                current_database,
            );
        }
        return crate::engine::iceberg_ctas::execute_iceberg_ctas(
            state,
            stmt,
            current_catalog,
            current_database,
        );
    }
    match stmt.kind {
        CreateTableKind::Iceberg {
            columns,
            key_desc,
            bucket_count,
            partition_fields,
            properties,
        } => {
            if current_catalog.is_none()
                && stmt.name.parts.len() <= 2
                && state.managed_lake_config.is_none()
            {
                return Err(
                    "managed lake is not configured; set `warehouse_uri` to run CREATE TABLE"
                        .to_string(),
                );
            }

            let target = crate::engine::backend_resolver::resolve_table_target(
                state,
                &stmt.name,
                current_catalog,
                current_database,
            )?;
            let backend = state
                .connectors
                .read()
                .expect("connector registry read")
                .catalog_backend(target.backend_name)?;
            // Honour `IF NOT EXISTS`: skip creation when the table already
            // exists. CTAS has its own existence check in `iceberg_ctas`.
            if stmt.if_not_exists
                && backend.table_exists(&target.catalog, &target.namespace, &target.table)?
            {
                return Ok(StatementResult::Ok);
            }
            backend.create_table(crate::connector::backend::CreateTableRequest {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                columns,
                key_desc,
                bucket_count,
                partition_fields,
                properties,
            })?;
            if !legacy_range_partitions.is_empty() && target.backend_name == "managed" {
                state
                    .catalog
                    .write()
                    .expect("standalone catalog write lock")
                    .set_legacy_range_partitions(
                        &target.namespace,
                        &target.table,
                        legacy_range_partitions,
                    )?;
            }
            if target.backend_name == "iceberg" {
                persist_iceberg_table_if_needed(
                    state,
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
            }
            Ok(StatementResult::Ok)
        }
    }
}

pub(crate) fn execute_drop_catalog_statement(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let mut guard = state
        .iceberg_catalogs
        .write()
        .expect("standalone iceberg catalog write lock");
    match guard.drop_catalog(catalog_name) {
        Ok(()) => {
            drop(guard);
            delete_iceberg_catalog_if_needed(state, &normalize_identifier(catalog_name)?)?;
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("unknown catalog") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

pub(crate) fn execute_drop_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    if_exists: bool,
    force: bool,
) -> Result<StatementResult, String> {
    let target =
        crate::engine::backend_resolver::resolve_namespace_target(state, name, current_catalog)?;
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    if target.backend_name == "iceberg"
        && !backend.namespace_exists(&target.catalog, &target.namespace)?
    {
        return if if_exists {
            Ok(StatementResult::Ok)
        } else {
            Err(format!("unknown database `{}`", name.parts.join(".")))
        };
    }
    match backend.drop_namespace(&target.catalog, &target.namespace, force) {
        Ok(()) => {
            if target.backend_name == "iceberg" {
                delete_iceberg_namespace_if_needed(state, &target.catalog, &target.namespace)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
        Err(err) if if_exists && target.backend_name == "iceberg" && err.contains("namespace") => {
            Ok(StatementResult::Ok)
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn execute_drop_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
    _force: bool,
) -> Result<StatementResult, String> {
    let target = match crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        name,
        current_catalog,
        current_database,
    ) {
        Ok(target) => target,
        Err(_) if current_catalog.is_none() && name.parts.len() <= 2 => {
            // External parquet tables registered through the embedding API are
            // still catalog-only entries. Dropping them does not involve a
            // connector backend.
            return drop_local_catalog_table(state, name, current_database, if_exists);
        }
        Err(err) => return Err(err),
    };
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    match backend.drop_table(&target.catalog, &target.namespace, &target.table, if_exists) {
        Ok(()) => {
            if target.backend_name == "iceberg" {
                delete_iceberg_table_if_needed(
                    state,
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
                crate::engine::query_prep::drop_registered_external_table(
                    state,
                    &target.namespace,
                    &target.table,
                )?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("table") => {
            if target.backend_name == "iceberg" {
                crate::engine::query_prep::drop_registered_external_table(
                    state,
                    &target.namespace,
                    &target.table,
                )?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) => Err(err),
    }
}

fn drop_local_catalog_table(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_database: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name, current_database)?;
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    match guard.drop_table(&resolved.database, &resolved.table) {
        Ok(()) => Ok(StatementResult::Ok),
        Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

// ---------------------------------------------------------------------------
// DML handlers
// ---------------------------------------------------------------------------

pub(crate) fn execute_truncate_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    target_ref: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name, current_database)?;
    if current_catalog.is_none()
        && state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock")
            .contains_table(&resolved.database, &resolved.table)?
    {
        if target_ref != "main" {
            return Err(format!(
                "TRUNCATE TABLE: branch target `{target_ref}` is only supported for iceberg tables"
            ));
        }
        return truncate_managed_lake_table(state, &resolved.database, &resolved.table);
    }

    // Fall through to iceberg backend resolution.
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "TRUNCATE TABLE only supports managed-lake or iceberg tables: {}.{}",
            resolved.database, resolved.table
        ));
    }
    let catalog = {
        let reg = state.connectors.read().expect("connector registry read");
        reg.catalog_backend(target.backend_name)?
    };
    let resolved_table = catalog.load_table(&target.catalog, &target.namespace, &target.table)?;
    crate::engine::iceberg_truncate::execute_iceberg_truncate_table(
        state,
        &target,
        &resolved_table,
        target_ref,
    )
}

pub(crate) fn execute_insert_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    overwrite_mode: crate::sql::parser::ast::OverwriteMode,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    crate::engine::insert_flow::run_insert(
        state,
        name,
        columns,
        source,
        overwrite_mode,
        current_catalog,
        current_database,
    )
}

// ---------------------------------------------------------------------------
// ADD FILES SQL parsing
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AddEqualityDeleteStmt {
    pub(crate) table: ObjectName,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<Literal>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AlterIcebergSchemaStmt {
    pub(crate) table: ObjectName,
    pub(crate) change: IcebergSchemaChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlterIcebergPropertiesStmt {
    pub(crate) table: ObjectName,
    pub(crate) op: PropertiesOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PropertiesOp {
    Set { entries: Vec<(String, String)> },
    Unset { keys: Vec<String>, if_exists: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AlterTableOptimizeStmt {
    pub(crate) table: ObjectName,
}

#[derive(Clone, Debug)]
pub(crate) struct AlterTableExpireSnapshotsStmt {
    pub(crate) table: ObjectName,
    /// epoch-ms threshold; expire only snapshots with timestamp_ms < this.
    /// Mutually optional with retain_last but at least one must be Some
    /// (parser enforces).
    pub(crate) older_than_ms: Option<i64>,
    /// Retain at least N most-recent snapshots in the main ancestor chain.
    /// Must be >= 1 if Some (parser enforces).
    pub(crate) retain_last: Option<u32>,
}

#[derive(Clone, Debug)]
pub(crate) struct AlterTableRemoveOrphanFilesStmt {
    pub(crate) table: ObjectName,
    /// Mandatory (parser enforces). epoch-ms threshold; only files with
    /// last_modified_ms < this are eligible for removal.
    pub(crate) older_than_ms: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct AlterTableRewriteManifestsStmt {
    pub(crate) table: ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShowAlterTableOptimizeStmt {
    pub(crate) catalog: Option<String>,
    pub(crate) database: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) order_by_create_time_desc: bool,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AlterLegacyRangePartitionStmt {
    pub(crate) table: ObjectName,
    pub(crate) partition: LegacyRangePartition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnPath {
    segments: Vec<String>,
}

impl ColumnPath {
    pub(crate) fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub(crate) fn parse(input: &str) -> Result<Self, String> {
        if input.is_empty() {
            return Err("column path is empty".to_string());
        }
        let mut segments = Vec::new();
        for raw in input.split('.') {
            if raw.is_empty() {
                return Err(format!("invalid column path '{input}': empty segment"));
            }
            segments.push(raw.to_ascii_lowercase());
        }
        Ok(Self { segments })
    }

    pub(crate) fn from_segments(segments: Vec<String>) -> Self {
        Self {
            segments: segments
                .into_iter()
                .map(|s| s.to_ascii_lowercase())
                .collect(),
        }
    }

    pub(crate) fn segments(&self) -> &[String] {
        &self.segments
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub(crate) fn last(&self) -> Option<&str> {
        self.segments.last().map(String::as_str)
    }

    pub(crate) fn parent(&self) -> ColumnPath {
        if self.segments.is_empty() {
            return ColumnPath::root();
        }
        Self {
            segments: self.segments[..self.segments.len() - 1].to_vec(),
        }
    }

    pub(crate) fn dotted(&self) -> String {
        self.segments.join(".")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AddPosition {
    Default,
    First,
    After(String),
    Before(String),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IcebergSchemaChange {
    AddColumn {
        parent: ColumnPath,
        name: String,
        data_type: SqlType,
        default: Option<DefaultLiteral>,
        position: AddPosition,
    },
    DropColumn {
        path: ColumnPath,
    },
    RenameColumn {
        path: ColumnPath,
        new_name: String,
    },
    ModifyColumn {
        path: ColumnPath,
        new_type: SqlType,
    },
    SetNullable {
        path: ColumnPath,
        nullable: bool,
    },
    Reorder {
        path: ColumnPath,
        position: AddPosition,
    },
}

pub(crate) fn looks_like_alter_table_optimize(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    peek_token_word_eq(&parser, "OPTIMIZE")
}

pub(crate) fn looks_like_show_alter_table_optimize(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    parser.parse_keyword(Keyword::SHOW)
        && parser.parse_keyword(Keyword::ALTER)
        && parser.parse_keyword(Keyword::TABLE)
        && peek_token_word_eq(&parser, "OPTIMIZE")
}

pub(crate) fn parse_alter_table_optimize_sql(sql: &str) -> Result<AlterTableOptimizeStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE OPTIMIZE: {e}"))?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let mut table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;
    table.parts = table
        .parts
        .into_iter()
        .map(|part| normalize_identifier(&part))
        .collect::<Result<Vec<_>, _>>()?;
    expect_word(&mut parser, "OPTIMIZE")?;
    if peek_token_word_eq(&parser, "PARTITION") {
        return Err("OPTIMIZE only supports whole-table compaction".to_string());
    }
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser).map_err(|err| {
        if peek_token_word_eq(&parser, "PARTITION") {
            "OPTIMIZE only supports whole-table compaction".to_string()
        } else {
            format!("unsupported trailing ALTER TABLE OPTIMIZE tokens: {err}")
        }
    })?;
    Ok(AlterTableOptimizeStmt { table })
}

/// Detect `ALTER TABLE <name> REWRITE MANIFESTS` without full parsing.
///
/// Used for fast early-routing in `execute_in_context` before dispatching
/// to `parse_alter_table_rewrite_manifests_sql`.
pub(crate) fn looks_like_alter_table_rewrite_manifests(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    peek_token_word_eq(&parser, "REWRITE") && peek_token_word_eq_at(&parser, 1, "MANIFESTS")
}

/// Parse `ALTER TABLE <name> REWRITE MANIFESTS [;]`.
///
/// Rejects:
/// * Table names whose last part starts with `branch_` or `tag_` AND there are
///   at least 2 parts (spec §1.1: REWRITE MANIFESTS is table-level only).
/// * Trailing tokens after MANIFESTS (excluding an optional final semicolon).
pub(crate) fn parse_alter_table_rewrite_manifests_sql(
    sql: &str,
) -> Result<AlterTableRewriteManifestsStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE REWRITE MANIFESTS: {e}"))?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let mut table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;
    table.parts = table
        .parts
        .into_iter()
        .map(|part| normalize_identifier(&part))
        .collect::<Result<Vec<_>, _>>()?;

    // Reject branch / tag suffix per spec §1.1.
    // Rule: the LAST part starts with `branch_` or `tag_` AND there are ≥ 2 parts.
    // (A single-part table named `branch_x` is a legitimate table name, not a suffix.)
    if table.parts.len() >= 2
        && let Some(last) = table.parts.last()
            && (last.starts_with("branch_") || last.starts_with("tag_")) {
                return Err(format!(
                    "REWRITE MANIFESTS does not support branch/tag suffix on table name: {}",
                    table.parts.join(".")
                ));
            }

    expect_word(&mut parser, "REWRITE")?;
    expect_word(&mut parser, "MANIFESTS")?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser).map_err(|err| {
        format!("unsupported trailing ALTER TABLE REWRITE MANIFESTS tokens: {err}")
    })?;
    Ok(AlterTableRewriteManifestsStmt { table })
}

/// Detect `ALTER TABLE <name> EXPIRE SNAPSHOTS [OLDER THAN ...] [RETAIN LAST n]`
/// without full parsing. Used for fast early-routing before dispatching to
/// `parse_alter_table_expire_snapshots_sql`.
pub(crate) fn looks_like_alter_table_expire_snapshots(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    peek_token_word_eq(&parser, "EXPIRE") && peek_token_word_eq_at(&parser, 1, "SNAPSHOTS")
}

/// Parse `ALTER TABLE <name> EXPIRE SNAPSHOTS [OLDER THAN '<ts>'] [RETAIN LAST <n>] [;]`.
///
/// At least one of `OLDER THAN` or `RETAIN LAST` is required (spec §1.1).
/// Both clauses may appear in either order. `RETAIN LAST 0` is rejected.
///
/// Rejects:
/// * No clauses after SNAPSHOTS.
/// * `RETAIN LAST 0`.
/// * Branch/tag suffix (≥2 parts, last starts with `branch_` or `tag_`).
/// * Duplicate `OLDER THAN` clause.
/// * Trailing tokens after all clauses (excluding an optional final semicolon).
pub(crate) fn parse_alter_table_expire_snapshots_sql(
    sql: &str,
) -> Result<AlterTableExpireSnapshotsStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE EXPIRE SNAPSHOTS: {e}"))?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let mut table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;
    table.parts = table
        .parts
        .into_iter()
        .map(|part| normalize_identifier(&part))
        .collect::<Result<Vec<_>, _>>()?;

    // Reject branch / tag suffix per spec §1.1.
    // Rule: the LAST part starts with `branch_` or `tag_` AND there are ≥ 2 parts.
    if table.parts.len() >= 2
        && let Some(last) = table.parts.last()
            && (last.starts_with("branch_") || last.starts_with("tag_")) {
                return Err(format!(
                    "EXPIRE SNAPSHOTS does not support branch/tag suffix on table name: {}",
                    table.parts.join(".")
                ));
            }

    expect_word(&mut parser, "EXPIRE")?;
    expect_word(&mut parser, "SNAPSHOTS")?;

    // Parse optional clauses: OLDER THAN '<ts>' and RETAIN LAST <n>.
    // Both optional but at least one required. Order doesn't matter.
    let mut older_than_ms: Option<i64> = None;
    let mut retain_last: Option<u32> = None;
    loop {
        if peek_token_word_eq(&parser, "OLDER") {
            if older_than_ms.is_some() {
                return Err("EXPIRE SNAPSHOTS: duplicate OLDER THAN clause".to_string());
            }
            expect_word(&mut parser, "OLDER")?;
            expect_word(&mut parser, "THAN")?;
            older_than_ms = Some(parse_expire_timestamp_ms(&mut parser)?);
            continue;
        }
        if peek_token_word_eq(&parser, "RETAIN") {
            if retain_last.is_some() {
                return Err("EXPIRE SNAPSHOTS: duplicate RETAIN LAST clause".to_string());
            }
            expect_word(&mut parser, "RETAIN")?;
            expect_word(&mut parser, "LAST")?;
            let n = parse_expire_uint(&mut parser)?;
            if n == 0 {
                return Err("EXPIRE SNAPSHOTS: RETAIN LAST must be >= 1".to_string());
            }
            retain_last = Some(
                n.try_into()
                    .map_err(|_| "EXPIRE SNAPSHOTS: RETAIN LAST value too large".to_string())?,
            );
            continue;
        }
        break;
    }
    if older_than_ms.is_none() && retain_last.is_none() {
        return Err(
            "EXPIRE SNAPSHOTS requires at least OLDER THAN or RETAIN LAST clause".to_string(),
        );
    }
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser).map_err(|e| format!("unsupported trailing tokens: {e}"))?;
    Ok(AlterTableExpireSnapshotsStmt {
        table,
        older_than_ms,
        retain_last,
    })
}

/// Parse a timestamp for OLDER THAN: accepts `'YYYY-MM-DD HH:MM:SS'`,
/// `'<RFC 3339>'`, or a bare epoch-ms integer.
///
/// Duplicates the logic from `sql::analyzer::iceberg_ref::parse_timestamp_string`
/// (which is private) to avoid introducing a cross-module public API for a
/// one-off use. Both use the same chrono parsing strategy.
fn parse_expire_timestamp_ms(parser: &mut Parser<'_>) -> Result<i64, String> {
    use chrono::{DateTime, NaiveDateTime, Utc};
    let tok = parser.next_token();
    match tok.token {
        Token::SingleQuotedString(s) => {
            if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                return Ok(dt.with_timezone(&Utc).timestamp_millis());
            }
            if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                return Ok(ndt.and_utc().timestamp_millis());
            }
            Err(format!(
                "EXPIRE SNAPSHOTS: cannot parse timestamp '{s}'; \
                 expected RFC 3339 (e.g. '2026-04-01T00:00:00Z') \
                 or 'YYYY-MM-DD HH:MM:SS'"
            ))
        }
        Token::Number(n, _) => n
            .parse::<i64>()
            .map_err(|e| format!("EXPIRE SNAPSHOTS: invalid epoch-ms integer '{n}': {e}")),
        other => Err(format!(
            "EXPIRE SNAPSHOTS: expected timestamp literal (quoted string or integer), got {other}"
        )),
    }
}

/// Parse a non-negative integer token (for RETAIN LAST <n>).
fn parse_expire_uint(parser: &mut Parser<'_>) -> Result<u64, String> {
    let tok = parser.next_token();
    match tok.token {
        Token::Number(n, _) => n
            .parse::<u64>()
            .map_err(|e| format!("EXPIRE SNAPSHOTS: invalid RETAIN LAST value '{n}': {e}")),
        other => Err(format!(
            "EXPIRE SNAPSHOTS: expected integer for RETAIN LAST, got {other}"
        )),
    }
}

/// Detector for `ALTER TABLE x REMOVE ORPHAN FILES [OLDER THAN ...]`.
///
/// Used by `mod.rs::execute_in_context` to route before generic sqlparser-rs.
pub(crate) fn looks_like_alter_table_remove_orphan_files(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    peek_token_word_eq(&parser, "REMOVE")
        && peek_token_word_eq_at(&parser, 1, "ORPHAN")
        && peek_token_word_eq_at(&parser, 2, "FILES")
}

/// Parse `ALTER TABLE <name> REMOVE ORPHAN FILES OLDER THAN '<ts>' [;]`.
///
/// `OLDER THAN` is mandatory (spec §1.1; unlike EXPIRE SNAPSHOTS which can
/// use RETAIN LAST instead).
///
/// Rejects:
/// * No `OLDER THAN` clause.
/// * Branch/tag suffix (≥2 parts, last starts with `branch_` or `tag_`).
/// * Trailing tokens after the clause.
pub(crate) fn parse_alter_table_remove_orphan_files_sql(
    sql: &str,
) -> Result<AlterTableRemoveOrphanFilesStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE REMOVE ORPHAN FILES: {e}"))?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let mut table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;
    table.parts = table
        .parts
        .into_iter()
        .map(|part| normalize_identifier(&part))
        .collect::<Result<Vec<_>, _>>()?;

    // Reject branch / tag suffix per spec §1.1.
    if table.parts.len() >= 2
        && let Some(last) = table.parts.last()
            && (last.starts_with("branch_") || last.starts_with("tag_")) {
                return Err(format!(
                    "REMOVE ORPHAN FILES does not support branch/tag suffix on table name: {}",
                    table.parts.join(".")
                ));
            }

    expect_word(&mut parser, "REMOVE")?;
    expect_word(&mut parser, "ORPHAN")?;
    expect_word(&mut parser, "FILES")?;

    // OLDER THAN is mandatory for REMOVE ORPHAN FILES (spec §1.1 / §4.1).
    if !peek_token_word_eq(&parser, "OLDER") {
        return Err(
            "REMOVE ORPHAN FILES requires OLDER THAN clause (e.g. OLDER THAN '2026-01-01')"
                .to_string(),
        );
    }
    expect_word(&mut parser, "OLDER")?;
    expect_word(&mut parser, "THAN")?;
    let older_than_ms = parse_expire_timestamp_ms(&mut parser)?;

    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser).map_err(|e| format!("unsupported trailing tokens: {e}"))?;
    Ok(AlterTableRemoveOrphanFilesStmt {
        table,
        older_than_ms,
    })
}

/// Peek at the token `offset` positions ahead of current position.
///
/// offset=0 is the same as `peek_token_word_eq`, offset=1 is one token ahead.
fn peek_token_word_eq_at(parser: &Parser<'_>, offset: usize, word: &str) -> bool {
    matches!(
        &parser.peek_nth_token(offset).token,
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word)
    )
}

pub(crate) fn parse_show_alter_table_optimize_sql(
    sql: &str,
) -> Result<ShowAlterTableOptimizeStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse SHOW ALTER TABLE OPTIMIZE: {e}"))?;
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    expect_word(&mut parser, "OPTIMIZE")?;

    let (catalog, database) =
        if parser.parse_keyword(Keyword::FROM) || parser.parse_keyword(Keyword::IN) {
            let mut name = crate::sql::parser::dialect::convert_object_name(
                parser
                    .parse_object_name(false)
                    .map_err(|e| format!("parse SHOW ALTER TABLE OPTIMIZE namespace: {e}"))?,
            )?;
            name.parts = name
                .parts
                .into_iter()
                .map(|part| normalize_identifier(&part))
                .collect::<Result<Vec<_>, _>>()?;
            match name.parts.as_slice() {
                [database] => (None, Some(database.clone())),
                [catalog, database] => (Some(catalog.clone()), Some(database.clone())),
                _ => {
                    return Err(
                        "SHOW ALTER TABLE OPTIMIZE FROM only supports db or catalog.db".to_string(),
                    );
                }
            }
        } else {
            (None, None)
        };

    let table_name = if parser.parse_keyword(Keyword::WHERE) {
        let ident = parser
            .parse_identifier()
            .map_err(|e| format!("parse SHOW ALTER TABLE OPTIMIZE WHERE column: {e}"))?;
        if !ident.value.eq_ignore_ascii_case("TableName") {
            return Err(
                "SHOW ALTER TABLE OPTIMIZE only supports WHERE TableName = '...'".to_string(),
            );
        }
        if !parser.consume_token(&Token::Eq) {
            return Err(
                "SHOW ALTER TABLE OPTIMIZE only supports WHERE TableName = '...'".to_string(),
            );
        }
        let value = parser
            .parse_literal_string()
            .map_err(|e| format!("parse SHOW ALTER TABLE OPTIMIZE TableName filter: {e}"))?;
        Some(normalize_identifier(&value)?)
    } else {
        None
    };

    let mut order_by_create_time_desc = false;
    if parser.parse_keyword(Keyword::ORDER) {
        parser
            .expect_keyword(Keyword::BY)
            .map_err(|e| format!("parse SHOW ALTER TABLE OPTIMIZE ORDER BY: {e}"))?;
        let ident = parser
            .parse_identifier()
            .map_err(|e| format!("parse SHOW ALTER TABLE OPTIMIZE ORDER BY column: {e}"))?;
        if !ident.value.eq_ignore_ascii_case("CreateTime") {
            return Err("SHOW ALTER TABLE OPTIMIZE only supports ORDER BY CreateTime".to_string());
        }
        if parser.parse_keyword(Keyword::DESC) {
            order_by_create_time_desc = true;
        } else {
            let _ = parser.parse_keyword(Keyword::ASC);
        }
    }

    let limit = if parser.parse_keyword(Keyword::LIMIT) {
        let token = parser.next_token();
        let value = match token.token {
            Token::Number(value, false) => value,
            other => {
                return Err(format!(
                    "SHOW ALTER TABLE OPTIMIZE LIMIT expects number, got {other}"
                ));
            }
        };
        Some(
            value
                .parse::<usize>()
                .map_err(|e| format!("parse SHOW ALTER TABLE OPTIMIZE LIMIT: {e}"))?,
        )
    } else {
        None
    };

    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser)?;
    Ok(ShowAlterTableOptimizeStmt {
        catalog,
        database,
        table_name,
        order_by_create_time_desc,
        limit,
    })
}

pub(crate) fn looks_like_add_legacy_range_partition(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.starts_with("ALTER TABLE")
        && upper.contains(" ADD ")
        && upper.contains(" PARTITION ")
        && upper.contains(" VALUES ")
}

pub(crate) fn parse_add_legacy_range_partition_sql(
    sql: &str,
) -> Result<AlterLegacyRangePartitionStmt, String> {
    let dialect = StarRocksDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| format!("parse ALTER TABLE ADD PARTITION: {e}"))?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| format!("expected ALTER: {e}"))?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| format!("expected TABLE: {e}"))?;
    let table = crate::sql::parser::dialect::convert_object_name(
        parser
            .parse_object_name(false)
            .map_err(|e| format!("parse ALTER TABLE name: {e}"))?,
    )?;
    expect_word(&mut parser, "ADD")?;
    let _ = parser.parse_keyword(Keyword::TEMPORARY);
    expect_word(&mut parser, "PARTITION")?;
    let name = parser
        .parse_identifier()
        .map_err(|e| format!("expected partition name: {e}"))?
        .value;
    expect_word(&mut parser, "VALUES")?;
    let (lower_sql, upper_sql) =
        crate::sql::parser::dialect::create_table::parse_legacy_range_values(&mut parser)?;
    if parser.consume_token(&Token::SemiColon) && parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "unsupported trailing ALTER TABLE ADD PARTITION tokens: {}",
            parser.peek_token_ref().token
        ));
    }
    if parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "unsupported trailing ALTER TABLE ADD PARTITION tokens: {}",
            parser.peek_token_ref().token
        ));
    }
    Ok(AlterLegacyRangePartitionStmt {
        table,
        partition: LegacyRangePartition {
            name,
            column: String::new(),
            lower_sql,
            upper_sql,
        },
    })
}

/// Check if SQL looks like ALTER TABLE ... ADD FILES FROM ...
pub(crate) fn looks_like_add_files(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE") && upper.contains("ADD FILES FROM")
}

/// Parse: ALTER TABLE [catalog.db.]table ADD FILES FROM 's3://...'
pub(crate) fn parse_add_files_sql(sql: &str) -> Result<(Vec<String>, String), String> {
    // Extract the part between ALTER TABLE and ADD FILES FROM
    let upper = sql.to_ascii_uppercase();
    let alter_idx = upper.find("ALTER TABLE").ok_or("missing ALTER TABLE")?;
    let add_files_idx = upper
        .find("ADD FILES FROM")
        .ok_or("missing ADD FILES FROM")?;

    let table_str = sql[alter_idx + 11..add_files_idx].trim();
    let table_parts: Vec<String> = table_str
        .split('.')
        .map(|s| s.trim().trim_matches('`').to_lowercase())
        .collect();

    // Extract the path after ADD FILES FROM
    let after_from = &sql[add_files_idx + 14..];
    let path = after_from
        .trim()
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    if path.is_empty() {
        return Err("ADD FILES FROM requires a path".to_string());
    }

    Ok((table_parts, path))
}

pub(crate) fn looks_like_alter_iceberg_schema(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };

    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }

    if parser.parse_keyword(Keyword::ADD) {
        return parser.parse_keyword(Keyword::COLUMN);
    }
    if parser.parse_keyword(Keyword::DROP) {
        return parser.parse_keyword(Keyword::COLUMN);
    }
    if parser.parse_keyword(Keyword::RENAME) {
        return parser.parse_keyword(Keyword::COLUMN);
    }
    if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "MODIFY") {
        parser.next_token();
        return parser.parse_keyword(Keyword::COLUMN);
    }
    if parser.parse_keyword(Keyword::ALTER) {
        return parser.parse_keyword(Keyword::COLUMN);
    }
    false
}

pub(crate) fn looks_like_alter_iceberg_properties(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    // Use peek_word_eq for both SET and UNSET so no tokens are consumed before the check.
    if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "SET")
        && crate::sql::parser::dialect::peek_word_eq(&parser, 1, "TBLPROPERTIES")
    {
        return true;
    }
    if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "UNSET")
        && crate::sql::parser::dialect::peek_word_eq(&parser, 1, "TBLPROPERTIES")
    {
        return true;
    }
    false
}

pub(crate) fn parse_alter_iceberg_properties_sql(
    sql: &str,
) -> Result<AlterIcebergPropertiesStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE TBLPROPERTIES DDL: {e}"))?;

    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;

    let op = if parser.parse_keyword(Keyword::SET) {
        if !crate::sql::parser::dialect::peek_word_eq(&parser, 0, "TBLPROPERTIES") {
            return Err("expected TBLPROPERTIES after SET".to_string());
        }
        parser.next_token(); // consume TBLPROPERTIES
        let entries = parse_property_entries(&mut parser)?;
        PropertiesOp::Set { entries }
    } else if parser.parse_keyword(Keyword::UNSET) {
        if !crate::sql::parser::dialect::peek_word_eq(&parser, 0, "TBLPROPERTIES") {
            return Err("expected TBLPROPERTIES after UNSET".to_string());
        }
        parser.next_token(); // consume TBLPROPERTIES
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let keys = parse_property_keys(&mut parser)?;
        PropertiesOp::Unset { keys, if_exists }
    } else {
        return Err("expected SET or UNSET TBLPROPERTIES".to_string());
    };

    if parser.peek_token_ref().token == Token::SemiColon {
        parser.next_token();
    }
    if parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "unsupported trailing tokens at {}",
            parser.peek_token_ref().token
        ));
    }
    Ok(AlterIcebergPropertiesStmt { table, op })
}

fn parse_property_entries(parser: &mut Parser<'_>) -> Result<Vec<(String, String)>, String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| e.to_string())?;
    if parser.peek_token_ref().token == Token::RParen {
        parser.next_token();
        return Err("SET TBLPROPERTIES requires at least one key=value pair".to_string());
    }
    let mut entries = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    loop {
        let key = parse_string_literal(parser)?;
        parser.expect_token(&Token::Eq).map_err(|e| e.to_string())?;
        let value = parse_string_literal(parser)?;
        if !seen.insert(key.clone()) {
            return Err(format!("duplicate key '{key}' in SET TBLPROPERTIES"));
        }
        entries.push((key, value));
        if parser.consume_token(&Token::Comma) {
            continue;
        }
        break;
    }
    parser
        .expect_token(&Token::RParen)
        .map_err(|e| e.to_string())?;
    Ok(entries)
}

fn parse_property_keys(parser: &mut Parser<'_>) -> Result<Vec<String>, String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| e.to_string())?;
    if parser.peek_token_ref().token == Token::RParen {
        parser.next_token();
        return Err("UNSET TBLPROPERTIES requires at least one key".to_string());
    }
    let mut keys = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    loop {
        let key = parse_string_literal(parser)?;
        if !seen.insert(key.clone()) {
            return Err(format!("duplicate key '{key}' in UNSET TBLPROPERTIES"));
        }
        keys.push(key);
        if parser.consume_token(&Token::Comma) {
            continue;
        }
        break;
    }
    parser
        .expect_token(&Token::RParen)
        .map_err(|e| e.to_string())?;
    Ok(keys)
}

fn parse_string_literal(parser: &mut Parser<'_>) -> Result<String, String> {
    let tok = parser.next_token();
    match tok.token {
        Token::SingleQuotedString(s) => Ok(s),
        Token::DoubleQuotedString(s) => Ok(s),
        other => Err(format!(
            "TBLPROPERTIES key/value must be a string literal, got `{other}`"
        )),
    }
}

fn parse_column_path(parser: &mut Parser<'_>) -> Result<ColumnPath, String> {
    let mut segments = Vec::new();
    loop {
        let id = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        segments.push(id);
        if parser.consume_token(&Token::Period) {
            continue;
        }
        break;
    }
    Ok(ColumnPath::from_segments(segments))
}

pub(crate) fn parse_alter_iceberg_schema_sql(sql: &str) -> Result<AlterIcebergSchemaStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE schema DDL: {e}"))?;

    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;
    let table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;

    let change = if parser.parse_keywords(&[Keyword::ADD, Keyword::COLUMN]) {
        parse_add_column_change(&mut parser)?
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::COLUMN]) {
        let path = parse_column_path(&mut parser)?;
        if path.is_empty() {
            return Err("DROP COLUMN requires a column path".to_string());
        }
        IcebergSchemaChange::DropColumn { path }
    } else if parser.parse_keywords(&[Keyword::RENAME, Keyword::COLUMN]) {
        let path = parse_column_path(&mut parser)?;
        if path.is_empty() {
            return Err("RENAME COLUMN requires a column path".to_string());
        }
        parser
            .expect_keyword(Keyword::TO)
            .map_err(|e| e.to_string())?;
        let new_path = parse_column_path(&mut parser)?;
        if new_path.is_empty() {
            return Err("RENAME COLUMN target requires an identifier".to_string());
        }
        // The target may be a single identifier OR a dotted path whose parent
        // matches the source's parent (i.e. the rename does not move the column).
        let new_segments = new_path.segments();
        let src_parent = &path.segments()[..path.segments().len() - 1];
        let new_parent = &new_segments[..new_segments.len() - 1];
        if !new_parent.is_empty() && new_parent != src_parent {
            return Err(
                "RENAME COLUMN target must share the same parent path as the source".to_string(),
            );
        }
        IcebergSchemaChange::RenameColumn {
            path,
            new_name: new_segments.last().unwrap().clone(),
        }
    } else if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "MODIFY") {
        parser.next_token();
        parser
            .expect_keyword(Keyword::COLUMN)
            .map_err(|e| e.to_string())?;
        let path = parse_column_path(&mut parser)?;
        let new_type = crate::sql::parser::dialect::convert_sql_type(
            parser.parse_data_type().map_err(|e| e.to_string())?,
        )?;
        if parser.parse_keyword(Keyword::FIRST)
            || parser.parse_keyword(Keyword::AFTER)
            || crate::sql::parser::dialect::peek_word_eq(&parser, 0, "BEFORE")
        {
            return Err(
                "MODIFY COLUMN cannot combine type change with FIRST/AFTER/BEFORE; use a separate ALTER COLUMN statement".to_string(),
            );
        }
        IcebergSchemaChange::ModifyColumn { path, new_type }
    } else if parser.parse_keywords(&[Keyword::ALTER, Keyword::COLUMN]) {
        let path = parse_column_path(&mut parser)?;
        if path.is_empty() {
            return Err("ALTER COLUMN requires a column path".to_string());
        }
        if parser.parse_keyword(Keyword::FIRST) {
            IcebergSchemaChange::Reorder {
                path,
                position: AddPosition::First,
            }
        } else if parser.parse_keyword(Keyword::AFTER) {
            let target_path = parse_column_path(&mut parser)?;
            let last = target_path
                .segments()
                .last()
                .ok_or_else(|| "AFTER target empty".to_string())?
                .clone();
            IcebergSchemaChange::Reorder {
                path,
                position: AddPosition::After(last),
            }
        } else if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "BEFORE") {
            parser.next_token();
            let target_path = parse_column_path(&mut parser)?;
            let last = target_path
                .segments()
                .last()
                .ok_or_else(|| "BEFORE target empty".to_string())?
                .clone();
            IcebergSchemaChange::Reorder {
                path,
                position: AddPosition::Before(last),
            }
        } else if parser.parse_keywords(&[Keyword::SET, Keyword::NOT, Keyword::NULL]) {
            IcebergSchemaChange::SetNullable {
                path,
                nullable: false,
            }
        } else if parser.parse_keywords(&[Keyword::DROP, Keyword::NOT, Keyword::NULL]) {
            IcebergSchemaChange::SetNullable {
                path,
                nullable: true,
            }
        } else {
            return Err(
                "ALTER COLUMN must be followed by FIRST / AFTER / BEFORE / SET NOT NULL / DROP NOT NULL".to_string(),
            );
        }
    } else {
        return Err("unsupported ALTER TABLE schema evolution clause".to_string());
    };

    if parser.peek_token_ref().token == Token::SemiColon {
        parser.next_token();
    }
    if parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "unsupported trailing ALTER TABLE schema tokens starting at {}",
            parser.peek_token_ref().token
        ));
    }

    Ok(AlterIcebergSchemaStmt { table, change })
}

fn parse_add_column_change(parser: &mut Parser<'_>) -> Result<IcebergSchemaChange, String> {
    let path = parse_column_path(parser)?;
    if path.is_empty() {
        return Err("ADD COLUMN requires a column path".to_string());
    }
    let last = path.segments().last().unwrap().clone();
    let parent_segments = path.segments()[..path.segments().len() - 1].to_vec();
    let parent = ColumnPath::from_segments(parent_segments);

    let data_type = crate::sql::parser::dialect::convert_sql_type(
        parser.parse_data_type().map_err(|e| e.to_string())?,
    )?;
    let mut default: Option<DefaultLiteral> = None;
    let mut seen_null = false;
    let mut seen_default = false;
    let mut position = AddPosition::Default;
    let mut seen_position = false;
    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            return Err(
                "ADD COLUMN NOT NULL is not supported for Iceberg schema evolution".to_string(),
            );
        }
        if parser.parse_keyword(Keyword::NULL) {
            if seen_null {
                return Err("duplicate NULL clause in ADD COLUMN".to_string());
            }
            seen_null = true;
            continue;
        }
        if parser.parse_keyword(Keyword::DEFAULT) {
            if seen_default {
                return Err("duplicate DEFAULT clause in ADD COLUMN".to_string());
            }
            seen_default = true;
            // DEFAULT NULL keeps existing v2 behavior (does not persist).
            if parser.parse_keyword(Keyword::NULL) {
                default = Some(DefaultLiteral::Null);
                continue;
            }
            default = Some(
                crate::sql::parser::dialect::create_table::parse_default_literal(
                    parser, &data_type,
                )?,
            );
            continue;
        }
        if parser.parse_keyword(Keyword::FIRST) {
            if seen_position {
                return Err("duplicate column position clause in ADD COLUMN".to_string());
            }
            seen_position = true;
            position = AddPosition::First;
            continue;
        }
        if parser.parse_keyword(Keyword::AFTER) {
            if seen_position {
                return Err("duplicate column position clause in ADD COLUMN".to_string());
            }
            seen_position = true;
            let target = parser.parse_identifier().map_err(|e| e.to_string())?.value;
            position = AddPosition::After(target);
            continue;
        }
        if crate::sql::parser::dialect::peek_word_eq(parser, 0, "BEFORE") {
            if seen_position {
                return Err("duplicate column position clause in ADD COLUMN".to_string());
            }
            seen_position = true;
            parser.next_token();
            let target = parser.parse_identifier().map_err(|e| e.to_string())?.value;
            position = AddPosition::Before(target);
            continue;
        }
        break;
    }
    Ok(IcebergSchemaChange::AddColumn {
        parent,
        name: last,
        data_type,
        default,
        position,
    })
}

pub(crate) fn looks_like_alter_partition_column(sql: &str) -> bool {
    let mut parser = match Parser::new(&StarRocksDialect).try_with_sql(sql) {
        Ok(parser) => parser,
        Err(_) => return false,
    };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }

    (parser.parse_keyword(Keyword::ADD) || parser.parse_keyword(Keyword::DROP))
        && parser.parse_keyword(Keyword::PARTITION)
        && peek_token_word_eq(&parser, "COLUMN")
}

pub(crate) fn parse_alter_partition_column_sql(
    sql: &str,
) -> Result<crate::sql::parser::ast::AlterIcebergPartitionSpecStmt, String> {
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(sql)
        .map_err(|e| format!("parse ALTER TABLE partition column: {e}"))?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|e| format!("expected ALTER: {e}"))?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| format!("expected TABLE after ALTER: {e}"))?;

    let mut table = crate::sql::parser::dialect::convert_object_name(
        parser
            .parse_object_name(false)
            .map_err(|e| format!("parse ALTER TABLE name: {e}"))?,
    )?;
    table.parts = table
        .parts
        .into_iter()
        .map(|part| normalize_identifier(&part))
        .collect::<Result<Vec<_>, _>>()?;

    let is_add = if parser.parse_keyword(Keyword::ADD) {
        true
    } else if parser.parse_keyword(Keyword::DROP) {
        false
    } else {
        return Err("expected ADD or DROP before PARTITION COLUMN".to_string());
    };
    parser
        .expect_keyword(Keyword::PARTITION)
        .map_err(|e| format!("expected PARTITION after ADD/DROP: {e}"))?;
    expect_word(&mut parser, "COLUMN")?;

    let field = crate::sql::parser::dialect::create_table::parse_partition_field_expr(&mut parser)?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser)?;

    if is_add {
        Ok(
            crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                table,
                field,
            },
        )
    } else {
        Ok(
            crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::DropPartitionColumn {
                table,
                field,
            },
        )
    }
}

fn peek_token_word_eq(parser: &Parser<'_>, word: &str) -> bool {
    matches!(
        &parser.peek_token_ref().token,
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word)
    )
}

fn expect_word(parser: &mut Parser<'_>, word: &str) -> Result<(), String> {
    let token = parser.next_token();
    match token.token {
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word) => Ok(()),
        other => Err(format!("expected {word}, got {other}")),
    }
}

fn consume_optional_final_semicolon(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.consume_token(&Token::SemiColon) && parser.peek_token_ref().token == Token::SemiColon
    {
        return Err("only one final semicolon is allowed".to_string());
    }
    Ok(())
}

fn expect_parser_eof(parser: &Parser<'_>) -> Result<(), String> {
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!("unexpected token after statement: {other}")),
    }
}

/// Check if SQL looks like ALTER TABLE ... ADD EQUALITY DELETE (...) VALUES ...
pub(crate) fn looks_like_add_equality_delete(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE") && upper.contains("ADD EQUALITY DELETE")
}

/// Parse: ALTER TABLE [catalog.db.]table ADD EQUALITY DELETE (k1, k2) VALUES (...)
pub(crate) fn parse_add_equality_delete_sql(sql: &str) -> Result<AddEqualityDeleteStmt, String> {
    const ALTER_TABLE: &str = "ALTER TABLE";
    const ADD_EQ_DELETE: &str = "ADD EQUALITY DELETE";
    const VALUES: &str = "VALUES";

    let upper = sql.to_ascii_uppercase();
    let alter_idx = upper.find(ALTER_TABLE).ok_or("missing ALTER TABLE")?;
    let add_idx = upper
        .find(ADD_EQ_DELETE)
        .ok_or("missing ADD EQUALITY DELETE")?;
    let values_idx = upper[add_idx + ADD_EQ_DELETE.len()..]
        .find(VALUES)
        .map(|idx| add_idx + ADD_EQ_DELETE.len() + idx)
        .ok_or("missing VALUES")?;

    let table_str = sql[alter_idx + ALTER_TABLE.len()..add_idx].trim();
    let table_parts = table_str
        .split('.')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    if table_parts.is_empty() {
        return Err("ADD EQUALITY DELETE requires a table name".to_string());
    }

    let columns_part = sql[add_idx + ADD_EQ_DELETE.len()..values_idx].trim();
    let columns_inner = columns_part
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| "ADD EQUALITY DELETE requires columns in parentheses".to_string())?;
    let columns = columns_inner
        .split(',')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err("ADD EQUALITY DELETE requires at least one equality column".to_string());
    }

    let values_part = sql[values_idx + VALUES.len()..]
        .trim()
        .trim_end_matches(';');
    if values_part.is_empty() {
        return Err("ADD EQUALITY DELETE VALUES requires at least one row".to_string());
    }
    let fake_sql = format!(
        "INSERT INTO __eq_delete ({}) VALUES {values_part}",
        columns.join(", ")
    );
    let stmt = crate::sql::parser::parse_normalized_sql_raw(&fake_sql)
        .map_err(|e| format!("parse ADD EQUALITY DELETE VALUES: {e}"))?;
    let insert = match stmt {
        sqlparser::ast::Statement::Insert(insert) => insert,
        other => {
            return Err(format!(
                "internal ADD EQUALITY DELETE VALUES parser expected INSERT, got {other:?}"
            ));
        }
    };
    let converted = convert_sqlparser_insert_to_custom(&insert)?;
    let rows = match converted.source {
        InsertSource::Values(rows) => rows,
        other => {
            return Err(format!(
                "ADD EQUALITY DELETE expects literal VALUES rows, got {other:?}"
            ));
        }
    };
    Ok(AddEqualityDeleteStmt {
        table: ObjectName { parts: table_parts },
        columns,
        rows,
    })
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::ast::Literal;

    #[test]
    fn convert_update_from_table_source() {
        let stmt = crate::sql::parser::parse_sql_raw(
            "update ice.db1.t as t set v = s.v from staging.src as s where t.id = s.id",
        )
        .expect("parse");
        let sqlparser::ast::Statement::Update(_) = &stmt else {
            panic!("expected update statement: {stmt:?}");
        };
        let update = super::convert_sqlparser_update_to_custom(&stmt).expect("convert");
        assert_eq!(update.table.parts, vec!["ice", "db1", "t"]);
        assert_eq!(update.alias.as_deref(), Some("t"));
        assert_eq!(update.assignments.len(), 1);
        assert_eq!(update.assignments[0].column, "v");
        let Some(crate::sql::parser::ast::MutationSource::Table { name, alias }) = &update.source
        else {
            panic!("expected table source: {:?}", update.source);
        };
        assert_eq!(name.parts, vec!["staging", "src"]);
        assert_eq!(alias.as_deref(), Some("s"));
        assert!(update.where_clause.is_some());
    }

    #[test]
    fn convert_update_rejects_multi_column_assignment() {
        let stmt = crate::sql::parser::parse_sql_raw(
            "update ice.db1.t set (v1, v2) = (1, 2) where id = 1",
        )
        .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(err.contains("single-column UPDATE assignments"), "{err}");
    }

    #[test]
    fn convert_update_rejects_target_join() {
        let stmt = crate::sql::parser::parse_sql_raw(
            "update ice.db1.t as t join staging.src as s on t.id = s.id set v = s.v",
        )
        .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(
            err.contains("UPDATE target joins are not supported"),
            "{err}"
        );
    }

    #[test]
    fn convert_update_rejects_conflict_clause() {
        let stmt = crate::sql::parser::parse_sql_raw("update or ignore ice.db1.t set v = 1")
            .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(
            err.contains("UPDATE conflict clauses are not supported"),
            "{err}"
        );
    }

    #[test]
    fn convert_update_rejects_target_alias_column_list() {
        let stmt =
            crate::sql::parser::parse_sql_raw("update ice.db1.t as t(c) set v = 1").expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(
            err.contains("UPDATE target alias column lists are not supported"),
            "{err}"
        );
    }

    #[test]
    fn convert_update_rejects_source_alias_column_list() {
        let stmt = crate::sql::parser::parse_sql_raw(
            "update ice.db1.t set v = s.v from staging.src as s(id)",
        )
        .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(
            err.contains("UPDATE ... FROM source alias column lists are not supported"),
            "{err}"
        );
    }

    #[test]
    fn parse_add_equality_delete_values_statement() {
        let stmt = super::parse_add_equality_delete_sql(
            "ALTER TABLE ice.db.orders ADD EQUALITY DELETE (id, category) VALUES (2, 'B'), (4, 'A')",
        )
        .expect("parse");

        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
        assert_eq!(stmt.columns, vec!["id", "category"]);
        assert_eq!(
            stmt.rows,
            vec![
                vec![Literal::Int(2), Literal::String("B".to_string())],
                vec![Literal::Int(4), Literal::String("A".to_string())],
            ]
        );
    }

    #[test]
    fn parse_alter_table_optimize_accepts_three_part_table() {
        let stmt = super::parse_alter_table_optimize_sql("ALTER TABLE ice.db.orders OPTIMIZE")
            .expect("parse");

        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
    }

    #[test]
    fn parse_alter_table_optimize_rejects_partition_clause() {
        let err = super::parse_alter_table_optimize_sql(
            "ALTER TABLE ice.db.orders OPTIMIZE PARTITION (p1)",
        )
        .expect_err("partition should fail");

        assert!(err.contains("OPTIMIZE only supports whole-table compaction"));
    }

    #[test]
    fn parse_show_alter_table_optimize_extracts_filter() {
        let stmt = super::parse_show_alter_table_optimize_sql(
            "SHOW ALTER TABLE OPTIMIZE FROM ns WHERE TableName = 'orders' ORDER BY CreateTime DESC LIMIT 1",
        )
        .expect("parse");

        assert_eq!(stmt.database.as_deref(), Some("ns"));
        assert_eq!(stmt.table_name.as_deref(), Some("orders"));
        assert!(stmt.order_by_create_time_desc);
        assert_eq!(stmt.limit, Some(1));
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_default_null() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT DEFAULT NULL",
        )
        .expect("parse");

        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
        assert_eq!(
            stmt.change,
            super::IcebergSchemaChange::AddColumn {
                parent: super::ColumnPath::root(),
                name: "discount".to_string(),
                data_type: crate::sql::parser::ast::SqlType::Int,
                default: Some(super::DefaultLiteral::Null),
                position: super::AddPosition::Default,
            }
        );
    }

    #[test]
    fn parse_alter_iceberg_schema_drop_rename_modify() {
        let drop_stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE ice.db.orders DROP COLUMN old_col")
                .expect("drop");
        let super::IcebergSchemaChange::DropColumn { path } = drop_stmt.change else {
            panic!("expected DropColumn");
        };
        assert_eq!(path.dotted(), "old_col");

        let rename_stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders RENAME COLUMN old_col TO new_col",
        )
        .expect("rename");
        let super::IcebergSchemaChange::RenameColumn { path, new_name } = rename_stmt.change else {
            panic!("expected RenameColumn");
        };
        assert_eq!(path.dotted(), "old_col");
        assert_eq!(new_name, "new_col");

        let modify_stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders MODIFY COLUMN id BIGINT",
        )
        .expect("modify");
        let super::IcebergSchemaChange::ModifyColumn { path, new_type } = modify_stmt.change else {
            panic!("expected ModifyColumn");
        };
        assert_eq!(path.dotted(), "id");
        assert_eq!(new_type, crate::sql::parser::ast::SqlType::BigInt);
    }

    #[test]
    fn parse_alter_iceberg_schema_rejects_unsupported_add_forms() {
        let not_null = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT NOT NULL",
        )
        .expect_err("not null should fail");
        assert!(not_null.contains("ADD COLUMN NOT NULL is not supported"));

        // String literal as DEFAULT for an INT column must be rejected because
        // string values are not valid for integer columns.
        let type_mismatch = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT DEFAULT 'abc'",
        )
        .expect_err("string default for INT should fail");
        assert!(
            type_mismatch.contains("DEFAULT not supported"),
            "expected 'DEFAULT not supported' but got: {type_mismatch}"
        );
    }

    #[test]
    fn parse_alter_iceberg_schema_probe_matches_only_schema_clauses() {
        for sql in [
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT",
            "ALTER TABLE ice.db.orders DROP COLUMN old_col",
            "ALTER TABLE ice.db.orders RENAME COLUMN old_col TO new_col",
            "ALTER TABLE ice.db.orders MODIFY COLUMN id BIGINT",
        ] {
            assert!(
                super::looks_like_alter_iceberg_schema(sql),
                "expected schema DDL probe to match {sql}"
            );
        }

        for sql in [
            "ALTER TABLE ice.db.orders ADD FILES FROM 's3://bucket/path'",
            "ALTER TABLE ice.db.orders ADD EQUALITY DELETE (id) VALUES (1)",
            "ALTER TABLE ice.db.orders SET COMMENT = 'ADD COLUMN c INT'",
            "ALTER TABLE ice.db.orders /* ADD COLUMN c INT */ ADD FILES FROM 's3://bucket/path'",
            "ALTER TABLE ice.db.orders ADD PARTITION p1 VALUES LESS THAN (10)",
            "ALTER TABLE ice.db.orders ADD PARTITION COLUMN city",
        ] {
            assert!(
                !super::looks_like_alter_iceberg_schema(sql),
                "expected schema DDL probe not to match {sql}"
            );
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_rejects_trailing_unsupported_syntax() {
        let err = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN c INT COMMENT 'x'",
        )
        .expect_err("comment should fail");
        assert!(err.contains("unsupported trailing ALTER TABLE schema tokens"));
    }

    #[test]
    fn parse_alter_iceberg_schema_rejects_duplicate_add_column_attributes() {
        let duplicate_null = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN c INT NULL NULL",
        )
        .expect_err("duplicate null should fail");
        assert!(duplicate_null.contains("duplicate NULL"));

        let duplicate_default = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN c INT DEFAULT NULL DEFAULT NULL",
        )
        .expect_err("duplicate default should fail");
        assert!(duplicate_default.contains("duplicate DEFAULT clause"));
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_date_default() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c DATE DEFAULT '1970-01-02'",
        )
        .expect("date default");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn { default, .. } => {
                assert_eq!(default, Some(super::DefaultLiteral::Date(1)));
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_datetime_default() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c DATETIME DEFAULT '1970-01-01 00:00:01'",
        )
        .expect("datetime default");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn { default, .. } => {
                assert_eq!(default, Some(super::DefaultLiteral::DateTime(1_000_000)),);
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_alter_partition_column_statement() {
        use crate::sql::parser::ast::{
            AlterIcebergPartitionSpecStmt, IcebergPartitionFieldExpr, ObjectName,
        };

        assert!(super::looks_like_alter_partition_column(
            "alter table ice.db.orders add partition column city"
        ));
        assert_eq!(
            super::parse_alter_partition_column_sql(
                "ALTER TABLE ice.db.orders ADD PARTITION COLUMN city;"
            )
            .expect("parse add with final semicolon"),
            AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                table: ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()]
                },
                field: IcebergPartitionFieldExpr::Identity {
                    column: "city".to_string()
                }
            }
        );

        let add = super::parse_alter_partition_column_sql(
            "ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(user_id, 32)",
        )
        .expect("parse add");
        assert_eq!(
            add,
            AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                table: ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()]
                },
                field: IcebergPartitionFieldExpr::Bucket {
                    column: "user_id".to_string(),
                    num_buckets: 32
                }
            }
        );

        let drop = super::parse_alter_partition_column_sql(
            "ALTER TABLE ice.db.orders DROP PARTITION COLUMN month(ts)",
        )
        .expect("parse drop");
        assert_eq!(
            drop,
            AlterIcebergPartitionSpecStmt::DropPartitionColumn {
                table: ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()]
                },
                field: IcebergPartitionFieldExpr::Month {
                    column: "ts".to_string()
                }
            }
        );
    }

    #[test]
    fn parse_alter_partition_column_accepts_flexible_whitespace() {
        use crate::sql::parser::ast::{AlterIcebergPartitionSpecStmt, IcebergPartitionFieldExpr};

        assert!(super::looks_like_alter_partition_column(
            "ALTER TABLE ice.db.orders\nADD   PARTITION\tCOLUMN bucket(user_id, 32)"
        ));

        let add = super::parse_alter_partition_column_sql(
            "ALTER TABLE ice.db.orders\nADD   PARTITION\tCOLUMN bucket(user_id, 32)",
        )
        .expect("parse add");
        assert_eq!(
            add,
            AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                table: crate::sql::parser::ast::ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()]
                },
                field: IcebergPartitionFieldExpr::Bucket {
                    column: "user_id".to_string(),
                    num_buckets: 32
                }
            }
        );

        let drop = super::parse_alter_partition_column_sql(
            "ALTER TABLE ice.db.orders\tDROP\nPARTITION   COLUMN month(ts)",
        )
        .expect("parse drop");
        assert_eq!(
            drop,
            AlterIcebergPartitionSpecStmt::DropPartitionColumn {
                table: crate::sql::parser::ast::ObjectName {
                    parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()]
                },
                field: IcebergPartitionFieldExpr::Month {
                    column: "ts".to_string()
                }
            }
        );
    }

    #[test]
    fn parse_alter_partition_column_rejects_multi_statement_tails() {
        for sql in [
            "ALTER TABLE ice.db.orders; ADD PARTITION COLUMN bucket(user_id, 32)",
            "ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(user_id, 32); SELECT 1",
            "ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(user_id, 32);;",
        ] {
            assert!(
                super::parse_alter_partition_column_sql(sql).is_err(),
                "expected ALTER partition parse failure for {sql}"
            );
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_int_default() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c INT DEFAULT 5",
        )
        .expect("parsed");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn { default, .. } => {
                assert_eq!(default, Some(super::DefaultLiteral::Int(5)));
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_string_default() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c STRING DEFAULT 'hi'",
        )
        .expect("parsed");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn { default, .. } => {
                assert_eq!(default, Some(super::DefaultLiteral::String("hi".into())));
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_default_overflow_rejected() {
        let err = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c TINYINT DEFAULT 200",
        )
        .expect_err("overflow");
        assert!(err.contains("TINYINT"));
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_null_then_default_null() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c INT NULL DEFAULT NULL",
        )
        .expect("null before default null");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn { default, .. } => {
                assert_eq!(default, Some(super::DefaultLiteral::Null));
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_default_null_then_null() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c INT DEFAULT NULL NULL",
        )
        .expect("default null before null");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn { default, .. } => {
                assert_eq!(default, Some(super::DefaultLiteral::Null));
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_drop_nested_column() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t DROP COLUMN address.street")
                .unwrap();
        let super::IcebergSchemaChange::DropColumn { path } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "address.street");
    }

    #[test]
    fn parse_rename_nested_column() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE t RENAME COLUMN address.zip TO address.postal_code",
        )
        .unwrap();
        let super::IcebergSchemaChange::RenameColumn { path, new_name } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "address.zip");
        assert_eq!(new_name, "postal_code");
    }

    #[test]
    fn parse_modify_nested_column() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t MODIFY COLUMN address.zip BIGINT")
                .unwrap();
        let super::IcebergSchemaChange::ModifyColumn { path, new_type } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "address.zip");
        assert!(matches!(new_type, crate::sql::parser::ast::SqlType::BigInt));
    }

    #[test]
    fn parse_modify_array_element() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE t MODIFY COLUMN tags.element VARCHAR",
        )
        .unwrap();
        let super::IcebergSchemaChange::ModifyColumn { path, .. } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "tags.element");
    }

    #[test]
    fn parse_rename_extracts_only_last_segment_in_new_name() {
        assert!(
            super::parse_alter_iceberg_schema_sql(
                "ALTER TABLE t RENAME COLUMN address.zip TO foo.bar"
            )
            .is_err()
        );
    }

    #[test]
    fn parse_add_column_first() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t ADD COLUMN c INT FIRST").unwrap();
        let super::IcebergSchemaChange::AddColumn { position, .. } = stmt.change else {
            panic!();
        };
        assert!(matches!(position, super::AddPosition::First));
    }

    #[test]
    fn parse_add_column_after_target() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t ADD COLUMN c INT AFTER existing")
                .unwrap();
        let super::IcebergSchemaChange::AddColumn { position, .. } = stmt.change else {
            panic!();
        };
        assert!(matches!(position, super::AddPosition::After(ref s) if s == "existing"));
    }

    #[test]
    fn parse_add_column_before_target() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t ADD COLUMN c INT BEFORE existing")
                .unwrap();
        let super::IcebergSchemaChange::AddColumn { position, .. } = stmt.change else {
            panic!();
        };
        assert!(matches!(position, super::AddPosition::Before(ref s) if s == "existing"));
    }

    #[test]
    fn parse_add_column_into_nested_struct() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t ADD COLUMN address.zip INT")
                .unwrap();
        let super::IcebergSchemaChange::AddColumn { parent, name, .. } = stmt.change else {
            panic!();
        };
        assert_eq!(parent.dotted(), "address");
        assert_eq!(name, "zip");
    }

    #[test]
    fn parse_alter_column_first() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t ALTER COLUMN c FIRST").unwrap();
        let super::IcebergSchemaChange::Reorder { path, position } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "c");
        assert!(matches!(position, super::AddPosition::First));
    }

    #[test]
    fn parse_alter_column_after_target() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t ALTER COLUMN c AFTER d").unwrap();
        let super::IcebergSchemaChange::Reorder { path, position } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "c");
        assert!(matches!(position, super::AddPosition::After(ref s) if s == "d"));
    }

    #[test]
    fn parse_alter_column_nested_before() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE t ALTER COLUMN address.street BEFORE address.city",
        )
        .unwrap();
        let super::IcebergSchemaChange::Reorder { path, position } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "address.street");
        let super::AddPosition::Before(ref s) = position else {
            panic!();
        };
        assert_eq!(s, "city");
    }

    #[test]
    fn parse_alter_column_set_not_null() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t ALTER COLUMN c SET NOT NULL")
                .unwrap();
        let super::IcebergSchemaChange::SetNullable { path, nullable } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "c");
        assert!(!nullable);
    }

    #[test]
    fn parse_alter_column_drop_not_null() {
        let stmt =
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t ALTER COLUMN c DROP NOT NULL")
                .unwrap();
        let super::IcebergSchemaChange::SetNullable { path, nullable } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "c");
        assert!(nullable);
    }

    #[test]
    fn parse_alter_column_set_not_null_nested() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE t ALTER COLUMN address.street SET NOT NULL",
        )
        .unwrap();
        let super::IcebergSchemaChange::SetNullable { path, .. } = stmt.change else {
            panic!();
        };
        assert_eq!(path.dotted(), "address.street");
    }

    #[test]
    fn parse_modify_column_with_position_rejected() {
        assert!(
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t MODIFY COLUMN c BIGINT FIRST")
                .is_err()
        );
        assert!(
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t MODIFY COLUMN c BIGINT AFTER d")
                .is_err()
        );
        assert!(
            super::parse_alter_iceberg_schema_sql("ALTER TABLE t MODIFY COLUMN c BIGINT BEFORE d")
                .is_err()
        );
    }
}

#[cfg(test)]
mod parse_alter_iceberg_properties_tests {
    use super::{
        AlterIcebergPropertiesStmt, PropertiesOp, looks_like_alter_iceberg_properties,
        parse_alter_iceberg_properties_sql,
    };

    #[test]
    fn looks_like_set_tblproperties() {
        assert!(looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t SET TBLPROPERTIES ('k' = 'v')"
        ));
    }

    #[test]
    fn looks_like_unset_tblproperties() {
        assert!(looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t UNSET TBLPROPERTIES ('k')"
        ));
    }

    #[test]
    fn looks_like_unset_tblproperties_if_exists() {
        assert!(looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t UNSET TBLPROPERTIES IF EXISTS ('k')"
        ));
    }

    #[test]
    fn looks_like_does_not_match_alter_column() {
        assert!(!looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t ADD COLUMN c INT"
        ));
        assert!(!looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t ALTER COLUMN c FIRST"
        ));
    }

    #[test]
    fn parse_set_one_pair() {
        let stmt = parse_alter_iceberg_properties_sql(
            "ALTER TABLE ice.db.t SET TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')",
        )
        .expect("parse");
        assert_eq!(stmt.table.parts, vec!["ice", "db", "t"]);
        let PropertiesOp::Set { entries } = stmt.op else {
            panic!()
        };
        assert_eq!(
            entries,
            vec![(
                "write.parquet.compression-codec".to_string(),
                "zstd".to_string()
            )]
        );
    }

    #[test]
    fn parse_set_multiple_pairs() {
        let stmt = parse_alter_iceberg_properties_sql(
            "ALTER TABLE t SET TBLPROPERTIES ('a' = 'x', 'b' = 'y', 'c' = 'z')",
        )
        .expect("parse");
        let PropertiesOp::Set { entries } = stmt.op else {
            panic!()
        };
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], ("a".to_string(), "x".to_string()));
        assert_eq!(entries[2], ("c".to_string(), "z".to_string()));
    }

    #[test]
    fn parse_unset_strict() {
        let stmt =
            parse_alter_iceberg_properties_sql("ALTER TABLE t UNSET TBLPROPERTIES ('a', 'b')")
                .expect("parse");
        let PropertiesOp::Unset { keys, if_exists } = stmt.op else {
            panic!()
        };
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
        assert!(!if_exists);
    }

    #[test]
    fn parse_unset_if_exists() {
        let stmt =
            parse_alter_iceberg_properties_sql("ALTER TABLE t UNSET TBLPROPERTIES IF EXISTS ('a')")
                .expect("parse");
        let PropertiesOp::Unset { keys, if_exists } = stmt.op else {
            panic!()
        };
        assert_eq!(keys, vec!["a".to_string()]);
        assert!(if_exists);
    }

    #[test]
    fn parse_set_empty_parens_rejected() {
        assert!(parse_alter_iceberg_properties_sql("ALTER TABLE t SET TBLPROPERTIES ()").is_err());
    }

    #[test]
    fn parse_unset_empty_parens_rejected() {
        assert!(
            parse_alter_iceberg_properties_sql("ALTER TABLE t UNSET TBLPROPERTIES ()").is_err()
        );
    }

    #[test]
    fn parse_set_duplicate_key_rejected() {
        let res = parse_alter_iceberg_properties_sql(
            "ALTER TABLE t SET TBLPROPERTIES ('a' = 'x', 'a' = 'y')",
        );
        assert!(res.is_err());
        assert!(res.unwrap_err().to_lowercase().contains("duplicate"));
    }

    #[test]
    fn parse_unset_duplicate_key_rejected() {
        let res =
            parse_alter_iceberg_properties_sql("ALTER TABLE t UNSET TBLPROPERTIES ('a', 'a')");
        assert!(res.is_err());
        assert!(res.unwrap_err().to_lowercase().contains("duplicate"));
    }

    #[test]
    fn parse_unquoted_key_rejected() {
        // Keys must be string literals, not identifiers.
        assert!(
            parse_alter_iceberg_properties_sql("ALTER TABLE t SET TBLPROPERTIES (foo = 'bar')")
                .is_err()
        );
    }
}

#[cfg(test)]
mod column_path_tests {
    use super::ColumnPath;

    #[test]
    fn column_path_parses_single_segment() {
        let p = ColumnPath::parse("address").unwrap();
        assert_eq!(p.segments(), &["address".to_string()]);
        assert!(!p.is_empty());
    }

    #[test]
    fn column_path_parses_dotted() {
        let p = ColumnPath::parse("address.street").unwrap();
        assert_eq!(p.segments(), &["address".to_string(), "street".to_string()]);
    }

    #[test]
    fn column_path_normalizes_case() {
        let p = ColumnPath::parse("Address.Street").unwrap();
        assert_eq!(p.segments(), &["address".to_string(), "street".to_string()]);
    }

    #[test]
    fn column_path_rejects_empty_segment() {
        assert!(ColumnPath::parse("address.").is_err());
        assert!(ColumnPath::parse(".street").is_err());
        assert!(ColumnPath::parse("").is_err());
        assert!(ColumnPath::parse("a..b").is_err());
    }

    #[test]
    fn column_path_root_is_empty() {
        assert!(ColumnPath::root().is_empty());
        assert!(ColumnPath::root().segments().is_empty());
    }

    #[test]
    fn add_position_default_constructed() {
        use super::AddPosition;
        let pos = AddPosition::Default;
        assert!(matches!(pos, AddPosition::Default));
    }

    #[test]
    fn add_position_variants_construct() {
        use super::AddPosition;
        let _ = AddPosition::First;
        let _ = AddPosition::After("col_a".to_string());
        let _ = AddPosition::Before("col_b".to_string());
    }
}

#[cfg(test)]
mod insert_overwrite_partitions_parser_tests {
    // ----- INSERT OVERWRITE PARTITIONS parser tests -----

    /// Normalize + parse + convert an INSERT statement through the full pipeline.
    fn parse_insert_overwrite(sql: &str) -> crate::sql::parser::ast::InsertStmt {
        use crate::sql::parser::dialect::{StarRocksDialect, normalize_for_raw_parse};
        let normalized = normalize_for_raw_parse(sql).expect("normalize");
        let statements =
            sqlparser::parser::Parser::parse_sql(&StarRocksDialect, &normalized).expect("parse");
        let sqlparser::ast::Statement::Insert(insert) = &statements[0] else {
            panic!("expected INSERT statement; got {:?}", statements[0]);
        };
        super::convert_sqlparser_insert_to_custom(insert).expect("convert")
    }

    #[test]
    fn parse_insert_overwrite_partitions_table() {
        let stmt = parse_insert_overwrite("INSERT OVERWRITE PARTITIONS TABLE t SELECT * FROM s");
        assert_eq!(
            stmt.overwrite_mode,
            crate::sql::parser::ast::OverwriteMode::DynamicPartitions
        );
        assert_eq!(stmt.table.parts, vec!["t"]);
    }

    #[test]
    fn parse_insert_overwrite_partitions_no_table_keyword() {
        let stmt = parse_insert_overwrite("INSERT OVERWRITE PARTITIONS t VALUES (1)");
        assert_eq!(
            stmt.overwrite_mode,
            crate::sql::parser::ast::OverwriteMode::DynamicPartitions
        );
        assert_eq!(stmt.table.parts, vec!["t"]);
    }

    #[test]
    fn parse_insert_overwrite_table_remains_full_table() {
        let stmt = parse_insert_overwrite("INSERT OVERWRITE TABLE t SELECT * FROM s");
        assert_eq!(
            stmt.overwrite_mode,
            crate::sql::parser::ast::OverwriteMode::FullTable
        );
        assert_eq!(stmt.table.parts, vec!["t"]);
    }

    #[test]
    fn insert_select_generate_series_routes_via_standard_query_plan() {
        let stmt = parse_insert_overwrite(
            "INSERT INTO t SELECT generate_series FROM TABLE(generate_series(1, 3))",
        );
        assert!(
            matches!(
                stmt.source,
                crate::sql::parser::ast::InsertSource::FromQuery(_)
            ),
            "generate_series INSERT SELECT must use the standard table-function query plan"
        );
    }

    #[test]
    fn parse_insert_overwrite_partitions_with_branch() {
        // Branch resolution happens later in run_insert via split_ref_suffix;
        // here we just verify the overwrite mode and that the branch segment
        // is preserved in the table name.
        let stmt = parse_insert_overwrite("INSERT OVERWRITE PARTITIONS t.branch_dev VALUES (1)");
        assert_eq!(
            stmt.overwrite_mode,
            crate::sql::parser::ast::OverwriteMode::DynamicPartitions
        );
        assert_eq!(stmt.table.parts, vec!["t", "branch_dev"]);
    }

    // ---- looks_like_alter_table_rewrite_manifests tests ----

    #[test]
    fn looks_like_alter_table_rewrite_manifests_positive() {
        assert!(super::looks_like_alter_table_rewrite_manifests(
            "ALTER TABLE x REWRITE MANIFESTS"
        ));
        assert!(super::looks_like_alter_table_rewrite_manifests(
            "alter table x rewrite manifests;"
        ));
        assert!(super::looks_like_alter_table_rewrite_manifests(
            "ALTER TABLE ice.db.orders REWRITE MANIFESTS"
        ));
    }

    #[test]
    fn looks_like_alter_table_rewrite_manifests_negative() {
        // Different action keyword.
        assert!(!super::looks_like_alter_table_rewrite_manifests(
            "ALTER TABLE x OPTIMIZE"
        ));
        assert!(!super::looks_like_alter_table_rewrite_manifests(
            "ALTER TABLE x EXPIRE SNAPSHOTS"
        ));
        assert!(!super::looks_like_alter_table_rewrite_manifests(
            "ALTER TABLE x REWRITE DATA FILES"
        ));
        // Missing MANIFESTS.
        assert!(!super::looks_like_alter_table_rewrite_manifests(
            "ALTER TABLE x REWRITE"
        ));
    }

    // ---- parse_alter_table_rewrite_manifests_sql tests ----

    #[test]
    fn parse_alter_table_rewrite_manifests_basic() {
        let stmt = super::parse_alter_table_rewrite_manifests_sql(
            "ALTER TABLE ice.db.orders REWRITE MANIFESTS",
        )
        .unwrap();
        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
    }

    #[test]
    fn parse_alter_table_rewrite_manifests_single_part_table() {
        let stmt =
            super::parse_alter_table_rewrite_manifests_sql("ALTER TABLE orders REWRITE MANIFESTS")
                .unwrap();
        assert_eq!(stmt.table.parts, vec!["orders"]);
    }

    #[test]
    fn parse_alter_table_rewrite_manifests_accepts_trailing_semicolon() {
        let stmt =
            super::parse_alter_table_rewrite_manifests_sql("ALTER TABLE t REWRITE MANIFESTS;")
                .unwrap();
        assert_eq!(stmt.table.parts, vec!["t"]);
    }

    #[test]
    fn parse_alter_table_rewrite_manifests_rejects_branch_suffix() {
        // Two-part name ending in branch_ → rejected.
        let err = super::parse_alter_table_rewrite_manifests_sql(
            "ALTER TABLE db.orders.branch_dev REWRITE MANIFESTS",
        )
        .unwrap_err();
        assert!(
            err.contains("does not support branch"),
            "expected branch rejection, got: {err}"
        );

        // tag_ suffix also rejected.
        let err2 = super::parse_alter_table_rewrite_manifests_sql(
            "ALTER TABLE db.orders.tag_v1 REWRITE MANIFESTS",
        )
        .unwrap_err();
        assert!(
            err2.contains("does not support branch"),
            "expected tag rejection, got: {err2}"
        );
    }

    #[test]
    fn parse_alter_table_rewrite_manifests_single_part_branch_name_accepted() {
        // A single-part table literally named `branch_x` is a legitimate table
        // name (there's no preceding table part, so it can't be a suffix).
        let stmt = super::parse_alter_table_rewrite_manifests_sql(
            "ALTER TABLE branch_x REWRITE MANIFESTS",
        )
        .unwrap();
        assert_eq!(stmt.table.parts, vec!["branch_x"]);
    }

    #[test]
    fn parse_alter_table_rewrite_manifests_rejects_trailing_tokens() {
        let err = super::parse_alter_table_rewrite_manifests_sql(
            "ALTER TABLE x REWRITE MANIFESTS WHERE size_in_bytes < 100",
        )
        .unwrap_err();
        assert!(
            err.contains("unsupported trailing"),
            "expected trailing token rejection, got: {err}"
        );
    }

    // ---- looks_like_alter_table_expire_snapshots tests ----

    #[test]
    fn looks_like_alter_table_expire_snapshots_positive() {
        assert!(super::looks_like_alter_table_expire_snapshots(
            "ALTER TABLE x EXPIRE SNAPSHOTS RETAIN LAST 5"
        ));
        assert!(super::looks_like_alter_table_expire_snapshots(
            "alter table db.t expire snapshots older than '2026-01-01 00:00:00'"
        ));
        assert!(super::looks_like_alter_table_expire_snapshots(
            "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS"
        ));
    }

    #[test]
    fn looks_like_alter_table_expire_snapshots_negative() {
        // Different action keyword.
        assert!(!super::looks_like_alter_table_expire_snapshots(
            "ALTER TABLE x REWRITE MANIFESTS"
        ));
        assert!(!super::looks_like_alter_table_expire_snapshots(
            "ALTER TABLE x OPTIMIZE"
        ));
        // Missing SNAPSHOTS.
        assert!(!super::looks_like_alter_table_expire_snapshots(
            "ALTER TABLE x EXPIRE"
        ));
        // Not an ALTER TABLE.
        assert!(!super::looks_like_alter_table_expire_snapshots(
            "SELECT * FROM t"
        ));
    }

    // ---- parse_alter_table_expire_snapshots_sql tests ----

    #[test]
    fn parse_expire_older_than_only() {
        let stmt = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS OLDER THAN '2026-04-01 00:00:00'",
        )
        .unwrap();
        assert!(stmt.older_than_ms.is_some(), "older_than_ms should be Some");
        assert_eq!(stmt.retain_last, None);
        assert_eq!(stmt.table.parts, vec!["db", "t"]);
    }

    #[test]
    fn parse_expire_retain_last_only() {
        let stmt = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS RETAIN LAST 5",
        )
        .unwrap();
        assert_eq!(stmt.older_than_ms, None);
        assert_eq!(stmt.retain_last, Some(5));
    }

    #[test]
    fn parse_expire_both_clauses_older_then_retain() {
        let stmt = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS OLDER THAN '2026-04-01T00:00:00Z' RETAIN LAST 3",
        )
        .unwrap();
        assert!(stmt.older_than_ms.is_some());
        assert_eq!(stmt.retain_last, Some(3));
    }

    #[test]
    fn parse_expire_both_clauses_retain_then_older() {
        // Clauses in reverse order — both accepted.
        let stmt = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS RETAIN LAST 2 OLDER THAN '2026-04-01 00:00:00'",
        )
        .unwrap();
        assert!(stmt.older_than_ms.is_some());
        assert_eq!(stmt.retain_last, Some(2));
    }

    #[test]
    fn parse_expire_epoch_ms_int_accepted() {
        let stmt = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS OLDER THAN 1700000000000",
        )
        .unwrap();
        assert_eq!(stmt.older_than_ms, Some(1_700_000_000_000));
        assert_eq!(stmt.retain_last, None);
    }

    #[test]
    fn parse_expire_no_clause_rejects() {
        let err =
            super::parse_alter_table_expire_snapshots_sql("ALTER TABLE db.t EXPIRE SNAPSHOTS")
                .unwrap_err();
        assert!(
            err.contains("requires at least"),
            "expected 'requires at least' in error, got: {err}"
        );
    }

    #[test]
    fn parse_expire_retain_zero_rejects() {
        let err = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS RETAIN LAST 0",
        )
        .unwrap_err();
        assert!(
            err.contains("RETAIN LAST must be >= 1"),
            "expected retain-zero rejection, got: {err}"
        );
    }

    #[test]
    fn parse_expire_branch_suffix_rejects() {
        let err = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t.branch_dev EXPIRE SNAPSHOTS RETAIN LAST 5",
        )
        .unwrap_err();
        assert!(
            err.contains("does not support branch/tag suffix"),
            "expected branch suffix rejection, got: {err}"
        );

        // tag_ suffix also rejected.
        let err2 = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t.tag_v1 EXPIRE SNAPSHOTS RETAIN LAST 5",
        )
        .unwrap_err();
        assert!(
            err2.contains("does not support branch/tag suffix"),
            "expected tag suffix rejection, got: {err2}"
        );
    }

    #[test]
    fn parse_expire_branch_name_single_part_accepted() {
        // A single-part table literally named `branch_x` is valid.
        let stmt = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE branch_x EXPIRE SNAPSHOTS RETAIN LAST 5",
        )
        .unwrap();
        assert_eq!(stmt.table.parts, vec!["branch_x"]);
    }

    #[test]
    fn parse_expire_duplicate_older_than_rejects() {
        let err = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS OLDER THAN '2026-01-01 00:00:00' OLDER THAN '2026-02-01 00:00:00'",
        )
        .unwrap_err();
        assert!(
            err.contains("duplicate OLDER THAN"),
            "expected duplicate-clause rejection, got: {err}"
        );
    }

    #[test]
    fn parse_expire_accepts_trailing_semicolon() {
        let stmt = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS RETAIN LAST 1;",
        )
        .unwrap();
        assert_eq!(stmt.retain_last, Some(1));
    }

    #[test]
    fn parse_expire_rejects_trailing_tokens() {
        let err = super::parse_alter_table_expire_snapshots_sql(
            "ALTER TABLE db.t EXPIRE SNAPSHOTS RETAIN LAST 5 SOMETHING EXTRA",
        )
        .unwrap_err();
        assert!(
            err.contains("unsupported trailing tokens"),
            "expected trailing token rejection, got: {err}"
        );
    }

    // ---- looks_like_alter_table_remove_orphan_files tests ----

    #[test]
    fn looks_like_alter_table_remove_orphan_files_positive() {
        assert!(super::looks_like_alter_table_remove_orphan_files(
            "ALTER TABLE x REMOVE ORPHAN FILES OLDER THAN '2026-01-01'"
        ));
        assert!(super::looks_like_alter_table_remove_orphan_files(
            "alter table db.t remove orphan files older than '2026-01-01 00:00:00'"
        ));
        assert!(super::looks_like_alter_table_remove_orphan_files(
            "ALTER TABLE ice.db.orders REMOVE ORPHAN FILES OLDER THAN 1700000000000"
        ));
        // Detector only checks REMOVE ORPHAN FILES — OLDER THAN is not required by detector.
        assert!(super::looks_like_alter_table_remove_orphan_files(
            "ALTER TABLE x REMOVE ORPHAN FILES"
        ));
    }

    #[test]
    fn looks_like_alter_table_remove_orphan_files_negative() {
        // Different action keyword.
        assert!(!super::looks_like_alter_table_remove_orphan_files(
            "ALTER TABLE x EXPIRE SNAPSHOTS"
        ));
        assert!(!super::looks_like_alter_table_remove_orphan_files(
            "ALTER TABLE x OPTIMIZE"
        ));
        assert!(!super::looks_like_alter_table_remove_orphan_files(
            "ALTER TABLE x REWRITE MANIFESTS"
        ));
        // Missing ORPHAN.
        assert!(!super::looks_like_alter_table_remove_orphan_files(
            "ALTER TABLE x REMOVE FILES"
        ));
        // Missing FILES.
        assert!(!super::looks_like_alter_table_remove_orphan_files(
            "ALTER TABLE x REMOVE ORPHAN"
        ));
        // Not an ALTER TABLE.
        assert!(!super::looks_like_alter_table_remove_orphan_files(
            "SELECT * FROM t"
        ));
    }

    // ---- parse_alter_table_remove_orphan_files_sql tests ----

    #[test]
    fn parse_remove_orphan_files_basic() {
        let stmt = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE ice.db.orders REMOVE ORPHAN FILES OLDER THAN '2026-01-01 00:00:00'",
        )
        .unwrap();
        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
        // 2026-01-01 00:00:00 UTC in epoch-ms.
        assert_eq!(stmt.older_than_ms, 1_767_225_600_000i64);
    }

    #[test]
    fn parse_remove_orphan_files_rfc3339_timestamp() {
        let stmt = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE db.t REMOVE ORPHAN FILES OLDER THAN '2026-01-01T00:00:00Z'",
        )
        .unwrap();
        assert_eq!(stmt.older_than_ms, 1_767_225_600_000i64);
    }

    #[test]
    fn parse_remove_orphan_files_epoch_ms_int() {
        let stmt = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE db.t REMOVE ORPHAN FILES OLDER THAN 1700000000000",
        )
        .unwrap();
        assert_eq!(stmt.older_than_ms, 1_700_000_000_000i64);
    }

    #[test]
    fn parse_remove_orphan_files_accepts_trailing_semicolon() {
        let stmt = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE t REMOVE ORPHAN FILES OLDER THAN '2026-01-01 00:00:00';",
        )
        .unwrap();
        assert_eq!(stmt.table.parts, vec!["t"]);
    }

    #[test]
    fn parse_remove_orphan_files_no_older_than_rejects() {
        let err = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE db.t REMOVE ORPHAN FILES",
        )
        .unwrap_err();
        assert!(
            err.contains("OLDER THAN"),
            "expected OLDER THAN rejection, got: {err}"
        );
    }

    #[test]
    fn parse_remove_orphan_files_branch_suffix_rejects() {
        // Two-part name ending in branch_ → rejected.
        let err = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE db.t.branch_dev REMOVE ORPHAN FILES OLDER THAN '2026-01-01 00:00:00'",
        )
        .unwrap_err();
        assert!(
            err.contains("does not support branch"),
            "expected branch rejection, got: {err}"
        );

        // tag_ suffix also rejected.
        let err2 = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE db.t.tag_v1 REMOVE ORPHAN FILES OLDER THAN '2026-01-01 00:00:00'",
        )
        .unwrap_err();
        assert!(
            err2.contains("does not support branch"),
            "expected tag rejection, got: {err2}"
        );
    }

    #[test]
    fn parse_remove_orphan_files_single_part_branch_name_accepted() {
        // A single-part table literally named `branch_x` is a valid table name.
        let stmt = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE branch_x REMOVE ORPHAN FILES OLDER THAN '2026-01-01 00:00:00'",
        )
        .unwrap();
        assert_eq!(stmt.table.parts, vec!["branch_x"]);
    }

    #[test]
    fn parse_remove_orphan_files_rejects_trailing_tokens() {
        let err = super::parse_alter_table_remove_orphan_files_sql(
            "ALTER TABLE db.t REMOVE ORPHAN FILES OLDER THAN '2026-01-01 00:00:00' EXTRA TOKEN",
        )
        .unwrap_err();
        assert!(
            err.contains("unsupported trailing tokens"),
            "expected trailing token rejection, got: {err}"
        );
    }
}
