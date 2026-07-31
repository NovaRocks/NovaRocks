// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DDL/DML statement handlers for the standalone engine.
//!
//! Top-level dispatchers route statement families that remain in the core
//! command kernel to connector-owned catalogs based on the parsed name and
//! current catalog/database session context.

use std::sync::Arc;

use crate::engine::{
    StandaloneState, StatementResult, delete_catalog_attachment_if_needed,
    retire_iceberg_control_binding,
};
use crate::sql::parser::ast::{CreateTableKind, DefaultLiteral, Literal, ObjectName};
use crate::sql::parser::dialect::StarRocksDialect;
use bytes::Bytes;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::identifier::resolve_local_table_name;
use novarocks_catalog::schema::SqlType;
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorColumnAggregation, ConnectorColumnDefinition,
    ConnectorDataType, ConnectorDefaultValue, ConnectorDropTableDataDisposition,
    ConnectorInstanceId, ConnectorNamespaceIdentity, ConnectorPartitionTransform,
    ConnectorTableIdentity, ConnectorTableKey, ConnectorTableKeyKind, CreatePolicy, DropPolicy,
};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::sql::literal::sqlparser_expr_to_literal;

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
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target =
        crate::engine::backend_resolver::resolve_namespace_target(state, name, current_catalog)?;
    let instance_id = mutation_instance_id(&target.catalog)?;
    crate::connector::mutation::execute_catalog_mutation(
        state.connector_control.as_ref(),
        &instance_id,
        ConnectorCatalogMutationOperation::CreateNamespace {
            namespace: ConnectorNamespaceIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace),
            },
            policy: if if_not_exists {
                CreatePolicy::NoOpIfExists
            } else {
                CreatePolicy::FailIfExists
            },
        },
        connector_context.clone(),
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_create_table_statement(
    state: &Arc<StandaloneState>,
    stmt: crate::sql::parser::ast::CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let legacy_range_partitions = stmt.legacy_range_partitions.clone();
    // CTAS dispatch: when the statement carries an AS SELECT clause, route
    // targets to the iceberg helper. The parser already rejected non-iceberg-
    // compatible CTAS forms (branch target / format-version=2 / explicit
    // columns / etc.).
    if stmt.as_select.is_some() {
        crate::engine::backend_resolver::resolve_table_target(
            state,
            &stmt.name,
            current_catalog,
            current_database,
        )?;
        return crate::engine::iceberg_ctas::execute_iceberg_ctas(
            state,
            stmt,
            current_catalog,
            current_database,
            connector_context,
        );
    }
    match stmt.kind {
        CreateTableKind::Iceberg {
            columns,
            key_desc,
            bucket_count,
            distribution_columns,
            partition_fields,
            properties,
        } => {
            // BITMAP / HLL columns cannot be used as distribution keys —
            // they are opaque blobs with no hash semantics that match a
            // scalar column. Reject the CREATE TABLE before any catalog
            // mutation. Column names are case-insensitive in StarRocks.
            for dist_col in &distribution_columns {
                let dist_lower = dist_col.to_ascii_lowercase();
                if let Some(column) = columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&dist_lower))
                    && matches!(
                        column.data_type,
                        novarocks_catalog::schema::SqlType::Bitmap
                            | novarocks_catalog::schema::SqlType::Hll
                    )
                {
                    return Err(format!(
                        "BITMAP/HLL columns cannot be used as distribution key (column `{}` has type {:?})",
                        column.name, column.data_type
                    ));
                }
            }
            // This validation must precede the connector mutation dispatcher:
            // its reconciliation path may inspect a not-yet-created table,
            // whereas an invalid partition source is a deterministic statement
            // error independent of catalog state.
            for partition_field in &partition_fields {
                let source_column = match partition_field {
                    crate::sql::parser::ast::IcebergPartitionFieldExpr::Identity { column }
                    | crate::sql::parser::ast::IcebergPartitionFieldExpr::Year { column }
                    | crate::sql::parser::ast::IcebergPartitionFieldExpr::Month { column }
                    | crate::sql::parser::ast::IcebergPartitionFieldExpr::Day { column }
                    | crate::sql::parser::ast::IcebergPartitionFieldExpr::Hour { column }
                    | crate::sql::parser::ast::IcebergPartitionFieldExpr::Bucket {
                        column, ..
                    }
                    | crate::sql::parser::ast::IcebergPartitionFieldExpr::Truncate {
                        column, ..
                    }
                    | crate::sql::parser::ast::IcebergPartitionFieldExpr::Void { column } => column,
                };
                if let Some(column) = columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(source_column))
                    && matches!(
                        column.data_type,
                        novarocks_catalog::schema::SqlType::Variant
                    )
                {
                    return Err(format!(
                        "iceberg table column `{}` is variant; variant columns cannot appear in the partition spec. Use a non-variant source column for partition transforms.",
                        column.name
                    ));
                }
            }

            let target = crate::engine::backend_resolver::resolve_table_target(
                state,
                &stmt.name,
                current_catalog,
                current_database,
            )?;
            let instance_id = mutation_instance_id(&target.catalog)?;
            let _ = bucket_count;
            crate::connector::mutation::execute_catalog_mutation(
                state.connector_control.as_ref(),
                &instance_id,
                ConnectorCatalogMutationOperation::CreateTable {
                    table: ConnectorTableIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace),
                        table: Arc::from(target.table),
                    },
                    columns: columns
                        .iter()
                        .map(connector_column)
                        .collect::<Result<_, _>>()?,
                    key: key_desc.as_ref().map(connector_table_key),
                    partitioning: partition_fields
                        .iter()
                        .map(connector_partition_transform)
                        .collect(),
                    properties: properties
                        .into_iter()
                        .map(|(key, value)| (Arc::from(key), Arc::from(value)))
                        .collect(),
                    policy: if stmt.if_not_exists {
                        CreatePolicy::NoOpIfExists
                    } else {
                        CreatePolicy::FailIfExists
                    },
                },
                connector_context.clone(),
            )?;
            let _ = legacy_range_partitions;
            Ok(StatementResult::Ok)
        }
    }
}

fn mutation_instance_id(catalog: &str) -> Result<ConnectorInstanceId, String> {
    ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())
}

pub(crate) fn connector_column(
    column: &crate::sql::parser::ast::TableColumnDef,
) -> Result<ConnectorColumnDefinition, String> {
    Ok(ConnectorColumnDefinition {
        name: Arc::from(column.name.as_str()),
        data_type: connector_data_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: column.aggregation.map(connector_column_aggregation),
        default: column.default.as_ref().map(connector_default).transpose()?,
    })
}

pub(crate) fn connector_data_type(data_type: &SqlType) -> Result<ConnectorDataType, String> {
    Ok(match data_type {
        SqlType::Boolean => ConnectorDataType::Boolean,
        SqlType::TinyInt => ConnectorDataType::TinyInt,
        SqlType::SmallInt => ConnectorDataType::SmallInt,
        SqlType::Int => ConnectorDataType::Int,
        SqlType::BigInt => ConnectorDataType::BigInt,
        SqlType::LargeInt => ConnectorDataType::LargeInt,
        SqlType::Float => ConnectorDataType::Float,
        SqlType::Double => ConnectorDataType::Double,
        SqlType::Decimal { precision, scale } => ConnectorDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        SqlType::String => ConnectorDataType::String,
        SqlType::Json => ConnectorDataType::Json,
        SqlType::Binary => ConnectorDataType::Binary,
        SqlType::Bitmap => ConnectorDataType::Bitmap,
        SqlType::Hll => ConnectorDataType::Hll,
        SqlType::Date => ConnectorDataType::Date,
        SqlType::DateTime => ConnectorDataType::DateTime,
        SqlType::DateTimeNs => ConnectorDataType::DateTimeNs,
        SqlType::Time => ConnectorDataType::Time,
        SqlType::Array(element) => {
            ConnectorDataType::Array(Box::new(connector_data_type(element)?))
        }
        SqlType::Map(key, value) => ConnectorDataType::Map(
            Box::new(connector_data_type(key)?),
            Box::new(connector_data_type(value)?),
        ),
        SqlType::Struct(fields) => ConnectorDataType::Struct(
            fields
                .iter()
                .map(|(name, data_type)| {
                    Ok(novarocks_spi::connector::ConnectorStructField {
                        name: Arc::from(name.as_str()),
                        data_type: connector_data_type(data_type)?,
                        // SQL's current struct AST has no child-nullability bit.
                        nullable: true,
                    })
                })
                .collect::<Result<_, String>>()?,
        ),
        SqlType::Variant => ConnectorDataType::Variant,
    })
}

fn connector_default(value: &DefaultLiteral) -> Result<ConnectorDefaultValue, String> {
    Ok(match value {
        DefaultLiteral::Null => ConnectorDefaultValue::Null,
        DefaultLiteral::Bool(value) => ConnectorDefaultValue::Bool(*value),
        DefaultLiteral::Int(value) => ConnectorDefaultValue::Int(*value),
        DefaultLiteral::Float(value) => ConnectorDefaultValue::Float(*value),
        DefaultLiteral::Decimal { unscaled, scale } => ConnectorDefaultValue::Decimal {
            unscaled: *unscaled,
            scale: *scale,
        },
        DefaultLiteral::String(value) => ConnectorDefaultValue::String(Arc::from(value.as_str())),
        DefaultLiteral::Date(value) => ConnectorDefaultValue::Date(*value),
        DefaultLiteral::DateTime(value) => ConnectorDefaultValue::DateTime(*value),
        DefaultLiteral::Binary(value) => {
            ConnectorDefaultValue::Binary(Bytes::copy_from_slice(value))
        }
    })
}

fn connector_column_aggregation(
    aggregation: crate::sql::parser::ast::ColumnAggregation,
) -> ConnectorColumnAggregation {
    match aggregation {
        crate::sql::parser::ast::ColumnAggregation::Sum => ConnectorColumnAggregation::Sum,
        crate::sql::parser::ast::ColumnAggregation::Min => ConnectorColumnAggregation::Min,
        crate::sql::parser::ast::ColumnAggregation::Max => ConnectorColumnAggregation::Max,
        crate::sql::parser::ast::ColumnAggregation::Replace => ConnectorColumnAggregation::Replace,
        crate::sql::parser::ast::ColumnAggregation::ReplaceIfNotNull => {
            ConnectorColumnAggregation::ReplaceIfNotNull
        }
        crate::sql::parser::ast::ColumnAggregation::BitmapUnion => {
            ConnectorColumnAggregation::BitmapUnion
        }
        crate::sql::parser::ast::ColumnAggregation::HllUnion => {
            ConnectorColumnAggregation::HllUnion
        }
    }
}

pub(crate) fn connector_table_key(
    key: &crate::sql::parser::ast::TableKeyDesc,
) -> ConnectorTableKey {
    ConnectorTableKey {
        kind: match key.kind {
            crate::sql::parser::ast::TableKeyKind::Duplicate => ConnectorTableKeyKind::Duplicate,
            crate::sql::parser::ast::TableKeyKind::Unique => ConnectorTableKeyKind::Unique,
            crate::sql::parser::ast::TableKeyKind::Aggregate => ConnectorTableKeyKind::Aggregate,
            crate::sql::parser::ast::TableKeyKind::Primary => ConnectorTableKeyKind::Primary,
        },
        columns: key
            .columns
            .iter()
            .map(|column| Arc::from(column.as_str()))
            .collect(),
    }
}

pub(crate) fn connector_partition_transform(
    field: &crate::sql::parser::ast::IcebergPartitionFieldExpr,
) -> ConnectorPartitionTransform {
    use crate::sql::parser::ast::IcebergPartitionFieldExpr;
    match field {
        IcebergPartitionFieldExpr::Identity { column } => ConnectorPartitionTransform::Identity {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Year { column } => ConnectorPartitionTransform::Year {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Month { column } => ConnectorPartitionTransform::Month {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Day { column } => ConnectorPartitionTransform::Day {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Hour { column } => ConnectorPartitionTransform::Hour {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Bucket {
            column,
            num_buckets,
        } => ConnectorPartitionTransform::Bucket {
            column: Arc::from(column.as_str()),
            num_buckets: *num_buckets,
        },
        IcebergPartitionFieldExpr::Truncate { column, width } => {
            ConnectorPartitionTransform::Truncate {
                column: Arc::from(column.as_str()),
                width: *width,
            }
        }
        IcebergPartitionFieldExpr::Void { column } => ConnectorPartitionTransform::Void {
            column: Arc::from(column.as_str()),
        },
    }
}

pub(crate) fn execute_drop_catalog_statement(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    // The catalog name passed in by the user may carry quoting/case the
    // dependency graph normalizes away; the MV drop-scope guards already do
    // case-insensitive matching, so pass the user-typed value straight through.
    crate::engine::mv::dependency::ensure_no_iceberg_mv_targets_in_scope(
        state,
        catalog_name,
        None,
    )?;
    crate::engine::mv::dependency::ensure_no_external_iceberg_dependents(
        state,
        catalog_name,
        None,
    )?;
    let mut guard = state
        .iceberg_catalogs
        .write()
        .expect("standalone iceberg catalog write lock");
    match guard.drop_catalog(catalog_name) {
        Ok(()) => {
            drop(guard);
            let normalized_catalog = normalize_identifier(catalog_name)?;
            retire_iceberg_control_binding(state, &normalized_catalog)?;
            delete_catalog_attachment_if_needed(state, &normalized_catalog)?;
            state
                .catalog_service
                .unregister_catalog(&normalized_catalog);
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
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target =
        crate::engine::backend_resolver::resolve_namespace_target(state, name, current_catalog)?;
    if target.backend_name == "iceberg" {
        crate::engine::mv::dependency::ensure_no_iceberg_mv_targets_in_scope(
            state,
            &target.catalog,
            Some(&target.namespace),
        )?;
        crate::engine::mv::dependency::ensure_no_external_iceberg_dependents(
            state,
            &target.catalog,
            Some(&target.namespace),
        )?;
    }
    if force {
        let entry = state
            .iceberg_catalogs
            .read()
            .map_err(|error| format!("iceberg catalog registry read lock: {error}"))?
            .get(&target.catalog)?;
        // `IF EXISTS` applies to the complete FORCE decomposition.  In
        // particular, do not ask a remote catalog to enumerate a namespace
        // which the final DropNamespace mutation would correctly treat as a
        // no-op.
        let namespace_exists = crate::connector::iceberg::catalog::registry::namespace_exists(
            &entry,
            &target.namespace,
        )?;
        if !namespace_exists {
            if if_exists {
                return Ok(StatementResult::Ok);
            }
            return Err(format!("namespace `{}` does not exist", target.namespace));
        }
        let mut tables =
            crate::connector::iceberg::catalog::registry::list_tables(&entry, &target.namespace)?;
        tables.sort();
        let mut views = if matches!(
            entry.kind,
            crate::connector::iceberg::catalog::registry::IcebergCatalogKind::Rest
        ) {
            crate::connector::iceberg::catalog::views::list_views(&entry, &target.namespace)?
        } else {
            Vec::new()
        };
        views.sort();
        let instance_id = mutation_instance_id(&target.catalog)?;
        for table in tables {
            crate::connector::mutation::execute_catalog_mutation(
                state.connector_control.as_ref(),
                &instance_id,
                ConnectorCatalogMutationOperation::DropTable {
                    table: ConnectorTableIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace.as_str()),
                        table: Arc::from(table.as_str()),
                    },
                    policy: DropPolicy::FailIfMissing,
                    data_disposition: ConnectorDropTableDataDisposition::Purge,
                },
                connector_context.clone(),
            )?;
            state
                .catalog_service
                .invalidate_table(&target.catalog, &target.namespace, &table)?;
            crate::engine::query_prep::drop_local_table_registration_if_exists(
                state,
                &target.namespace,
                &table,
            )?;
        }
        for view in views {
            crate::connector::mutation::execute_catalog_mutation(
                state.connector_control.as_ref(),
                &instance_id,
                ConnectorCatalogMutationOperation::DropView {
                    view: novarocks_spi::connector::ConnectorViewIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace.as_str()),
                        view: Arc::from(view.as_str()),
                    },
                    policy: DropPolicy::FailIfMissing,
                },
                connector_context.clone(),
            )?;
        }
    }
    let instance_id = mutation_instance_id(&target.catalog)?;
    crate::connector::mutation::execute_catalog_mutation(
        state.connector_control.as_ref(),
        &instance_id,
        ConnectorCatalogMutationOperation::DropNamespace {
            namespace: ConnectorNamespaceIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace),
            },
            policy: if if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
        },
        connector_context.clone(),
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_drop_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
    _force: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
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
    let dependency_ref = if target.backend_name == "iceberg" {
        crate::mv::dependency::model::iceberg_table_object_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        )
    } else {
        crate::mv::dependency::model::external_table_object_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        )
    };
    match crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
        state,
        &target,
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::DropTable,
    ) {
        Ok(()) => {}
        Err(err)
            if if_exists
                && target.backend_name == "iceberg"
                && is_missing_table_guard_error(&err) =>
        {
            cleanup_iceberg_drop_table_registration_if_exists(state, &target)?;
            return Ok(StatementResult::Ok);
        }
        Err(err) => return Err(err),
    }
    crate::engine::mv::dependency::ensure_no_downstream_dependencies(state, &dependency_ref)?;
    let instance_id = mutation_instance_id(&target.catalog)?;
    match crate::connector::mutation::execute_catalog_mutation(
        state.connector_control.as_ref(),
        &instance_id,
        ConnectorCatalogMutationOperation::DropTable {
            table: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            policy: if if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
            data_disposition: ConnectorDropTableDataDisposition::Purge,
        },
        connector_context.clone(),
    ) {
        Ok(_) => {
            if target.backend_name == "iceberg" {
                state.catalog_service.invalidate_table(
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
                crate::engine::query_prep::drop_local_table_registration_if_exists(
                    state,
                    &target.namespace,
                    &target.table,
                )?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("NotFound") => {
            if target.backend_name == "iceberg" {
                cleanup_iceberg_drop_table_registration_if_exists(state, &target)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) => {
            // A DROP TABLE aimed at a view must say so instead of "unknown
            // table" — views and tables are separate REST resources.
            if target.backend_name == "iceberg"
                && state
                    .iceberg_catalogs
                    .read()
                    .expect("iceberg catalog registry read")
                    .get(&target.catalog)
                    .and_then(|entry| {
                        crate::connector::iceberg::catalog::views::view_exists(
                            &entry,
                            &target.namespace,
                            &target.table,
                        )
                    })
                    .unwrap_or(false)
            {
                return Err(format!(
                    "{}.{}.{} is a view, use DROP VIEW",
                    target.catalog, target.namespace, target.table
                ));
            }
            Err(err)
        }
    }
}

fn is_missing_table_guard_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("unknown table:")
        || lower.contains("table not found")
        || lower.contains("no metadata files")
}

fn cleanup_iceberg_drop_table_registration_if_exists(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<(), String> {
    state
        .catalog_service
        .invalidate_table(&target.catalog, &target.namespace, &target.table)?;
    crate::engine::query_prep::drop_local_table_registration_if_exists(
        state,
        &target.namespace,
        &target.table,
    )
}

fn drop_local_catalog_table(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_database: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name.parts.as_slice(), current_database)?;
    let mut guard = state
        .catalog_service
        .local()
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
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "TRUNCATE TABLE only supports iceberg tables: {}.{}",
            target.namespace, target.table
        ));
    }
    let resolved_table = {
        crate::connector::metadata_load_table(
            state.connector_control.as_ref(),
            connector_context.clone(),
            &target.catalog,
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?
        .0
    };
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
        state,
        &target,
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Truncate,
    )?;
    crate::engine::iceberg_truncate::execute_iceberg_truncate_table(
        state,
        &target,
        &resolved_table,
        target_ref,
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
    UpdateComment {
        path: ColumnPath,
        comment: String,
    },
}

/// Detect `SHOW CREATE TABLE <name>` statements so the server layer can
/// route them to the engine instead of treating them as session noops.
pub(crate) fn looks_like_show_create_table(sql: &str) -> bool {
    let lower = sql.trim_start().to_ascii_lowercase();
    // Match "SHOW CREATE TABLE ..." quickly without full parsing.
    if !lower.starts_with("show") {
        return false;
    }
    let rest = lower["show".len()..].trim_start();
    if !rest.starts_with("create") {
        return false;
    }
    let rest2 = rest["create".len()..].trim_start();
    rest2.starts_with("table")
}

/// Parse `SHOW CREATE TABLE <catalog>.<db>.<table>` and return the parsed
/// `ObjectName`.  Returns `Err` if parsing fails.
pub(crate) fn parse_show_create_table(
    sql: &str,
) -> Result<crate::sql::parser::ast::ObjectName, String> {
    use sqlparser::keywords::Keyword;
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse SHOW CREATE TABLE: {e}"))?;
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|e| format!("parse SHOW CREATE TABLE: {e}"))?;
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| format!("parse SHOW CREATE TABLE: {e}"))?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| format!("parse SHOW CREATE TABLE: {e}"))?;
    let obj = parser
        .parse_object_name(false)
        .map_err(|e| format!("parse SHOW CREATE TABLE table name: {e}"))?;
    crate::sql::parser::dialect::convert_object_name(obj)
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
    if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "SET") {
        if crate::sql::parser::dialect::peek_word_eq(&parser, 1, "TBLPROPERTIES") {
            return true;
        }
        parser.next_token();
        if parser.peek_token_ref().token == Token::LParen {
            return true;
        }
    }
    if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "UNSET")
        && crate::sql::parser::dialect::peek_word_eq(&parser, 1, "TBLPROPERTIES")
    {
        return true;
    }
    // ALTER TABLE t COMMENT 'x'  — set the table-level comment property.
    if parser.parse_keyword(Keyword::COMMENT) {
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
        if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "TBLPROPERTIES") {
            parser.next_token(); // consume TBLPROPERTIES
        } else if parser.peek_token_ref().token != Token::LParen {
            return Err("expected TBLPROPERTIES or property list after SET".to_string());
        }
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
    } else if parser.parse_keyword(Keyword::COMMENT) {
        // ALTER TABLE t COMMENT 'x' — shorthand for setting the "comment" property.
        let comment = parser
            .parse_literal_string()
            .map_err(|e| format!("COMMENT expects a string literal: {e}"))?;
        PropertiesOp::Set {
            entries: vec![("comment".to_string(), comment)],
        }
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
        // Use parse_sql_type_definition so that MAP<K,V>/ARRAY<T> work without normalize.
        let new_type =
            crate::sql::parser::dialect::create_table::parse_sql_type_definition(&mut parser)?;
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
        } else if parser.parse_keyword(Keyword::COMMENT) {
            let comment = parser
                .parse_literal_string()
                .map_err(|e| format!("COMMENT expects a string literal: {e}"))?;
            IcebergSchemaChange::UpdateComment { path, comment }
        } else {
            return Err(
                "ALTER COLUMN must be followed by FIRST / AFTER / BEFORE / SET NOT NULL / DROP NOT NULL / COMMENT".to_string(),
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

    // Use parse_sql_type_definition (not parser.parse_data_type + convert_sql_type) so that
    // collection types like MAP<K,V> and ARRAY<T> are parsed via native angle-bracket syntax
    // rather than going through normalize_for_raw_parse which only rewrites MAP<> inside CAST.
    let data_type = crate::sql::parser::dialect::create_table::parse_sql_type_definition(parser)?;
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
    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| "ADD EQUALITY DELETE requires a VALUES source".to_string())?;
    let rows = match source.body.as_ref() {
        sqlparser::ast::SetExpr::Values(values) => values
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(sqlparser_expr_to_literal)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?,
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
mod drop_table_if_exists_tests {
    #[test]
    fn guard_missing_table_error_is_soft_drop_candidate_but_mv_error_is_not() {
        assert!(super::is_missing_table_guard_error(
            "unknown table: db.missing"
        ));
        assert!(super::is_missing_table_guard_error(
            "load iceberg table db.missing: table not found: warehouse/db/missing"
        ));
        assert!(super::is_missing_table_guard_error(
            "no metadata files for db.missing"
        ));
        assert!(!super::is_missing_table_guard_error(
            "table ice.db.mv_orders is a materialized view; use DROP MATERIALIZED VIEW"
        ));
    }
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
    fn looks_like_show_alter_table_optimize_detects_only_live_show_route() {
        assert!(super::looks_like_show_alter_table_optimize(
            "SHOW ALTER TABLE OPTIMIZE"
        ));
        assert!(super::looks_like_show_alter_table_optimize(
            " show alter table optimize from db "
        ));
        assert!(!super::looks_like_show_alter_table_optimize(
            "ALTER TABLE ice.db.orders OPTIMIZE"
        ));
        assert!(!super::looks_like_show_alter_table_optimize(
            "SHOW CREATE TABLE ice.db.orders"
        ));
        assert!(!super::looks_like_show_alter_table_optimize(
            "SHOW ALTER TABLE orders OPTIMIZE"
        ));
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
                data_type: novarocks_catalog::schema::SqlType::Int,
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
        assert_eq!(new_type, novarocks_catalog::schema::SqlType::BigInt);
    }

    #[test]
    fn parse_alter_iceberg_schema_rejects_unsupported_add_forms() {
        let not_null = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT NOT NULL",
        )
        .expect_err("not null should fail");
        assert!(not_null.contains("ADD COLUMN NOT NULL is not supported"));

        // Quoted DEFAULT values are accepted for numeric columns iff the
        // string parses as the column's numeric type (StarRocks compat:
        // `DEFAULT "0"` for INT works). A genuinely non-numeric string —
        // here `'abc'` — must still be rejected.
        let type_mismatch = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.db.orders ADD COLUMN discount INT DEFAULT 'abc'",
        )
        .expect_err("non-numeric string default for INT should fail");
        assert!(
            type_mismatch.contains("invalid integer DEFAULT"),
            "expected 'invalid integer DEFAULT' but got: {type_mismatch}"
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
        assert!(matches!(
            new_type,
            novarocks_catalog::schema::SqlType::BigInt
        ));
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
    fn parse_add_column_map_default_empty() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE t ADD COLUMN counts MAP<STRING, INT> DEFAULT '{}'",
        )
        .expect("parse MAP column with default");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn {
                name, data_type, ..
            } => {
                assert_eq!(name, "counts");
                assert!(
                    matches!(data_type, novarocks_catalog::schema::SqlType::Map(_, _)),
                    "expected Map type, got {:?}",
                    data_type
                );
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_add_column_array_default_empty() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE t ADD COLUMN tags ARRAY<INT> DEFAULT '[]'",
        )
        .expect("parse ARRAY column with default");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn {
                name, data_type, ..
            } => {
                assert_eq!(name, "tags");
                assert!(
                    matches!(data_type, novarocks_catalog::schema::SqlType::Array(_)),
                    "expected Array type, got {:?}",
                    data_type
                );
            }
            _ => panic!("expected AddColumn"),
        }
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
    fn parse_alter_column_comment() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE t ALTER COLUMN v COMMENT 'value column'",
        )
        .unwrap();
        let super::IcebergSchemaChange::UpdateComment { path, comment } = stmt.change else {
            panic!("expected UpdateComment");
        };
        assert_eq!(path.dotted(), "v");
        assert_eq!(comment, "value column");
    }

    #[test]
    fn parse_alter_column_comment_nested() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE t ALTER COLUMN address.street COMMENT 'street name'",
        )
        .unwrap();
        let super::IcebergSchemaChange::UpdateComment { path, comment } = stmt.change else {
            panic!("expected UpdateComment");
        };
        assert_eq!(path.dotted(), "address.street");
        assert_eq!(comment, "street name");
    }

    #[test]
    fn parse_alter_column_comment_empty_string() {
        let stmt = super::parse_alter_iceberg_schema_sql("ALTER TABLE t ALTER COLUMN c COMMENT ''")
            .unwrap();
        let super::IcebergSchemaChange::UpdateComment { path, comment } = stmt.change else {
            panic!("expected UpdateComment");
        };
        assert_eq!(path.dotted(), "c");
        assert_eq!(comment, "");
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
    fn looks_like_set_property_list() {
        assert!(looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t SET ('unique_constraints' = 'id')"
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
    fn parse_set_property_list_one_pair() {
        let stmt = parse_alter_iceberg_properties_sql(
            "ALTER TABLE ice.db.t SET ('unique_constraints' = 'id')",
        )
        .expect("parse");
        assert_eq!(stmt.table.parts, vec!["ice", "db", "t"]);
        let PropertiesOp::Set { entries } = stmt.op else {
            panic!()
        };
        assert_eq!(
            entries,
            vec![("unique_constraints".to_string(), "id".to_string())]
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

    // ---------------------------------------------------------------------------
    // Table-level COMMENT tests
    // ---------------------------------------------------------------------------

    #[test]
    fn looks_like_table_comment() {
        assert!(looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t COMMENT 'my table'"
        ));
    }

    #[test]
    fn parse_table_comment() {
        let stmt =
            parse_alter_iceberg_properties_sql("ALTER TABLE ice.db.t COMMENT 'my table comment'")
                .expect("parse");
        assert_eq!(stmt.table.parts, vec!["ice", "db", "t"]);
        let PropertiesOp::Set { entries } = stmt.op else {
            panic!("expected Set op")
        };
        assert_eq!(
            entries,
            vec![("comment".to_string(), "my table comment".to_string())]
        );
    }

    #[test]
    fn parse_table_comment_three_part_name() {
        let stmt =
            parse_alter_iceberg_properties_sql("ALTER TABLE cat.ns.tbl COMMENT 'hello world'")
                .expect("parse");
        assert_eq!(stmt.table.parts, vec!["cat", "ns", "tbl"]);
        let PropertiesOp::Set { entries } = stmt.op else {
            panic!("expected Set op")
        };
        assert_eq!(entries[0].0, "comment");
        assert_eq!(entries[0].1, "hello world");
    }

    #[test]
    fn parse_table_comment_empty_string() {
        let stmt = parse_alter_iceberg_properties_sql("ALTER TABLE t COMMENT ''").expect("parse");
        let PropertiesOp::Set { entries } = stmt.op else {
            panic!("expected Set op")
        };
        assert_eq!(entries, vec![("comment".to_string(), String::new())]);
    }

    #[test]
    fn parse_table_comment_missing_literal_rejected() {
        // COMMENT without a string literal must error.
        assert!(parse_alter_iceberg_properties_sql("ALTER TABLE t COMMENT").is_err());
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
