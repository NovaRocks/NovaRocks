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

//! Standalone-mode `DELETE FROM iceberg ... WHERE ...` entry point.
//!
//! Distributed position-delete path:
//! 1. Resolve + load the iceberg table.
//! 2. Run pre-lowering validators and choose the Iceberg write mode.
//! 3. Translate the sqlparser WHERE into an iceberg [`Predicate`]. Phase 1
//!    supports comparison operators (`= != < <= > >=`), `IN (...)`, and
//!    `AND` / `OR` against primitive columns (int / long / string / bool / timestamp).
//!    Other expressions are rejected with an explicit error.
//! 4. Rewrite DELETE into a SELECT of `_file`, `_pos`, and partition source
//!    columns, then run it through the distributed `ICEBERG_DELETE_SINK`.
//! 5. Route the sink output through the Iceberg write transaction runner,
//!    which commits the generated position-delete files and drives
//!    finalization lifecycle.

use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use sqlparser::ast as sqlast;

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::dml::delete::{
    DeleteOperation, PreparedDelete, PreparedDeleteExecution, prepared_delete,
};
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::planning::write_sink::{
    admit_prepared_frozen_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use novarocks::catalog_application::query_bindings::QueryTableBindingStore;
use novarocks::catalog_application::resolver::{TargetBackend, resolve_existing_table_target};
use novarocks_catalog::schema::ColumnDef;
use novarocks_spi::connector::ConnectorRowMutationStrategy;
use novarocks_spi::connector::ConnectorWriteOperationId;
use novarocks_sql::planning::dml::{DmlWriteSinkMode, IcebergRefSuffix, split_ref_suffix};
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;
use novarocks_sql::syntax::{DeleteStmt, ObjectName};

pub(crate) fn prepare_delete_statement(
    state: &DmlExecutionKernel,
    stmt: &DeleteStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedDelete, String> {
    // Detect branch/tag suffix in the target table name.
    let (stripped_parts, ref_suffix) = split_ref_suffix(&stmt.table.parts);
    let effective_name;
    let table_name: &ObjectName = match ref_suffix {
        Some(IcebergRefSuffix::Tag(ref tag_name)) => {
            return Err(format!(
                "iceberg ref: tag '{tag_name}' is read-only; use a branch as DML target"
            ));
        }
        Some(IcebergRefSuffix::Branch(_)) => {
            effective_name = ObjectName {
                parts: stripped_parts,
            };
            &effective_name
        }
        None => &stmt.table,
    };
    let target_ref = match &ref_suffix {
        Some(IcebergRefSuffix::Branch(b)) => b.clone(),
        _ => "main".to_string(),
    };

    // 1. Resolve target.
    let target =
        resolve_existing_table_target(state, table_name, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "phase 1 DELETE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    let target_binding = novarocks::connector::write_target::load_write_target_binding(
        state.connector_control().as_ref(),
        &target.catalog,
        &target.namespace,
        &target.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        connector_context.clone(),
    )?;
    let planning_lease = target_binding.lease().clone();

    // Reject a managed materialized view from neutral metadata under an exact
    // generation, the same way INSERT, TRUNCATE and ADD FILES already do. This
    // check cannot move into row-mutation admission: incremental MV refresh
    // drives its own writes through that same admission, so at that level a
    // user statement is indistinguishable from the MV machinery maintaining its
    // own target.
    novarocks::mv::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        state.connector_control().as_ref(),
        state.mv_storage_observation().as_ref(),
        &target,
        novarocks::mv::iceberg_guard::IcebergMvUserMutation::Delete,
    )?;

    // 3. Reject an unsupported WHERE clause before any external side effect.
    //    The distributed SELECT planner owns scan pruning and existing delete
    //    visibility from this point onward. Column types come from the provider,
    //    so this check never decodes an Iceberg schema itself.
    validate_where(&stmt.where_clause, &target_binding.dml_target_columns())?;

    // 4. Ask the provider to plan the row mutation. The physical strategy, the
    //    branch/format admission gates and the base version the frontend
    //    journals all come back signed; nothing here re-derives them. The
    //    provider reservation stays where DELETE has always made it, before the
    //    frontend persists its operation intent -- unlike UPDATE and MERGE,
    //    which defer activation until after. Aligning the two is a lifecycle
    //    change and not part of this cutover.
    let connector_operation_id = ConnectorWriteOperationId::new();
    let (write_lease, row_mutation) = target_binding.prepare_row_mutation(
        &target_ref,
        connector_operation_id,
        novarocks_spi::connector::ConnectorRowMutationIntent::Delete,
        connector_context.clone(),
    )?;
    let strategy = row_mutation.strategy();
    let base_snapshot_id = row_mutation.base_version_ordinal();
    let routes = write_lease
        .activate_row_mutation(
            novarocks_spi::connector::ConnectorRowMutationActivationRequest::Direct {
                preparation: row_mutation,
                context: connector_context.clone(),
            },
        )
        .map_err(|error| format!("activate Provider DELETE plan: {error}"))?;
    let route = routes
        .routes()
        .first()
        .ok_or_else(|| "Provider returned an empty DELETE route set".to_string())?;
    let preparation = route.preparation().clone();

    prepare_delete_write(
        state,
        &target,
        strategy,
        preparation,
        base_snapshot_id,
        connector_operation_id,
        &write_lease,
        &target_ref,
        &stmt.where_clause,
        execution.clone(),
        connector_context,
        planning_lease,
    )
}

struct DistributedDeleteWriteExecutor {
    state: DmlExecutionKernel,
    target: TargetBackend,
    delete_query: sqlparser::ast::Query,
    sql_write_input: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<QueryTableBindingStore>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
    /// Deletion vectors are written one per target data file, so the sink output
    /// is shuffled by its first column. Position deletes have no such
    /// requirement. Both follow from the provider-signed strategy.
    shuffle_by_first_output: bool,
    native_assembly: Mutex<Option<crate::query_execution::compiler::PreparedDmlWriteAssembly>>,
}

impl PreparedDeleteExecution for DistributedDeleteWriteExecutor {
    /// DELETE activates its write generation during preparation, so the
    /// authority already exists before anything is dispatched. The resource
    /// identity comes from the activated template, never from a name the
    /// frontend supplied.
    fn external_fence_authority(
        &self,
    ) -> Result<
        crate::query_execution::dml::external_write_fence::ExternalWriteFenceAuthority,
        novarocks_spi::connector::ConnectorError,
    > {
        crate::query_execution::dml::external_write_fence::ExternalWriteFenceAuthority::try_new(
            self.connector_write.lease(),
            self.connector_write.operation_id(),
            &self.target.namespace,
            &self.target.table,
            self.connector_write.preparation().target_ref().clone(),
            self.connector_context.clone(),
        )
    }

    fn native_encoding(
        &self,
    ) -> Result<crate::query_execution::dml::delete::DeleteNativeEncoding<'_>, String> {
        let mut assembly = self
            .native_assembly
            .lock()
            .expect("prepared DELETE native assembly lock poisoned");
        if assembly.is_none() {
            let distribution = if self.shuffle_by_first_output {
                crate::query_execution::compiler::iceberg_write_shuffle_by_output_index(0)
            } else {
                novarocks_sql::compiler::RootDistributionRequirement::Any
            };
            *assembly = Some(
                crate::query_execution::compiler::prepare_query_as_iceberg_write_with_connector_context(
                    &self.state,
                    Some(&self.target.catalog),
                    &self.target.namespace,
                    &self.delete_query,
                    self.sql_write_input.clone(),
                    Arc::clone(&self.table_bindings),
                    None,
                    distribution,
                    Some(&self.execution),
                    &self.connector_context,
                    Some(self.connector_write.clone()),
                )?,
            );
        }
        Ok(crate::query_execution::dml::delete::DeleteNativeEncoding {
            inner: super::DeleteNativeEncodingInner::Assembly(assembly),
        })
    }

    fn run_with_native_bundle(
        &self,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<QueryExecutionResult, String> {
        self.native_assembly
            .lock()
            .expect("prepared DELETE native assembly lock poisoned")
            .take()
            .ok_or_else(|| "prepared DELETE native assembly was already consumed".to_string())?
            .finish(native_bundle)
    }

    fn commit_terminal(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        completion
            .session()
            .commit(self.connector_context.clone())
            .map_err(|error| error.to_string())
    }

    fn finalize(&self) -> Result<(), String> {
        self.state.catalog_service().invalidate_table(
            &self.target.catalog,
            &self.target.namespace,
            &self.target.table,
        )
    }
}

/// Plan the distributed write for a DELETE whose physical strategy the provider
/// has already signed.
///
/// Both supported strategies share the same sink query and admission shape; only
/// the sink mode and the root distribution differ, and both follow from the
/// signed strategy rather than from anything this engine decides.
#[allow(clippy::too_many_arguments)]
fn prepare_delete_write(
    state: &DmlExecutionKernel,
    target: &TargetBackend,
    strategy: ConnectorRowMutationStrategy,
    preparation: novarocks_spi::connector::ConnectorWritePreparation,
    base_snapshot_id: Option<i64>,
    connector_operation_id: ConnectorWriteOperationId,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    target_ref: &str,
    where_clause: &sqlast::Expr,
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<PreparedDelete, String> {
    let deletion_vectors = match strategy {
        ConnectorRowMutationStrategy::DeletionVector => true,
        ConnectorRowMutationStrategy::PositionDelete => false,
        other => {
            return Err(format!(
                "DELETE cannot be served by row-mutation strategy {other:?}"
            ));
        }
    };
    let sink_mode = if deletion_vectors {
        DmlWriteSinkMode::DeletionVectors
    } else {
        DmlWriteSinkMode::PositionDeletes
    };

    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding = admit_prepared_frozen_connector_write_target(
        table_bindings.as_ref(),
        FrozenConnectorScanIdentity::new(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        ),
        preparation.clone(),
        planning_lease.clone(),
    )?;
    let sql_write_input = dml_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        sink_mode,
        novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
    )?;
    let delete_query = build_delete_position_sink_query(
        target,
        where_clause,
        &write_input_columns(&preparation),
        target_ref,
    )?;
    let connector_write =
        crate::query_execution::contract::ConnectorWritePlanningTemplate::activate_prepared(
            connector_operation_id,
            preparation,
            connector_context.clone(),
            write_lease.clone(),
        )
        .map_err(|error| format!("activate Provider DELETE write: {error}"))?;
    let executor = DistributedDeleteWriteExecutor {
        state: state.clone(),
        target: target.clone(),
        delete_query,
        sql_write_input,
        table_bindings,
        execution,
        connector_context: connector_context.clone(),
        connector_write,
        // Deletion vectors are written one per target data file, so the sink
        // output is shuffled by its first column; position deletes have no such
        // requirement.
        shuffle_by_first_output: deletion_vectors,
        native_assembly: Mutex::new(None),
    };
    Ok(prepared_delete(
        DeleteOperation {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            target_ref: target_ref.to_string(),
            attempt_id: connector_operation_id.to_string(),
            base_snapshot_id,
        },
        Arc::new(executor),
    ))
}

fn build_delete_position_sink_query(
    target: &TargetBackend,
    where_clause: &sqlast::Expr,
    sink_columns: &[ColumnDef],
    target_ref: &str,
) -> Result<sqlparser::ast::Query, String> {
    let projection = sink_columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let version_clause = if target_ref == "main" {
        String::new()
    } else {
        format!(" FOR VERSION AS OF {}", sql_string_literal(target_ref))
    };
    let sql = format!(
        "SELECT {projection} FROM {}{version_clause} WHERE {where_clause}",
        qualify_iceberg_table(target)
    );
    parse_generated_query(&sql, "DELETE position-delete rewrite")
}

fn write_input_columns(
    preparation: &novarocks_spi::connector::ConnectorWritePreparation,
) -> Vec<ColumnDef> {
    preparation
        .input()
        .fields()
        .into_iter()
        .map(|binding| ColumnDef {
            name: binding.field().name().to_string(),
            data_type: binding.field().data_type().clone(),
            nullable: binding.field().is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect()
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match novarocks_sql::planning::dml::parse_raw_statement(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context}: generated non-query statement: {other}")),
    }
}

fn qualify_iceberg_table(target: &TargetBackend) -> String {
    format!(
        "{}.{}.{}",
        sql_identifier(&target.catalog),
        sql_identifier(&target.namespace),
        sql_identifier(&target.table)
    )
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Check that a DELETE `WHERE` clause is inside the subset this engine supports.
///
/// Nothing is produced: the distributed SELECT planner owns the actual filtering
/// from here on. This exists only to reject an unsupported clause before the
/// statement reaches any external side effect.
///
/// Phase 1 supports the following node shapes; everything else is rejected
/// with an explicit error pointing at the unsupported construct so the caller
/// can rewrite the WHERE clause.
fn validate_where(expr: &sqlast::Expr, columns: &[ColumnDef]) -> Result<(), String> {
    match expr {
        sqlast::Expr::BinaryOp { left, op, right } => match op {
            sqlast::BinaryOperator::And | sqlast::BinaryOperator::Or => {
                validate_where(left, columns)?;
                validate_where(right, columns)
            }
            sqlast::BinaryOperator::Eq
            | sqlast::BinaryOperator::NotEq
            | sqlast::BinaryOperator::Lt
            | sqlast::BinaryOperator::LtEq
            | sqlast::BinaryOperator::Gt
            | sqlast::BinaryOperator::GtEq => {
                // Detect scalar_fn(col) <op> literal pattern first.
                // Function-call predicates cannot be pushed into Iceberg column
                // statistics (the function obscures the underlying column value),
                // so we return AlwaysTrue here to scan all files and leave
                // correctness to the per-row evaluator in evaluate_where_at_row.
                if extract_scalar_fn_comparison(left, right).is_some()
                    || extract_variant_get_comparison(left, right).is_some()
                {
                    return Ok(());
                }
                let (col_name, value_expr, _flipped) = extract_comparison(left, right)?;
                validate_literal_for_column(value_expr, columns, &col_name)
            }
            other => Err(format!(
                "phase 1 DELETE WHERE does not support binary operator `{other:?}`"
            )),
        },
        sqlast::Expr::InList { expr, list, .. } => {
            let col_name = expr_to_column_name(expr)?;
            for literal in list {
                validate_literal_for_column(literal, columns, &col_name)?;
            }
            Ok(())
        }
        sqlast::Expr::IsNull(inner) | sqlast::Expr::IsNotNull(inner) => {
            expr_to_column_name(inner).map(|_| ())
        }
        sqlast::Expr::Nested(inner) => validate_where(inner, columns),
        other => Err(format!(
            "phase 1 DELETE WHERE supports comparison / IN / IS NULL / AND / OR \
             over primitive columns; rewrite this clause and retry. Unsupported: {other:?}"
        )),
    }
}

/// One side of a comparison must be a column reference and the other a literal.
/// Returns `(column_name, literal_expr, flipped)` where `flipped = true`
/// indicates the original was `<literal> <op> <column>`.
fn extract_comparison<'a>(
    left: &'a sqlast::Expr,
    right: &'a sqlast::Expr,
) -> Result<(String, &'a sqlast::Expr, bool), String> {
    if let Ok(name) = expr_to_column_name(left) {
        return Ok((name, right, false));
    }
    if let Ok(name) = expr_to_column_name(right) {
        return Ok((name, left, true));
    }
    Err(
        "phase 1 DELETE WHERE comparison must have exactly one side that is a \
         column reference (the other must be a literal)"
            .to_string(),
    )
}

/// Attempt to detect a `scalar_fn(col_ref) <op> literal` pattern.
///
/// Returns `Some((fn_name, col_name, literal_expr, flipped))` when:
///   - One side is a single-argument function call whose sole argument is a
///     column reference.
///   - The other side is a value literal.
///   - The function name is in the supported deterministic string-function set.
///
/// `flipped = true` means the original was `literal <op> fn(col)`.
fn extract_scalar_fn_comparison<'a>(
    left: &'a sqlast::Expr,
    right: &'a sqlast::Expr,
) -> Option<(String, String, &'a sqlast::Expr, bool)> {
    if let Some((fn_name, col_name)) = expr_as_supported_scalar_fn_on_col(left) {
        if is_literal_expr(right) {
            return Some((fn_name, col_name, right, false));
        }
    }
    if let Some((fn_name, col_name)) = expr_as_supported_scalar_fn_on_col(right) {
        if is_literal_expr(left) {
            return Some((fn_name, col_name, left, true));
        }
    }
    None
}

/// Detect a `variant_get(col, 'path', 'type') <op> literal` predicate.
///
/// The generated DELETE rewrite runs the original WHERE clause through the
/// normal query pipeline, where `variant_get` is evaluated with full analyzer
/// and execution support. The Iceberg predicate translator only needs to
/// accept this shape and avoid unsafe file pruning, so callers treat it as
/// `AlwaysTrue`.
fn extract_variant_get_comparison<'a>(
    left: &'a sqlast::Expr,
    right: &'a sqlast::Expr,
) -> Option<(String, &'a sqlast::Expr, bool)> {
    if let Some(col_name) = expr_as_variant_get_on_col(left) {
        if is_literal_expr(right) {
            return Some((col_name, right, false));
        }
    }
    if let Some(col_name) = expr_as_variant_get_on_col(right) {
        if is_literal_expr(left) {
            return Some((col_name, left, true));
        }
    }
    None
}

fn expr_as_variant_get_on_col(expr: &sqlast::Expr) -> Option<String> {
    let sqlast::Expr::Function(func) = expr else {
        return None;
    };
    let name = func.name.to_string().to_ascii_lowercase();
    if !matches!(name.as_str(), "variant_get" | "try_variant_get") {
        return None;
    }
    let args = function_expr_args(func)?;
    if args.len() != 3 {
        return None;
    }
    let col_name = expr_to_column_name(args[0]).ok()?;
    extract_string_literal(args[1])?;
    extract_string_literal(args[2])?;
    Some(col_name)
}

/// Return `(fn_name_lowercase, col_name_lowercase)` when `expr` is a
/// single-argument function call over a bare column reference and the function
/// name is in the deterministic set we support for row-level evaluation.
fn expr_as_supported_scalar_fn_on_col(expr: &sqlast::Expr) -> Option<(String, String)> {
    let sqlast::Expr::Function(func) = expr else {
        return None;
    };
    let name = func.name.to_string().to_ascii_lowercase();
    if !is_supported_scalar_fn(&name) {
        return None;
    }
    let args = function_expr_args(func)?;
    if args.len() != 1 {
        return None;
    }
    let col_name = expr_to_column_name(args[0]).ok()?;
    Some((name, col_name))
}

fn function_expr_args(func: &sqlast::Function) -> Option<Vec<&sqlast::Expr>> {
    match &func.args {
        sqlast::FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| {
                if let sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) = arg {
                    Some(e)
                } else {
                    None
                }
            })
            .collect(),
        _ => None,
    }
}

/// The set of deterministic, single-argument scalar functions that the phase-1
/// DELETE evaluator can apply per-row.  These functions cannot be pushed into
/// Iceberg column statistics (the predicate is treated as AlwaysTrue for file
/// skipping), but they are applied during the row-level filter pass.
fn is_supported_scalar_fn(name: &str) -> bool {
    matches!(
        name,
        "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "length" | "char_length"
    )
}

/// Returns `true` when `expr` is a value literal (or a nested/negated literal)
/// that `literal_to_datum` can parse.
fn is_literal_expr(expr: &sqlast::Expr) -> bool {
    match expr {
        sqlast::Expr::Value(_) => true,
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => matches!(inner.as_ref(), sqlast::Expr::Value(_)),
        sqlast::Expr::Nested(inner) => is_literal_expr(inner),
        _ => false,
    }
}

fn expr_to_column_name(expr: &sqlast::Expr) -> Result<String, String> {
    match expr {
        sqlast::Expr::Identifier(ident) => Ok(ident.value.to_lowercase()),
        sqlast::Expr::CompoundIdentifier(parts) => {
            // a.b.c → take the last part (the column name); table-qualified
            // refs work because the Predicate is bound against the
            // single-table schema via TableScan.with_filter.
            parts
                .last()
                .map(|p| p.value.to_lowercase())
                .ok_or_else(|| "compound identifier has no parts".to_string())
        }
        other => Err(format!(
            "phase 1 DELETE WHERE expected a column identifier here, got {other:?}"
        )),
    }
}

/// Check that `expr` is a literal this engine can interpret as `column_name`'s
/// type.
///
/// The value itself is not retained: the caller only needs to know whether the
/// clause is inside the supported subset. Actual filtering belongs to the
/// distributed SELECT planner.
fn validate_literal_for_column(
    expr: &sqlast::Expr,
    columns: &[ColumnDef],
    column_name: &str,
) -> Result<(), String> {
    let column = columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| format!("column `{column_name}` not found in iceberg table schema"))?;
    let column_type = match &column.data_type {
        nested @ (DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(_, _)) => {
            return Err(format!(
                "phase 1 DELETE WHERE only supports primitive columns; column `{column_name}` is {nested:?}"
            ));
        }
        other => other,
    };
    let lit_value = match expr {
        sqlast::Expr::Value(v) => v,
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            sqlast::Expr::Value(v) => v,
            other => {
                return Err(format!(
                    "phase 1 DELETE WHERE expects a literal value, got -{other:?}"
                ));
            }
        },
        other => {
            return Err(format!(
                "phase 1 DELETE WHERE expects a literal value, got {other:?}"
            ));
        }
    };
    let negate = matches!(
        expr,
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            ..
        }
    );
    let lit_str = match &lit_value.value {
        sqlast::Value::Number(s, _) => s.clone(),
        sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => s.clone(),
        sqlast::Value::Boolean(b) => b.to_string(),
        sqlast::Value::Null => {
            return Err(format!(
                "phase 1 DELETE WHERE does not support NULL literals; use IS NULL/IS NOT NULL instead \
                 (column `{column_name}`)"
            ));
        }
        other => {
            return Err(format!(
                "phase 1 DELETE WHERE literal value `{other:?}` is not yet supported"
            ));
        }
    };
    let owned;
    let lit_str = if negate {
        owned = format!("-{lit_str}");
        owned.as_str()
    } else {
        lit_str.as_str()
    };
    match column_type {
        DataType::Int32 => lit_str
            .parse::<i32>()
            .map(|_| ())
            .map_err(|e| format!("parse INT literal `{lit_str}` for column `{column_name}`: {e}")),
        DataType::Int64 => lit_str
            .parse::<i64>()
            .map(|_| ())
            .map_err(|e| format!("parse LONG literal `{lit_str}` for column `{column_name}`: {e}")),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(()),
        DataType::Boolean => lit_str
            .parse::<bool>()
            .map(|_| ())
            .map_err(|e| format!("parse BOOL literal `{lit_str}` for column `{column_name}`: {e}")),
        DataType::Timestamp(TimeUnit::Microsecond, zone) => {
            // SQL DATETIME literals arrive as 'YYYY-MM-DD HH:MM:SS[.ffffff]'.
            // Try sub-second precision first, then whole-second form.
            let label = if zone.is_some() {
                "TIMESTAMPTZ"
            } else {
                "DATETIME"
            };
            NaiveDateTime::parse_from_str(lit_str, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(lit_str, "%Y-%m-%d %H:%M:%S"))
                .map(|_| ())
                .map_err(|e| {
                    format!("parse {label} literal `{lit_str}` for column `{column_name}`: {e}")
                })
        }
        other => Err(format!(
            "phase 1 DELETE WHERE primitive type {other:?} not yet supported (column `{column_name}`)"
        )),
    }
}

/// Extract the string value from a SQL literal expression (`'...'` or `"..."`).
fn extract_string_literal(expr: &sqlast::Expr) -> Option<&str> {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => match value {
            sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
                Some(s.as_str())
            }
            _ => None,
        },
        sqlast::Expr::Nested(inner) => extract_string_literal(inner),
        _ => None,
    }
}

/// Extract the integer value from a SQL literal expression (`123` or `-123`).
fn extract_integer_literal(expr: &sqlast::Expr) -> Option<i64> {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(s, _),
            ..
        }) => s.parse::<i64>().ok(),
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::Number(s, _),
                ..
            }) => s.parse::<i64>().ok().map(|n| -n),
            _ => None,
        },
        sqlast::Expr::Nested(inner) => extract_integer_literal(inner),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use sqlparser::ast as sqlast;

    fn column(name: &str, data_type: DataType) -> novarocks_catalog::schema::ColumnDef {
        novarocks_catalog::schema::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn where_expr(sql: &str) -> sqlast::Expr {
        let statement =
            novarocks_sql::planning::dml::parse_raw_statement(sql).expect("parse query");
        let sqlast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        let sqlast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        select.selection.clone().expect("where clause")
    }

    /// Variant columns reach row DML as LargeBinary, which is what keeps them
    /// distinguishable from a genuine string column.
    fn columns_with_variant() -> Vec<novarocks_catalog::schema::ColumnDef> {
        vec![
            column("id", DataType::Int32),
            column("v", DataType::LargeBinary),
        ]
    }

    fn columns_with_timestamp() -> Vec<novarocks_catalog::schema::ColumnDef> {
        vec![
            column("id", DataType::Int32),
            column(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ),
        ]
    }

    #[test]
    fn delete_validate_accepts_variant_get_predicate_for_pipeline_filtering() {
        let where_clause =
            where_expr("SELECT 1 FROM orders WHERE try_variant_get(v, '$.a', 'bigint') = 2");
        super::validate_where(&where_clause, &columns_with_variant())
            .expect("variant_get predicate should be delegated to the query pipeline");
    }

    #[test]
    fn delete_validate_rejects_a_direct_comparison_against_a_variant_column() {
        // Without the write-target type a variant column would look like a
        // string here and the comparison would be wrongly accepted.
        let where_clause = where_expr("SELECT 1 FROM orders WHERE v = 'x'");
        let error = super::validate_where(&where_clause, &columns_with_variant())
            .expect_err("a bare variant comparison is not supported");
        assert!(error.contains("LargeBinary"), "{error}");
    }

    #[test]
    fn delete_position_sink_query_projects_row_identity_and_partition_sources() {
        let target = novarocks::catalog_application::resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
        };
        let sink_columns = vec![
            column("_file", DataType::Utf8),
            column("_pos", DataType::Int64),
            column("region", DataType::Utf8),
        ];
        let where_clause = where_expr("SELECT 1 FROM orders WHERE region = 'east' AND amount = 10");

        let query =
            super::build_delete_position_sink_query(&target, &where_clause, &sink_columns, "main")
                .expect("rewrite query");
        let rendered = query.to_string();

        assert!(rendered.contains("`_file`"));
        assert!(rendered.contains("`_pos`"));
        assert!(rendered.contains("`region`"));
        assert!(rendered.contains("FROM `ice`.`db`.`orders`"));
        assert!(!rendered.contains("FOR VERSION AS OF"));
    }

    #[test]
    fn delete_position_sink_query_pins_branch_read_snapshot() {
        let target = novarocks::catalog_application::resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
        };
        let sink_columns = vec![
            column("_file", DataType::Utf8),
            column("_pos", DataType::Int64),
        ];
        let where_clause = where_expr("SELECT 1 FROM orders WHERE id = 1");

        let query =
            super::build_delete_position_sink_query(&target, &where_clause, &sink_columns, "dev")
                .expect("rewrite query");

        let rendered = query.to_string();
        assert!(rendered.contains("FROM `ice`.`db`.`orders`"));
        assert!(rendered.contains("FOR SYSTEM_TIME AS OF '__nr_ref:dev'"));
    }

    // --------------- Timestamp predicate tests ---------------

    #[test]
    fn delete_validate_accepts_datetime_literals_with_and_without_subseconds() {
        for literal in ["2020-01-01 00:00:00", "2020-01-01 00:00:00.5"] {
            let expr = sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::SingleQuotedString(literal.to_string()),
                span: sqlparser::tokenizer::Span::empty(),
            });
            super::validate_literal_for_column(&expr, &columns_with_timestamp(), "ts")
                .unwrap_or_else(|error| panic!("`{literal}` must be accepted: {error}"));
        }
    }

    #[test]
    fn delete_validate_rejects_a_malformed_datetime_literal() {
        let expr = sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::SingleQuotedString("2020-01-01T00:00:00".to_string()),
            span: sqlparser::tokenizer::Span::empty(),
        });
        let error = super::validate_literal_for_column(&expr, &columns_with_timestamp(), "ts")
            .expect_err("ISO-8601 `T` separator is not the accepted DATETIME form");
        assert!(error.contains("DATETIME"), "{error}");
    }
}
