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
//! 3. Validate the parser-owned WHERE before constructing the generated sink query. Phase 1
//!    supports comparison operators (`= != < <= > >=`), `IN (...)`, and
//!    `AND` / `OR` against primitive columns (int / long / string / bool / timestamp).
//!    Other expressions are rejected with an explicit error.
//! 4. Rewrite DELETE into a SELECT of `_file`, `_pos`, and partition source
//!    columns, then run it through the distributed `ICEBERG_DELETE_SINK`.
//! 5. Route the sink output through the Iceberg write transaction runner,
//!    which commits the generated position-delete files and drives
//!    finalization lifecycle.

use std::sync::{Arc, Mutex};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::resolver::{TargetBackend, resolve_existing_table_target};
use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::dml::delete::{
    DeleteOperation, PreparedDelete, PreparedDeleteExecution, prepared_delete,
};
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::planning::write_sink::{
    admit_session_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use novarocks_parser::ast::{
    BinaryOperator, Delete, Expr, FunctionCall, IsPredicate, LiteralKind,
    ObjectName as ParserObjectName, Query, Statement, UnaryOperator,
};
use novarocks_spi::connector::ConnectorRowMutationStrategy;
use novarocks_spi::connector::ConnectorWriteOperationId;
use novarocks_sql::planning::dml::{DmlWriteSinkMode, IcebergRefSuffix, split_ref_suffix};
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;
use novarocks_sql::semantic::ObjectName as SqlObjectName;
use novarocks_types::schema::ColumnDef;

pub(crate) fn prepare_delete_statement(
    state: &DmlExecutionKernel,
    stmt: &Delete,
    _source: &str,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    publication_id: novarocks_spi::connector::LakePublicationId,
) -> Result<PreparedDelete, String> {
    // Detect branch/tag suffix in the target table name.
    let target_name = sql_object_name(&stmt.target);
    let (stripped_parts, ref_suffix) = split_ref_suffix(&target_name.parts);
    let effective_name;
    let table_name: &SqlObjectName = match ref_suffix {
        Some(IcebergRefSuffix::Tag(ref tag_name)) => {
            return Err(format!(
                "iceberg ref: tag '{tag_name}' is read-only; use a branch as DML target"
            ));
        }
        Some(IcebergRefSuffix::Branch(_)) => {
            effective_name = SqlObjectName {
                parts: stripped_parts,
            };
            &effective_name
        }
        None => &target_name,
    };
    let target_ref = match &ref_suffix {
        Some(IcebergRefSuffix::Branch(b)) => b.clone(),
        _ => "main".to_string(),
    };

    // 1. Resolve target.
    let target =
        resolve_existing_table_target(state, table_name, current_catalog, current_database)?;
    if target.provider_id.as_str() != "iceberg" {
        return Err(format!(
            "phase 1 DELETE only supports iceberg backends, got `{}`",
            target.provider_id.as_str()
        ));
    }
    let target_binding = crate::connector::write_target::load_write_target_binding(
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
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        state.connector_control().as_ref(),
        state.mv_storage_observation().as_ref(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Delete,
    )?;

    // 3. Reject an unsupported WHERE clause before any external side effect.
    //    The distributed SELECT planner owns scan pruning and existing delete
    //    visibility from this point onward. Column types come from the provider,
    //    so this check never decodes an Iceberg schema itself.
    let where_clause = stmt.selection.as_ref().ok_or_else(|| {
        "DELETE requires a WHERE clause; for full table replacement use \
         INSERT OVERWRITE t SELECT * FROM t WHERE FALSE"
            .to_string()
    })?;
    validate_where(where_clause, &target_binding.dml_target_columns())?;
    let where_sql = novarocks_parser::printer::print_expr(where_clause);

    // 4. Ask the provider to admit the row mutation. The physical strategy, the
    //    branch/format admission gates and the base version the frontend
    //    journals all come back signed; nothing here re-derives them. The
    //    provider reservation stays where DELETE has always made it, before the
    //    frontend persists its operation intent -- unlike UPDATE and MERGE,
    //    which defer admission until after. Aligning the two is a lifecycle
    //    change and not part of this cutover.
    let connector_operation_id = publication_id.into();
    let (write_lease, row_mutation) = target_binding.prepare_row_mutation(
        &target_ref,
        connector_operation_id,
        novarocks_spi::connector::ConnectorRowMutationIntent::Delete,
        connector_context.clone(),
    )?;
    let strategy = row_mutation.strategy();
    let base_snapshot_id = row_mutation.base_version_ordinal();

    prepare_delete_write(
        state,
        &target,
        strategy,
        base_snapshot_id,
        connector_operation_id,
        &write_lease,
        &target_ref,
        &where_sql,
        execution.clone(),
        connector_context,
        planning_lease,
        publication_id,
    )
}

struct DistributedDeleteWriteExecutor {
    state: DmlExecutionKernel,
    target: TargetBackend,
    delete_query: Query,
    sql_write_input: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<QueryTableBindingStore>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    /// The one commit authority for this statement.
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    /// Deletion vectors are written one per target data file, so the sink output
    /// is shuffled by its first column. Position deletes have no such
    /// requirement. Both follow from the provider-signed strategy.
    shuffle_by_first_output: bool,
    native_assembly: Mutex<Option<crate::query_execution::compiler::PreparedDmlWriteAssembly>>,
}

impl PreparedDeleteExecution for DistributedDeleteWriteExecutor {
    fn native_encoding(
        &self,
    ) -> Result<
        crate::query_execution::dml::delete::DeleteNativeEncoding<'_>,
        crate::dml::error::DmlExecutionError,
    > {
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
                crate::query_execution::compiler::prepare_query_as_iceberg_write_with_write_session(
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
                    Arc::clone(&self.write_session),
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

    fn terminal_request_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        self.connector_context.clone()
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
    base_snapshot_id: Option<i64>,
    connector_operation_id: ConnectorWriteOperationId,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    target_ref: &str,
    where_sql: &str,
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    publication_id: novarocks_spi::connector::LakePublicationId,
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

    // One row-mutation delete branch. The provider-signed strategy chooses
    // between a deletion vector and position deletes; the session admits that
    // shape and signs back the branch this statement writes.
    let write_session = crate::query_execution::write_session::begin_connector_write_session(
        crate::connector::write_target::derive_write_stack_lease(
            state.typed_connector_control(),
            &planning_lease,
        )?,
        write_lease,
        crate::query_execution::dml::iceberg_writer::connector_write_begin_request(
            target,
            target_ref,
            novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            delete_input_request(deletion_vectors),
            novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::RowMutation,
            connector_context.clone(),
        )?,
    )?;
    // A DELETE writes exactly one branch, so the session must have sealed
    // exactly one target and that target must carry the provider's row-mutation
    // routing facts. Both are proved here rather than assumed: a session that
    // sealed something else would otherwise plan a writer for a branch this
    // statement never admitted.
    let write_target = match write_session.targets() {
        [write_target] => write_target,
        targets => {
            return Err(format!(
                "DELETE requires exactly one sealed write target, session sealed {}",
                targets.len()
            ));
        }
    };
    if write_target.route().is_none() {
        return Err(format!(
            "DELETE write target {} carries no provider routing facts",
            write_target.ordinal().get()
        ));
    }

    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding = admit_session_connector_write_target(
        table_bindings.as_ref(),
        FrozenConnectorScanIdentity::new(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        ),
        write_target,
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
        where_sql,
        &write_input_columns(write_target.input()),
        target_ref,
    )?;
    let executor = DistributedDeleteWriteExecutor {
        state: state.clone(),
        target: target.clone(),
        delete_query,
        sql_write_input,
        table_bindings,
        execution,
        connector_context: connector_context.clone(),
        write_session,
        // Deletion vectors are written one per target data file, so the sink
        // output is shuffled by its first column; position deletes have no such
        // requirement.
        shuffle_by_first_output: deletion_vectors,
        native_assembly: Mutex::new(None),
    };
    Ok(prepared_delete(
        DeleteOperation {
            publication_id,
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
    where_clause: &str,
    sink_columns: &[ColumnDef],
    target_ref: &str,
) -> Result<Query, String> {
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

/// The delete-shaped input this statement asks the session to admit.
///
/// The identity columns are the engine's own row-position projection; the
/// Iceberg Provider derives the frozen partition-source fields from the exact
/// admitted metadata, so SQL never reconstructs them.
fn delete_input_request(
    deletion_vectors: bool,
) -> novarocks_spi::connector::ConnectorWriteInputRequest {
    use novarocks_spi::connector::{ConnectorWriteFieldRequest, ConnectorWriteInputRequest};

    let identity_fields = vec![
        ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(
            novarocks_execution::exec::row_position::ICEBERG_FILE_PATH_COL,
            DataType::Utf8,
            false,
        )),
        ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(
            novarocks_execution::exec::row_position::ICEBERG_ROW_POS_COL,
            DataType::Int64,
            false,
        )),
    ];
    if deletion_vectors {
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields: Vec::new(),
        }
    } else {
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields: Vec::new(),
        }
    }
}

fn write_input_columns(
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
) -> Vec<ColumnDef> {
    input
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

fn parse_generated_query(sql: &str, context: &str) -> Result<Query, String> {
    let statements = novarocks_parser::parse(sql)
        .map_err(|error| format!("{context}: native SQL parser rejection: {error}"))?;
    match statements.as_slice() {
        [Statement::Query(query)] => Ok(query.clone()),
        [other] => Err(format!(
            "{context}: generated non-query statement: {}",
            novarocks_parser::printer::print_statement(other)
        )),
        _ => Err(format!(
            "{context}: generated {} statements, expected exactly one query",
            statements.len()
        )),
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

fn sql_object_name(name: &ParserObjectName) -> SqlObjectName {
    SqlObjectName {
        parts: name.parts.iter().map(|part| part.value.clone()).collect(),
    }
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
fn validate_where(expr: &Expr, columns: &[ColumnDef]) -> Result<(), String> {
    match expr {
        Expr::Binary(binary) => match binary.operator {
            BinaryOperator::And | BinaryOperator::Or => {
                validate_where(&binary.left, columns)?;
                validate_where(&binary.right, columns)
            }
            BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual => {
                // Detect scalar_fn(col) <op> literal pattern first.
                // Function-call predicates cannot be pushed into Iceberg column
                // statistics (the function obscures the underlying column value),
                // so we return AlwaysTrue here to scan all files and leave
                // correctness to the per-row evaluator in evaluate_where_at_row.
                if extract_scalar_fn_comparison(&binary.left, &binary.right).is_some()
                    || extract_variant_get_comparison(&binary.left, &binary.right).is_some()
                {
                    return Ok(());
                }
                let (col_name, value_expr, _flipped) =
                    extract_comparison(&binary.left, &binary.right)?;
                validate_literal_for_column(value_expr, columns, &col_name)
            }
            other => Err(format!(
                "phase 1 DELETE WHERE does not support binary operator `{other:?}`"
            )),
        },
        Expr::InList(in_list) => {
            let col_name = expr_to_column_name(&in_list.expr)?;
            for literal in &in_list.list {
                validate_literal_for_column(literal, columns, &col_name)?;
            }
            Ok(())
        }
        Expr::IsPredicate(predicate)
            if matches!(
                predicate.predicate,
                IsPredicate::Null | IsPredicate::NotNull
            ) =>
        {
            expr_to_column_name(&predicate.expr).map(|_| ())
        }
        Expr::Nested(nested) => validate_where(&nested.expression, columns),
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
    left: &'a Expr,
    right: &'a Expr,
) -> Result<(String, &'a Expr, bool), String> {
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
    left: &'a Expr,
    right: &'a Expr,
) -> Option<(String, String, &'a Expr, bool)> {
    if let Some((fn_name, col_name)) = expr_as_supported_scalar_fn_on_col(left)
        && is_literal_expr(right)
    {
        return Some((fn_name, col_name, right, false));
    }
    if let Some((fn_name, col_name)) = expr_as_supported_scalar_fn_on_col(right)
        && is_literal_expr(left)
    {
        return Some((fn_name, col_name, left, true));
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
    left: &'a Expr,
    right: &'a Expr,
) -> Option<(String, &'a Expr, bool)> {
    if let Some(col_name) = expr_as_variant_get_on_col(left)
        && is_literal_expr(right)
    {
        return Some((col_name, right, false));
    }
    if let Some(col_name) = expr_as_variant_get_on_col(right)
        && is_literal_expr(left)
    {
        return Some((col_name, left, true));
    }
    None
}

fn expr_as_variant_get_on_col(expr: &Expr) -> Option<String> {
    let Expr::FunctionCall(func) = expr else {
        return None;
    };
    let name = func
        .name
        .parts
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
        .to_ascii_lowercase();
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
fn expr_as_supported_scalar_fn_on_col(expr: &Expr) -> Option<(String, String)> {
    let Expr::FunctionCall(func) = expr else {
        return None;
    };
    let name = func
        .name
        .parts
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
        .to_ascii_lowercase();
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

fn function_expr_args(func: &FunctionCall) -> Option<Vec<&Expr>> {
    (func.quantifier == novarocks_parser::ast::FunctionQuantifier::None
        && func.order_by.is_empty()
        && func.separator.is_none()
        && func.filter.is_none()
        && func.over.is_none())
    .then(|| func.arguments.iter().collect())
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
fn is_literal_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Unary(unary) if unary.operator == UnaryOperator::Minus => {
            matches!(unary.expression.as_ref(), Expr::Literal(_))
        }
        Expr::Nested(nested) => is_literal_expr(&nested.expression),
        _ => false,
    }
}

fn expr_to_column_name(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => {
            // a.b.c → take the last part (the column name); table-qualified
            // refs work because the Predicate is bound against the
            // single-table schema via TableScan.with_filter.
            parts
                .parts
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
    expr: &Expr,
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
        Expr::Literal(literal) => literal,
        Expr::Unary(unary) if unary.operator == UnaryOperator::Minus => {
            match unary.expression.as_ref() {
                Expr::Literal(literal) => literal,
                other => {
                    return Err(format!(
                        "phase 1 DELETE WHERE expects a literal value, got -{other:?}"
                    ));
                }
            }
        }
        other => {
            return Err(format!(
                "phase 1 DELETE WHERE expects a literal value, got {other:?}"
            ));
        }
    };
    let negate = matches!(
        expr,
        Expr::Unary(unary) if unary.operator == UnaryOperator::Minus
    );
    let lit_str = match &lit_value.kind {
        LiteralKind::Number(s) | LiteralKind::String(s) => s.clone(),
        LiteralKind::Boolean(b) => b.to_string(),
        LiteralKind::Null => {
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
fn extract_string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(literal) => match &literal.kind {
            LiteralKind::String(value) => Some(value.as_str()),
            _ => None,
        },
        Expr::Nested(nested) => extract_string_literal(&nested.expression),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_parser::Span;
    use novarocks_parser::ast::{DmlStatement, Expr, Literal, LiteralKind, Statement};

    fn column(name: &str, data_type: DataType) -> novarocks_types::schema::ColumnDef {
        novarocks_types::schema::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn where_expr(sql: &str) -> Expr {
        let statements = novarocks_parser::parse(&format!("DELETE FROM orders WHERE {sql}"))
            .expect("parse DELETE");
        let Statement::Dml(DmlStatement::Delete(delete)) = &statements[0] else {
            panic!("expected DELETE");
        };
        delete.selection.clone().expect("where clause")
    }

    /// Variant columns reach row DML as LargeBinary, which is what keeps them
    /// distinguishable from a genuine string column.
    fn columns_with_variant() -> Vec<novarocks_types::schema::ColumnDef> {
        vec![
            column("id", DataType::Int32),
            column("v", DataType::LargeBinary),
        ]
    }

    fn columns_with_timestamp() -> Vec<novarocks_types::schema::ColumnDef> {
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
        let where_clause = where_expr("try_variant_get(v, '$.a', 'bigint') = 2");
        super::validate_where(&where_clause, &columns_with_variant())
            .expect("variant_get predicate should be delegated to the query pipeline");
    }

    #[test]
    fn delete_validate_rejects_a_direct_comparison_against_a_variant_column() {
        // Without the write-target type a variant column would look like a
        // string here and the comparison would be wrongly accepted.
        let where_clause = where_expr("v = 'x'");
        let error = super::validate_where(&where_clause, &columns_with_variant())
            .expect_err("a bare variant comparison is not supported");
        assert!(error.contains("LargeBinary"), "{error}");
    }

    #[test]
    fn delete_position_sink_query_projects_row_identity_and_partition_sources() {
        let target = crate::catalog_application::resolver::TargetBackend {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static Iceberg provider ID"),
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
        };
        let sink_columns = vec![
            column("_file", DataType::Utf8),
            column("_pos", DataType::Int64),
            column("region", DataType::Utf8),
        ];
        let where_clause = "region = 'east' AND amount = 10";

        let query =
            super::build_delete_position_sink_query(&target, where_clause, &sink_columns, "main")
                .expect("rewrite query");
        let rendered = novarocks_parser::printer::print_query(&query);

        assert!(rendered.contains("`_file`"));
        assert!(rendered.contains("`_pos`"));
        assert!(rendered.contains("`region`"));
        assert!(rendered.contains("FROM `ice`.`db`.`orders`"));
        assert!(!rendered.contains("FOR VERSION AS OF"));
    }

    #[test]
    fn delete_position_sink_query_pins_branch_read_snapshot() {
        let target = crate::catalog_application::resolver::TargetBackend {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static Iceberg provider ID"),
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
        };
        let sink_columns = vec![
            column("_file", DataType::Utf8),
            column("_pos", DataType::Int64),
        ];
        let where_clause = "id = 1";

        let query =
            super::build_delete_position_sink_query(&target, where_clause, &sink_columns, "dev")
                .expect("rewrite query");

        let rendered = novarocks_parser::printer::print_query(&query);
        assert!(rendered.contains("FROM `ice`.`db`.`orders`"));
        assert!(rendered.contains("FOR VERSION AS OF 'dev'"));
    }

    // --------------- Timestamp predicate tests ---------------

    #[test]
    fn delete_validate_accepts_datetime_literals_with_and_without_subseconds() {
        for literal in ["2020-01-01 00:00:00", "2020-01-01 00:00:00.5"] {
            let expr = Expr::Literal(Literal {
                kind: LiteralKind::String(literal.to_string()),
                span: Span::new(0, literal.len()),
            });
            super::validate_literal_for_column(&expr, &columns_with_timestamp(), "ts")
                .unwrap_or_else(|error| panic!("`{literal}` must be accepted: {error}"));
        }
    }

    #[test]
    fn delete_validate_rejects_a_malformed_datetime_literal() {
        let expr = Expr::Literal(Literal {
            kind: LiteralKind::String("2020-01-01T00:00:00".to_string()),
            span: Span::new(0, 21),
        });
        let error = super::validate_literal_for_column(&expr, &columns_with_timestamp(), "ts")
            .expect_err("ISO-8601 `T` separator is not the accepted DATETIME form");
        assert!(error.contains("DATETIME"), "{error}");
    }
}
