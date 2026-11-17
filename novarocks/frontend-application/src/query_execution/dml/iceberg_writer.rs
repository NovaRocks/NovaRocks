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

//! Core-owned Iceberg write preparation and execution primitives.
//!
//! Frontend DML services own production statement routing and transaction
//! orchestration over these primitives.

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog_application::resolver::TargetBackend;
use crate::connector::backend::ResolvedTable;
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::planning::write_sink::{
    admit_session_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use novarocks_parser::ast::Query;
use novarocks_parser::{SYNTHETIC, ast, build};
use novarocks_query_application::admitted_query_context::QueryExecutionContext;
use novarocks_spi::connector::{
    ConnectorWriteAdmissionPurpose, ConnectorWriteInputRequest, ConnectorWriteIntent,
};
use novarocks_sql::planning::dml::DmlWriteSinkMode;
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;
use novarocks_types::schema::{ColumnDef, ColumnDefault};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IcebergWriteMode {
    Append,
    FullTableOverwrite,
    DynamicPartitionOverwrite,
}

/// Resolve and prepare the original INSERT through direct asynchronous
/// Provider I/O. The admitted action retains its InitialComputationReservation;
/// this entry neither reserves the old coordinator nor compiles a native plan.
#[expect(
    clippy::too_many_arguments,
    reason = "The write preparation boundary keeps the exact target, statement and provider generation explicit."
)]
pub(crate) async fn prepare_iceberg_write_async(
    state: &DmlExecutionKernel,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &Query,
    overwrite_mode: IcebergWriteMode,
    target_ref: &str,
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    begin_receiving: Arc<crate::query_execution::write_session::WriteBeginReceiving>,
) -> Result<PreparedIcebergWrite, String> {
    debug_assert_eq!(target.provider_id.as_str(), "iceberg");
    let context =
        crate::connector::context_for_planning_pin(&planning_pin, connector_context.clone())?;
    let metadata = crate::connector::schema_creation::load_frontend_table_async(
        planning_pin.binding().metadata().as_ref(),
        novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: planning_pin.binding().descriptor().instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let write_target =
        crate::connector::write_target::ConnectorWriteTargetBinding::new(metadata, planning_pin);
    let write_lease = write_target.derive_write_lease()?;
    let source_control =
        crate::query_execution::planning::sql_compile_control_from_connector_request(
            connector_context,
        );
    let intent = iceberg_write_intent(overwrite_mode);
    // The same closed SQL author admits the original provider input before
    // shaping any casts. This is pure work, never a joining metadata wrapper.
    let (query, source_constants, input) = build_iceberg_write_plan(
        &write_lease,
        target_ref,
        intent,
        connector_context,
        resolved,
        insert_columns,
        source,
        write_target.metadata(),
        state.constant_policy(),
        &source_control,
    )?;
    let catalog_properties = write_lease.catalog_properties().cloned().ok_or_else(|| {
        "connector write lease has no immutable catalog runtime identity".to_string()
    })?;
    // The same original receiving source stores the plan/session before the
    // Provider child Future's Drop. The action retains this Arc and observes
    // its local health until actual exit; this phase owns only an alias.
    let write_session =
        crate::query_execution::write_session::ConnectorWriteSession::begin_async_in(
            write_target.derive_write_stack_lease(state.typed_connector_control())?,
            catalog_properties,
            connector_write_begin_request(
                target,
                target_ref,
                intent,
                input,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::Ordinary,
                connector_context.clone(),
            )?,
            begin_receiving.as_ref(),
        )
        .await
        .map_err(|error| format!("begin connector write session: {error}"))?;
    let (sql_write_input, table_bindings) =
        bind_iceberg_session_write_input(target, &write_target, &write_session)?;
    // Preserve the original RefHead observation and branch validation order.
    // The write session retains its provider-sealed base independently.
    let facts = crate::connector::metadata_read_reference_facts_with_planning_pin_async(
        write_target.planning_pin().clone(),
        connector_context.clone(),
        write_target.identity().namespace.as_ref(),
        write_target.identity().table.as_ref(),
    )
    .await?;
    let _base_snapshot_id = if target_ref == "main" {
        facts.current_snapshot_id()
    } else {
        Some(
            facts
                .named_references()
                .iter()
                .find(|reference| reference.name.as_ref() == target_ref)
                .map(|reference| reference.snapshot_id)
                .ok_or_else(|| {
                    format!("iceberg ref: branch '{target_ref}' not found in table metadata")
                })?,
        )
    };
    Ok(finish_iceberg_prepared_write(
        state,
        target,
        query,
        source_constants,
        sql_write_input,
        table_bindings,
        Some(execution),
        connector_context,
        write_session,
    ))
}

const fn iceberg_write_intent(overwrite_mode: IcebergWriteMode) -> ConnectorWriteIntent {
    match overwrite_mode {
        IcebergWriteMode::Append => ConnectorWriteIntent::Append,
        IcebergWriteMode::FullTableOverwrite => ConnectorWriteIntent::Overwrite,
        IcebergWriteMode::DynamicPartitionOverwrite => ConnectorWriteIntent::PartitionOverwrite,
    }
}

fn bind_iceberg_session_write_input(
    target: &TargetBackend,
    write_target: &crate::connector::write_target::ConnectorWriteTargetBinding,
    write_session: &crate::query_execution::write_session::ConnectorWriteSession,
) -> Result<
    (
        novarocks_sql::planning::dml::DmlWritePlanInput,
        Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    ),
    String,
> {
    // The plan's target is the one the session sealed: its input shape carries
    // the provider's field tokens for the very handle the writer carries, so
    // the plan and that writer's recipe cannot name two different signings.
    let session_target = sole_session_write_target(write_session)?;
    let table_bindings =
        Arc::new(crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()?);
    let target_binding = admit_session_connector_write_target(
        table_bindings.as_ref(),
        FrozenConnectorScanIdentity::new(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        ),
        session_target,
        write_target.planning_pin().clone(),
    )?;
    let sql_write_input = dml_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        DmlWriteSinkMode::Data,
        novarocks_sql::planning::dml::ConnectorWriteInputBinding::RootOutputByOrdinal,
    )?;

    Ok((sql_write_input, table_bindings))
}

fn finish_iceberg_prepared_write(
    state: &DmlExecutionKernel,
    target: &TargetBackend,
    query: Query,
    source_constants: Option<novarocks_sql::compiler::SqlExpressionConstants>,
    sql_write_input: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    execution: Option<QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
) -> PreparedIcebergWrite {
    let semantic_binding = FrozenIcebergWriteSemanticBinding {
        state: state.clone(),
        target: target.clone(),
        query,
        source_constants,
        sql_write_input,
        table_bindings,
        execution,
        connector_context: connector_context.clone(),
        write_session,
    };
    PreparedIcebergWrite {
        semantic_binding: Arc::new(semantic_binding),
    }
}

/// Build the begin request for one distributed write.
///
/// The table is named the way the provider parses it -- namespace-qualified,
/// last dot separating the table -- so a multi-level namespace round-trips
/// instead of losing its trailing level.
pub(crate) fn connector_write_begin_request(
    target: &TargetBackend,
    target_ref: &str,
    intent: ConnectorWriteIntent,
    input: ConnectorWriteInputRequest,
    purpose: ConnectorWriteAdmissionPurpose,
    flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, String> {
    connector_write_begin_request_on_base(
        target, target_ref, intent, input, purpose, None, flavor, context,
    )
}

/// The same request, opened on an exact provider-issued base version.
///
/// An application-document publication must name the base its declaration was
/// built against, so the provider can refuse a session that would publish onto
/// a different one.
pub(crate) fn connector_write_begin_request_on_base(
    target: &TargetBackend,
    target_ref: &str,
    intent: ConnectorWriteIntent,
    input: ConnectorWriteInputRequest,
    purpose: ConnectorWriteAdmissionPurpose,
    base: Option<novarocks_spi::connector::ConnectorWriteBaseVersion>,
    flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, String> {
    Ok(
        novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
            table: Arc::from(format!("{}.{}", target.namespace, target.table).as_str()),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse(target_ref)
                .map_err(|error| format!("validate connector write target ref: {error}"))?,
            intent,
            purpose,
            input,
            base,
            flavor,
            context,
        },
    )
}

/// The sole logical target an ordinary single-target write session sealed.
pub(crate) fn sole_session_write_target(
    session: &crate::query_execution::write_session::ConnectorWriteSession,
) -> Result<&novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan, String> {
    let ordinal = session
        .seal_write_targets()
        .map_err(|error| error.to_string())?
        .sole_target_ordinal()?;
    session
        .targets()
        .iter()
        .find(|target| target.ordinal() == ordinal)
        .ok_or_else(|| "write session omitted its sealed write target".to_string())
}

// Resolve an opaque Iceberg write target through the connector metadata
// capability owned by the exact generation observed at write admission.
// Core only forwards the target identity; it never builds a handle payload.

pub(crate) struct PreparedIcebergWrite {
    semantic_binding: Arc<FrozenIcebergWriteSemanticBinding>,
}

impl PreparedIcebergWrite {
    /// Compile only after the original action has installed this Prepared and
    /// session in its stable source. The phase Future merely borrows them.
    pub(crate) async fn prepare_native_assembly_for_execution_async(
        &self,
        provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
        query_options: Option<novarocks_proto_codec::lifecycle::QueryOptions>,
    ) -> Result<
        crate::query_execution::compiler::PreparedDmlWriteAssembly,
        crate::dml::error::DmlExecutionError,
    > {
        let execution = self.semantic_binding.execution.as_ref().ok_or_else(|| {
            "async Iceberg write compilation requires the original admitted execution".to_string()
        })?;
        crate::query_execution::compiler::prepare_query_as_iceberg_write_with_write_session_and_constants_async(
            &self.semantic_binding.state,
            Some(&self.semantic_binding.target.catalog),
            &self.semantic_binding.target.namespace,
            &self.semantic_binding.query,
            self.semantic_binding.source_constants.clone(),
            self.semantic_binding.sql_write_input.clone(),
            Arc::clone(&self.semantic_binding.table_bindings),
            query_options,
            novarocks_sql::compiler::RootDistributionRequirement::Any,
            execution,
            &self.semantic_binding.connector_context,
            Arc::clone(&self.semantic_binding.write_session),
            &[],
            provider_source,
            crate::query_execution::completion_facts::SystemTableSourcePolicy::Query,
        ).await
    }

    pub(crate) fn finalize(&self) -> Result<(), String> {
        crate::catalog_application::resolver::invalidate_iceberg_caches(
            &self.semantic_binding.state,
            &self.semantic_binding.target,
        )
    }
}

/// Prepared execution payload consumed by frontend DML adapters.
///
/// This type owns no SQL routing or application transaction policy. The
/// frontend DML services drive production statement lifecycles.
/// First-admission write semantics retained until its one native round. It
/// intentionally contains no native fragments, splits, writer cohorts,
/// runtime-filter layout, schedule, native bundle, or request.
struct FrozenIcebergWriteSemanticBinding {
    state: DmlExecutionKernel,
    target: TargetBackend,
    query: Query,
    source_constants: Option<novarocks_sql::compiler::SqlExpressionConstants>,
    sql_write_input: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    execution: Option<QueryExecutionContext>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    /// The one commit authority for this statement. Every round of this
    /// statement encodes the same sealed recipes from it.
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

/// Admit the complete provider-declared input before shaping INSERT source
/// expressions. This opens no write session and performs no external effect.
pub(crate) fn build_iceberg_write_plan(
    write_lease: &novarocks_spi::connector::ConnectorWrite,
    target_ref: &str,
    intent: ConnectorWriteIntent,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &Query,
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<
    (
        Query,
        Option<novarocks_sql::compiler::SqlExpressionConstants>,
        ConnectorWriteInputRequest,
    ),
    String,
> {
    let write_columns = insert_columns_from_connector_metadata(
        metadata,
        &write_defaults_by_name(&resolved.columns),
    )?;
    let input = ConnectorWriteInputRequest::Data {
        fields: write_columns
            .iter()
            .map(|column| {
                let ordinal = metadata
                    .schema()
                    .fields()
                    .iter()
                    .position(|field| field.name() == &column.name)
                    .ok_or_else(|| "INSERT target is absent from its signed metadata".to_owned())?;
                crate::connector::connector_write_input(metadata, ordinal)
            })
            .collect::<Result<Vec<_>, String>>()?,
    };
    // This admission trait signs input and checks provider-owned frozen facts;
    // it cannot open writers, register a session, or perform an external effect.
    // Keep the original request for begin_write after shaping succeeds.
    match write_lease
        .prepare_write(novarocks_spi::connector::ConnectorWritePreparationRequest {
            table: metadata.table.clone(),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse(target_ref)
                .map_err(|error| error.to_string())?,
            intent,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: input.clone(),
            context: connector_context.clone(),
        })
        .map_err(|error| error.to_string())?
    {
        novarocks_spi::connector::ConnectorWritePreparationOutcome::Prepared(_) => (),
        novarocks_spi::connector::ConnectorWritePreparationOutcome::Denied(error) => {
            return Err(format!("Iceberg write admission denied: {error}"));
        }
    }
    let source_columns = sql_write_source_columns(&resolved.columns, &write_columns);
    let (query, constants) = match crate::dml::insert::shape_typed_write_source(
        source,
        insert_columns,
        &source_columns,
        &write_columns,
        Some((policy, control)),
    )? {
        Some(shaped) => shaped,
        None => {
            let mut query = append_source_to_query_for_write(
                source,
                insert_columns,
                &source_columns,
                &write_columns,
            )?;
            let constants = crate::dml::insert::capture_typed_write_constants(
                &mut query,
                insert_columns,
                &source_columns,
                &write_columns,
                policy,
                control,
            )?;
            (query, constants)
        }
    };
    Ok((query, constants, input))
}

/// A connector read schema can carry execution-only fields (for example
/// row-lineage fields) alongside SQL target columns.  INSERT shaping is owned
/// by the SQL write target contract, so retain only the columns that exist in
/// that contract before assigning derived-query aliases.
fn sql_write_source_columns(
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Vec<ColumnDef> {
    source_columns
        .iter()
        .filter(|source| {
            write_columns
                .iter()
                .any(|target| target.name.eq_ignore_ascii_case(&source.name))
        })
        .cloned()
        .collect()
}

fn append_source_to_query_for_write(
    source: &Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Query, String> {
    if let Some((query, _)) = crate::dml::insert::shape_typed_write_source(
        source,
        insert_columns,
        source_columns,
        write_columns,
        None,
    )? {
        return Ok(query);
    }
    if insert_columns.is_empty() && same_column_sequence(source_columns, write_columns)? {
        Ok(source.clone())
    } else {
        wrap_insert_query_with_write_projection(
            source,
            insert_columns,
            source_columns,
            write_columns,
        )
    }
}

fn wrap_insert_query_with_write_projection(
    query: &Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Query, String> {
    let insert_idx_by_target = if insert_columns.is_empty() {
        std::collections::HashMap::new()
    } else {
        insert_column_index_by_target_name(insert_columns, write_columns)?
    };
    let source_alias = "__nr_insert_src";
    let mut projection = Vec::with_capacity(write_columns.len());
    for (write_idx, column) in write_columns.iter().enumerate() {
        let target_name = novarocks_types::naming::normalize_identifier(&column.name)?;
        let source = if let Some(source_idx) = insert_idx_by_target.get(&target_name) {
            Some((*source_idx, insert_columns[*source_idx].as_str()))
        } else if insert_columns.is_empty() {
            source_index_for_write_column(column, write_idx, source_columns, write_columns)
                .map(|index| (index, source_columns[index].name.as_str()))
        } else {
            None
        };
        let expression = match source {
            Some((ordinal, name)) => build::column(
                build::ident(source_alias, true),
                build::ident_at(name, true, insert_source_expression_span(query, ordinal)),
            ),
            None => crate::dml::insert::omitted_insert_expr(column)?,
        };
        projection.push(build::item(
            expression,
            Some(build::ident(&column.name, true)),
            true,
        ));
    }
    let alias_source_columns = if insert_columns.is_empty() {
        source_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
    } else {
        insert_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    };
    let alias_source_columns = alias_source_columns
        .into_iter()
        .map(|name| build::ident(name, true))
        .collect::<Vec<_>>();
    let alias_source_columns = (!alias_source_columns.is_empty()).then_some(alias_source_columns);
    let wrapped = build::query(ast::SetExpr::Select(novarocks_parser::ast::AstBox::new(
        build::select(
            projection,
            Some(build::alias(
                build::derived(query.clone()),
                build::ident(source_alias, true),
                alias_source_columns,
            )),
            None,
            None,
        )
        .map_err(|error| error.to_string())?,
    )));
    Ok(wrapped)
}

/// A direct SELECT output has one user expression that the outer cast derives
/// from. Other query shapes have no unique source expression for that output.
fn insert_source_expression_span(query: &Query, ordinal: usize) -> novarocks_parser::Span {
    match query.body.as_ref() {
        ast::SetExpr::Select(select) => {
            select
                .projection
                .get(ordinal)
                .map_or(SYNTHETIC, |item| match item {
                    ast::SelectItem::UnnamedExpr(expr)
                    | ast::SelectItem::ExprWithAlias { expr, .. } => expr.span(),
                    _ => SYNTHETIC,
                })
        }
        ast::SetExpr::Query(subquery) => {
            let query = &subquery.query;
            insert_source_expression_span(query, ordinal)
        }
        _ => SYNTHETIC,
    }
}

fn same_column_sequence(left: &[ColumnDef], right: &[ColumnDef]) -> Result<bool, String> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (source, target) in left.iter().zip(right) {
        let source_type = source
            .declared_value_type()
            .map_err(|error| error.to_string())?;
        let target_type = target
            .declared_value_type()
            .map_err(|error| error.to_string())?;
        if !source.name.eq_ignore_ascii_case(&target.name) || source_type != target_type {
            return Ok(false);
        }
    }
    Ok(true)
}

fn source_index_for_write_column(
    write_column: &ColumnDef,
    write_idx: usize,
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Option<usize> {
    source_columns
        .iter()
        .position(|source| source.name.eq_ignore_ascii_case(&write_column.name))
        .or_else(|| {
            (source_columns.len() == write_columns.len() && write_idx < source_columns.len())
                .then_some(write_idx)
        })
}

/// Derive the INSERT write columns from neutral connector facts.
///
/// This replaces reading the provider's Iceberg schema. Two facts make the
/// substitution exact rather than approximate:
///
/// - `ConnectorTableMetadata::schema` is the full physical Arrow schema.
///   Hidden columns (the IMV apply key, declared aggregate-state columns) are
///   *marked* in the planning facts rather than filtered out of the schema, so
///   the field set here is the same one `current_schema()` produced.
/// - Optional planning `write_input` carries the Provider's complete signed W
///   and independent storage T. SQL columns use its exact execution type W;
///   the original request retains T for writer validation and byte adaptation.
///   Without a separate write declaration, the complete execution-schema
///   binding authors the Exact input. Arrow carriers never infer a domain.
///
/// Write defaults keep coming from the resolved SQL table columns, unchanged.
fn insert_columns_from_connector_metadata(
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    write_defaults: &HashMap<String, ColumnDefault>,
) -> Result<Vec<ColumnDef>, String> {
    let column_facts = metadata.planning_facts.column_facts();
    metadata
        .schema()
        .fields()
        .iter()
        .enumerate()
        // The neutral schema is the read schema: it carries the Iceberg
        // metadata columns (`_file`, `_pos`, row-lineage) that a scan exposes
        // but that are not part of the table's declared column list. SQL column
        // binding must see only declared columns, so drop the ones the provider
        // marked as system columns. Hidden-but-declared columns (the IMV apply
        // key, aggregate-state columns) are `Ordinary` and stay.
        .filter(|(ordinal, _)| {
            column_facts.get(*ordinal).is_none_or(|fact| {
                fact.role() != novarocks_spi::connector::ConnectorTableColumnRole::RowLineageSystem
            })
        })
        .map(|(ordinal, field)| {
            crate::connector::connector_write_column(
                metadata,
                ordinal,
                write_defaults.get(field.name()).cloned(),
            )
        })
        .collect()
}

/// Index already-neutral write defaults by column name.
fn write_defaults_by_name(columns: &[ColumnDef]) -> HashMap<String, ColumnDefault> {
    columns
        .iter()
        .filter_map(|column| {
            column
                .write_default
                .as_ref()
                .map(|value| (column.name.clone(), value.clone()))
        })
        .collect()
}

fn insert_column_index_by_target_name(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<std::collections::HashMap<String, usize>, String> {
    let mut target_names = std::collections::HashSet::with_capacity(target_columns.len());
    for column in target_columns {
        target_names.insert(novarocks_types::naming::normalize_identifier(&column.name)?);
    }

    let mut mapping = std::collections::HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let normalized = novarocks_types::naming::normalize_identifier(column)?;
        if !target_names.contains(&normalized) {
            return Err(format!("unknown INSERT column `{column}`"));
        }
        if mapping.insert(normalized.clone(), idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }
    Ok(mapping)
}

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn target_string(t: &TargetBackend) -> String {
    format!("{}.{}.{}", t.catalog, t.namespace, t.table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields};
    use novarocks_parser::{ast, printer};
    use novarocks_spi::connector::ConnectorTableHandle;
    use novarocks_types::schema::ColumnDefault;

    fn test_column(
        name: &str,
        data_type: DataType,
        write_default: Option<ColumnDefault>,
    ) -> novarocks_types::schema::ColumnDef {
        novarocks_types::schema::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            write_default,
            logical_type: None,
        }
    }

    pub(super) fn write_metadata(
        fields: Vec<Field>,
        bindings: Vec<novarocks_type_contract::FunctionValueType>,
    ) -> novarocks_spi::connector::ConnectorTableMetadata {
        use novarocks_spi::connector::{
            ConnectorInstanceId, ConnectorTableDefinitionFacts, ConnectorTableIdentity,
            ConnectorTablePlanningFacts,
        };

        let instance = ConnectorInstanceId::parse("writer_domain_test").expect("instance");
        crate::connector::schema_creation::declared_test_metadata(
            novarocks_spi::connector::ConnectorTableMetadataHeader {
                identity: ConnectorTableIdentity {
                    instance_id: instance.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("target"),
                },
                planning_facts: ConnectorTablePlanningFacts::empty(),
                definition_facts: ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(
                    instance,
                    bytes::Bytes::from_static(b"target"),
                )
                .expect("checked table handle"),
            },
            Arc::new(arrow::datatypes::Schema::new(fields)),
            bindings,
        )
    }

    #[test]
    fn metadata_write_columns_preserve_authored_domains_defaults_and_nested_facts() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

        let cases = [
            (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
            (DataType::LargeBinary, ValueLogicalType::Variant),
            (DataType::Binary, ValueLogicalType::Hll),
            (DataType::Binary, ValueLogicalType::Bitmap),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
            (DataType::FixedSizeBinary(16), ValueLogicalType::Physical),
            (DataType::LargeBinary, ValueLogicalType::Physical),
            (DataType::Binary, ValueLogicalType::Physical),
        ];
        for (carrier, domain) in cases {
            let authored = FunctionValueType::try_with_logical_type(carrier, true, domain)
                .expect("authored provider field type");
            let metadata = write_metadata(
                vec![authored.try_to_field("Value").expect("field")],
                vec![authored.clone()],
            );
            let defaults = HashMap::from([("Value".to_string(), ColumnDefault::Null)]);
            let columns = insert_columns_from_connector_metadata(&metadata, &defaults)
                .expect("exact metadata columns");
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].name, "Value");
            assert_eq!(columns[0].write_default, Some(ColumnDefault::Null));
            assert_eq!(
                columns[0].declared_value_type().expect("column type"),
                authored
            );
            assert_eq!(
                columns[0]
                    .declared_value_type()
                    .expect("column type")
                    .try_to_field("Value")
                    .expect("write request field"),
                *metadata.schema().field(0),
            );
        }

        use novarocks_type_contract::{
            ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime as S,
        };
        let child =
            ValueTypeRef::try_new(ValueKind::Nominal(ValueLogicalType::Uuid), false, S::pure())
                .unwrap();
        let layout = DataType::Struct(
            vec![Arc::new(Field::new(
                "nested_uuid",
                DataType::FixedSizeBinary(16),
                false,
            ))]
            .into(),
        );
        let nested = FunctionValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::Struct(
                    vec![ValueField::new("nested_uuid", child, S::pure())].into_boxed_slice(),
                ),
                false,
                S::pure(),
            )
            .unwrap(),
            ValueLayout::try_new(&layout, S::pure()).unwrap(),
        )
        .unwrap();
        let metadata = write_metadata(
            vec![nested.try_to_field("nested").expect("nested field")],
            vec![nested.clone()],
        );
        let columns = insert_columns_from_connector_metadata(&metadata, &HashMap::new())
            .expect("complete nested write type");
        assert_eq!(
            columns[0].declared_value_type().expect("nested column"),
            nested
        );
    }

    #[test]
    fn metadata_write_columns_refuse_invalid_declarations_and_ignore_generic_tags() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType as L};
        for (carrier, logical) in [
            (DataType::Binary, L::Uuid),
            (DataType::FixedSizeBinary(16), L::Variant),
        ] {
            assert!(FunctionValueType::try_with_logical_type(carrier, true, logical).is_err());
        }
        let ty = FunctionValueType::new(DataType::Binary, true);
        let metadata = write_metadata(
            vec![Field::new("v", DataType::Binary, true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "nr_logical_type".into(),
                    "unknown".into(),
                )]),
            )],
            vec![ty.clone()],
        );
        let columns = insert_columns_from_connector_metadata(&metadata, &HashMap::new()).unwrap();
        assert_eq!(columns[0].declared_value_type().unwrap(), ty);
    }

    #[test]
    fn write_carrier_override_cannot_erase_an_authored_uuid_domain() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

        let uuid = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .expect("UUID type");
        let metadata = write_metadata(
            vec![uuid.try_to_field("v").expect("UUID field")],
            vec![uuid.clone()],
        );
        let error = novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
            Field::new("v", DataType::Binary, true),
            novarocks_type_contract::FunctionValueType::new(DataType::Binary, true),
        )
        .unwrap()
        .with_storage_type(uuid.clone())
        .expect_err("nominal UUID cannot grant a plain binary projection");
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(metadata.execution_schema().binding(0), Some(&uuid));
        // This fixture authors no optional planning override. Its complete
        // execution binding remains the authority for the UUID domain.
        assert!(metadata.planning_facts.column_facts().is_empty());
        assert_eq!(metadata.schema().fields().len(), 1);
        let columns = insert_columns_from_connector_metadata(&metadata, &HashMap::new()).unwrap();
        assert_eq!(columns[0].declared_value_type().unwrap(), uuid);
    }

    #[test]
    fn write_column_matching_and_casts_do_not_infer_domains_from_carriers() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

        for (carrier, domain, spelling) in [
            (DataType::Binary, ValueLogicalType::Hll, "HLL"),
            (DataType::Binary, ValueLogicalType::Bitmap, "BITMAP"),
            (
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
                "LARGEINT",
            ),
            (DataType::LargeBinary, ValueLogicalType::Variant, "VARIANT"),
        ] {
            let declared = ColumnDef::from_value_type(
                "v".into(),
                FunctionValueType::try_with_logical_type(carrier.clone(), true, domain)
                    .expect("declared source"),
                None,
            )
            .expect("semantic column");
            let plain = test_column("v", carrier, None);
            assert!(
                !same_column_sequence(
                    std::slice::from_ref(&declared),
                    std::slice::from_ref(&plain)
                )
                .expect("checked columns")
            );
            assert_eq!(
                printer::print_type_name(
                    &novarocks_sql::sql_type_syntax(&declared.declared_value_type().unwrap())
                        .expect("declared type")
                ),
                spelling
            );
            assert_eq!(
                printer::print_type_name(
                    &novarocks_sql::sql_type_syntax(&plain.declared_value_type().unwrap())
                        .expect("plain type")
                ),
                "VARBINARY"
            );
        }
        let uuid = ColumnDef::from_value_type(
            "v".into(),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .expect("UUID source"),
            None,
        )
        .expect("UUID column");
        assert_eq!(
            novarocks_sql::sql_type_syntax(&uuid.declared_value_type().unwrap())
                .expect_err("UUID is not a numeric DDL type")
                .to_string(),
            "UUID has no SQL DDL type spelling"
        );
    }

    pub(super) fn parse_query(sql: &str) -> Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected exactly one query statement");
        };
        query.clone()
    }

    fn test_map_type(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", key, false)),
                    Arc::new(Field::new("value", value, true)),
                ])),
                false,
            )),
            false,
        )
    }

    fn rows_query(rows: Vec<Vec<ast::Expr>>) -> Query {
        build::query(build::values(rows).expect("nonempty VALUES fixture"))
    }

    fn test_struct_type(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(name, data_type)| Arc::new(Field::new(name, data_type, true)))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn write_mode_preserves_provider_intent() {
        assert_eq!(
            iceberg_write_intent(IcebergWriteMode::Append),
            ConnectorWriteIntent::Append
        );
        assert_eq!(
            iceberg_write_intent(IcebergWriteMode::FullTableOverwrite),
            ConnectorWriteIntent::Overwrite
        );
        assert_eq!(
            iceberg_write_intent(IcebergWriteMode::DynamicPartitionOverwrite),
            ConnectorWriteIntent::PartitionOverwrite
        );
    }

    #[test]
    fn append_source_to_query_values_reorders_columns_and_fills_defaults() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, Some(ColumnDefault::Int32(5))),
            test_column("c", DataType::Int32, None),
        ];
        let source = rows_query(vec![vec![
            build::number("30").unwrap(),
            build::number("10").unwrap(),
        ]]);

        let query = append_source_to_query_for_write(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
            &target_columns,
        )
        .expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let row = values.rows.first().expect("one row");
        let rendered: Vec<String> = row.iter().map(printer::print_expr).collect();
        assert_eq!(rendered, vec!["10", "5", "30"]);
    }

    #[test]
    fn append_source_to_query_values_preserves_original_literals_for_typed_assignment() {
        let target_columns = vec![
            test_column("id", DataType::Int64, None),
            test_column("region", DataType::Utf8, None),
            test_column("amount", DataType::Float64, None),
        ];
        let source = rows_query(vec![
            vec![
                build::number("1").unwrap(),
                build::string("us"),
                build::number("10.5").unwrap(),
            ],
            vec![
                build::number("2").unwrap(),
                build::string("eu"),
                build::number("20").unwrap(),
            ],
        ]);

        let query =
            append_source_to_query_for_write(&source, &[], &target_columns, &target_columns)
                .expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let first_row: Vec<String> = values.rows[0].iter().map(printer::print_expr).collect();
        let second_row: Vec<String> = values.rows[1].iter().map(printer::print_expr).collect();
        assert_eq!(first_row, vec!["1", "'us'", "10.5"]);
        assert_eq!(second_row, vec!["2", "'eu'", "20"]);
    }

    #[test]
    fn append_source_to_query_values_does_not_position_fill_added_middle_column() {
        let source_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("amount", DataType::Int32, None),
        ];
        let write_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("category", DataType::Utf8, None),
            test_column("amount", DataType::Int32, None),
        ];
        let source = rows_query(vec![vec![
            build::number("1").unwrap(),
            build::number("10").unwrap(),
        ]]);

        let query = append_source_to_query_for_write(&source, &[], &source_columns, &write_columns)
            .expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let row: Vec<String> = values.rows[0].iter().map(printer::print_expr).collect();
        assert_eq!(row, vec!["1", "NULL", "10"]);
    }

    #[test]
    fn spi5b_write_projection_excludes_execution_only_read_fields() {
        let source_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("value", DataType::Utf8, None),
            test_column("_file", DataType::Utf8, None),
            test_column("_pos", DataType::Int64, None),
        ];
        let write_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("value", DataType::Utf8, None),
        ];

        let source = sql_write_source_columns(&source_columns, &write_columns);
        assert_eq!(
            source
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"]
        );
    }

    #[test]
    fn append_source_to_query_values_preserves_backslash_string_literals() {
        let target_columns = vec![test_column("region", DataType::Utf8, None)];
        let source = rows_query(vec![vec![build::string(r"e\f")]]);

        let query =
            append_source_to_query_for_write(&source, &[], &target_columns, &target_columns)
                .expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let ast::Expr::Literal(value) = &values.rows[0][0] else {
            panic!("original source string literal");
        };
        let ast::LiteralKind::String(s) = &value.kind else {
            panic!("expected single-quoted string");
        };
        assert_eq!(s, r"e\f");
    }

    #[test]
    fn append_source_to_query_values_retains_binary_target_source_text_until_typed_conversion() {
        let target_columns = vec![test_column("payload", DataType::Binary, None)];
        let source = rows_query(vec![vec![build::string(
            novarocks_sql::literal::bytes_to_latin1_string(&[0xab, 0x01]),
        )]]);

        let query =
            append_source_to_query_for_write(&source, &[], &target_columns, &target_columns)
                .expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let ast::Expr::Literal(value) = &values.rows[0][0] else {
            panic!("original literal");
        };
        let ast::LiteralKind::String(s) = &value.kind else {
            panic!("original string source");
        };
        assert_eq!(
            novarocks_sql::literal::latin1_string_to_bytes(s).unwrap(),
            [0xab, 0x01]
        );
    }

    #[test]
    fn append_source_to_query_values_rejects_column_list_width_mismatch() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, None),
        ];
        let source = rows_query(vec![vec![
            build::number("1").unwrap(),
            build::number("2").unwrap(),
        ]]);

        let err = append_source_to_query_for_write(
            &source,
            &["a".to_string()],
            &target_columns,
            &target_columns,
        )
        .expect_err("extra value must be rejected");
        assert!(
            err.contains("expected 1 values for column list, got 2"),
            "got: {err}"
        );
    }

    #[test]
    fn append_source_to_query_from_query_column_list_wraps_projection() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, Some(ColumnDefault::Int32(7))),
            test_column("c", DataType::Int32, None),
        ];
        let source = parse_query("SELECT x, y FROM src");

        let query = append_source_to_query_for_write(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
            &target_columns,
        )
        .expect("append source query");

        use ast::SyntaxEq;
        let expected = parse_query(
            "SELECT `__nr_insert_src`.`a` AS `a`, 7 AS `b`, `__nr_insert_src`.`c` AS `c` FROM (SELECT x, y FROM src) AS `__nr_insert_src`(`c`, `a`)",
        );
        assert!(
            query.syntax_eq(&expected),
            "unexpected INSERT target projection: {query:#?}"
        );
        let ast::SetExpr::Select(outer) = query.body.as_ref() else {
            panic!("outer SELECT");
        };
        let Some(ast::Relation::Aliased(alias)) = &outer.from else {
            panic!("source alias");
        };
        let ast::Relation::TableSubquery(node) = alias.relation.as_ref() else {
            panic!("derived source");
        };
        let subquery = &node.query;
        assert_eq!(
            subquery.as_ref(),
            &source,
            "the original source and its positions are retained"
        );
        let ast::SetExpr::Select(original) = source.body.as_ref() else {
            panic!("source SELECT");
        };
        let ast::SelectItem::ExprWithAlias { expr, .. } = &outer.projection[0] else {
            panic!("target source reference");
        };
        assert_eq!(expr.span(), original.projection[1].span());
    }

    #[test]
    fn append_source_to_query_from_query_omitted_complex_columns_parse() {
        let target_columns = vec![
            test_column("k1", DataType::Int64, None),
            test_column(
                "c_map",
                test_map_type(DataType::Int32, DataType::Int32),
                None,
            ),
            test_column(
                "c_struct",
                test_struct_type(vec![("k1", DataType::Int32), ("k2", DataType::Int32)]),
                None,
            ),
        ];
        let source = parse_query("SELECT idx FROM row_util ORDER BY idx LIMIT 1000");

        let query = append_source_to_query_for_write(
            &source,
            &["k1".to_string()],
            &target_columns,
            &target_columns,
        )
        .expect("append source query");
        let ast::SetExpr::Select(outer) = query.body.as_ref() else {
            panic!("outer SELECT");
        };
        for item in &outer.projection[1..] {
            let ast::SelectItem::ExprWithAlias { expr, .. } = item else {
                panic!("target source expression");
            };
            assert!(
                matches!(
                    expr,
                    ast::Expr::Literal(ast::Literal {
                        kind: ast::LiteralKind::Null,
                        ..
                    })
                ),
                "omitted complex NULL waits for full typed target context"
            );
        }
    }
    #[test]
    fn write_projection_keeps_singleton_struct_default_and_quoted_source_identity() {
        let target_columns = vec![
            test_column("id", DataType::Int64, None),
            test_column(
                "s",
                test_struct_type(vec![("only", DataType::Int64)]),
                Some(ColumnDefault::Struct(vec![(
                    "only".into(),
                    ColumnDefault::Int64(7),
                )])),
            ),
        ];
        let source = parse_query("SELECT idx FROM row_util");
        let query = wrap_insert_query_with_write_projection(
            &source,
            &["id".into()],
            &target_columns,
            &target_columns,
        )
        .unwrap();
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("outer SELECT");
        };
        let ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::Dereference(reference),
            alias,
            ..
        } = &select.projection[0]
        else {
            panic!("source column reference");
        };
        assert!(alias.quoted);
        assert!(reference.field.as_ref().unwrap().quoted);
        assert!(
            matches!(&reference.base, ast::Expr::Identifier(qualifier) if qualifier.quoted && qualifier.value == "__nr_insert_src")
        );
        assert_eq!(reference.span, insert_source_expression_span(&source, 0));
        let ast::SelectItem::ExprWithAlias { expr, alias, .. } = &select.projection[1] else {
            panic!("default projection");
        };
        assert!(alias.quoted);
        assert!(matches!(expr, ast::Expr::Row(row) if row.fields.len() == 1));
        assert_eq!(
            novarocks_sql::literal::expr_to_literal(expr).unwrap(),
            novarocks_sql::semantic::Literal::Struct(vec![novarocks_sql::semantic::Literal::Int(
                7
            )])
        );
        let Some(ast::Relation::Aliased(alias)) = &select.from else {
            panic!("source alias");
        };
        let ast::Relation::TableSubquery(node) = alias.relation.as_ref() else {
            panic!("source query");
        };
        let subquery = &node.query;
        assert_eq!(subquery.as_ref(), &source);
    }
}

#[cfg(test)]
mod provider_admission_before_shaping_tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorWrite,
        ConnectorWriteControl, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
        ProviderBindingEpoch,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Control {
        key: ConnectorProviderBindingKey,
        calls: AtomicUsize,
        failure: Option<ConnectorErrorKind>,
    }
    impl ConnectorWriteControl for Control {
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            &self.key
        }
        fn prepare_write(
            &self,
            request: ConnectorWritePreparationRequest,
        ) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let ConnectorWriteInputRequest::Data { fields } = request.input else {
                panic!("data admission")
            };
            assert_eq!(fields.len(), 1);
            assert_eq!(
                fields[0].execution_type().unwrap().logical_type(),
                novarocks_type_contract::ValueLogicalType::Uuid
            );
            if let Some(kind) = self.failure {
                return Err(ConnectorError::new(kind, "original provider refusal"));
            }
            Ok(ConnectorWritePreparationOutcome::Denied(
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "Iceberg uuid writes are not supported",
                ),
            ))
        }
    }
    fn run(failure: Option<ConnectorErrorKind>) -> (String, Arc<Control>) {
        let value = novarocks_type_contract::FunctionValueType::try_with_logical_type(
            arrow::datatypes::DataType::FixedSizeBinary(16),
            true,
            novarocks_type_contract::ValueLogicalType::Uuid,
        )
        .unwrap();
        let metadata =
            super::tests::write_metadata(vec![value.try_to_field("key").unwrap()], vec![value]);
        run_with_metadata(metadata, failure)
    }

    fn run_with_metadata(
        metadata: novarocks_spi::connector::ConnectorTableMetadata,
        failure: Option<ConnectorErrorKind>,
    ) -> (String, Arc<Control>) {
        let columns = insert_columns_from_connector_metadata(&metadata, &HashMap::new()).unwrap();
        // A real shape error remains independent from the UUID's SQL type
        // spelling: one target field cannot consume two source cells.
        // Provider admission must retain precedence over this actual failure.
        let source = super::tests::parse_query("VALUES (NULL, NULL)");
        assert!(
            append_source_to_query_for_write(&source, &[], &columns, &columns)
                .unwrap_err()
                .contains("insert column count mismatch: expected 1 values for column list, got 2")
        );
        let resolved = ResolvedTable { columns };
        let control = Arc::new(Control {
            key: ConnectorProviderBindingKey {
                instance_id: metadata.table.owner().clone(),
                incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
            },
            calls: AtomicUsize::new(0),
            failure,
        });
        let lease = ConnectorWrite::new(control.key.clone(), control.clone(), || {}).unwrap();
        let stop = novarocks_spi::connector::ConnectorStopOwner::default();
        let context = novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            stop.view(),
            64 * 1024,
            1024 * 1024,
        )
        .unwrap();
        let policy = novarocks_functions::ConstantPolicy {
            max_rows: 100,
            max_array_nodes: 1000,
            max_logical_elements: 1000,
            max_retained_buffer_bytes: 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 1000,
            max_dictionary_depth: 4,
            max_metadata_bytes: 64 * 1024,
            max_library_validation_work: 10000,
            max_library_validation_bytes: 1024 * 1024,
        };
        let error = build_iceberg_write_plan(
            &lease,
            "main",
            ConnectorWriteIntent::Append,
            &context,
            &resolved,
            &[],
            &source,
            &metadata,
            policy,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        );
        let error = match error {
            Err(error) => error,
            Ok(_) => panic!("provider refusal must precede source shaping"),
        };
        (error, control)
    }
    #[test]
    fn provider_uuid_denial_precedes_source_shape_validation_without_starting_a_session() {
        let (error, control) = run(None);
        assert!(error.contains("Iceberg uuid writes are not supported"));
        assert!(!error.contains("UUID has no SQL DDL type spelling"));
        assert_eq!(control.calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn provider_resource_and_control_refusals_are_not_replaced_by_sql_shaping() {
        for kind in [
            ConnectorErrorKind::ResourceExhausted,
            ConnectorErrorKind::Cancelled,
            ConnectorErrorKind::DeadlineExceeded,
        ] {
            let (error, control) = run(Some(kind));
            assert_eq!(
                error,
                ConnectorError::new(kind, "original provider refusal").to_string()
            );
            assert_eq!(control.calls.load(Ordering::SeqCst), 1);
        }
    }

    struct AsyncAdmissionMetadata {
        owner: novarocks_spi::connector::ConnectorInstanceId,
        release: tokio::sync::Notify,
        observed: AtomicUsize,
        created: AtomicUsize,
        value: novarocks_type_contract::FunctionValueType,
    }
    impl novarocks_spi::connector::ConnectorMetadata for AsyncAdmissionMetadata {
        fn instance_id(&self) -> &novarocks_spi::connector::ConnectorInstanceId {
            &self.owner
        }
        fn namespace_exists(
            &self,
            _: novarocks_spi::connector::ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("write target load does not probe a namespace")
        }
        fn table_exists(
            &self,
            _: novarocks_spi::connector::ConnectorTableRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("write target load does not probe table existence")
        }
        fn list_tables(
            &self,
            _: novarocks_spi::connector::ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            unreachable!("write target load does not list tables")
        }
        fn load_table(
            &self,
            _: novarocks_spi::connector::ConnectorTableRequest,
            _: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            novarocks_spi::connector::ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            panic!("async INSERT must not call the synchronous joining table load")
        }
        fn prepare_table(
            &self,
            request: novarocks_spi::connector::ConnectorTableRequest,
        ) -> novarocks_spi::connector::ConnectorTablePreparationFuture<'_> {
            Box::pin(async move {
                assert_eq!(request.table.instance_id, self.owner);
                assert_eq!(
                    request.resolution,
                    novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable
                );
                self.observed.fetch_add(1, Ordering::SeqCst);
                self.release.notified().await;
                Ok(novarocks_spi::connector::ConnectorTablePreparation::new(
                    self.owner.clone(),
                    request.table,
                ))
            })
        }
        fn create_prepared_table(
            &self,
            preparation: novarocks_spi::connector::ConnectorTablePreparation,
            creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            novarocks_spi::connector::ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            let identity = preparation
                .into_facts::<novarocks_spi::connector::ConnectorTableIdentity>(&self.owner)?;
            self.created.fetch_add(1, Ordering::SeqCst);
            crate::connector::schema_creation::declared_test_metadata_with_creation(
                novarocks_spi::connector::ConnectorTableMetadataHeader {
                    identity,
                    planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
                    definition_facts:
                        novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
                    version: None,
                    statistics_data_version: None,
                    table: novarocks_spi::connector::ConnectorTableHandle::try_new(
                        self.owner.clone(),
                        bytes::Bytes::from_static(b"target"),
                    )?,
                },
                Arc::new(arrow::datatypes::Schema::new(vec![
                    self.value.try_to_field("key").unwrap(),
                ])),
                vec![self.value.clone()],
                creation,
            )
        }
    }

    #[tokio::test]
    async fn async_target_observation_keeps_provider_admission_before_source_shaping() {
        use futures::FutureExt;
        let provider = AsyncAdmissionMetadata {
            owner: novarocks_spi::connector::ConnectorInstanceId::parse("writer_domain_test")
                .unwrap(),
            release: tokio::sync::Notify::new(),
            observed: AtomicUsize::new(0),
            created: AtomicUsize::new(0),
            value: novarocks_type_contract::FunctionValueType::try_with_logical_type(
                arrow::datatypes::DataType::FixedSizeBinary(16),
                true,
                novarocks_type_contract::ValueLogicalType::Uuid,
            )
            .unwrap(),
        };
        let stop = novarocks_spi::connector::ConnectorStopOwner::default();
        let context = crate::connector::connector_request_context(None, stop.view()).unwrap();
        let request = novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: provider.owner.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("target"),
            },
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context,
        };
        let load = crate::connector::schema_creation::load_frontend_table_async(&provider, request);
        tokio::pin!(load);
        assert!(load.as_mut().now_or_never().is_none());
        assert_eq!(provider.observed.load(Ordering::SeqCst), 1);
        assert_eq!(provider.created.load(Ordering::SeqCst), 0);
        provider.release.notify_one();
        let metadata = load.await.unwrap();
        assert_eq!(provider.created.load(Ordering::SeqCst), 1);
        // The original source has a real width mismatch. The same Provider
        // author must reject the write before that shape validation runs.
        let (error, control) = run_with_metadata(metadata, None);
        assert!(error.contains("Iceberg uuid writes are not supported"));
        assert!(!error.contains("UUID has no SQL DDL type spelling"));
        assert_eq!(control.calls.load(Ordering::SeqCst), 1);
    }
}
