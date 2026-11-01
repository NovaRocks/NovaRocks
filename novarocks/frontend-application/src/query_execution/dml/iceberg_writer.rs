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
use novarocks_parser::ast::{Query, Statement};
use novarocks_query_application::admitted_query_context::QueryExecutionContext;
use novarocks_spi::connector::{
    ConnectorWriteAdmissionPurpose, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
    ConnectorWriteIntent,
};
#[cfg(test)]
use novarocks_sql::literal::bytes_to_latin1_string;
use novarocks_sql::literal::latin1_string_to_bytes;
use novarocks_sql::planning::dml::DmlWriteSinkMode;
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;
use novarocks_sql::semantic::Literal;
use novarocks_types::schema::{ColumnDef, ColumnDefault, SqlType};

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
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    begin_receiving: Arc<crate::query_execution::write_session::WriteBeginReceiving>,
) -> Result<PreparedIcebergWrite, String> {
    debug_assert_eq!(target.provider_id.as_str(), "iceberg");
    let context =
        crate::connector::context_for_planning_lease(&planning_lease, connector_context.clone())?;
    let metadata = crate::connector::schema_creation::load_frontend_table_async(
        planning_lease.binding().metadata().as_ref(),
        novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: planning_lease.binding().descriptor().instance_id.clone(),
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
        crate::connector::write_target::ConnectorWriteTargetBinding::new(metadata, planning_lease);
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
    let facts = crate::connector::metadata_read_reference_facts_with_planning_lease_async(
        write_target.lease().clone(),
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
        write_target.lease().clone(),
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
            provider_source,
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
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
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
                let value_type = column
                    .declared_value_type()
                    .map_err(|error| error.to_string())?;
                let field = value_type
                    .try_to_field(&column.name)
                    .map_err(|error| error.to_string())?;
                ConnectorWriteFieldRequest::try_new_typed(field, value_type)
                    .map_err(|error| error.to_string())
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
    let (query, constants) = match crate::dml::insert::shape_insert_source_with_constants(
        source,
        insert_columns,
        &source_columns,
        &write_columns,
        policy,
        control,
    )? {
        Some(shaped) => shaped,
        None => (
            append_source_to_query_for_write(
                source,
                insert_columns,
                &source_columns,
                &write_columns,
            )?,
            None,
        ),
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

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn append_source_to_query(
    source: &Query,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Query, String> {
    append_source_to_query_for_write(source, insert_columns, target_columns, target_columns)
}

fn append_source_to_query_for_write(
    source: &Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Query, String> {
    if let Some(query) = crate::dml::insert::shape_insert_source(
        source,
        insert_columns,
        source_columns,
        write_columns,
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
        let expr = if let Some(source_idx) = insert_idx_by_target.get(&target_name) {
            let source_expr = format!(
                "{}.{}",
                sql_identifier(source_alias),
                sql_identifier(&insert_columns[*source_idx])
            );
            target_cast_expr_sql(&source_expr, column)?
        } else if insert_columns.is_empty() {
            if let Some(source_idx) =
                source_index_for_write_column(column, write_idx, source_columns, write_columns)
            {
                let source_expr = format!(
                    "{}.{}",
                    sql_identifier(source_alias),
                    sql_identifier(&source_columns[source_idx].name)
                );
                target_cast_expr_sql(&source_expr, column)?
            } else {
                target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)?
            }
        } else {
            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)?
        };
        projection.push(format!("{expr} AS {}", sql_identifier(&column.name)));
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
    let alias_columns = alias_source_columns
        .into_iter()
        .map(sql_identifier)
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {} FROM ({}) AS {} ({})",
        projection.join(", "),
        novarocks_parser::printer::print_query(query),
        sql_identifier(source_alias),
        alias_columns
    );
    parse_generated_query(&sql, "append INSERT SELECT projection")
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
/// - `write_target_type` is the provider-signed DML write type for variant and
///   binary columns (ADR-0055 decision 5). The provider only signs it when it
///   differs from the read type, so falling back to the Arrow field type
///   reproduces the previous inline override exactly.
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

fn omitted_column_expr_sql(column: &ColumnDef) -> Result<String, String> {
    Ok(novarocks_parser::printer::print_expr(
        &crate::dml::insert::omitted_insert_expr(column)?,
    ))
}

pub(crate) fn target_cast_expr_sql(expr_sql: &str, column: &ColumnDef) -> Result<String, String> {
    Ok(format!(
        "CAST({expr_sql} AS {})",
        value_type_to_sql_type_name(
            &column
                .declared_value_type()
                .map_err(|error| error.to_string())?,
        )?
    ))
}

/// Render the original complete write declaration, including nominal children.
pub(crate) fn value_type_to_sql_type_name(
    value: &novarocks_type_contract::FunctionValueType,
) -> Result<String, String> {
    sql_type_name(
        &novarocks_types::sql_type_from_value_type(value).map_err(|error| error.to_string())?,
    )
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

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn literal_to_sql(literal: &Literal) -> Result<String, String> {
    Ok(match literal {
        Literal::Null => "NULL".to_string(),
        Literal::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => {
            if !value.is_finite() {
                return Err(format!(
                    "non-finite floating literal is not supported: {value}"
                ));
            }
            value.to_string()
        }
        Literal::String(value) | Literal::Date(value) => single_quoted_sql(value),
        Literal::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        Literal::Map(entries) => {
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(literal_to_sql(key)?);
                args.push(literal_to_sql(value)?);
            }
            format!("map({})", args.join(", "))
        }
        Literal::Struct(values) => format!(
            "row({})",
            values
                .iter()
                .map(literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
    })
}

pub(crate) fn literal_to_sql_for_arrow_type(
    literal: &Literal,
    data_type: &arrow::datatypes::DataType,
) -> Result<String, String> {
    use arrow::datatypes::DataType;

    match (literal, data_type) {
        (
            Literal::String(value) | Literal::Date(value),
            DataType::Binary | DataType::LargeBinary,
        ) => {
            let bytes = latin1_string_to_bytes(value)?;
            Ok(format!("X'{}'", hex::encode_upper(bytes)))
        }
        (Literal::Array(items), DataType::List(item_field)) => {
            let values = items
                .iter()
                .map(|item| literal_to_sql_for_arrow_type(item, item_field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("[{}]", values.join(", ")))
        }
        (Literal::Map(entries), DataType::Map(entries_field, _)) => {
            let DataType::Struct(fields) = entries_field.data_type() else {
                return literal_to_sql(literal);
            };
            if fields.len() != 2 {
                return literal_to_sql(literal);
            }
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(literal_to_sql_for_arrow_type(key, fields[0].data_type())?);
                args.push(literal_to_sql_for_arrow_type(value, fields[1].data_type())?);
            }
            Ok(format!("map({})", args.join(", ")))
        }
        (Literal::Struct(values), DataType::Struct(fields)) if values.len() == fields.len() => {
            let values = values
                .iter()
                .zip(fields.iter())
                .map(|(value, field)| literal_to_sql_for_arrow_type(value, field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("row({})", values.join(", ")))
        }
        _ => literal_to_sql(literal),
    }
}

fn single_quoted_sql(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    for ch in value.chars() {
        match ch {
            '\'' => escaped.push_str("''"),
            '\\' => escaped.push_str(r"\\"),
            _ => escaped.push(ch),
        }
    }
    format!("'{escaped}'")
}

#[cfg(test)]
fn arrow_data_type_to_sql_type(dt: &arrow::datatypes::DataType) -> Result<SqlType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match dt {
        DataType::Boolean => SqlType::Boolean,
        DataType::Int8 => SqlType::TinyInt,
        DataType::Int16 => SqlType::SmallInt,
        DataType::Int32 => SqlType::Int,
        DataType::Int64 => SqlType::BigInt,
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            SqlType::LargeInt
        }
        DataType::Float32 => SqlType::Float,
        DataType::Float64 => SqlType::Double,
        DataType::Decimal128(precision, scale) => SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Utf8 | DataType::LargeUtf8 => SqlType::String,
        DataType::Date32 => SqlType::Date,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,
        DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => SqlType::Time,
        DataType::Binary => SqlType::Binary,
        DataType::LargeBinary => SqlType::Variant,
        DataType::List(element_field) => SqlType::Array(Box::new(arrow_data_type_to_sql_type(
            element_field.data_type(),
        )?)),
        DataType::Map(entries_field, _) => {
            let DataType::Struct(fields) = entries_field.data_type() else {
                return Err(format!("unsupported Arrow map entries type: {dt:?}"));
            };
            if fields.len() != 2 {
                return Err(format!("unsupported Arrow map entries field count: {dt:?}"));
            }
            SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(fields[0].data_type())?),
                Box::new(arrow_data_type_to_sql_type(fields[1].data_type())?),
            )
        }
        DataType::Struct(fields) => SqlType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name().clone(),
                        arrow_data_type_to_sql_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
        other => {
            return Err(format!(
                "unsupported Arrow type for INSERT default conversion: {other:?}"
            ));
        }
    })
}

fn sql_type_name(sql_type: &SqlType) -> Result<String, String> {
    Ok(match sql_type {
        SqlType::TinyInt => "TINYINT".to_string(),
        SqlType::SmallInt => "SMALLINT".to_string(),
        SqlType::Int => "INT".to_string(),
        SqlType::BigInt => "BIGINT".to_string(),
        SqlType::LargeInt => "LARGEINT".to_string(),
        SqlType::Float => "FLOAT".to_string(),
        SqlType::Double => "DOUBLE".to_string(),
        SqlType::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        SqlType::String => "STRING".to_string(),
        SqlType::Json => "JSON".to_string(),
        SqlType::Binary => "VARBINARY".to_string(),
        SqlType::Bitmap => "BITMAP".to_string(),
        SqlType::Hll => "HLL".to_string(),
        SqlType::Object | SqlType::Percentile => {
            return Err(
                "internal opaque value domains have no public Iceberg SQL type name".into(),
            );
        }
        SqlType::Boolean => "BOOLEAN".to_string(),
        SqlType::Date => "DATE".to_string(),
        SqlType::DateTime => "DATETIME".to_string(),
        SqlType::DateTimeNs => "DATETIME_NS".to_string(),
        SqlType::Time => "TIME".to_string(),
        SqlType::Array(inner) => format!("ARRAY<{}>", sql_type_name(inner)?),
        SqlType::Map(key, value) => {
            format!("MAP<{}, {}>", sql_type_name(key)?, sql_type_name(value)?)
        }
        SqlType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|(name, ty)| Ok(format!("{} {}", sql_identifier(name), sql_type_name(ty)?)))
                .collect::<Result<Vec<_>, String>>()?
                .join(", ")
        ),
        SqlType::Variant => "VARIANT".to_string(),
        SqlType::Uuid => return Err("UUID has no SQL DDL type spelling".into()),
    })
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
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
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
        use novarocks_spi::connector::{
            ConnectorRequestContext, ConnectorStopOwner, ConnectorTableColumnPlanningFact,
            ConnectorTableColumnRole, ConnectorTableColumnSemanticKind,
            ConnectorTableColumnVisibility, ConnectorTablePlanningFacts,
        };
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

        let uuid = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .expect("UUID type");
        let mut metadata = write_metadata(
            vec![uuid.try_to_field("v").expect("UUID field")],
            vec![uuid.clone()],
        );
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            ConnectorStopOwner::new().view(),
            1024,
            65536,
        )
        .expect("bounded request");
        let source_schema = Arc::new(metadata.schema().clone());
        metadata.planning_facts = ConnectorTablePlanningFacts::try_new(
            &source_schema,
            vec![
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                )
                .with_write_target_type(Some(DataType::Binary)),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &context,
        )
        .expect("provider write carrier override");
        assert_eq!(metadata.execution_schema().binding(0), Some(&uuid));
        assert_eq!(
            metadata.planning_facts.column_facts()[0].write_target_type(),
            Some(&DataType::Binary)
        );
        let error = insert_columns_from_connector_metadata(&metadata, &HashMap::new())
            .expect_err("a carrier override retains and validates the authored UUID identity");
        assert_eq!(
            error,
            "connector write layout conflicts with its value contract: invalid Arrow carrier for Uuid"
        );
        assert_eq!(metadata.execution_schema().binding(0), Some(&uuid));
        assert_eq!(
            metadata.planning_facts.column_facts()[0].write_target_type(),
            Some(&DataType::Binary)
        );
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
                target_cast_expr_sql("v", &declared).expect("declared cast"),
                format!("CAST(v AS {spelling})")
            );
            assert_eq!(
                target_cast_expr_sql("v", &plain).expect("plain cast"),
                "CAST(v AS VARBINARY)"
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
            target_cast_expr_sql("v", &uuid).expect_err("UUID is not a numeric DDL type"),
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

    fn rows_query(rows: Vec<Vec<Literal>>) -> Query {
        let sql = format!(
            "VALUES {}",
            rows.iter()
                .map(|row| {
                    format!(
                        "({})",
                        row.iter()
                            .map(|value| literal_to_sql(value).unwrap())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
        parse_query(&sql)
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
    fn arrow_data_type_to_sql_type_accepts_time64_for_insert_defaults() {
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::Time64(TimeUnit::Microsecond)).expect("type"),
            novarocks_types::schema::SqlType::Time
        );
    }

    #[test]
    fn append_source_to_query_values_reorders_columns_and_fills_defaults() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, Some(ColumnDefault::Int32(5))),
            test_column("c", DataType::Int32, None),
        ];
        let source = rows_query(vec![vec![Literal::Int(30), Literal::Int(10)]]);

        let query = append_source_to_query(
            &source,
            &["c".to_string(), "a".to_string()],
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
        assert_eq!(
            rendered,
            vec!["CAST(10 AS INT)", "CAST(5 AS INT)", "CAST(30 AS INT)"]
        );
    }

    #[test]
    fn omitted_column_expr_characterizes_neutral_write_defaults() {
        let full_binary = (0_u16..=255).map(|byte| byte as u8).collect::<Vec<_>>();
        let full_binary_sql = format!(
            "X'{}'",
            (0_u16..=255)
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        );
        let cases = vec![
            (
                "integer",
                DataType::Int32,
                Some(ColumnDefault::Int32(5)),
                "5".to_string(),
            ),
            (
                "string",
                DataType::Utf8,
                Some(ColumnDefault::String("value".to_string())),
                "'value'".to_string(),
            ),
            (
                "decimal",
                DataType::Decimal128(10, 2),
                Some(ColumnDefault::Decimal {
                    unscaled: 12_345,
                    precision: 10,
                    scale: 2,
                }),
                "123.45".to_string(),
            ),
            (
                "date",
                DataType::Date32,
                Some(ColumnDefault::Date {
                    days_since_epoch: -1,
                }),
                "'1969-12-31'".to_string(),
            ),
            (
                "datetime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Some(ColumnDefault::TimestampMicros {
                    micros_since_epoch: 1_704_110_400_123_456,
                }),
                "'2024-01-01 12:00:00'".to_string(),
            ),
            (
                "datetime-ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                Some(ColumnDefault::TimestampNanos {
                    nanos_since_epoch: 1_704_164_645_123_456_789,
                }),
                "'2024-01-02 03:04:05.123456789'".to_string(),
            ),
            (
                "binary",
                DataType::Binary,
                Some(ColumnDefault::Binary(full_binary)),
                full_binary_sql,
            ),
            (
                "empty-array",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                Some(ColumnDefault::Array(Vec::new())),
                "[]".to_string(),
            ),
            (
                "empty-map",
                test_map_type(DataType::Int32, DataType::Utf8),
                Some(ColumnDefault::Map(Vec::new())),
                "MAP{}".to_string(),
            ),
            ("missing", DataType::Int32, None, "NULL".to_string()),
        ];

        for (name, data_type, write_default, expected) in cases {
            let column = test_column(name, data_type, write_default);
            assert_eq!(
                omitted_column_expr_sql(&column),
                Ok(expected),
                "case={name}"
            );
        }
    }

    #[test]
    fn fixed_size_binary_largeint_maps_to_largeint_sql_type() {
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::FixedSizeBinary(
                novarocks_types::largeint::LARGEINT_BYTE_WIDTH
            )),
            Ok(SqlType::LargeInt)
        );
    }

    #[test]
    fn omitted_column_expr_preserves_non_empty_collection_defaults() {
        let list_column = test_column(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            Some(ColumnDefault::Array(vec![ColumnDefault::Int32(1)])),
        );
        assert_eq!(omitted_column_expr_sql(&list_column).unwrap(), "[1]");

        let map_column = test_column(
            "attributes",
            test_map_type(DataType::Int32, DataType::Utf8),
            Some(ColumnDefault::Map(vec![(
                ColumnDefault::Int32(1),
                ColumnDefault::String("value".to_string()),
            )])),
        );
        assert_eq!(
            omitted_column_expr_sql(&map_column).unwrap(),
            "MAP{1: 'value'}"
        );
    }

    #[test]
    fn append_source_to_query_values_casts_literals_to_target_types() {
        let target_columns = vec![
            test_column("id", DataType::Int64, None),
            test_column("region", DataType::Utf8, None),
            test_column("amount", DataType::Float64, None),
        ];
        let source = rows_query(vec![
            vec![
                Literal::Int(1),
                Literal::String("us".to_string()),
                Literal::Float(10.5),
            ],
            vec![
                Literal::Int(2),
                Literal::String("eu".to_string()),
                Literal::Float(20.0),
            ],
        ]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let first_row: Vec<String> = values.rows[0].iter().map(printer::print_expr).collect();
        let second_row: Vec<String> = values.rows[1].iter().map(printer::print_expr).collect();
        assert_eq!(
            first_row,
            vec![
                "CAST(1 AS BIGINT)",
                "CAST('us' AS STRING)",
                "CAST(10.5 AS DOUBLE)"
            ]
        );
        assert_eq!(
            second_row,
            vec![
                "CAST(2 AS BIGINT)",
                "CAST('eu' AS STRING)",
                "CAST(20 AS DOUBLE)"
            ]
        );
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
        let source = rows_query(vec![vec![Literal::Int(1), Literal::Int(10)]]);

        let query = append_source_to_query_for_write(&source, &[], &source_columns, &write_columns)
            .expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let row: Vec<String> = values.rows[0].iter().map(printer::print_expr).collect();
        assert_eq!(
            row,
            vec!["CAST(1 AS INT)", "CAST(NULL AS STRING)", "CAST(10 AS INT)"]
        );
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
        let source = rows_query(vec![vec![Literal::String(r"e\f".to_string())]]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let ast::Expr::Cast(cast) = &values.rows[0][0] else {
            panic!("expected CAST expression");
        };
        let ast::Expr::Literal(value) = cast.expr.as_ref() else {
            panic!("expected string literal inside CAST");
        };
        let ast::LiteralKind::String(s) = &value.kind else {
            panic!("expected single-quoted string");
        };
        assert_eq!(s, r"e\f");
    }

    #[test]
    fn append_source_to_query_values_renders_binary_literals_as_hex() {
        let target_columns = vec![test_column("payload", DataType::Binary, None)];
        let packed = bytes_to_latin1_string(&[0xab, 0x01]);
        let source = rows_query(vec![vec![Literal::String(packed)]]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!(
                "expected VALUES query, got: {}",
                printer::print_query(&query)
            );
        };
        let ast::Expr::Cast(cast) = &values.rows[0][0] else {
            panic!("expected CAST expression");
        };
        let ast::Expr::Literal(value) = cast.expr.as_ref() else {
            panic!("expected hex literal inside CAST");
        };
        let ast::LiteralKind::HexString(s) = &value.kind else {
            panic!("expected hex literal");
        };
        assert_eq!(s, "AB01");
    }

    #[test]
    fn target_cast_expr_sql_renders_large_binary_as_variant() {
        let column = ColumnDef::from_value_type(
            "v".into(),
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                novarocks_type_contract::ValueLogicalType::Variant,
            )
            .expect("authored Variant"),
            None,
        )
        .expect("Variant column");

        let sql = target_cast_expr_sql("X'AB01'", &column).expect("cast sql");

        assert_eq!(sql, "CAST(X'AB01' AS VARIANT)");
    }

    #[test]
    fn append_source_to_query_values_rejects_column_list_width_mismatch() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, None),
        ];
        let source = rows_query(vec![vec![Literal::Int(1), Literal::Int(2)]]);

        let err = append_source_to_query(&source, &["a".to_string()], &target_columns)
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

        let query = append_source_to_query(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
        )
        .expect("append source query");

        let rendered = printer::print_query(&query);
        assert!(
            rendered.contains("FROM (SELECT x, y FROM src) AS `__nr_insert_src`(`c`, `a`)"),
            "derived query should carry source column aliases, got: {rendered}"
        );
        assert!(
            rendered.starts_with(
                "SELECT CAST(`__nr_insert_src`.`a` AS INT) AS `a`, CAST(7 AS INT) AS `b`, CAST(`__nr_insert_src`.`c` AS INT) AS `c`"
            ),
            "projection should target table column order, got: {rendered}"
        );
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

        let query = append_source_to_query(&source, &["k1".to_string()], &target_columns)
            .expect("append source query");
        let rendered = printer::print_query(&query);

        assert!(
            rendered.contains("CAST(NULL AS MAP"),
            "omitted map column should be cast from NULL once, got: {rendered}"
        );
        assert!(
            rendered.contains("CAST(NULL AS STRUCT"),
            "omitted struct column should be cast from NULL once, got: {rendered}"
        );
        assert!(
            !rendered.contains("CAST(CAST(NULL"),
            "omitted complex columns must not produce nested casts, got: {rendered}"
        );
    }
}

#[cfg(test)]
mod provider_admission_before_shaping_tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorWriteControl,
        ConnectorWriteLease, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
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
        // This source cannot be shaped without inventing a SQL UUID spelling.
        let source = super::tests::parse_query("SELECT NULL");
        assert!(
            append_source_to_query_for_write(&source, &[], &columns, &columns)
                .unwrap_err()
                .contains("UUID has no SQL DDL type spelling")
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
        let lease = ConnectorWriteLease::new(control.key.clone(), control.clone(), || {}).unwrap();
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
    fn provider_uuid_denial_precedes_sql_cast_rendering_without_starting_a_session() {
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
        // The actual original SELECT source still cannot be rendered as UUID;
        // the same provider author must reject it before that SQL projection.
        let (error, control) = run_with_metadata(metadata, None);
        assert!(error.contains("Iceberg uuid writes are not supported"));
        assert!(!error.contains("UUID has no SQL DDL type spelling"));
        assert_eq!(control.calls.load(Ordering::SeqCst), 1);
    }
}
