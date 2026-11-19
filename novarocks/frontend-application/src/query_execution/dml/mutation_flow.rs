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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use arrow::datatypes::DataType;

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::planning::write_sink::{
    admit_session_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use crate::query_execution::write_session::ConnectorWriteSession;
use novarocks_parser::{SYNTHETIC, ast, build};
use novarocks_query_application::admitted_query_context::QueryExecutionContext;
use novarocks_sql::planning::dml::{
    DmlChangeStreamCompileRequest, DmlChangeStreamKind, DmlChangeStreamRoute,
    DmlPreExpandKeyedAssert, DmlWriteSinkMode, dml_change_stream_optimizer_settings,
};
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;
use novarocks_sql::semantic::ObjectName;

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn deletion_vector_input_request() -> novarocks_spi::connector::ConnectorWriteInputRequest {
    use novarocks_spi::connector::{ConnectorWriteFieldRequest, ConnectorWriteInputRequest};

    ConnectorWriteInputRequest::DeletionVector {
        identity_fields: vec![
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
        ],
        // The Iceberg Provider derives frozen partition-source fields from
        // the exact admitted metadata. SQL never reconstructs them.
        partition_source_fields: Vec::new(),
    }
}

/// Logical change-stream branches remain a mutation-kernel decision. SQL owns
/// their physical layout binding and the Iceberg connector owns terminal
/// handles and aggregate report routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DmlRowMutationEffectSet {
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    UpdateMor,
    Merge {
        matched_update: bool,
        matched_delete: bool,
        not_matched_insert: bool,
    },
}

/// Provider-signed row-mutation admission retained before stage.
///
/// It is pure: signing it decides the physical strategy and the base/written
/// versions this statement runs against, and nothing external happens until the
/// statement opens its write session.
#[derive(Clone)]
pub(crate) struct DmlChangeStreamPreparations {
    _write: novarocks_spi::connector::ConnectorWrite,
    preparation: novarocks_spi::connector::ConnectorRowMutationPreparation,
    match_observation: Option<novarocks_spi::connector::ConnectorTableMetadata>,
}

impl DmlChangeStreamPreparations {
    fn prepare(
        target: &crate::connector::write_target::ConnectorWriteTargetBinding,
        target_ref: &str,
        effect_set: DmlRowMutationEffectSet,
        context: novarocks_spi::connector::ConnectorRequestContext,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    ) -> Result<Self, crate::dml::error::DmlExecutionError> {
        use novarocks_spi::connector::ConnectorRowMutationIntent;

        let intent = match effect_set {
            DmlRowMutationEffectSet::UpdateMor => ConnectorRowMutationIntent::Update,
            DmlRowMutationEffectSet::Merge { .. } => ConnectorRowMutationIntent::Merge {
                effects: effect_set.effects(),
            },
        };
        let (lease, preparation) =
            target.prepare_row_mutation(target_ref, operation_id, intent, context)?;
        Ok(Self::from_signed(lease, preparation, target.metadata()))
    }

    /// Wrap a preparation this statement already obtained.
    ///
    /// A statement signs exactly one row-mutation preparation. Callers that read
    /// the strategy off it during admission reuse that same value here rather
    /// than asking the provider again, so one statement never carries two base
    /// versions or two digests.
    fn from_signed(
        lease: novarocks_spi::connector::ConnectorWrite,
        preparation: novarocks_spi::connector::ConnectorRowMutationPreparation,
        observation: &novarocks_spi::connector::ConnectorTableMetadata,
    ) -> Self {
        let match_observation = (preparation.strategy()
            == novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead)
            .then(|| observation.clone());
        Self {
            _write: lease,
            preparation,
            match_observation,
        }
    }
}

impl DmlRowMutationEffectSet {
    fn effects(self) -> Vec<novarocks_spi::connector::ConnectorRowMutationEffect> {
        use novarocks_spi::connector::ConnectorRowMutationEffect;

        match self {
            Self::UpdateMor => vec![ConnectorRowMutationEffect::Replace],
            Self::Merge {
                matched_update,
                matched_delete,
                not_matched_insert,
            } => {
                let mut effects = Vec::with_capacity(3);
                if matched_update || matched_delete {
                    effects.push(ConnectorRowMutationEffect::Delete);
                }
                if matched_update {
                    effects.push(ConnectorRowMutationEffect::Replace);
                }
                if not_matched_insert {
                    effects.push(ConnectorRowMutationEffect::Insert);
                }
                effects
            }
        }
    }
}

fn mor_change_stream_input_request(
    target_columns: &[novarocks_types::schema::ColumnDef],
    signed: &novarocks_spi::connector::ConnectorMutationMatchContract,
) -> Result<novarocks_spi::connector::ConnectorWriteInputRequest, String> {
    use novarocks_execution::exec::row_position::{
        ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
        ICEBERG_ROW_POS_COL,
    };
    use novarocks_spi::connector::{ConnectorWriteFieldRequest, ConnectorWriteInputRequest};

    use novarocks_type_contract::FunctionValueType;
    let field =
        |name: &str, value: FunctionValueType| -> Result<ConnectorWriteFieldRequest, String> {
            ConnectorWriteFieldRequest::try_new_typed(
                arrow::datatypes::Field::new(name, value.data_type().clone(), value.nullable()),
                value,
            )
            .map_err(|error| error.to_string())
        };
    if signed.after_fields().len() != target_columns.len() {
        return Err("MOR write input width differs from its original signed target".into());
    }
    let mut data_fields = target_columns
        .iter()
        .zip(signed.after_fields())
        .map(|(column, signed)| {
            let input = signed
                .execution_type()
                .ok_or_else(|| "MOR target lacks its complete write input".to_string())?;
            if column.name != *signed.field().name()
                || column
                    .declared_value_type()
                    .map_err(|error| error.to_string())?
                    != *input
            {
                return Err("MOR write input differs from its original signed target".into());
            }
            let request =
                ConnectorWriteFieldRequest::try_new_typed(signed.field().clone(), input.clone())
                    .map_err(|error| error.to_string())?;
            match signed.storage_type() {
                Some(storage) => request
                    .with_storage_type(storage.clone())
                    .map_err(|error| error.to_string()),
                None => Ok(request),
            }
        })
        .collect::<Result<Vec<_>, String>>()?;
    data_fields.push(field(
        ICEBERG_ROW_ID_COL,
        FunctionValueType::new(DataType::Int64, true),
    )?);
    data_fields.push(field(
        ICEBERG_LAST_UPDATED_SEQ_COL,
        FunctionValueType::new(DataType::Int64, true),
    )?);
    Ok(ConnectorWriteInputRequest::RowLineage {
        data_fields,
        row_identity_fields: vec![
            field(
                ICEBERG_FILE_PATH_COL,
                FunctionValueType::new(DataType::Utf8, false),
            )?,
            field(
                ICEBERG_ROW_POS_COL,
                FunctionValueType::new(DataType::Int64, false),
            )?,
        ],
    })
}

/// One logical write target of a row-mutation session paired with the routing
/// facts the provider signed for it.
pub(crate) type ChangeStreamRoutedTarget<'a> = (
    &'a novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan,
    &'a novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
);

/// The router's branches, in the order their write target ordinals put them.
///
/// Two facts are established here rather than assumed by the caller. A
/// row-mutation session routes every branch, so a branch that arrived without
/// routing facts is a contract violation and fails closed instead of being
/// defaulted into one. And the branch order *is* the write target order, so the
/// branches are sorted by their sealed ordinal rather than taken in whatever
/// order the provider happened to hand them over -- the router gives branch `i`
/// the writer holding ordinal `i`, so a permuted list would silently feed one
/// branch another branch's rows.
fn change_stream_routed_targets(
    write_session: &ConnectorWriteSession,
) -> Result<Vec<ChangeStreamRoutedTarget<'_>>, String> {
    routed_write_targets(write_session.targets())
}

pub(crate) fn routed_write_targets(
    targets: &[novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan],
) -> Result<Vec<ChangeStreamRoutedTarget<'_>>, String> {
    let mut routed = Vec::new();
    for target in targets {
        if let Some(group) = target.position_materialization() {
            routed.extend(group.routes().iter().map(|route| (target, route)));
        } else {
            let route = target.route().ok_or_else(|| {
                format!(
                    "row-mutation write target {} carries no provider routing facts",
                    target.ordinal().get()
                )
            })?;
            routed.push((target, route));
        }
    }
    routed.sort_by_key(|(target, _)| target.ordinal());
    Ok(routed)
}

pub(crate) fn signed_change_stream_route(
    target: &novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
) -> DmlChangeStreamRoute {
    let sink = match target.position_materialization() {
        Some(group) => sink.with_position_materialization(group.clone()),
        None => sink,
    };
    DmlChangeStreamRoute {
        route_id: route.route_id(),
        write_target_ordinal: target.ordinal(),
        accepted_effects: route.accepted_effects().to_vec(),
        input_ordinals: route.input_ordinals().to_vec(),
        requirements: route.requirements().clone(),
        sink,
    }
}

pub(crate) fn bind_change_stream_routes(
    target: &crate::catalog_application::resolver::TargetBackend,
    write_session: &ConnectorWriteSession,
    write_planning_pin: &novarocks_spi::connector::ConnectorControlPlanningPin,
    table_bindings: &QueryTableBindingStore,
) -> Result<
    (
        Vec<DmlChangeStreamRoute>,
        Vec<novarocks_sql::planning::dml::DmlChangeStreamStatisticsTarget>,
    ),
    String,
> {
    use novarocks_spi::connector::ConnectorWriteInputShape;
    // The recipes are sealed before the plan is compiled because the plan's
    // writer nodes carry them: a plan and the session that sealed it must not be
    // separable.
    let _sealed_write_targets = write_session
        .seal_write_targets()
        .map_err(|error| format!("seal row-mutation write targets: {error}"))?;
    let routed_targets = change_stream_routed_targets(write_session)?;
    let mut routes = Vec::with_capacity(routed_targets.len());
    let mut statistics_targets = Vec::with_capacity(routed_targets.len());
    let mut last_sink: Option<(
        novarocks_spi::connector::write_stack::WriteTargetOrdinal,
        novarocks_sql::planning::dml::DmlWritePlanInput,
    )> = None;
    for (write_target, route) in routed_targets {
        if let Some((ordinal, sink)) = &last_sink
            && *ordinal == write_target.ordinal()
        {
            routes.push(signed_change_stream_route(
                write_target,
                route,
                sink.clone(),
            ));
            continue;
        }
        let target_binding = admit_session_connector_write_target(
            table_bindings,
            FrozenConnectorScanIdentity::new(
                target.catalog.clone(),
                target.namespace.clone(),
                target.table.clone(),
            ),
            write_target,
            write_planning_pin.clone(),
        )?;
        let mode = match write_target.input() {
            ConnectorWriteInputShape::Data { .. } => DmlWriteSinkMode::Data,
            ConnectorWriteInputShape::RowLineage { .. } => DmlWriteSinkMode::RowLineageData,
            ConnectorWriteInputShape::PositionDelete { .. } => DmlWriteSinkMode::PositionDeletes,
            ConnectorWriteInputShape::DeletionVector { .. } => DmlWriteSinkMode::DeletionVectors,
            ConnectorWriteInputShape::EqualityDelete { .. } => DmlWriteSinkMode::EqualityDeletes,
        };
        let sink = dml_write_plan_input_for_admitted_target(
            table_bindings,
            target_binding,
            mode,
            novarocks_sql::planning::dml::ConnectorWriteInputBinding::RootOutputByOrdinal,
        )
        .map_err(|error| format!("build row-mutation route sink: {error}"))?;

        routes.push(signed_change_stream_route(
            write_target,
            route,
            sink.clone(),
        ));
        last_sink = Some((write_target.ordinal(), sink));
        statistics_targets.push(
            novarocks_sql::planning::dml::DmlChangeStreamStatisticsTarget {
                write_target_ordinal: write_target.ordinal(),
                requirements: write_session
                    .statistics_requirements(write_target.ordinal())
                    .map_err(|error| error.to_string())?
                    .to_vec(),
            },
        );
    }
    Ok((routes, statistics_targets))
}

fn finish_change_stream_write(
    state: &DmlExecutionKernel,
    completion: novarocks_sql::planning::dml::DmlChangeStreamCompletion,
    facts: Vec<novarocks_sql::compiler::ProviderReadFact>,
    access: novarocks_query_application::preparation::FinalPlanRuntimeAccess<
        crate::query_execution::provider_read_facts::FrozenProviderRead,
    >,
    write_session: &Arc<ConnectorWriteSession>,
    query_options: Option<novarocks_proto_codec::lifecycle::QueryOptions>,
    completion_control: &novarocks_sql::compiler::SqlCompileControl,
    capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
) -> Result<
    crate::query_execution::compiler::PreparedDmlWriteAssembly,
    crate::dml::error::DmlExecutionError,
> {
    let targets = novarocks_sql::planning::dml::DmlFinalizedWriteTargetSet::try_new(
        write_session
            .targets()
            .iter()
            .map(|target| {
                Ok(novarocks_sql::planning::dml::DmlFinalizedWriteTarget {
                    ordinal: target.ordinal(),
                    handle: write_session
                        .encode_writer_handle_payload(target.handle())
                        .map_err(|error| error.to_string())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    )?;
    let reads =
        novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new(facts.into_iter().map(
            |fact| novarocks_sql::planning::dml::DmlFinalizedProviderRead {
                fact,
                read_budget: novarocks_physical_plan::ScanReadBudget {
                    max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
                    max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
                },
            },
        ))?;
    let version = crate::query_execution::physical_encoding::mint_plan_version();
    let dop_domain =
        crate::query_execution::contract::completed_plan_dop_domain(query_options.as_ref())?;
    let finalized = completion.finish(
        novarocks_sql::planning::dml::DmlFinalWritePlanContext::new(
            novarocks_sql::planning::dml::DmlFinalPlanContext::new(version, dop_domain, reads),
            targets,
        ),
        completion_control,
    )?;
    let (plan, _) = finalized.into_parts();
    let candidate =
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate::for_sql_program(
            plan,
            completion_control,
        )
        .and_then(|candidate| {
            candidate.freeze_root_output(
                novarocks_result_contract::FrozenRootOutput::InternalFacts(
                    novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
                ),
            )
        })
        .map_err(crate::dml::error::DmlExecutionError::from_completion)?;
    let paired = novarocks_query_application::preparation::CompletedPlanWithAccess::try_pair(
        candidate, access,
    )
    .map_err(|(error, _returned)| error.to_string())?;
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
        paired,
        state.static_plan_carrier(),
        state.constant_policy(),
        Some(
            &crate::query_execution::physical_encoding::WriteTargetFacts {
                session: write_session.as_ref(),
            },
        ),
        completion_control,
        capacity,
    )?;
    Ok(
        crate::query_execution::compiler::PreparedDmlWriteAssembly::new(
            encoded,
            version,
            query_options,
            Arc::clone(write_session),
        )?,
    )
}

/// Admitted syntax is the sole mutation input.
#[derive(Clone, Debug)]
pub(crate) struct PreparedUpdateStatement {
    pub(crate) table: super::mutation::RowMutationTarget,
    pub(crate) alias: Option<novarocks_parser::ast::Ident>,
    pub(crate) assignments: Vec<PreparedMutationAssignment>,
    pub(crate) source: Option<PreparedMutationSource>,
    pub(crate) selection: Option<novarocks_parser::ast::Expr>,
}
impl PreparedUpdateStatement {}
#[derive(Clone, Debug)]
pub(crate) struct PreparedMergeStatement {
    pub(crate) table: super::mutation::RowMutationTarget,
    pub(crate) target_alias: Option<novarocks_parser::ast::Ident>,
    pub(crate) source: PreparedMutationSource,
    pub(crate) on: novarocks_parser::ast::Expr,
    pub(crate) matched: Option<PreparedMergeClause<PreparedMergeMatchedAction>>,
    pub(crate) not_matched: Option<PreparedMergeClause<PreparedMergeNotMatchedAction>>,
}
impl PreparedMergeStatement {}
#[derive(Clone, Debug)]
pub(crate) struct PreparedMutationAssignment {
    pub(crate) column: String,
    pub(crate) column_span: novarocks_parser::Span,
    pub(crate) value: novarocks_parser::ast::Expr,
}
#[derive(Clone, Debug)]
pub(crate) enum PreparedMutationSource {
    Table {
        name: ObjectName,
        alias: Option<novarocks_parser::ast::Ident>,
    },
    Query {
        query: Box<novarocks_parser::ast::Query>,
        alias: Option<novarocks_parser::ast::Ident>,
    },
}
#[derive(Clone, Debug)]
pub(crate) struct PreparedMergeClause<A> {
    pub(crate) predicate: Option<novarocks_parser::ast::Expr>,
    pub(crate) action: A,
}
#[derive(Clone, Debug)]
pub(crate) enum PreparedMergeMatchedAction {
    Update {
        assignments: Vec<PreparedMutationAssignment>,
    },
    Delete,
}
#[derive(Clone, Debug)]
pub(crate) struct PreparedMergeNotMatchedAction {
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<novarocks_parser::ast::Expr>,
}

pub(crate) struct PreparedUpdateMutation {
    pub(crate) stmt: PreparedUpdateStatement,
    pub(crate) current_catalog: Option<String>,
    pub(crate) target: crate::catalog_application::resolver::TargetBackend,
    pub(crate) target_columns: Vec<novarocks_types::schema::ColumnDef>,
    pub(crate) target_ref: String,
    /// The one exact connector generation admitted with this statement.
    pub(crate) planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    /// The one write lease this statement will use, derived once here.
    ///
    /// `derive_write_lease` mints a fresh fence cell on every call, so deriving
    /// it again inside staging would fence a lease that nothing later commits
    /// through. Deriving once at preparation lets the coordinator establish the
    /// external fence before any writer is dispatched, and staging reuses the
    /// same authority.
    pub(crate) write: novarocks_spi::connector::ConnectorWrite,
    pub(crate) mor_preparations: Option<DmlChangeStreamPreparations>,
    /// The physical route the provider signed for this statement. Kept as the
    /// neutral strategy rather than re-encoded into the provider's own write-mode
    /// enum, so nothing downstream re-decides it.
    pub(crate) mode: novarocks_spi::connector::ConnectorRowMutationStrategy,
    pub(crate) execution: QueryExecutionContext,
    pub(crate) connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

pub(crate) struct PreparedMergeMutation {
    pub(crate) stmt: PreparedMergeStatement,
    pub(crate) current_catalog: Option<String>,
    pub(crate) target: crate::catalog_application::resolver::TargetBackend,
    pub(crate) target_columns: Vec<novarocks_types::schema::ColumnDef>,
    pub(crate) target_ref: String,
    /// See [`PreparedUpdateMutation::mode`].
    pub(crate) table_write_mode: novarocks_spi::connector::ConnectorRowMutationStrategy,
    /// The one exact connector generation admitted with this statement.
    pub(crate) planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    /// The one write lease this statement will use, derived once here.
    ///
    /// `derive_write_lease` mints a fresh fence cell on every call, so deriving
    /// it again inside staging would fence a lease that nothing later commits
    /// through. Deriving once at preparation lets the coordinator establish the
    /// external fence before any writer is dispatched, and staging reuses the
    /// same authority.
    pub(crate) write: novarocks_spi::connector::ConnectorWrite,
    pub(crate) mor_preparations: Option<DmlChangeStreamPreparations>,
    pub(crate) insert_columns_resolved: Option<MergeInsertColumns>,
    pub(crate) execution: QueryExecutionContext,
    pub(crate) connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

pub(super) fn resolve_update_target(
    state: &DmlExecutionKernel,
    stmt: &PreparedUpdateStatement,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<
    (crate::catalog_application::resolver::TargetBackend, String),
    crate::catalog_application::resolver::TargetResolutionError,
> {
    let (table_name, target_ref) = stmt
        .table
        .table_and_reference()
        .map_err(crate::catalog_application::resolver::TargetResolutionError::from)?;
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.provider_id.as_str() != "iceberg" {
        return Err((format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.provider_id.as_str()
        ))
        .into());
    }

    Ok((target, target_ref.to_owned()))
}

fn finish_update_mutation_preparation(
    stmt: &PreparedUpdateStatement,
    current_catalog: Option<&str>,
    target: crate::catalog_application::resolver::TargetBackend,
    target_ref: String,
    target_binding: crate::connector::write_target::ConnectorWriteTargetBinding,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
) -> Result<PreparedUpdateMutation, crate::dml::error::DmlExecutionError> {
    let planning_pin = target_binding.planning_pin().clone();
    // Validate assignments against the Provider's exact target declaration.
    // Partition columns are ordinary target values: the signed strategy keeps
    // old-file deletion separate from partitioning the replacement after-image.
    // Branch/format and transform admission stay with the Provider.
    // The physical strategy is whatever the provider signs for this table state.
    let strategy_operation_id = operation_id;
    let (strategy_lease, strategy_preparation) = target_binding.prepare_row_mutation(
        &target_ref,
        strategy_operation_id,
        novarocks_spi::connector::ConnectorRowMutationIntent::Update,
        connector_context.clone(),
    )?;
    // UPDATE consumes the Provider's signed merge-on-read route.
    let mode = match strategy_preparation.strategy() {
        strategy @ novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead => strategy,
        other => {
            return Err(
                format!("UPDATE cannot be served by row-mutation strategy {other:?}").into(),
            );
        }
    };
    let target_columns = target_binding.dml_target_columns()?;
    validate_update_assignments(&stmt.assignments, &target_columns)?;
    let signed_preparations = DmlChangeStreamPreparations::from_signed(
        strategy_lease,
        strategy_preparation,
        target_binding.metadata(),
    );
    // Retain the exact preparation that signed the original strategy and base.
    let mor_preparations = Some(signed_preparations);
    let write = planning_pin
        .derive_write_lease()
        .map_err(|error| format!("derive UPDATE write lease: {error}"))?;
    Ok(PreparedUpdateMutation {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        target,
        target_columns,
        target_ref,
        planning_pin,
        write,
        mor_preparations,
        mode,
        execution: execution.clone(),
        connector_context: connector_context.clone(),
    })
}

/// Resolve and validate MERGE without materializing source rows, registering a
/// cohort, or creating a staging artifact. It retains one exact planning pin
/// for every later read or writer admission.
pub(super) fn resolve_merge_target(
    state: &DmlExecutionKernel,
    stmt: &PreparedMergeStatement,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<
    (crate::catalog_application::resolver::TargetBackend, String),
    crate::catalog_application::resolver::TargetResolutionError,
> {
    let (table_name, target_ref) = stmt
        .table
        .table_and_reference()
        .map_err(crate::catalog_application::resolver::TargetResolutionError::from)?;
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.provider_id.as_str() != "iceberg" {
        return Err((format!(
            "MERGE only supports iceberg backends, got `{}`",
            target.provider_id.as_str()
        ))
        .into());
    }
    Ok((target, target_ref.to_owned()))
}

fn finish_merge_mutation_preparation(
    stmt: &PreparedMergeStatement,
    current_catalog: Option<&str>,
    target: crate::catalog_application::resolver::TargetBackend,
    target_ref: String,
    target_binding: crate::connector::write_target::ConnectorWriteTargetBinding,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
) -> Result<PreparedMergeMutation, crate::dml::error::DmlExecutionError> {
    let planning_pin = target_binding.planning_pin().clone();
    // The clause composition is a statement fact. The Provider consumes this
    // exact effect set when admitting the merge-on-read mutation.
    let effect_set = DmlRowMutationEffectSet::Merge {
        matched_update: matches!(
            stmt.matched.as_ref().map(|clause| &clause.action),
            Some(PreparedMergeMatchedAction::Update { .. })
        ),
        matched_delete: matches!(
            stmt.matched.as_ref().map(|clause| &clause.action),
            Some(PreparedMergeMatchedAction::Delete)
        ),
        not_matched_insert: stmt.not_matched.is_some(),
    };
    let preparations = DmlChangeStreamPreparations::prepare(
        &target_binding,
        &target_ref,
        effect_set,
        connector_context.clone(),
        operation_id,
    )?;
    // MERGE consumes the same signed merge-on-read route as UPDATE.
    let table_write_mode = match preparations.preparation.strategy() {
        strategy @ novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead => strategy,
        other => {
            return Err(
                format!("MERGE cannot be served by row-mutation strategy {other:?}").into(),
            );
        }
    };
    let target_columns = target_binding.dml_target_columns()?;
    if let Some(clause) = stmt.matched.as_ref()
        && let PreparedMergeMatchedAction::Update { assignments } = &clause.action
    {
        validate_update_assignments(assignments, &target_columns)?;
    }
    let insert_columns_resolved = stmt
        .not_matched
        .as_ref()
        .map(|clause| resolve_merge_insert_columns(&clause.action, &target_columns))
        .transpose()?;
    let mor_preparations = Some(preparations);
    let write = planning_pin
        .derive_write_lease()
        .map_err(|error| format!("derive MERGE write lease: {error}"))?;
    Ok(PreparedMergeMutation {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        target,
        target_columns,
        target_ref,
        table_write_mode,
        planning_pin,
        write,
        mor_preparations,
        insert_columns_resolved,
        execution: execution.clone(),
        connector_context: connector_context.clone(),
    })
}

fn generated_name(parts: &[&str], quoted: bool) -> Result<ast::ObjectName, String> {
    build::object_name(
        parts
            .iter()
            .map(|part| build::ident(*part, quoted))
            .collect(),
    )
    .map_err(|error| error.to_string())
}

fn mutation_target_relation(
    target: &crate::catalog_application::resolver::TargetBackend,
    alias: ast::Ident,
    target_ref: &str,
) -> Result<ast::Relation, String> {
    let version = (target_ref != "main").then(|| ast::QueryPeriod {
        range_type: ast::QueryPeriodRangeType::Version,
        start: None,
        end: Some(ast::AstBox::new(build::string(target_ref))),
        span: SYNTHETIC,
    });
    Ok(build::alias(
        build::table(
            generated_name(&[&target.catalog, &target.namespace, &target.table], true)?,
            version,
            None,
        ),
        alias,
        None,
    ))
}

fn mutation_source_table_name(
    resolved: &novarocks_types::naming::TableIdentity,
    target: &crate::catalog_application::resolver::TargetBackend,
) -> Result<ast::ObjectName, String> {
    let parts = if resolved.catalog != target.catalog {
        vec![
            resolved.catalog.as_str(),
            resolved.namespace.as_str(),
            resolved.table.as_str(),
        ]
    } else if resolved.namespace != target.namespace {
        vec![resolved.namespace.as_str(), resolved.table.as_str()]
    } else {
        vec![resolved.table.as_str()]
    };
    generated_name(&parts, false)
}

fn mutation_source_relation(
    source: &PreparedMutationSource,
    current_catalog: Option<&str>,
    target: &crate::catalog_application::resolver::TargetBackend,
) -> Result<ast::Relation, String> {
    match source {
        PreparedMutationSource::Table { name, alias } => {
            let resolved = novarocks_types::naming::resolve_catalog_table_name(
                &name.parts,
                current_catalog,
                &target.namespace,
            )?;
            let relation = build::table(mutation_source_table_name(&resolved, target)?, None, None);
            Ok(match alias {
                Some(alias) => build::alias(
                    relation,
                    build::ident_at(&alias.value, false, alias.span),
                    None,
                ),
                None => relation,
            })
        }
        PreparedMutationSource::Query { query, alias } => {
            let alias = alias
                .as_ref()
                .ok_or_else(|| "MERGE/UPDATE subquery source requires an alias".to_owned())?;
            Ok(build::alias(
                build::derived(query.as_ref().clone()),
                build::ident_at(&alias.value, false, alias.span),
                None,
            ))
        }
    }
}

fn update_assignment_projection(
    assignments: &[PreparedMutationAssignment],
    target_columns: &[novarocks_types::schema::ColumnDef],
) -> Result<Vec<(ast::Ident, ast::Expr)>, String> {
    assignments
        .iter()
        .map(|assignment| {
            let column = target_columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&assignment.column))
                .ok_or_else(|| {
                    format!(
                        "UPDATE assignment references unknown target column `{}`",
                        assignment.column
                    )
                })?;
            Ok((
                build::ident_at(&column.name, false, assignment.column_span),
                assignment.value.clone(),
            ))
        })
        .collect()
}

fn build_update_match_query(
    target: ast::Relation,
    target_alias: ast::Ident,
    source: Option<ast::Relation>,
    assignments: Vec<(ast::Ident, ast::Expr)>,
    selection: Option<ast::Expr>,
) -> Result<ast::Query, String> {
    let mut projection = [
        ("_file", "__nr_file"),
        ("_pos", "__nr_pos"),
        ("_row_id", "__nr_row_id"),
        (
            "_last_updated_sequence_number",
            "__nr_last_updated_sequence_number",
        ),
        ("_spec_id", "__nr_spec_id"),
        ("_partition", "__nr_partition"),
    ]
    .into_iter()
    .map(|(name, alias)| {
        build::item(
            build::column(target_alias.clone(), build::ident(name, false)),
            Some(build::ident(alias, false)),
        )
    })
    .collect::<Vec<_>>();
    projection
        .push(build::qualified_wildcard(vec![target_alias]).map_err(|error| error.to_string())?);
    projection.extend(assignments.into_iter().map(|(column, expr)| {
        build::item(
            expr,
            Some(build::ident_at(
                format!("__nr_new_{}", column.value),
                false,
                column.span,
            )),
        )
    }));
    let mut from = target;
    if let Some(source) = source {
        from = build::join(from, novarocks_parser::ast::JoinType::Cross, source, None);
    }
    let select = build::select(projection, Some(from), selection, None)
        .map_err(|error| error.to_string())?;
    Ok(build::query(ast::SetExpr::Select(
        novarocks_parser::ast::AstBox::new(select),
    )))
}

fn build_update_mor_producer(
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedUpdateStatement,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_types::schema::ColumnDef],
    target_ref: &str,
) -> Result<
    (
        ast::Query,
        DmlChangeStreamKind,
        Option<DmlPreExpandKeyedAssert>,
    ),
    String,
> {
    let alias = stmt
        .alias
        .clone()
        .unwrap_or_else(|| build::ident("__nr_t", false));
    let alias = build::ident_at(alias.value, false, alias.span);
    let query = build_update_match_query(
        mutation_target_relation(target, alias.clone(), target_ref)?,
        alias,
        stmt.source
            .as_ref()
            .map(|source| mutation_source_relation(source, current_catalog, target))
            .transpose()?,
        update_assignment_projection(&stmt.assignments, target_columns)?,
        stmt.selection.clone(),
    )?;
    Ok((
        query,
        DmlChangeStreamKind::Update {
            file_metadata: Some([4, 5]),
            target_columns: target_columns.to_vec(),
        },
        Some(DmlPreExpandKeyedAssert {
            declared_violation: Some(
                novarocks_failure_contract::sql_reason::UPDATE_TARGET_ROW_MULTIPLE_MATCHES,
            ),
            applicability_output_ordinal: None,
            key_columns: vec![
                ("__nr_file".into(), "_file".into()),
                ("__nr_pos".into(), "_pos".into()),
            ],
            message_prefix: "MOR UPDATE matched target row".into(),
        }),
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "The MERGE query author preserves each admitted clause and frozen target declaration."
)]
fn build_merge_match_query(
    target: ast::Relation,
    target_alias: ast::Ident,
    source: ast::Relation,
    on: ast::Expr,
    matched_predicate: Option<ast::Expr>,
    not_matched_predicate: Option<ast::Expr>,
    target_columns: &[novarocks_types::schema::ColumnDef],
    matched_assignments: Vec<(ast::Ident, ast::Expr)>,
    insert_values: Vec<(ast::Ident, ast::Expr)>,
    matched_action: Option<i32>,
    has_not_matched_insert: bool,
) -> Result<ast::Query, String> {
    let qualify = |name: &str| build::column(target_alias.clone(), build::ident(name, true));
    let nullable = |name: &str| {
        build::case_when(
            vec![(build::is_null(qualify("_file"), true), qualify(name))],
            Some(build::null()),
        )
        .map_err(|error| error.to_string())
    };
    let mut action_cases = Vec::new();
    if let Some(action) = matched_action {
        action_cases.push((
            build::and_all(vec![
                build::is_null(qualify("_file"), true),
                matched_predicate.unwrap_or_else(|| build::boolean(true)),
            ])
            .expect("MERGE matched action has two predicates"),
            build::integer(i64::from(action)),
        ));
    }
    if has_not_matched_insert {
        action_cases.push((
            build::and_all(vec![
                build::is_null(qualify("_file"), false),
                not_matched_predicate.unwrap_or_else(|| build::boolean(true)),
            ])
            .expect("MERGE not-matched action has two predicates"),
            build::integer(i64::from(MERGE_ACTION_NOT_MATCHED_INSERT)),
        ));
    }
    let action = if action_cases.is_empty() {
        build::integer(0)
    } else {
        build::case_when(action_cases, Some(build::integer(0)))
            .map_err(|error| error.to_string())?
    };
    let mut projection = Vec::new();
    for (name, alias) in [
        ("_file", "__nr_file"),
        ("_pos", "__nr_pos"),
        ("_row_id", "__nr_row_id"),
        (
            "_last_updated_sequence_number",
            "__nr_last_updated_sequence_number",
        ),
        ("_spec_id", "__nr_spec_id"),
        ("_partition", "__nr_partition"),
    ] {
        projection.push(build::item(
            nullable(name)?,
            Some(build::ident(alias, false)),
        ));
    }
    projection.push(build::item(
        action,
        Some(build::ident("__nr_merge_action", false)),
    ));
    for column in target_columns {
        projection.push(build::item(
            nullable(&column.name)?,
            Some(build::ident(&column.name, true)),
        ));
    }
    for (prefix, values) in [
        ("__nr_new_", matched_assignments),
        ("__nr_ins_", insert_values),
    ] {
        projection.extend(values.into_iter().map(|(column, expr)| {
            build::item(
                expr,
                Some(build::ident_at(
                    format!("{prefix}{}", column.value),
                    false,
                    column.span,
                )),
            )
        }));
    }
    let from = build::join(source, novarocks_parser::ast::JoinType::Left, target, {
        let expression = on;
        Some(novarocks_parser::ast::JoinCriteria::On(
            novarocks_parser::ast::JoinOn {
                span: expression.span(),
                expression,
            },
        ))
    });
    let select =
        build::select(projection, Some(from), None, None).map_err(|error| error.to_string())?;
    Ok(build::query(ast::SetExpr::Select(
        novarocks_parser::ast::AstBox::new(select),
    )))
}

fn build_merge_mor_producer(
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedMergeStatement,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_types::schema::ColumnDef],
    insert_columns: Option<&[MergeInsertColumn]>,
    target_ref: &str,
) -> Result<
    (
        ast::Query,
        DmlChangeStreamKind,
        Option<DmlPreExpandKeyedAssert>,
    ),
    String,
> {
    let alias = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| build::ident(MERGE_TARGET_DEFAULT_ALIAS, false));
    let alias = build::ident_at(alias.value, false, alias.span);
    let mut source = mutation_source_relation(&stmt.source, current_catalog, target)?;
    if !matches!(&source, ast::Relation::Aliased(_)) {
        source = build::alias(
            source,
            build::ident(MERGE_SOURCE_DEFAULT_ALIAS, false),
            None,
        );
    }
    let matched_assignments = match stmt.matched.as_ref().map(|clause| &clause.action) {
        Some(PreparedMergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|assignment| {
                let column = target_columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&assignment.column))
                    .ok_or_else(|| {
                        format!(
                            "MERGE UPDATE assignment references unknown target column `{}`",
                            assignment.column
                        )
                    })?;
                Ok((
                    build::ident_at(&column.name, false, assignment.column_span),
                    assignment.value.clone(),
                ))
            })
            .collect::<Result<Vec<_>, String>>()?,
        _ => Vec::new(),
    };
    let mut insert_values = Vec::new();
    if let (Some(columns), Some(clause)) = (insert_columns, stmt.not_matched.as_ref()) {
        for column in columns {
            if let Some(index) = column.value_index {
                let expr = clause.action.values[index].clone();
                insert_values.push((build::ident(&column.name, false), expr));
            }
        }
    }
    let action = stmt.matched.as_ref().map(|clause| match clause.action {
        PreparedMergeMatchedAction::Update { .. } => MERGE_ACTION_MATCHED_UPDATE,
        PreparedMergeMatchedAction::Delete => MERGE_ACTION_MATCHED_DELETE,
    });
    let query = build_merge_match_query(
        mutation_target_relation(target, alias.clone(), target_ref)?,
        alias,
        source,
        stmt.on.clone(),
        stmt.matched
            .as_ref()
            .and_then(|clause| clause.predicate.clone()),
        stmt.not_matched
            .as_ref()
            .and_then(|clause| clause.predicate.clone()),
        target_columns,
        matched_assignments,
        insert_values,
        action,
        stmt.not_matched.is_some(),
    )?;
    Ok((
        query,
        DmlChangeStreamKind::Merge {
            file_metadata: Some([4, 5]),
            target_columns: target_columns.to_vec(),
            matched_update: action == Some(MERGE_ACTION_MATCHED_UPDATE),
            matched_delete: action == Some(MERGE_ACTION_MATCHED_DELETE),
            not_matched_insert: stmt.not_matched.is_some(),
        },
        Some(mor_merge_pre_expand_assertion()),
    ))
}

fn validate_update_assignments(
    assignments: &[PreparedMutationAssignment],
    target_columns: &[novarocks_types::schema::ColumnDef],
) -> Result<(), String> {
    let target_names = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        let name = assignment.column.to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "UPDATE cannot assign reserved Iceberg metadata column `{}`",
                assignment.column
            ));
        }
        if !target_names.contains(&name) {
            return Err(format!(
                "UPDATE assignment references unknown target column `{}`",
                assignment.column
            ));
        }
        if !seen.insert(name) {
            return Err(format!(
                "UPDATE assignment lists target column `{}` more than once",
                assignment.column
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// MERGE INTO
// ---------------------------------------------------------------------------

const MERGE_TARGET_DEFAULT_ALIAS: &str = "__nr_t";
const MERGE_SOURCE_DEFAULT_ALIAS: &str = "__nr_s";
const MERGE_ACTION_MATCHED_UPDATE: i32 = 1;
const MERGE_ACTION_MATCHED_DELETE: i32 = 2;
const MERGE_ACTION_NOT_MATCHED_INSERT: i32 = 3;

pub(crate) struct MergeInsertColumns {
    columns: Vec<MergeInsertColumn>,
}

pub(crate) struct MergeInsertColumn {
    name: String,
    /// `Some(idx)` when the user supplied a value for this target column at
    /// position `idx` in the `VALUES` tuple. `None` means "no value
    /// supplied"; we project a NULL of the column's type instead.
    value_index: Option<usize>,
}

impl std::ops::Deref for MergeInsertColumns {
    type Target = [MergeInsertColumn];
    fn deref(&self) -> &[MergeInsertColumn] {
        &self.columns
    }
}

fn resolve_merge_insert_columns(
    action: &PreparedMergeNotMatchedAction,
    target_columns: &[novarocks_types::schema::ColumnDef],
) -> Result<MergeInsertColumns, String> {
    let target_names_lower: Vec<String> = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect();

    // Empty `INSERT VALUES (...)` (no column list) means "values match target
    // schema in declaration order". Iceberg row-lineage columns (`_row_id`
    // etc.) are reserved/owned and never appear in the user-visible target
    // schema returned from `iceberg_table_columns`, so we don't have to
    // filter them here.
    if action.columns.is_empty() {
        if action.values.len() != target_columns.len() {
            return Err(format!(
                "MERGE WHEN NOT MATCHED INSERT VALUES count {} does not match target column count {}",
                action.values.len(),
                target_columns.len()
            ));
        }
        let columns = target_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| MergeInsertColumn {
                name: col.name.clone(),
                value_index: Some(idx),
            })
            .collect();
        return Ok(MergeInsertColumns { columns });
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut by_target: HashMap<String, usize> = HashMap::new();
    for (idx, raw_name) in action.columns.iter().enumerate() {
        let lower = raw_name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "MERGE INSERT cannot assign reserved Iceberg metadata column `{raw_name}`"
            ));
        }
        if !target_names_lower.contains(&lower) {
            return Err(format!(
                "MERGE INSERT references unknown target column `{raw_name}`"
            ));
        }
        if !seen.insert(lower.clone()) {
            return Err(format!(
                "MERGE INSERT lists target column `{raw_name}` more than once"
            ));
        }
        by_target.insert(lower, idx);
    }

    let columns = target_columns
        .iter()
        .map(|col| MergeInsertColumn {
            name: col.name.clone(),
            value_index: by_target.get(&col.name.to_ascii_lowercase()).copied(),
        })
        .collect();
    Ok(MergeInsertColumns { columns })
}

/// Every matched target row participates, including rows for which no WHEN
/// clause applies. Unmatched INSERT rows keep their actual NULL target locator.
// Design: ADR-0219 (docs/adr/ADR-0219-iceberg-v3-lineage-and-planned-duplicate-match-errors.md)
fn mor_merge_pre_expand_assertion() -> DmlPreExpandKeyedAssert {
    DmlPreExpandKeyedAssert {
        declared_violation: Some(
            novarocks_failure_contract::sql_reason::MERGE_TARGET_ROW_MULTIPLE_MATCHES,
        ),
        applicability_output_ordinal: None,
        key_columns: vec![
            ("__nr_file".into(), "_file".into()),
            ("__nr_pos".into(), "_pos".into()),
            // With one supported WHEN MATCHED clause, action identifies the
            // selected clause; zero is the group for no selected clause.
            ("__nr_merge_action".into(), "__nr_merge_action".into()),
        ],
        message_prefix: "MOR MERGE matched target row".to_string(),
    }
}

#[cfg(test)]
mod tests {
    mod selected_action {
        use super::*;
        include!("merge_selected_action_tests.rs");
    }
    use super::*;
    mod merge_router_completion {
        use super::*;
        include!("merge_router_completion_tests.rs");
    }

    use arrow::datatypes::DataType;
    use novarocks_types::schema::ColumnDef;

    fn test_query(source: &str) -> ast::Query {
        novarocks_parser::parser::parse_query(source).unwrap()
    }
    fn test_expr(source: &str) -> ast::Expr {
        let query = test_query(&format!("SELECT {source}"));
        let ast::SetExpr::Select(mut select) = query.body.into_inner() else {
            panic!("select fixture");
        };
        let ast::SelectItem::SingleColumn(column) = select.projection.remove(0) else {
            panic!("expression fixture");
        };
        assert!(column.alias.is_none());
        column.into_inner().expression
    }
    fn prepared_update(source: &str) -> PreparedUpdateStatement {
        let ast::TrinoStatement::Update(statement) =
            novarocks_parser::parser::parse_statement(source).unwrap()
        else {
            panic!("update fixture");
        };
        let super::super::mutation::OwnedMutationStatement::Update(prepared) =
            crate::dml::mutation::prepare_trino_update_shape(statement, source).unwrap()
        else {
            panic!("update fixture");
        };
        *prepared
    }
    fn prepared_merge(source: &str) -> PreparedMergeStatement {
        let ast::TrinoStatement::Merge(statement) =
            novarocks_parser::parser::parse_statement(source).unwrap()
        else {
            panic!("merge fixture");
        };
        let super::super::mutation::OwnedMutationStatement::Merge(prepared) =
            crate::dml::mutation::prepare_trino_merge_shape(statement, source).unwrap()
        else {
            panic!("merge fixture");
        };
        *prepared
    }
    fn prepared_assignment(column: &str, value: &str) -> PreparedMutationAssignment {
        PreparedMutationAssignment {
            column: column.into(),
            column_span: SYNTHETIC,
            value: test_expr(value),
        }
    }
    fn test_relation(source: &str) -> ast::Relation {
        let query = test_query(&format!("SELECT * FROM {source}"));
        let ast::SetExpr::Select(mut select) = query.body.into_inner() else {
            panic!("select fixture");
        };
        select.from.take().expect("FROM relation")
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "The compiler witness supplies independent literal relations for every production MERGE clause."
    )]
    fn merge_query_fixture(
        target: &str,
        target_alias: &str,
        source: &str,
        on: &str,
        matched_predicate: Option<&str>,
        insert_predicate: Option<&str>,
        columns: &[ColumnDef],
        assignments: &[(&str, &str)],
        inserts: &[(&str, &str)],
        matched_action: Option<i32>,
        insert: bool,
    ) -> ast::Query {
        build_merge_match_query(
            test_relation(target),
            build::ident(target_alias, false),
            test_relation(source),
            test_expr(on),
            matched_predicate.map(test_expr),
            insert_predicate.map(test_expr),
            columns,
            assignments
                .iter()
                .map(|(name, value)| (build::ident(*name, false), test_expr(value)))
                .collect(),
            inserts
                .iter()
                .map(|(name, value)| (build::ident(*name, false), test_expr(value)))
                .collect(),
            matched_action,
            insert,
        )
        .unwrap()
    }
    fn connector_context_for_test() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            64 * 1024,
            1024 * 1024,
        )
        .expect("connector request context")
    }

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    fn non_null_col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_target() -> crate::catalog_application::resolver::TargetBackend {
        crate::catalog_application::resolver::TargetBackend {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static Iceberg provider ID"),
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "t".to_string(),
        }
    }

    #[test]
    fn mor_source_names_retain_cross_catalog_identity() {
        let target = iceberg_target();
        for (catalog, namespace, expected) in [
            ("ice", "db1", vec!["s"]),
            ("ice", "db2", vec!["db2", "s"]),
            ("other", "db1", vec!["other", "db1", "s"]),
        ] {
            let source = novarocks_types::naming::TableIdentity::new(catalog, namespace, "s");
            let name = mutation_source_table_name(&source, &target).unwrap();
            assert_eq!(
                name.parts
                    .iter()
                    .map(|part| part.value.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn mor_source_table_and_internal_names_keep_admitted_identity() {
        let merge = prepared_merge(
            "MERGE INTO ice.db1.t AS t\nUSING other.db2.source AS s\nON t.id = s.id\nWHEN MATCHED THEN UPDATE SET \"odd name\" = 7",
        );
        let source =
            mutation_source_relation(&merge.source, Some("ice"), &iceberg_target()).unwrap();
        let ast::Relation::Aliased(source) = source else {
            panic!("source alias");
        };
        let ast::Relation::Table(table) = source.relation.as_ref() else {
            panic!("source table");
        };
        assert_eq!(
            table
                .name
                .parts
                .iter()
                .map(|part| part.value.as_str())
                .collect::<Vec<_>>(),
            ["other", "db2", "source"]
        );
        let prepared = prepared_update("UPDATE ice.db1.t SET \"odd name\" = 7 WHERE id = 1");
        let columns = [col("id"), col("odd name")];
        let query = build_update_match_query(
            mutation_target_relation(&iceberg_target(), build::ident("t", false), "main").unwrap(),
            build::ident("t", false),
            None,
            update_assignment_projection(&prepared.assignments, &columns).unwrap(),
            prepared.selection.clone(),
        )
        .unwrap();
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT");
        };
        let ast::SelectItem::SingleColumn(single_column) = select.projection.last().unwrap() else {
            panic!("assignment alias");
        };
        let Some(alias) = &single_column.alias else {
            panic!("assignment alias");
        };

        assert_eq!(alias.value, "__nr_new_odd name");
        assert_eq!(alias.span, prepared.assignments[0].column_span);
    }

    #[test]
    fn mor_target_branch_literal_keeps_backslashes_without_text_escaping() {
        let relation =
            mutation_target_relation(&iceberg_target(), build::ident("t", false), "dev\\branch")
                .unwrap();
        let ast::Relation::Aliased(alias) = &relation else {
            panic!("target alias");
        };
        let ast::Relation::Table(table) = alias.relation.as_ref() else {
            panic!("target table");
        };
        let Some(version) = &table.query_period else {
            panic!("versioned target");
        };
        assert_eq!(version.end.as_deref(), Some(&build::string("dev\\branch")));
    }

    #[test]
    fn mor_lowering_preserves_admitted_ast_without_reading_source_spans() {
        use ast::SyntaxEq;
        let columns = [col("id"), col("v")];
        let source = "UPDATE ice.db1.t SET v = 99 WHERE id = 1";
        let original = prepared_update(source);
        let ast::TrinoStatement::Update(mut update) =
            novarocks_parser::parser::parse_statement(source).unwrap()
        else {
            panic!("UPDATE");
        };
        let ast::Expr::Literal(value) = &mut update.assignments[0].value else {
            panic!("literal");
        };
        value.span = novarocks_parser::Span::new(source.len() + 1, source.len() + 2);
        let super::super::mutation::OwnedMutationStatement::Update(lowered) =
            crate::dml::mutation::prepare_trino_update_shape(update, source).unwrap()
        else {
            panic!("UPDATE");
        };
        let (expected, _, _) =
            build_update_mor_producer(&iceberg_target(), &original, Some("ice"), &columns, "main")
                .unwrap();
        let (actual, _, _) =
            build_update_mor_producer(&iceberg_target(), &lowered, Some("ice"), &columns, "main")
                .unwrap();
        assert!(actual.syntax_eq(&expected));

        let source = "MERGE INTO ice.db1.t t USING (SELECT 1 AS id, 3 AS v) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = 99";
        let original = prepared_merge(source);
        let ast::TrinoStatement::Merge(mut merge) =
            novarocks_parser::parser::parse_statement(source).unwrap()
        else {
            panic!("MERGE");
        };
        let ast::TrinoMergeCase::Update { assignments, .. } = &mut merge.cases[0] else {
            panic!("UPDATE clause");
        };
        let ast::Expr::Literal(value) = &mut assignments[0].value else {
            panic!("literal");
        };
        value.span = novarocks_parser::Span::new(source.len() + 1, source.len() + 2);
        let super::super::mutation::OwnedMutationStatement::Merge(lowered) =
            crate::dml::mutation::prepare_trino_merge_shape(merge, source).unwrap()
        else {
            panic!("MERGE");
        };
        let (expected, _, _) = build_merge_mor_producer(
            &iceberg_target(),
            &original,
            Some("ice"),
            &columns,
            None,
            "main",
        )
        .unwrap();
        let (actual, _, _) = build_merge_mor_producer(
            &iceberg_target(),
            &lowered,
            Some("ice"),
            &columns,
            None,
            "main",
        )
        .unwrap();
        assert!(actual.syntax_eq(&expected));
    }

    fn analyze_mutation_query(
        query: ast::Query,
    ) -> Result<(), novarocks_sql::compiler::SqlCompileError> {
        use novarocks_sql::compiler::{
            SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler,
            SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
        };
        let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
        let snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
        let assignment = match query.body.as_ref() {
            ast::SetExpr::Select(select) => {
                select
                    .projection
                    .iter()
                    .enumerate()
                    .find_map(|(ordinal, item)| match item {
                        ast::SelectItem::SingleColumn(column)
                            if column
                                .alias
                                .as_ref()
                                .is_some_and(|alias| alias.value == "__nr_new_v") =>
                        {
                            Some((ordinal, column.expression.span()))
                        }
                        _ => None,
                    })
            }
            _ => None,
        };
        let request = SqlAnalyzeRequest::new(
            SqlStatementInput::parsed_query(Box::new(query)),
            SqlCompileIntent::ChangeStreamWrite,
            SqlSessionContext {
                current_catalog: None,
                current_database: "default_database".into(),
                optimizer_settings: dml_change_stream_optimizer_settings(),
                sql_semantics: Default::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &snapshot,
            &functions,
            crate::query_execution::constant_eval::constant_evaluator(),
            None,
            crate::application::test_constant_policy(),
            SqlCompileControl::unbounded(),
        );
        let request = match assignment {
            Some((ordinal, span)) => {
                let control = SqlCompileControl::unbounded();
                let site = novarocks_sql::compiler::SqlWriteAssignmentSite::try_new(
                    ordinal,
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
                    None,
                    &control,
                )?
                .with_source_span(span);
                request.with_write_assignments(Arc::new(
                    novarocks_sql::compiler::SqlWriteAssignments::try_new(
                        vec![site].into_boxed_slice(),
                        &control,
                    )?,
                ))?
            }
            None => request,
        };
        SqlCompiler::analyze(request).map(|_| ())
    }

    #[test]
    fn mor_generated_errors_point_to_user_expression_and_typed_assignment() {
        // The source expression is independently admitted. Its ordinary
        // assignment conversion belongs to the complete-query typed projection.
        analyze_mutation_query(test_query("SELECT hll_hash('x')")).unwrap();
        for (value, code) in [
            ("s.missing", "sql.analyze.unknown_column"),
            ("hll_hash('x')", "sql.analyze.type_mismatch"),
        ] {
            let source = format!(
                "MERGE INTO ice.db1.t AS t\nUSING (SELECT 1 AS id) AS s ON t.id = s.id\nWHEN MATCHED THEN UPDATE SET v = {value}"
            );
            let prepared = prepared_merge(&source);
            let (mut query, _, _) = build_merge_mor_producer(
                &iceberg_target(),
                &prepared,
                Some("ice"),
                &[col("id"), col("v")],
                None,
                "main",
            )
            .unwrap();
            let ast::SetExpr::Select(select) = query.body.as_mut() else {
                panic!("SELECT");
            };
            // Supply exactly the external target's facts through a local
            // relation, retaining the production-generated expression tree.
            let Some(ast::Relation::Join(join)) = select.from.as_mut() else {
                panic!("MERGE join");
            };
            *join.right = test_relation(
                "(SELECT 1 AS id, 2 AS v, 'f' AS _file, CAST(0 AS BIGINT) AS _pos, CAST(1 AS BIGINT) AS _row_id, CAST(1 AS BIGINT) AS _last_updated_sequence_number, CAST(0 AS INTEGER) AS _spec_id, CAST(NULL AS ROW(bucket INTEGER)) AS _partition) AS t",
            );
            let error = analyze_mutation_query(query).unwrap_err();
            let error = crate::dml::error::DmlExecutionError::from_compile(error)
                .into_dml_error(Some(&source));
            let user = error.user_error().expect("typed SQL analysis error");
            assert_eq!(user.code().as_str(), code, "{user}");
            let location = user.location().expect("original expression location");
            let expected = novarocks_parser::Span::new(
                source.find(value).unwrap(),
                source.find(value).unwrap() + value.len(),
            )
            .to_user_error_location(&source);
            assert_eq!(
                (location.line(), location.column()),
                (expected.line(), expected.column()),
                "{user}"
            );
            assert_eq!(location.line(), 3);
        }
    }

    #[test]
    fn reject_reserved_update_columns() {
        let err = validate_update_assignments(
            &[prepared_assignment("_row_id", "1")],
            &[col("id"), col("v")],
        )
        .expect_err("must reject");
        assert!(err.contains("reserved Iceberg metadata column"), "{err}");
    }

    #[test]
    fn replacement_assignment_uses_target_conversion_and_original_old_identity() {
        use ast::SyntaxEq;
        let columns = [col("id"), col("p")];
        let assignments = [prepared_assignment("P", "NULL")];
        validate_update_assignments(&assignments, &columns).unwrap();
        let projected = update_assignment_projection(&assignments, &columns).unwrap();
        assert_eq!(projected[0].0.value, "p");
        assert!(projected[0].1.syntax_eq(&test_expr("(NULL)")));
        let query = build_update_match_query(
            test_relation("ice.db.t AS t"),
            build::ident("t", false),
            None,
            projected,
            Some(test_expr("t.id = 1")),
        )
        .unwrap();
        assert!(query.syntax_eq(&test_query("SELECT t._file AS __nr_file, t._pos AS __nr_pos, t._row_id AS __nr_row_id, t._last_updated_sequence_number AS __nr_last_updated_sequence_number, t._spec_id AS __nr_spec_id, t._partition AS __nr_partition, t.*, (NULL) AS __nr_new_p FROM ice.db.t AS t WHERE t.id = 1")));
        for assignments in [
            vec![prepared_assignment("missing", "1")],
            vec![prepared_assignment("p", "1"), prepared_assignment("P", "2")],
        ] {
            assert!(validate_update_assignments(&assignments, &columns).is_err());
        }
    }

    #[test]
    fn update_match_query_projects_identity_columns() {
        use ast::SyntaxEq;
        let query = build_update_match_query(
            test_relation("ice.db1.t AS t"),
            build::ident("t", false),
            Some(test_relation("staging.s AS s")),
            vec![(build::ident("v", false), test_expr("s.v"))],
            Some(test_expr("t.id = s.id")),
        )
        .unwrap();
        assert!(query.syntax_eq(&test_query("SELECT t._file AS __nr_file, t._pos AS __nr_pos, t._row_id AS __nr_row_id, t._last_updated_sequence_number AS __nr_last_updated_sequence_number, t._spec_id AS __nr_spec_id, t._partition AS __nr_partition, t.*, s.v AS __nr_new_v FROM ice.db1.t AS t CROSS JOIN staging.s AS s WHERE t.id = s.id")));
    }

    #[test]
    fn update_change_stream_target_pins_branch_read_snapshot() {
        use ast::SyntaxEq;
        let target =
            mutation_target_relation(&iceberg_target(), build::ident("t", false), "dev").unwrap();
        assert!(target.syntax_eq(&test_relation(
            r#""ice"."db1"."t" FOR VERSION AS OF 'dev' AS t"#
        )));
    }

    #[test]
    fn update_assignment_projection_casts_to_target_type() {
        use ast::SyntaxEq;
        let projected = update_assignment_projection(
            &[prepared_assignment("v", "src_v")],
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Int32),
            ],
        )
        .expect("assignment projection");
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].0.value, "v");
        assert!(projected[0].1.syntax_eq(&test_expr("(src_v)")));
    }

    #[test]
    fn update_change_stream_match_query_uses_casted_assignment_projection() {
        use ast::SyntaxEq;
        let projected = update_assignment_projection(
            &[prepared_assignment("v", "src_v")],
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Int32),
            ],
        )
        .expect("assignment projection");
        let query = build_update_match_query(
            mutation_target_relation(&iceberg_target(), build::ident("t", false), "main").unwrap(),
            build::ident("t", false),
            Some(test_relation("staging.s AS s")),
            projected,
            Some(test_expr("t.id = s.id")),
        )
        .unwrap();
        assert!(query.syntax_eq(&test_query("SELECT t._file AS __nr_file, t._pos AS __nr_pos, t._row_id AS __nr_row_id, t._last_updated_sequence_number AS __nr_last_updated_sequence_number, t._spec_id AS __nr_spec_id, t._partition AS __nr_partition, t.*, (src_v) AS __nr_new_v FROM \"ice\".\"db1\".\"t\" AS t CROSS JOIN staging.s AS s WHERE t.id = s.id")));
    }

    fn typed_col(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    #[test]
    fn merge_actions_keep_clause_predicates_nested_in_original_order() {
        let matched = test_expr("s.v > 5 OR (s.v = 5 AND s.id > 0)");
        let inserted = test_expr("s.id > 10 AND (s.v > 0 OR s.v IS NULL)");
        let query = build_merge_match_query(
            test_relation("ice.db1.t AS t"),
            build::ident("t", false),
            test_relation("staging.s AS s"),
            test_expr("t.id = s.id"),
            Some(matched.clone()),
            Some(inserted.clone()),
            &[col("id"), col("v")],
            Vec::new(),
            Vec::new(),
            Some(MERGE_ACTION_MATCHED_UPDATE),
            true,
        )
        .unwrap();
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT");
        };
        let action = select
            .projection
            .iter()
            .find_map(|item| match item {
                ast::SelectItem::SingleColumn(column)
                    if column
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias.value == "__nr_merge_action") =>
                {
                    Some(&column.expression)
                }
                _ => None,
            })
            .unwrap();
        let ast::Expr::SearchedCase(case) = action else {
            panic!("ordered action CASE");
        };
        assert_eq!(case.when_clauses.len(), 2);
        assert!(matches!(
            &case.default_value,
            Some(ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Long {
                    parsed_value: 0,
                    ..
                },
                ..
            }))
        ));
        for (index, (original, guard, result)) in [
            (&matched, true, MERGE_ACTION_MATCHED_UPDATE),
            (&inserted, false, MERGE_ACTION_NOT_MATCHED_INSERT),
        ]
        .into_iter()
        .enumerate()
        {
            let branch = &case.when_clauses[index];
            let ast::WhenMatch::Operand(condition) = &branch.match_value else {
                panic!("searched CASE condition must be an operand");
            };
            let ast::Expr::Logical(condition) = condition else {
                panic!("action AND");
            };
            assert_eq!(condition.operator, ast::LogicalOperator::And);
            assert_eq!(condition.terms.len(), 2);
            let ast::Expr::Predicated(predicate) = &condition.terms[0] else {
                panic!("action null guard");
            };
            assert!(
                matches!(&predicate.predicate, ast::Predicate::IsNull(null) if null.negated == guard)
            );
            assert!(
                ast::identifier_chain_observed(&predicate.value, |_| Ok::<
                    (),
                    std::convert::Infallible,
                >(()))
                .unwrap_or_else(|never| match never {})
                .is_some_and(|chain| chain.last().value == "_file")
            );
            assert_eq!(&condition.terms[1], original);
            assert_eq!(condition.terms[1].span(), original.span());
            assert!(
                matches!(&branch.result, ast::Expr::Literal(ast::Literal { kind: ast::LiteralKind::Long { parsed_value, .. }, .. }) if *parsed_value == i64::from(result))
            );
        }
    }

    #[test]
    fn merge_match_query_projects_nullable_target_columns() {
        use ast::SyntaxEq;
        let query = merge_query_fixture(
            "ice.db1.t AS t",
            "t",
            "staging.s AS s",
            "t.id = s.id",
            None,
            None,
            &[col("id"), col("v")],
            &[("v", "s.v")],
            &[("id", "s.id"), ("v", "s.v")],
            Some(MERGE_ACTION_MATCHED_UPDATE),
            true,
        );
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT");
        };
        assert!(
            !select
                .projection
                .iter()
                .any(|item| matches!(item, ast::SelectItem::AllColumns(_)))
        );
        for (alias, expected) in [
            (
                "id",
                r#"CASE WHEN t."_file" IS NOT NULL THEN t."id" ELSE NULL END"#,
            ),
            (
                "__nr_row_id",
                r#"CASE WHEN t."_file" IS NOT NULL THEN t."_row_id" ELSE NULL END"#,
            ),
            ("__nr_new_v", "(s.v)"),
            ("__nr_ins_id", "(s.id)"),
        ] {
            let expr = select
                .projection
                .iter()
                .find_map(|item| match item {
                    ast::SelectItem::SingleColumn(column)
                        if column
                            .alias
                            .as_ref()
                            .is_some_and(|name| name.value == alias) =>
                    {
                        Some(&column.expression)
                    }
                    _ => None,
                })
                .expect("projected alias");
            assert!(expr.syntax_eq(&test_expr(expected)), "{alias}");
        }
        assert!(select.projection.iter().any(|item| matches!(item, ast::SelectItem::SingleColumn(column) if column.alias.as_ref().is_some_and(|alias| alias.value == "__nr_merge_action"))));
    }

    // ---- the merge-on-read write-session data plane ----------------------
    //
    // The session under test is scripted rather than provider-backed: what these
    // cases are about is which branch a row reaches, how many times the
    // connector is asked to commit, and whether it is asked at all -- none of
    // which needs a real Iceberg table.

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeRowMutationCommit;
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeRowMutationWriter(u32);
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeRowMutationFragment;

    struct FakeRowMutationProvider {
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        catalog_handle: novarocks_spi::connector::CatalogHandle,
    }

    impl novarocks_spi::connector::write_stack::ProviderWriteRuntime for FakeRowMutationProvider {
        type CommitSession = FakeRowMutationCommit;
        type WriterHandle = FakeRowMutationWriter;
        type CommitFragment = FakeRowMutationFragment;

        fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
            &self.catalog_handle
        }
    }

    fn row_mutation_catalog_handle() -> novarocks_spi::connector::CatalogHandle {
        novarocks_spi::connector::CatalogHandle::new(
            novarocks_spi::connector::ConnectorInstanceId::parse("mor_session_unit")
                .expect("instance id"),
            novarocks_spi::connector::CatalogVersion::from_bytes([9; 32]),
        )
    }

    fn row_mutation_payload(
        category: novarocks_spi::connector::ConnectorCodecCategory,
        payload: impl Into<bytes::Bytes>,
    ) -> novarocks_spi::connector::ConnectorEncodedPayload {
        novarocks_spi::connector::ConnectorEncodedPayload::new(
            novarocks_spi::connector::ConnectorEnvelopeHeader::new(
                novarocks_spi::connector::ConnectorProviderId::parse("fake").expect("provider id"),
                row_mutation_catalog_handle(),
                category,
                novarocks_spi::connector::ConnectorCodecRevision::try_new(1)
                    .expect("codec revision"),
            ),
            payload.into(),
        )
    }

    fn row_mutation_catalog_properties() -> novarocks_spi::connector::CatalogProperties {
        novarocks_spi::connector::CatalogProperties::new(
            row_mutation_catalog_handle(),
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static provider ID"),
            1,
            Vec::new(),
            Vec::new(),
        )
        .expect("test catalog properties")
    }

    fn row_mutation_adapter()
    -> novarocks_spi::connector::write_stack::WriteRuntimeAdapter<FakeRowMutationProvider> {
        let handle = row_mutation_catalog_handle();
        novarocks_spi::connector::write_stack::WriteRuntimeAdapter::new(Arc::new(
            FakeRowMutationProvider {
                descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor {
                    provider_id: novarocks_spi::connector::ConnectorProviderId::parse("fake")
                        .expect("provider id"),
                    instance_id: handle.catalog_name().clone(),
                },
                catalog_handle: handle,
            },
        ))
    }

    #[derive(Default)]
    struct RowMutationSessionCalls {
        finish: usize,
        abort: usize,
    }

    /// A row-mutation control that seals the merge-on-read branch pair.
    ///
    /// The branches are handed back in descending ordinal order on purpose: a
    /// consumer that took a branch's identity from its position in this list
    /// would give every route the other branch's writer.
    struct FakeRowMutationControl {
        adapter:
            novarocks_spi::connector::write_stack::WriteRuntimeAdapter<FakeRowMutationProvider>,
        binding_key: novarocks_spi::connector::ConnectorProviderBindingKey,
        routed: bool,
        calls: Arc<Mutex<RowMutationSessionCalls>>,
        finish_outcome: Mutex<
            Option<
                novarocks_spi::connector::ExternalMutationOutcome<
                    novarocks_spi::connector::ConnectorWriteReceipt,
                >,
            >,
        >,
    }

    fn field_binding(
        key: u8,
        name: &str,
        data_type: DataType,
    ) -> novarocks_spi::connector::ConnectorWriteFieldBinding {
        novarocks_spi::connector::ConnectorWriteFieldBinding::new(
            novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes([key; 32]),
            arrow::datatypes::Field::new(name, data_type, false),
        )
    }

    /// The data branch: after-image values, accepting replacements and inserts.
    const MOR_DATA_ORDINAL: u32 = 0;
    /// The deletion-vector branch: row identity, accepting deletes and the
    /// before-image half of a replacement.
    const MOR_DELETE_ORDINAL: u32 = 1;

    fn mor_branch(
        adapter: &novarocks_spi::connector::write_stack::WriteRuntimeAdapter<
            FakeRowMutationProvider,
        >,
        ordinal: u32,
        routed: bool,
    ) -> Result<
        novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan,
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_spi::connector::ConnectorRowMutationEffect;
        use novarocks_spi::connector::write_stack::{
            ConnectorWriteRouteFacts, ConnectorWriteTargetPlan, WriteTargetOrdinal,
        };

        let (input, effects) = if ordinal == MOR_DATA_ORDINAL {
            (
                novarocks_spi::connector::ConnectorWriteInputShape::Data {
                    fields: vec![field_binding(1, "v", DataType::Int64)],
                },
                vec![
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ],
            )
        } else {
            (
                novarocks_spi::connector::ConnectorWriteInputShape::DeletionVector {
                    identity_fields: vec![
                        field_binding(2, "_file", DataType::Utf8),
                        field_binding(3, "_pos", DataType::Int64),
                    ],
                    partition_source_fields: Vec::new(),
                },
                vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                ],
            )
        };
        let input_ordinals = input
            .fields()
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                novarocks_spi::connector::ConnectorMutationRouteInput::new(
                    field.token(),
                    u32::try_from(index).expect("bounded test input"),
                )
            })
            .collect();
        let plan = ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(ordinal)?,
            adapter.wrap_writer_handle(FakeRowMutationWriter(ordinal)),
            input,
        );
        if !routed {
            return Ok(plan);
        }
        Ok(plan.with_route(ConnectorWriteRouteFacts::try_new(
            novarocks_spi::connector::ConnectorWriteRouteId::from_bytes(
                [u8::try_from(ordinal).expect("bounded ordinal"); 32],
            ),
            effects,
            input_ordinals,
            novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
        )?))
    }

    impl novarocks_spi::connector::write_stack::ConnectorWriteControl for FakeRowMutationControl {
        fn io_profile(
            &self,
        ) -> Result<
            novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile,
            novarocks_spi::connector::ConnectorError,
        > {
            unreachable!("scripted row-mutation fixture performs no asynchronous Provider I/O")
        }
        fn cleanup_responsibility(
            &self,
            _commit: &novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
        ) -> Result<
            novarocks_spi::connector::ConnectorMetadataCleanupResponsibility,
            novarocks_spi::connector::ConnectorError,
        > {
            unreachable!("scripted row-mutation fixture publishes no cleanup responsibility")
        }

        fn begin_write_query(
            &self,
            _: &novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
            _: &novarocks_spi::connector::write_stack::ConnectorWriteQueryIdentity,
            _: &[novarocks_spi::connector::write_stack::WriteTargetOrdinal],
            _: Option<
                std::sync::Arc<
                    dyn novarocks_spi::connector::write_stack::ConnectorWriteProductionProof,
                >,
            >,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            Ok(())
        }

        fn arrive_write_fragments(
            &self,
            _: &novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
            _: &novarocks_spi::connector::write_stack::ConnectorWriteQueryIdentity,
            _: Vec<(
                novarocks_spi::connector::write_stack::WriteTargetOrdinal,
                novarocks_spi::connector::write_stack::ConnectorCommitFragment,
            )>,
            _: &novarocks_spi::connector::ConnectorPayloadRetentionGuard,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            unreachable!("scripted row-mutation fixture receives no commit fragments")
        }

        fn confirm_write_received(
            &self,
            _: &novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
            _: &novarocks_spi::connector::write_stack::ConnectorWriteQueryIdentity,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            Ok(())
        }

        fn begin_write_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move { self.begin_write(request) })
        }

        fn finish_write_async<'a>(
            &'a self,
            _request: novarocks_spi::connector::write_stack::ConnectorWriteFinishRequestRef<'a>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::ExternalMutationOutcome<
                                novarocks_spi::connector::ConnectorWriteReceipt,
                            >,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.calls.lock().expect("recorded calls").finish += 1;
                self.finish_outcome
                    .lock()
                    .expect("scripted outcome")
                    .take()
                    .ok_or_else(|| {
                        novarocks_spi::connector::ConnectorError::new(
                            novarocks_spi::connector::ConnectorErrorKind::Internal,
                            "no scripted commit outcome",
                        )
                    })
            })
        }

        fn finish_write_with_needed_ready_async<'a>(
            &'a self,
            _request: novarocks_spi::connector::write_stack::ConnectorWriteFinishRequestRef<'a>,

            receiving:&'a novarocks_spi::connector::write_stack::ConnectorWriteNeededReadyReceiving,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        >{
            Box::pin(async move {
                let outcome = async {
                    self.calls.lock().expect("recorded calls").finish += 1;
                    self.finish_outcome
                        .lock()
                        .expect("scripted outcome")
                        .take()
                        .ok_or_else(|| {
                            novarocks_spi::connector::ConnectorError::new(
                                novarocks_spi::connector::ConnectorErrorKind::Internal,
                                "no scripted commit outcome",
                            )
                        })
                }
                .await.map(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::from_outcome);
                if let Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. }) = &outcome {
                    receiving.publish(std::sync::Arc::clone(needed))?;
                }
                outcome
            })
        }

        fn abort_write_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest<'a>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::ConnectorWriteAbortOutcome,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move { self.abort_write(request) })
        }

        fn reconcile_write_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteSessionReconcileRequest<
                'a,
            >,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::ExternalMutationOutcome<
                                novarocks_spi::connector::ConnectorWriteReceipt,
                            >,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move { self.reconcile_write(request) })
        }

        fn reconcile_write_with_needed_ready_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteSessionReconcileRequest<
                'a,
            >,

            receiving:&'a novarocks_spi::connector::write_stack::ConnectorWriteNeededReadyReceiving,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        >{
            Box::pin(async move {
                let outcome = async { self.reconcile_write(request) }.await.map(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::from_outcome);
                if let Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. }) = &outcome {
                    receiving.publish(std::sync::Arc::clone(needed))?;
                }
                outcome
            })
        }

        fn binding_key(&self) -> &novarocks_spi::connector::ConnectorProviderBindingKey {
            &self.binding_key
        }

        fn begin_write(
            &self,
            _request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
        ) -> Result<
            novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
            novarocks_spi::connector::ConnectorError,
        > {
            novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan::try_new(
                self.adapter.wrap_commit_session(FakeRowMutationCommit),
                vec![
                    mor_branch(&self.adapter, MOR_DELETE_ORDINAL, self.routed)?,
                    mor_branch(&self.adapter, MOR_DATA_ORDINAL, self.routed)?,
                ],
            )
        }

        fn finish_write(
            &self,
            _request: novarocks_spi::connector::write_stack::ConnectorWriteFinishRequest<'_>,
        ) -> Result<
            novarocks_spi::connector::ExternalMutationOutcome<
                novarocks_spi::connector::ConnectorWriteReceipt,
            >,
            novarocks_spi::connector::ConnectorError,
        > {
            self.calls.lock().expect("recorded calls").finish += 1;
            self.finish_outcome
                .lock()
                .expect("scripted outcome")
                .take()
                .ok_or_else(|| {
                    novarocks_spi::connector::ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::Internal,
                        "no scripted commit outcome",
                    )
                })
        }

        fn abort_write(
            &self,
            _request: novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest<'_>,
        ) -> Result<
            novarocks_spi::connector::ConnectorWriteAbortOutcome,
            novarocks_spi::connector::ConnectorError,
        > {
            self.calls.lock().expect("recorded calls").abort += 1;
            Ok(
                novarocks_spi::connector::ConnectorWriteAbortOutcome::KnownUncommitted {
                    cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
                },
            )
        }

        fn reconcile_write(
            &self,
            _request: novarocks_spi::connector::write_stack::ConnectorWriteSessionReconcileRequest<
                '_,
            >,
        ) -> Result<
            novarocks_spi::connector::ExternalMutationOutcome<
                novarocks_spi::connector::ConnectorWriteReceipt,
            >,
            novarocks_spi::connector::ConnectorError,
        > {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "reconcile is not scripted in this test",
            ))
        }
    }

    struct FakeRowMutationEncoder;

    impl novarocks_spi::connector::ConnectorWriteHandleWireEncoder for FakeRowMutationEncoder {
        fn owner(&self) -> &str {
            "fake"
        }

        fn encode_writer_handle_payload(
            &self,
            _handle: &novarocks_spi::connector::write_stack::ConnectorWriterHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            Ok(row_mutation_payload(
                novarocks_spi::connector::ConnectorCodecCategory::WriteHandle,
                bytes::Bytes::from_static(b"row-mutation-handle"),
            ))
        }
    }

    struct FakeRowMutationDecoder {
        adapter:
            novarocks_spi::connector::write_stack::WriteRuntimeAdapter<FakeRowMutationProvider>,
    }

    impl novarocks_spi::connector::ConnectorWriteFragmentWireDecoder for FakeRowMutationDecoder {
        fn owner(&self) -> &str {
            "fake"
        }

        fn decode_commit_fragment_payload(
            &self,
            _fragment: &novarocks_spi::connector::ConnectorEncodedPayload,
            _author: &novarocks_spi::connector::ConnectorPayloadRetentionGuard,
        ) -> Result<
            novarocks_spi::connector::write_stack::ConnectorCommitFragment,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            Ok(self.adapter.wrap_commit_fragment(FakeRowMutationFragment))
        }
    }

    struct UnusedLegacyWriteControl;

    impl novarocks_spi::connector::ConnectorWriteControl for UnusedLegacyWriteControl {
        fn binding_key(&self) -> &novarocks_spi::connector::ConnectorProviderBindingKey {
            unreachable!("the legacy control is not exercised by a write session")
        }
    }

    struct RowMutationSessionFixture {
        session: Arc<ConnectorWriteSession>,
    }

    fn row_mutation_session_fixture(
        routed: bool,
        outcome: novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
    ) -> RowMutationSessionFixture {
        let adapter = row_mutation_adapter();
        let calls = Arc::new(Mutex::new(RowMutationSessionCalls::default()));
        let control = Arc::new(FakeRowMutationControl {
            adapter: adapter.clone(),
            binding_key: novarocks_spi::connector::ConnectorProviderBindingKey {
                instance_id: row_mutation_catalog_handle().catalog_name().clone(),
                incarnation: novarocks_spi::connector::ProviderBindingEpoch::new(),
            },
            routed,
            calls: Arc::clone(&calls),
            finish_outcome: Mutex::new(Some(outcome)),
        });
        let lease = novarocks_catalog_application::test_support::write_stack_lease(
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            novarocks_spi::connector::ConnectorControlWriteBinding::new(
                Arc::new(UnusedLegacyWriteControl),
                control,
                Arc::new(FakeRowMutationEncoder),
                Arc::new(FakeRowMutationDecoder { adapter }),
            ),
            || {},
        );
        let session = crate::query_execution::write_session::tests::begin_for_test(
            lease,
            row_mutation_catalog_properties(),
            novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
                catalog_session: Default::default(),
                table: Arc::from("db1.t"),
                target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
                intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
                purpose: novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
                input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
                    fields: vec![novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                        arrow::datatypes::Field::new("v", DataType::Int64, true),
                    )],
                },
                base: None,
                flavor:
                    novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::RowMutation,
                context: connector_context_for_test(),
            },
        )
        .expect("begin row-mutation write session");
        RowMutationSessionFixture { session }
    }

    fn known_uncommitted_outcome() -> novarocks_spi::connector::ExternalMutationOutcome<
        novarocks_spi::connector::ConnectorWriteReceipt,
    > {
        novarocks_spi::connector::ExternalMutationOutcome::KnownUncommitted {
            failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                "scripted",
            ),
            cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
        }
    }

    /// Each merge-on-read branch feeds the writer holding its own sealed
    /// ordinal, and the router sees them in ordinal order.
    ///
    /// The scripted provider returns its branches in descending ordinal order,
    /// so a builder that read a branch's identity from its position in the list
    /// would pair the delete route with the data writer and vice versa.
    #[test]
    fn a_row_mutation_session_routes_each_branch_to_its_own_write_target_ordinal() {
        let fixture = row_mutation_session_fixture(true, known_uncommitted_outcome());
        let routed = change_stream_routed_targets(&fixture.session).expect("routed branches");

        assert_eq!(
            routed
                .iter()
                .map(|(write_target, _)| write_target.ordinal().get())
                .collect::<Vec<_>>(),
            vec![MOR_DATA_ORDINAL, MOR_DELETE_ORDINAL],
        );
        for (write_target, route) in &routed {
            // The route the branch carries is the one the provider signed for
            // that exact ordinal, not the one sitting at the same position.
            assert_eq!(
                route.route_id(),
                novarocks_spi::connector::ConnectorWriteRouteId::from_bytes(
                    [u8::try_from(write_target.ordinal().get()).expect("bounded ordinal"); 32]
                ),
            );
        }
        let data_effects = routed[0].1.accepted_effects();
        assert!(
            data_effects.contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Insert)
        );
        let delete_effects = routed[1].1.accepted_effects();
        assert!(
            delete_effects.contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Delete)
        );
    }

    /// A branch that reached the router without routing facts would leave SQL
    /// with rows it has nowhere to send, so it is refused rather than defaulted.
    #[test]
    fn a_row_mutation_branch_without_routing_facts_fails_closed() {
        let fixture = row_mutation_session_fixture(false, known_uncommitted_outcome());
        let error = change_stream_routed_targets(&fixture.session)
            .expect_err("an unrouted branch must fail closed");
        assert!(
            error.contains("carries no provider routing facts"),
            "{error}"
        );
    }
}

#[path = "mutation_async.rs"]
mod async_preparation;
pub(crate) use async_preparation::{
    PreparedMorMutationWrite, PreparedMutationFacts, RoutedMutationWrite,
    compile_routed_mutation_write, prepare_mutation_facts_async,
};
