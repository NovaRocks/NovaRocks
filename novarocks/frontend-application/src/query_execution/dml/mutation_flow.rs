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
    let reads = novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new(facts)?;
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
mod tests;

#[path = "mutation_async.rs"]
mod async_preparation;
pub(crate) use async_preparation::{
    PreparedMorMutationWrite, PreparedMutationFacts, RoutedMutationWrite,
    compile_routed_mutation_write, prepare_mutation_facts_async,
};
