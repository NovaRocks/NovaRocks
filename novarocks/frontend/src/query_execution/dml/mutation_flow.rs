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
use std::sync::Mutex;

use arrow::array::{Array, Int8Array, StringArray};
#[cfg(test)]
use arrow::array::{ArrayRef, BooleanArray, Int64Array};
#[cfg(test)]
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::planning::write_sink::{
    admit_session_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use crate::query_execution::write_session::ConnectorWriteSession;
use crate::runtime::query_result::QueryResult;
use novarocks_sql::literal::literal_from_batch;
use novarocks_sql::planning::dml::{
    DmlChangeStreamCompileRequest, DmlChangeStreamKind, DmlChangeStreamRoute,
    DmlChangeStreamRouteField, DmlPreExpandKeyedAssert, DmlWriteSinkMode, IcebergRefSuffix,
    dml_change_stream_optimizer_settings, split_ref_suffix,
};
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;
use novarocks_sql::semantic::ObjectName;

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn row_lineage_input_request(
    columns: &[novarocks_types::schema::ColumnDef],
) -> novarocks_spi::connector::ConnectorWriteInputRequest {
    use novarocks_spi::connector::{ConnectorWriteFieldRequest, ConnectorWriteInputRequest};

    ConnectorWriteInputRequest::RowLineage {
        data_fields: columns
            .iter()
            .map(|column| {
                ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(
                    &column.name,
                    column.data_type.clone(),
                    column.nullable,
                ))
            })
            .collect(),
        row_identity_fields: vec![
            ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(
                novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
            )),
            ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(
                novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                DataType::Int64,
                true,
            )),
        ],
    }
}

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

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn data_input_request(
    columns: &[novarocks_types::schema::ColumnDef],
) -> novarocks_spi::connector::ConnectorWriteInputRequest {
    use novarocks_spi::connector::{ConnectorWriteFieldRequest, ConnectorWriteInputRequest};

    ConnectorWriteInputRequest::Data {
        fields: columns
            .iter()
            .map(|column| {
                ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(
                    &column.name,
                    column.data_type.clone(),
                    column.nullable,
                ))
            })
            .collect(),
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
/// statement opens its write session or activates its copy-on-write plan.
#[derive(Clone)]
pub(crate) struct DmlChangeStreamPreparations {
    lease: novarocks_spi::connector::ConnectorWriteLease,
    preparation: novarocks_spi::connector::ConnectorRowMutationPreparation,
}

impl DmlChangeStreamPreparations {
    fn prepare(
        target: &crate::connector::write_target::ConnectorWriteTargetBinding,
        target_ref: &str,
        effect_set: DmlRowMutationEffectSet,
        context: novarocks_spi::connector::ConnectorRequestContext,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    ) -> Result<Self, String> {
        use novarocks_spi::connector::ConnectorRowMutationIntent;

        let intent = match effect_set {
            DmlRowMutationEffectSet::UpdateMor => ConnectorRowMutationIntent::Update,
            DmlRowMutationEffectSet::Merge { .. } => ConnectorRowMutationIntent::Merge {
                effects: effect_set.effects(),
            },
        };
        let (lease, preparation) =
            target.prepare_row_mutation(target_ref, operation_id, intent, context)?;
        Ok(Self { lease, preparation })
    }

    /// Wrap a preparation this statement already obtained.
    ///
    /// A statement signs exactly one row-mutation preparation. Callers that read
    /// the strategy off it during admission reuse that same value here rather
    /// than asking the provider again, so one statement never carries two base
    /// versions or two digests.
    const fn from_signed(
        lease: novarocks_spi::connector::ConnectorWriteLease,
        preparation: novarocks_spi::connector::ConnectorRowMutationPreparation,
    ) -> Self {
        Self { lease, preparation }
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

/// Open the write session one merge-on-read change-stream statement writes
/// through.
///
/// The producer emits one row stream carrying both halves of every change
/// event: the `_file`/`_pos` identity of the row being superseded, and the
/// after-image of the row replacing it. That is exactly a row-lineage input, so
/// it is described as one, and the provider decides from it how many branches
/// the mutation needs and what each accepts. The v3 lineage columns travel with
/// the data half so an updated row keeps the identity it already had rather
/// than being re-minted as a fresh row.
fn begin_mor_change_stream_write_session(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    target_ref: &str,
    target_columns: &[novarocks_types::schema::ColumnDef],
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    write_planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<ConnectorWriteSession>, String> {
    use novarocks_execution::exec::row_position::{
        ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
        ICEBERG_ROW_POS_COL,
    };
    use novarocks_spi::connector::{ConnectorWriteFieldRequest, ConnectorWriteInputRequest};

    let field = |name: &str, data_type: DataType, nullable: bool| {
        ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(name, data_type, nullable))
    };
    let mut data_fields = target_columns
        .iter()
        .map(|column| field(&column.name, column.data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    data_fields.push(field(ICEBERG_ROW_ID_COL, DataType::Int64, true));
    data_fields.push(field(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true));
    crate::query_execution::write_session::begin_connector_write_session(
        crate::connector::write_target::derive_write_stack_lease(
            state.typed_connector_control(),
            write_planning_lease,
        )?,
        write_lease,
        crate::query_execution::dml::iceberg_writer::connector_write_begin_request(
            target,
            target_ref,
            novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            ConnectorWriteInputRequest::RowLineage {
                data_fields,
                row_identity_fields: vec![
                    field(ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
                    field(ICEBERG_ROW_POS_COL, DataType::Int64, false),
                ],
            },
            novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::RowMutation,
            connector_context.clone(),
        )?,
    )
}

/// Release a session whose plan never compiled.
///
/// Nothing external has happened -- a begin performs reads only -- but the
/// provider is holding a session for a plan that will never run, and the
/// statement's one terminal decision is the only thing that releases it. The
/// planning failure is what the caller reports, so a failure to release is
/// logged rather than substituted for it.
fn release_unplanned_write_session(
    write_session: &ConnectorWriteSession,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    statement: &str,
) {
    if let Err(error) = write_session.abort(connector_context.clone()) {
        tracing::warn!(
            statement,
            %error,
            "releasing an unplanned row-mutation write session failed",
        );
    }
}

/// One logical write target of a row-mutation session paired with the routing
/// facts the provider signed for it.
type ChangeStreamRoutedTarget<'a> = (
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
    let mut routed = write_session
        .targets()
        .iter()
        .map(|write_target| {
            write_target
                .route()
                .map(|route| (write_target, route))
                .ok_or_else(|| {
                    format!(
                        "row-mutation write target {} carries no provider routing facts",
                        write_target.ordinal().get()
                    )
                })
        })
        .collect::<Result<Vec<_>, String>>()?;
    routed.sort_by_key(|(write_target, _)| write_target.ordinal());
    Ok(routed)
}

#[allow(clippy::too_many_arguments)]
fn compile_dml_change_stream_write(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    query: novarocks_parser::ast::Query,
    kind: DmlChangeStreamKind,
    pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: &ConnectorWriteSession,
    write_planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<
    crate::query_execution::compiler::PlannedIcebergChangeStreamWrite,
    crate::dml::error::DmlExecutionError,
> {
    use novarocks_spi::connector::ConnectorWriteInputShape;

    let catalog_service_snapshot =
        crate::catalog_application::query_catalog::catalog_service_snapshot(state);
    let analyzer_provider =
        crate::catalog_application::query_materializer::build_catalog_service_provider(
            Some(&target.catalog),
            &catalog_service_snapshot,
            state.connector_control().as_ref(),
            connector_context.clone(),
            novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
            state.catalog_application().map(Arc::as_ref),
        );
    let table_bindings = analyzer_provider.query_table_bindings();
    // The recipes are sealed before the plan is compiled because the plan's
    // writer nodes carry them: a plan and the session that sealed it must not be
    // separable.
    let sealed_write_targets = write_session
        .seal_write_targets()
        .map_err(|error| format!("seal row-mutation write targets: {error}"))?;
    let routed_targets = change_stream_routed_targets(write_session)?;
    let mut routes = Vec::with_capacity(routed_targets.len());
    let mut statistics_targets = Vec::with_capacity(routed_targets.len());
    for (write_target, route) in routed_targets {
        let target_binding = admit_session_connector_write_target(
            table_bindings.as_ref(),
            FrozenConnectorScanIdentity::new(
                target.catalog.clone(),
                target.namespace.clone(),
                target.table.clone(),
            ),
            write_target,
            write_planning_lease.clone(),
        )?;
        let mode = match write_target.input() {
            ConnectorWriteInputShape::Data { .. } => DmlWriteSinkMode::Data,
            ConnectorWriteInputShape::RowLineage { .. } => DmlWriteSinkMode::RowLineageData,
            ConnectorWriteInputShape::PositionDelete { .. } => DmlWriteSinkMode::PositionDeletes,
            ConnectorWriteInputShape::DeletionVector { .. } => DmlWriteSinkMode::DeletionVectors,
            ConnectorWriteInputShape::EqualityDelete { .. } => DmlWriteSinkMode::EqualityDeletes,
        };
        let sink = dml_write_plan_input_for_admitted_target(
            table_bindings.as_ref(),
            target_binding,
            mode,
            novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
        )
        .map_err(|error| format!("build row-mutation route sink: {error}"))?;
        let input_fields = write_target
            .input()
            .fields()
            .into_iter()
            .map(|field| DmlChangeStreamRouteField {
                token: field.token(),
                output_name: field.field().name().to_string(),
            })
            .collect();
        routes.push(DmlChangeStreamRoute {
            route_id: route.route_id(),
            // The branch's identity is its sealed ordinal, never its position
            // in this loop.
            write_target_ordinal: write_target.ordinal(),
            accepted_effects: route.accepted_effects().to_vec(),
            input_fields,
            partition_input_tokens: route.partition_fields().to_vec(),
            sink,
        });
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
    let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let request = novarocks_sql::compiler::SqlAnalyzeRequest::new(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query)),
        novarocks_sql::compiler::SqlCompileIntent::ChangeStreamWrite,
        novarocks_sql::compiler::SqlSessionContext {
            current_catalog: None,
            current_database: target.namespace.clone(),
            optimizer_settings: dml_change_stream_optimizer_settings(),
        },
        novarocks_sql::compiler::SqlPlanningEnvironment::Distributed,
        &catalog,
        state.function_catalog().as_ref(),
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        novarocks_sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::query_execution::planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(request)
        .map_err(crate::dml::error::DmlExecutionError::from_compile)?
        .into_pending()
        .map_err(|error| error.to_string())?;
    let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
        state,
        Arc::clone(&table_bindings),
        connector_context,
    )?;
    let sealed =
        novarocks_sql::planning::dml::compile_dml_change_stream(DmlChangeStreamCompileRequest {
            optimize_request: novarocks_sql::compiler::SqlOptimizeRequest::new(
                analyzed,
                &statistics,
            ),
            kind,
            routes,
            statistics_targets,
            pre_expand_keyed_assert,
            // Every writer is an ordinary dataflow node whose rows gather into
            // one Root finish fragment; the session, not a terminal sink, owns
            // the commit.
            shape: novarocks_sql::planning::dml::DmlWritePlanShape::Dataflow,
        })?;
    let planned = crate::query_execution::compiler::prepare_dml_change_stream_write(
        state.connector_control().as_ref(),
        state.typed_connector_control(),
        sealed,
        table_bindings.as_ref(),
        connector_context,
    )?;
    Ok(
        crate::query_execution::compiler::PlannedIcebergChangeStreamWrite {
            // The recipes travel with the plan they were sealed for, so an
            // encode can never pair one round's plan with another's session.
            encoding: planned
                .encoding
                .with_sealed_write_targets(sealed_write_targets),
            writer_routes: planned.writer_routes,
        },
    )
}

/// Core-private staged mutation execution retained behind `MutationEngine`'s
/// opaque handles.  It intentionally has no journal or SQL routing policy.
pub(crate) trait MutationExecution: Send + Sync {
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    fn stage(&self) -> Result<QueryExecutionResult, String>;
    fn needs_abort_on_stage_error(&self) -> bool {
        false
    }
    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String>;
    fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext;
    fn commit_terminal(
        &self,
        completion: MutationCommitCompletion,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        match completion {
            MutationCommitCompletion::Session(completion) => {
                crate::query_execution::write_session::finish_write_session(
                    completion,
                    self.terminal_context(),
                )
                .map(crate::query_execution::write_session::CommittedWriteSession::into_outcome)
                .map_err(|error| error.to_string())
            }
            MutationCommitCompletion::AccumulatedSession(session) => session
                .finish_accumulated(self.terminal_context())
                .map_err(|error| error.to_string()),
        }
    }
    fn finalize(&self) -> Result<(), String>;
}

/// The one commit authority a staged mutation hands its statement owner.
///
/// Every mutation writes through the NCP-6 write session, but a single-query
/// mutation hands over the set its last execution produced while a
/// copy-on-write mutation hands over the session that already accumulated
/// every query it drove, so the carrier names which one it is instead of
/// letting a caller guess.
pub(crate) enum MutationCommitCompletion {
    /// The write-session carrier of a merge-on-read change-stream mutation.
    Session(crate::query_execution::outcome::ConnectorWriteSessionCompletion),
    /// A session that already accumulated every query it drove.
    ///
    /// A copy-on-write mutation compiles one query per rewritten file, each
    /// complete for its own execution graph, and commits their union exactly
    /// once. There is no last set to hand over here because every one of them
    /// is already inside the session.
    AccumulatedSession(Arc<ConnectorWriteSession>),
}

/// The authority that may resolve a commit whose external outcome is unknown.
///
/// It is captured beside the completion because the completion is consumed by
/// the commit itself, and an unknown outcome must be adjudicated through the
/// exact authority that issued that commit, never through a replacement.
pub(crate) enum MutationPublicationAuthority {
    Session(Arc<ConnectorWriteSession>),
}

impl MutationCommitCompletion {
    pub(crate) fn publication_authority(&self) -> MutationPublicationAuthority {
        match self {
            Self::Session(completion) => {
                MutationPublicationAuthority::Session(Arc::clone(completion.session()))
            }
            Self::AccumulatedSession(session) => {
                MutationPublicationAuthority::Session(Arc::clone(session))
            }
        }
    }
}

impl MutationPublicationAuthority {
    /// Ask the issuing authority whether the write this evidence describes
    /// became visible.
    ///
    /// `context` is the statement's own terminal context and is what the write
    /// session reconciles under.
    pub(crate) fn adjudicate(
        &self,
        evidence: novarocks_spi::connector::ExternalMutationEvidence,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        match self {
            Self::Session(session) => session
                .reconcile(evidence, context)
                .map_err(|error| error.to_string()),
        }
    }
}

/// Result of the post-journal mutation staging phase.  The connector
/// completion stays paired with the exact execution that accepted it, so a
/// frontend cannot commit a completion through another mutation handle.
pub(crate) enum MutationStagedWrite {
    NoOp,
    AbortRequired {
        reason: String,
        execution: Arc<dyn MutationExecution>,
    },
    CommitRequired {
        execution: Arc<dyn MutationExecution>,
        completion: MutationCommitCompletion,
    },
}

/// Frontend-local mutation input lowered from SQLP-5's typed AST.  Expression
/// and derived-query text are exact slices of the admitted SQL source; they
/// are never rebuilt through the canonical printer.
#[derive(Clone, Debug)]
pub(crate) struct PreparedUpdateStatement {
    pub(crate) table: ObjectName,
    pub(crate) alias: Option<String>,
    pub(crate) assignments: Vec<PreparedMutationAssignment>,
    pub(crate) source: Option<PreparedMutationSource>,
    pub(crate) where_sql: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMergeStatement {
    pub(crate) table: ObjectName,
    pub(crate) target_alias: Option<String>,
    pub(crate) source: PreparedMutationSource,
    pub(crate) on_sql: String,
    pub(crate) matched: Option<PreparedMergeClause<PreparedMergeMatchedAction>>,
    pub(crate) not_matched: Option<PreparedMergeClause<PreparedMergeNotMatchedAction>>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMutationAssignment {
    pub(crate) column: String,
    pub(crate) value_sql: String,
}

#[derive(Clone, Debug)]
pub(crate) enum PreparedMutationSource {
    Table {
        name: ObjectName,
        alias: Option<String>,
    },
    Query {
        query_text: String,
        alias: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMergeClause<A> {
    pub(crate) predicate_sql: Option<String>,
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
    pub(crate) values_sql: Vec<String>,
}

pub(crate) struct PreparedUpdateMutation {
    pub(crate) stmt: PreparedUpdateStatement,
    pub(crate) current_catalog: Option<String>,
    pub(crate) target: crate::catalog_application::resolver::TargetBackend,
    pub(crate) target_columns: Vec<novarocks_types::schema::ColumnDef>,
    pub(crate) target_ref: String,
    /// The one exact connector generation admitted with this statement.
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    /// The one write lease this statement will use, derived once here.
    ///
    /// `derive_write_lease` mints a fresh fence cell on every call, so deriving
    /// it again inside staging would fence a lease that nothing later commits
    /// through. Deriving once at preparation lets the coordinator establish the
    /// external fence before any writer is dispatched, and staging reuses the
    /// same authority.
    pub(crate) write_lease: novarocks_spi::connector::ConnectorWriteLease,
    pub(crate) cow_preparations: Option<DmlChangeStreamPreparations>,
    pub(crate) mor_write_target: Option<PreparedMorUpdateWriteTarget>,
    /// The physical route the provider signed for this statement. Kept as the
    /// neutral strategy rather than re-encoded into the provider's own write-mode
    /// enum, so nothing downstream re-decides it.
    pub(crate) mode: novarocks_spi::connector::ConnectorRowMutationStrategy,
    /// The base version the provider signed for this target ref. The frontend
    /// persists it in its durable DML journal; nothing here re-derives it from a
    /// table handle.
    pub(crate) admitted_base_snapshot_id: Option<i64>,
    pub(crate) execution: QueryExecutionContext,
    pub(crate) connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

/// MOR-only writer facts frozen during UPDATE admission.
///
/// COW UPDATE retains its existing per-file application lifecycle.  In
/// contrast, MOR builds one SQL change-stream producer after the frontend has
/// persisted the mutation intent, so its writer target must be frozen here.
pub(crate) struct PreparedMorUpdateWriteTarget {
    /// Provider-signed writer facts frozen with `planning_lease`. They are
    /// admitted into the same query-local store as the producer compile, never
    /// rebuilt during stage/preparation.
    pub(crate) preparations: DmlChangeStreamPreparations,
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
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
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    /// The one write lease this statement will use, derived once here.
    ///
    /// `derive_write_lease` mints a fresh fence cell on every call, so deriving
    /// it again inside staging would fence a lease that nothing later commits
    /// through. Deriving once at preparation lets the coordinator establish the
    /// external fence before any writer is dispatched, and staging reuses the
    /// same authority.
    pub(crate) write_lease: novarocks_spi::connector::ConnectorWriteLease,
    pub(crate) cow_preparations: Option<DmlChangeStreamPreparations>,
    pub(crate) mor_write_target: Option<PreparedMorMergeWriteTarget>,
    pub(crate) insert_columns_resolved: Option<MergeInsertColumns>,
    /// See [`PreparedUpdateMutation::admitted_base_snapshot_id`].
    pub(crate) admitted_base_snapshot_id: Option<i64>,
    pub(crate) execution: QueryExecutionContext,
    pub(crate) connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

/// Frozen MOR writer facts for MERGE.  The producer query and its terminal
/// sink must use the same admission lease and physical target envelope.
pub(crate) struct PreparedMorMergeWriteTarget {
    pub(crate) preparations: DmlChangeStreamPreparations,
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

#[derive(Clone, Copy)]
enum CowSelectionFieldRole<'a> {
    Identity(&'a novarocks_spi::connector::ConnectorMutationSourceField),
    Before(&'a novarocks_spi::connector::ConnectorMutationTargetField),
    After(&'a novarocks_spi::connector::ConnectorMutationTargetField),
    Effect(&'a novarocks_spi::connector::ConnectorMutationEffectField),
}

fn cow_selection_layout(
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Result<(arrow::datatypes::SchemaRef, Vec<CowSelectionFieldRole<'_>>), String> {
    let contract = preparation.match_contract();
    let mut by_ordinal = Vec::<Option<CowSelectionFieldRole<'_>>>::new();
    fn insert_role<'a>(
        by_ordinal: &mut Vec<Option<CowSelectionFieldRole<'a>>>,
        ordinal: u32,
        role: CowSelectionFieldRole<'a>,
    ) -> Result<(), String> {
        let ordinal = usize::try_from(ordinal)
            .map_err(|_| "COW selection ordinal does not fit this process".to_string())?;
        if by_ordinal.len() <= ordinal {
            by_ordinal.resize(ordinal + 1, None);
        }
        if by_ordinal[ordinal].replace(role).is_some() {
            return Err("COW match contract reuses a selection ordinal".to_string());
        }
        Ok(())
    }
    for field in contract.identity_fields() {
        insert_role(
            &mut by_ordinal,
            field.source_ordinal(),
            CowSelectionFieldRole::Identity(field),
        )?;
    }
    for field in contract.before_fields() {
        insert_role(
            &mut by_ordinal,
            field.target_ordinal(),
            CowSelectionFieldRole::Before(field),
        )?;
    }
    for field in contract.after_fields() {
        insert_role(
            &mut by_ordinal,
            field.target_ordinal(),
            CowSelectionFieldRole::After(field),
        )?;
    }
    insert_role(
        &mut by_ordinal,
        contract.effect_field().target_ordinal(),
        CowSelectionFieldRole::Effect(contract.effect_field()),
    )?;
    let roles = by_ordinal
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| "COW match contract has a gap in its selection ordinals".to_string())?;
    let fields = roles
        .iter()
        .map(|role| match role {
            CowSelectionFieldRole::Identity(field) => field.field().clone(),
            CowSelectionFieldRole::Before(field) | CowSelectionFieldRole::After(field) => {
                field.field().clone()
            }
            CowSelectionFieldRole::Effect(field) => field.field().clone(),
        })
        .collect::<Vec<_>>();
    Ok((Arc::new(Schema::new(fields)), roles))
}

fn cow_selection_from_query_result(
    result: QueryResult,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorRowMutationSelection, String> {
    let (schema, _) = cow_selection_layout(preparation)?;
    let mut collector =
        crate::query_execution::row_mutation::BoundedRowMutationMatchCollector::try_new_with_schema(
            context,
            None,
            Arc::clone(&schema),
        )
        .map_err(|error| format!("create bounded COW match collector: {error}"))?;
    for chunk in result.chunks {
        if chunk.batch.num_columns() != schema.fields().len() {
            return Err(
                "COW match query output width differs from its signed contract".to_string(),
            );
        }
        let columns = chunk
            .batch
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(column, field)| {
                novarocks_execution::exec::expr::cast_array_to_target(column, field.data_type())
                    .map_err(|error| {
                        format!(
                            "cast COW match ordinal to its signed type {:?}: {error}",
                            field.data_type()
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .map_err(|error| format!("assemble signed COW match batch: {error}"))?;
        collector
            .push(batch)
            .map_err(|error| format!("collect bounded COW match batch: {error}"))?;
    }
    let selection = collector
        .finish()
        .map_err(|error| format!("finish bounded COW match collection: {error}"))?;
    let mut validator = crate::query_execution::row_mutation::RowMutationMatchValidator::try_new(
        preparation.match_contract().clone(),
        preparation.intent().clone(),
    )
    .map_err(|error| format!("initialize COW match contract validator: {error}"))?;
    validator
        .validate_selection(&selection)
        .map_err(|error| format!("validate COW match contract: {error}"))?;
    Ok(selection)
}

fn cow_target_columns(
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Vec<novarocks_types::schema::ColumnDef> {
    preparation
        .match_contract()
        .after_fields()
        .iter()
        .map(|field| novarocks_types::schema::ColumnDef {
            name: field.field().name().to_string(),
            data_type: field.field().data_type().clone(),
            nullable: field.field().is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect()
}

pub(crate) fn prepare_update_mutation(
    state: &DmlExecutionKernel,
    stmt: &PreparedUpdateStatement,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    publication_id: novarocks_spi::connector::LakePublicationId,
) -> Result<PreparedUpdateMutation, String> {
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

    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.provider_id.as_str() != "iceberg" {
        return Err(format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.provider_id.as_str()
        ));
    }

    // Reject a managed materialized view from neutral metadata under an exact
    // generation. This cannot move into row-mutation admission: incremental MV
    // refresh drives its own writes through that same admission, so at that
    // level a user statement is indistinguishable from the MV machinery
    // maintaining its own target.
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        state.connector_control().as_ref(),
        state.mv_storage_observation().as_ref(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Update,
    )?;

    let target_binding = crate::connector::write_target::load_write_target_binding(
        state.connector_control().as_ref(),
        &target.catalog,
        &target.namespace,
        &target.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        connector_context.clone(),
    )?;
    let planning_lease = target_binding.lease().clone();
    // Target columns and the partition-column set are provider-signed facts, so
    // assignment validation never decodes an Iceberg schema. The branch/format
    // gate now lives in row-mutation admission below.
    // The physical strategy is whatever the provider signs for this table state.
    let strategy_operation_id = publication_id.into();
    let (strategy_lease, strategy_preparation) = target_binding.prepare_row_mutation(
        &target_ref,
        strategy_operation_id,
        novarocks_spi::connector::ConnectorRowMutationIntent::Update,
        connector_context.clone(),
    )?;
    // Only the two row-rewrite routes can serve UPDATE; anything else is a
    // provider/consumer disagreement and stays fail-fast.
    let mode = match strategy_preparation.strategy() {
        strategy @ (novarocks_spi::connector::ConnectorRowMutationStrategy::CopyOnWrite
        | novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead) => strategy,
        other => {
            return Err(format!(
                "UPDATE cannot be served by row-mutation strategy {other:?}"
            ));
        }
    };
    let admitted_base_snapshot_id = strategy_preparation.base_version_ordinal();
    let target_columns =
        if mode == novarocks_spi::connector::ConnectorRowMutationStrategy::CopyOnWrite {
            cow_target_columns(&strategy_preparation)
        } else {
            target_binding.dml_target_columns()
        };
    let partition_source_columns = target_binding
        .metadata()
        .planning_facts
        .partition_source_column_ordinals()
        .iter()
        .map(|ordinal| {
            target_columns
                .get(*ordinal as usize)
                .map(|column| column.name.clone())
                .ok_or_else(|| {
                    "connector write target has a partition source ordinal outside its admitted schema"
                        .to_string()
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_update_assignments(
        &stmt.assignments,
        &target_columns,
        &partition_source_columns,
    )?;
    let signed_preparations =
        DmlChangeStreamPreparations::from_signed(strategy_lease, strategy_preparation);
    let cow_preparations = (mode
        == novarocks_spi::connector::ConnectorRowMutationStrategy::CopyOnWrite)
        .then(|| signed_preparations.clone());
    let mor_write_target =
        if mode == novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead {
            // The writer target is the preparation that already named the strategy.
            // Signing a second one here would give a single UPDATE two base versions
            // and two digests. Stage runs after frontend lifecycle persistence and
            // must never reopen the connector generation or observe a later
            // snapshot.
            Some(PreparedMorUpdateWriteTarget {
                preparations: signed_preparations,
                planning_lease: planning_lease.clone(),
            })
        } else {
            None
        };
    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| format!("derive UPDATE write lease: {error}"))?;
    Ok(PreparedUpdateMutation {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        target,
        target_columns,
        target_ref,
        planning_lease,
        write_lease,
        cow_preparations,
        mor_write_target,
        mode,
        admitted_base_snapshot_id,
        execution: execution.clone(),
        connector_context: connector_context.clone(),
    })
}

/// Resolve and validate MERGE without materializing source rows, registering a
/// cohort, or creating a staging artifact. It retains one exact planning lease
/// for every later read or writer admission.
pub(crate) fn prepare_merge_mutation(
    state: &DmlExecutionKernel,
    stmt: &PreparedMergeStatement,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    publication_id: novarocks_spi::connector::LakePublicationId,
) -> Result<PreparedMergeMutation, String> {
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
        Some(IcebergRefSuffix::Branch(branch)) => branch.clone(),
        _ => "main".to_string(),
    };
    let target = crate::catalog_application::resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.provider_id.as_str() != "iceberg" {
        return Err(format!(
            "MERGE only supports iceberg backends, got `{}`",
            target.provider_id.as_str()
        ));
    }
    // See the UPDATE path for why this rejection cannot live in row-mutation
    // admission.
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        state.connector_control().as_ref(),
        state.mv_storage_observation().as_ref(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Merge,
    )?;
    let target_binding = crate::connector::write_target::load_write_target_binding(
        state.connector_control().as_ref(),
        &target.catalog,
        &target.namespace,
        &target.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        connector_context.clone(),
    )?;
    let planning_lease = target_binding.lease().clone();
    // The clause composition is a statement fact; what it implies physically is
    // not. A MERGE that can delete matched rows needs merge-on-read even on a
    // copy-on-write table, and that rule now lives with the provider, which
    // reads it off the intent's effect set.
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
        publication_id.into(),
    )?;
    // Same two-route restriction as UPDATE; see `prepare_update_mutation`.
    let table_write_mode = match preparations.preparation.strategy() {
        strategy @ (novarocks_spi::connector::ConnectorRowMutationStrategy::CopyOnWrite
        | novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead) => strategy,
        other => {
            return Err(format!(
                "MERGE cannot be served by row-mutation strategy {other:?}"
            ));
        }
    };
    let admitted_base_snapshot_id = preparations.preparation.base_version_ordinal();
    let target_columns = if table_write_mode
        == novarocks_spi::connector::ConnectorRowMutationStrategy::CopyOnWrite
    {
        cow_target_columns(&preparations.preparation)
    } else {
        target_binding.dml_target_columns()
    };
    let partition_source_columns = target_binding
        .metadata()
        .planning_facts
        .partition_source_column_ordinals()
        .iter()
        .map(|ordinal| {
            target_columns
                .get(*ordinal as usize)
                .map(|column| column.name.clone())
                .ok_or_else(|| {
                    "connector write target has a partition source ordinal outside its admitted schema"
                        .to_string()
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(clause) = stmt.matched.as_ref()
        && let PreparedMergeMatchedAction::Update { assignments } = &clause.action
    {
        validate_update_assignments(assignments, &target_columns, &partition_source_columns)?;
    }
    let insert_columns_resolved = stmt
        .not_matched
        .as_ref()
        .map(|clause| resolve_merge_insert_columns(&clause.action, &target_columns))
        .transpose()?;
    let cow_preparations = (table_write_mode
        == novarocks_spi::connector::ConnectorRowMutationStrategy::CopyOnWrite)
        .then(|| preparations.clone());
    let mor_write_target = if table_write_mode
        == novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead
    {
        Some(PreparedMorMergeWriteTarget {
            preparations,
            planning_lease: planning_lease.clone(),
        })
    } else {
        None
    };
    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| format!("derive MERGE write lease: {error}"))?;
    Ok(PreparedMergeMutation {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        target,
        target_columns,
        target_ref,
        table_write_mode,
        planning_lease,
        write_lease,
        cow_preparations,
        mor_write_target,
        insert_columns_resolved,
        admitted_base_snapshot_id,
        execution: execution.clone(),
        connector_context: connector_context.clone(),
    })
}

/// Execute the post-intent half of an UPDATE. Preparation above only freezes
/// validation and connector planning facts; match materialization, cohort
/// registration and distributed staging happen only here, after the frontend
/// has persisted its `Preparing` record.
pub(crate) fn stage_prepared_update_mutation(
    state: &DmlExecutionKernel,
    prepared: PreparedUpdateMutation,
    native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
) -> Result<MutationStagedWrite, crate::dml::error::DmlExecutionError> {
    let PreparedUpdateMutation {
        stmt,
        current_catalog,
        target,
        target_columns,
        target_ref,
        planning_lease,
        write_lease,
        cow_preparations,
        mor_write_target,
        mode,
        admitted_base_snapshot_id: _,
        execution,
        connector_context,
    } = prepared;
    match mode {
        novarocks_spi::connector::ConnectorRowMutationStrategy::CopyOnWrite => {
            let cow_preparations = cow_preparations.ok_or_else(|| {
                "COW UPDATE reached stage without its signed row-mutation preparation".to_string()
            })?;
            let source_sql =
                mutation_source_to_sql(state, &stmt.source, current_catalog.as_deref(), &target)?;
            let query = build_exact_cow_update_selection_query(
                &target,
                &stmt,
                source_sql.as_deref(),
                &cow_preparations.preparation,
            )?;
            let matched = execute_exact_cow_match_query(
                state,
                &target,
                &query,
                &execution,
                &connector_context,
                native_encoder,
            )?;
            let selection = cow_selection_from_query_result(
                matched,
                &cow_preparations.preparation,
                connector_context.clone(),
            )?;
            if selection.row_count() == 0 {
                return Ok(MutationStagedWrite::NoOp);
            }
            // The session is opened only now, after the match query has run:
            // the provider seals one branch per rewritten file, and which files
            // those are is exactly what the selection says.
            let write_session = begin_cow_write_session(
                state,
                &target,
                &target_ref,
                &cow_preparations.preparation,
                selection.clone(),
                &write_lease,
                &planning_lease,
                &connector_context,
            )?;
            let write = match build_cow_update_distributed_write(
                &target,
                planning_lease,
                &cow_preparations.preparation,
                &selection,
                Arc::clone(&write_session),
            ) {
                Ok(write) => write,
                Err(error) => {
                    release_unplanned_write_session(
                        &write_session,
                        &connector_context,
                        "COW UPDATE",
                    );
                    return Err(error.into());
                }
            };
            let execution_handle = Arc::new(DistributedCowUpdateExecutor {
                state: state.clone(),
                target: target.clone(),
                write: Mutex::new(Some(write)),
                write_session: Arc::clone(&write_session),
                execution,
                connector_context,
            });
            let staged = match execution_handle.run_stage(native_encoder) {
                Ok(staged) => staged,
                Err(error @ crate::dml::error::DmlExecutionError::Analyze(_)) => {
                    return Err(error);
                }
                Err(error) => {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason: error.to_string(),
                        execution: execution_handle,
                    });
                }
            };
            // A statement whose every branch closed without staging an artifact
            // has no snapshot to publish, so the session is released instead of
            // committing one that describes nothing.
            if !staged.staged_any_artifact {
                if let Err(reason) = execution_handle.release_empty_write_session() {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason,
                        execution: execution_handle,
                    });
                }
                return Ok(MutationStagedWrite::NoOp);
            }
            Ok(MutationStagedWrite::CommitRequired {
                execution: execution_handle,
                completion: MutationCommitCompletion::AccumulatedSession(write_session),
            })
        }
        other @ (novarocks_spi::connector::ConnectorRowMutationStrategy::PositionDelete
        | novarocks_spi::connector::ConnectorRowMutationStrategy::DeletionVector
        | novarocks_spi::connector::ConnectorRowMutationStrategy::EqualityDelete) => {
            Err(format!("UPDATE cannot be served by row-mutation strategy {other:?}").into())
        }
        novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead => {
            let PreparedMorUpdateWriteTarget {
                preparations,
                planning_lease: write_planning_lease,
            } = mor_write_target.ok_or_else(|| {
                "MOR UPDATE reached stage without an admitted frozen write target".to_string()
            })?;
            // The version rewritten rows belong to, signed at admission: a
            // merge-on-read writer stamps it on every row it emits, and it must
            // not be re-derived from a table that may have moved since.
            let written_version = preparations
                .preparation
                .written_version_ordinal()
                .ok_or_else(|| {
                    "MOR UPDATE requires a provider-signed written version".to_string()
                })?;
            // The write lease was derived once at preparation so the
            // coordinator could fence it before dispatch; re-deriving here
            // would mint a fresh fence cell and silently discard that fence.
            let write_session = begin_mor_change_stream_write_session(
                state,
                &target,
                &target_ref,
                &target_columns,
                &write_lease,
                &write_planning_lease,
                &connector_context,
            )?;
            let planned = match build_update_mor_change_stream_write_plan(
                state,
                &target,
                &stmt,
                current_catalog.as_deref(),
                &target_columns,
                &target_ref,
                written_version,
                &execution,
                &connector_context,
                &write_session,
                write_planning_lease,
            ) {
                Ok(planned) => planned,
                Err(error) => {
                    release_unplanned_write_session(
                        &write_session,
                        &connector_context,
                        "MOR UPDATE",
                    );
                    return Err(error);
                }
            };
            let execution_handle = Arc::new(MorUpdateChangeStreamExecutor {
                state: state.clone(),
                target: target.clone(),
                planned: Mutex::new(Some(planned)),
                execution,
                connector_context,
                write_session,
            });
            let result = match execution_handle.run_stage(native_encoder) {
                Ok(result) => result,
                Err(reason) => {
                    if execution_handle.needs_abort_on_stage_error() {
                        return Ok(MutationStagedWrite::AbortRequired {
                            reason,
                            execution: execution_handle,
                        });
                    }
                    return Err(reason.into());
                }
            };
            let Some(completion) = result.write_session else {
                return Ok(MutationStagedWrite::AbortRequired {
                    reason: "MOR UPDATE staged without a write-session completion".to_string(),
                    execution: execution_handle,
                });
            };
            // An UPDATE that matched nothing produced no commit fragment at
            // all. Committing that would publish a snapshot describing nothing,
            // so the session is released instead and the statement reports the
            // same no-op terminal the staged-report path reached through its
            // own known-empty check.
            if completion.is_empty() {
                if let Err(reason) = execution_handle.release_empty_write_session(&completion) {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason,
                        execution: execution_handle,
                    });
                }
                return Ok(MutationStagedWrite::NoOp);
            }
            Ok(MutationStagedWrite::CommitRequired {
                execution: execution_handle,
                completion: MutationCommitCompletion::Session(completion),
            })
        }
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn materialize_update_matches(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedUpdateStatement,
    current_catalog: Option<&str>,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MatchedUpdateBatch, String> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    // The match SELECT runs against the standalone analyzer with
    // `current_database = target.namespace` (so 1-part target name resolves
    // to the iceberg target). Source relations may live in a different
    // namespace; `mutation_source_to_sql` qualifies them with their
    // namespace so the analyzer can find them.
    let target_sql = format!("{} AS {}", target.table, target_alias);
    let assignments_sql = stmt
        .assignments
        .iter()
        .map(|assignment| (assignment.column.as_str(), assignment.value_sql.as_str()))
        .collect::<Vec<_>>();
    let assignments_sql = assignments_sql
        .iter()
        .map(|(column, expr)| (*column, *expr))
        .collect::<Vec<_>>();
    let where_sql = stmt.where_sql.as_deref();
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let match_sql = build_update_match_query_sql(
        &target_sql,
        target_alias,
        source_sql.as_deref(),
        &assignments_sql,
        where_sql,
    );
    execute_update_match_query(
        state,
        Some(&target.catalog),
        &match_sql,
        &target.namespace,
        execution,
        connector_context,
    )
}

fn mutation_source_to_sql(
    state: &DmlExecutionKernel,
    source: &Option<PreparedMutationSource>,
    current_catalog: Option<&str>,
    target: &crate::catalog_application::resolver::TargetBackend,
) -> Result<Option<String>, String> {
    match source {
        None => Ok(None),
        Some(source) => {
            mutation_source_relation_to_sql(state, source, current_catalog, target).map(Some)
        }
    }
}

fn mutation_source_relation_to_sql(
    state: &DmlExecutionKernel,
    source: &PreparedMutationSource,
    current_catalog: Option<&str>,
    target: &crate::catalog_application::resolver::TargetBackend,
) -> Result<String, String> {
    match source {
        PreparedMutationSource::Table { name, alias } => {
            // The match SELECT runs with `current_database = target.namespace`
            // and `current_catalog = Some(target.catalog)`. Resolve the source
            // against the user's surface name to get its concrete (catalog,
            // namespace, table). Emit a 1-part name when the source shares the
            // target's namespace+catalog (lets refresh follow the
            // current-catalog path), and a 2-part `<namespace>.<table>` name
            // otherwise so the standalone analyzer can find it directly.
            let resolved = crate::catalog_application::resolver::resolve_existing_table_target(
                state,
                name,
                current_catalog,
                &target.namespace,
            )?;
            let mut sql =
                if resolved.catalog == target.catalog && resolved.namespace == target.namespace {
                    resolved.table.clone()
                } else {
                    format!("{}.{}", resolved.namespace, resolved.table)
                };
            if let Some(alias) = alias {
                sql.push_str(" AS ");
                sql.push_str(alias);
            }
            Ok(sql)
        }
        PreparedMutationSource::Query { query_text, alias } => {
            let alias = alias
                .as_deref()
                .ok_or_else(|| "MERGE/UPDATE subquery source requires an alias".to_string())?;
            Ok(format!("({query_text}) AS {alias}"))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_update_mor_change_stream_write_plan(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedUpdateStatement,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_types::schema::ColumnDef],
    target_ref: &str,
    new_sequence_number: i64,
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: &ConnectorWriteSession,
    write_planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<
    crate::query_execution::compiler::PlannedIcebergChangeStreamWrite,
    crate::dml::error::DmlExecutionError,
> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let where_sql = stmt.where_sql.as_deref();
    let assignments_sql = update_assignment_projection_sql(&stmt.assignments, target_columns)?;
    let assignments_sql_refs = assignments_sql
        .iter()
        .map(|(column, expr)| (column.as_str(), expr.as_str()))
        .collect::<Vec<_>>();
    let target_sql = update_change_stream_target_sql(target, target_alias, target_ref);
    let match_sql = build_update_match_query_sql(
        &target_sql,
        target_alias,
        source_sql.as_deref(),
        &assignments_sql_refs,
        where_sql,
    );
    let mut query = parse_generated_query(&match_sql, "MOR UPDATE change-stream producer")?;
    if crate::query_execution::planning::time_travel::has_time_travel_refs(&query) {
        crate::query_execution::planning::time_travel::rewrite_time_travel_refs(
            state,
            Some(&target.catalog),
            &target.namespace,
            &mut query,
            connector_context,
        )?;
    }

    compile_dml_change_stream_write(
        state,
        target,
        query,
        DmlChangeStreamKind::Update {
            target_columns: target_columns.to_vec(),
            new_sequence_number,
        },
        Some(DmlPreExpandKeyedAssert {
            key_column_name: "__nr_row_id".to_string(),
            key_label: novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
            message_prefix: "MOR UPDATE matched target row".to_string(),
        }),
        execution,
        connector_context,
        write_session,
        write_planning_lease,
    )
}

fn update_assignment_projection_sql(
    assignments: &[PreparedMutationAssignment],
    target_columns: &[novarocks_types::schema::ColumnDef],
) -> Result<Vec<(String, String)>, String> {
    assignments
        .iter()
        .map(|assignment| {
            let target_column = target_columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&assignment.column))
                .ok_or_else(|| {
                    format!(
                        "UPDATE assignment references unknown target column `{}`",
                        assignment.column
                    )
                })?;
            Ok((
                target_column.name.clone(),
                crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                    &format!("({})", assignment.value_sql),
                    target_column,
                )?,
            ))
        })
        .collect()
}

fn update_change_stream_target_sql(
    target: &crate::catalog_application::resolver::TargetBackend,
    target_alias: &str,
    target_ref: &str,
) -> String {
    let version_clause = if target_ref == "main" {
        String::new()
    } else {
        format!(" FOR VERSION AS OF {}", sql_string_literal(target_ref))
    };
    format!(
        "{}{} AS {}",
        qualify_iceberg_table(target),
        version_clause,
        target_alias
    )
}

fn parse_generated_query(sql: &str, context: &str) -> Result<novarocks_parser::ast::Query, String> {
    let statements = novarocks_parser::parse(sql).map_err(|error| format!("{context}: {error}"))?;
    match statements.as_slice() {
        [novarocks_parser::ast::Statement::Query(query)] => Ok(query.clone()),
        [other] => Err(format!(
            "{context} generated non-query statement: {other:?}"
        )),
        _ => Err(format!(
            "{context} generated an empty or multi-statement query"
        )),
    }
}

fn qualify_iceberg_table(target: &crate::catalog_application::resolver::TargetBackend) -> String {
    format!(
        "{}.{}.{}",
        sql_identifier(&target.catalog),
        sql_identifier(&target.namespace),
        sql_identifier(&target.table)
    )
}

fn qualify_column(alias: &str, column: &str) -> String {
    format!("{}.{}", sql_identifier(alias), sql_identifier(column))
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

struct MorUpdateChangeStreamExecutor {
    state: DmlExecutionKernel,
    target: crate::catalog_application::resolver::TargetBackend,
    planned: Mutex<Option<crate::query_execution::compiler::PlannedIcebergChangeStreamWrite>>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    /// The one commit authority of this statement, opened on the exact
    /// generation that admitted it. The plan's writer nodes carry the recipes
    /// this session sealed, so the two travel together.
    write_session: Arc<ConnectorWriteSession>,
}

struct MorMergeChangeStreamExecutor {
    state: DmlExecutionKernel,
    target: crate::catalog_application::resolver::TargetBackend,
    planned: Mutex<Option<crate::query_execution::compiler::PlannedIcebergChangeStreamWrite>>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    /// See the corresponding UPDATE executor.
    write_session: Arc<ConnectorWriteSession>,
}

/// Run one sealed change-stream plan through the write-session data plane.
///
/// The writers are ordinary dataflow nodes: their rows gather into the Root
/// finish fragment, and the session that sealed their recipes rides along as the
/// request's single commit authority. No operation, cohort, execution, or
/// attempt identity reaches the writer data plane.
fn run_change_stream_write_session_stage(
    state: &DmlExecutionKernel,
    execution: &QueryExecutionContext,
    write_session: &Arc<ConnectorWriteSession>,
    planned: crate::query_execution::compiler::PlannedIcebergChangeStreamWrite,
    native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
    _statement: &str,
) -> Result<QueryExecutionResult, String> {
    let crate::query_execution::compiler::PlannedIcebergChangeStreamWrite {
        encoding,
        // The sealed routes are read only by the build observer; the plan the
        // backends receive carries its writer identities itself.
        writer_routes: _writer_routes,
        ..
    } = planned;
    #[cfg(test)]
    if let Some(result) =
        crate::query_execution::compiler::observe_change_stream_write_build_for_test(
            &_writer_routes,
        )
    {
        return Ok(result);
    }
    let native_bundle = native_encoder.encode(&encoding)?;
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        encoding,
        native_bundle,
        None,
        crate::query_execution::contract::DistributedQueryIntent::Write,
        execution,
    )
    .map_err(|error| error.to_string())?;
    let request = crate::query_execution::contract::with_connector_write_session(
        request,
        Arc::clone(write_session),
    )
    .map_err(|error| error.to_string())?;
    crate::query_execution::dml::write::execute_bound_distributed_write_request(
        state.query_execution(),
        request,
    )
}

impl MorUpdateChangeStreamExecutor {
    /// Release a session whose closed data plane produced no commit fragment.
    fn release_empty_write_session(
        &self,
        completion: &crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    ) -> Result<(), String> {
        completion
            .session()
            .abort(self.connector_context.clone())
            .map(|_| ())
            .map_err(|error| format!("release empty MOR UPDATE write session: {error}"))
    }

    fn run_stage(
        &self,
        native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
    ) -> Result<QueryExecutionResult, String> {
        let planned = self
            .planned
            .lock()
            .expect("MOR UPDATE change-stream plan lock poisoned")
            .take()
            .ok_or_else(|| "MOR UPDATE change-stream plan was already consumed".to_string())?;
        run_change_stream_write_session_stage(
            &self.state,
            &self.execution,
            &self.write_session,
            planned,
            native_encoder,
            "MOR UPDATE",
        )
    }
}

impl MutationExecution for MorUpdateChangeStreamExecutor {
    fn stage(&self) -> Result<QueryExecutionResult, String> {
        Err("MOR UPDATE staging requires the Frontend native fragment encoder".to_string())
    }

    fn needs_abort_on_stage_error(&self) -> bool {
        true
    }

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        self.write_session
            .abort(self.connector_context.clone())
            .map_err(|error| format!("abort MOR UPDATE write session: {error}"))
    }

    fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        self.connector_context.clone()
    }

    fn finalize(&self) -> Result<(), String> {
        crate::catalog_application::resolver::invalidate_iceberg_caches(&self.state, &self.target)
    }
}

impl MorMergeChangeStreamExecutor {
    /// Release a session whose closed data plane produced no commit fragment.
    fn release_empty_write_session(
        &self,
        completion: &crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    ) -> Result<(), String> {
        completion
            .session()
            .abort(self.connector_context.clone())
            .map(|_| ())
            .map_err(|error| format!("release empty MOR MERGE write session: {error}"))
    }

    fn run_stage(
        &self,
        native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
    ) -> Result<QueryExecutionResult, String> {
        let planned = self
            .planned
            .lock()
            .expect("MOR MERGE change-stream plan lock poisoned")
            .take()
            .ok_or_else(|| "MOR MERGE change-stream plan was already consumed".to_string())?;
        run_change_stream_write_session_stage(
            &self.state,
            &self.execution,
            &self.write_session,
            planned,
            native_encoder,
            "MOR MERGE",
        )
    }
}

impl MutationExecution for MorMergeChangeStreamExecutor {
    fn stage(&self) -> Result<QueryExecutionResult, String> {
        Err("MOR MERGE staging requires the Frontend native fragment encoder".to_string())
    }

    fn needs_abort_on_stage_error(&self) -> bool {
        true
    }

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        self.write_session
            .abort(self.connector_context.clone())
            .map_err(|error| format!("abort MOR MERGE write session: {error}"))
    }

    fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        self.connector_context.clone()
    }

    fn finalize(&self) -> Result<(), String> {
        crate::catalog_application::resolver::invalidate_iceberg_caches(&self.state, &self.target)
    }
}
/// Open the write session one copy-on-write mutation writes through.
///
/// Unlike every other write, this session cannot be opened before the statement
/// runs: which files it rewrites, and which rows inside them it matched, is the
/// materialized result of the match query, and the provider seals one branch per
/// rewritten file from exactly that. So the selection travels in the flavor, and
/// a session without it is unconstructible rather than merely refused.
///
/// The input is a row-lineage one whose identity is `_row_id` /
/// `_last_updated_sequence_number`: a rewrite re-emits rows that already have a
/// lineage, and carrying it through is what keeps their identity stable across
/// the file replacement.
fn begin_cow_write_session(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    target_ref: &str,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    selection: novarocks_spi::connector::ConnectorRowMutationSelection,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    write_planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<ConnectorWriteSession>, String> {
    use novarocks_execution::exec::row_position::{
        ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
    };
    use novarocks_spi::connector::{ConnectorWriteFieldRequest, ConnectorWriteInputRequest};

    let field = |name: &str, data_type: DataType, nullable: bool| {
        ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(name, data_type, nullable))
    };
    let data_fields = cow_target_columns(preparation)
        .iter()
        .map(|column| field(&column.name, column.data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    let request = novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
        table: Arc::from(format!("{}.{}", target.namespace, target.table).as_str()),
        target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse(target_ref)
            .map_err(|error| format!("validate copy-on-write target ref: {error}"))?,
        intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
        purpose: novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
        input: ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields: vec![
                field(ICEBERG_ROW_ID_COL, DataType::Int64, true),
                field(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
            ],
        },
        // The base the match query ran against. The provider stamps its digest
        // onto every branch's read contract, so a branch that re-read a
        // different base than the statement matched fails closed here rather
        // than rewriting rows nobody selected.
        base: Some(preparation.base_version().clone()),
        flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::CopyOnWrite(
            selection,
        ),
        context: connector_context.clone(),
    };
    crate::query_execution::write_session::begin_connector_write_session(
        crate::connector::write_target::derive_write_stack_lease(
            state.typed_connector_control(),
            write_planning_lease,
        )?,
        write_lease,
        request,
    )
}

/// Which selection rows belong to which old data file, and which belong to no
/// file at all.
///
/// The provider grouped the same selection the same way when it sealed the
/// session's branches; this grouping is what lets each branch's query name only
/// its own rows. The two are joined by the old file path, which is a fact of
/// the selection rather than a position either side could drift on.
type CowSelectionGroups = (
    HashMap<String, Vec<novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal>>,
    Vec<novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal>,
);

fn cow_selection_groups(
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
) -> Result<CowSelectionGroups, String> {
    use novarocks_spi::connector::{
        ConnectorRowMutationEffect, ConnectorRowMutationSelectionOrdinal,
    };

    let contract = preparation.match_contract();
    let file_ordinal = contract
        .identity_fields()
        .iter()
        .find(|field| {
            field.field().name().eq_ignore_ascii_case(
                novarocks_execution::exec::row_position::ICEBERG_FILE_PATH_COL,
            )
        })
        .map(|field| field.source_ordinal() as usize)
        .ok_or_else(|| "COW match contract lacks its `_file` identity".to_string())?;
    let effect_ordinal = contract.effect_field().target_ordinal() as usize;
    let mut rewrites: HashMap<String, Vec<ConnectorRowMutationSelectionOrdinal>> = HashMap::new();
    let mut appends = Vec::new();
    let mut ordinal = 0_u64;
    for batch in selection.batches() {
        let effects = batch
            .column(effect_ordinal)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| "COW selection effect column is not Int8".to_string())?;
        let files = batch
            .column(file_ordinal)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "COW selection `_file` identity is not UTF-8".to_string())?;
        for index in 0..batch.num_rows() {
            let selection_ordinal = ConnectorRowMutationSelectionOrdinal::new(ordinal);
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| "COW selection ordinal overflowed".to_string())?;
            if effects.is_null(index) {
                return Err("COW selection effect column contains nulls".to_string());
            }
            if effects.value(index) == ConnectorRowMutationEffect::Insert as i8 {
                appends.push(selection_ordinal);
                continue;
            }
            if files.is_null(index) {
                return Err("COW matched row has no `_file` identity".to_string());
            }
            rewrites
                .entry(files.value(index).to_string())
                .or_default()
                .push(selection_ordinal);
        }
    }
    Ok((rewrites, appends))
}

/// The pinned relation one COW rewrite query scans.
///
/// It carries no planned scan: the session froze which file this branch
/// rewrites, and preparation asks the same connector generation to freeze that
/// exact relation. Planning the read here instead would need an opaque handle
/// the typed scan stack cannot admit.
struct CowFrozenRead {
    identity: FrozenConnectorScanIdentity,
    schema: arrow::datatypes::SchemaRef,
    read: crate::query_execution::preparation::scan::QueryPinnedFileSetRead,
}

/// One sealed write target's query, at the ordinal that target holds.
///
/// The ordinal is the only name a branch has, and it is read off the session
/// rather than derived from this vector's position: a query compiled at the
/// wrong ordinal would attribute one file's replacement rows to another file's
/// writer, and the prepared write set cannot notice because both ordinals were
/// really sealed.
struct CowTargetWritePlan {
    ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    input: novarocks_spi::connector::ConnectorWriteInputShape,
    query: novarocks_parser::ast::Query,
    frozen_read: Option<CowFrozenRead>,
}

struct CowUpdateDistributedWrite {
    targets: Vec<CowTargetWritePlan>,
    write_session: Arc<ConnectorWriteSession>,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

/// Compile one query per sealed target of an already-opened copy-on-write
/// session.
///
/// Each rewrite target names exactly one old data file through its read
/// contract, and that file is the join key back to the selection rows the
/// statement matched inside it. The append target -- the one with no read
/// contract -- takes the rows that matched nothing.
fn build_cow_update_distributed_write(
    target: &crate::catalog_application::resolver::TargetBackend,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    write_session: Arc<ConnectorWriteSession>,
) -> Result<CowUpdateDistributedWrite, String> {
    let (mut rewrites, appends) = cow_selection_groups(preparation, selection)?;
    let mut sealed = write_session.targets().to_vec();
    sealed.sort_by_key(novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan::ordinal);
    let mut targets = Vec::with_capacity(sealed.len());
    let mut sealed_append = false;
    for write_target in &sealed {
        let route = write_target.route().ok_or_else(|| {
            format!(
                "copy-on-write write target {} carries no provider routing facts",
                write_target.ordinal().get()
            )
        })?;
        let (query, frozen_read) = match write_target.rewrite_source() {
            Some(source) => {
                if source.base_version_digest() != preparation.base_version().digest() {
                    return Err(
                        "COW rewrite branch base differs from its signed preparation".to_string(),
                    );
                }
                let old_file = match source.pinned_source().files() {
                    [file] => file.to_string(),
                    _ => {
                        return Err(
                            "COW rewrite branch must replace exactly one data file".to_string()
                        );
                    }
                };
                let rows = rewrites.remove(&old_file).ok_or_else(|| {
                    format!("COW rewrite branch names file `{old_file}`, which matched no row")
                })?;
                let identity = FrozenConnectorScanIdentity::new(
                    "default_catalog",
                    target.namespace.clone(),
                    format!("__nr_cow_{}", uuid::Uuid::new_v4().simple()),
                );
                let read = crate::query_execution::preparation::scan::QueryPinnedFileSetRead {
                    pinned: source.pinned_source().clone(),
                    owner: source.source().owner().clone(),
                    planning_lease: planning_lease.clone(),
                };
                let query = build_cow_rewrite_query(
                    selection,
                    &rows,
                    write_target.input(),
                    route,
                    source,
                    preparation,
                    &identity,
                )?;
                (
                    query,
                    Some(CowFrozenRead {
                        identity,
                        schema: source.scan_schema().clone(),
                        read,
                    }),
                )
            }
            None => {
                if sealed_append {
                    return Err("COW session sealed more than one append branch".to_string());
                }
                sealed_append = true;
                if appends.is_empty() {
                    return Err(
                        "COW session sealed an append branch for a statement with no net-new row"
                            .to_string(),
                    );
                }
                (
                    build_cow_append_query(
                        selection,
                        &appends,
                        write_target.input(),
                        route,
                        preparation,
                    )?,
                    None,
                )
            }
        };
        targets.push(CowTargetWritePlan {
            ordinal: write_target.ordinal(),
            input: write_target.input().clone(),
            query,
            frozen_read,
        });
    }
    // Every matched file must have been sealed as its own branch. A leftover
    // group means the session and the statement disagree about what the
    // selection said, and its rows would be silently left unwritten.
    if !rewrites.is_empty() {
        return Err(format!(
            "COW session sealed no branch for {} matched data file(s)",
            rewrites.len()
        ));
    }
    if !appends.is_empty() && !sealed_append {
        return Err("COW session sealed no branch for its net-new rows".to_string());
    }
    Ok(CowUpdateDistributedWrite {
        targets,
        write_session,
        planning_lease,
    })
}

/// One sealed target's writer fields, in the order its signed route puts them.
///
/// The order is read off the route rather than off this loop, because the
/// writer reads the root output positionally: a permuted projection would feed
/// every column into its neighbour's slot.
fn ordered_route_inputs(
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
) -> Result<Vec<novarocks_spi::connector::ConnectorWriteFieldBinding>, String> {
    let by_token = input
        .fields()
        .into_iter()
        .map(|field| (field.token(), field.clone()))
        .collect::<HashMap<_, _>>();
    let mut inputs = route.input_ordinals().to_vec();
    inputs.sort_by_key(novarocks_spi::connector::ConnectorMutationRouteInput::input_ordinal);
    inputs
        .into_iter()
        .map(|input| {
            by_token
                .get(&input.token())
                .cloned()
                .ok_or_else(|| "COW route names a field its target does not carry".to_string())
        })
        .collect()
}

/// Where one signed writer field's value lives in the match selection.
///
/// The writer's field tokens and the match contract's are two different
/// provider-signed spaces -- the session signed one, the row-mutation
/// preparation signed the other -- so they are joined by the column name the
/// same provider put on both sides. Identity is consulted before the
/// after-image because the two can share a name only for a column that is both,
/// and the identity's is the one a rewrite joins on. The before-image is never
/// consulted: the VALUES relation carries what a matched row becomes, never
/// what it was.
fn selection_ordinal_of_writer_field(
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    name: &str,
) -> Option<u32> {
    contract
        .identity_fields()
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case(name))
        .map(novarocks_spi::connector::ConnectorMutationSourceField::source_ordinal)
        .or_else(|| {
            contract
                .after_fields()
                .iter()
                .find(|field| field.field().name().eq_ignore_ascii_case(name))
                .map(novarocks_spi::connector::ConnectorMutationTargetField::target_ordinal)
        })
}

/// Whether one signed writer field carries a matched row's after-image.
fn writer_field_is_after_image(
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    name: &str,
) -> bool {
    contract
        .after_fields()
        .iter()
        .any(|field| field.field().name().eq_ignore_ascii_case(name))
}

fn selection_value_sql(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    row: novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal,
    field_ordinal: u32,
    field: &arrow::datatypes::Field,
) -> Result<String, String> {
    let view = selection
        .locate(row)
        .ok_or_else(|| "COW selection ordinal is out of bounds".to_string())?;
    let array = view
        .batch()
        .columns()
        .get(field_ordinal as usize)
        .ok_or_else(|| "COW selection field ordinal is out of bounds".to_string())?;
    let literal = literal_from_batch(array, view.row_index())?;
    let column = novarocks_types::schema::ColumnDef {
        name: field.name().to_string(),
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
        write_default: None,
        logical_type: None,
    };
    let literal = crate::query_execution::dml::iceberg_writer::literal_to_sql_for_arrow_type(
        &literal,
        field.data_type(),
    )?;
    crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(&literal, &column)
}

/// The net-new rows of a folded `MERGE` insert, as a literal relation.
///
/// They belong to no rewritten file, so this query reads nothing at all.
fn build_cow_append_query(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Result<novarocks_parser::ast::Query, String> {
    let contract = preparation.match_contract();
    let inputs = ordered_route_inputs(input, route)?;
    let mut value_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let values = inputs
            .iter()
            .map(|binding| {
                let field = binding.field();
                let ordinal = selection_ordinal_of_writer_field(contract, field.name())
                    .ok_or_else(|| {
                        "COW append field is absent from the signed selection".to_string()
                    })?;
                selection_value_sql(selection, *row, ordinal, field)
            })
            .collect::<Result<Vec<_>, String>>()?;
        value_rows.push(format!("({})", values.join(", ")));
    }
    let aliases = (0..inputs.len())
        .map(|ordinal| sql_identifier(&format!("__nr_v_{ordinal}")))
        .collect::<Vec<_>>();
    let select_items = inputs
        .iter()
        .enumerate()
        .map(|(ordinal, binding)| {
            let field = binding.field();
            let column = novarocks_types::schema::ColumnDef {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                write_default: None,
                logical_type: None,
            };
            Ok(format!(
                "{} AS {}",
                crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                    &qualify_column("__nr_values", &format!("__nr_v_{ordinal}")),
                    &column,
                )?,
                sql_identifier(field.name())
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    parse_generated_query(
        &format!(
            "SELECT {} FROM (VALUES {}) AS {}({})",
            select_items.join(", "),
            value_rows.join(", "),
            sql_identifier("__nr_values"),
            aliases.join(", ")
        ),
        "COW append branch",
    )
}

/// One rewrite branch's producer: every live row of the file it replaces, with
/// the matched rows carrying their after-image instead.
///
/// The scan is the branch's own frozen single-file source, joined to a literal
/// relation of the rows the statement matched inside it. A deleted row is
/// dropped by the trailing predicate; every other row is re-emitted so the
/// replacement file is complete.
#[allow(clippy::too_many_arguments)]
fn build_cow_rewrite_query(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    rows: &[novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal],
    input: &novarocks_spi::connector::ConnectorWriteInputShape,
    route: &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    source: &novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    identity: &FrozenConnectorScanIdentity,
) -> Result<novarocks_parser::ast::Query, String> {
    let contract = preparation.match_contract();
    let inputs = ordered_route_inputs(input, route)?;
    let scan_schema = source.scan_schema();
    let scan_by_token = source
        .scan_bindings()
        .iter()
        .map(|binding| (binding.token(), binding.scan_ordinal()))
        .collect::<HashMap<_, _>>();
    let field_by_token = inputs
        .iter()
        .map(|binding| (binding.token(), binding.field().clone()))
        .collect::<HashMap<_, _>>();
    // The literal relation carries the join key and, for every after-image
    // column, the value the matched row becomes.
    let mut values_tokens = source.match_tokens().to_vec();
    for binding in &inputs {
        if writer_field_is_after_image(contract, binding.field().name())
            && !values_tokens.contains(&binding.token())
        {
            values_tokens.push(binding.token());
        }
    }
    let marker_alias = "__nr_matched";
    let effect_alias = "__nr_effect";
    let value_alias = |ordinal: usize| format!("__nr_v_{ordinal}");
    let selection_field = |token: novarocks_spi::connector::ConnectorWriteFieldToken| {
        let field = field_by_token
            .get(&token)
            .ok_or_else(|| "COW rewrite token has no signed writer field".to_string())?;
        let ordinal = selection_ordinal_of_writer_field(contract, field.name())
            .ok_or_else(|| "COW rewrite field is absent from the signed selection".to_string())?;
        let selection_field = selection
            .schema()
            .fields()
            .get(ordinal as usize)
            .cloned()
            .ok_or_else(|| "COW selection field is out of bounds".to_string())?;
        Ok::<_, String>((ordinal, selection_field))
    };
    let mut value_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(values_tokens.len() + 2);
        for token in &values_tokens {
            let (ordinal, field) = selection_field(*token)?;
            values.push(selection_value_sql(selection, *row, ordinal, &field)?);
        }
        values.push("TRUE".to_string());
        values.push(selection_value_sql(
            selection,
            *row,
            contract.effect_field().target_ordinal(),
            contract.effect_field().field(),
        )?);
        value_rows.push(format!("({})", values.join(", ")));
    }
    let mut aliases = (0..values_tokens.len())
        .map(|ordinal| sql_identifier(&value_alias(ordinal)))
        .collect::<Vec<_>>();
    aliases.push(sql_identifier(marker_alias));
    aliases.push(sql_identifier(effect_alias));
    let values_position = values_tokens
        .iter()
        .enumerate()
        .map(|(ordinal, token)| (*token, ordinal))
        .collect::<HashMap<_, _>>();
    let matched = format!("{} IS NOT NULL", qualify_column("__nr_match", marker_alias));
    let scan_column = |token: novarocks_spi::connector::ConnectorWriteFieldToken| {
        let scan_ordinal = scan_by_token
            .get(&token)
            .copied()
            .ok_or_else(|| "COW rewrite field has no scan binding".to_string())?;
        let scan_field = scan_schema
            .fields()
            .get(scan_ordinal as usize)
            .ok_or_else(|| "COW scan binding is outside the frozen scan schema".to_string())?;
        Ok::<_, String>(qualify_column("__nr_scan", scan_field.name()))
    };
    let mut select_items = Vec::with_capacity(inputs.len());
    for binding in &inputs {
        let field = binding.field();
        let scan_value = scan_column(binding.token())?;
        let expression = if Some(binding.token()) == source.written_version_token() {
            let written_version = preparation.written_version_ordinal().ok_or_else(|| {
                "COW rewrite branch requires a signed written version".to_string()
            })?;
            format!("CASE WHEN {matched} THEN {written_version} ELSE {scan_value} END")
        } else if writer_field_is_after_image(contract, field.name()) {
            let position = values_position
                .get(&binding.token())
                .copied()
                .ok_or_else(|| "COW after-image field has no VALUES binding".to_string())?;
            format!(
                "CASE WHEN {matched} THEN {} ELSE {scan_value} END",
                qualify_column("__nr_match", &value_alias(position))
            )
        } else {
            scan_value
        };
        let column = novarocks_types::schema::ColumnDef {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        };
        select_items.push(format!(
            "{} AS {}",
            crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                &expression,
                &column
            )?,
            sql_identifier(field.name())
        ));
    }
    let joins = source
        .match_tokens()
        .iter()
        .map(|token| {
            let position = values_position
                .get(token)
                .copied()
                .ok_or_else(|| "COW match token has no VALUES binding".to_string())?;
            Ok(format!(
                "{} = {}",
                scan_column(*token)?,
                qualify_column("__nr_match", &value_alias(position))
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    if joins.is_empty() {
        return Err("COW rewrite branch carries no match key".to_string());
    }
    let scan = format!(
        "{}.{}.{} AS {}",
        sql_identifier(identity.catalog()),
        sql_identifier(identity.namespace()),
        sql_identifier(identity.table()),
        sql_identifier("__nr_scan")
    );
    let values = format!(
        "(VALUES {}) AS {}({})",
        value_rows.join(", "),
        sql_identifier("__nr_match"),
        aliases.join(", ")
    );
    parse_generated_query(
        &format!(
            "SELECT {} FROM {} LEFT JOIN {} ON {} WHERE {} IS NULL OR {} <> {}",
            select_items.join(", "),
            scan,
            values,
            joins.join(" AND "),
            qualify_column("__nr_match", effect_alias),
            qualify_column("__nr_match", effect_alias),
            novarocks_spi::connector::ConnectorRowMutationEffect::Delete as i8,
        ),
        "COW rewrite branch",
    )
}

struct DistributedCowUpdateExecutor {
    state: DmlExecutionKernel,
    target: crate::catalog_application::resolver::TargetBackend,
    write: Mutex<Option<CowUpdateDistributedWrite>>,
    /// The one commit authority of this statement. Every branch's query writes
    /// through it and it commits their union exactly once.
    write_session: Arc<ConnectorWriteSession>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl DistributedCowUpdateExecutor {
    fn run_stage(
        &self,
        native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
    ) -> Result<CowStagedWrite, crate::dml::error::DmlExecutionError> {
        let write = self
            .write
            .lock()
            .expect("COW write plan lock poisoned")
            .take()
            .ok_or_else(|| "COW write plan was already consumed".to_string())?;
        run_cow_target_writes(
            &self.state,
            &self.target,
            write,
            &self.execution,
            &self.connector_context,
            native_encoder,
        )
    }

    /// Release a session whose closed data plane produced no commit fragment.
    fn release_empty_write_session(&self) -> Result<(), String> {
        self.write_session
            .abort(self.connector_context.clone())
            .map(|_| ())
            .map_err(|error| format!("release empty COW write session: {error}"))
    }
}

impl MutationExecution for DistributedCowUpdateExecutor {
    fn stage(&self) -> Result<QueryExecutionResult, String> {
        Err("COW staging requires the Frontend native fragment encoder".to_string())
    }

    fn needs_abort_on_stage_error(&self) -> bool {
        true
    }

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        self.write_session
            .abort(self.connector_context.clone())
            .map_err(|error| format!("abort COW write session: {error}"))
    }

    fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        self.connector_context.clone()
    }

    fn finalize(&self) -> Result<(), String> {
        crate::catalog_application::resolver::invalidate_iceberg_caches(&self.state, &self.target)
    }
}

/// What one copy-on-write statement's whole data plane produced.
///
/// Emptiness is a statement-level fact, not a per-branch one: a branch whose
/// every matched row was deleted stages nothing and still has its file retired
/// by the commit. Only a statement that staged nothing at all has no snapshot
/// to publish.
struct CowStagedWrite {
    staged_any_artifact: bool,
}

/// Run every sealed branch's query against the one session, then stop.
///
/// Each branch is an ordinary distributed write compiled at its own sealed
/// ordinal: its writers are dataflow nodes, its rows gather into that query's
/// Root finish fragment, and its prepared write set is complete for its own
/// execution graph. The session accumulates them and commits their union once,
/// after every branch has closed.
fn run_cow_target_writes(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    write: CowUpdateDistributedWrite,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
) -> Result<CowStagedWrite, crate::dml::error::DmlExecutionError> {
    let CowUpdateDistributedWrite {
        targets,
        write_session,
        planning_lease,
    } = write;
    let mut staged_any_artifact = false;
    for plan in targets {
        let result = run_one_cow_target(
            state,
            target,
            plan,
            &planning_lease,
            &write_session,
            execution,
            connector_context,
            native_encoder,
        )?;
        let completion = result
            .write_session
            .ok_or_else(|| "COW branch closed without a write-session completion".to_string())?;
        staged_any_artifact |= !completion.is_empty();
        let (session, prepared) = completion.into_parts();
        if !Arc::ptr_eq(&session, &write_session) {
            return Err("COW branch committed through a substituted write session"
                .to_string()
                .into());
        }
        session
            .accumulate(prepared)
            .map_err(|error| format!("accumulate COW branch write set: {error}"))?;
    }
    Ok(CowStagedWrite {
        staged_any_artifact,
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "One copy-on-write branch requires separately validated execution, read, and session inputs."
)]
fn run_one_cow_target(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    plan: CowTargetWritePlan,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    write_session: &Arc<ConnectorWriteSession>,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
) -> Result<QueryExecutionResult, crate::dml::error::DmlExecutionError> {
    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let write_target = write_session
        .targets()
        .iter()
        .find(|candidate| candidate.ordinal() == plan.ordinal)
        .ok_or_else(|| {
            format!(
                "COW branch names write target {}, which this session never sealed",
                plan.ordinal.get()
            )
        })?;
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
    let sink_mode = match &plan.input {
        novarocks_spi::connector::ConnectorWriteInputShape::Data { .. } => DmlWriteSinkMode::Data,
        novarocks_spi::connector::ConnectorWriteInputShape::RowLineage { .. } => {
            DmlWriteSinkMode::RowLineageData
        }
        _ => {
            return Err("COW branch sealed an unsupported writer input shape"
                .to_string()
                .into());
        }
    };
    let sink = dml_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        sink_mode,
        novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
    )?;
    let assembly = match plan.frozen_read {
        Some(frozen) => {
            let binding =
                crate::query_execution::pinned_connector_read::admit_pinned_file_set_scan_binding(
                    table_bindings.as_ref(),
                    &frozen.identity,
                    &frozen.schema,
                )?;
            let overlay =
                crate::query_execution::pinned_connector_read::pinned_file_set_query_local_overlay(
                    &frozen.identity,
                    &frozen.schema,
                );
            let resolver =
                crate::query_execution::pinned_connector_read::PinnedFileSetReadResolver::new(
                    binding,
                    frozen.identity,
                    frozen.read,
                );
            crate::query_execution::compiler::prepare_query_as_iceberg_write_at_write_target(
                state,
                Some(&target.catalog),
                &target.namespace,
                &plan.query,
                sink,
                table_bindings,
                novarocks_sql::compiler::RootDistributionRequirement::Any,
                Some(execution),
                connector_context,
                Arc::clone(write_session),
                plan.ordinal,
                Some(&resolver),
                std::slice::from_ref(&overlay),
            )?
        }
        None => crate::query_execution::compiler::prepare_query_as_iceberg_write_at_write_target(
            state,
            Some(&target.catalog),
            &target.namespace,
            &plan.query,
            sink,
            table_bindings,
            novarocks_sql::compiler::RootDistributionRequirement::Any,
            Some(execution),
            connector_context,
            Arc::clone(write_session),
            plan.ordinal,
            None,
            &[],
        )?,
    };
    let native_bundle = native_encoder.encode(assembly.encoding())?;
    Ok(assembly.finish(native_bundle)?)
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
struct MatchedUpdateBatch {
    row_ids: Vec<i64>,
    file_paths: Vec<String>,
    row_positions: Vec<i64>,
    last_updated_sequences: Vec<Option<i64>>,
    /// Global match-row index to its non-concatenated Arrow batch/row.
    row_locations: Vec<(usize, usize)>,
    old_rows: Vec<RecordBatch>,
    new_rows: Vec<RecordBatch>,
}

/// Convert the already-matched UPDATE rows into the provider-signed COW
/// layout. This is deliberately token/ordinal driven after construction: the
/// generic validator checks the signed match contract before activation and
/// the Provider alone groups identities into cohort recipes.
#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn cow_selection_from_matched_update(
    matched: &MatchedUpdateBatch,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorRowMutationSelection, String> {
    cow_selection_from_matched_and_insert(matched, None, preparation, context)
}

/// Builds one bounded selection for a COW MERGE.  Insert rows intentionally
/// carry null target identity/before-image fields: the signed contract and
/// Provider both treat logical `Insert` as outside target-row uniqueness.
#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn cow_selection_from_matched_and_insert(
    matched: &MatchedUpdateBatch,
    insert_batch: Option<&RecordBatch>,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorRowMutationSelection, String> {
    use novarocks_spi::connector::ConnectorRowMutationEffect;

    let contract = preparation.match_contract();
    let mut collector =
        crate::query_execution::row_mutation::BoundedRowMutationMatchCollector::try_new(
            context, None,
        )
        .map_err(|error| format!("create bounded COW match collector: {error}"))?;
    for (batch_index, (old_rows, new_rows)) in
        matched.old_rows.iter().zip(&matched.new_rows).enumerate()
    {
        let global_rows = matched
            .row_locations
            .iter()
            .enumerate()
            .filter_map(|(global, (part, _))| (*part == batch_index).then_some(global))
            .collect::<Vec<_>>();
        let mut fields = Vec::new();
        let mut columns = Vec::<ArrayRef>::new();
        for identity in contract.identity_fields() {
            fields.push(Arc::new(identity.field().clone()));
            let column: ArrayRef = match identity.field().name().as_str() {
                "_file" => Arc::new(StringArray::from(
                    global_rows
                        .iter()
                        .map(|row| matched.file_paths[*row].clone())
                        .collect::<Vec<_>>(),
                )),
                "_pos" => Arc::new(Int64Array::from(
                    global_rows
                        .iter()
                        .map(|row| matched.row_positions[*row])
                        .collect::<Vec<_>>(),
                )),
                "_row_id" => Arc::new(Int64Array::from(
                    global_rows
                        .iter()
                        .map(|row| matched.row_ids[*row])
                        .collect::<Vec<_>>(),
                )),
                "_last_updated_sequence_number" => Arc::new(Int64Array::from(
                    global_rows
                        .iter()
                        .map(|row| matched.last_updated_sequences[*row])
                        .collect::<Vec<_>>(),
                )),
                other => {
                    return Err(format!(
                        "provider match contract requested an unsupported COW identity field `{other}`"
                    ));
                }
            };
            columns.push(column);
        }
        for field in contract.before_fields() {
            fields.push(Arc::new(field.field().clone()));
            let ordinal = old_rows
                .schema()
                .index_of(field.field().name())
                .map_err(|_| {
                    format!(
                        "provider COW before-image field `{}` is absent from the matched result",
                        field.field().name()
                    )
                })?;
            columns.push(
                novarocks_execution::exec::expr::cast_array_to_target(
                    old_rows.column(ordinal),
                    field.field().data_type(),
                )
                .map_err(|error| {
                    format!(
                        "cast provider COW before-image field `{}` to its sealed contract: {error}",
                        field.field().name()
                    )
                })?,
            );
        }
        for field in contract.after_fields() {
            fields.push(Arc::new(field.field().clone()));
            let ordinal = new_rows
                .schema()
                .index_of(field.field().name())
                .map_err(|_| {
                    format!(
                        "provider COW after-image field `{}` is absent from the matched result",
                        field.field().name()
                    )
                })?;
            columns.push(
                novarocks_execution::exec::expr::cast_array_to_target(
                    new_rows.column(ordinal),
                    field.field().data_type(),
                )
                .map_err(|error| {
                    format!(
                        "cast provider COW after-image field `{}` to its sealed contract: {error}",
                        field.field().name()
                    )
                })?,
            );
        }
        fields.push(Arc::new(contract.effect_field().field().clone()));
        columns.push(Arc::new(Int8Array::from(vec![
            ConnectorRowMutationEffect::Replace
                as i8;
            global_rows.len()
        ])));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| format!("assemble provider COW match selection: {error}"))?;
        collector
            .push(batch)
            .map_err(|error| format!("collect bounded COW match batch: {error}"))?;
    }
    if let Some(insert_batch) = insert_batch.filter(|batch| batch.num_rows() > 0) {
        let mut fields = Vec::new();
        let mut columns = Vec::<ArrayRef>::new();
        for identity in contract.identity_fields() {
            fields.push(Arc::new(identity.field().clone()));
            columns.push(arrow::array::new_null_array(
                identity.field().data_type(),
                insert_batch.num_rows(),
            ));
        }
        for field in contract.before_fields() {
            fields.push(Arc::new(field.field().clone()));
            columns.push(arrow::array::new_null_array(
                field.field().data_type(),
                insert_batch.num_rows(),
            ));
        }
        for field in contract.after_fields() {
            fields.push(Arc::new(field.field().clone()));
            let ordinal = insert_batch
                .schema()
                .index_of(field.field().name())
                .map_err(|_| {
                    format!(
                        "provider COW after-image field `{}` is absent from the MERGE INSERT result",
                        field.field().name()
                    )
                })?;
            columns.push(
                novarocks_execution::exec::expr::cast_array_to_target(
                    insert_batch.column(ordinal),
                    field.field().data_type(),
                )
                .map_err(|error| {
                    format!(
                        "cast provider COW MERGE INSERT field `{}` to its sealed contract: {error}",
                        field.field().name()
                    )
                })?,
            );
        }
        fields.push(Arc::new(contract.effect_field().field().clone()));
        columns.push(Arc::new(Int8Array::from(vec![
            ConnectorRowMutationEffect::Insert
                as i8;
            insert_batch.num_rows()
        ])));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| format!("assemble provider COW INSERT selection: {error}"))?;
        collector
            .push(batch)
            .map_err(|error| format!("collect bounded COW INSERT batch: {error}"))?;
    }
    let selection = collector
        .finish()
        .map_err(|error| format!("finish bounded COW match collection: {error}"))?;
    let mut validator = crate::query_execution::row_mutation::RowMutationMatchValidator::try_new(
        contract.clone(),
        preparation.intent().clone(),
    )
    .map_err(|error| format!("initialize COW match contract validator: {error}"))?;
    validator
        .validate_selection(&selection)
        .map_err(|error| format!("validate COW match contract: {error}"))?;
    Ok(selection)
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn execute_update_match_query(
    state: &DmlExecutionKernel,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MatchedUpdateBatch, String> {
    let _ = (
        state,
        current_catalog,
        sql,
        current_database,
        execution,
        connector_context,
    );
    Err(
        "test-only UPDATE match materialization requires an explicit query preparation kernel"
            .to_string(),
    )
}

/// Execute a COW match query whose target is pinned to the exact snapshot the
/// commit will be validated against. The pin is expressed by the generated
/// statement itself (see [`exact_cow_match_target_relation_sql`]), so the
/// target resolves through the ordinary admitted frozen-snapshot scan lane and
/// cannot observe a later ref head. Other statement sources still resolve
/// normally.
fn execute_exact_cow_match_query(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    query: &novarocks_parser::ast::Query,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
) -> Result<QueryResult, crate::dml::error::DmlExecutionError> {
    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let catalog_service_snapshot =
        crate::catalog_application::query_catalog::catalog_service_snapshot(state);
    let analyzer_catalog =
        crate::catalog_application::query_materializer::build_catalog_service_provider_with_bindings_and_query_local_overlays(
            Some(&target.catalog),
            &catalog_service_snapshot,
            state.connector_control().as_ref(),
            connector_context.clone(),
            Arc::clone(&table_bindings),
            Vec::new(),
            state.catalog_application().map(Arc::as_ref),
        );
    let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_catalog);
    let request = novarocks_sql::compiler::SqlAnalyzeRequest::new(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query.clone())),
        novarocks_sql::compiler::SqlCompileIntent::Query,
        novarocks_sql::compiler::SqlSessionContext {
            current_catalog: Some(target.catalog.clone()),
            current_database: target.namespace.clone(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        novarocks_sql::compiler::SqlPlanningEnvironment::Distributed,
        &catalog,
        state.function_catalog().as_ref(),
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        novarocks_sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::query_execution::planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(request)
        .map_err(crate::dml::error::DmlExecutionError::from_compile)?
        .into_pending()
        .map_err(|error| error.to_string())?;
    let statistics =
        crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
            state,
            Arc::clone(&table_bindings),
            connector_context,
        )?;
    let distributed = novarocks_sql::planning::dml::compile_query_distributed_plan(
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics),
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        state.connector_control().as_ref(),
        connector_context,
        Some(table_bindings.as_ref()),
        None,
        crate::query_execution::dml::write::scan_preparation_options(
            state.typed_connector_control(),
            execution.optimizer_settings(),
        )?,
    )?;
    let encoding = crate::query_execution::compiler::NativeFragmentEncodingInput::new(prepared);
    let native_bundle = native_encoder.encode(&encoding)?;
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        encoding,
        native_bundle,
        None,
        crate::query_execution::contract::DistributedQueryIntent::Result,
        execution,
    )
    .map_err(|error| error.to_string())?;
    Ok(state
        .query_execution()
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_result)
        .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
        .map_err(|error| error.to_string())?)
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn matched_update_batch_from_query_result(
    result: QueryResult,
) -> Result<MatchedUpdateBatch, String> {
    let mut merged = empty_matched_update_batch()?;
    for chunk in result.chunks {
        merged.append(matched_update_batch_from_record_batch(&chunk.batch)?);
    }
    Ok(merged)
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn matched_update_batch_from_record_batch(
    batch: &RecordBatch,
) -> Result<MatchedUpdateBatch, String> {
    if batch.num_rows() == 0 {
        return empty_matched_update_batch();
    }

    let file_col = cast(required_column(batch, "__nr_file")?, &DataType::Utf8)
        .map_err(|e| format!("cast __nr_file to Utf8 failed: {e}"))?;
    let pos_col = cast(required_column(batch, "__nr_pos")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_pos to Int64 failed: {e}"))?;
    let row_id_col = cast(required_column(batch, "__nr_row_id")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_row_id to Int64 failed: {e}"))?;
    let file_arr = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "__nr_file was not Utf8 after cast".to_string())?;
    let pos_arr = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_pos was not Int64 after cast".to_string())?;
    let row_id_arr = row_id_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_row_id was not Int64 after cast".to_string())?;
    let last_updated_col = cast(
        required_column(batch, "__nr_last_updated_sequence_number")?,
        &DataType::Int64,
    )
    .map_err(|e| format!("cast __nr_last_updated_sequence_number to Int64 failed: {e}"))?;
    let last_updated_arr = last_updated_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_last_updated_sequence_number was not Int64 after cast".to_string())?;

    let mut file_paths = Vec::with_capacity(batch.num_rows());
    let mut row_positions = Vec::with_capacity(batch.num_rows());
    let mut row_ids = Vec::with_capacity(batch.num_rows());
    let mut last_updated_sequences = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if file_arr.is_null(row) || pos_arr.is_null(row) || row_id_arr.is_null(row) {
            return Err("UPDATE match query produced null row identity columns".to_string());
        }
        file_paths.push(file_arr.value(row).to_string());
        row_positions.push(pos_arr.value(row));
        row_ids.push(row_id_arr.value(row));
        last_updated_sequences
            .push((!last_updated_arr.is_null(row)).then(|| last_updated_arr.value(row)));
    }

    let old_indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.name().starts_with("__nr_"))
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();
    let old_fields = old_indices
        .iter()
        .map(|idx| batch.schema().field(*idx).clone())
        .collect::<Vec<_>>();
    let old_schema = Arc::new(Schema::new(old_fields));
    let old_columns = old_indices
        .iter()
        .map(|idx| batch.column(*idx).clone())
        .collect::<Vec<_>>();
    let old_rows = RecordBatch::try_new(old_schema.clone(), old_columns)
        .map_err(|e| format!("build UPDATE old-row batch failed: {e}"))?;

    let mut new_columns = Vec::with_capacity(old_schema.fields().len());
    for (old_idx, field) in old_indices.iter().zip(old_schema.fields().iter()) {
        let new_name = format!("__nr_new_{}", field.name());
        let column = match batch.schema().index_of(&new_name) {
            Ok(idx) => cast(batch.column(idx), field.data_type()).map_err(|e| {
                format!(
                    "cast UPDATE assignment column `{new_name}` to {:?} failed: {e}",
                    field.data_type()
                )
            })?,
            Err(_) => batch.column(*old_idx).clone(),
        };
        new_columns.push(column);
    }
    let new_rows = RecordBatch::try_new(old_schema, new_columns)
        .map_err(|e| format!("build UPDATE new-row batch failed: {e}"))?;

    Ok(MatchedUpdateBatch {
        row_ids,
        file_paths,
        row_positions,
        last_updated_sequences,
        row_locations: (0..batch.num_rows()).map(|row| (0, row)).collect(),
        old_rows: vec![old_rows],
        new_rows: vec![new_rows],
    })
}

#[cfg(test)]
impl MatchedUpdateBatch {
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    fn append(&mut self, mut next: Self) {
        let batch_offset = self.new_rows.len();
        self.row_ids.append(&mut next.row_ids);
        self.file_paths.append(&mut next.file_paths);
        self.row_positions.append(&mut next.row_positions);
        self.last_updated_sequences
            .append(&mut next.last_updated_sequences);
        self.row_locations.extend(
            next.row_locations
                .drain(..)
                .map(|(batch, row)| (batch + batch_offset, row)),
        );
        self.old_rows.append(&mut next.old_rows);
        self.new_rows.append(&mut next.new_rows);
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn empty_matched_update_batch() -> Result<MatchedUpdateBatch, String> {
    Ok(MatchedUpdateBatch {
        row_ids: Vec::new(),
        file_paths: Vec::new(),
        row_positions: Vec::new(),
        last_updated_sequences: Vec::new(),
        row_locations: Vec::new(),
        old_rows: Vec::new(),
        new_rows: Vec::new(),
    })
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| format!("UPDATE match query missing `{name}` column"))?;
    Ok(batch.column(idx))
}

fn validate_update_assignments(
    assignments: &[PreparedMutationAssignment],
    target_columns: &[novarocks_types::schema::ColumnDef],
    partition_columns: &[String],
) -> Result<(), String> {
    let target_names = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let partition_names = partition_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
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
        if partition_names.contains(&name) {
            return Err(format!(
                "UPDATE cannot modify Iceberg partition column `{}` in the first implementation",
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

fn build_update_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: Option<&str>,
    assignments_sql: &[(&str, &str)],
    where_sql: Option<&str>,
) -> String {
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            column.to_string()
        } else {
            format!("{target_alias}.{column}")
        }
    };
    let star = if target_alias.is_empty() {
        "*".to_string()
    } else {
        format!("{target_alias}.*")
    };
    let mut select_items = vec![
        format!("{} AS __nr_file", qualify("_file")),
        format!("{} AS __nr_pos", qualify("_pos")),
        format!("{} AS __nr_row_id", qualify("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            qualify("_last_updated_sequence_number")
        ),
        star,
    ];
    for (column, expr) in assignments_sql {
        select_items.push(format!("{expr} AS __nr_new_{column}"));
    }
    let mut sql = format!("SELECT {} FROM {target_sql}", select_items.join(", "));
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(pred) = where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(pred);
    }
    sql
}

/// Name the mutation target as the relation frozen at the exact snapshot the
/// provider signed as this statement's base version.
///
/// The COW match must read that snapshot and no other. The ordinary relation
/// name means "current", which for a branch mutation is a different ref
/// entirely, and even on the same ref would let a write that landed after
/// admission enter the match and be silently overwritten by the rewrite. The
/// query-local snapshot identity states the pin in the statement itself, so the
/// target resolves through the admitted frozen-snapshot scan lane that time
/// travel already uses.
fn exact_cow_match_target_relation_sql(
    target: &crate::catalog_application::resolver::TargetBackend,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Result<String, String> {
    let base_snapshot_id = preparation.base_version_ordinal().ok_or_else(|| {
        format!(
            "COW match target `{}`.`{}`.`{}` has no provider-signed base version ordinal; \
             reading its current snapshot instead would silently drop a concurrent write",
            target.catalog, target.namespace, target.table
        )
    })?;
    Ok(format!(
        "{}.{}.{}",
        sql_identifier(&target.catalog),
        sql_identifier(&target.namespace),
        sql_identifier(
            &crate::catalog_application::query_bindings::time_travel_overlay_identity(
                &target.table,
                base_snapshot_id,
            )
        ),
    ))
}

fn build_exact_cow_update_selection_query(
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedUpdateStatement,
    source_sql: Option<&str>,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Result<novarocks_parser::ast::Query, String> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    let qualify = |name: &str| format!("{}.{}", sql_identifier(target_alias), sql_identifier(name));
    let assignments = stmt
        .assignments
        .iter()
        .map(|assignment| {
            (
                assignment.column.to_ascii_lowercase(),
                assignment.value_sql.clone(),
            )
        })
        .collect::<HashMap<_, _>>();
    let (_, roles) = cow_selection_layout(preparation)?;
    let select_items = roles
        .iter()
        .enumerate()
        .map(|(ordinal, role)| {
            let expression = match role {
                CowSelectionFieldRole::Identity(field) => qualify(field.field().name()),
                CowSelectionFieldRole::Before(field) => qualify(field.field().name()),
                CowSelectionFieldRole::After(field) => assignments
                    .get(&field.field().name().to_ascii_lowercase())
                    .cloned()
                    .unwrap_or_else(|| qualify(field.field().name())),
                CowSelectionFieldRole::Effect(_) => {
                    (novarocks_spi::connector::ConnectorRowMutationEffect::Replace as i8)
                        .to_string()
                }
            };
            format!(
                "({expression}) AS {}",
                sql_identifier(&format!("__nr_sel_{ordinal}"))
            )
        })
        .collect::<Vec<_>>();
    let mut sql = format!(
        "SELECT {} FROM {} AS {}",
        select_items.join(", "),
        exact_cow_match_target_relation_sql(target, preparation)?,
        sql_identifier(target_alias),
    );
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(predicate) = &stmt.where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(predicate);
    }
    parse_generated_query(&sql, "exact COW UPDATE selection")
}

// ---------------------------------------------------------------------------
// MERGE INTO
// ---------------------------------------------------------------------------

const MERGE_TARGET_DEFAULT_ALIAS: &str = "__nr_t";
const MERGE_SOURCE_DEFAULT_ALIAS: &str = "__nr_s";
const MERGE_ACTION_MATCHED_UPDATE: i32 = 1;
const MERGE_ACTION_MATCHED_DELETE: i32 = 2;
const MERGE_ACTION_NOT_MATCHED_INSERT: i32 = 3;

/// Stage the COW half of a prepared MERGE after frontend durable intent.  The
/// COW rewrite and optional append share one sealed connector operation and
/// therefore one aggregate commit handle/snapshot.
pub(crate) fn stage_prepared_merge_mutation(
    state: &DmlExecutionKernel,
    prepared: PreparedMergeMutation,
    native_encoder: &dyn crate::query_execution::dml::mutation::MutationNativeFragmentEncoder,
) -> Result<MutationStagedWrite, crate::dml::error::DmlExecutionError> {
    let PreparedMergeMutation {
        stmt,
        current_catalog,
        target,
        target_columns,
        target_ref,
        table_write_mode,
        planning_lease,
        write_lease,
        cow_preparations,
        mor_write_target,
        insert_columns_resolved,
        admitted_base_snapshot_id: _,
        execution,
        connector_context,
    } = prepared;
    let has_matched_update = matches!(
        stmt.matched.as_ref().map(|clause| &clause.action),
        Some(PreparedMergeMatchedAction::Update { .. })
    );
    let has_matched_delete = matches!(
        stmt.matched.as_ref().map(|clause| &clause.action),
        Some(PreparedMergeMatchedAction::Delete)
    );
    let has_not_matched_insert = stmt.not_matched.is_some();
    if table_write_mode == novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead
        || has_matched_delete
    {
        if !has_matched_update && !has_matched_delete && !has_not_matched_insert {
            return Ok(MutationStagedWrite::NoOp);
        }
        let PreparedMorMergeWriteTarget {
            preparations,
            planning_lease: write_planning_lease,
        } = mor_write_target.ok_or_else(|| {
            "MOR MERGE reached stage without an admitted frozen write target".to_string()
        })?;
        // See the MOR UPDATE path: the written version is signed at admission.
        let written_version = preparations
            .preparation
            .written_version_ordinal()
            .ok_or_else(|| "MOR MERGE requires a provider-signed written version".to_string())?;
        // The write lease was derived once at preparation so the coordinator
        // could fence it before dispatch; re-deriving here would mint a fresh
        // fence cell and silently discard that fence.
        let write_session = begin_mor_change_stream_write_session(
            state,
            &target,
            &target_ref,
            &target_columns,
            &write_lease,
            &write_planning_lease,
            &connector_context,
        )?;
        let planned = match build_merge_mor_change_stream_write_plan(
            state,
            &target,
            &stmt,
            current_catalog.as_deref(),
            &target_columns,
            insert_columns_resolved.as_deref(),
            &target_ref,
            written_version,
            &execution,
            &connector_context,
            &write_session,
            write_planning_lease,
        ) {
            Ok(planned) => planned,
            Err(error) => {
                release_unplanned_write_session(&write_session, &connector_context, "MOR MERGE");
                return Err(error);
            }
        };
        let execution_handle = Arc::new(MorMergeChangeStreamExecutor {
            state: state.clone(),
            target: target.clone(),
            planned: Mutex::new(Some(planned)),
            execution,
            connector_context,
            write_session,
        });
        let result = match execution_handle.run_stage(native_encoder) {
            Ok(result) => result,
            Err(reason) => {
                if execution_handle.needs_abort_on_stage_error() {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason,
                        execution: execution_handle,
                    });
                }
                return Err(reason.into());
            }
        };
        let Some(completion) = result.write_session else {
            return Ok(MutationStagedWrite::AbortRequired {
                reason: "MOR MERGE staged without a write-session completion".to_string(),
                execution: execution_handle,
            });
        };
        // See the MOR UPDATE path: a closed data plane that produced no commit
        // fragment has nothing to publish, so the session is released rather
        // than committed.
        if completion.is_empty() {
            if let Err(reason) = execution_handle.release_empty_write_session(&completion) {
                return Ok(MutationStagedWrite::AbortRequired {
                    reason,
                    execution: execution_handle,
                });
            }
            return Ok(MutationStagedWrite::NoOp);
        }
        return Ok(MutationStagedWrite::CommitRequired {
            execution: execution_handle,
            completion: MutationCommitCompletion::Session(completion),
        });
    }
    let cow_preparations = cow_preparations.ok_or_else(|| {
        "COW MERGE reached stage without its signed row-mutation preparation".to_string()
    })?;
    let query = build_exact_cow_merge_selection_query(
        state,
        &target,
        &stmt,
        current_catalog.as_deref(),
        insert_columns_resolved.as_deref(),
        &cow_preparations.preparation,
    )?;
    let matched = execute_exact_cow_match_query(
        state,
        &target,
        &query,
        &execution,
        &connector_context,
        native_encoder,
    )?;
    let selection = cow_selection_from_query_result(
        matched,
        &cow_preparations.preparation,
        connector_context.clone(),
    )?;
    if selection.row_count() == 0 {
        return Ok(MutationStagedWrite::NoOp);
    }
    // See the COW UPDATE path: the session is opened only after the match query
    // has run, because the provider seals one branch per rewritten file.
    let write_session = begin_cow_write_session(
        state,
        &target,
        &target_ref,
        &cow_preparations.preparation,
        selection.clone(),
        &write_lease,
        &planning_lease,
        &connector_context,
    )?;
    let write = match build_cow_update_distributed_write(
        &target,
        planning_lease,
        &cow_preparations.preparation,
        &selection,
        Arc::clone(&write_session),
    ) {
        Ok(write) => write,
        Err(error) => {
            release_unplanned_write_session(&write_session, &connector_context, "COW MERGE");
            return Err(error.into());
        }
    };
    let execution_handle = Arc::new(DistributedCowUpdateExecutor {
        state: state.clone(),
        target: target.clone(),
        write: Mutex::new(Some(write)),
        write_session: Arc::clone(&write_session),
        execution,
        connector_context,
    });
    let staged = match execution_handle.run_stage(native_encoder) {
        Ok(staged) => staged,
        Err(error @ crate::dml::error::DmlExecutionError::Analyze(_)) => {
            return Err(error);
        }
        Err(error) => {
            return Ok(MutationStagedWrite::AbortRequired {
                reason: error.to_string(),
                execution: execution_handle,
            });
        }
    };
    if !staged.staged_any_artifact {
        if let Err(reason) = execution_handle.release_empty_write_session() {
            return Ok(MutationStagedWrite::AbortRequired {
                reason,
                execution: execution_handle,
            });
        }
        return Ok(MutationStagedWrite::NoOp);
    }
    Ok(MutationStagedWrite::CommitRequired {
        execution: execution_handle,
        completion: MutationCommitCompletion::AccumulatedSession(write_session),
    })
}
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
        if action.values_sql.len() != target_columns.len() {
            return Err(format!(
                "MERGE WHEN NOT MATCHED INSERT VALUES count {} does not match target column count {}",
                action.values_sql.len(),
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

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
struct MergeMatchRows {
    /// The full RecordBatch from the MERGE match SELECT, with rows for both
    /// matched and unmatched cases. Filters for each side are derived from
    /// `__nr_match_kind` / `__nr_matched_apply` / `__nr_unmatched_apply`.
    full: RecordBatch,
}

#[cfg(test)]
impl MergeMatchRows {
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    fn empty() -> Self {
        Self {
            full: RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    fn matched_batch(&self) -> Result<RecordBatch, String> {
        if self.full.num_rows() == 0 {
            return Ok(self.full.clone());
        }
        let filter = self.row_filter("matched", "__nr_matched_apply")?;
        filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE matched rows failed: {e}"))
    }

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    fn unmatched_insert_batch(
        &self,
        target_columns: &[novarocks_types::schema::ColumnDef],
        insert_columns: &MergeInsertColumns,
    ) -> Result<RecordBatch, String> {
        let target_arrow_schema = arrow::datatypes::Schema::new(
            target_columns
                .iter()
                .map(|c| {
                    arrow::datatypes::Field::new(c.name.clone(), c.data_type.clone(), c.nullable)
                })
                .collect::<Vec<_>>(),
        );
        let target_arrow_schema = Arc::new(target_arrow_schema);
        if self.full.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(target_arrow_schema));
        }
        let filter = self.row_filter("unmatched", "__nr_unmatched_apply")?;
        let filtered = filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE unmatched rows failed: {e}"))?;
        if filtered.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(target_arrow_schema));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_columns.len());
        for (target_col, insert_entry) in target_columns.iter().zip(insert_columns.iter()) {
            debug_assert_eq!(target_col.name, insert_entry.name);
            let column = match insert_entry.value_index {
                Some(_) => {
                    let projected_name = format!("__nr_ins_{}", target_col.name);
                    let idx = filtered.schema().index_of(&projected_name).map_err(|_| {
                        format!("MERGE INSERT projection missing column `{projected_name}`")
                    })?;
                    cast(filtered.column(idx), &target_col.data_type).map_err(|e| {
                        format!(
                            "cast MERGE INSERT column `{}` to {:?} failed: {e}",
                            target_col.name, target_col.data_type
                        )
                    })?
                }
                None => arrow::array::new_null_array(&target_col.data_type, filtered.num_rows()),
            };
            columns.push(column);
        }
        RecordBatch::try_new(target_arrow_schema, columns)
            .map_err(|e| format!("build MERGE INSERT batch failed: {e}"))
    }

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    fn row_filter(&self, kind: &str, apply_col: &str) -> Result<BooleanArray, String> {
        let kind_col = cast(
            required_column(&self.full, "__nr_match_kind")?,
            &DataType::Utf8,
        )
        .map_err(|e| format!("cast __nr_match_kind to Utf8 failed: {e}"))?;
        let kind_arr = kind_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "__nr_match_kind was not Utf8 after cast".to_string())?;
        let apply_col = cast(required_column(&self.full, apply_col)?, &DataType::Boolean)
            .map_err(|e| format!("cast {apply_col} to Boolean failed: {e}"))?;
        let apply_arr = apply_col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "MERGE apply column was not Boolean after cast".to_string())?;

        let mut bits = Vec::with_capacity(self.full.num_rows());
        for row in 0..self.full.num_rows() {
            if kind_arr.is_null(row) {
                bits.push(false);
                continue;
            }
            let matches_kind = kind_arr.value(row) == kind;
            let applies = !apply_arr.is_null(row) && apply_arr.value(row);
            bits.push(matches_kind && applies);
        }
        Ok(BooleanArray::from(bits))
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
#[expect(
    clippy::too_many_arguments,
    reason = "MERGE match materialization retains its independently validated schema and routing inputs."
)]
fn materialize_merge_match(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedMergeStatement,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_types::schema::ColumnDef],
    insert_columns: Option<&[MergeInsertColumn]>,
    _target_ref: &str,
    _match_target_schema: &arrow::datatypes::SchemaRef,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MergeMatchRows, String> {
    let target_alias = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| MERGE_TARGET_DEFAULT_ALIAS.to_string());
    let target_sql = format!("{} AS {}", target.table, target_alias);

    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    // `mutation_source_to_sql` preserves the user-provided alias when present.
    // When the source carries no alias, inject `__nr_s` so the projection /
    // ON predicate can reference source columns deterministically.
    let source_sql = match &stmt.source {
        PreparedMutationSource::Table { alias, .. }
        | PreparedMutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };

    let on_sql = stmt.on_sql.as_str();
    let matched_predicate_sql = stmt
        .matched
        .as_ref()
        .and_then(|c| c.predicate_sql.as_deref());
    let not_matched_predicate_sql = stmt
        .not_matched
        .as_ref()
        .and_then(|c| c.predicate_sql.as_deref());

    let matched_assignments_sql = match stmt.matched.as_ref().map(|c| &c.action) {
        Some(PreparedMergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|a| {
                let target_column = target_columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&a.column))
                    .ok_or_else(|| {
                        format!(
                            "MERGE UPDATE assignment references unknown target column `{}`",
                            a.column
                        )
                    })?;
                Ok((
                    target_column.name.clone(),
                    crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                        &format!("({})", a.value_sql),
                        target_column,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?,
        _ => Vec::new(),
    };
    let matched_assignments_sql_borrow: Vec<(&str, &str)> = matched_assignments_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();

    let insert_values_sql: Vec<(String, String)> =
        match (insert_columns, stmt.not_matched.as_ref().map(|c| &c.action)) {
            (Some(cols), Some(action)) => cols
                .iter()
                .filter_map(|col| {
                    col.value_index.map(|idx| {
                        let target_column = target_columns
                            .iter()
                            .find(|target_column| {
                                target_column.name.eq_ignore_ascii_case(&col.name)
                            })
                            .expect("resolved MERGE INSERT column exists in target columns");
                        Ok((
                            col.name.clone(),
                            crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                                &format!("({})", action.values_sql[idx]),
                                target_column,
                            )?,
                        ))
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            _ => Vec::new(),
        };
    let insert_values_sql_borrow: Vec<(&str, &str)> = insert_values_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();

    let sql = build_merge_match_query_sql(
        &target_sql,
        &target_alias,
        &source_sql,
        on_sql,
        matched_predicate_sql,
        not_matched_predicate_sql,
        target_columns,
        &matched_assignments_sql_borrow,
        &insert_values_sql_borrow,
        stmt.matched.as_ref().map(|clause| match clause.action {
            PreparedMergeMatchedAction::Update { .. } => MERGE_ACTION_MATCHED_UPDATE,
            PreparedMergeMatchedAction::Delete => MERGE_ACTION_MATCHED_DELETE,
        }),
        stmt.not_matched.is_some(),
    );

    let result = execute_merge_match_query(
        state,
        Some(&target.catalog),
        &sql,
        &target.namespace,
        execution,
        connector_context,
    )?;
    Ok(result)
}

fn build_exact_cow_merge_selection_query(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedMergeStatement,
    current_catalog: Option<&str>,
    insert_columns: Option<&[MergeInsertColumn]>,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Result<novarocks_parser::ast::Query, String> {
    let target_alias = stmt
        .target_alias
        .as_deref()
        .unwrap_or(MERGE_TARGET_DEFAULT_ALIAS);
    let qualify = |name: &str| format!("{}.{}", sql_identifier(target_alias), sql_identifier(name));
    let identity = preparation
        .match_contract()
        .identity_fields()
        .first()
        .ok_or_else(|| "COW MERGE match contract has no identity field".to_string())?;
    let matched = format!("{} IS NOT NULL", qualify(identity.field().name()));
    let matched_predicate = stmt
        .matched
        .as_ref()
        .and_then(|clause| clause.predicate_sql.clone())
        .unwrap_or_else(|| "TRUE".to_string());
    let insert_predicate = stmt
        .not_matched
        .as_ref()
        .and_then(|clause| clause.predicate_sql.clone())
        .unwrap_or_else(|| "TRUE".to_string());
    let assignments = match stmt.matched.as_ref().map(|clause| &clause.action) {
        Some(PreparedMergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|assignment| {
                (
                    assignment.column.to_ascii_lowercase(),
                    assignment.value_sql.clone(),
                )
            })
            .collect::<HashMap<_, _>>(),
        _ => HashMap::new(),
    };
    let insert_values = match (insert_columns, stmt.not_matched.as_ref()) {
        (Some(columns), Some(clause)) => columns
            .iter()
            .filter_map(|column| {
                column.value_index.map(|index| {
                    (
                        column.name.to_ascii_lowercase(),
                        clause.action.values_sql[index].clone(),
                    )
                })
            })
            .collect::<HashMap<_, _>>(),
        _ => HashMap::new(),
    };
    let (_, roles) = cow_selection_layout(preparation)?;
    let select_items = roles
        .iter()
        .enumerate()
        .map(|(ordinal, role)| {
            let expression = match role {
                CowSelectionFieldRole::Identity(field) => {
                    format!(
                        "CASE WHEN {matched} THEN {} ELSE NULL END",
                        qualify(field.field().name())
                    )
                }
                CowSelectionFieldRole::Before(field) => {
                    format!(
                        "CASE WHEN {matched} THEN {} ELSE NULL END",
                        qualify(field.field().name())
                    )
                }
                CowSelectionFieldRole::After(field) => {
                    let matched_value = assignments
                        .get(&field.field().name().to_ascii_lowercase())
                        .cloned()
                        .unwrap_or_else(|| qualify(field.field().name()));
                    let inserted_value = insert_values
                        .get(&field.field().name().to_ascii_lowercase())
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string());
                    format!(
                        "CASE WHEN {matched} THEN ({matched_value}) ELSE ({inserted_value}) END"
                    )
                }
                CowSelectionFieldRole::Effect(_) => format!(
                    "CASE WHEN {matched} THEN {} ELSE {} END",
                    novarocks_spi::connector::ConnectorRowMutationEffect::Replace as i8,
                    novarocks_spi::connector::ConnectorRowMutationEffect::Insert as i8,
                ),
            };
            format!(
                "({expression}) AS {}",
                sql_identifier(&format!("__nr_sel_{ordinal}"))
            )
        })
        .collect::<Vec<_>>();
    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    let source_sql = match &stmt.source {
        PreparedMutationSource::Table { alias, .. }
        | PreparedMutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!(
                    "{source_table_sql} AS {}",
                    sql_identifier(MERGE_SOURCE_DEFAULT_ALIAS)
                )
            }
        }
    };
    let mut admitted_actions = Vec::new();
    if matches!(
        stmt.matched.as_ref().map(|clause| &clause.action),
        Some(PreparedMergeMatchedAction::Update { .. })
    ) {
        admitted_actions.push(format!("({matched} AND ({matched_predicate}))"));
    }
    if stmt.not_matched.is_some() {
        admitted_actions.push(format!("(NOT ({matched}) AND ({insert_predicate}))"));
    }
    if admitted_actions.is_empty() {
        return Err("COW MERGE has no Replace or Insert action".to_string());
    }
    parse_generated_query(
        &format!(
            "SELECT {} FROM {} LEFT JOIN {} AS {} ON {} WHERE {}",
            select_items.join(", "),
            source_sql,
            exact_cow_match_target_relation_sql(target, preparation)?,
            sql_identifier(target_alias),
            stmt.on_sql,
            admitted_actions.join(" OR "),
        ),
        "exact COW MERGE selection",
    )
}

#[allow(clippy::too_many_arguments)]
fn build_merge_mor_change_stream_write_plan(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedMergeStatement,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_types::schema::ColumnDef],
    insert_columns: Option<&[MergeInsertColumn]>,
    target_ref: &str,
    new_sequence_number: i64,
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: &ConnectorWriteSession,
    write_planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<
    crate::query_execution::compiler::PlannedIcebergChangeStreamWrite,
    crate::dml::error::DmlExecutionError,
> {
    let target_alias = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| MERGE_TARGET_DEFAULT_ALIAS.to_string());
    let target_sql = update_change_stream_target_sql(target, &target_alias, target_ref);
    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    let source_sql = match &stmt.source {
        PreparedMutationSource::Table { alias, .. }
        | PreparedMutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };

    let matched_assignments_sql = match stmt.matched.as_ref().map(|c| &c.action) {
        Some(PreparedMergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|a| {
                let target_column = target_columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&a.column))
                    .ok_or_else(|| {
                        format!(
                            "MERGE UPDATE assignment references unknown target column `{}`",
                            a.column
                        )
                    })?;
                Ok((
                    target_column.name.clone(),
                    crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                        &format!("({})", a.value_sql),
                        target_column,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?,
        _ => Vec::new(),
    };
    let matched_assignments_sql_borrow = matched_assignments_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect::<Vec<_>>();

    let insert_values_sql: Vec<(String, String)> =
        match (insert_columns, stmt.not_matched.as_ref().map(|c| &c.action)) {
            (Some(cols), Some(action)) => cols
                .iter()
                .filter_map(|col| {
                    col.value_index.map(|idx| {
                        let target_column = target_columns
                            .iter()
                            .find(|target_column| {
                                target_column.name.eq_ignore_ascii_case(&col.name)
                            })
                            .expect("resolved MERGE INSERT column exists in target columns");
                        Ok((
                            col.name.clone(),
                            crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                                &format!("({})", action.values_sql[idx]),
                                target_column,
                            )?,
                        ))
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            _ => Vec::new(),
        };
    let insert_values_sql_borrow = insert_values_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect::<Vec<_>>();

    let matched_action = stmt.matched.as_ref().map(|clause| match clause.action {
        PreparedMergeMatchedAction::Update { .. } => MERGE_ACTION_MATCHED_UPDATE,
        PreparedMergeMatchedAction::Delete => MERGE_ACTION_MATCHED_DELETE,
    });
    let has_matched_update = matched_action == Some(MERGE_ACTION_MATCHED_UPDATE);
    let has_matched_delete = matched_action == Some(MERGE_ACTION_MATCHED_DELETE);
    let has_not_matched_insert = stmt.not_matched.is_some();
    let matched_predicate_sql = stmt
        .matched
        .as_ref()
        .and_then(|c| c.predicate_sql.as_deref());
    let not_matched_predicate_sql = stmt
        .not_matched
        .as_ref()
        .and_then(|c| c.predicate_sql.as_deref());

    let match_sql = build_merge_match_query_sql(
        &target_sql,
        &target_alias,
        &source_sql,
        &stmt.on_sql,
        matched_predicate_sql,
        not_matched_predicate_sql,
        target_columns,
        &matched_assignments_sql_borrow,
        &insert_values_sql_borrow,
        matched_action,
        has_not_matched_insert,
    );
    let mut query = parse_generated_query(&match_sql, "MOR MERGE change-stream producer")?;
    if crate::query_execution::planning::time_travel::has_time_travel_refs(&query) {
        crate::query_execution::planning::time_travel::rewrite_time_travel_refs(
            state,
            Some(&target.catalog),
            &target.namespace,
            &mut query,
            connector_context,
        )?;
    }

    compile_dml_change_stream_write(
        state,
        target,
        query,
        DmlChangeStreamKind::Merge {
            target_columns: target_columns.to_vec(),
            new_sequence_number,
            matched_update: has_matched_update,
            matched_delete: has_matched_delete,
            not_matched_insert: has_not_matched_insert,
        },
        (has_matched_update || has_matched_delete).then(|| DmlPreExpandKeyedAssert {
            // Matched rows use the real target `_row_id`; unmatched rows use
            // a generated negative row number so fresh-only rows do not
            // collide under the same NULL key before expansion.
            key_column_name: "__nr_merge_assert_key".to_string(),
            key_label: novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
            message_prefix: "MOR MERGE matched target row".to_string(),
        }),
        execution,
        connector_context,
        write_session,
        write_planning_lease,
    )
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn execute_merge_match_query(
    state: &DmlExecutionKernel,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MergeMatchRows, String> {
    let _ = (
        state,
        current_catalog,
        sql,
        current_database,
        execution,
        connector_context,
    );
    Err(
        "test-only MERGE match materialization requires an explicit query preparation kernel"
            .to_string(),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "MERGE SQL construction keeps all user-visible clauses and frozen target facts explicit."
)]
fn build_merge_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: &str,
    on_sql: &str,
    matched_predicate_sql: Option<&str>,
    not_matched_predicate_sql: Option<&str>,
    target_columns: &[novarocks_types::schema::ColumnDef],
    matched_assignments_sql: &[(&str, &str)],
    insert_values_sql: &[(&str, &str)],
    matched_action: Option<i32>,
    has_not_matched_insert: bool,
) -> String {
    let quote_ident = |ident: &str| format!("`{}`", ident.replace('`', "``"));
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            quote_ident(column)
        } else {
            format!("{target_alias}.{}", quote_ident(column))
        }
    };
    let row_id = qualify("_row_id");
    let nullable_target_column = |column: &str| {
        let value = qualify(column);
        format!("CASE WHEN {row_id} IS NOT NULL THEN {value} ELSE NULL END")
    };
    let matched_apply_expr = format!(
        "(CASE WHEN ({}) THEN TRUE ELSE FALSE END)",
        matched_predicate_sql.unwrap_or("TRUE")
    );
    let unmatched_apply_expr = format!(
        "(CASE WHEN ({}) THEN TRUE ELSE FALSE END)",
        not_matched_predicate_sql.unwrap_or("TRUE")
    );
    let mut action_cases = Vec::new();
    if let Some(action) = matched_action {
        action_cases.push(format!(
            "WHEN {row_id} IS NOT NULL AND ({}) THEN {action}",
            matched_predicate_sql.unwrap_or("TRUE")
        ));
    }
    if has_not_matched_insert {
        action_cases.push(format!(
            "WHEN {row_id} IS NULL AND ({}) THEN {MERGE_ACTION_NOT_MATCHED_INSERT}",
            not_matched_predicate_sql.unwrap_or("TRUE")
        ));
    }
    let action_expr = if action_cases.is_empty() {
        "0".to_string()
    } else {
        format!("CASE {} ELSE 0 END", action_cases.join(" "))
    };
    let target_select_items = target_columns
        .iter()
        .map(|column| {
            format!(
                "{} AS {}",
                nullable_target_column(&column.name),
                quote_ident(&column.name)
            )
        })
        .collect::<Vec<_>>();

    let mut select_items = vec![
        format!("{} AS __nr_file", nullable_target_column("_file")),
        format!("{} AS __nr_pos", nullable_target_column("_pos")),
        format!("{} AS __nr_row_id", nullable_target_column("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            nullable_target_column("_last_updated_sequence_number")
        ),
        format!(
            "CASE WHEN {row_id} IS NOT NULL THEN {row_id} ELSE -ROW_NUMBER() OVER () END AS __nr_merge_assert_key"
        ),
        format!("({action_expr}) AS __nr_merge_action"),
        format!(
            "(CASE WHEN {} IS NOT NULL THEN 'matched' ELSE 'unmatched' END) AS __nr_match_kind",
            row_id
        ),
    ];
    select_items.extend(target_select_items);
    select_items.push(format!("{matched_apply_expr} AS __nr_matched_apply"));
    select_items.push(format!("{unmatched_apply_expr} AS __nr_unmatched_apply"));
    for (column, expr) in matched_assignments_sql {
        select_items.push(format!("({expr}) AS __nr_new_{column}"));
    }
    for (column, expr) in insert_values_sql {
        select_items.push(format!("({expr}) AS __nr_ins_{column}"));
    }

    format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {}",
        select_items.join(", "),
        source_sql,
        target_sql,
        on_sql
    )
}

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn build_merge_unmatched_insert_query(
    state: &DmlExecutionKernel,
    target: &crate::catalog_application::resolver::TargetBackend,
    stmt: &PreparedMergeStatement,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_types::schema::ColumnDef],
    insert_columns: &MergeInsertColumns,
) -> Result<novarocks_parser::ast::Query, String> {
    let target_alias = stmt
        .target_alias
        .as_deref()
        .unwrap_or(MERGE_TARGET_DEFAULT_ALIAS);
    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    let source_sql = match &stmt.source {
        PreparedMutationSource::Table { alias, .. }
        | PreparedMutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };
    let not_matched = stmt
        .not_matched
        .as_ref()
        .ok_or_else(|| "MERGE unmatched INSERT write requires a not-matched clause".to_string())?;
    let select_items = target_columns
        .iter()
        .zip(insert_columns.iter())
        .map(|(target_column, insert_column)| {
            if target_column.name != insert_column.name {
                return Err(format!(
                    "MERGE INSERT column order mismatch: target `{}`, insert `{}`",
                    target_column.name, insert_column.name
                ));
            }
            let raw_expr = match insert_column.value_index {
                Some(idx) => format!("({})", not_matched.action.values_sql[idx]),
                None => "NULL".to_string(),
            };
            let expr = crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
                &raw_expr,
                target_column,
            )?;
            Ok(format!("{expr} AS {}", sql_identifier(&target_column.name)))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let target_sql = format!(
        "{} AS {}",
        qualify_iceberg_table(target),
        sql_identifier(target_alias)
    );
    let mut predicates = vec![format!(
        "{} IS NULL",
        qualify_column(
            target_alias,
            novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL
        )
    )];
    if let Some(predicate) = not_matched.predicate_sql.as_deref() {
        predicates.push(format!("({predicate})"));
    }
    let sql = format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {} WHERE {}",
        select_items.join(", "),
        source_sql,
        target_sql,
        stmt.on_sql,
        predicates.join(" AND ")
    );
    parse_generated_query(&sql, "MERGE unmatched INSERT sink")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_types::schema::ColumnDef;

    fn test_dml_kernel() -> DmlExecutionKernel {
        let connector_control: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> =
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default());
        DmlExecutionKernel::new(
            crate::query_execution::kernels::DmlPlanningServices::new(
                Arc::new(
                    novarocks_sql::compiler::build_builtin_engine_function_catalog()
                        .expect("builtin function catalog"),
                ),
                Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            ),
            None,
            Arc::clone(&connector_control),
            std::sync::Arc::new(crate::connector::ConnectorControlHost::new()),
            Arc::new(crate::connector::unified_statistics::UnifiedStatisticsResolver::default()),
            Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
            crate::query_execution::compiler::test_query_execution_service(),
        )
    }

    struct NeverCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn connector_context_for_test() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            Arc::new(NeverCancelled),
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

    /// One signed COW row-mutation preparation whose base version ordinal is
    /// under the test's control. Everything else is the minimum the SPI accepts.
    fn cow_match_preparation(
        base_version_ordinal: Option<i64>,
    ) -> novarocks_spi::connector::ConnectorRowMutationPreparation {
        use novarocks_spi::connector::{
            ConnectorInstanceId, ConnectorMutationEffectField, ConnectorMutationMatchContract,
            ConnectorMutationSourceField, ConnectorMutationTargetField,
            ConnectorProviderBindingKey, ConnectorRowMutationIntent,
            ConnectorRowMutationPreparation, ConnectorRowMutationStrategy, ConnectorTableHandle,
            ConnectorWriteBaseVersion, ConnectorWriteFieldToken, ConnectorWriteOperationId,
            ConnectorWriteTargetRef, ProviderBindingEpoch,
        };

        let instance_id = ConnectorInstanceId::parse("iceberg").expect("instance ID");
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        };
        let table = ConnectorTableHandle::try_new(instance_id, bytes::Bytes::from_static(b"table"))
            .expect("table handle");
        let base = ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"base"))
            .expect("base version");
        let row_id_token = ConnectorWriteFieldToken::from_bytes([1; 32]);
        let value_token = ConnectorWriteFieldToken::from_bytes([4; 32]);
        let effect_token = ConnectorWriteFieldToken::from_bytes([5; 32]);
        let match_contract = ConnectorMutationMatchContract::try_new(
            owner.clone(),
            table.clone(),
            base.clone(),
            vec![ConnectorMutationSourceField::new(
                row_id_token,
                arrow::datatypes::Field::new("match_key", DataType::Int64, false),
                0,
            )],
            Vec::new(),
            vec![ConnectorMutationTargetField::new(
                value_token,
                arrow::datatypes::Field::new("after_value", DataType::Int64, true),
                1,
            )],
            vec![row_id_token],
            ConnectorMutationEffectField::try_new(
                effect_token,
                arrow::datatypes::Field::new("effect", DataType::Int8, false),
                2,
            )
            .expect("effect field"),
        )
        .expect("match contract");
        ConnectorRowMutationPreparation::try_new(
            owner,
            ConnectorWriteOperationId::from_bytes([8; 16]),
            table.clone(),
            table,
            Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new("match_key", DataType::Int64, false),
                arrow::datatypes::Field::new("after_value", DataType::Int64, false),
            ])),
            ConnectorWriteTargetRef::main(),
            ConnectorRowMutationIntent::Update,
            base,
            match_contract,
            ConnectorRowMutationStrategy::CopyOnWrite,
            base_version_ordinal,
            Some(42),
            bytes::Bytes::from_static(b"row-mutation"),
        )
        .expect("row-mutation preparation")
    }

    fn cow_update_statement() -> PreparedUpdateStatement {
        PreparedUpdateStatement {
            table: ObjectName {
                parts: vec!["ice".to_string(), "db1".to_string(), "t".to_string()],
            },
            alias: None,
            assignments: vec![PreparedMutationAssignment {
                column: "after_value".to_string(),
                value_sql: "99".to_string(),
            }],
            source: None,
            where_sql: None,
        }
    }

    /// The COW match must name the mutation target as the relation frozen at
    /// the provider-signed base snapshot. Naming the bare relation produced a
    /// pre-pinned opaque connector read, which scan preparation refuses before
    /// it ever consults a resolver; the pinned identity instead resolves through
    /// the admitted frozen-snapshot lane time travel already uses.
    #[test]
    fn cow_match_selection_pins_the_target_to_its_signed_base_snapshot() {
        let target = iceberg_target();
        let preparation = cow_match_preparation(Some(41));
        let query = build_exact_cow_update_selection_query(
            &target,
            &cow_update_statement(),
            None,
            &preparation,
        )
        .expect("exact COW UPDATE selection");
        let printed = novarocks_parser::printer::print_query(&query);

        assert!(
            printed.contains("__sqlx1_tt_t_41"),
            "COW match must read the target at its signed base snapshot: {printed}"
        );
        let unpinned = format!(
            "{}.{}.{}",
            sql_identifier(&target.catalog),
            sql_identifier(&target.namespace),
            sql_identifier(&target.table),
        );
        assert!(
            !printed.contains(&unpinned),
            "COW match must not name the unpinned current relation: {printed}"
        );
        assert_eq!(
            crate::catalog_application::query_bindings::QueryTableBindingKey::analysis_lookup(
                &target.catalog,
                &target.namespace,
                "__sqlx1_tt_t_41",
            ),
            crate::catalog_application::query_bindings::QueryTableBindingKey::snapshot(
                &target.catalog,
                &target.namespace,
                &target.table,
                41,
            ),
            "the pinned COW match identity must resolve to the frozen-snapshot binding"
        );
    }

    /// Reading a later snapshot than the one the commit is validated against is
    /// how a mutation silently loses a concurrent write, so an unsigned base
    /// version fails the statement instead of falling back to `Current`.
    #[test]
    fn cow_match_selection_fails_closed_without_a_signed_base_snapshot() {
        let target = iceberg_target();
        let preparation = cow_match_preparation(None);
        let error = build_exact_cow_update_selection_query(
            &target,
            &cow_update_statement(),
            None,
            &preparation,
        )
        .expect_err("COW match without a signed base version must fail closed");

        assert!(
            error.contains("no provider-signed base version ordinal"),
            "unexpected COW match failure: {error}"
        );
    }

    struct CowRewriteQueryFixture {
        selection: novarocks_spi::connector::ConnectorRowMutationSelection,
        recipe: novarocks_spi::connector::ConnectorRowMutationCohortRecipe,
        route: novarocks_spi::connector::ConnectorRowMutationRoute,
        /// What the session hands a copy-on-write target: the branch's input
        /// shape, its routing facts, the rows it owns, and the read contract
        /// for the file it rewrites. None of them names a cohort.
        input: novarocks_spi::connector::ConnectorWriteInputShape,
        route_facts: novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
        rows: Vec<novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal>,
        rewrite_source: novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource,
        preparation: novarocks_spi::connector::ConnectorRowMutationPreparation,
        identity: FrozenConnectorScanIdentity,
        scan_schema: Arc<Schema>,
        scan_bindings: Vec<novarocks_spi::connector::ConnectorRowMutationScanBinding>,
        match_tokens: Vec<novarocks_spi::connector::ConnectorWriteFieldToken>,
        written_version_token: novarocks_spi::connector::ConnectorWriteFieldToken,
    }

    fn cow_rewrite_query_fixture(
        row_ids: Vec<i64>,
        after_ids: Vec<i64>,
        after_values: ArrayRef,
        value_type: DataType,
    ) -> CowRewriteQueryFixture {
        use novarocks_spi::connector::{
            ConnectorInstanceId, ConnectorMutationEffectField, ConnectorMutationMatchContract,
            ConnectorMutationRouteInput, ConnectorMutationSourceField,
            ConnectorMutationTargetField, ConnectorProviderBindingKey,
            ConnectorRowMutationCohortRecipe, ConnectorRowMutationEffect,
            ConnectorRowMutationIntent, ConnectorRowMutationPreparation, ConnectorRowMutationRoute,
            ConnectorRowMutationScanBinding, ConnectorRowMutationSelection,
            ConnectorRowMutationSelectionOrdinal, ConnectorRowMutationStrategy,
            ConnectorTableHandle, ConnectorWriteBaseVersion, ConnectorWriteCohortId,
            ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
            ConnectorWriteIntent, ConnectorWriteOperationId, ConnectorWritePreparation,
            ConnectorWriteRouteId, ConnectorWriteTargetRef, ProviderBindingEpoch,
        };

        let row_count = row_ids.len();
        assert_eq!(after_ids.len(), row_count);
        assert_eq!(after_values.len(), row_count);
        let instance_id = ConnectorInstanceId::parse("iceberg").expect("instance ID");
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        };
        let table =
            ConnectorTableHandle::try_new(instance_id.clone(), bytes::Bytes::from_static(b"table"))
                .expect("table handle");
        let base = ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"base"))
            .expect("base version");
        let operation_id = ConnectorWriteOperationId::from_bytes([8; 16]);
        let row_id_token = ConnectorWriteFieldToken::from_bytes([1; 32]);
        let source_version_token = ConnectorWriteFieldToken::from_bytes([2; 32]);
        let id_token = ConnectorWriteFieldToken::from_bytes([3; 32]);
        let value_token = ConnectorWriteFieldToken::from_bytes([4; 32]);
        let effect_token = ConnectorWriteFieldToken::from_bytes([5; 32]);
        let match_contract = ConnectorMutationMatchContract::try_new(
            owner.clone(),
            table.clone(),
            base.clone(),
            vec![
                ConnectorMutationSourceField::new(
                    row_id_token,
                    arrow::datatypes::Field::new("match_key", DataType::Int64, false),
                    0,
                ),
                ConnectorMutationSourceField::new(
                    source_version_token,
                    arrow::datatypes::Field::new("match_version", DataType::Int64, false),
                    1,
                ),
            ],
            Vec::new(),
            vec![
                ConnectorMutationTargetField::new(
                    id_token,
                    arrow::datatypes::Field::new("after_id", DataType::Int64, true),
                    2,
                ),
                ConnectorMutationTargetField::new(
                    value_token,
                    arrow::datatypes::Field::new("after_value", value_type.clone(), true),
                    3,
                ),
            ],
            vec![row_id_token],
            ConnectorMutationEffectField::try_new(
                effect_token,
                arrow::datatypes::Field::new("effect", DataType::Int8, false),
                4,
            )
            .expect("effect field"),
        )
        .expect("match contract");
        let preparation = ConnectorRowMutationPreparation::try_new(
            owner.clone(),
            operation_id,
            table.clone(),
            table.clone(),
            Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new("match_key", DataType::Int64, false),
                arrow::datatypes::Field::new("match_version", DataType::Int64, false),
                arrow::datatypes::Field::new("after_id", DataType::Int64, false),
                arrow::datatypes::Field::new("after_value", value_type.clone(), false),
            ])),
            ConnectorWriteTargetRef::main(),
            ConnectorRowMutationIntent::Update,
            base.clone(),
            match_contract,
            ConnectorRowMutationStrategy::CopyOnWrite,
            Some(41),
            Some(42),
            bytes::Bytes::from_static(b"row-mutation"),
        )
        .expect("row-mutation preparation");
        let selection_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("match_key", DataType::Int64, false),
            arrow::datatypes::Field::new("match_version", DataType::Int64, false),
            arrow::datatypes::Field::new("after_id", DataType::Int64, true),
            arrow::datatypes::Field::new("after_value", value_type.clone(), true),
            arrow::datatypes::Field::new("effect", DataType::Int8, false),
        ]));
        let selection_batch = RecordBatch::try_new(
            selection_schema.clone(),
            vec![
                Arc::new(Int64Array::from(row_ids)) as ArrayRef,
                Arc::new(Int64Array::from(vec![1; row_count])) as ArrayRef,
                Arc::new(Int64Array::from(after_ids)) as ArrayRef,
                after_values,
                Arc::new(Int8Array::from(vec![
                    ConnectorRowMutationEffect::Replace
                        as i8;
                    row_count
                ])) as ArrayRef,
            ],
        )
        .expect("selection batch");
        let selection = ConnectorRowMutationSelection::try_new(
            selection_schema,
            vec![selection_batch],
            row_count as u64,
            64 * 1024,
        )
        .expect("selection");
        // The provider signs the branch input and the match contract together,
        // so a field carries one name in both. The builder bridges them by that
        // name, so a fixture that invented separate names would exercise a
        // bridge production never takes.
        let route_input = ConnectorWriteInputShape::RowLineage {
            data_fields: vec![
                ConnectorWriteFieldBinding::new(
                    id_token,
                    arrow::datatypes::Field::new("after_id", DataType::Int64, true),
                ),
                ConnectorWriteFieldBinding::new(
                    value_token,
                    arrow::datatypes::Field::new("after_value", value_type.clone(), true),
                ),
            ],
            row_identity_fields: vec![
                ConnectorWriteFieldBinding::new(
                    row_id_token,
                    arrow::datatypes::Field::new("match_key", DataType::Int64, false),
                ),
                ConnectorWriteFieldBinding::new(
                    source_version_token,
                    arrow::datatypes::Field::new("match_version", DataType::Int64, false),
                ),
            ],
        };
        let writer = ConnectorWritePreparation::try_new(
            owner.clone(),
            table,
            ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::RowDelta,
            base.clone(),
            route_input.clone(),
            bytes::Bytes::from_static(b"writer"),
        )
        .expect("writer preparation");
        let cohort_id =
            ConnectorWriteCohortId::derive(operation_id, b"rewrite", [9; 32]).expect("cohort ID");
        let route = ConnectorRowMutationRoute::try_new(
            ConnectorWriteRouteId::from_bytes([10; 32]),
            cohort_id,
            vec![ConnectorRowMutationEffect::Replace],
            route_input,
            vec![
                ConnectorMutationRouteInput::new(id_token, 0),
                ConnectorMutationRouteInput::new(value_token, 1),
                ConnectorMutationRouteInput::new(row_id_token, 2),
                ConnectorMutationRouteInput::new(source_version_token, 3),
            ],
            Vec::new(),
            writer,
        )
        .expect("rewrite route");
        let scan_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("source_id", DataType::Int64, true),
            arrow::datatypes::Field::new("source_value", value_type, true),
            arrow::datatypes::Field::new("source_key", DataType::Int64, false),
            arrow::datatypes::Field::new("source_version", DataType::Int64, false),
        ]));
        let scan_bindings = vec![
            ConnectorRowMutationScanBinding::new(id_token, 0),
            ConnectorRowMutationScanBinding::new(value_token, 1),
            ConnectorRowMutationScanBinding::new(row_id_token, 2),
            ConnectorRowMutationScanBinding::new(source_version_token, 3),
        ];
        let match_tokens = vec![row_id_token];
        let recipe = ConnectorRowMutationCohortRecipe::try_rewrite(
            cohort_id,
            route.route_id(),
            &selection,
            (0..row_count as u64)
                .map(ConnectorRowMutationSelectionOrdinal::new)
                .collect(),
            ConnectorTableHandle::try_new(
                instance_id.clone(),
                bytes::Bytes::from_static(b"frozen-source"),
            )
            .expect("frozen source"),
            novarocks_spi::connector::ConnectorPinnedFileSet::try_new(
                "db",
                "t",
                11,
                ["s3://bucket/db/t/data/a.parquet"],
            )
            .expect("pinned source"),
            base.digest(),
            scan_schema.clone(),
            scan_bindings.clone(),
            match_tokens.clone(),
            Some(source_version_token),
            bytes::Bytes::from_static(b"recipe"),
        )
        .expect("rewrite recipe");

        let route_facts = novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts::try_new(
            route.route_id(),
            route.accepted_effects().to_vec(),
            route.input_ordinals().to_vec(),
            route.partition_fields().to_vec(),
        )
        .expect("route facts");
        let rewrite_source =
            novarocks_spi::connector::write_stack::ConnectorWriteRewriteSource::new(
                ConnectorTableHandle::try_new(
                    instance_id,
                    bytes::Bytes::from_static(b"frozen-source"),
                )
                .expect("frozen source"),
                novarocks_spi::connector::ConnectorPinnedFileSet::try_new(
                    "db",
                    "t",
                    11,
                    ["s3://bucket/db/t/data/a.parquet"],
                )
                .expect("pinned source"),
                base.digest(),
                scan_schema.clone(),
                scan_bindings.clone(),
                match_tokens.clone(),
                Some(source_version_token),
            );
        let rows = (0..row_count as u64)
            .map(ConnectorRowMutationSelectionOrdinal::new)
            .collect::<Vec<_>>();
        let input = route.input().clone();

        CowRewriteQueryFixture {
            selection,
            recipe,
            input,
            route_facts,
            rows,
            rewrite_source,
            route,
            preparation,
            identity: FrozenConnectorScanIdentity::new(
                "default_catalog",
                "__nr_cow",
                "__nr_cow_t_abc",
            ),
            scan_schema,
            scan_bindings,
            match_tokens,
            written_version_token: source_version_token,
        }
    }

    struct AbortOutcomeExecution {
        outcome: novarocks_spi::connector::ConnectorWriteAbortOutcome,
        context: novarocks_spi::connector::ConnectorRequestContext,
    }

    impl MutationExecution for AbortOutcomeExecution {
        fn stage(&self) -> Result<QueryExecutionResult, String> {
            Err("synthetic post-begin staging failure".to_string())
        }

        fn abort_terminal(
            &self,
        ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
            Ok(self.outcome.clone())
        }

        fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
            self.context.clone()
        }

        fn finalize(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn abort_unknown_evidence() -> novarocks_spi::connector::ExternalMutationEvidence {
        use novarocks_spi::connector::{
            ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
            ConnectorProviderId, ExternalMutationEvidence, ProviderBindingEpoch,
        };

        ExternalMutationEvidence::try_new(
            1,
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("test-provider").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("test-instance").expect("instance ID"),
            },
            ProviderBindingEpoch::from_bytes([33; 16]),
            ConnectorMutationOperationId::from_bytes([44; 16]),
            "row-mutation-abort",
            bytes::Bytes::from_static(b"uncertain"),
        )
        .expect("abort evidence")
    }

    #[test]
    fn abort_required_preserves_known_committed_and_commit_unknown_outcomes() {
        use novarocks_spi::connector::{
            ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorWriteAbortOutcome,
            ConnectorWriteReceipt, ExternalMutationFinalization,
        };

        let outcomes = [
            ConnectorWriteAbortOutcome::KnownCommitted {
                receipt: ConnectorWriteReceipt::try_new(bytes::Bytes::from_static(b"committed"))
                    .expect("receipt"),
                finalization: ExternalMutationFinalization::Complete,
            },
            ConnectorWriteAbortOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "commit state unavailable",
                ),
                evidence: abort_unknown_evidence(),
            },
        ];

        for expected in outcomes {
            let staged = MutationStagedWrite::AbortRequired {
                reason: "synthetic post-begin staging failure".to_string(),
                execution: Arc::new(AbortOutcomeExecution {
                    outcome: expected.clone(),
                    context: connector_context_for_test(),
                }),
            };
            let MutationStagedWrite::AbortRequired { reason, execution } = staged else {
                panic!("expected AbortRequired");
            };
            assert_eq!(reason, "synthetic post-begin staging failure");
            assert_eq!(
                execution.abort_terminal().expect("typed abort outcome"),
                expected
            );
        }
    }

    #[test]
    fn cow_rewrite_query_rewrites_whole_file_and_preserves_row_id() {
        let fixture = cow_rewrite_query_fixture(
            vec![7, 9],
            vec![2, 4],
            Arc::new(StringArray::from(vec!["bb", "dd"])) as ArrayRef,
            DataType::Utf8,
        );
        let query = build_cow_rewrite_query(
            &fixture.selection,
            &fixture.rows,
            &fixture.input,
            &fixture.route_facts,
            &fixture.rewrite_source,
            &fixture.preparation,
            &fixture.identity,
        )
        .expect("query");
        let sql = novarocks_parser::printer::print_query(&query);

        // The source and its field names are provider-signed scan facts; Core
        // binds them only through recipe tokens and ordinals.
        assert!(sql.contains("`default_catalog`"), "{sql}");
        assert!(sql.contains("`__nr_cow_t_abc`"), "{sql}");
        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(sql.contains("VALUES"), "{sql}");
        // Unmatched source rows are retained, matched Delete effects are
        // filtered, and Replace values use the bounded selection after-image.
        assert!(sql.contains(" WHERE "), "{sql}");
        assert!(sql.contains("CASE WHEN"), "{sql}");
        assert!(sql.contains("IS NOT NULL"), "{sql}");
        // The rewritten row keeps its identity from the scanned file rather
        // than from the match relation: that is what preserves row lineage
        // across a whole-file rewrite.
        assert!(
            sql.contains("CAST(`__nr_scan`.`source_key` AS BIGINT) AS `match_key`"),
            "{sql}"
        );
        assert!(sql.contains("AS `match_version`"), "{sql}");
        assert!(sql.contains("42"), "{sql}");
        assert!(sql.contains("'bb'"), "{sql}");
        assert!(sql.contains("'dd'"), "{sql}");
        assert!(!sql.contains("_row_id"), "{sql}");
    }

    #[test]
    fn cow_selection_preserves_signed_schema_when_query_returns_no_chunks() {
        let fixture = cow_rewrite_query_fixture(
            vec![7],
            vec![2],
            Arc::new(StringArray::from(vec!["bb"])) as ArrayRef,
            DataType::Utf8,
        );
        let selection = cow_selection_from_query_result(
            QueryResult {
                columns: Vec::new(),
                chunks: Vec::new(),
            },
            &fixture.preparation,
            connector_context_for_test(),
        )
        .expect("typed empty COW selection");

        assert_eq!(selection.schema().fields().len(), 5);
        assert_eq!(selection.row_count(), 0);
        assert!(selection.batches().is_empty());
    }

    #[test]
    fn cow_rewrite_query_casts_variant_values_payloads() {
        let payload = [0x0c_u8, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
        let fixture = cow_rewrite_query_fixture(
            vec![7],
            vec![10],
            Arc::new(arrow::array::LargeBinaryArray::from_iter_values([
                payload.as_slice()
            ])) as ArrayRef,
            DataType::LargeBinary,
        );
        let query = build_cow_rewrite_query(
            &fixture.selection,
            &fixture.rows,
            &fixture.input,
            &fixture.route_facts,
            &fixture.rewrite_source,
            &fixture.preparation,
            &fixture.identity,
        )
        .expect("query");
        let sql = novarocks_parser::printer::print_query(&query);

        assert!(sql.contains("CAST(X'0C000000010203' AS VARIANT)"), "{sql}");
        assert!(sql.contains("CASE WHEN"), "{sql}");
    }

    #[test]
    fn reject_reserved_update_columns() {
        let err = validate_update_assignments(
            &[PreparedMutationAssignment {
                column: "_row_id".to_string(),
                value_sql: "1".to_string(),
            }],
            &[col("id"), col("v")],
            &[],
        )
        .expect_err("must reject");
        assert!(err.contains("reserved Iceberg metadata column"), "{err}");
    }

    #[test]
    fn reject_partition_column_update() {
        let err = validate_update_assignments(
            &[PreparedMutationAssignment {
                column: "id".to_string(),
                value_sql: "1".to_string(),
            }],
            &[col("id"), col("v")],
            &["id".to_string()],
        )
        .expect_err("must reject");
        assert!(err.contains("partition column"), "{err}");
    }

    #[test]
    fn update_match_query_projects_identity_columns() {
        let sql = build_update_match_query_sql(
            "ice.db1.t AS t",
            "t",
            Some("staging.s AS s"),
            &[("v", "s.v")],
            Some("t.id = s.id"),
        );
        assert!(sql.contains("t._row_id AS __nr_row_id"), "{sql}");
        assert!(sql.contains("s.v AS __nr_new_v"), "{sql}");
        assert!(sql.contains("WHERE t.id = s.id"), "{sql}");
    }

    #[test]
    fn update_change_stream_target_sql_pins_branch_read_snapshot() {
        let sql = update_change_stream_target_sql(&iceberg_target(), "t", "dev");
        assert!(sql.contains("FOR VERSION AS OF 'dev'"), "{sql}");
        assert!(sql.ends_with(" AS t"), "{sql}");
    }

    #[test]
    fn update_assignment_projection_casts_to_target_type() {
        let assignments = vec![PreparedMutationAssignment {
            column: "v".to_string(),
            value_sql: "src_v".to_string(),
        }];
        let projected = update_assignment_projection_sql(
            &assignments,
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Int32),
            ],
        )
        .expect("assignment projection");

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].0, "v");
        assert!(
            projected[0].1.contains("CAST((src_v) AS INT)"),
            "{:?}",
            projected
        );
    }

    #[test]
    fn update_change_stream_match_query_uses_casted_assignment_projection() {
        let assignments = vec![PreparedMutationAssignment {
            column: "v".to_string(),
            value_sql: "src_v".to_string(),
        }];
        let projected = update_assignment_projection_sql(
            &assignments,
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Int32),
            ],
        )
        .expect("assignment projection");
        let projected_refs = projected
            .iter()
            .map(|(column, expr)| (column.as_str(), expr.as_str()))
            .collect::<Vec<_>>();
        let target_sql = update_change_stream_target_sql(&iceberg_target(), "t", "main");
        let sql = build_update_match_query_sql(
            &target_sql,
            "t",
            Some("staging.s AS s"),
            &projected_refs,
            Some("t.id = s.id"),
        );
        assert!(sql.contains("CAST((src_v) AS INT) AS __nr_new_v"), "{sql}");
        assert!(sql.contains("t._row_id AS __nr_row_id"), "{sql}");
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
    fn merge_match_query_projects_nullable_target_columns() {
        let sql = build_merge_match_query_sql(
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

        assert!(!sql.contains("t.*"), "{sql}");
        assert!(
            sql.contains("CASE WHEN t.`_row_id` IS NOT NULL THEN t.`id` ELSE NULL END AS `id`"),
            "{sql}"
        );
        assert!(sql.contains("(s.v) AS __nr_new_v"), "{sql}");
        assert!(sql.contains("(s.id) AS __nr_ins_id"), "{sql}");
        assert!(sql.contains("AS __nr_merge_action"), "{sql}");
        assert!(sql.contains("AS __nr_merge_assert_key"), "{sql}");
    }

    #[test]
    fn merge_unmatched_insert_query_uses_distributed_append_shape() {
        let stmt = PreparedMergeStatement {
            table: ObjectName {
                parts: vec!["t".to_string()],
            },
            target_alias: Some("t".to_string()),
            source: PreparedMutationSource::Query {
                query_text: "SELECT 3 AS id, 4 AS v".to_string(),
                alias: Some("s".to_string()),
            },
            on_sql: "t.id = s.id".to_string(),
            matched: None,
            not_matched: Some(PreparedMergeClause {
                predicate_sql: Some("s.id > 0".to_string()),
                action: PreparedMergeNotMatchedAction {
                    columns: vec!["id".to_string()],
                    values_sql: vec!["s.id".to_string()],
                },
            }),
        };
        let target_columns = vec![col("id"), col("v")];
        let insert_columns = resolve_merge_insert_columns(
            &stmt.not_matched.as_ref().expect("not matched").action,
            &target_columns,
        )
        .expect("insert columns");
        let state = test_dml_kernel();

        let query = build_merge_unmatched_insert_query(
            &state,
            &iceberg_target(),
            &stmt,
            None,
            &target_columns,
            &insert_columns,
        )
        .expect("query");
        let sql = novarocks_parser::printer::print_query(&query);

        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(sql.contains("_row_id"), "{sql}");
        assert!(sql.contains("IS NULL"), "{sql}");
        assert!(sql.contains("CAST((s.id) AS BIGINT) AS `id`"), "{sql}");
        assert!(sql.contains("CAST(NULL AS BIGINT) AS `v`"), "{sql}");
        assert!(sql.contains("(s.id > 0)"), "{sql}");
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
        type CommitHandle = FakeRowMutationCommit;
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
            Vec::new(),
            Vec::new(),
        )?))
    }

    impl novarocks_spi::connector::write_stack::ConnectorWriteControl for FakeRowMutationControl {
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
                self.adapter.wrap_commit_handle(FakeRowMutationCommit),
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
        calls: Arc<Mutex<RowMutationSessionCalls>>,
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
        let lease = crate::connector::control_host::ConnectorWriteStackLease::new(
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            novarocks_spi::connector::ConnectorControlWriteBinding::new(
                Arc::new(UnusedLegacyWriteControl),
                control,
                Arc::new(FakeRowMutationEncoder),
                Arc::new(FakeRowMutationDecoder { adapter }),
            ),
            || {},
        );
        let session = Arc::new(
            ConnectorWriteSession::begin(
                lease,
                row_mutation_catalog_properties(),
                novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
                    table: Arc::from("db1.t"),
                    target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
                    intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
                    purpose:
                        novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
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
            .expect("begin row-mutation write session"),
        );
        RowMutationSessionFixture { session, calls }
    }

    fn known_uncommitted_outcome() -> novarocks_spi::connector::ExternalMutationOutcome<
        novarocks_spi::connector::ConnectorWriteReceipt,
    > {
        novarocks_spi::connector::ExternalMutationOutcome::KnownUncommitted {
            failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                "scripted",
            ),
        }
    }

    fn empty_session_completion(
        session: &Arc<ConnectorWriteSession>,
    ) -> crate::query_execution::outcome::ConnectorWriteSessionCompletion {
        crate::query_execution::outcome::ConnectorWriteSessionCompletion::for_test(
            Arc::clone(session),
            crate::query_execution::write_result::DecodedPreparedWriteSet::for_test(0, Vec::new()),
        )
    }

    fn mor_update_executor(
        session: Arc<ConnectorWriteSession>,
    ) -> Arc<MorUpdateChangeStreamExecutor> {
        Arc::new(MorUpdateChangeStreamExecutor {
            state: test_dml_kernel(),
            target: iceberg_target(),
            // No plan: this executor models a stage that never dispatched.
            planned: Mutex::new(None),
            execution: crate::common::admitted_query_context::QueryExecutionContext::new(
                novarocks_types::ClusterRole::Fe,
                crate::common::backend_topology::BackendTopologySnapshot::empty(3),
                None,
                crate::common::query_cancellation::QueryCancellationSource::new().view(),
                novarocks_sql::compiler::SessionOptimizerSettings::default(),
            ),
            connector_context: connector_context_for_test(),
            write_session: session,
        })
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

    /// A staged merge-on-read write commits its session once, and the terminal
    /// is single-shot: the connector is not asked a second time.
    #[test]
    fn a_staged_mor_write_commits_its_session_exactly_once() {
        let fixture = row_mutation_session_fixture(true, known_uncommitted_outcome());
        let execution = mor_update_executor(Arc::clone(&fixture.session));
        let completion =
            MutationCommitCompletion::Session(empty_session_completion(&fixture.session));

        execution
            .commit_terminal(completion)
            .expect("the session performs the one external commit");
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.calls.lock().expect("recorded calls").finish, 1);

        // A second terminal decision on the same session is refused, and the
        // connector is not asked again.
        let error = execution
            .abort_terminal()
            .expect_err("a committed session cannot also abort");
        assert!(error.contains("already reached"), "{error}");
        assert_eq!(fixture.session.finish_invocations(), 1);
        let calls = fixture.calls.lock().expect("recorded calls");
        assert_eq!(calls.finish, 1);
        assert_eq!(calls.abort, 0);
    }

    /// A merge-on-read write whose data plane never closed reaches the provider
    /// zero times.
    ///
    /// Staging fails before dispatch, the statement takes the abort branch, and
    /// the session is released. `finish_invocations` is what makes "the
    /// connector was never asked to commit" an assertable fact rather than an
    /// inference.
    #[test]
    fn a_mor_write_whose_data_plane_never_closed_never_reaches_the_provider() {
        let fixture = row_mutation_session_fixture(true, known_uncommitted_outcome());
        let execution = mor_update_executor(Arc::clone(&fixture.session));

        let reason = execution
            .run_stage(&PanicOnEncodeNativeEncoder)
            .expect_err("a stage with no dispatched plan fails");
        assert!(reason.contains("already consumed"), "{reason}");
        assert!(execution.needs_abort_on_stage_error());
        assert_eq!(fixture.session.finish_invocations(), 0);

        assert_eq!(
            execution.abort_terminal().expect("release the session"),
            novarocks_spi::connector::ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            },
        );
        assert_eq!(fixture.session.finish_invocations(), 0);
        let calls = fixture.calls.lock().expect("recorded calls");
        assert_eq!(calls.finish, 0);
        assert_eq!(calls.abort, 1);
    }

    /// A merge-on-read write that matched nothing produced no commit fragment.
    /// Committing it would publish a snapshot describing nothing, so the session
    /// is released and the connector is never asked to commit.
    #[test]
    fn an_empty_mor_write_releases_its_session_without_committing() {
        let fixture = row_mutation_session_fixture(true, known_uncommitted_outcome());
        let execution = mor_update_executor(Arc::clone(&fixture.session));
        let completion = empty_session_completion(&fixture.session);
        assert!(completion.is_empty());

        execution
            .release_empty_write_session(&completion)
            .expect("release an empty write session");
        assert_eq!(fixture.session.finish_invocations(), 0);
        let calls = fixture.calls.lock().expect("recorded calls");
        assert_eq!(calls.finish, 0);
        assert_eq!(calls.abort, 1);
    }

    struct PanicOnEncodeNativeEncoder;

    impl crate::query_execution::dml::mutation::MutationNativeFragmentEncoder
        for PanicOnEncodeNativeEncoder
    {
        fn encode(
            &self,
            _input: &crate::query_execution::compiler::NativeFragmentEncodingInput,
        ) -> Result<crate::query_execution::native_fragment::NativeFragmentAttachment, String>
        {
            panic!("a stage that never reached dispatch must not encode a bundle")
        }
    }
}
