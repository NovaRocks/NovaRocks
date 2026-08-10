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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, Int64Array, StringArray};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_connector_iceberg::iceberg::Catalog;
use novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome, IcebergUpdateMode};
use crate::connector::iceberg::commit::{
    CommitServiceError, IcebergCommitCollector, ensure_iceberg_write_supported,
    select_iceberg_update_mode,
};
use crate::connector::iceberg::write_commit::IcebergWriteCommitExecutor;
use crate::engine::StandaloneState;
use crate::engine::query_planning::bindings::QueryTableBindingStore;
use crate::engine::query_planning::write_sink::{
    admit_prepared_connector_write_target, sql_write_plan_input_for_admitted_target,
};
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::runtime::query_result::QueryResult;
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::parser::ast::{
    MergeMatchedAction, MergeNotMatchedAction, MergeStmt, ObjectName, UpdateStmt,
};

fn write_commit_has_files(write_commit: &crate::query_execution::write::WriteCommitInput) -> bool {
    write_commit
        .writers
        .iter()
        .any(|writer| !writer.connector_staged_report_frames.is_empty())
}

fn row_lineage_input_request(
    columns: &[novarocks_catalog::schema::ColumnDef],
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

fn data_input_request(
    columns: &[novarocks_catalog::schema::ColumnDef],
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
    UpdateMor,
    Merge {
        matched_update: bool,
        matched_delete: bool,
        not_matched_insert: bool,
    },
}

/// Provider-signed row-mutation admission retained before stage. It is pure:
/// routes are activated only after the frontend has persisted the operation
/// intent that owns this exact operation id.
#[derive(Clone)]
struct DmlChangeStreamPreparations {
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    lease: novarocks_spi::connector::ConnectorWriteLease,
    preparation: novarocks_spi::connector::ConnectorRowMutationPreparation,
    context: novarocks_spi::connector::ConnectorRequestContext,
}

/// Provider-signed opaque route set available only during post-intent staging.
#[derive(Clone)]
struct ActivatedDmlChangeStreamPreparations {
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    routes: Vec<novarocks_spi::connector::ConnectorRowMutationRoute>,
}

impl DmlChangeStreamPreparations {
    fn prepare(
        materialization: &crate::connector::iceberg::provider::IcebergQueryTableMaterialization,
        target_ref: &str,
        effect_set: DmlRowMutationEffectSet,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Self, String> {
        use novarocks_spi::connector::{ConnectorRowMutationIntent, ConnectorWriteOperationId};

        let intent = match effect_set {
            DmlRowMutationEffectSet::UpdateMor => ConnectorRowMutationIntent::Update,
            DmlRowMutationEffectSet::Merge { .. } => ConnectorRowMutationIntent::Merge {
                effects: effect_set.effects(),
            },
        };
        let operation_id = ConnectorWriteOperationId::new();
        let (lease, preparation) = materialization.prepare_row_mutation(
            target_ref,
            operation_id,
            intent,
            context.clone(),
        )?;
        Ok(Self {
            operation_id,
            lease,
            preparation,
            context,
        })
    }
}

impl DmlChangeStreamPreparations {
    fn activate(&self) -> Result<ActivatedDmlChangeStreamPreparations, String> {
        let plan = self
            .lease
            .activate_row_mutation(
                novarocks_spi::connector::ConnectorRowMutationActivationRequest::Direct {
                    preparation: self.preparation.clone(),
                    context: self.context.clone(),
                },
            )
            .map_err(|error| {
                format!("activate Iceberg row mutation after durable intent: {error}")
            })?;
        Ok(ActivatedDmlChangeStreamPreparations {
            operation_id: self.operation_id,
            routes: plan.routes().to_vec(),
        })
    }
}

impl ActivatedDmlChangeStreamPreparations {
    fn primary(&self) -> &novarocks_spi::connector::ConnectorWritePreparation {
        self.routes
            .first()
            .expect("row-mutation route plan is non-empty")
            .preparation()
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

struct DmlChangeStreamWritePlan {
    producer: crate::sql::optimizer::OptimizedOperatorNode,
    dag: crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
    table_bindings: Arc<QueryTableBindingStore>,
    execution: QueryExecutionContext,
}

#[derive(Clone, Debug)]
struct DmlPreExpandKeyedAssert {
    key_column_name: String,
    key_label: String,
    message_prefix: String,
}

#[allow(clippy::too_many_arguments)]
fn build_dml_change_stream_write_plan(
    target: &crate::engine::backend_resolver::TargetBackend,
    producer: crate::sql::optimizer::OptimizedOperatorNode,
    table_bindings: Arc<QueryTableBindingStore>,
    execution: QueryExecutionContext,
    _effect_set: DmlRowMutationEffectSet,
    preparations: &ActivatedDmlChangeStreamPreparations,
) -> Result<DmlChangeStreamWritePlan, String> {
    use crate::sql::planner::distributed::write::change_stream::{
        ChangeStreamWriteLayoutRequest, ChangeStreamWriteLayoutRoute,
        bind_change_stream_write_layout,
    };
    use novarocks_spi::connector::{ConnectorMutationRouteInput, ConnectorWriteInputShape};

    let mut routes = Vec::new();
    for route in &preparations.routes {
        let target_binding = table_bindings.admitted_iceberg_write_binding_id_for_preparation(
            &target.catalog,
            &target.namespace,
            &target.table,
            route.preparation(),
        )?;
        let mode = match route.input() {
            ConnectorWriteInputShape::Data { .. } => {
                crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::Data
            }
            ConnectorWriteInputShape::RowLineage { .. } => {
                crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::RowLineageData
            }
            ConnectorWriteInputShape::PositionDelete { .. } => {
                crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::PositionDeletes
            }
            ConnectorWriteInputShape::DeletionVector { .. } => {
                crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::DeletionVectors
            }
            ConnectorWriteInputShape::EqualityDelete { .. } => {
                crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::EqualityDeletes
            }
        };
        let sink = sql_write_plan_input_for_admitted_target(
            table_bindings.as_ref(),
            target_binding,
            mode,
            crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
            None,
        )
        .map_err(|error| format!("build row-mutation route sink: {error}"))?;
        // The Provider signs route field tokens, while SQL owns the physical
        // producer layout.  Bind each token to this exact producer output here
        // rather than treating the provider's match-contract ordinal as a
        // planner output ordinal.  A logical Replace can therefore fan out to
        // an identity-only delete route and an after-image data route.
        let input_ordinals = route
            .input()
            .fields()
            .into_iter()
            .map(|field| {
                producer
                    .output_columns
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(field.field().name()))
                    .ok_or_else(|| {
                        format!(
                            "row-mutation producer has no output for Provider route field `{}`",
                            field.field().name()
                        )
                    })
                    .and_then(|ordinal| {
                        u32::try_from(ordinal).map_err(|_| {
                            "row-mutation producer output ordinal exceeds u32".to_string()
                        })
                    })
                    .map(|ordinal| ConnectorMutationRouteInput::new(field.token(), ordinal))
            })
            .collect::<Result<Vec<_>, _>>()?;
        routes.push(ChangeStreamWriteLayoutRoute {
            route_id: route.route_id(),
            cohort_id: route.cohort_id(),
            accepted_effects: route.accepted_effects().to_vec(),
            input_ordinals,
            partition_input_tokens: route.partition_fields().to_vec(),
            sink,
        });
    }
    let effect_output_ordinal = producer
        .output_columns
        .iter()
        .position(|column| column.name == crate::sql::common::ROW_MUTATION_EFFECT_COLUMN)
        .ok_or_else(|| "row-mutation producer has no logical effect output".to_string())?;
    let dag = bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
        producer_output_columns: &producer.output_columns,
        effect_output_ordinal,
        routes,
    })?;
    Ok(DmlChangeStreamWritePlan {
        producer,
        dag,
        pre_expand_keyed_assert: None,
        table_bindings,
        execution,
    })
}

fn plan_dml_change_stream_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    plan: &mut DmlChangeStreamWritePlan,
) -> Result<crate::engine::PlannedIcebergChangeStreamWrite, String> {
    let keyed_assert = plan.pre_expand_keyed_assert.as_ref().map(|keyed_assert| {
        crate::sql::planner::physical::PreExpandKeyedAssertSpec {
            key_column_name: keyed_assert.key_column_name.clone(),
            key_label: keyed_assert.key_label.clone(),
            message_prefix: keyed_assert.message_prefix.clone(),
        }
    });
    crate::engine::build_physical_plan_as_iceberg_change_stream_write(
        state,
        Some(&target.catalog),
        &target.namespace,
        &plan.producer,
        Some(plan.table_bindings.as_ref()),
        &mut plan.dag,
        None,
        keyed_assert,
    )
}

fn target_partition_source_column_names(
    data_sink: Option<&crate::sql::planner::distributed::write::contract::SqlWritePlanInput>,
) -> Result<Vec<String>, String> {
    let Some(data_sink) = data_sink else {
        return Ok(Vec::new());
    };
    // Partition transforms are Provider-owned.  The signed input shape
    // already contains the complete Arrow layout SQL must produce; it must
    // not reconstruct partition source field IDs from Iceberg metadata.
    let _ = data_sink;
    Ok(Vec::new())
}

/// Core-private staged mutation execution retained behind `MutationEngine`'s
/// opaque handles.  It intentionally has no journal or SQL routing policy.
pub(crate) trait MutationExecution: Send + Sync {
    fn stage(&self) -> Result<QueryExecutionResult, String>;
    fn needs_abort_on_stage_error(&self) -> bool {
        false
    }
    fn abort(&self, reason: String) -> Result<CommitOutcome, CommitServiceError>;
    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String>;
    fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError>;
    fn commit_terminal(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        crate::connector::iceberg::write_control::terminal_outcome_from_iceberg_commit(
            completion.session().owner(),
            completion.session().operation_id(),
            self.commit(completion),
        )
    }
    fn finalize(&self) -> Result<(), String>;
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
        completion: crate::query_execution::ConnectorWriteCompletion,
    },
}

pub(crate) struct PreparedUpdateMutation {
    pub(crate) stmt: UpdateStmt,
    pub(crate) current_catalog: Option<String>,
    pub(crate) target: crate::engine::backend_resolver::TargetBackend,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) table_ident: novarocks_connector_iceberg::iceberg::TableIdent,
    pub(crate) table: novarocks_connector_iceberg::iceberg::table::Table,
    pub(crate) target_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub(crate) entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    pub(crate) target_ref: String,
    /// The one exact connector generation admitted with this statement.
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    pub(crate) mor_write_target: Option<PreparedMorUpdateWriteTarget>,
    pub(crate) mode: IcebergUpdateMode,
    pub(crate) execution: QueryExecutionContext,
    pub(crate) connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

/// MOR-only writer facts frozen during UPDATE admission.
///
/// COW UPDATE retains its existing per-file application lifecycle.  In
/// contrast, MOR builds one SQL change-stream producer after the frontend has
/// persisted the mutation intent, so its writer target must be frozen here.
pub(crate) struct PreparedMorUpdateWriteTarget {
    /// The branch/current snapshot selected during admission. MOR production
    /// planning must not observe a later branch head after the frontend has
    /// recorded the mutation intent.
    pub(crate) read_snapshot_id: Option<i64>,
    /// Provider-signed writer facts frozen with `planning_lease`. They are
    /// admitted into the same query-local store as the producer compile, never
    /// rebuilt during stage/preparation.
    pub(crate) preparations: DmlChangeStreamPreparations,
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

pub(crate) struct PreparedMergeMutation {
    pub(crate) stmt: MergeStmt,
    pub(crate) current_catalog: Option<String>,
    pub(crate) target: crate::engine::backend_resolver::TargetBackend,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) table_ident: novarocks_connector_iceberg::iceberg::TableIdent,
    pub(crate) table: novarocks_connector_iceberg::iceberg::table::Table,
    pub(crate) target_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub(crate) entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    pub(crate) table_write_mode: IcebergUpdateMode,
    /// The one exact connector generation admitted with this statement.
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    pub(crate) mor_write_target: Option<PreparedMorMergeWriteTarget>,
    pub(crate) insert_columns_resolved: Option<MergeInsertColumns>,
    pub(crate) execution: QueryExecutionContext,
    pub(crate) connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

/// Frozen MOR writer facts for MERGE.  The producer query and its terminal
/// sink must use the same admission lease and physical target envelope.
pub(crate) struct PreparedMorMergeWriteTarget {
    pub(crate) preparations: DmlChangeStreamPreparations,
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

pub(crate) fn prepare_update_mutation(
    state: &Arc<StandaloneState>,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
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

    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = novarocks_connector_iceberg::iceberg::TableIdent::new(
        novarocks_connector_iceberg::iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_properties(
        &target,
        table.metadata().properties(),
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Update,
    )?;

    // Branch writes require Iceberg v3 (row-lineage semantics).
    if target_ref != "main" {
        let fmt = table.metadata().format_version();
        if fmt != novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3 {
            return Err(format!(
                "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                table_ident, fmt as u8,
            ));
        }
    }

    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;
    validate_update_assignments(&stmt.assignments, &target_columns, &partition_columns)?;

    let mode = select_iceberg_update_mode(&table)?;
    let planning_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        &target.catalog,
    )?;
    let mor_write_target = if mode == IcebergUpdateMode::MergeOnRead {
        let read_snapshot_id = if target_ref != "main" {
            novarocks_connector_iceberg::ref_snapshot::resolve_branch_head_snapshot_id(
                table.metadata(),
                &target_ref,
            )?
        } else {
            table
                .metadata()
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id())
        };
        // Freeze the writer target at admission, alongside the prepared
        // mutation. Stage runs after frontend lifecycle persistence and must
        // never reopen the connector generation or observe a later snapshot.
        let materialization =
            crate::connector::iceberg::provider::load_schema_materialization_from_exact_lease(
                planning_lease.clone(),
                connector_context.clone(),
                &target.namespace,
                &target.table,
            )?;
        // Retain the lease returned beside the provider facts, rather than an
        // independent clone, so the stored writer envelope has one explicit
        // generation authority.
        let planning_lease = materialization.planning_lease.clone();
        let preparations = DmlChangeStreamPreparations::prepare(
            &materialization,
            &target_ref,
            DmlRowMutationEffectSet::UpdateMor,
            connector_context.clone(),
        )?;
        Some(PreparedMorUpdateWriteTarget {
            read_snapshot_id,
            preparations,
            planning_lease,
        })
    } else {
        None
    };
    Ok(PreparedUpdateMutation {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        target,
        catalog,
        table_ident,
        table,
        target_columns,
        entry,
        target_ref,
        planning_lease,
        mor_write_target,
        mode,
        execution: execution.clone(),
        connector_context: connector_context.clone(),
    })
}

/// Resolve and validate MERGE without materializing source rows, registering a
/// cohort, or creating a staging artifact. It retains one exact planning lease
/// for every later read or writer admission.
pub(crate) fn prepare_merge_mutation(
    state: &Arc<StandaloneState>,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedMergeMutation, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "MERGE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|error| format!("iceberg catalog registry read lock: {error}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = novarocks_connector_iceberg::iceberg::TableIdent::new(
        novarocks_connector_iceberg::iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|error| format!("load iceberg table {}: {error}", &table_ident))?;
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_properties(
        &target,
        table.metadata().properties(),
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Merge,
    )?;
    let _ = ensure_iceberg_write_supported(&table)?;
    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;
    let table_write_mode = select_iceberg_update_mode(&table)?;
    if let Some(clause) = stmt.matched.as_ref()
        && let MergeMatchedAction::Update { assignments } = &clause.action
    {
        validate_update_assignments(assignments, &target_columns, &partition_columns)?;
    }
    let insert_columns_resolved = stmt
        .not_matched
        .as_ref()
        .map(|clause| resolve_merge_insert_columns(&clause.action, &target_columns))
        .transpose()?;
    let planning_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        &target.catalog,
    )?;
    let mor_write_target = if table_write_mode == IcebergUpdateMode::MergeOnRead
        || matches!(
            stmt.matched.as_ref().map(|clause| &clause.action),
            Some(MergeMatchedAction::Delete)
        ) {
        let materialization =
            crate::connector::iceberg::provider::load_schema_materialization_from_exact_lease(
                planning_lease.clone(),
                connector_context.clone(),
                &target.namespace,
                &target.table,
            )?;
        let planning_lease = materialization.planning_lease.clone();
        let effect_set = DmlRowMutationEffectSet::Merge {
            matched_update: matches!(
                stmt.matched.as_ref().map(|clause| &clause.action),
                Some(MergeMatchedAction::Update { .. })
            ),
            matched_delete: matches!(
                stmt.matched.as_ref().map(|clause| &clause.action),
                Some(MergeMatchedAction::Delete)
            ),
            not_matched_insert: stmt.not_matched.is_some(),
        };
        let preparations = DmlChangeStreamPreparations::prepare(
            &materialization,
            "main",
            effect_set,
            connector_context.clone(),
        )?;
        Some(PreparedMorMergeWriteTarget {
            preparations,
            planning_lease,
        })
    } else {
        None
    };
    Ok(PreparedMergeMutation {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        target,
        catalog,
        table_ident,
        table,
        target_columns,
        entry,
        table_write_mode,
        planning_lease,
        mor_write_target,
        insert_columns_resolved,
        execution: execution.clone(),
        connector_context: connector_context.clone(),
    })
}

/// Execute the post-intent half of an UPDATE. Preparation above only freezes
/// validation and connector planning facts; match materialization, cohort
/// registration and distributed staging happen only here, after the frontend
/// has persisted its `Preparing` record.
pub(crate) fn stage_prepared_update_mutation(
    state: &Arc<StandaloneState>,
    prepared: PreparedUpdateMutation,
) -> Result<MutationStagedWrite, String> {
    let PreparedUpdateMutation {
        stmt,
        current_catalog,
        target,
        catalog,
        table_ident,
        table,
        target_columns,
        entry,
        target_ref,
        planning_lease,
        mor_write_target,
        mode,
        execution,
        connector_context,
    } = prepared;
    match mode {
        IcebergUpdateMode::CopyOnWrite => {
            let matched = materialize_update_matches(
                state,
                &target,
                &stmt,
                current_catalog.as_deref(),
                &execution,
                &connector_context,
            )?;
            if matched.row_ids.is_empty() {
                return Ok(MutationStagedWrite::NoOp);
            }
            let materialization =
                crate::connector::iceberg::provider::load_schema_materialization_from_exact_lease(
                    planning_lease.clone(),
                    connector_context.clone(),
                    &target.namespace,
                    &target.table,
                )?;
            let operation_id = novarocks_spi::connector::ConnectorWriteOperationId::new();
            let (row_mutation_lease, row_mutation_preparation) = materialization
                .prepare_row_mutation(
                    &target_ref,
                    operation_id,
                    novarocks_spi::connector::ConnectorRowMutationIntent::Update,
                    connector_context.clone(),
                )?;
            let selection = cow_selection_from_matched_update(
                &matched,
                &row_mutation_preparation,
                connector_context.clone(),
            )?;
            let provider_plan = row_mutation_lease
                .activate_row_mutation(
                    novarocks_spi::connector::ConnectorRowMutationActivationRequest::CopyOnWrite {
                        preparation: row_mutation_preparation,
                        selection,
                        context: connector_context.clone(),
                    },
                )
                .map_err(|error| format!("activate Provider COW UPDATE plan: {error}"))?;
            let provider_binding =
                crate::connector::iceberg::provider::bind_iceberg_cow_execution_plan(
                    &provider_plan,
                )
                .map_err(|error| format!("bind Provider COW UPDATE plan: {error}"))?;
            let metadata = table.metadata();
            let base_snapshot_id = if target_ref != "main" {
                novarocks_connector_iceberg::ref_snapshot::resolve_branch_head_snapshot_id(
                    metadata,
                    &target_ref,
                )?
            } else {
                metadata
                    .current_snapshot()
                    .map(|snapshot| snapshot.snapshot_id())
            };
            let collector = Arc::new(
                IcebergCommitCollector::new(
                    CommitOpKind::CowUpdate,
                    table_ident,
                    base_snapshot_id,
                    metadata.last_sequence_number(),
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().clone(),
                    format!(
                        "{}/data/_staging/{}",
                        metadata.location(),
                        uuid::Uuid::new_v4()
                    ),
                    novarocks_types::UniqueId::new(0, 0),
                )
                .with_table_metadata(metadata.clone()),
            );
            let write = build_cow_update_distributed_write(
                &target,
                &table,
                &matched,
                &target_columns,
                base_snapshot_id,
                &target_ref,
                planning_lease,
                &connector_context,
                provider_plan,
                provider_binding,
                row_mutation_lease,
            )?;
            let execution_handle = build_cow_update_distributed_execution(
                state,
                &target,
                catalog,
                table,
                collector,
                entry,
                &target_ref,
                write,
                execution,
                &connector_context,
            )?;
            let result = match execution_handle.stage() {
                Ok(result) => result,
                Err(reason) => {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason,
                        execution: execution_handle,
                    });
                }
            };
            let Some(completion) = result.connector_completion else {
                return Ok(MutationStagedWrite::AbortRequired {
                    reason: "COW UPDATE staged without a connector completion".to_string(),
                    execution: execution_handle,
                });
            };
            Ok(MutationStagedWrite::CommitRequired {
                execution: execution_handle,
                completion,
            })
        }
        IcebergUpdateMode::MergeOnRead => {
            let PreparedMorUpdateWriteTarget {
                read_snapshot_id,
                preparations,
                planning_lease: write_planning_lease,
            } = mor_write_target.ok_or_else(|| {
                "MOR UPDATE reached stage without an admitted frozen write target".to_string()
            })?;
            let preparations = preparations.activate()?;
            let write_lease = write_planning_lease
                .derive_write_lease()
                .map_err(|error| format!("derive MOR UPDATE write lease: {error}"))?;
            let metadata = table.metadata();
            let collector = Arc::new(
                IcebergCommitCollector::new(
                    CommitOpKind::RowDeltaDvFromFiles,
                    table_ident,
                    read_snapshot_id,
                    metadata.last_sequence_number(),
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().clone(),
                    format!(
                        "{}/data/_staging/{}",
                        metadata.location(),
                        uuid::Uuid::new_v4()
                    ),
                    novarocks_types::UniqueId::new(0, 0),
                )
                .with_table_metadata(metadata.clone()),
            );
            let write = build_update_mor_change_stream_write_plan(
                state,
                &target,
                &stmt,
                current_catalog.as_deref(),
                &target_columns,
                &target_ref,
                metadata.last_sequence_number() + 1,
                &execution,
                &connector_context,
                &preparations,
                write_planning_lease,
            )?;
            let abort_cleanup =
                crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
            let commit_executor = Arc::new(IcebergWriteCommitExecutor {
                catalog,
                table,
                collector,
                fs: abort_cleanup.fs,
                cleanup_path_mapper: abort_cleanup.path_mapper,
                cow_update_rewrite: None,
                target_ref: target_ref.clone(),
                snapshot_properties: BTreeMap::new(),
            });
            let operation_id = preparations.operation_id;
            let mut write = write;
            let planned = plan_dml_change_stream_write(state, &target, &mut write)?;
            let provider_binding = Arc::new(
                crate::connector::iceberg::change_stream_write::bind_iceberg_change_stream_provider(
                    crate::connector::iceberg::change_stream_write::IcebergChangeStreamProviderRequest {
                        target: &format!("{}.{}.{}", target.catalog, target.namespace, target.table),
                        target_ref: &target_ref,
                        table: &commit_executor.table,
                        entry: &entry,
                    base_snapshot_id: read_snapshot_id,
                    operation_id,
                    topology: &planned.topology,
                    table_bindings: write.table_bindings.as_ref(),
                    commit_executor: Arc::clone(&commit_executor),
                    },
                )?,
            );
            let connector_write =
                crate::engine::iceberg_writer::iceberg_change_stream_provider_binding_template(
                    state,
                    &target,
                    &provider_binding,
                    operation_id,
                    connector_context.clone(),
                    &write_lease,
                    preparations.primary(),
                )?;
            let execution_handle = Arc::new(MorUpdateChangeStreamExecutor {
                state: Arc::clone(state),
                target: target.clone(),
                planned: Mutex::new(Some(planned)),
                connector_write,
                provider_binding,
                commit_executor,
                execution,
                connector_context,
                write_lease,
                operation_session: Mutex::new(None),
            });
            let result = match execution_handle.stage() {
                Ok(result) => result,
                Err(reason) => {
                    if execution_handle.needs_abort_on_stage_error() {
                        return Ok(MutationStagedWrite::AbortRequired {
                            reason,
                            execution: execution_handle,
                        });
                    }
                    return Err(reason);
                }
            };
            if let Some(completion) = result.connector_completion.as_ref() {
                let known_empty = match completion.is_known_empty() {
                    Ok(known_empty) => known_empty,
                    Err(error) => {
                        return Ok(MutationStagedWrite::AbortRequired {
                            reason: format!(
                                "summarize MOR UPDATE change-stream aggregate: {error}"
                            ),
                            execution: execution_handle,
                        });
                    }
                };
                if known_empty {
                    if let Err(reason) = execution_handle.finish_known_empty_noop(completion) {
                        return Ok(MutationStagedWrite::AbortRequired {
                            reason,
                            execution: execution_handle,
                        });
                    }
                    return Ok(MutationStagedWrite::NoOp);
                }
            } else if let Some(commit) = result.write_commit.as_ref()
                && !write_commit_has_files(commit)
            {
                if commit.writers.iter().any(|writer| writer.loaded_rows > 0) {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason:
                            "MOR UPDATE change-stream write produced rows but no data or DV files"
                                .to_string(),
                        execution: execution_handle,
                    });
                }
                return Ok(MutationStagedWrite::AbortRequired {
                    reason: "MOR UPDATE missing connector completion for an empty aggregate"
                        .to_string(),
                    execution: execution_handle,
                });
            }
            let Some(completion) = result.connector_completion else {
                return Ok(MutationStagedWrite::AbortRequired {
                    reason: "MOR UPDATE staged without a connector completion".to_string(),
                    execution: execution_handle,
                });
            };
            Ok(MutationStagedWrite::CommitRequired {
                execution: execution_handle,
                completion,
            })
        }
    }
}

fn materialize_update_matches(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &UpdateStmt,
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
        .map(|assignment| (assignment.column.as_str(), assignment.value.to_string()))
        .collect::<Vec<_>>();
    let assignments_sql = assignments_sql
        .iter()
        .map(|(column, expr)| (*column, expr.as_str()))
        .collect::<Vec<_>>();
    let where_sql = stmt.where_clause.as_ref().map(|expr| expr.to_string());
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let match_sql = build_update_match_query_sql(
        &target_sql,
        target_alias,
        source_sql.as_deref(),
        &assignments_sql,
        where_sql.as_deref(),
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
    state: &Arc<StandaloneState>,
    source: &Option<crate::sql::parser::ast::MutationSource>,
    current_catalog: Option<&str>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<Option<String>, String> {
    match source {
        None => Ok(None),
        Some(source) => {
            mutation_source_relation_to_sql(state, source, current_catalog, target).map(Some)
        }
    }
}

fn mutation_source_relation_to_sql(
    state: &Arc<StandaloneState>,
    source: &crate::sql::parser::ast::MutationSource,
    current_catalog: Option<&str>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<String, String> {
    use crate::sql::parser::ast::MutationSource;
    match source {
        MutationSource::Table { name, alias } => {
            // The match SELECT runs with `current_database = target.namespace`
            // and `current_catalog = Some(target.catalog)`. Resolve the source
            // against the user's surface name to get its concrete (catalog,
            // namespace, table). Emit a 1-part name when the source shares the
            // target's namespace+catalog (lets refresh follow the
            // current-catalog path), and a 2-part `<namespace>.<table>` name
            // otherwise so the standalone analyzer can find it directly.
            let resolved = crate::engine::backend_resolver::resolve_existing_table_target(
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
        MutationSource::Query { query, alias } => {
            let alias = alias
                .as_deref()
                .ok_or_else(|| "MERGE/UPDATE subquery source requires an alias".to_string())?;
            Ok(format!("({query}) AS {alias}"))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_update_mor_change_stream_write_plan(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    target_ref: &str,
    new_sequence_number: i64,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    preparations: &ActivatedDmlChangeStreamPreparations,
    write_planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<DmlChangeStreamWritePlan, String> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let where_sql = stmt.where_clause.as_ref().map(|expr| expr.to_string());
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
        where_sql.as_deref(),
    );
    let mut query = parse_generated_query(&match_sql, "MOR UPDATE change-stream producer")?;
    if crate::engine::query_prep::has_time_travel_refs(&query) {
        crate::engine::query_prep::rewrite_time_travel_refs(
            state,
            Some(&target.catalog),
            &target.namespace,
            &mut query,
            connector_context,
        )?;
    }

    let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
    let analyzer_provider = crate::engine::build_catalog_service_provider(
        Some(&target.catalog),
        &catalog_service_snapshot,
        state.connector_control.as_ref(),
        connector_context.clone(),
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let table_bindings = analyzer_provider.query_table_bindings();
    // This admission uses the exact lease and table facts selected above.  It
    // intentionally precedes compilation, so `build_dml_change_stream_write_plan`
    // can recover the write token from the same store that resolves producer
    // scans. No preparation phase is allowed to reacquire current/latest.
    for route in &preparations.routes {
        crate::engine::query_planning::write_sink::admit_prepared_connector_write_target(
            table_bindings.as_ref(),
            crate::sql::planner::table::SqlTableIdentity {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            route.preparation().clone(),
            write_planning_lease.clone(),
        )?;
    }
    let planned = crate::engine::plan_query_for_iceberg_change_stream_refresh(
        state,
        &query,
        &analyzer_provider,
        &target.namespace,
        None,
        table_bindings,
        execution,
    )?;
    let producer = build_update_mor_change_event_expand_plan(
        planned.optimized_tree,
        target_columns,
        new_sequence_number,
    )?;
    let mut plan = build_dml_change_stream_write_plan(
        target,
        producer,
        planned.table_bindings.ok_or_else(|| {
            "MOR UPDATE change-stream compilation did not retain query table bindings".to_string()
        })?,
        execution.clone(),
        DmlRowMutationEffectSet::UpdateMor,
        preparations,
    )?;
    plan.pre_expand_keyed_assert = Some(DmlPreExpandKeyedAssert {
        key_column_name: "__nr_row_id".to_string(),
        key_label: novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
        message_prefix: "MOR UPDATE matched target row".to_string(),
    });
    Ok(plan)
}

fn update_assignment_projection_sql(
    assignments: &[crate::sql::parser::ast::UpdateAssignment],
    target_columns: &[novarocks_catalog::schema::ColumnDef],
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
                crate::engine::iceberg_writer::target_cast_expr_sql(
                    &format!("({})", assignment.value),
                    target_column,
                )?,
            ))
        })
        .collect()
}

fn update_change_stream_target_sql(
    target: &crate::engine::backend_resolver::TargetBackend,
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

fn build_update_mor_change_event_expand_plan(
    optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    new_sequence_number: i64,
) -> Result<crate::sql::optimizer::OptimizedOperatorNode, String> {
    use crate::sql::optimizer::operator::{
        ChangeEventExpandOp, ChangeEventOutputExpr, ChangeEventSpec, Operator,
        PhysicalDistributionOp,
    };
    use crate::sql::optimizer::optimized_tree::{
        OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarNode};

    let mut scalar_arena = optimized_tree
        .execution_props
        .scalar_arena
        .as_deref()
        .cloned()
        .ok_or_else(|| "MOR UPDATE physical plan is missing scalar arena".to_string())?;
    let child_outputs = optimized_tree.output_columns.clone();
    let row_id_input = output_column_by_name(&child_outputs, "__nr_row_id", "UPDATE row id")?;
    let hash_distribution = DistributionSpec::shuffle_agg([row_id_input.column_id]);

    let child_stats = optimized_tree.stats.clone();
    let distributed = OptimizedOperatorNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: hash_distribution,
        }),
        children: vec![optimized_tree],
        stats: child_stats.clone(),
        explain_stats: OptimizerExplainStats::default(),
        output_columns: child_outputs.clone(),
        execution_props: PlanExecutionProps::default(),
    };

    let mut next_column_id = max_physical_column_id(&distributed) + 1;
    let mut alloc_output =
        |name: &str, data_type: arrow::datatypes::DataType, nullable: bool, is_internal: bool| {
            let column = crate::sql::analysis::OutputColumn {
                column_id: crate::sql::column_id::ColumnId(next_column_id),
                name: name.to_string(),
                data_type,
                nullable,
                is_internal,
            };
            next_column_id += 1;
            column
        };

    let file_output = alloc_output(
        novarocks_execution::exec::row_position::ICEBERG_FILE_PATH_COL,
        arrow::datatypes::DataType::Utf8,
        true,
        true,
    );
    let pos_output = alloc_output(
        novarocks_execution::exec::row_position::ICEBERG_ROW_POS_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let mut target_outputs = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        target_outputs.push((
            column.name.clone(),
            alloc_output(
                &column.name,
                column.data_type.clone(),
                column.nullable,
                false,
            ),
        ));
    }
    let row_id_output = alloc_output(
        novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let last_sequence_output = alloc_output(
        novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let effect_output = alloc_output(
        crate::sql::common::change_stream::ROW_MUTATION_EFFECT_COLUMN,
        arrow::datatypes::DataType::Int8,
        false,
        true,
    );

    let file_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_file",
        "UPDATE old file",
    )?;
    let pos_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_pos",
        "UPDATE old row position",
    )?;
    let row_id_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_row_id",
        "UPDATE old row id",
    )?;
    let new_sequence_expr = scalar_arena.intern(
        ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(
            new_sequence_number,
        ))),
        arrow::datatypes::DataType::Int64,
        false,
    );

    let mut reuse_assignments = vec![
        ChangeEventOutputExpr {
            output_column_id: file_output.column_id,
            expr: Some(file_expr),
        },
        ChangeEventOutputExpr {
            output_column_id: pos_output.column_id,
            expr: Some(pos_expr),
        },
    ];
    for (name, output) in &target_outputs {
        let old_expr = child_column_expr(
            &mut scalar_arena,
            &child_outputs,
            name,
            "UPDATE old target column",
        )?;
        let _ = old_expr;

        let new_name = format!("__nr_new_{name}");
        let new_expr = match maybe_output_column_by_name(&child_outputs, &new_name)? {
            Some(column) => scalar_arena.intern(
                ScalarNode::ColumnRef(column.column_id),
                column.data_type.clone(),
                column.nullable,
            ),
            None => child_column_expr(
                &mut scalar_arena,
                &child_outputs,
                name,
                "UPDATE unchanged target column",
            )?,
        };
        reuse_assignments.push(ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(new_expr),
        });
    }
    reuse_assignments.push(ChangeEventOutputExpr {
        output_column_id: row_id_output.column_id,
        expr: Some(row_id_expr),
    });
    reuse_assignments.push(ChangeEventOutputExpr {
        output_column_id: last_sequence_output.column_id,
        expr: Some(new_sequence_expr),
    });

    let mut output_columns = Vec::with_capacity(target_columns.len() + 6);
    output_columns.push(file_output);
    output_columns.push(pos_output);
    output_columns.extend(target_outputs.into_iter().map(|(_, column)| column));
    output_columns.push(row_id_output.clone());
    output_columns.push(last_sequence_output);
    output_columns.push(effect_output.clone());

    let stats = child_stats;
    let mut root = OptimizedOperatorNode {
        op: Operator::PhysicalChangeEventExpand(ChangeEventExpandOp {
            events: vec![ChangeEventSpec {
                predicate: None,
                effect: novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
                assignments: reuse_assignments,
            }],
            output_columns: output_columns.clone(),
            effect_column_id: effect_output.column_id,
        }),
        children: vec![distributed],
        stats,
        explain_stats: OptimizerExplainStats::default(),
        output_columns,
        execution_props: PlanExecutionProps::default(),
    };
    crate::sql::optimizer::optimized_tree::attach_scalar_arena(&mut root, Arc::new(scalar_arena));
    Ok(root)
}

fn build_merge_mor_change_event_expand_plan(
    optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    new_sequence_number: i64,
    matched_update: bool,
    matched_delete: bool,
    not_matched_insert: bool,
) -> Result<crate::sql::optimizer::OptimizedOperatorNode, String> {
    use crate::sql::common::BinOp;
    use crate::sql::optimizer::operator::{
        ChangeEventExpandOp, ChangeEventOutputExpr, ChangeEventSpec, Operator,
        PhysicalDistributionOp,
    };
    use crate::sql::optimizer::optimized_tree::{
        OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarNode};

    let mut scalar_arena = optimized_tree
        .execution_props
        .scalar_arena
        .as_deref()
        .cloned()
        .ok_or_else(|| "MOR MERGE physical plan is missing scalar arena".to_string())?;
    let child_outputs = optimized_tree.output_columns.clone();
    let assert_key_input =
        output_column_by_name(&child_outputs, "__nr_merge_assert_key", "MERGE assert key")?;
    let hash_distribution = DistributionSpec::shuffle_agg([assert_key_input.column_id]);

    let child_stats = optimized_tree.stats.clone();
    let distributed = OptimizedOperatorNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: hash_distribution,
        }),
        children: vec![optimized_tree],
        stats: child_stats.clone(),
        explain_stats: OptimizerExplainStats::default(),
        output_columns: child_outputs.clone(),
        execution_props: PlanExecutionProps::default(),
    };

    let mut next_column_id = max_physical_column_id(&distributed) + 1;
    let mut alloc_output =
        |name: &str, data_type: arrow::datatypes::DataType, nullable: bool, is_internal: bool| {
            let column = crate::sql::analysis::OutputColumn {
                column_id: crate::sql::column_id::ColumnId(next_column_id),
                name: name.to_string(),
                data_type,
                nullable,
                is_internal,
            };
            next_column_id += 1;
            column
        };

    let file_output = alloc_output(
        novarocks_execution::exec::row_position::ICEBERG_FILE_PATH_COL,
        arrow::datatypes::DataType::Utf8,
        true,
        true,
    );
    let pos_output = alloc_output(
        novarocks_execution::exec::row_position::ICEBERG_ROW_POS_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let mut target_outputs = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        target_outputs.push((
            column.name.clone(),
            alloc_output(
                &column.name,
                column.data_type.clone(),
                column.nullable,
                false,
            ),
        ));
    }
    let row_id_output = alloc_output(
        novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let last_sequence_output = alloc_output(
        novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let effect_output = alloc_output(
        crate::sql::common::change_stream::ROW_MUTATION_EFFECT_COLUMN,
        arrow::datatypes::DataType::Int8,
        false,
        true,
    );

    let file_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_file",
        "MERGE old file",
    )?;
    let pos_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_pos",
        "MERGE old row position",
    )?;
    let row_id_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_row_id",
        "MERGE old row id",
    )?;
    let new_sequence_expr = scalar_arena.intern(
        ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(
            new_sequence_number,
        ))),
        arrow::datatypes::DataType::Int64,
        false,
    );

    let mut delete_assignments = vec![
        ChangeEventOutputExpr {
            output_column_id: file_output.column_id,
            expr: Some(file_expr),
        },
        ChangeEventOutputExpr {
            output_column_id: pos_output.column_id,
            expr: Some(pos_expr),
        },
    ];
    let mut reuse_assignments = vec![
        ChangeEventOutputExpr {
            output_column_id: file_output.column_id,
            expr: Some(file_expr),
        },
        ChangeEventOutputExpr {
            output_column_id: pos_output.column_id,
            expr: Some(pos_expr),
        },
    ];
    let mut fresh_assignments = Vec::with_capacity(target_columns.len());
    for (name, output) in &target_outputs {
        let old_expr = child_column_expr(
            &mut scalar_arena,
            &child_outputs,
            name,
            "MERGE old target column",
        )?;
        delete_assignments.push(ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(old_expr),
        });

        let new_name = format!("__nr_new_{name}");
        let reuse_expr = match maybe_output_column_by_name(&child_outputs, &new_name)? {
            Some(column) => scalar_arena.intern(
                ScalarNode::ColumnRef(column.column_id),
                column.data_type.clone(),
                column.nullable,
            ),
            None => child_column_expr(
                &mut scalar_arena,
                &child_outputs,
                name,
                "MERGE unchanged target column",
            )?,
        };
        reuse_assignments.push(ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(reuse_expr),
        });

        let insert_name = format!("__nr_ins_{name}");
        if let Some(column) = maybe_output_column_by_name(&child_outputs, &insert_name)? {
            let fresh_expr = scalar_arena.intern(
                ScalarNode::ColumnRef(column.column_id),
                column.data_type.clone(),
                column.nullable,
            );
            fresh_assignments.push(ChangeEventOutputExpr {
                output_column_id: output.column_id,
                expr: Some(fresh_expr),
            });
        }
    }
    reuse_assignments.push(ChangeEventOutputExpr {
        output_column_id: row_id_output.column_id,
        expr: Some(row_id_expr),
    });
    reuse_assignments.push(ChangeEventOutputExpr {
        output_column_id: last_sequence_output.column_id,
        expr: Some(new_sequence_expr),
    });

    let action_predicate = |arena: &mut crate::sql::optimizer::scalar::ScalarArena,
                            action: i32|
     -> Result<crate::sql::optimizer::scalar::ScalarId, String> {
        let action_expr =
            child_column_expr(arena, &child_outputs, "__nr_merge_action", "MERGE action")?;
        let literal = arena.intern(
            ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(
                i64::from(action),
            ))),
            arrow::datatypes::DataType::Int64,
            false,
        );
        Ok(arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left: action_expr,
                right: literal,
            },
            arrow::datatypes::DataType::Boolean,
            false,
        ))
    };

    let mut events = Vec::new();
    if matched_update {
        let predicate = action_predicate(&mut scalar_arena, MERGE_ACTION_MATCHED_UPDATE)?;
        events.push(ChangeEventSpec {
            predicate: Some(predicate),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
            assignments: reuse_assignments,
        });
    }
    if matched_delete {
        events.push(ChangeEventSpec {
            predicate: Some(action_predicate(
                &mut scalar_arena,
                MERGE_ACTION_MATCHED_DELETE,
            )?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
            assignments: delete_assignments,
        });
    }
    if not_matched_insert {
        events.push(ChangeEventSpec {
            predicate: Some(action_predicate(
                &mut scalar_arena,
                MERGE_ACTION_NOT_MATCHED_INSERT,
            )?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
            assignments: fresh_assignments,
        });
    }
    if events.is_empty() {
        return Err("MOR MERGE change-stream expand requires at least one event".to_string());
    }

    let mut output_columns = Vec::with_capacity(target_columns.len() + 6);
    output_columns.push(file_output);
    output_columns.push(pos_output);
    output_columns.extend(target_outputs.into_iter().map(|(_, column)| column));
    output_columns.push(row_id_output);
    output_columns.push(last_sequence_output);
    output_columns.push(effect_output.clone());

    let mut stats = child_stats;
    let mut root = OptimizedOperatorNode {
        op: Operator::PhysicalChangeEventExpand(ChangeEventExpandOp {
            events,
            output_columns: output_columns.clone(),
            effect_column_id: effect_output.column_id,
        }),
        children: vec![distributed],
        stats,
        explain_stats: OptimizerExplainStats::default(),
        output_columns,
        execution_props: PlanExecutionProps::default(),
    };
    crate::sql::optimizer::optimized_tree::attach_scalar_arena(&mut root, Arc::new(scalar_arena));
    Ok(root)
}

fn output_column_by_name(
    columns: &[crate::sql::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<crate::sql::analysis::OutputColumn, String> {
    maybe_output_column_by_name(columns, name)?.ok_or_else(|| {
        format!("MOR UPDATE change-stream {label} column `{name}` not found in producer output")
    })
}

fn maybe_output_column_by_name(
    columns: &[crate::sql::analysis::OutputColumn],
    name: &str,
) -> Result<Option<crate::sql::analysis::OutputColumn>, String> {
    let mut matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name));
    let Some(column) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(format!(
            "MOR UPDATE change-stream producer column `{name}` is ambiguous"
        ));
    }
    Ok(Some(column.clone()))
}

fn child_column_expr(
    scalar_arena: &mut crate::sql::optimizer::scalar::ScalarArena,
    columns: &[crate::sql::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<crate::sql::optimizer::scalar::ScalarId, String> {
    use crate::sql::optimizer::scalar::ScalarNode;

    let column = output_column_by_name(columns, name, label)?;
    Ok(scalar_arena.intern(
        ScalarNode::ColumnRef(column.column_id),
        column.data_type,
        column.nullable,
    ))
}

fn max_physical_column_id(node: &crate::sql::optimizer::OptimizedOperatorNode) -> u32 {
    node.output_columns
        .iter()
        .map(|column| column.column_id.0)
        .chain(node.children.iter().map(max_physical_column_id))
        .max()
        .unwrap_or(0)
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context} generated non-query statement: {other}")),
    }
}

fn qualify_iceberg_table(target: &crate::engine::backend_resolver::TargetBackend) -> String {
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
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    planned: Mutex<Option<crate::engine::PlannedIcebergChangeStreamWrite>>,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
    provider_binding:
        Arc<crate::connector::iceberg::change_stream_write::IcebergChangeStreamProviderBinding>,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    /// Exact write authority derived during admission.  Staging must seal the
    /// operation against this lease; it must not reacquire a current control
    /// generation after frontend durable intent.
    write_lease: novarocks_spi::connector::ConnectorWriteLease,
    operation_session:
        Mutex<Option<crate::query_execution::write_operation::ConnectorWriteOperationSession>>,
}

struct MorMergeChangeStreamExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    planned: Mutex<Option<crate::engine::PlannedIcebergChangeStreamWrite>>,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
    provider_binding:
        Arc<crate::connector::iceberg::change_stream_write::IcebergChangeStreamProviderBinding>,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    /// Exact write authority derived during admission.  See the corresponding
    /// UPDATE executor for why this is retained through staging.
    write_lease: novarocks_spi::connector::ConnectorWriteLease,
    operation_session:
        Mutex<Option<crate::query_execution::write_operation::ConnectorWriteOperationSession>>,
}

impl MorUpdateChangeStreamExecutor {
    fn finish_known_empty_noop(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<(), String> {
        completion
            .finish_known_empty_noop()
            .map_err(|error| format!("terminalize MOR UPDATE known-empty session: {error}"))
    }

    fn run_stage(&self) -> Result<QueryExecutionResult, String> {
        let planned = self
            .planned
            .lock()
            .expect("MOR UPDATE change-stream plan lock poisoned")
            .take()
            .ok_or_else(|| "MOR UPDATE change-stream plan was already consumed".to_string())?;
        let crate::engine::PlannedIcebergChangeStreamWrite {
            prepared,
            native_bundle,
            topology,
            ..
        } = planned;
        #[cfg(test)]
        if let Some(result) = crate::engine::observe_change_stream_write_build_for_test(&topology) {
            return Ok(result);
        }
        let prepared_request = crate::engine::prepare_planned_iceberg_change_stream_write(
            prepared,
            native_bundle,
            None,
            &self.execution,
            Some(crate::engine::DistributedConnectorWrite::Begin(
                self.connector_write.clone(),
            )),
        )?;
        let exact_lease = prepared_request.lease();
        let session = self
            .state
            .query_execution
            .begin_write_operation(prepared_request.registration(), exact_lease)
            .map_err(|error| error.to_string())?;
        *self
            .operation_session
            .lock()
            .expect("MOR UPDATE operation session lock poisoned") = Some(session.clone());
        crate::engine::iceberg_writer::activate_iceberg_change_stream_provider_binding_after_session(
            &self.state,
            &self.target,
            &self.provider_binding,
            session.operation_id(),
            &session,
        )?;
        let registration =
            crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                session,
                prepared_request.write_cohort_id(),
            )
            .map_err(|error| error.to_string())?;
        let request = prepared_request
            .into_request(&self.execution, registration)
            .map_err(|error| error.to_string())?;
        crate::engine::execute_bound_distributed_write_request(&self.state.query_execution, request)
    }
}

impl MutationExecution for MorUpdateChangeStreamExecutor {
    fn stage(&self) -> Result<QueryExecutionResult, String> {
        self.run_stage()
    }

    fn needs_abort_on_stage_error(&self) -> bool {
        self.operation_session
            .lock()
            .expect("MOR UPDATE operation session lock poisoned")
            .is_some()
    }

    fn abort(&self, reason: String) -> Result<CommitOutcome, CommitServiceError> {
        let session = self
            .operation_session
            .lock()
            .expect("MOR UPDATE operation session lock poisoned")
            .clone()
            .expect("MOR UPDATE abort requires a retained operation session");
        crate::connector::iceberg::write_commit::abort_iceberg_connector_write(
            &self.commit_executor,
            &session,
            self.connector_context.clone(),
            reason,
        )
    }

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        let session = self
            .operation_session
            .lock()
            .expect("MOR UPDATE operation session lock poisoned")
            .clone()
            .expect("MOR UPDATE abort requires a retained operation session");
        session
            .abort(self.connector_context.clone())
            .map_err(|error| format!("abort MOR UPDATE connector operation: {error}"))
    }

    fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError> {
        crate::connector::iceberg::write_commit::commit_iceberg_connector_write(
            &self.commit_executor,
            completion,
        )
    }

    fn finalize(&self) -> Result<(), String> {
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.state, &self.target)
    }
}

impl MorMergeChangeStreamExecutor {
    fn finish_known_empty_noop(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<(), String> {
        completion
            .finish_known_empty_noop()
            .map_err(|error| format!("terminalize MOR MERGE known-empty session: {error}"))
    }

    fn run_stage(&self) -> Result<QueryExecutionResult, String> {
        let planned = self
            .planned
            .lock()
            .expect("MOR MERGE change-stream plan lock poisoned")
            .take()
            .ok_or_else(|| "MOR MERGE change-stream plan was already consumed".to_string())?;
        let crate::engine::PlannedIcebergChangeStreamWrite {
            prepared,
            native_bundle,
            topology,
            ..
        } = planned;
        #[cfg(test)]
        if let Some(result) = crate::engine::observe_change_stream_write_build_for_test(&topology) {
            return Ok(result);
        }
        let prepared_request = crate::engine::prepare_planned_iceberg_change_stream_write(
            prepared,
            native_bundle,
            None,
            &self.execution,
            Some(crate::engine::DistributedConnectorWrite::Begin(
                self.connector_write.clone(),
            )),
        )?;
        let exact_lease = prepared_request.lease();
        let session = self
            .state
            .query_execution
            .begin_write_operation(prepared_request.registration(), exact_lease)
            .map_err(|error| error.to_string())?;
        *self
            .operation_session
            .lock()
            .expect("MOR MERGE operation session lock poisoned") = Some(session.clone());
        crate::engine::iceberg_writer::activate_iceberg_change_stream_provider_binding_after_session(
            &self.state,
            &self.target,
            &self.provider_binding,
            session.operation_id(),
            &session,
        )?;
        let registration =
            crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                session,
                prepared_request.write_cohort_id(),
            )
            .map_err(|error| error.to_string())?;
        let request = prepared_request
            .into_request(&self.execution, registration)
            .map_err(|error| error.to_string())?;
        crate::engine::execute_bound_distributed_write_request(&self.state.query_execution, request)
    }
}

impl MutationExecution for MorMergeChangeStreamExecutor {
    fn stage(&self) -> Result<QueryExecutionResult, String> {
        self.run_stage()
    }

    fn needs_abort_on_stage_error(&self) -> bool {
        self.operation_session
            .lock()
            .expect("MOR MERGE operation session lock poisoned")
            .is_some()
    }

    fn abort(&self, reason: String) -> Result<CommitOutcome, CommitServiceError> {
        let session = self
            .operation_session
            .lock()
            .expect("MOR MERGE operation session lock poisoned")
            .clone()
            .expect("MOR MERGE abort requires a retained operation session");
        crate::connector::iceberg::write_commit::abort_iceberg_connector_write(
            &self.commit_executor,
            &session,
            self.connector_context.clone(),
            reason,
        )
    }

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        let session = self
            .operation_session
            .lock()
            .expect("MOR MERGE operation session lock poisoned")
            .clone()
            .expect("MOR MERGE abort requires a retained operation session");
        session
            .abort(self.connector_context.clone())
            .map_err(|error| format!("abort MOR MERGE connector operation: {error}"))
    }

    fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError> {
        crate::connector::iceberg::write_commit::commit_iceberg_connector_write(
            &self.commit_executor,
            completion,
        )
    }

    fn finalize(&self) -> Result<(), String> {
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.state, &self.target)
    }
}

#[allow(clippy::too_many_arguments)]
struct CowFileRewritePlan {
    old_file: String,
    query_local_overlay:
        crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay,
    rewrite_query: sqlparser::ast::Query,
}

/// Fully-planned distributed COW UPDATE write. The old/new-file mapping is
/// constructed only by the Iceberg aggregate control adapter after every
/// sealed cohort has staged successfully.
struct CowUpdateDistributedWrite {
    file_plans: Vec<CowFileRewritePlan>,
    rewrite_preparation: novarocks_spi::connector::ConnectorWritePreparation,
    provider_plan: novarocks_spi::connector::ConnectorRowMutationExecutionPlan,
    provider_binding: crate::connector::iceberg::provider::IcebergCowExecutionBinding,
    write_lease: novarocks_spi::connector::ConnectorWriteLease,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

#[allow(clippy::too_many_arguments)]
fn build_cow_update_distributed_write(
    target: &crate::engine::backend_resolver::TargetBackend,
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    provider_plan: novarocks_spi::connector::ConnectorRowMutationExecutionPlan,
    provider_binding: crate::connector::iceberg::provider::IcebergCowExecutionBinding,
    write_lease: novarocks_spi::connector::ConnectorWriteLease,
) -> Result<CowUpdateDistributedWrite, String> {
    let base_snapshot_id =
        base_snapshot_id.ok_or_else(|| "COW UPDATE requires a current snapshot".to_string())?;
    let rewrite_preparation = provider_plan
        .routes()
        .iter()
        .find(|route| {
            route
                .accepted_effects()
                .contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Replace)
        })
        .map(|route| route.preparation().clone())
        .ok_or_else(|| "Provider COW activation returned no replacement route".to_string())?;

    // Index the snapshot's data files by path so each touched file inherits its
    // `first_row_id` / `data_sequence_number` / pre-existing delete files. The
    // BE scan computes `_row_id = first_row_id + _pos` and honors these deletes,
    // so the rewrite re-emits exactly the rows that were live in the file.
    let data_files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
            table,
            base_snapshot_id,
        )?;
    let mut data_file_by_path = std::collections::HashMap::with_capacity(data_files.len());
    for file in data_files {
        data_file_by_path.insert(file.path.clone(), file);
    }

    // Group matched rows by their owning data file, preserving the new-row batch
    // index so the rewrite query can project the replacement values.
    let mut matched_rows_by_file: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (idx, file_path) in matched.file_paths.iter().enumerate() {
        matched_rows_by_file
            .entry(file_path.clone())
            .or_default()
            .push(idx);
    }

    let new_sequence_number = table.metadata().last_sequence_number() + 1;
    let mut file_plans = Vec::with_capacity(matched_rows_by_file.len());
    for (old_file, matched_indices) in matched_rows_by_file {
        let data_file = data_file_by_path.get(&old_file).cloned().ok_or_else(|| {
            format!("COW UPDATE matched data file `{old_file}` is missing from snapshot metadata")
        })?;
        let synthetic_table_name = format!(
            "__nr_cow_{}_{}",
            target.table,
            uuid::Uuid::new_v4().simple()
        );
        let query_local_overlay = build_cow_rewrite_query_local_overlay(
            target,
            &synthetic_table_name,
            data_file,
            base_snapshot_id,
            target_ref,
            planning_lease.clone(),
            connector_context,
        )?;
        let rewrite_query = build_cow_rewrite_query(
            target,
            &synthetic_table_name,
            matched,
            &matched_indices,
            target_columns,
            new_sequence_number,
        )?;
        file_plans.push(CowFileRewritePlan {
            old_file,
            query_local_overlay,
            rewrite_query,
        });
    }

    Ok(CowUpdateDistributedWrite {
        file_plans,
        rewrite_preparation,
        provider_plan,
        provider_binding,
        write_lease,
        planning_lease,
    })
}

/// Freeze one synthetic COW input as a request-local overlay. The provider
/// materialization and exact lease remain in the binding store; no synthetic
/// table is ever registered in shared catalog state.
fn build_cow_rewrite_query_local_overlay(
    target: &crate::engine::backend_resolver::TargetBackend,
    synthetic_table_name: &str,
    data_file: crate::connector::iceberg::catalog::registry::DataFileWithStats,
    base_snapshot_id: i64,
    target_ref: &str,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay, String> {
    if data_file.first_row_id.is_none() {
        return Err(format!(
            "COW UPDATE requires first_row_id for iceberg data file `{}`",
            data_file.path
        ));
    }
    let materialization =
        crate::connector::iceberg::provider::load_schema_materialization_from_exact_lease(
            planning_lease,
            connector_context.clone(),
            &target.namespace,
            &target.table,
        )?;
    if target_ref == "main" && materialization.current_snapshot_id() != Some(base_snapshot_id) {
        return Err(format!(
            "COW UPDATE source {}.{}.{} changed after admission: expected snapshot {}, got {:?}",
            target.catalog,
            target.namespace,
            target.table,
            base_snapshot_id,
            materialization.current_snapshot_id(),
        ));
    }
    // The provider's strict-base materialization is rooted at the table's
    // main snapshot. A branch COW rewrite uses explicit files selected from
    // the admitted branch head, so carry that branch snapshot into the
    // request-local overlay instead of rejecting the schema-only materialization
    // because its default snapshot is main.
    let materialization = materialization.with_frozen_files_at_snapshot(
        vec![
            crate::connector::iceberg::catalog::backend::data_file_with_stats_to_iceberg_data_file_info(
                data_file,
            ),
        ],
        base_snapshot_id,
    )?;
    // The single-file scan must expose `_row_id` / `_last_updated_sequence_number`
    // for the rewrite projection; the table is v3 row-lineage (COW mode was
    // selected) and the file carries `first_row_id`, so the builder advertises
    // them. Guard against a silent drop.
    if !materialization
        .iceberg_row_lineage_metadata_columns
        .iter()
        .any(|c| novarocks_execution::exec::row_position::is_iceberg_row_id(&c.name))
    {
        return Err(format!(
            "COW UPDATE synthetic scan for table {}.{} does not expose _row_id; \
             the data file lacks v3 row-lineage metadata",
            target.namespace, target.table
        ));
    }
    let catalog = target.catalog.clone();
    let namespace = target.namespace.clone();
    let table = target.table.clone();
    let synthetic_table_name = synthetic_table_name.to_string();
    let key = crate::engine::query_planning::bindings::QueryTableBindingKey::snapshot(
        &catalog,
        &namespace,
        &table,
        base_snapshot_id,
    );
    Ok(
        crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay::new(
            namespace.clone(),
            synthetic_table_name.clone(),
            key,
            move |binding| {
                crate::engine::query_planning::catalog_materializer::iceberg_query_binding_from_materialization(
                materialization.clone(),
                &catalog,
                &namespace,
                &synthetic_table_name,
                binding,
            )
            },
        ),
    )
}

/// Build the whole-file rewrite SELECT for one touched data file (approach
/// "drive-from-matched"): scan every live row of the file via the synthetic
/// `ExplicitFiles` table, LEFT JOIN the matched new rows that belong to this
/// file on `_row_id`, and project user columns (replacement value where
/// matched, original otherwise) plus `_row_id` and a conditional
/// `_last_updated_sequence_number`. Ordered by `_row_id` for deterministic
/// output. The matched new values come from the already-materialized
/// `matched.new_rows`, so this path is uniform for both UPDATE and
/// MERGE matched-UPDATE (no source re-join).
fn build_cow_rewrite_query(
    target: &crate::engine::backend_resolver::TargetBackend,
    synthetic_table_name: &str,
    matched: &MatchedUpdateBatch,
    matched_indices: &[usize],
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    new_sequence_number: i64,
) -> Result<sqlparser::ast::Query, String> {
    if matched_indices.is_empty() {
        return Err("COW UPDATE rewrite query requires at least one matched row".to_string());
    }
    let scan_alias = "__nr_cow_t";
    let match_alias = "__nr_cow_m";
    let row_id_col = novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL;
    let last_seq_col = novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL;

    // VALUES relation of the matched new rows in this file: (_row_id, <user
    // columns...>). Values are typed literals read positionally from the
    // already-materialized `new_rows` batch (mirrors the MERGE MOR data sink).
    let mut value_rows = Vec::with_capacity(matched_indices.len());
    for &idx in matched_indices {
        let mut values = Vec::with_capacity(target_columns.len() + 1);
        values.push(matched.row_ids[idx].to_string());
        let (batch_index, row_index) = matched.row_locations[idx];
        let new_rows = matched.new_rows.get(batch_index).ok_or_else(|| {
            "COW UPDATE matched row references a missing non-concatenated new-row batch".to_string()
        })?;
        for target_column in target_columns {
            let col_idx = new_rows
                .schema()
                .index_of(&target_column.name)
                .map_err(|_| {
                    format!(
                        "COW UPDATE new-row batch missing target column `{}`",
                        target_column.name
                    )
                })?;
            let literal =
                crate::sql::literal::literal_from_batch(new_rows.column(col_idx), row_index)?;
            values.push(literal_to_sql_for_values_target_column(
                &literal,
                target_column,
            )?);
        }
        value_rows.push(format!("({})", values.join(", ")));
    }
    let mut match_value_columns = Vec::with_capacity(target_columns.len() + 1);
    match_value_columns.push(sql_identifier(row_id_col));
    for target_column in target_columns {
        match_value_columns.push(sql_identifier(&target_column.name));
    }
    let values_sql = format!(
        "(VALUES {}) AS {}({})",
        value_rows.join(", "),
        sql_identifier(match_alias),
        match_value_columns.join(", ")
    );

    let matched_predicate = format!("{} IS NOT NULL", qualify_column(match_alias, row_id_col));

    let mut select_items = Vec::with_capacity(target_columns.len() + 2);
    for column in target_columns {
        // Replacement value where the row matched, original scan value
        // otherwise. The CASE result is cast to the target column type so the
        // sink sees the declared schema (mirrors the MOR/MERGE data sinks).
        let case_expr = format!(
            "CASE WHEN {matched_predicate} THEN {} ELSE {} END",
            qualify_column(match_alias, &column.name),
            qualify_column(scan_alias, &column.name),
        );
        select_items.push(format!(
            "{} AS {}",
            crate::engine::iceberg_writer::target_cast_expr_sql(&case_expr, column)?,
            sql_identifier(&column.name)
        ));
    }
    select_items.push(format!(
        "{} AS {}",
        qualify_column(scan_alias, row_id_col),
        sql_identifier(row_id_col)
    ));
    // Matched rows advance to the new sequence number; untouched rows keep the
    // per-row `_last_updated_sequence_number` the scan synthesized from the
    // file's data sequence number.
    select_items.push(format!(
        "CAST(CASE WHEN {matched_predicate} THEN {} ELSE {} END AS BIGINT) AS {}",
        new_sequence_number,
        qualify_column(scan_alias, last_seq_col),
        sql_identifier(last_seq_col)
    ));

    // Reference the synthetic table explicitly under `default_catalog` so a
    // session-level Iceberg current catalog cannot route it back through the
    // CatalogMgr entry (mirrors the time-travel rewrite).
    let scan_sql = format!(
        "{}.{}.{} AS {}",
        sql_identifier("default_catalog"),
        sql_identifier(&target.namespace),
        sql_identifier(synthetic_table_name),
        sql_identifier(scan_alias),
    );
    let sql = format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {} = {} ORDER BY {}",
        select_items.join(", "),
        scan_sql,
        values_sql,
        qualify_column(scan_alias, row_id_col),
        qualify_column(match_alias, row_id_col),
        qualify_column(scan_alias, row_id_col),
    );
    parse_generated_query(&sql, "COW UPDATE rewrite")
}

fn literal_to_sql_for_values_target_column(
    literal: &crate::sql::parser::ast::Literal,
    target_column: &novarocks_catalog::schema::ColumnDef,
) -> Result<String, String> {
    let literal_sql = crate::engine::iceberg_writer::literal_to_sql_for_arrow_type(
        literal,
        &target_column.data_type,
    )?;
    if matches!(target_column.data_type, DataType::LargeBinary) {
        crate::engine::iceberg_writer::target_cast_expr_sql(&literal_sql, target_column)
    } else {
        Ok(literal_sql)
    }
}

fn no_mutation_write_result() -> QueryExecutionResult {
    QueryExecutionResult {
        query_result: QueryResult::empty(),
        write_commit: None,
        write_abort: None,
        connector_completion: None,
        fragment_profiles: Vec::new(),
    }
}

struct DistributedCowUpdateExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    write: Mutex<Option<CowUpdateDistributedWrite>>,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    operation_session: crate::query_execution::write_operation::ConnectorWriteOperationSession,
    cohort_by_old_file: BTreeMap<String, novarocks_spi::connector::ConnectorWriteCohortId>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl DistributedCowUpdateExecutor {
    fn run_stage(&self) -> Result<QueryExecutionResult, String> {
        let write = self
            .write
            .lock()
            .expect("COW UPDATE write plan lock poisoned")
            .take()
            .ok_or_else(|| "COW UPDATE write plan was already consumed".to_string())?;
        if write.file_plans.is_empty() {
            return Ok(no_mutation_write_result());
        }
        run_cow_update_file_rewrites(
            &self.state,
            &self.target,
            write,
            &self.operation_session,
            &self.cohort_by_old_file,
            &self.execution,
            &self.connector_context,
        )
    }
}

impl MutationExecution for DistributedCowUpdateExecutor {
    fn stage(&self) -> Result<QueryExecutionResult, String> {
        self.run_stage()
    }

    fn needs_abort_on_stage_error(&self) -> bool {
        true
    }

    fn abort(&self, reason: String) -> Result<CommitOutcome, CommitServiceError> {
        crate::connector::iceberg::write_commit::abort_iceberg_connector_write(
            &self.commit_executor,
            &self.operation_session,
            self.connector_context.clone(),
            reason,
        )
    }

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        self.operation_session
            .abort(self.connector_context.clone())
            .map_err(|error| format!("abort COW UPDATE connector operation: {error}"))
    }

    fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError> {
        crate::connector::iceberg::write_commit::commit_iceberg_connector_write(
            &self.commit_executor,
            completion,
        )
    }

    fn finalize(&self) -> Result<(), String> {
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.state, &self.target)
    }
}

/// Run every sealed old-file cohort serially. Each execution registers one
/// accepted attempt in the shared operation session; only the final completion
/// is returned to the transaction runner because it commits the aggregate.
fn run_cow_update_file_rewrites(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    write: CowUpdateDistributedWrite,
    operation_session: &crate::query_execution::write_operation::ConnectorWriteOperationSession,
    cohort_by_old_file: &BTreeMap<String, novarocks_spi::connector::ConnectorWriteCohortId>,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryExecutionResult, String> {
    let mut final_result = None;
    for plan in write.file_plans {
        let cohort_id = cohort_by_old_file
            .get(&plan.old_file)
            .copied()
            .ok_or_else(|| {
                format!(
                    "COW UPDATE old file `{}` has no sealed rewrite cohort",
                    plan.old_file
                )
            })?;
        let registration =
            crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                operation_session.clone(),
                cohort_id,
            )
            .map_err(|error| error.to_string())?;
        let result = run_one_cow_file_rewrite(
            state,
            target,
            &plan,
            &write.rewrite_preparation,
            &write.planning_lease,
            registration,
            execution,
            connector_context,
        )?;
        if result.connector_completion.is_none() {
            return Err(format!(
                "COW UPDATE rewrite for data file `{}` completed without a connector cohort",
                plan.old_file
            ));
        }
        final_result = Some(result);
    }
    final_result.ok_or_else(|| "COW UPDATE operation has no rewrite cohorts".to_string())
}

/// Run the scoped BE rewrite with its single-file application overlay. The
/// overlay is materialized into this request's binding store and cannot leak
/// into the shared catalog; the write's reported data-file paths become this
/// old file's `new_files`.
fn run_one_cow_file_rewrite(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    plan: &CowFileRewritePlan,
    preparation: &novarocks_spi::connector::ConnectorWritePreparation,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_write: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryExecutionResult, String> {
    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding = admit_prepared_connector_write_target(
        table_bindings.as_ref(),
        crate::sql::planner::table::SqlTableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        preparation.clone(),
        planning_lease.clone(),
    )?;
    let sink = sql_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::RowLineageData,
        crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
        None,
    )?;
    let result =
        crate::engine::execute_query_as_iceberg_write_in_operation_with_query_local_overlays(
            state,
            Some(&target.catalog),
            &target.namespace,
            &plan.rewrite_query,
            sink,
            table_bindings,
            None,
            crate::sql::compiler::RootDistributionRequirement::Any,
            Some(execution),
            connector_context,
            connector_write,
            std::slice::from_ref(&plan.query_local_overlay),
        );
    let result = result?;

    if let Some(abort) = &result.write_abort {
        return Err(format!(
            "COW UPDATE rewrite for data file `{}` aborted: {}",
            plan.old_file, abort.reason
        ));
    }
    let staging = result
        .connector_completion
        .as_ref()
        .expect("COW rewrite checked connector completion above")
        .staging_summary()
        .map_err(|error| {
            format!(
                "COW UPDATE rewrite for data file `{}` has an invalid connector staging summary: {error}",
                plan.old_file
            )
        })?;
    if staging.input_rows() == 0 || staging.artifact_count() == 0 {
        return Err(format!(
            "COW UPDATE rewrite for data file `{}` produced no replacement data files \
             (staged_rows={}, artifacts={}, query={})",
            plan.old_file,
            staging.input_rows(),
            staging.artifact_count(),
            plan.rewrite_query
        ));
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn build_cow_update_distributed_execution(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: novarocks_connector_iceberg::iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_ref: &str,
    write: CowUpdateDistributedWrite,
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<DistributedCowUpdateExecutor>, String> {
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = Arc::new(IcebergWriteCommitExecutor {
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    });
    let mut write = write;
    let operation_id = write
        .provider_plan
        .copy_on_write()
        .ok_or_else(|| "COW UPDATE is missing the Provider sealed cohort set".to_string())?
        .0
        .operation_id();
    let write_lease = write.write_lease.clone();
    write
        .file_plans
        .sort_by(|left, right| left.old_file.cmp(&right.old_file));
    let mut cohort_templates = Vec::with_capacity(write.file_plans.len());
    let mut cohort_by_old_file = BTreeMap::new();
    for file_plan in &mut write.file_plans {
        let cohort_id = write
            .provider_binding
            .rewrite_cohort_for_file(&file_plan.old_file)
            .map_err(|error| format!("resolve Provider COW rewrite cohort: {error}"))?;
        cohort_templates.push(
            crate::query_execution::contract::ConnectorWritePlanningTemplate::new_in_cohort(
                operation_id,
                cohort_id,
                write.rewrite_preparation.clone(),
                connector_context.clone(),
                write_lease.clone(),
            ),
        );
        if cohort_by_old_file
            .insert(file_plan.old_file.clone(), cohort_id)
            .is_some()
        {
            return Err("COW UPDATE generated a duplicate rewrite cohort".to_string());
        }
    }
    let committer: Arc<dyn crate::connector::iceberg::write_service::IcebergWriteReportCommitter> =
        Arc::new(
            crate::connector::iceberg::write_service::IcebergCowWriteReportCommitter::new(
                Arc::clone(&commit_executor),
                entry.clone(),
            ),
        );
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    crate::connector::iceberg::provider::register_iceberg_cow_write_service_from_execution_plan(
        services,
        &write.provider_plan,
        target_ref,
        &entry,
        committer,
    )
    .map_err(|error| format!("activate Iceberg COW provider service: {error}"))?;
    let registration =
        crate::query_execution::contract::ConnectorWriteOperationRegistration::try_new(
            cohort_templates,
        )
        .map_err(|error| error.to_string())?;
    let operation_session = state
        .query_execution
        .begin_write_operation(registration, write_lease)
        .map_err(|error| error.to_string())?;
    Ok(Arc::new(DistributedCowUpdateExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        write: Mutex::new(Some(write)),
        commit_executor,
        operation_session,
        cohort_by_old_file,
        execution,
        connector_context: connector_context.clone(),
    }))
}

#[allow(clippy::too_many_arguments)]
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
fn cow_selection_from_matched_and_insert(
    matched: &MatchedUpdateBatch,
    insert_batch: Option<&RecordBatch>,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorRowMutationSelection, String> {
    use novarocks_spi::connector::ConnectorRowMutationEffect;

    let contract = preparation.match_contract();
    let mut collector =
        crate::engine::row_mutation::BoundedRowMutationMatchCollector::try_new(context, None)
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
                crate::exec::expr::cast_array_to_target(
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
                crate::exec::expr::cast_array_to_target(
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
                crate::exec::expr::cast_array_to_target(
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
    let mut validator = crate::engine::row_mutation::RowMutationMatchValidator::try_new(
        contract.clone(),
        preparation.intent().clone(),
    )
    .map_err(|error| format!("initialize COW match contract validator: {error}"))?;
    validator
        .validate_selection(&selection)
        .map_err(|error| format!("validate COW match contract: {error}"))?;
    Ok(selection)
}

fn execute_update_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MatchedUpdateBatch, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal UPDATE match query was not a SELECT".to_string());
    };
    let result = crate::engine::execute_query_with_catalog_service_with_execution(
        state,
        current_catalog,
        current_database,
        &query,
        None,
        execution,
        connector_context,
    )?;
    matched_update_batch_from_query_result(result)
}

fn matched_update_batch_from_query_result(
    result: QueryResult,
) -> Result<MatchedUpdateBatch, String> {
    let mut merged = empty_matched_update_batch()?;
    for chunk in result.chunks {
        merged.append(matched_update_batch_from_record_batch(&chunk.batch)?);
    }
    Ok(merged)
}

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

impl MatchedUpdateBatch {
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

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| format!("UPDATE match query missing `{name}` column"))?;
    Ok(batch.column(idx))
}

fn iceberg_table_columns(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<Vec<novarocks_catalog::schema::ColumnDef>, String> {
    let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?;
    let iceberg_schema = table.metadata().current_schema();
    arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let nested = iceberg_schema
                .field_by_name(field.name())
                .ok_or_else(|| format!("iceberg column `{}` missing from schema", field.name()))?;
            let data_type = match nested.field_type.as_ref() {
                novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                    novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Variant,
                ) => DataType::LargeBinary,
                novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                    novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Binary,
                ) => DataType::Binary,
                _ => field.data_type().clone(),
            };
            Ok(novarocks_catalog::schema::ColumnDef {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                write_default: None,
                logical_type: None,
            })
        })
        .collect()
}

fn iceberg_partition_source_columns(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<Vec<String>, String> {
    let schema = table.metadata().current_schema();
    let mut out = Vec::new();
    for field in table.metadata().default_partition_spec().fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "partition source field id {} is missing from iceberg schema",
                field.source_id
            )
        })?;
        out.push(source.name.clone());
    }
    Ok(out)
}

fn validate_update_assignments(
    assignments: &[crate::sql::parser::ast::UpdateAssignment],
    target_columns: &[novarocks_catalog::schema::ColumnDef],
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
    state: &Arc<StandaloneState>,
    prepared: PreparedMergeMutation,
) -> Result<MutationStagedWrite, String> {
    let PreparedMergeMutation {
        stmt,
        current_catalog,
        target,
        catalog,
        table_ident,
        table,
        target_columns,
        entry,
        table_write_mode,
        planning_lease,
        mor_write_target,
        insert_columns_resolved,
        execution,
        connector_context,
    } = prepared;
    let has_matched_update = matches!(
        stmt.matched.as_ref().map(|clause| &clause.action),
        Some(MergeMatchedAction::Update { .. })
    );
    let has_matched_delete = matches!(
        stmt.matched.as_ref().map(|clause| &clause.action),
        Some(MergeMatchedAction::Delete)
    );
    let has_not_matched_insert = stmt.not_matched.is_some();
    if table_write_mode == IcebergUpdateMode::MergeOnRead || has_matched_delete {
        if !has_matched_update && !has_matched_delete && !has_not_matched_insert {
            return Ok(MutationStagedWrite::NoOp);
        }
        let base_snapshot_id = table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        let metadata = table.metadata();
        let PreparedMorMergeWriteTarget {
            preparations,
            planning_lease: write_planning_lease,
        } = mor_write_target.ok_or_else(|| {
            "MOR MERGE reached stage without an admitted frozen write target".to_string()
        })?;
        let preparations = preparations.activate()?;
        let write_lease = write_planning_lease
            .derive_write_lease()
            .map_err(|error| format!("derive MOR MERGE write lease: {error}"))?;
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::RowDeltaDvFromFiles,
                table_ident,
                base_snapshot_id,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!(
                    "{}/data/_staging/{}",
                    metadata.location(),
                    uuid::Uuid::new_v4()
                ),
                novarocks_types::UniqueId::new(0, 0),
            )
            .with_table_metadata(metadata.clone()),
        );
        let write = build_merge_mor_change_stream_write_plan(
            state,
            &target,
            &stmt,
            current_catalog.as_deref(),
            &target_columns,
            insert_columns_resolved.as_deref(),
            "main",
            metadata.last_sequence_number() + 1,
            &execution,
            &connector_context,
            &preparations,
            write_planning_lease,
        )?;
        let abort_cleanup =
            crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
        let commit_executor = Arc::new(IcebergWriteCommitExecutor {
            catalog,
            table,
            collector,
            fs: abort_cleanup.fs,
            cleanup_path_mapper: abort_cleanup.path_mapper,
            cow_update_rewrite: None,
            target_ref: "main".to_string(),
            snapshot_properties: BTreeMap::new(),
        });
        let operation_id = preparations.operation_id;
        let mut write = write;
        let planned = plan_dml_change_stream_write(state, &target, &mut write)?;
        let provider_binding = Arc::new(
            crate::connector::iceberg::change_stream_write::bind_iceberg_change_stream_provider(
                crate::connector::iceberg::change_stream_write::IcebergChangeStreamProviderRequest {
                    target: &format!("{}.{}.{}", target.catalog, target.namespace, target.table),
                    target_ref: "main",
                    table: &commit_executor.table,
                    entry: &entry,
                    base_snapshot_id,
                    operation_id,
                    topology: &planned.topology,
                    table_bindings: write.table_bindings.as_ref(),
                    commit_executor: Arc::clone(&commit_executor),
                },
            )?,
        );
        let connector_write =
            crate::engine::iceberg_writer::iceberg_change_stream_provider_binding_template(
                state,
                &target,
                &provider_binding,
                operation_id,
                connector_context.clone(),
                &write_lease,
                preparations.primary(),
            )?;
        let execution_handle = Arc::new(MorMergeChangeStreamExecutor {
            state: Arc::clone(state),
            target: target.clone(),
            planned: Mutex::new(Some(planned)),
            connector_write,
            provider_binding,
            commit_executor,
            execution,
            connector_context,
            write_lease,
            operation_session: Mutex::new(None),
        });
        let result = match execution_handle.stage() {
            Ok(result) => result,
            Err(reason) => {
                if execution_handle.needs_abort_on_stage_error() {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason,
                        execution: execution_handle,
                    });
                }
                return Err(reason);
            }
        };
        if let Some(completion) = result.connector_completion.as_ref() {
            let known_empty = match completion.is_known_empty() {
                Ok(known_empty) => known_empty,
                Err(error) => {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason: format!("summarize MOR MERGE change-stream aggregate: {error}"),
                        execution: execution_handle,
                    });
                }
            };
            if known_empty {
                if let Err(reason) = execution_handle.finish_known_empty_noop(completion) {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason,
                        execution: execution_handle,
                    });
                }
                return Ok(MutationStagedWrite::NoOp);
            }
        } else if let Some(commit) = result.write_commit.as_ref()
            && !write_commit_has_files(commit)
        {
            if commit.writers.iter().any(|writer| writer.loaded_rows > 0) {
                return Ok(MutationStagedWrite::AbortRequired {
                    reason: "MOR MERGE change-stream write produced rows but no data or DV files"
                        .to_string(),
                    execution: execution_handle,
                });
            }
            return Ok(MutationStagedWrite::AbortRequired {
                reason: "MOR MERGE missing connector completion for an empty aggregate".to_string(),
                execution: execution_handle,
            });
        }
        let Some(completion) = result.connector_completion else {
            return Ok(MutationStagedWrite::AbortRequired {
                reason: "MOR MERGE staged without an aggregate connector completion".to_string(),
                execution: execution_handle,
            });
        };
        return Ok(MutationStagedWrite::CommitRequired {
            execution: execution_handle,
            completion,
        });
    }
    let match_rows = materialize_merge_match(
        state,
        &target,
        &stmt,
        current_catalog.as_deref(),
        &target_columns,
        insert_columns_resolved.as_deref(),
        &execution,
        &connector_context,
    )?;
    // Keep the logical INSERT result for the COW selection, but do not admit
    // an ordinary append preparation yet.  A COW MERGE must use only the
    // Provider-sealed append route returned with the rewrite cohorts.
    let insert_candidate = if has_not_matched_insert {
        let insert_columns = insert_columns_resolved
            .as_ref()
            .expect("not_matched populated => insert columns resolved");
        let insert_batch = match_rows.unmatched_insert_batch(&target_columns, insert_columns)?;
        if insert_batch.num_rows() > 0 {
            let insert_query = build_merge_unmatched_insert_query(
                state,
                &target,
                &stmt,
                current_catalog.as_deref(),
                &target_columns,
                insert_columns,
            )?;
            let planning_lease = crate::connector::acquire_metadata_planning_lease(
                state.connector_control.as_ref(),
                &target.catalog,
            )?;
            Some((insert_query, insert_batch, planning_lease))
        } else {
            None
        }
    } else {
        None
    };
    let insert_selection_batch = insert_candidate.as_ref().map(|(_, batch, _)| batch);
    let mut matched_branch = if let Some(clause) = stmt.matched.as_ref() {
        let matched = matched_update_batch_from_record_batch(&match_rows.matched_batch()?)?;
        if matched.row_ids.is_empty() {
            MergeMatchedBranch::None
        } else {
            match &clause.action {
                MergeMatchedAction::Update { .. } => {
                    let materialization = crate::connector::iceberg::provider::load_schema_materialization_from_exact_lease(
                        planning_lease.clone(),
                        connector_context.clone(),
                        &target.namespace,
                        &target.table,
                    )?;
                    let operation_id = novarocks_spi::connector::ConnectorWriteOperationId::new();
                    let intent = if insert_selection_batch.is_some() {
                        novarocks_spi::connector::ConnectorRowMutationIntent::Merge {
                            effects: vec![
                                novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
                                novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
                            ],
                        }
                    } else {
                        novarocks_spi::connector::ConnectorRowMutationIntent::Update
                    };
                    let (row_mutation_lease, row_mutation_preparation) = materialization
                        .prepare_row_mutation(
                            "main",
                            operation_id,
                            intent,
                            connector_context.clone(),
                        )?;
                    let selection = cow_selection_from_matched_and_insert(
                        &matched,
                        insert_selection_batch,
                        &row_mutation_preparation,
                        connector_context.clone(),
                    )?;
                    let provider_plan = row_mutation_lease
                        .activate_row_mutation(
                            novarocks_spi::connector::ConnectorRowMutationActivationRequest::CopyOnWrite {
                                preparation: row_mutation_preparation,
                                selection,
                                context: connector_context.clone(),
                            },
                        )
                        .map_err(|error| format!("activate Provider COW MERGE plan: {error}"))?;
                    let provider_binding =
                        crate::connector::iceberg::provider::bind_iceberg_cow_execution_plan(
                            &provider_plan,
                        )
                        .map_err(|error| format!("bind Provider COW MERGE plan: {error}"))?;
                    MergeMatchedBranch::CowUpdate(build_cow_update_distributed_write(
                        &target,
                        &table,
                        &matched,
                        &target_columns,
                        table
                            .metadata()
                            .current_snapshot()
                            .map(|snapshot| snapshot.snapshot_id()),
                        "main",
                        planning_lease.clone(),
                        &connector_context,
                        provider_plan,
                        provider_binding,
                        row_mutation_lease,
                    )?)
                }
                MergeMatchedAction::Delete => unreachable!("MOR DELETE was selected above"),
            }
        }
    } else {
        MergeMatchedBranch::None
    };
    if insert_candidate.is_none() && matches!(matched_branch, MergeMatchedBranch::None) {
        return Ok(MutationStagedWrite::NoOp);
    }
    if matches!(matched_branch, MergeMatchedBranch::None) {
        let (query, _, planning_lease) = insert_candidate
            .as_ref()
            .expect("non-noop MERGE without a matched branch has an insert branch");
        let resolved = crate::connector::metadata_load_table_with_planning_lease(
            planning_lease.clone(),
            connector_context.clone(),
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?
        .0;
        let prepared_write = crate::engine::iceberg_writer::prepare_iceberg_write(
            state,
            &target,
            &resolved,
            &[],
            &crate::engine::iceberg_writer::IcebergWriteInput::Query(Box::new(query.clone())),
            crate::engine::iceberg_writer::IcebergWriteMode::Append,
            "main",
            Some(execution),
            &connector_context,
            planning_lease.clone(),
        )?;
        let execution_handle = prepared_write.into_mutation_execution()?;
        let result = match execution_handle.stage() {
            Ok(result) => result,
            Err(reason) => {
                if execution_handle.needs_abort_on_stage_error() {
                    return Ok(MutationStagedWrite::AbortRequired {
                        reason,
                        execution: execution_handle,
                    });
                }
                return Err(reason);
            }
        };
        let Some(completion) = result.connector_completion else {
            return Ok(MutationStagedWrite::AbortRequired {
                reason: "MERGE INSERT-only staged without a connector completion".to_string(),
                execution: execution_handle,
            });
        };
        return Ok(MutationStagedWrite::CommitRequired {
            execution: execution_handle,
            completion,
        });
    }
    let base_snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let metadata = table.metadata();
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::CowUpdate,
            table_ident,
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!(
                "{}/data/_staging/{}",
                metadata.location(),
                uuid::Uuid::new_v4()
            ),
            novarocks_types::UniqueId::new(0, 0),
        )
        .with_table_metadata(metadata.clone()),
    );
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = Arc::new(IcebergWriteCommitExecutor {
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: "main".to_string(),
        snapshot_properties: BTreeMap::new(),
    });
    let cow_operation = match &mut matched_branch {
        MergeMatchedBranch::CowUpdate(write) => Some(prepare_cow_merge_operation(
            state,
            "main",
            write,
            insert_candidate.is_some(),
            &entry,
            Arc::clone(&commit_executor),
            &connector_context,
        )?),
        MergeMatchedBranch::None => unreachable!("checked above"),
    };
    let planning_lease = match &matched_branch {
        MergeMatchedBranch::CowUpdate(write) => write.planning_lease.clone(),
        MergeMatchedBranch::None => unreachable!("checked above"),
    };
    let insert_branch = match (
        insert_candidate,
        cow_operation
            .as_ref()
            .and_then(|operation| operation.append_preparation.as_ref()),
    ) {
        (Some((query, _, _)), Some(preparation)) => {
            Some((query, preparation.clone(), planning_lease.clone()))
        }
        (None, None) => None,
        (Some(_), None) => {
            return Err("MERGE COW INSERT has no Provider-sealed append route".to_string());
        }
        (None, Some(_)) => {
            return Err(
                "Provider COW plan sealed an append route without INSERT input".to_string(),
            );
        }
    };
    let execution_handle = Arc::new(DistributedMergeExecutor {
        state: Arc::clone(state),
        target,
        commit_op_kind: CommitOpKind::CowUpdate,
        branches: Mutex::new(Some(MergeBranchSet {
            insert: insert_branch,
            matched: matched_branch,
        })),
        commit_executor,
        execution,
        planning_lease,
        cow_operation,
        connector_context,
    });
    let result = match execution_handle.stage() {
        Ok(result) => result,
        Err(reason) => {
            return Ok(MutationStagedWrite::AbortRequired {
                reason,
                execution: execution_handle,
            });
        }
    };
    let Some(completion) = result.connector_completion else {
        return Ok(MutationStagedWrite::AbortRequired {
            reason: "MERGE staged without an aggregate connector completion".to_string(),
            execution: execution_handle,
        });
    };
    Ok(MutationStagedWrite::CommitRequired {
        execution: execution_handle,
        completion,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepare_cow_merge_operation(
    state: &Arc<StandaloneState>,
    target_ref: &str,
    write: &mut CowUpdateDistributedWrite,
    has_insert: bool,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<CowMergeOperation, String> {
    let operation_id = write
        .provider_plan
        .copy_on_write()
        .ok_or_else(|| "COW MERGE is missing the Provider sealed cohort set".to_string())?
        .0
        .operation_id();
    let write_lease = write.write_lease.clone();
    write
        .file_plans
        .sort_by(|left, right| left.old_file.cmp(&right.old_file));
    let mut templates = Vec::with_capacity(write.file_plans.len() + usize::from(has_insert));
    let mut cohort_by_old_file = BTreeMap::new();
    for file_plan in &mut write.file_plans {
        let cohort_id = write
            .provider_binding
            .rewrite_cohort_for_file(&file_plan.old_file)
            .map_err(|error| format!("resolve Provider COW MERGE rewrite cohort: {error}"))?;
        templates.push(
            crate::query_execution::contract::ConnectorWritePlanningTemplate::new_in_cohort(
                operation_id,
                cohort_id,
                write.rewrite_preparation.clone(),
                connector_context.clone(),
                write_lease.clone(),
            ),
        );
        cohort_by_old_file.insert(file_plan.old_file.clone(), cohort_id);
    }
    let (append_cohort, append_preparation) = if has_insert {
        let cohort_id = write.provider_binding.append_cohort().ok_or_else(|| {
            "MERGE COW INSERT branch has no Provider-sealed append cohort".to_string()
        })?;
        let preparation = write
            .provider_plan
            .routes()
            .iter()
            .find(|route| route.cohort_id() == cohort_id)
            .map(|route| route.preparation().clone())
            .ok_or_else(|| "Provider COW append cohort has no route preparation".to_string())?;
        templates.push(
            crate::query_execution::contract::ConnectorWritePlanningTemplate::new_in_cohort(
                operation_id,
                cohort_id,
                preparation.clone(),
                connector_context.clone(),
                write_lease.clone(),
            ),
        );
        (Some(cohort_id), Some(preparation))
    } else {
        (None, None)
    };
    let committer: Arc<dyn crate::connector::iceberg::write_service::IcebergWriteReportCommitter> =
        Arc::new(
            crate::connector::iceberg::write_service::IcebergCowWriteReportCommitter::new(
                commit_executor,
                entry.clone(),
            ),
        );
    let services = state
        .iceberg_catalogs
        .read()
        .map_err(|error| format!("Iceberg catalog registry read lock: {error}"))?
        .write_services();
    crate::connector::iceberg::provider::register_iceberg_cow_write_service_from_execution_plan(
        services,
        &write.provider_plan,
        target_ref,
        entry,
        committer,
    )
    .map_err(|error| format!("activate MERGE COW provider service: {error}"))?;
    let registration =
        crate::query_execution::contract::ConnectorWriteOperationRegistration::try_new(templates)
            .map_err(|error| error.to_string())?;
    let session = state
        .query_execution
        .begin_write_operation(registration, write_lease)
        .map_err(|error| error.to_string())?;
    Ok(CowMergeOperation {
        session,
        cohort_by_old_file,
        append_cohort,
        append_preparation,
    })
}

/// Resolved target column ordering for `WHEN NOT MATCHED INSERT`. Each entry
/// maps a target column name to either an explicit value expression (sourced
/// from `INSERT (cols) VALUES (exprs)`) or a `NULL` default when the user did
/// not list the column. Validates that every named column exists, that the
/// list has no duplicates, and that no reserved row-lineage column is named.
struct MergeInsertColumns {
    columns: Vec<MergeInsertColumn>,
}

struct MergeInsertColumn {
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
    action: &MergeNotMatchedAction,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
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

struct MergeMatchRows {
    /// The full RecordBatch from the MERGE match SELECT, with rows for both
    /// matched and unmatched cases. Filters for each side are derived from
    /// `__nr_match_kind` / `__nr_matched_apply` / `__nr_unmatched_apply`.
    full: RecordBatch,
}

impl MergeMatchRows {
    fn empty() -> Self {
        Self {
            full: RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    fn matched_batch(&self) -> Result<RecordBatch, String> {
        if self.full.num_rows() == 0 {
            return Ok(self.full.clone());
        }
        let filter = self.row_filter("matched", "__nr_matched_apply")?;
        filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE matched rows failed: {e}"))
    }

    fn unmatched_insert_batch(
        &self,
        target_columns: &[novarocks_catalog::schema::ColumnDef],
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

fn materialize_merge_match(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    insert_columns: Option<&[MergeInsertColumn]>,
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
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };

    let on_sql = stmt.on.to_string();
    let matched_predicate_sql = stmt
        .matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());
    let not_matched_predicate_sql = stmt
        .not_matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());

    let matched_assignments_sql = match stmt.matched.as_ref().map(|c| &c.action) {
        Some(MergeMatchedAction::Update { assignments }) => assignments
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
                    crate::engine::iceberg_writer::target_cast_expr_sql(
                        &format!("({})", a.value),
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
                            crate::engine::iceberg_writer::target_cast_expr_sql(
                                &format!("({})", action.values[idx]),
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
        &on_sql,
        matched_predicate_sql.as_deref(),
        not_matched_predicate_sql.as_deref(),
        target_columns,
        &matched_assignments_sql_borrow,
        &insert_values_sql_borrow,
        stmt.matched.as_ref().map(|clause| match clause.action {
            MergeMatchedAction::Update { .. } => MERGE_ACTION_MATCHED_UPDATE,
            MergeMatchedAction::Delete => MERGE_ACTION_MATCHED_DELETE,
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

#[allow(clippy::too_many_arguments)]
fn build_merge_mor_change_stream_write_plan(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    insert_columns: Option<&[MergeInsertColumn]>,
    target_ref: &str,
    new_sequence_number: i64,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    preparations: &ActivatedDmlChangeStreamPreparations,
    write_planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<DmlChangeStreamWritePlan, String> {
    let target_alias = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| MERGE_TARGET_DEFAULT_ALIAS.to_string());
    let target_sql = update_change_stream_target_sql(target, &target_alias, target_ref);
    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    let source_sql = match &stmt.source {
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };

    let matched_assignments_sql = match stmt.matched.as_ref().map(|c| &c.action) {
        Some(MergeMatchedAction::Update { assignments }) => assignments
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
                    crate::engine::iceberg_writer::target_cast_expr_sql(
                        &format!("({})", a.value),
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
                            crate::engine::iceberg_writer::target_cast_expr_sql(
                                &format!("({})", action.values[idx]),
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
        MergeMatchedAction::Update { .. } => MERGE_ACTION_MATCHED_UPDATE,
        MergeMatchedAction::Delete => MERGE_ACTION_MATCHED_DELETE,
    });
    let has_matched_update = matched_action == Some(MERGE_ACTION_MATCHED_UPDATE);
    let has_matched_delete = matched_action == Some(MERGE_ACTION_MATCHED_DELETE);
    let has_not_matched_insert = stmt.not_matched.is_some();
    let matched_predicate_sql = stmt
        .matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());
    let not_matched_predicate_sql = stmt
        .not_matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());

    let match_sql = build_merge_match_query_sql(
        &target_sql,
        &target_alias,
        &source_sql,
        &stmt.on.to_string(),
        matched_predicate_sql.as_deref(),
        not_matched_predicate_sql.as_deref(),
        target_columns,
        &matched_assignments_sql_borrow,
        &insert_values_sql_borrow,
        matched_action,
        has_not_matched_insert,
    );
    let mut query = parse_generated_query(&match_sql, "MOR MERGE change-stream producer")?;
    if crate::engine::query_prep::has_time_travel_refs(&query) {
        crate::engine::query_prep::rewrite_time_travel_refs(
            state,
            Some(&target.catalog),
            &target.namespace,
            &mut query,
            connector_context,
        )?;
    }

    let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
    let analyzer_provider = crate::engine::build_catalog_service_provider(
        Some(&target.catalog),
        &catalog_service_snapshot,
        state.connector_control.as_ref(),
        connector_context.clone(),
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let table_bindings = analyzer_provider.query_table_bindings();
    let effect_set = DmlRowMutationEffectSet::Merge {
        matched_update: has_matched_update,
        matched_delete: has_matched_delete,
        not_matched_insert: has_not_matched_insert,
    };
    for route in &preparations.routes {
        crate::engine::query_planning::write_sink::admit_prepared_connector_write_target(
            table_bindings.as_ref(),
            crate::sql::planner::table::SqlTableIdentity {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            route.preparation().clone(),
            write_planning_lease.clone(),
        )?;
    }
    let planned = crate::engine::plan_query_for_iceberg_change_stream_refresh(
        state,
        &query,
        &analyzer_provider,
        &target.namespace,
        None,
        table_bindings,
        execution,
    )?;
    let producer = build_merge_mor_change_event_expand_plan(
        planned.optimized_tree,
        target_columns,
        new_sequence_number,
        has_matched_update,
        has_matched_delete,
        has_not_matched_insert,
    )?;
    let mut plan = build_dml_change_stream_write_plan(
        target,
        producer,
        planned.table_bindings.ok_or_else(|| {
            "MOR MERGE change-stream compilation did not retain query table bindings".to_string()
        })?,
        execution.clone(),
        effect_set,
        preparations,
    )?;
    if has_matched_update || has_matched_delete {
        plan.pre_expand_keyed_assert = Some(DmlPreExpandKeyedAssert {
            // Matched rows use the real target `_row_id`; unmatched rows use
            // a generated negative row number so fresh-only rows do not
            // collide under the same NULL key before expansion.
            key_column_name: "__nr_merge_assert_key".to_string(),
            key_label: novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
            message_prefix: "MOR MERGE matched target row".to_string(),
        });
    }
    Ok(plan)
}

fn execute_merge_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MergeMatchRows, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal MERGE match query was not a SELECT".to_string());
    };
    let result = crate::engine::execute_query_with_catalog_service_with_execution(
        state,
        current_catalog,
        current_database,
        &query,
        None,
        execution,
        connector_context,
    )?;
    let Some(first_chunk) = result.chunks.first() else {
        return Ok(MergeMatchRows::empty());
    };
    let schema = first_chunk.batch.schema();
    let batches = result
        .chunks
        .iter()
        .map(|c| c.batch.clone())
        .collect::<Vec<_>>();
    let full = concat_batches(&schema, batches.iter())
        .map_err(|e| format!("concatenate MERGE match batches failed: {e}"))?;
    Ok(MergeMatchRows { full })
}

fn build_merge_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: &str,
    on_sql: &str,
    matched_predicate_sql: Option<&str>,
    not_matched_predicate_sql: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
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

fn build_merge_unmatched_insert_query(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    insert_columns: &MergeInsertColumns,
) -> Result<sqlparser::ast::Query, String> {
    let target_alias = stmt
        .target_alias
        .as_deref()
        .unwrap_or(MERGE_TARGET_DEFAULT_ALIAS);
    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    let source_sql = match &stmt.source {
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
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
                Some(idx) => format!("({})", not_matched.action.values[idx]),
                None => "NULL".to_string(),
            };
            let expr =
                crate::engine::iceberg_writer::target_cast_expr_sql(&raw_expr, target_column)?;
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
    if let Some(predicate) = not_matched.predicate.as_ref() {
        predicates.push(format!("({predicate})"));
    }
    let sql = format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {} WHERE {}",
        select_items.join(", "),
        source_sql,
        target_sql,
        stmt.on,
        predicates.join(" AND ")
    );
    parse_generated_query(&sql, "MERGE unmatched INSERT sink")
}

/// The matched-side write plan of a folded MERGE, by table write mode. `None`
/// when the statement has no matched clause (or the matched batch is empty).
enum MergeMatchedBranch {
    None,
    CowUpdate(CowUpdateDistributedWrite),
}

/// All active write branches of one MERGE statement, fed to
/// [`DistributedMergeExecutor`] so they share one collector and one commit.
struct MergeBranchSet {
    /// Not-matched INSERT plan (`build_iceberg_write_plan` output). Its files are
    /// FRESH (net-new rows, no preserved `_row_id`).
    insert: Option<(
        sqlparser::ast::Query,
        novarocks_spi::connector::ConnectorWritePreparation,
        novarocks_spi::connector::ConnectorControlPlanningLease,
    )>,
    matched: MergeMatchedBranch,
}

/// Single multi-branch MERGE write executor: runs every active branch into one
/// shared `IcebergCommitCollector` and commits exactly once, so a MERGE lands
/// as ONE Iceberg snapshot. Routes fresh (INSERT) vs reuse (matched UPDATE
/// rewrite) row-lineage channels by branch shape — it KNOWS which branch
/// produced which files, so the commit-layer never has to content-sniff.
struct DistributedMergeExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    commit_op_kind: CommitOpKind,
    branches: Mutex<Option<MergeBranchSet>>,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    execution: QueryExecutionContext,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    cow_operation: Option<CowMergeOperation>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

struct CowMergeOperation {
    session: crate::query_execution::write_operation::ConnectorWriteOperationSession,
    cohort_by_old_file: BTreeMap<String, novarocks_spi::connector::ConnectorWriteCohortId>,
    append_cohort: Option<novarocks_spi::connector::ConnectorWriteCohortId>,
    append_preparation: Option<novarocks_spi::connector::ConnectorWritePreparation>,
}

fn admitted_merge_write_input(
    target: &crate::engine::backend_resolver::TargetBackend,
    preparation: &novarocks_spi::connector::ConnectorWritePreparation,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<
    (
        crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
        Arc<QueryTableBindingStore>,
    ),
    String,
> {
    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding = admit_prepared_connector_write_target(
        table_bindings.as_ref(),
        crate::sql::planner::table::SqlTableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        preparation.clone(),
        planning_lease.clone(),
    )?;
    let sink = sql_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::Data,
        crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
        None,
    )?;
    Ok((sink, table_bindings))
}

impl DistributedMergeExecutor {
    fn run_insert_cohort(
        &self,
        query: &sqlparser::ast::Query,
        preparation: &novarocks_spi::connector::ConnectorWritePreparation,
        registration: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
    ) -> Result<QueryExecutionResult, String> {
        let (sink, table_bindings) =
            admitted_merge_write_input(&self.target, preparation, &self.planning_lease)?;
        let result =
            crate::engine::execute_query_as_iceberg_write_in_operation_with_connector_context(
                &self.state,
                Some(&self.target.catalog),
                &self.target.namespace,
                query,
                sink,
                table_bindings,
                None,
                crate::sql::compiler::RootDistributionRequirement::Any,
                Some(&self.execution),
                &self.connector_context,
                registration,
            )?;
        if let Some(abort) = &result.write_abort {
            return Err(format!(
                "MERGE not-matched INSERT cohort aborted: {}",
                abort.reason
            ));
        }
        let completion = result.connector_completion.as_ref().ok_or_else(|| {
            "MERGE not-matched INSERT cohort completed without a connector completion".to_string()
        })?;
        let staging = completion.staging_summary().map_err(|error| {
            format!(
                "MERGE not-matched INSERT cohort has an invalid connector staging summary: {error}"
            )
        })?;
        if staging.input_rows() == 0 || staging.artifact_count() == 0 {
            return Err("MERGE not-matched INSERT cohort produced no data files".to_string());
        }
        Ok(result)
    }

    fn run_insert_branch(
        &self,
        query: &sqlparser::ast::Query,
        preparation: &novarocks_spi::connector::ConnectorWritePreparation,
    ) -> Result<QueryExecutionResult, String> {
        let (sink, table_bindings) =
            admitted_merge_write_input(&self.target, preparation, &self.planning_lease)?;
        let result = crate::engine::execute_query_as_iceberg_write_with_connector_context(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            query,
            sink,
            table_bindings,
            None,
            crate::sql::compiler::RootDistributionRequirement::Any,
            Some(&self.execution),
            &self.connector_context,
            None,
        )?;
        if let Some(abort) = &result.write_abort {
            return Err(format!(
                "MERGE not-matched INSERT branch aborted: {}",
                abort.reason
            ));
        }
        if result.connector_completion.is_none() {
            return Err("MERGE not-matched INSERT branch produced no data files".to_string());
        }
        Ok(result)
    }

    fn run_stage(&self) -> Result<QueryExecutionResult, String> {
        let branches = self
            .branches
            .lock()
            .expect("MERGE branch set lock poisoned")
            .take()
            .ok_or_else(|| "MERGE branch set was already consumed".to_string())?;

        match branches.matched {
            MergeMatchedBranch::None => {}
            MergeMatchedBranch::CowUpdate(write) => {
                let cow = self.cow_operation.as_ref().ok_or_else(|| {
                    "MERGE COW branches have no sealed connector operation".to_string()
                })?;
                let mut final_result = run_cow_update_file_rewrites(
                    &self.state,
                    &self.target,
                    write,
                    &cow.session,
                    &cow.cohort_by_old_file,
                    &self.execution,
                    &self.connector_context,
                )?;
                if let Some((query, preparation, _)) = branches.insert.as_ref() {
                    let cohort_id = cow.append_cohort.ok_or_else(|| {
                        "MERGE COW append branch has no sealed append cohort".to_string()
                    })?;
                    let registration = crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                        cow.session.clone(),
                        cohort_id,
                    )
                    .map_err(|error| error.to_string())?;
                    final_result = self.run_insert_cohort(query, preparation, registration)?;
                }
                return Ok(final_result);
            }
        }

        if let Some((query, preparation, _)) = branches.insert.as_ref() {
            if self.commit_op_kind != CommitOpKind::FastAppend {
                return Err(format!(
                    "MERGE not-matched INSERT fold does not support commit op {:?}",
                    self.commit_op_kind
                ));
            }
            return self.run_insert_branch(query, preparation);
        }
        Err("MERGE operation produced no writable branch".to_string())
    }
}

impl MutationExecution for DistributedMergeExecutor {
    fn stage(&self) -> Result<QueryExecutionResult, String> {
        self.run_stage()
    }

    fn needs_abort_on_stage_error(&self) -> bool {
        self.cow_operation.is_some()
    }

    fn abort(&self, reason: String) -> Result<CommitOutcome, CommitServiceError> {
        let cow = self
            .cow_operation
            .as_ref()
            .expect("MERGE typed abort requires a sealed connector operation");
        crate::connector::iceberg::write_commit::abort_iceberg_connector_write(
            &self.commit_executor,
            &cow.session,
            self.connector_context.clone(),
            reason,
        )
    }

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        let cow = self
            .cow_operation
            .as_ref()
            .expect("MERGE typed abort requires a sealed connector operation");
        cow.session
            .abort(self.connector_context.clone())
            .map_err(|error| format!("abort COW MERGE connector operation: {error}"))
    }

    fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError> {
        crate::connector::iceberg::write_commit::commit_iceberg_connector_write(
            &self.commit_executor,
            completion,
        )
    }

    fn finalize(&self) -> Result<(), String> {
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.state, &self.target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    fn non_null_col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_target() -> crate::engine::backend_resolver::TargetBackend {
        crate::engine::backend_resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "t".to_string(),
        }
    }

    fn optimizer_output_column(
        name: &str,
        column_id: u32,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> crate::sql::analysis::OutputColumn {
        crate::sql::analysis::OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(column_id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    fn update_mor_expand_child_plan_for_test() -> crate::sql::optimizer::OptimizedOperatorNode {
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::optimized_tree::{
            OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
        };
        use crate::sql::optimizer::statistics::Statistics;

        let output_columns = vec![
            optimizer_output_column("__nr_file", 1, DataType::Utf8, false, true),
            optimizer_output_column("__nr_pos", 2, DataType::Int64, false, true),
            optimizer_output_column("__nr_row_id", 3, DataType::Int64, false, true),
            optimizer_output_column("id", 4, DataType::Int64, false, false),
            optimizer_output_column("qty", 5, DataType::Int64, true, false),
            optimizer_output_column("__nr_new_qty", 6, DataType::Int64, true, true),
        ];
        let mut node = OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: output_columns.clone(),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 3.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        crate::sql::optimizer::optimized_tree::attach_scalar_arena(
            &mut node,
            Arc::new(crate::sql::optimizer::scalar::ScalarArena::new()),
        );
        node
    }

    fn merge_mor_expand_child_plan_for_test(
        include_insert_qty: bool,
    ) -> crate::sql::optimizer::OptimizedOperatorNode {
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::optimized_tree::{
            OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
        };
        use crate::sql::optimizer::statistics::Statistics;

        let mut output_columns = vec![
            optimizer_output_column("__nr_file", 1, DataType::Utf8, true, true),
            optimizer_output_column("__nr_pos", 2, DataType::Int64, true, true),
            optimizer_output_column("__nr_row_id", 3, DataType::Int64, true, true),
            optimizer_output_column("__nr_merge_assert_key", 4, DataType::Int64, false, true),
            optimizer_output_column("__nr_merge_action", 5, DataType::Int64, false, true),
            optimizer_output_column("id", 6, DataType::Int64, true, false),
            optimizer_output_column("qty", 7, DataType::Int64, true, false),
            optimizer_output_column("__nr_new_qty", 8, DataType::Int64, true, true),
            optimizer_output_column("__nr_ins_id", 9, DataType::Int64, true, true),
        ];
        if include_insert_qty {
            output_columns.push(optimizer_output_column(
                "__nr_ins_qty",
                10,
                DataType::Int64,
                true,
                true,
            ));
        }

        let mut node = OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: output_columns.clone(),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 5.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        crate::sql::optimizer::optimized_tree::attach_scalar_arena(
            &mut node,
            Arc::new(crate::sql::optimizer::scalar::ScalarArena::new()),
        );
        node
    }

    fn output_column_by_name_for_test<'a>(
        columns: &'a [crate::sql::analysis::OutputColumn],
        name: &str,
    ) -> &'a crate::sql::analysis::OutputColumn {
        columns
            .iter()
            .find(|column| column.name == name)
            .unwrap_or_else(|| panic!("missing output column {name}"))
    }

    fn assignment_expr_for_output(
        event: &crate::sql::optimizer::operator::ChangeEventSpec,
        output_column_id: crate::sql::column_id::ColumnId,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        event
            .assignments
            .iter()
            .find(|assignment| assignment.output_column_id == output_column_id)
            .unwrap_or_else(|| panic!("missing assignment for output {output_column_id:?}"))
            .expr
            .expect("assignment expression")
    }

    fn assert_assignment_is_column_ref(
        arena: &crate::sql::optimizer::scalar::ScalarArena,
        expr: crate::sql::optimizer::scalar::ScalarId,
        expected: u32,
    ) {
        assert_eq!(
            arena.node(expr),
            &crate::sql::optimizer::scalar::ScalarNode::ColumnRef(
                crate::sql::column_id::ColumnId::new_for_test(expected)
            )
        );
    }

    fn assert_assignment_is_int_literal(
        arena: &crate::sql::optimizer::scalar::ScalarArena,
        expr: crate::sql::optimizer::scalar::ScalarId,
        expected: i64,
    ) {
        assert_eq!(
            arena.node(expr),
            &crate::sql::optimizer::scalar::ScalarNode::Literal(
                crate::sql::optimizer::scalar::HashableLiteral(
                    crate::sql::analysis::LiteralValue::Int(expected)
                )
            )
        );
    }

    fn assert_no_assignment_for_output(
        event: &crate::sql::optimizer::operator::ChangeEventSpec,
        output_column_id: crate::sql::column_id::ColumnId,
    ) {
        assert!(
            event
                .assignments
                .iter()
                .all(|assignment| assignment.output_column_id != output_column_id),
            "unexpected assignment for output {output_column_id:?}"
        );
    }

    fn assert_event_predicate_matches_action(
        arena: &crate::sql::optimizer::scalar::ScalarArena,
        event: &crate::sql::optimizer::operator::ChangeEventSpec,
        expected_action: i32,
    ) {
        use crate::sql::common::BinOp;
        use crate::sql::optimizer::scalar::{HashableLiteral, ScalarNode};

        let predicate = event.predicate.expect("action predicate");
        let ScalarNode::BinaryOp { op, left, right } = arena.node(predicate) else {
            panic!("expected action equality predicate");
        };
        assert_eq!(*op, BinOp::Eq);

        let mut saw_action_column = false;
        let mut saw_action_literal = false;
        for child in [*left, *right] {
            match arena.node(child) {
                ScalarNode::ColumnRef(id)
                    if *id == crate::sql::column_id::ColumnId::new_for_test(5) =>
                {
                    saw_action_column = true;
                }
                ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(
                    value,
                ))) if *value == i64::from(expected_action) => {
                    saw_action_literal = true;
                }
                other => panic!("unexpected action predicate child: {other:?}"),
            }
        }
        assert!(saw_action_column, "predicate must read __nr_merge_action");
        assert!(saw_action_literal, "predicate must compare expected action");
    }

    fn effects_for_test(
        expand: &crate::sql::optimizer::operator::ChangeEventExpandOp,
    ) -> Vec<novarocks_spi::connector::ConnectorRowMutationEffect> {
        expand.events.iter().map(|event| event.effect).collect()
    }

    #[test]
    fn update_mor_change_event_expand_emits_one_replace_effect() {
        use crate::sql::optimizer::operator::Operator;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};

        let target_columns = vec![non_null_col("id"), col("qty")];
        let plan = build_update_mor_change_event_expand_plan(
            update_mor_expand_child_plan_for_test(),
            &target_columns,
            77,
        )
        .expect("MOR UPDATE expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        let Operator::PhysicalDistribution(distribution) = &plan.children[0].op else {
            panic!("expected pre-expand PhysicalDistribution");
        };
        assert_eq!(
            distribution.spec,
            DistributionSpec::HashPartitioned {
                cols: vec![crate::sql::column_id::ColumnId::new_for_test(3)],
                source: HashSource::ShuffleAgg,
            }
        );
        assert_eq!(
            effects_for_test(expand),
            vec![novarocks_spi::connector::ConnectorRowMutationEffect::Replace]
        );
        let effect = output_column_by_name_for_test(
            &expand.output_columns,
            crate::sql::common::change_stream::ROW_MUTATION_EFFECT_COLUMN,
        );
        assert!(effect.is_internal);
        assert_eq!(effect.data_type, DataType::Int8);
        assert_eq!(expand.effect_column_id, effect.column_id);
    }

    #[test]
    fn merge_mor_matched_update_is_one_replace_effect() {
        use crate::sql::optimizer::operator::Operator;

        let plan = build_merge_mor_change_event_expand_plan(
            merge_mor_expand_child_plan_for_test(true),
            &[non_null_col("id"), col("qty")],
            101,
            true,
            false,
            false,
        )
        .expect("MOR MERGE matched UPDATE expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        assert_eq!(
            effects_for_test(expand),
            vec![novarocks_spi::connector::ConnectorRowMutationEffect::Replace]
        );
        let arena = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .expect("scalar arena");
        assert_event_predicate_matches_action(
            arena,
            &expand.events[0],
            MERGE_ACTION_MATCHED_UPDATE,
        );
    }

    #[test]
    fn merge_mor_mixed_delete_and_insert_keep_logical_effects() {
        use crate::sql::optimizer::operator::Operator;

        let plan = build_merge_mor_change_event_expand_plan(
            merge_mor_expand_child_plan_for_test(true),
            &[non_null_col("id"), col("qty")],
            101,
            false,
            true,
            true,
        )
        .expect("MOR MERGE delete+insert expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        assert_eq!(
            effects_for_test(expand),
            vec![
                novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
                novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
            ]
        );
        let arena = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .expect("scalar arena");
        assert_event_predicate_matches_action(
            arena,
            &expand.events[0],
            MERGE_ACTION_MATCHED_DELETE,
        );
        assert_event_predicate_matches_action(
            arena,
            &expand.events[1],
            MERGE_ACTION_NOT_MATCHED_INSERT,
        );
    }

    #[test]
    fn cow_rewrite_query_rewrites_whole_file_and_preserves_row_id() {
        // Two matched rows (row_ids 7,9) for one touched file; the rewrite query
        // must scan the whole file via the synthetic ExplicitFiles table, LEFT
        // JOIN the matched new values on `_row_id`, project user columns
        // (replacement where matched, original scan value otherwise), preserve
        // `_row_id`, and bump `_last_updated_sequence_number` only for matched
        // rows.
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, true),
            arrow::datatypes::Field::new("v", DataType::Utf8, true),
        ]));
        let new_rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![2, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec!["bb", "dd"])) as ArrayRef,
            ],
        )
        .expect("new rows");
        let old_rows = RecordBatch::new_empty(schema);
        let matched = MatchedUpdateBatch {
            row_ids: vec![7, 9],
            file_paths: vec!["f.parquet".to_string(), "f.parquet".to_string()],
            row_positions: vec![1, 3],
            last_updated_sequences: vec![Some(1), Some(1)],
            row_locations: vec![(0, 0), (0, 1)],
            old_rows: vec![old_rows],
            new_rows: vec![new_rows],
        };

        let query = build_cow_rewrite_query(
            &iceberg_target(),
            "__nr_cow_t_abc",
            &matched,
            &[0, 1],
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Utf8),
            ],
            42,
        )
        .expect("query");
        let sql = query.to_string();

        // Scans the synthetic ExplicitFiles table under default_catalog (so a
        // session iceberg catalog cannot reroute it), LEFT JOINs the matched
        // VALUES on `_row_id`, and orders by `_row_id`.
        assert!(sql.contains("`default_catalog`"), "{sql}");
        assert!(sql.contains("`__nr_cow_t_abc`"), "{sql}");
        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(sql.contains("VALUES"), "{sql}");
        // Whole-file rewrite: no outer WHERE filter (all rows re-emitted).
        assert!(!sql.contains(" WHERE "), "{sql}");
        // Conditional replacement on the match key, `_row_id` preserved from the
        // scan, and the new sequence number applied only to matched rows.
        assert!(sql.contains("CASE WHEN"), "{sql}");
        assert!(sql.contains("IS NOT NULL"), "{sql}");
        assert!(sql.contains("AS `_row_id`"), "{sql}");
        assert!(sql.contains("_last_updated_sequence_number"), "{sql}");
        assert!(sql.contains("42"), "{sql}");
        // Replacement values flow from the matched new_rows VALUES.
        assert!(sql.contains("'bb'"), "{sql}");
        assert!(sql.contains("'dd'"), "{sql}");
        assert!(sql.contains("ORDER BY"), "{sql}");
    }

    #[test]
    fn cow_rewrite_query_casts_variant_values_payloads() {
        let payload = [0x0c_u8, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, true),
            arrow::datatypes::Field::new("v", DataType::LargeBinary, true),
        ]));
        let new_rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![10])) as ArrayRef,
                Arc::new(arrow::array::LargeBinaryArray::from_iter_values([
                    payload.as_slice()
                ])) as ArrayRef,
            ],
        )
        .expect("new rows");
        let old_rows = RecordBatch::new_empty(schema);
        let matched = MatchedUpdateBatch {
            row_ids: vec![7],
            file_paths: vec!["f.parquet".to_string()],
            row_positions: vec![1],
            last_updated_sequences: vec![Some(1)],
            row_locations: vec![(0, 0)],
            old_rows: vec![old_rows],
            new_rows: vec![new_rows],
        };

        let query = build_cow_rewrite_query(
            &iceberg_target(),
            "__nr_cow_t_abc",
            &matched,
            &[0],
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::LargeBinary),
            ],
            42,
        )
        .expect("query");
        let sql = query.to_string();

        assert!(sql.contains("CAST(X'0C000000010203' AS VARIANT)"), "{sql}");
        assert!(sql.contains("CASE WHEN"), "{sql}");
    }

    #[test]
    fn reject_reserved_update_columns() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "_row_id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
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
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
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
        let assignments = vec![crate::sql::parser::ast::UpdateAssignment {
            column: "v".to_string(),
            value: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("src_v")),
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
        let assignments = vec![crate::sql::parser::ast::UpdateAssignment {
            column: "v".to_string(),
            value: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("src_v")),
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
        let raw = crate::sql::parser::parse_sql_raw(
            "MERGE INTO t AS t \
             USING (SELECT 3 AS id, 4 AS v) AS s \
             ON t.id = s.id \
             WHEN NOT MATCHED AND s.id > 0 THEN INSERT (id) VALUES (s.id)",
        )
        .expect("parse MERGE");
        let stmt = crate::engine::statement::convert_sqlparser_merge_to_custom(&raw)
            .expect("convert MERGE");
        let target_columns = vec![col("id"), col("v")];
        let insert_columns = resolve_merge_insert_columns(
            &stmt.not_matched.as_ref().expect("not matched").action,
            &target_columns,
        )
        .expect("insert columns");
        let state = Arc::new(StandaloneState::default());

        let query = build_merge_unmatched_insert_query(
            &state,
            &iceberg_target(),
            &stmt,
            None,
            &target_columns,
            &insert_columns,
        )
        .expect("query");
        let sql = query.to_string();

        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(sql.contains("_row_id"), "{sql}");
        assert!(sql.contains("IS NULL"), "{sql}");
        assert!(sql.contains("CAST((s.id) AS BIGINT) AS `id`"), "{sql}");
        assert!(sql.contains("CAST(NULL AS BIGINT) AS `v`"), "{sql}");
        assert!(sql.contains("(s.id > 0)"), "{sql}");
    }
}
