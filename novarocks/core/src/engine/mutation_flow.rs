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

#[cfg(test)]
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, Int64Array, StringArray};
#[cfg(test)]
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

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
        target: &crate::connector::write_target::ConnectorWriteTargetBinding,
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
        let (lease, preparation) =
            target.prepare_row_mutation(target_ref, operation_id, intent, context.clone())?;
        Ok(Self {
            operation_id,
            lease,
            preparation,
            context,
        })
    }

    /// Wrap a preparation this statement already obtained.
    ///
    /// A statement signs exactly one row-mutation preparation. Callers that read
    /// the strategy off it during admission reuse that same value here rather
    /// than asking the provider again, so one statement never carries two base
    /// versions or two digests.
    fn from_signed(
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        lease: novarocks_spi::connector::ConnectorWriteLease,
        preparation: novarocks_spi::connector::ConnectorRowMutationPreparation,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            operation_id,
            lease,
            preparation,
            context,
        }
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
    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String>;
    fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext;
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
            .commit(self.terminal_context())
            .map_err(|error| error.to_string())
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
    pub(crate) target_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub(crate) target_ref: String,
    /// Exact Provider schema that belongs to the opaque preparation table.
    /// COW match materialization scans this schema through the retained lease;
    /// it must not resolve the target through the current catalog again.
    pub(crate) match_target_schema: arrow::datatypes::SchemaRef,
    /// The one exact connector generation admitted with this statement.
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
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
    pub(crate) target_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub(crate) target_ref: String,
    /// See [`PreparedUpdateMutation::match_target_schema`].
    pub(crate) match_target_schema: arrow::datatypes::SchemaRef,
    /// See [`PreparedUpdateMutation::mode`].
    pub(crate) table_write_mode: novarocks_spi::connector::ConnectorRowMutationStrategy,
    /// The one exact connector generation admitted with this statement.
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
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
        crate::engine::row_mutation::BoundedRowMutationMatchCollector::try_new_with_schema(
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
    let mut validator = crate::engine::row_mutation::RowMutationMatchValidator::try_new(
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
) -> Vec<novarocks_catalog::schema::ColumnDef> {
    preparation
        .match_contract()
        .after_fields()
        .iter()
        .map(|field| novarocks_catalog::schema::ColumnDef {
            name: field.field().name().to_string(),
            data_type: field.field().data_type().clone(),
            nullable: field.field().is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect()
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

    // Reject a managed materialized view from neutral metadata under an exact
    // generation. This cannot move into row-mutation admission: incremental MV
    // refresh drives its own writes through that same admission, so at that
    // level a user statement is indistinguishable from the MV machinery
    // maintaining its own target.
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
        state,
        &target,
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Update,
    )?;

    let target_binding = crate::connector::write_target::load_write_target_binding(
        state.connector_control.as_ref(),
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
    let strategy_operation_id = novarocks_spi::connector::ConnectorWriteOperationId::new();
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
    let match_target_schema = strategy_preparation.match_source_schema().clone();
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
    let signed_preparations = DmlChangeStreamPreparations::from_signed(
        strategy_operation_id,
        strategy_lease,
        strategy_preparation,
        connector_context.clone(),
    );
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
                read_snapshot_id: admitted_base_snapshot_id,
                preparations: signed_preparations,
                planning_lease: planning_lease.clone(),
            })
        } else {
            None
        };
    Ok(PreparedUpdateMutation {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        target,
        target_columns,
        target_ref,
        match_target_schema,
        planning_lease,
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
    state: &Arc<StandaloneState>,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
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
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "MERGE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    // See the UPDATE path for why this rejection cannot live in row-mutation
    // admission.
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
        state,
        &target,
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Merge,
    )?;
    let target_binding = crate::connector::write_target::load_write_target_binding(
        state.connector_control.as_ref(),
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
            Some(MergeMatchedAction::Update { .. })
        ),
        matched_delete: matches!(
            stmt.matched.as_ref().map(|clause| &clause.action),
            Some(MergeMatchedAction::Delete)
        ),
        not_matched_insert: stmt.not_matched.is_some(),
    };
    let preparations = DmlChangeStreamPreparations::prepare(
        &target_binding,
        &target_ref,
        effect_set,
        connector_context.clone(),
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
    let match_target_schema = preparations.preparation.match_source_schema().clone();
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
        && let MergeMatchedAction::Update { assignments } = &clause.action
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
    Ok(PreparedMergeMutation {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        target,
        target_columns,
        target_ref,
        match_target_schema,
        table_write_mode,
        planning_lease,
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
    state: &Arc<StandaloneState>,
    prepared: PreparedUpdateMutation,
) -> Result<MutationStagedWrite, String> {
    let PreparedUpdateMutation {
        stmt,
        current_catalog,
        target,
        target_columns,
        target_ref,
        match_target_schema,
        planning_lease,
        cow_preparations,
        mor_write_target,
        mode,
        admitted_base_snapshot_id,
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
                &cow_preparations.preparation,
                planning_lease.clone(),
                &match_target_schema,
                &execution,
                &connector_context,
            )?;
            let selection = cow_selection_from_query_result(
                matched,
                &cow_preparations.preparation,
                connector_context.clone(),
            )?;
            if selection.row_count() == 0 {
                return Ok(MutationStagedWrite::NoOp);
            }
            let provider_plan = cow_preparations
                .lease
                .activate_row_mutation(
                    novarocks_spi::connector::ConnectorRowMutationActivationRequest::CopyOnWrite {
                        preparation: cow_preparations.preparation,
                        selection,
                        context: connector_context.clone(),
                    },
                )
                .map_err(|error| format!("activate Provider COW UPDATE plan: {error}"))?;
            let write = build_cow_update_distributed_write(
                &target,
                planning_lease,
                &connector_context,
                provider_plan,
                cow_preparations.lease,
                &execution,
            )?;
            let execution_handle = build_cow_update_distributed_execution(
                state,
                &target,
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
        other @ (novarocks_spi::connector::ConnectorRowMutationStrategy::PositionDelete
        | novarocks_spi::connector::ConnectorRowMutationStrategy::DeletionVector
        | novarocks_spi::connector::ConnectorRowMutationStrategy::EqualityDelete) => Err(format!(
            "UPDATE cannot be served by row-mutation strategy {other:?}"
        )),
        novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead => {
            let PreparedMorUpdateWriteTarget {
                read_snapshot_id,
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
            let preparations = preparations.activate()?;
            let write_lease = write_planning_lease
                .derive_write_lease()
                .map_err(|error| format!("derive MOR UPDATE write lease: {error}"))?;
            let (commit_executor, entry) =
                crate::engine::iceberg_writer::build_iceberg_row_commit_executor(
                    state,
                    &target,
                    &target_ref,
                    novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead,
                    read_snapshot_id,
                )?;
            let write = build_update_mor_change_stream_write_plan(
                state,
                &target,
                &stmt,
                current_catalog.as_deref(),
                &target_columns,
                &target_ref,
                written_version,
                &execution,
                &connector_context,
                &preparations,
                write_planning_lease,
            )?;
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

#[cfg(test)]
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

    fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        self.connector_context.clone()
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

    fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        self.connector_context.clone()
    }

    fn finalize(&self) -> Result<(), String> {
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.state, &self.target)
    }
}
struct CowFrozenRead {
    identity: crate::sql::planner::table::SqlTableIdentity,
    schema: arrow::datatypes::SchemaRef,
    read: crate::query_execution::preparation::scan::PlannedConnectorRead,
}

struct CowCohortWritePlan {
    cohort_id: novarocks_spi::connector::ConnectorWriteCohortId,
    preparation: novarocks_spi::connector::ConnectorWritePreparation,
    query: sqlparser::ast::Query,
    frozen_read: Option<CowFrozenRead>,
}

struct CowUpdateDistributedWrite {
    cohorts: Vec<CowCohortWritePlan>,
    provider_plan: novarocks_spi::connector::ConnectorRowMutationExecutionPlan,
    write_lease: novarocks_spi::connector::ConnectorWriteLease,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

fn build_cow_update_distributed_write(
    target: &crate::engine::backend_resolver::TargetBackend,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    provider_plan: novarocks_spi::connector::ConnectorRowMutationExecutionPlan,
    write_lease: novarocks_spi::connector::ConnectorWriteLease,
    execution: &QueryExecutionContext,
) -> Result<CowUpdateDistributedWrite, String> {
    let (selection, _, recipes) = provider_plan
        .copy_on_write()
        .ok_or_else(|| "COW mutation is missing provider-sealed recipes".to_string())?;
    let route_by_id = provider_plan
        .routes()
        .iter()
        .map(|route| (route.route_id(), route))
        .collect::<HashMap<_, _>>();
    let mut cohorts = Vec::with_capacity(recipes.len());
    for recipe in recipes {
        let route = route_by_id
            .get(&recipe.route_id())
            .copied()
            .ok_or_else(|| "COW recipe references an unknown route".to_string())?;
        let (query, frozen_read) = match recipe.body() {
            novarocks_spi::connector::ConnectorRowMutationCohortRecipeBody::Append => (
                build_cow_append_query(selection, recipe, route, provider_plan.preparation())?,
                None,
            ),
            novarocks_spi::connector::ConnectorRowMutationCohortRecipeBody::Rewrite {
                source,
                base_version_digest,
                scan_schema,
                scan_bindings,
                match_tokens,
                written_version_token,
                ..
            } => {
                if *base_version_digest != provider_plan.preparation().base_version().digest() {
                    return Err(
                        "COW rewrite recipe base differs from its signed preparation".to_string(),
                    );
                }
                let identity = crate::sql::planner::table::SqlTableIdentity {
                    catalog: "default_catalog".to_string(),
                    namespace: target.namespace.clone(),
                    table: format!("__nr_cow_{}", uuid::Uuid::new_v4().simple()),
                };
                let read =
                    crate::query_execution::frozen_connector_read::plan_frozen_connector_read(
                        planning_lease.clone(),
                        execution.topology(),
                        source,
                        scan_schema,
                        Vec::new(),
                        connector_context.clone(),
                    )
                    .map_err(|error| format!("plan provider-frozen COW source: {error}"))?;
                let query = build_cow_rewrite_query(
                    selection,
                    recipe,
                    route,
                    provider_plan.preparation(),
                    &identity,
                    scan_schema,
                    scan_bindings,
                    match_tokens,
                    *written_version_token,
                )?;
                (
                    query,
                    Some(CowFrozenRead {
                        identity,
                        schema: scan_schema.clone(),
                        read,
                    }),
                )
            }
        };
        cohorts.push(CowCohortWritePlan {
            cohort_id: recipe.cohort_id(),
            preparation: route.preparation().clone(),
            query,
            frozen_read,
        });
    }
    Ok(CowUpdateDistributedWrite {
        cohorts,
        provider_plan,
        write_lease,
        planning_lease,
    })
}

fn ordered_route_inputs(
    route: &novarocks_spi::connector::ConnectorRowMutationRoute,
) -> Result<Vec<novarocks_spi::connector::ConnectorMutationRouteInput>, String> {
    let mut inputs = route.input_ordinals().to_vec();
    inputs.sort_by_key(|input| input.input_ordinal());
    if inputs
        .iter()
        .enumerate()
        .any(|(ordinal, input)| input.input_ordinal() as usize != ordinal)
    {
        return Err("COW route input ordinals are not dense from zero".to_string());
    }
    Ok(inputs)
}

fn route_field_by_token(
    route: &novarocks_spi::connector::ConnectorRowMutationRoute,
) -> HashMap<novarocks_spi::connector::ConnectorWriteFieldToken, arrow::datatypes::Field> {
    route
        .input()
        .fields()
        .into_iter()
        .map(|binding| (binding.token(), binding.field().clone()))
        .collect()
}

fn selection_field_ordinal(
    contract: &novarocks_spi::connector::ConnectorMutationMatchContract,
    token: novarocks_spi::connector::ConnectorWriteFieldToken,
) -> Option<u32> {
    contract
        .identity_fields()
        .iter()
        .find(|field| field.token() == token)
        .map(|field| field.source_ordinal())
        .or_else(|| {
            contract
                .before_fields()
                .iter()
                .find(|field| field.token() == token)
                .map(|field| field.target_ordinal())
        })
        .or_else(|| {
            contract
                .after_fields()
                .iter()
                .find(|field| field.token() == token)
                .map(|field| field.target_ordinal())
        })
}

fn selection_value_sql(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    row: novarocks_spi::connector::ConnectorRowMutationSelectionOrdinal,
    field_ordinal: u32,
    field: &arrow::datatypes::Field,
) -> Result<String, String> {
    let view = selection
        .locate(row)
        .ok_or_else(|| "COW recipe selection ordinal is out of bounds".to_string())?;
    let array = view
        .batch()
        .columns()
        .get(field_ordinal as usize)
        .ok_or_else(|| "COW selection field ordinal is out of bounds".to_string())?;
    let literal = crate::sql::literal::literal_from_batch(array, view.row_index())?;
    let column = novarocks_catalog::schema::ColumnDef {
        name: field.name().to_string(),
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
        write_default: None,
        logical_type: None,
    };
    let literal =
        crate::engine::iceberg_writer::literal_to_sql_for_arrow_type(&literal, field.data_type())?;
    crate::engine::iceberg_writer::target_cast_expr_sql(&literal, &column)
}

fn build_cow_append_query(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    recipe: &novarocks_spi::connector::ConnectorRowMutationCohortRecipe,
    route: &novarocks_spi::connector::ConnectorRowMutationRoute,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Result<sqlparser::ast::Query, String> {
    let fields = route_field_by_token(route);
    let inputs = ordered_route_inputs(route)?;
    let mut value_rows = Vec::with_capacity(recipe.selection_ordinals().len());
    for row in recipe.selection_ordinals() {
        let values = inputs
            .iter()
            .map(|input| {
                let field = fields
                    .get(&input.token())
                    .ok_or_else(|| "COW append route token has no signed field".to_string())?;
                let field_ordinal =
                    selection_field_ordinal(preparation.match_contract(), input.token())
                        .ok_or_else(|| {
                            "COW append token is absent from the signed selection".to_string()
                        })?;
                selection_value_sql(selection, *row, field_ordinal, field)
            })
            .collect::<Result<Vec<_>, _>>()?;
        value_rows.push(format!("({})", values.join(", ")));
    }
    let aliases = (0..inputs.len())
        .map(|ordinal| sql_identifier(&format!("__nr_v_{ordinal}")))
        .collect::<Vec<_>>();
    let select_items = inputs
        .iter()
        .enumerate()
        .map(|(ordinal, input)| {
            let field = fields
                .get(&input.token())
                .ok_or_else(|| "COW append route token has no signed field".to_string())?;
            let column = novarocks_catalog::schema::ColumnDef {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                write_default: None,
                logical_type: None,
            };
            Ok(format!(
                "{} AS {}",
                crate::engine::iceberg_writer::target_cast_expr_sql(
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
        "COW append recipe",
    )
}

#[allow(clippy::too_many_arguments)]
fn build_cow_rewrite_query(
    selection: &novarocks_spi::connector::ConnectorRowMutationSelection,
    recipe: &novarocks_spi::connector::ConnectorRowMutationCohortRecipe,
    route: &novarocks_spi::connector::ConnectorRowMutationRoute,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    identity: &crate::sql::planner::table::SqlTableIdentity,
    scan_schema: &arrow::datatypes::SchemaRef,
    scan_bindings: &[novarocks_spi::connector::ConnectorRowMutationScanBinding],
    match_tokens: &[novarocks_spi::connector::ConnectorWriteFieldToken],
    written_version_token: Option<novarocks_spi::connector::ConnectorWriteFieldToken>,
) -> Result<sqlparser::ast::Query, String> {
    let contract = preparation.match_contract();
    let fields = route_field_by_token(route);
    let inputs = ordered_route_inputs(route)?;
    let scan_by_token = scan_bindings
        .iter()
        .map(|binding| (binding.token(), binding.scan_ordinal()))
        .collect::<HashMap<_, _>>();
    let after_by_token = contract
        .after_fields()
        .iter()
        .map(|field| (field.token(), field.target_ordinal()))
        .collect::<HashMap<_, _>>();
    let mut values_tokens = match_tokens.to_vec();
    for input in &inputs {
        if after_by_token.contains_key(&input.token()) && !values_tokens.contains(&input.token()) {
            values_tokens.push(input.token());
        }
    }
    let marker_alias = "__nr_matched";
    let effect_alias = "__nr_effect";
    let value_alias = |ordinal: usize| format!("__nr_v_{ordinal}");
    let mut value_rows = Vec::with_capacity(recipe.selection_ordinals().len());
    for row in recipe.selection_ordinals() {
        let mut values = Vec::with_capacity(values_tokens.len() + 2);
        for token in &values_tokens {
            let ordinal = selection_field_ordinal(contract, *token).ok_or_else(|| {
                "COW recipe token is absent from the signed selection".to_string()
            })?;
            let field = selection
                .schema()
                .fields()
                .get(ordinal as usize)
                .ok_or_else(|| "COW recipe selection field is out of bounds".to_string())?;
            values.push(selection_value_sql(selection, *row, ordinal, field)?);
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
    let mut select_items = Vec::with_capacity(inputs.len());
    for input in &inputs {
        let field = fields
            .get(&input.token())
            .ok_or_else(|| "COW rewrite route token has no signed field".to_string())?;
        let scan_ordinal = scan_by_token
            .get(&input.token())
            .copied()
            .ok_or_else(|| "COW rewrite route token has no scan binding".to_string())?;
        let scan_field = scan_schema
            .fields()
            .get(scan_ordinal as usize)
            .ok_or_else(|| {
                "COW rewrite scan binding is outside the signed scan schema".to_string()
            })?;
        let scan_value = qualify_column("__nr_scan", scan_field.name());
        let expression = if Some(input.token()) == written_version_token {
            let written_version = preparation.written_version_ordinal().ok_or_else(|| {
                "COW rewrite recipe requires a signed written version".to_string()
            })?;
            format!("CASE WHEN {matched} THEN {written_version} ELSE {scan_value} END")
        } else if after_by_token.contains_key(&input.token()) {
            let position = values_position
                .get(&input.token())
                .copied()
                .ok_or_else(|| "COW rewrite after-image token has no VALUES binding".to_string())?;
            format!(
                "CASE WHEN {matched} THEN {} ELSE {scan_value} END",
                qualify_column("__nr_match", &value_alias(position))
            )
        } else {
            scan_value
        };
        let column = novarocks_catalog::schema::ColumnDef {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        };
        select_items.push(format!(
            "{} AS {}",
            crate::engine::iceberg_writer::target_cast_expr_sql(&expression, &column)?,
            sql_identifier(field.name())
        ));
    }
    let joins = match_tokens
        .iter()
        .map(|token| {
            let scan_ordinal = scan_by_token
                .get(token)
                .copied()
                .ok_or_else(|| "COW match token has no scan binding".to_string())?;
            let scan_field = scan_schema
                .fields()
                .get(scan_ordinal as usize)
                .ok_or_else(|| {
                    "COW match scan binding is outside the signed scan schema".to_string()
                })?;
            let position = values_position
                .get(token)
                .copied()
                .ok_or_else(|| "COW match token has no VALUES binding".to_string())?;
            Ok(format!(
                "{} = {}",
                qualify_column("__nr_scan", scan_field.name()),
                qualify_column("__nr_match", &value_alias(position))
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let scan = format!(
        "{}.{}.{} AS {}",
        sql_identifier(&identity.catalog),
        sql_identifier(&identity.namespace),
        sql_identifier(&identity.table),
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
        "COW rewrite recipe",
    )
}

struct DistributedCowUpdateExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    write: Mutex<Option<CowUpdateDistributedWrite>>,
    operation_session: crate::query_execution::write_operation::ConnectorWriteOperationSession,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl DistributedCowUpdateExecutor {
    fn run_stage(&self) -> Result<QueryExecutionResult, String> {
        let write = self
            .write
            .lock()
            .expect("COW write plan lock poisoned")
            .take()
            .ok_or_else(|| "COW write plan was already consumed".to_string())?;
        run_cow_cohort_writes(
            &self.state,
            &self.target,
            write,
            &self.operation_session,
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

    fn abort_terminal(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        self.operation_session
            .abort(self.connector_context.clone())
            .map_err(|error| format!("abort COW connector operation: {error}"))
    }

    fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        self.connector_context.clone()
    }

    fn finalize(&self) -> Result<(), String> {
        // Terminal side effects, including Provider-local cache invalidation,
        // belong to the exact connector generation that committed the session.
        Ok(())
    }
}

fn run_cow_cohort_writes(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    write: CowUpdateDistributedWrite,
    operation_session: &crate::query_execution::write_operation::ConnectorWriteOperationSession,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryExecutionResult, String> {
    let planning_lease = write.planning_lease;
    let mut final_result = None;
    for plan in write.cohorts {
        let registration =
            crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                operation_session.clone(),
                plan.cohort_id,
            )
            .map_err(|error| error.to_string())?;
        let result = run_one_cow_cohort(
            state,
            target,
            plan,
            &planning_lease,
            registration,
            execution,
            connector_context,
        )?;
        if result.connector_completion.is_none() {
            return Err("COW cohort completed without a connector completion".to_string());
        }
        final_result = Some(result);
    }
    final_result.ok_or_else(|| "COW operation has no provider-sealed cohorts".to_string())
}

fn run_one_cow_cohort(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    plan: CowCohortWritePlan,
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
        plan.preparation.clone(),
        planning_lease.clone(),
    )?;
    let sink_mode = match plan.preparation.input() {
        novarocks_spi::connector::ConnectorWriteInputShape::Data { .. } => {
            crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::Data
        }
        novarocks_spi::connector::ConnectorWriteInputShape::RowLineage { .. } => {
            crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::RowLineageData
        }
        _ => return Err("COW recipe returned an unsupported writer input shape".to_string()),
    };
    let sink = sql_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        sink_mode,
        crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
        None,
    )?;
    let result = match plan.frozen_read {
        Some(frozen) => {
            let binding =
                crate::query_execution::frozen_connector_read::admit_frozen_connector_scan_binding(
                    table_bindings.as_ref(),
                    &frozen.identity,
                    &frozen.schema,
                )?;
            let overlay =
                crate::query_execution::frozen_connector_read::frozen_connector_query_local_overlay(
                    &frozen.identity,
                    &frozen.schema,
                );
            let resolver =
                crate::query_execution::frozen_connector_read::FrozenConnectorReadResolver::new(
                    binding,
                    frozen.identity,
                    frozen.read,
                );
            crate::engine::execute_query_as_iceberg_write_in_operation_with_query_local_overlays(
                state,
                Some(&target.catalog),
                &target.namespace,
                &plan.query,
                sink,
                table_bindings,
                None,
                crate::sql::compiler::RootDistributionRequirement::Any,
                Some(execution),
                connector_context,
                connector_write,
                &resolver,
                std::slice::from_ref(&overlay),
            )?
        }
        None => crate::engine::execute_query_as_iceberg_write_in_operation_with_connector_context(
            state,
            Some(&target.catalog),
            &target.namespace,
            &plan.query,
            sink,
            table_bindings,
            None,
            crate::sql::compiler::RootDistributionRequirement::Any,
            Some(execution),
            connector_context,
            connector_write,
        )?,
    };
    if let Some(abort) = &result.write_abort {
        return Err(format!("COW cohort aborted: {}", abort.reason));
    }
    let staging = result
        .connector_completion
        .as_ref()
        .ok_or_else(|| "COW cohort completed without a connector completion".to_string())?
        .staging_summary()
        .map_err(|error| format!("COW cohort staging summary is invalid: {error}"))?;
    if staging.input_rows() == 0 || staging.artifact_count() == 0 {
        return Err("COW cohort produced no staged rows or artifacts".to_string());
    }
    Ok(result)
}

fn build_cow_update_distributed_execution(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    write: CowUpdateDistributedWrite,
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<DistributedCowUpdateExecutor>, String> {
    let (_, sealed, _) = write
        .provider_plan
        .copy_on_write()
        .ok_or_else(|| "COW mutation is missing provider-sealed cohorts".to_string())?;
    let operation_id = sealed.operation_id();
    let sealed = sealed.clone();
    let write_lease = write.write_lease.clone();
    let activation = write_lease
        .activate_write(novarocks_spi::connector::ConnectorWriteActivationRequest {
            operation_id,
            source: novarocks_spi::connector::ConnectorWriteActivationSource::RowMutation(
                write.provider_plan.clone(),
            ),
            intent: novarocks_spi::connector::ConnectorWriteActivationIntent::Ordinary,
            context: connector_context.clone(),
        })
        .map_err(|error| format!("activate exact COW generation: {error}"))?;
    let begin = (|| {
        let mut templates = Vec::with_capacity(write.cohorts.len());
        for plan in &write.cohorts {
            let cohort = activation.cohort(plan.cohort_id).ok_or_else(|| {
                "exact COW activation omitted a provider-sealed cohort".to_string()
            })?;
            templates.push(
                crate::query_execution::contract::ConnectorWritePlanningTemplate::from_activated_cohort(
                    cohort,
                    connector_context.clone(),
                    write_lease.clone(),
                )
                .map_err(|error| format!("build activated COW template: {error}"))?,
            );
        }
        let registration =
            crate::query_execution::contract::ConnectorWriteOperationRegistration::try_new(
                templates,
            )
            .map_err(|error| error.to_string())?;
        state
            .query_execution
            .begin_write_operation(registration, write_lease.clone())
            .map_err(|error| error.to_string())
    })();
    let operation_session = match begin {
        Ok(session) => session,
        Err(error) => {
            let abort = novarocks_spi::connector::ConnectorWriteAbortRequest::try_new(
                write_lease.binding_key().clone(),
                sealed,
                Vec::new(),
                connector_context.clone(),
            )
            .and_then(|request| write_lease.control().abort(request));
            return match abort {
                Ok(_) => Err(error),
                Err(abort_error) => Err(format!(
                    "{error}; abort activated COW operation after begin failure: {abort_error}"
                )),
            };
        }
    };
    Ok(Arc::new(DistributedCowUpdateExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        write: Mutex::new(Some(write)),
        operation_session,
        execution,
        connector_context: connector_context.clone(),
    }))
}
#[cfg(test)]
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

#[cfg(test)]
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

/// Execute a COW match query with the target replaced by the exact opaque
/// table handle retained by row-mutation preparation. Other statement sources
/// still resolve normally, but the mutation target cannot observe a later
/// catalog generation or ref head.
#[allow(clippy::too_many_arguments)]
fn execute_exact_cow_match_query(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    query: &sqlparser::ast::Query,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    target_schema: &arrow::datatypes::SchemaRef,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryResult, String> {
    let identity = crate::sql::planner::table::SqlTableIdentity {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    };
    let read = crate::query_execution::frozen_connector_read::plan_frozen_connector_read(
        planning_lease,
        execution.topology(),
        preparation.match_source(),
        target_schema,
        Vec::new(),
        connector_context.clone(),
    )
    .map_err(|error| format!("plan exact COW match target: {error}"))?;
    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let binding =
        crate::query_execution::frozen_connector_read::admit_frozen_connector_scan_binding(
            table_bindings.as_ref(),
            &identity,
            target_schema,
        )?;
    let overlay =
        crate::query_execution::frozen_connector_read::frozen_connector_query_local_overlay(
            &identity,
            target_schema,
        );
    let resolver = crate::query_execution::frozen_connector_read::FrozenConnectorReadResolver::new(
        binding, identity, read,
    );
    let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
    let analyzer_catalog =
        crate::engine::build_catalog_service_provider_with_bindings_and_query_local_overlays(
            Some(&target.catalog),
            &catalog_service_snapshot,
            state.connector_control.as_ref(),
            connector_context.clone(),
            Arc::clone(&table_bindings),
            vec![overlay],
        );
    let statistics =
        crate::engine::query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
            state,
            Arc::clone(&table_bindings),
        );
    let catalog = crate::sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_catalog);
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| "COW match execution requires a non-empty admitted topology".to_string())?;
    let request = crate::sql::compiler::SqlCompileRequest::new(
        crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(query.clone())),
        crate::sql::compiler::SqlCompileIntent::Query,
        crate::sql::compiler::SqlSessionContext {
            current_catalog: Some(target.catalog.clone()),
            current_database: target.namespace.clone(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog,
        &statistics,
        crate::sql::functions::builtin_sql_function_catalog(),
        None,
        crate::sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::engine::query_planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let crate::sql::compiler::SqlCompileOutput::Distributed(compiled) =
        crate::sql::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err("COW match query did not produce a distributed plan".to_string());
    };
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &compiled.distributed_plan,
        state.connector_control.as_ref(),
        connector_context,
        Some(table_bindings.as_ref()),
        Some(&resolver),
        crate::engine::scan_preparation_options(execution.optimizer_settings(), execution)?,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &compiled.distributed_plan,
        &prepared,
    )?;
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        None,
        crate::query_execution::contract::DistributedQueryIntent::Result,
        execution,
    )
    .map_err(|error| error.to_string())?;
    state
        .query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_result)
        .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
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
fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| format!("UPDATE match query missing `{name}` column"))?;
    Ok(batch.column(idx))
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

fn build_exact_cow_update_selection_query(
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &UpdateStmt,
    source_sql: Option<&str>,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Result<sqlparser::ast::Query, String> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    let qualify = |name: &str| format!("{}.{}", sql_identifier(target_alias), sql_identifier(name));
    let assignments = stmt
        .assignments
        .iter()
        .map(|assignment| {
            (
                assignment.column.to_ascii_lowercase(),
                assignment.value.to_string(),
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
        "SELECT {} FROM {}.{}.{} AS {}",
        select_items.join(", "),
        sql_identifier(&target.catalog),
        sql_identifier(&target.namespace),
        sql_identifier(&target.table),
        sql_identifier(target_alias),
    );
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(predicate) = &stmt.where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(&predicate.to_string());
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
    state: &Arc<StandaloneState>,
    prepared: PreparedMergeMutation,
) -> Result<MutationStagedWrite, String> {
    let PreparedMergeMutation {
        stmt,
        current_catalog,
        target,
        target_columns,
        target_ref,
        match_target_schema,
        table_write_mode,
        planning_lease,
        cow_preparations,
        mor_write_target,
        insert_columns_resolved,
        admitted_base_snapshot_id,
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
    if table_write_mode == novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead
        || has_matched_delete
    {
        if !has_matched_update && !has_matched_delete && !has_not_matched_insert {
            return Ok(MutationStagedWrite::NoOp);
        }
        let base_snapshot_id = admitted_base_snapshot_id;
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
        let preparations = preparations.activate()?;
        let write_lease = write_planning_lease
            .derive_write_lease()
            .map_err(|error| format!("derive MOR MERGE write lease: {error}"))?;
        let (commit_executor, entry) =
            crate::engine::iceberg_writer::build_iceberg_row_commit_executor(
                state,
                &target,
                &target_ref,
                novarocks_spi::connector::ConnectorRowMutationStrategy::MergeOnRead,
                base_snapshot_id,
            )?;
        let write = build_merge_mor_change_stream_write_plan(
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
            &preparations,
            write_planning_lease,
        )?;
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
        &cow_preparations.preparation,
        planning_lease.clone(),
        &match_target_schema,
        &execution,
        &connector_context,
    )?;
    let selection = cow_selection_from_query_result(
        matched,
        &cow_preparations.preparation,
        connector_context.clone(),
    )?;
    if selection.row_count() == 0 {
        return Ok(MutationStagedWrite::NoOp);
    }
    let provider_plan = cow_preparations
        .lease
        .activate_row_mutation(
            novarocks_spi::connector::ConnectorRowMutationActivationRequest::CopyOnWrite {
                preparation: cow_preparations.preparation,
                selection,
                context: connector_context.clone(),
            },
        )
        .map_err(|error| format!("activate Provider COW MERGE plan: {error}"))?;
    let write = build_cow_update_distributed_write(
        &target,
        planning_lease,
        &connector_context,
        provider_plan,
        cow_preparations.lease,
        &execution,
    )?;
    let execution_handle = build_cow_update_distributed_execution(
        state,
        &target,
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

#[cfg(test)]
struct MergeMatchRows {
    /// The full RecordBatch from the MERGE match SELECT, with rows for both
    /// matched and unmatched cases. Filters for each side are derived from
    /// `__nr_match_kind` / `__nr_matched_apply` / `__nr_unmatched_apply`.
    full: RecordBatch,
}

#[cfg(test)]
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

#[cfg(test)]
fn materialize_merge_match(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
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

fn build_exact_cow_merge_selection_query(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    insert_columns: Option<&[MergeInsertColumn]>,
    preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
) -> Result<sqlparser::ast::Query, String> {
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
        .and_then(|clause| clause.predicate.as_ref())
        .map(ToString::to_string)
        .unwrap_or_else(|| "TRUE".to_string());
    let insert_predicate = stmt
        .not_matched
        .as_ref()
        .and_then(|clause| clause.predicate.as_ref())
        .map(ToString::to_string)
        .unwrap_or_else(|| "TRUE".to_string());
    let assignments = match stmt.matched.as_ref().map(|clause| &clause.action) {
        Some(MergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|assignment| {
                (
                    assignment.column.to_ascii_lowercase(),
                    assignment.value.to_string(),
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
                        clause.action.values[index].to_string(),
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
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
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
        Some(MergeMatchedAction::Update { .. })
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
            "SELECT {} FROM {} LEFT JOIN {}.{}.{} AS {} ON {} WHERE {}",
            select_items.join(", "),
            source_sql,
            sql_identifier(&target.catalog),
            sql_identifier(&target.namespace),
            sql_identifier(&target.table),
            sql_identifier(target_alias),
            stmt.on,
            admitted_actions.join(" OR "),
        ),
        "exact COW MERGE selection",
    )
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

#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

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

    struct CowRewriteQueryFixture {
        selection: novarocks_spi::connector::ConnectorRowMutationSelection,
        recipe: novarocks_spi::connector::ConnectorRowMutationCohortRecipe,
        route: novarocks_spi::connector::ConnectorRowMutationRoute,
        preparation: novarocks_spi::connector::ConnectorRowMutationPreparation,
        identity: crate::sql::planner::table::SqlTableIdentity,
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
            ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
            ConnectorMutationEffectField, ConnectorMutationMatchContract,
            ConnectorMutationRouteInput, ConnectorMutationSourceField,
            ConnectorMutationTargetField, ConnectorRowMutationCohortRecipe,
            ConnectorRowMutationEffect, ConnectorRowMutationIntent,
            ConnectorRowMutationPreparation, ConnectorRowMutationRoute,
            ConnectorRowMutationScanBinding, ConnectorRowMutationSelection,
            ConnectorRowMutationSelectionOrdinal, ConnectorRowMutationStrategy,
            ConnectorTableHandle, ConnectorWriteBaseVersion, ConnectorWriteCohortId,
            ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
            ConnectorWriteIntent, ConnectorWriteOperationId, ConnectorWritePreparation,
            ConnectorWriteRouteId, ConnectorWriteTargetRef,
        };

        let row_count = row_ids.len();
        assert_eq!(after_ids.len(), row_count);
        assert_eq!(after_values.len(), row_count);
        let instance_id = ConnectorInstanceId::parse("iceberg").expect("instance ID");
        let owner = ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
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
        let route_input = ConnectorWriteInputShape::RowLineage {
            data_fields: vec![
                ConnectorWriteFieldBinding::new(
                    id_token,
                    arrow::datatypes::Field::new("id", DataType::Int64, true),
                ),
                ConnectorWriteFieldBinding::new(
                    value_token,
                    arrow::datatypes::Field::new("v", value_type.clone(), true),
                ),
            ],
            row_identity_fields: vec![
                ConnectorWriteFieldBinding::new(
                    row_id_token,
                    arrow::datatypes::Field::new("row_identity", DataType::Int64, false),
                ),
                ConnectorWriteFieldBinding::new(
                    source_version_token,
                    arrow::datatypes::Field::new("version_identity", DataType::Int64, false),
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
            ConnectorTableHandle::try_new(instance_id, bytes::Bytes::from_static(b"frozen-source"))
                .expect("frozen source"),
            base.digest(),
            scan_schema.clone(),
            scan_bindings.clone(),
            match_tokens.clone(),
            Some(source_version_token),
            bytes::Bytes::from_static(b"recipe"),
        )
        .expect("rewrite recipe");

        CowRewriteQueryFixture {
            selection,
            recipe,
            route,
            preparation,
            identity: crate::sql::planner::table::SqlTableIdentity {
                catalog: "default_catalog".to_string(),
                namespace: "__nr_cow".to_string(),
                table: "__nr_cow_t_abc".to_string(),
            },
            scan_schema,
            scan_bindings,
            match_tokens,
            written_version_token: source_version_token,
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
            &fixture.recipe,
            &fixture.route,
            &fixture.preparation,
            &fixture.identity,
            &fixture.scan_schema,
            &fixture.scan_bindings,
            &fixture.match_tokens,
            Some(fixture.written_version_token),
        )
        .expect("query");
        let sql = query.to_string();

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
        assert!(sql.contains("AS `row_identity`"), "{sql}");
        assert!(sql.contains("AS `version_identity`"), "{sql}");
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
            &fixture.recipe,
            &fixture.route,
            &fixture.preparation,
            &fixture.identity,
            &fixture.scan_schema,
            &fixture.scan_bindings,
            &fixture.match_tokens,
            Some(fixture.written_version_token),
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
