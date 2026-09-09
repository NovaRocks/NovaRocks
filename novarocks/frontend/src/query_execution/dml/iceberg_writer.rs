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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::datatypes::Field;

use crate::catalog_application::resolver::TargetBackend;
use crate::common::admitted_query_context::QueryExecutionContext;
use crate::connector::backend::ResolvedTable;
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::planning::write_sink::{
    admit_prepared_frozen_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use crate::query_execution::write_transaction::{
    IcebergWriteCommitPolicy, IcebergWriteSource, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy,
};
use novarocks_parser::ast::{Query, Statement};
use novarocks_spi::connector::{
    ConnectorPreReadyWritePlanningRequest, ConnectorTableHandle, ConnectorWriteActivationIntent,
    ConnectorWriteActivationRequest, ConnectorWriteActivationSource,
    ConnectorWriteAdmissionPurpose, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
    ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationId,
    ConnectorWritePreparation, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
};
#[cfg(test)]
use novarocks_sql::literal::bytes_to_latin1_string;
use novarocks_sql::literal::{column_default_to_ast_literal, latin1_string_to_bytes};
use novarocks_sql::planning::dml::DmlWriteSinkMode;
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;
use novarocks_sql::semantic::Literal;
use novarocks_types::schema::{ColumnDef, ColumnDefault, SqlType};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum IcebergWriteInput {
    Rows(Vec<Vec<Literal>>),
    Query(Box<Query>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IcebergWriteMode {
    Append,
    FullTableOverwrite,
    DynamicPartitionOverwrite,
}

impl IcebergWriteMode {
    const fn is_overwrite(self) -> bool {
        !matches!(self, Self::Append)
    }
}

/// Provider-owned identity and snapshot facts for a prepared Iceberg write.
///
/// The application layer may preallocate the opaque operation identity, but it
/// never interprets or constructs Iceberg commit state. In particular, MV
/// refresh uses this to make the staged writer, snapshot marker, and durable
/// frontend ledger refer to one attempt before any external action starts.
#[derive(Clone, Debug)]
pub(crate) struct IcebergWritePreparationOptions {
    pub(crate) operation_id: ConnectorWriteOperationId,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

impl IcebergWritePreparationOptions {
    pub(crate) fn new(operation_id: ConnectorWriteOperationId) -> Self {
        Self {
            operation_id,
            snapshot_properties: BTreeMap::new(),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
    pub(crate) fn with_snapshot_properties(
        mut self,
        snapshot_properties: BTreeMap<String, String>,
    ) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

/// Prepare an Iceberg write with an application-preallocated operation
/// identity. This still performs no writer execution or catalog mutation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_iceberg_write_with_options(
    state: &DmlExecutionKernel,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &IcebergWriteInput,
    overwrite_mode: IcebergWriteMode,
    target_ref: &str,
    execution: Option<QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    options: IcebergWritePreparationOptions,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    attempt_reservation: crate::query_execution::completion::QueryAttemptReservation,
) -> Result<PreparedIcebergWrite, String> {
    debug_assert_eq!(target.provider_id.as_str(), "iceberg");

    // 1. Resolve the write target through the exact planning generation.
    //
    // The caller already holds the planning lease, so the metadata is loaded
    // through that same lease rather than re-resolving `latest`: a concurrent
    // commit must not be able to split one statement across two generations.
    // What comes back is neutral -- Arrow schema, bounded planning facts and an
    // opaque handle -- so this layer no longer holds a concrete Iceberg table.
    let write_target = crate::connector::write_target::ConnectorWriteTargetBinding::new(
        crate::connector::metadata_load_connector_table_with_planning_lease(
            &planning_lease,
            connector_context.clone(),
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?,
        planning_lease,
    );

    // 2. Write-support validation belongs to the Provider.
    //
    // These guards reject table shapes this writer cannot encode: unresolvable
    // default sort order, variant in partition spec or sort order, evolved
    // partition specs under INSERT OVERWRITE, pre-existing equality deletes
    // under INSERT OVERWRITE, unpartitioned targets under OVERWRITE PARTITIONS,
    // and pre-v3 tables under a branch write. Every one of them is an Iceberg
    // fact read off table metadata, so they now run inside
    // `ConnectorWriteControl::prepare_write` against the frozen admitted
    // metadata, and this layer no longer loads a table to answer them.
    //
    // Rejection set is unchanged. Two observable differences, both recorded in
    // the plan: the message now carries the `Iceberg write admission denied:`
    // prefix the SPI `Denied` outcome adds, and the guards fire after column
    // shaping rather than before it, so a statement that violates both a guard
    // and its column list now surfaces the column-list error first.

    prepare_iceberg_distributed_write(
        state,
        target,
        resolved,
        insert_columns,
        source,
        overwrite_mode,
        target_ref,
        &write_target,
        execution,
        connector_context,
        options,
        attempt_reservation,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_iceberg_distributed_write(
    state: &DmlExecutionKernel,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &IcebergWriteInput,
    overwrite_mode: IcebergWriteMode,
    target_ref: &str,
    write_target: &crate::connector::write_target::ConnectorWriteTargetBinding,
    execution: Option<QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    options: IcebergWritePreparationOptions,
    attempt_reservation: crate::query_execution::completion::QueryAttemptReservation,
) -> Result<PreparedIcebergWrite, String> {
    let write_lease = write_target.derive_write_lease()?;
    let (query, write_columns) = build_iceberg_write_plan(
        target,
        resolved,
        insert_columns,
        source,
        write_target.metadata(),
    )?;
    let intent = match overwrite_mode {
        IcebergWriteMode::Append => ConnectorWriteIntent::Append,
        IcebergWriteMode::FullTableOverwrite => ConnectorWriteIntent::Overwrite,
        IcebergWriteMode::DynamicPartitionOverwrite => ConnectorWriteIntent::PartitionOverwrite,
    };
    let input = ConnectorWriteInputRequest::Data {
        fields: write_columns
            .iter()
            .map(|column| {
                ConnectorWriteFieldRequest::new(Field::new(
                    &column.name,
                    column.data_type.clone(),
                    column.nullable,
                ))
            })
            .collect(),
    };
    let preparation = prepare_iceberg_connector_write(
        &write_lease,
        target,
        target_ref,
        intent,
        input.clone(),
        ConnectorWriteAdmissionPurpose::OrdinaryDml,
        connector_context.clone(),
    )?;
    // One logical data branch, admitted on the same generation that resolved
    // the target. The session is opened before the plan is compiled because it
    // owns the recipes that plan's writer node carries.
    let write_session = crate::query_execution::write_session::begin_connector_write_session(
        write_target.derive_write_stack_lease(state.typed_connector_control())?,
        &write_lease,
        connector_write_begin_request(
            target,
            target_ref,
            intent,
            input,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::Ordinary,
            connector_context.clone(),
        )?,
    )?;
    let table_bindings =
        Arc::new(crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()?);
    let target_binding = admit_prepared_frozen_connector_write_target(
        table_bindings.as_ref(),
        FrozenConnectorScanIdentity::new(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        ),
        preparation.clone(),
        write_target.lease().clone(),
    )?;
    let sql_write_input = dml_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        DmlWriteSinkMode::Data,
        novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
    )?;

    let connector_operation_id = options.operation_id;
    // Preserve the journal's historical RefHead observation. This is not the
    // opaque base sealed into `preparation`; aligning those two values is the
    // separately recorded F7 lifecycle change.
    let base_snapshot_id =
        write_target.journal_ref_head_snapshot_id(target_ref, connector_context.clone())?;
    let semantic_binding = FrozenIcebergWriteSemanticBinding {
        state: state.clone(),
        target: target.clone(),
        query: query.clone(),
        sql_write_input,
        table_bindings,
        execution,
        connector_context: connector_context.clone(),
        operation_id: connector_operation_id,
        write_lease: write_lease.clone(),
        write_session,
        pre_ready_planning_request: ConnectorPreReadyWritePlanningRequest::new(
            ConnectorWriteActivationRequest {
                operation_id: connector_operation_id,
                source: ConnectorWriteActivationSource::Prepared(preparation),
                intent: ConnectorWriteActivationIntent::Ordinary,
                context: connector_context.clone(),
            },
        ),
    };
    let spec = IcebergWriteTransactionSpec {
        is_overwrite: overwrite_mode.is_overwrite(),
        attempt_id: connector_operation_id.to_string(),
        commit: IcebergWriteCommitPolicy {
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: options.snapshot_properties,
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    Ok(PreparedIcebergWrite {
        semantic_binding: Arc::new(semantic_binding),
        spec,
        native_assembly: Mutex::new(None),
        attempt_reservation: Mutex::new(Some(attempt_reservation)),
    })
}

/// Build the begin request for one distributed write.
///
/// The table is named the way the provider parses it -- namespace-qualified,
/// last dot separating the table -- so a multi-level namespace round-trips
/// instead of losing its trailing level.
#[allow(clippy::too_many_arguments)]
pub(crate) fn connector_write_begin_request(
    target: &TargetBackend,
    target_ref: &str,
    intent: ConnectorWriteIntent,
    input: ConnectorWriteInputRequest,
    purpose: ConnectorWriteAdmissionPurpose,
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
            base: None,
            flavor,
            context,
        },
    )
}

/// Request a sealed preparation from the write-control generation retained by
/// the original planning lease.  This helper is the only generic-template
/// construction seam: callers provide Arrow fields, never a table-format
/// field ID, writer payload, or a freshly acquired connector generation.
pub(crate) fn prepare_iceberg_connector_write(
    exact_lease: &ConnectorWriteLease,
    target: &TargetBackend,
    target_ref: &str,
    intent: ConnectorWriteIntent,
    input: ConnectorWriteInputRequest,
    purpose: ConnectorWriteAdmissionPurpose,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorWritePreparation, String> {
    let table = crate::catalog_application::resolver::iceberg_connector_table_handle(
        exact_lease,
        target,
        context.clone(),
    )?;
    prepare_iceberg_connector_write_with_table(
        exact_lease,
        table,
        target_ref,
        intent,
        input,
        purpose,
        context,
    )
}

/// Request a sealed preparation for a table handle frozen by an earlier exact
/// metadata observation. The caller must keep the matching write lease; this
/// avoids reloading a newer table metadata value within the same connector
/// generation after admission facts have already been derived.
pub(crate) fn prepare_iceberg_connector_write_with_table(
    exact_lease: &ConnectorWriteLease,
    table: ConnectorTableHandle,
    target_ref: &str,
    intent: ConnectorWriteIntent,
    input: ConnectorWriteInputRequest,
    purpose: ConnectorWriteAdmissionPurpose,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorWritePreparation, String> {
    if !exact_lease.matches_provider_instance(table.owner()) {
        return Err(
            "frozen Iceberg write target belongs to a different connector instance".to_string(),
        );
    }
    let outcome = exact_lease
        .prepare_write(ConnectorWritePreparationRequest {
            table,
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse(target_ref)
                .map_err(|error| format!("validate Iceberg write target ref: {error}"))?,
            intent,
            purpose,
            input,
            context,
        })
        .map_err(|error| format!("prepare Iceberg connector write: {error}"))?;
    match outcome {
        ConnectorWritePreparationOutcome::Prepared(preparation) => Ok(preparation),
        ConnectorWritePreparationOutcome::Denied(error) => {
            Err(format!("Iceberg write admission denied: {error}"))
        }
    }
}

// Resolve an opaque Iceberg write target through the connector metadata
// capability owned by the exact generation observed at write admission.
// Core only forwards the target identity; it never builds a handle payload.

pub(crate) struct PreparedIcebergWrite {
    semantic_binding: Arc<FrozenIcebergWriteSemanticBinding>,
    spec: IcebergWriteTransactionSpec,
    native_assembly: Mutex<Option<crate::query_execution::compiler::PreparedDmlWriteAssembly>>,
    /// This exact reservation collected every vended response observed while
    /// preparing the write. It is consumed only when the matching native
    /// bundle is submitted to the raw attempt lifecycle.
    attempt_reservation: Mutex<Option<crate::query_execution::completion::QueryAttemptReservation>>,
}

/// Borrowed encoder input for an exact prepared INSERT. The mutex guard stays
/// held until Frontend finishes encoding, preventing a competing execution
/// from consuming or replacing the sealed plan/preparation pair.
pub struct PreparedIcebergWriteNativeEncoding<'a> {
    inner: PreparedIcebergWriteNativeEncodingInner<'a>,
}

enum PreparedIcebergWriteNativeEncodingInner<'a> {
    Assembly(
        std::sync::MutexGuard<
            'a,
            Option<crate::query_execution::compiler::PreparedDmlWriteAssembly>,
        >,
    ),
    TestFixture(&'static crate::query_execution::compiler::NativeFragmentEncodingInput),
}

impl PreparedIcebergWriteNativeEncoding<'_> {
    pub fn input(
        &self,
    ) -> Result<&crate::query_execution::compiler::NativeFragmentEncodingInput, String> {
        match &self.inner {
            PreparedIcebergWriteNativeEncodingInner::Assembly(assembly) => assembly
                .as_ref()
                .map(crate::query_execution::compiler::PreparedDmlWriteAssembly::encoding)
                .ok_or_else(|| {
                    "prepared Iceberg write native assembly was already consumed".to_string()
                }),
            PreparedIcebergWriteNativeEncodingInner::TestFixture(input) => Ok(input),
        }
    }

    /// Test-only fixture used by frontend DML doubles. It creates a minimal
    /// sealed writer plan and matching prepared fragments, so tests exercise
    /// the real native encoder without a Core-side encoding fallback.
    #[doc(hidden)]
    pub fn test_fixture() -> Result<PreparedIcebergWriteNativeEncoding<'static>, String> {
        use std::sync::OnceLock;

        static INPUT: OnceLock<crate::query_execution::compiler::NativeFragmentEncodingInput> =
            OnceLock::new();
        let input = INPUT.get_or_init(|| {
            let plan = novarocks_sql::planning::dml::native_encoder_test_fixture_plan()
                .expect("test native INSERT fixture plan must seal");
            let prepared =
                crate::query_execution::preparation::prepared_fragment_set_for_native_encode_test(
                    &plan,
                )
                .expect("test native INSERT fixture must prepare");
            crate::query_execution::compiler::NativeFragmentEncodingInput::new(plan, prepared)
        });
        Ok(PreparedIcebergWriteNativeEncoding {
            inner: PreparedIcebergWriteNativeEncodingInner::TestFixture(input),
        })
    }
}

impl PreparedIcebergWrite {
    pub(crate) fn target(&self) -> &TargetBackend {
        &self.semantic_binding.target
    }

    pub(crate) fn is_overwrite(&self) -> bool {
        self.spec.is_overwrite
    }

    pub(crate) fn base_snapshot_id(&self) -> Option<i64> {
        self.spec.commit.base_snapshot_id
    }

    fn prepare_native_assembly_for_execution(
        &self,
        execution: Option<&QueryExecutionContext>,
    ) -> Result<
        crate::query_execution::compiler::PreparedDmlWriteAssembly,
        crate::dml::error::DmlExecutionError,
    > {
        crate::query_execution::compiler::prepare_query_as_iceberg_write_with_write_session(
            &self.semantic_binding.state,
            Some(&self.semantic_binding.target.catalog),
            &self.semantic_binding.target.namespace,
            &self.semantic_binding.query,
            self.semantic_binding.sql_write_input.clone(),
            Arc::clone(&self.semantic_binding.table_bindings),
            None,
            novarocks_sql::compiler::RootDistributionRequirement::Any,
            execution,
            &self.semantic_binding.connector_context,
            Arc::clone(&self.semantic_binding.write_session),
        )
    }

    pub(crate) fn native_encoding(
        &self,
    ) -> Result<PreparedIcebergWriteNativeEncoding<'_>, crate::dml::error::DmlExecutionError> {
        let mut assembly = self
            .native_assembly
            .lock()
            .expect("prepared Iceberg write native assembly lock poisoned");
        if assembly.is_none() {
            *assembly =
                Some(self.prepare_native_assembly_for_execution(
                    self.semantic_binding.execution.as_ref(),
                )?);
            // The first assembly completed all semantic materialization. Any
            // later topology round may only reuse these exact captured facts.
            self.semantic_binding
                .table_bindings
                .seal_for_topology_replan();
        }
        Ok(PreparedIcebergWriteNativeEncoding {
            inner: PreparedIcebergWriteNativeEncodingInner::Assembly(assembly),
        })
    }

    pub(crate) fn run_coordinated_write_with_native_bundle(
        &self,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<QueryExecutionResult, String> {
        let assembly = self
            .native_assembly
            .lock()
            .expect("prepared Iceberg write native assembly lock poisoned")
            .take()
            .ok_or_else(|| {
                "prepared Iceberg write native assembly was already consumed".to_string()
            })?;
        let (query_execution, request) = assembly.into_request(native_bundle)?;
        let attempt_reservation = self
            .attempt_reservation
            .lock()
            .expect("prepared Iceberg write attempt reservation lock poisoned")
            .take()
            .ok_or_else(|| {
                "prepared Iceberg write attempt reservation was already consumed".to_string()
            })?;
        let publication_id = novarocks_spi::connector::LakePublicationId::try_from_bytes(
            self.semantic_binding.operation_id.to_bytes(),
        )
        .map_err(|error| {
            format!("Iceberg write operation lacks a UUIDv7 publication identity: {error}")
        })?;
        let outcome = query_execution
            .execute_prepared_raw(
                crate::query_execution::completion::PreparedRetriableDistributedRequest::new(
                    request,
                    Box::new(IcebergWriteRoundFactory {
                        binding: Arc::clone(&self.semantic_binding),
                        effect_tracker:
                            crate::common::statement_effect::StatementEffectTracker::mutating(
                                publication_id,
                            ),
                    }),
                )
                .with_attempt_reservation(attempt_reservation),
            )
            .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_write)
            .map_err(|error| error.to_string())?;
        Ok(outcome.into_execution_result())
    }

    /// Convert a validated Iceberg write into SQL's inert distributed-write
    /// handoff. This registers no writer attempt and executes no query; the
    /// connector control service has already retained the provider-private
    /// committer under the operation identity carried by the resulting plan.
    ///
    /// Frontend application owners use this form when they must persist their
    /// intent and retain an exact connector lease before submitting native
    /// fragments.
    pub(crate) fn terminal_request_context(
        &self,
    ) -> novarocks_spi::connector::ConnectorRequestContext {
        self.semantic_binding.connector_context.clone()
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
/// First-admission write semantics retained across topology-only rounds. It
/// intentionally contains no native fragments, splits, writer cohorts,
/// runtime-filter layout, schedule, native bundle, or request.
struct FrozenIcebergWriteSemanticBinding {
    state: DmlExecutionKernel,
    target: TargetBackend,
    query: Query,
    sql_write_input: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    execution: Option<QueryExecutionContext>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    /// This statement's durable publication identity. It stays in this
    /// frontend layer: it never reaches a writer recipe or a commit fragment.
    operation_id: ConnectorWriteOperationId,
    /// The exact generation's write lease, retained only so a topology replan
    /// can ask it for an effect-free planning proof.
    write_lease: ConnectorWriteLease,
    /// The one commit authority for this statement. Every round of this
    /// statement encodes the same sealed recipes from it.
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    pre_ready_planning_request: ConnectorPreReadyWritePlanningRequest,
}

impl FrozenIcebergWriteSemanticBinding {
    fn prepare_replanned_native_assembly(
        &self,
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<
        crate::query_execution::compiler::PreparedDmlWriteAssembly,
        crate::dml::error::DmlExecutionError,
    > {
        if !self.table_bindings.is_sealed_for_topology_replan() {
            return Err(crate::dml::error::DmlExecutionError::from(
                "Iceberg write topology replan requires a sealed first-round semantic binding store".to_string(),
            ));
        }
        let execution = self.execution_for_topology(topology)?;
        let proof = self
            .write_lease
            .certify_pre_ready_write_planning(self.pre_ready_planning_request.clone())
            .map_err(|error| {
                crate::dml::error::DmlExecutionError::from(format!(
                    "Iceberg write topology replan has no effect-free planning proof: {error}"
                ))
            })?;
        self.write_lease
            .validate_pre_ready_write_planning_proof(&proof, &self.pre_ready_planning_request)
            .map_err(|error| {
                crate::dml::error::DmlExecutionError::from(format!(
                    "Iceberg write topology replan lost its effect-free planning proof: {error}"
                ))
            })?;
        crate::query_execution::compiler::prepare_query_as_iceberg_write_with_write_session(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.query,
            self.sql_write_input.clone(),
            Arc::clone(&self.table_bindings),
            None,
            novarocks_sql::compiler::RootDistributionRequirement::Any,
            Some(&execution),
            &self.connector_context,
            Arc::clone(&self.write_session),
        )
    }

    fn execution_for_topology(
        &self,
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<QueryExecutionContext, crate::dml::error::DmlExecutionError> {
        let first = self.execution.as_ref().ok_or_else(|| {
            crate::dml::error::DmlExecutionError::from(
                "Iceberg write topology replan requires an admitted execution context".to_string(),
            )
        })?;
        Ok(Self::execution_from_first_round(first, topology))
    }

    fn execution_from_first_round(
        first: &QueryExecutionContext,
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> QueryExecutionContext {
        QueryExecutionContext::new(
            first.role(),
            topology,
            first.deadline(),
            first.cancellation().clone(),
            first.optimizer_settings().clone(),
        )
    }
}

struct IcebergWriteRoundFactory {
    binding: Arc<FrozenIcebergWriteSemanticBinding>,
    effect_tracker: crate::common::statement_effect::StatementEffectTracker,
}

impl crate::query_execution::completion::PreReadyRetryBoundary for IcebergWriteRoundFactory {
    fn permit_pre_ready_retry(
        &self,
    ) -> Result<(), crate::query_execution::contract::DistributedQueryError> {
        self.effect_tracker
            .issue_topology_retry_permit()
            .map(|_| ())
            .map_err(|error| {
                crate::query_execution::contract::DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::TopologyRetryUnsupported,
                    format!("Iceberg write topology retry is not effect-free: {error:?}"),
                )
            })
    }

    fn close_after_control_ready(&self) {
        self.effect_tracker.close_after_control_ready();
    }

    fn close_after_stage_or_start(&self) {
        self.effect_tracker.close_after_stage_or_start();
    }
}

impl crate::query_execution::completion::PreparedDistributedRequestFactory
    for IcebergWriteRoundFactory
{
    fn replan(
        &mut self,
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<
        crate::query_execution::contract::DistributedQueryRequest,
        crate::query_execution::contract::DistributedQueryError,
    > {
        let assembly = self.binding.prepare_replanned_native_assembly(topology).map_err(|error| {
            crate::query_execution::contract::DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::TopologyRetryUnsupported,
                error.to_string(),
            )
        })?;
        let native_bundle =
            crate::native::fragment_encoder::encode_native_fragment_bundle_for_input(
                assembly.encoding(),
            )
            .map_err(|error| {
                crate::query_execution::contract::DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::Failed,
                    error,
                )
            })?;
        let (_query_execution, request) =
            assembly.into_request(native_bundle).map_err(|error| {
                crate::query_execution::contract::DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::Failed,
                    error,
                )
            })?;
        Ok(request)
    }
}

/// Build the `(query, Arrow write layout)` pair for an iceberg INSERT/OVERWRITE write
/// without driving a transaction. The frontend INSERT adapter consumes this
/// plan through its DML-owned runner; the folded MERGE not-matched INSERT
/// branch runs the same pair into a shared collector so the INSERT commits in
/// the same snapshot as the matched branch. Both callers share one query/sink
/// construction to avoid semantic drift.
pub(crate) fn build_iceberg_write_plan(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &IcebergWriteInput,
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
) -> Result<(Query, Vec<ColumnDef>), String> {
    let write_columns = insert_columns_from_connector_metadata(
        metadata,
        &write_defaults_by_name(&resolved.columns),
    );
    let source_columns = sql_write_source_columns(&resolved.columns, &write_columns);
    let query =
        append_source_to_query_for_write(source, insert_columns, &source_columns, &write_columns)?;
    let _ = target;
    Ok((query, write_columns))
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
    source: &IcebergWriteInput,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Query, String> {
    append_source_to_query_for_write(source, insert_columns, target_columns, target_columns)
}

fn append_source_to_query_for_write(
    source: &IcebergWriteInput,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Query, String> {
    match source {
        IcebergWriteInput::Query(query)
            if insert_columns.is_empty() && same_column_sequence(source_columns, write_columns) =>
        {
            Ok((**query).clone())
        }
        IcebergWriteInput::Query(query) => wrap_insert_query_with_write_projection(
            query,
            insert_columns,
            source_columns,
            write_columns,
        ),
        IcebergWriteInput::Rows(rows) => values_append_source_to_query_for_write(
            rows,
            insert_columns,
            source_columns,
            write_columns,
        ),
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

fn values_append_source_to_query_for_write(
    rows: &[Vec<Literal>],
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Query, String> {
    let insert_idx_by_target = if insert_columns.is_empty() {
        std::collections::HashMap::new()
    } else {
        insert_column_index_by_target_name(insert_columns, write_columns)?
    };
    let rendered_rows = rows
        .iter()
        .map(|row| {
            if insert_columns.is_empty() {
                if row.len() != source_columns.len() {
                    return Err(format!(
                        "insert column count mismatch: expected {} values, got {}",
                        source_columns.len(),
                        row.len()
                    ));
                }
            } else if row.len() != insert_columns.len() {
                return Err(format!(
                    "insert column count mismatch: expected {} values for column list, got {}",
                    insert_columns.len(),
                    row.len()
                ));
            }
            let values = write_columns
                .iter()
                .enumerate()
                .map(|(write_idx, column)| {
                    if insert_columns.is_empty() {
                        if let Some(literal) = source_index_for_write_column(
                            column,
                            write_idx,
                            source_columns,
                            write_columns,
                        )
                        .and_then(|source_idx| row.get(source_idx))
                        {
                            target_literal_expr_sql(literal, column)
                        } else {
                            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)
                        }
                    } else {
                        let target_name =
                            novarocks_types::naming::normalize_identifier(&column.name)?;
                        if let Some(literal) = insert_idx_by_target
                            .get(&target_name)
                            .and_then(|source_idx| row.get(*source_idx))
                        {
                            target_literal_expr_sql(literal, column)
                        } else {
                            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Ok(format!("({values})"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let sql = format!("VALUES {}", rendered_rows.join(", "));
    parse_generated_query(&sql, "append INSERT VALUES")
}

fn same_column_sequence(left: &[ColumnDef], right: &[ColumnDef]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(l, r)| l.name.eq_ignore_ascii_case(&r.name) && l.data_type == r.data_type)
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
) -> Vec<ColumnDef> {
    let column_facts = metadata.planning_facts.column_facts();
    metadata
        .schema
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
        .map(|(ordinal, field)| ColumnDef {
            name: field.name().clone(),
            data_type: column_facts
                .get(ordinal)
                .and_then(|fact| fact.write_target_type())
                .cloned()
                .unwrap_or_else(|| field.data_type().clone()),
            nullable: field.is_nullable(),
            write_default: write_defaults.get(field.name()).cloned(),
            logical_type: None,
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
    let Some(write_default) = &column.write_default else {
        return Ok("NULL".to_string());
    };
    let sql_type = arrow_data_type_to_sql_type(&column.data_type)?;
    let literal = column_default_to_ast_literal(write_default, &sql_type)?;
    literal_to_sql_for_arrow_type(&literal, &column.data_type)
}

fn target_literal_expr_sql(literal: &Literal, column: &ColumnDef) -> Result<String, String> {
    target_cast_expr_sql(
        &literal_to_sql_for_arrow_type(literal, &column.data_type)?,
        column,
    )
}

pub(crate) fn target_cast_expr_sql(expr_sql: &str, column: &ColumnDef) -> Result<String, String> {
    Ok(format!(
        "CAST({expr_sql} AS {})",
        arrow_data_type_to_sql_type_name(&column.data_type)?
    ))
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

fn arrow_data_type_to_sql_type_name(dt: &arrow::datatypes::DataType) -> Result<String, String> {
    sql_type_name(&arrow_data_type_to_sql_type(dt)?)
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
    use std::time::{Duration, Instant};

    use crate::common::backend_topology::BackendTopologySnapshot;
    use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};
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

    fn parse_query(sql: &str) -> Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected exactly one query statement");
        };
        query.clone()
    }

    #[test]
    fn replanned_execution_preserves_statement_stable_inputs() {
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let settings = novarocks_sql::compiler::SessionOptimizerSettings {
            enable_global_runtime_filter: Some(false),
            effective_backend_count: Some(3.0),
            ..Default::default()
        };
        let first = QueryExecutionContext::new(
            novarocks_types::ClusterRole::Fe,
            BackendTopologySnapshot::empty(7),
            Some(deadline),
            cancellation.view(),
            settings.clone(),
        );

        let replanned = FrozenIcebergWriteSemanticBinding::execution_from_first_round(
            &first,
            BackendTopologySnapshot::empty(8),
        );

        assert_eq!(replanned.role(), first.role());
        assert_eq!(replanned.topology().revision(), 8);
        assert_eq!(replanned.deadline(), Some(deadline));
        assert_eq!(replanned.optimizer_settings(), &settings);
        assert!(!replanned.cancellation().is_cancelled());
        cancellation.request(QueryCancellationReason::DeadlineExceeded { timeout_ms: 30_000 });
        assert!(replanned.cancellation().is_cancelled());
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

    fn test_struct_type(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(name, data_type)| Arc::new(Field::new(name, data_type, true)))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn write_mode_preserves_append_and_overwrite_classification() {
        assert!(!IcebergWriteMode::Append.is_overwrite());
        assert!(IcebergWriteMode::FullTableOverwrite.is_overwrite());
        assert!(IcebergWriteMode::DynamicPartitionOverwrite.is_overwrite());
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
        let source = IcebergWriteInput::Rows(vec![vec![Literal::Int(30), Literal::Int(10)]]);

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
                "'123.45'".to_string(),
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
                "map()".to_string(),
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
    fn omitted_column_expr_characterizes_non_empty_collection_default_errors() {
        let list_column = test_column(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            Some(ColumnDefault::Array(vec![ColumnDefault::Int32(1)])),
        );
        assert_eq!(
            omitted_column_expr_sql(&list_column).unwrap_err(),
            "non-empty ARRAY write-default is not yet supported (1 elements)"
        );

        let map_column = test_column(
            "attributes",
            test_map_type(DataType::Int32, DataType::Utf8),
            Some(ColumnDefault::Map(vec![(
                ColumnDefault::Int32(1),
                ColumnDefault::String("value".to_string()),
            )])),
        );
        assert_eq!(
            omitted_column_expr_sql(&map_column).unwrap_err(),
            "non-empty MAP write-default is not yet supported (1 entries)"
        );
    }

    #[test]
    fn append_source_to_query_values_casts_literals_to_target_types() {
        let target_columns = vec![
            test_column("id", DataType::Int64, None),
            test_column("region", DataType::Utf8, None),
            test_column("amount", DataType::Float64, None),
        ];
        let source = IcebergWriteInput::Rows(vec![
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
        let source = IcebergWriteInput::Rows(vec![vec![Literal::Int(1), Literal::Int(10)]]);

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
        let source = IcebergWriteInput::Rows(vec![vec![Literal::String(r"e\f".to_string())]]);

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
        let source = IcebergWriteInput::Rows(vec![vec![Literal::String(packed)]]);

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
        let column = test_column("v", DataType::LargeBinary, None);

        let sql = target_cast_expr_sql("X'AB01'", &column).expect("cast sql");

        assert_eq!(sql, "CAST(X'AB01' AS VARIANT)");
    }

    #[test]
    fn append_source_to_query_values_rejects_column_list_width_mismatch() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, None),
        ];
        let source = IcebergWriteInput::Rows(vec![vec![Literal::Int(1), Literal::Int(2)]]);

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
        let source = IcebergWriteInput::Query(Box::new(parse_query("SELECT x, y FROM src")));

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
        let source = IcebergWriteInput::Query(Box::new(parse_query(
            "SELECT idx FROM row_util ORDER BY idx LIMIT 1000",
        )));

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
