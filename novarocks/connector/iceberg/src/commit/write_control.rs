// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Generation-local Iceberg writer control.
//!
//! This is the provider-owned operation table used by one exact control
//! generation.  It deliberately has no Core registry, current-generation
//! lookup, or process-global runtime.  The first closure slice owns exact
//! activation, deterministic data-writer planning, and known-empty abort.  A
//! later provider commit slice installs the external commit action behind the
//! same operation entries.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use base64::Engine;
use bytes::Bytes;
use parquet::basic::Compression;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceIncarnation, ConnectorManagedPublicationEmptyInputDisposition,
    ConnectorManagedPublicationTechnique, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorRowMutationActivationRequest,
    ConnectorRowMutationExecutionPlan, ConnectorRowMutationPreparationOutcome,
    ConnectorRowMutationPreparationRequest, ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest,
    ConnectorWriteActivation, ConnectorWriteActivationIntent, ConnectorWriteActivationRequest,
    ConnectorWriteActivationSource, ConnectorWriteCohortId, ConnectorWriteCommitRequest,
    ConnectorWriteControl, ConnectorWriteExecutionId, ConnectorWriteInputShape,
    ConnectorWriteOperationCompletion, ConnectorWriteOperationId, ConnectorWritePlan,
    ConnectorWritePlanningRequest, ConnectorWritePreparationOutcome,
    ConnectorWritePreparationRequest, ConnectorWriteReceipt, ConnectorWriteReconcileRequest,
    ConnectorWriterHandle, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::control_provider::IcebergControlProvider;
use crate::control_runtime::IcebergControlRuntime;
use crate::delete_file::IcebergFileFormat;
use crate::row_lineage_synth::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use crate::scan_model::IcebergSchemaFieldDef;
use crate::write_activation::IcebergWriteActivationReservations;
use crate::write_codec::{
    ICEBERG_WRITE_PAYLOAD_VERSION, IcebergPositionDeletePartitionInput, IcebergWriteHandleInput,
    IcebergWriteHandleMode, encode_write_handle,
};
use crate::write_payload::{IcebergFirstRefreshWritePlanPayloadV2, IcebergWritePlanPayloadV1};

use super::{
    CommitOpKind, CommitOutcome, CommitServiceError, CowUpdateRewriteSet, CowUpdateTouchedFile,
    DeletionVector, EqualityDeleteColumn, IcebergCommitCollector, RecoveryEvidence, RunInput,
    WrittenFile, run_iceberg_commit,
};

const ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION: u16 = 2;
const ICEBERG_WRITE_OPERATION_KIND: &str = "iceberg.connector_write.v2";
const ICEBERG_WRITE_OPERATION_MARKER_VERSION: u8 = 1;
const ICEBERG_WRITE_OPERATION_MARKER_PROPERTY: &str = "novarocks.write.operation.v1";
const MAX_ICEBERG_WRITE_TERMINAL_TOMBSTONES: usize = 16_384;

/// Concrete write capability assembled with every other capability of one
/// Iceberg control generation.
#[derive(Clone)]
pub struct IcebergWriteControl {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    provider: IcebergControlProvider,
    runtime: Arc<IcebergControlRuntime>,
    activations: Arc<IcebergWriteActivationReservations>,
    operations: Arc<Mutex<OperationTable>>,
}

#[derive(Default)]
struct OperationTable {
    entries: HashMap<ConnectorWriteOperationId, OperationState>,
    terminal_order: VecDeque<ConnectorWriteOperationId>,
}

impl OperationTable {
    fn get(&self, operation_id: &ConnectorWriteOperationId) -> Option<&OperationState> {
        self.entries.get(operation_id)
    }

    fn get_mut(&mut self, operation_id: &ConnectorWriteOperationId) -> Option<&mut OperationState> {
        self.entries.get_mut(operation_id)
    }

    fn insert(
        &mut self,
        operation_id: ConnectorWriteOperationId,
        state: OperationState,
    ) -> Option<OperationState> {
        self.terminal_order
            .retain(|candidate| candidate != &operation_id);
        if state.is_terminal_tombstone() {
            self.terminal_order.push_back(operation_id);
        }
        let previous = self.entries.insert(operation_id, state);
        while self.terminal_order.len() > MAX_ICEBERG_WRITE_TERMINAL_TOMBSTONES {
            let Some(expired) = self.terminal_order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&expired)
                .is_some_and(OperationState::is_terminal_tombstone)
            {
                self.entries.remove(&expired);
            }
        }
        previous
    }
}

#[derive(Clone)]
enum OperationState {
    Active(ActiveOperation),
    Committing(CommittingOperation),
    KnownUncommitted(KnownUncommittedOperation),
    KnownCommitted(KnownCommittedOperation),
    CommitUnknown(CommitUnknownOperation),
}

impl OperationState {
    fn is_terminal_tombstone(&self) -> bool {
        matches!(self, Self::KnownUncommitted(_) | Self::KnownCommitted(_))
    }
}

#[derive(Clone)]
struct CommittingOperation {
    cohort_set_digest: [u8; 32],
    aggregate_digest: [u8; 32],
}

#[derive(Clone)]
struct ActiveOperation {
    activation_digest: [u8; 32],
    activation_intent: ConnectorWriteActivationIntent,
    activation_source: ConnectorWriteActivationSource,
    target: ActiveTarget,
    cohorts: HashMap<ConnectorWriteCohortId, CohortService>,
    distributed_rewrite: Option<DistributedRewriteOperation>,
}

#[derive(Clone)]
struct DistributedRewriteOperation {
    kind: IcebergDistributedRewriteKind,
    data_paths: BTreeSet<String>,
    delete_paths: BTreeSet<String>,
}

/// Provider-private cohort facts frozen by distributed-rewrite planning.
/// These paths never enter SPI; they are retained only by the exact
/// generation's write operation table and revalidated at commit time.
#[derive(Clone)]
pub(crate) struct IcebergDistributedRewriteCohortActivation {
    pub cohort_id: ConnectorWriteCohortId,
    pub preparation: novarocks_spi::connector::ConnectorWritePreparation,
    pub control_payload: Bytes,
    pub data_paths: BTreeSet<String>,
    pub delete_paths: BTreeSet<String>,
}

#[derive(Clone)]
pub(crate) struct IcebergDistributedRewriteActivation {
    pub kind: IcebergDistributedRewriteKind,
    pub cohorts: Vec<IcebergDistributedRewriteCohortActivation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergDistributedRewriteKind {
    Data,
    PositionDeletes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveTarget {
    namespace: String,
    table: String,
    target_ref: String,
    table_uuid: String,
    base_snapshot_id: Option<i64>,
    schema_id: i32,
    partition_spec_id: i32,
    location: String,
}

#[derive(Clone)]
struct CohortService {
    preparation_digest: [u8; 32],
    writer_payload: Bytes,
    routed_writer_payloads: Option<Vec<Bytes>>,
    control_payload: Bytes,
    stable_digest: Option<[u8; 32]>,
    attempts: HashMap<ConnectorWriteExecutionId, CachedPlan>,
}

#[derive(Clone)]
struct CachedPlan {
    attempt_digest: [u8; 32],
    plan: ConnectorWritePlan,
}

struct DecodedCohort {
    cohort_id: ConnectorWriteCohortId,
    intent: novarocks_spi::connector::ConnectorWriteIntent,
    files: Vec<WrittenFile>,
}

#[derive(Clone)]
struct KnownUncommittedOperation {
    cohort_set_digest: [u8; 32],
    aggregate_digest: [u8; 32],
    outcome: ConnectorWriteAbortOutcome,
}

#[derive(Clone)]
struct KnownCommittedOperation {
    cohort_set_digest: [u8; 32],
    aggregate_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
}

#[derive(Clone)]
struct CommitUnknownOperation {
    cohort_set_digest: [u8; 32],
    aggregate_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    active: ActiveOperation,
    request: ConnectorWriteCommitRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWriteOperationMarkerV1 {
    version: u8,
    instance_id: String,
    incarnation_base64: String,
    operation_id_base64: String,
    target_ref: String,
    cohort_set_digest_base64: String,
    aggregate_digest_base64: String,
}

/// Canonical provider payload inside an SPI reconciliation evidence envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergWriteReconcileEvidenceV1 {
    version: u16,
    operation_id_base64: String,
    cohort_set_digest_base64: String,
    aggregate_digest_base64: String,
    recovery: IcebergRecoveryEvidenceV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergRecoveryEvidenceV1 {
    table_ident: String,
    op_kind: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    staging_dir: String,
}

#[derive(Serialize)]
struct IcebergCowCohortControlPayloadV1<'a> {
    version: u8,
    target: &'a str,
    target_ref: &'a str,
    role: &'a str,
    base_snapshot_id: i64,
    old_file: Option<&'a str>,
    matched_row_count: usize,
    matched_row_digest_base64: String,
}

impl IcebergWriteControl {
    /// Construct an exact-generation capability.  Its activation table is the
    /// one owned by `runtime`; callers cannot inject an independent registry.
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        runtime: Arc<IcebergControlRuntime>,
    ) -> Self {
        let key = ConnectorExecutionBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let activations = Arc::clone(runtime.write_activation_reservations());
        let provider =
            IcebergControlProvider::new(descriptor.clone(), incarnation, Arc::clone(&runtime));
        Self {
            key,
            descriptor,
            provider,
            runtime,
            activations,
            operations: Arc::new(Mutex::new(OperationTable::default())),
        }
    }

    fn ensure_owner(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg write request does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn build_active_operation(
        &self,
        request: &ConnectorWriteActivationRequest,
        activation: &ConnectorWriteActivation,
    ) -> Result<ActiveOperation, ConnectorError> {
        let cow_recipes = match &request.source {
            ConnectorWriteActivationSource::RowMutation(plan) => plan
                .copy_on_write()
                .map(|(_, recipes)| {
                    recipes
                        .iter()
                        .map(|recipe| {
                            crate::row_mutation_payload::decode_cow_recipe(recipe.payload())
                                .map(|decoded| (recipe.cohort_id(), decoded))
                                .map_err(invalid)
                        })
                        .collect::<Result<HashMap<_, _>, _>>()
                })
                .transpose()?
                .unwrap_or_default(),
            ConnectorWriteActivationSource::Prepared(_) => HashMap::new(),
        };
        let mut cohorts = HashMap::with_capacity(activation.cohorts().len());
        let mut operation_target = None;
        for activated in activation.cohorts() {
            let preparation = activated.preparation();
            let table = self.provider.table_payload(preparation.table())?;
            if table.metadata_table_type.is_some() {
                return Err(invalid("Iceberg metadata tables cannot be write targets"));
            }
            let table_info = table.table_info.ok_or_else(|| {
                corrupt("admitted Iceberg write table is missing its frozen table descriptor")
            })?;
            let serialized = table_info.serialized_metadata.as_deref().ok_or_else(|| {
                corrupt("admitted Iceberg write table is missing frozen metadata")
            })?;
            let metadata: crate::iceberg::spec::TableMetadata = serde_json::from_str(serialized)
                .map_err(|error| {
                    corrupt(format!(
                        "decode admitted Iceberg write table metadata: {error}"
                    ))
                })?;
            let target_snapshot_id = crate::ref_snapshot::resolve_branch_head_snapshot_id(
                &metadata,
                preparation.target_ref().as_str(),
            )
            .map_err(invalid)?;
            let target = ActiveTarget {
                namespace: table_info.namespace.clone(),
                table: table_info.table.clone(),
                target_ref: preparation.target_ref().as_str().to_string(),
                table_uuid: metadata.uuid().to_string(),
                base_snapshot_id: target_snapshot_id,
                schema_id: metadata.current_schema_id(),
                partition_spec_id: metadata.default_partition_spec_id(),
                location: metadata.location().to_string(),
            };
            if operation_target
                .as_ref()
                .is_some_and(|existing| existing != &target)
            {
                return Err(invalid(
                    "Iceberg write operation cohorts do not share one exact frozen target",
                ));
            }
            operation_target = Some(target);
            let writer_payload = self.writer_payload(
                preparation.input(),
                &metadata,
                &table_info.namespace,
                &table_info.table,
                preparation.target_ref().as_str(),
                target_snapshot_id,
                &request.context,
            )?;
            let plan_payload = IcebergWritePlanPayloadV1 {
                version: 1,
                target: format!(
                    "{}.{}.{}",
                    table_info.catalog, table_info.namespace, table_info.table
                ),
                target_ref: preparation.target_ref().as_str().to_string(),
            };
            let control_payload = self.control_payload(
                request,
                &plan_payload,
                &metadata,
                target_snapshot_id,
                cow_recipes.get(&activated.cohort_id()),
            )?;
            if cohorts
                .insert(
                    activated.cohort_id(),
                    CohortService {
                        preparation_digest: preparation.digest(),
                        writer_payload,
                        routed_writer_payloads: None,
                        control_payload,
                        stable_digest: None,
                        attempts: HashMap::new(),
                    },
                )
                .is_some()
            {
                return Err(corrupt("Iceberg activation contains a duplicate cohort"));
            }
        }
        if matches!(
            request.intent,
            ConnectorWriteActivationIntent::ManagedPublication(_)
        ) {
            if let ConnectorWriteActivationSource::RowMutation(plan) = &request.source {
                if plan.copy_on_write().is_some() {
                    return Err(invalid(
                        "managed Iceberg publication does not support copy-on-write routing",
                    ));
                }
                let mut route_payloads = plan
                    .routes()
                    .iter()
                    .map(|route| {
                        cohorts
                            .get(&route.cohort_id())
                            .map(|cohort| (route.cohort_id(), cohort.writer_payload.clone()))
                            .ok_or_else(|| {
                                corrupt(
                                    "managed Iceberg row-mutation route omitted its activated cohort",
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                route_payloads.sort_by_key(|(cohort_id, _)| *cohort_id);
                let route_payloads = route_payloads
                    .into_iter()
                    .map(|(_, payload)| payload)
                    .collect::<Vec<_>>();
                for cohort in cohorts.values_mut() {
                    cohort.routed_writer_payloads = Some(route_payloads.clone());
                }
            }
        }
        Ok(ActiveOperation {
            activation_digest: activation.digest(),
            activation_intent: request.intent.clone(),
            activation_source: request.source.clone(),
            target: operation_target
                .ok_or_else(|| corrupt("Iceberg activation has no operation target"))?,
            cohorts,
            distributed_rewrite: None,
        })
    }

    /// Activate every cohort frozen by one distributed rewrite through the
    /// same generation-local reservation and operation tables used by normal
    /// writes.  No Core service registry or current-generation lookup is
    /// consulted.
    pub(crate) fn activate_distributed_rewrite(
        &self,
        operation_id: ConnectorWriteOperationId,
        planned: IcebergDistributedRewriteActivation,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorWriteActivation, ConnectorError> {
        validate_context(&context)?;
        if planned.cohorts.is_empty() {
            return Err(invalid(
                "Iceberg distributed rewrite activation has no frozen cohorts",
            ));
        }
        let source = planned
            .cohorts
            .first()
            .expect("checked non-empty distributed rewrite cohorts")
            .preparation
            .clone();
        let request = ConnectorWriteActivationRequest {
            operation_id,
            source: ConnectorWriteActivationSource::Prepared(source),
            intent: ConnectorWriteActivationIntent::Ordinary,
            context,
        };
        let activation = self.activations.activate_cohorts(
            &self.key,
            &request,
            planned
                .cohorts
                .iter()
                .map(|cohort| (cohort.cohort_id, cohort.preparation.clone()))
                .collect(),
        )?;
        let mut active = match self.build_active_operation(&request, &activation) {
            Ok(active) => active,
            Err(error) => {
                self.activations.release(operation_id)?;
                return Err(error);
            }
        };
        let mut data_paths = BTreeSet::new();
        let mut delete_paths = BTreeSet::new();
        for cohort in &planned.cohorts {
            if cohort.data_paths.is_empty()
                || cohort.data_paths.iter().any(|path| path.is_empty())
                || cohort.delete_paths.iter().any(|path| path.is_empty())
            {
                self.activations.release(operation_id)?;
                return Err(invalid(
                    "Iceberg distributed rewrite cohort has invalid frozen paths",
                ));
            }
            if planned.kind == IcebergDistributedRewriteKind::PositionDeletes
                && cohort.delete_paths.is_empty()
            {
                self.activations.release(operation_id)?;
                return Err(invalid(
                    "Iceberg position-delete rewrite cohort has no frozen Puffin inputs",
                ));
            }
            if !data_paths.is_disjoint(&cohort.data_paths)
                || !delete_paths.is_disjoint(&cohort.delete_paths)
            {
                self.activations.release(operation_id)?;
                return Err(invalid(
                    "Iceberg distributed rewrite cohorts overlap frozen file ownership",
                ));
            }
            data_paths.extend(cohort.data_paths.iter().cloned());
            delete_paths.extend(cohort.delete_paths.iter().cloned());
            active
                .cohorts
                .get_mut(&cohort.cohort_id)
                .ok_or_else(|| corrupt("Iceberg rewrite activation lost a frozen cohort"))?
                .control_payload = cohort.control_payload.clone();
        }
        active.distributed_rewrite = Some(DistributedRewriteOperation {
            kind: planned.kind,
            data_paths,
            delete_paths,
        });
        let mut operations = self.operations.lock().map_err(operation_lock_error)?;
        match operations.get(&operation_id) {
            Some(OperationState::Active(existing))
                if existing.activation_digest == activation.digest() =>
            {
                Ok(activation)
            }
            Some(_) => {
                drop(operations);
                self.activations.release(operation_id)?;
                Err(invalid(
                    "Iceberg distributed rewrite conflicts with an existing operation service",
                ))
            }
            None => {
                operations.insert(operation_id, OperationState::Active(active));
                Ok(activation)
            }
        }
    }

    /// Validate a staged-create writer aggregate against the exact activated
    /// operation without dispatching the ordinary table commit.  CTAS owns the
    /// later atomic create-table publication, but it must not bypass the same
    /// generation/cohort/control-payload admission used by ordinary writes.
    pub(crate) fn validate_staged_completion(
        &self,
        completion: &ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorError> {
        if completion.owner() != &self.key {
            return Err(invalid(
                "Iceberg staged-create completion has a foreign write generation",
            ));
        }
        let operation_id = completion.sealed().operation_id();
        let operations = self.operations.lock().map_err(operation_lock_error)?;
        let OperationState::Active(active) = operations
            .get(&operation_id)
            .ok_or_else(|| not_found("Iceberg staged-create completion has no active write"))?
        else {
            return Err(invalid(
                "Iceberg staged-create completion cannot use a terminal write operation",
            ));
        };
        if active.cohorts.len() != completion.sealed().cohorts().len()
            || completion.sealed().cohorts().iter().any(|descriptor| {
                active
                    .cohorts
                    .get(&descriptor.cohort_id())
                    .is_none_or(|cohort| cohort.stable_digest != Some(descriptor.planning_digest()))
            })
        {
            return Err(invalid(
                "Iceberg staged-create sealed cohorts do not match the active writer service",
            ));
        }
        for cohort in completion.cohorts() {
            let active_cohort = active.cohorts.get(&cohort.cohort_id()).ok_or_else(|| {
                invalid("Iceberg staged-create completion contains an unknown cohort")
            })?;
            for attempt in cohort
                .accepted()
                .into_iter()
                .chain(cohort.superseded().iter())
            {
                if attempt.owner() != &self.key
                    || attempt.operation_id() != operation_id
                    || attempt.cohort_id() != cohort.cohort_id()
                    || attempt.control_payload() != &active_cohort.control_payload
                {
                    return Err(invalid(
                        "Iceberg staged-create attempt does not match its active cohort",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Release the generation-local write reservation only after staged-create
    /// publication or abort has a known terminal outcome.  Unknown publication
    /// deliberately retains the operation for exact-generation reconcile.
    pub(crate) fn finish_staged_terminal(
        &self,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<(), ConnectorError> {
        let removed = {
            let mut operations = self.operations.lock().map_err(operation_lock_error)?;
            let removed = match operations.get(&operation_id) {
                Some(OperationState::Active(_))
                | Some(OperationState::KnownCommitted(_))
                | Some(OperationState::KnownUncommitted(_)) => {
                    operations.entries.remove(&operation_id)
                }
                Some(OperationState::CommitUnknown(_)) | Some(OperationState::Committing(_)) => {
                    return Err(invalid(
                        "Iceberg staged-create write reservation is not known terminal",
                    ));
                }
                None => None,
            };
            if removed.is_some() {
                operations
                    .terminal_order
                    .retain(|candidate| candidate != &operation_id);
            }
            removed
        };
        if removed.is_some() {
            self.activations.release(operation_id)?;
        }
        Ok(())
    }

    fn writer_payload(
        &self,
        input: &ConnectorWriteInputShape,
        metadata: &crate::iceberg::spec::TableMetadata,
        namespace: &str,
        table_name: &str,
        target_ref: &str,
        target_snapshot_id: Option<i64>,
        context: &ConnectorRequestContext,
    ) -> Result<Bytes, ConnectorError> {
        match input {
            ConnectorWriteInputShape::Data { .. } => {
                data_writer_payload(metadata, target_snapshot_id, false)
            }
            ConnectorWriteInputShape::RowLineage { .. } => {
                data_writer_payload(metadata, target_snapshot_id, true)
            }
            ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
                equality_delete_writer_payload(metadata, target_snapshot_id, equality_fields)
            }
            ConnectorWriteInputShape::PositionDelete {
                identity_fields,
                partition_source_fields,
            } => self.position_delete_writer_payload(
                metadata,
                namespace,
                table_name,
                target_ref,
                target_snapshot_id,
                identity_fields,
                partition_source_fields,
                false,
                context,
            ),
            ConnectorWriteInputShape::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => self.position_delete_writer_payload(
                metadata,
                namespace,
                table_name,
                target_ref,
                target_snapshot_id,
                identity_fields,
                partition_source_fields,
                true,
                context,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn position_delete_writer_payload(
        &self,
        prepared_metadata: &crate::iceberg::spec::TableMetadata,
        namespace: &str,
        table_name: &str,
        target_ref: &str,
        target_snapshot_id: Option<i64>,
        identity_fields: &[novarocks_spi::connector::ConnectorWriteFieldBinding],
        partition_source_fields: &[novarocks_spi::connector::ConnectorWriteFieldBinding],
        deletion_vectors: bool,
        context: &ConnectorRequestContext,
    ) -> Result<Bytes, ConnectorError> {
        validate_position_delete_shape(
            prepared_metadata,
            identity_fields,
            partition_source_fields,
        )?;
        let snapshot_id = target_snapshot_id
            .ok_or_else(|| invalid("Iceberg row-level write requires a frozen target snapshot"))?;
        self.runtime
            .control_state()
            .physical_table_cache()
            .invalidate(namespace, table_name);
        let physical = self
            .runtime
            .load_table(namespace, table_name)
            .map_err(unavailable)?;
        let open_metadata = physical.table.metadata();
        if prepared_metadata.uuid() != open_metadata.uuid()
            || prepared_metadata.current_schema_id() != open_metadata.current_schema_id()
            || prepared_metadata.default_partition_spec_id()
                != open_metadata.default_partition_spec_id()
            || crate::ref_snapshot::resolve_branch_head_snapshot_id(open_metadata, target_ref)
                .ok()
                .flatten()
                != Some(snapshot_id)
        {
            return Err(invalid(
                "opened Iceberg row-write table no longer matches the sealed preparation",
            ));
        }
        let table = physical.table.clone();
        let files = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats_at(&table, snapshot_id).await
            })
            .map_err(unavailable)?
            .map_err(unavailable)?;
        validate_context(context)?;
        let binding = self.runtime.resources().planning_binding();
        let mut partitions = Vec::with_capacity(files.len());
        for file in files {
            let partition_spec_id = file.partition_spec_id.ok_or_else(|| {
                corrupt(format!(
                    "Iceberg data file {} has no frozen partition spec ID",
                    file.path
                ))
            })?;
            let partition_values = file.partition_values.as_ref().ok_or_else(|| {
                corrupt(format!(
                    "Iceberg data file {} has no frozen partition values",
                    file.path
                ))
            })?;
            let partition_spec = open_metadata
                .partition_spec_by_id(partition_spec_id)
                .ok_or_else(|| {
                    corrupt(format!(
                        "Iceberg data file {} references unknown partition spec {}",
                        file.path, partition_spec_id
                    ))
                })?;
            let (partition_path, null_fingerprint) =
                super::report::partition_path_from_struct(partition_values, partition_spec)
                    .map_err(corrupt)?;
            let descriptor = crate::write_descriptor::encode_partition_descriptor(
                partition_values,
                partition_spec_id,
                open_metadata,
            )
            .map_err(|error| corrupt(error.detail_message()))?;
            let existing_deletion_vector_payload = if deletion_vectors {
                let info =
                    crate::manifest::data_file_with_stats_to_iceberg_data_file_info(file.clone());
                let specs = crate::delete_file::delete_specs_for_data_file(&info)?;
                if specs.is_empty() {
                    None
                } else {
                    let access = binding.resolve_access_for_locations(
                        specs.iter().map(|spec| spec.path.as_str()),
                    )?;
                    let file_context = binding.file_read_context(
                        novarocks_fs::FileCancellation::new(),
                        context.deadline(),
                    )?;
                    let deleted = crate::position_delete::load_position_deletes_with_context(
                        &specs,
                        &file.path,
                        &access,
                        &file_context,
                    )
                    .map_err(unavailable)?;
                    if deleted.is_empty() {
                        None
                    } else {
                        let mut vector = DeletionVector::new();
                        for position in deleted.iter() {
                            vector.insert(position).map_err(|error| {
                                corrupt(format!("encode existing Iceberg deletion vector: {error}"))
                            })?;
                        }
                        Some(vector.to_iceberg_payload().map_err(|error| {
                            corrupt(format!("encode existing Iceberg deletion vector: {error}"))
                        })?)
                    }
                }
            } else {
                None
            };
            partitions.push(IcebergPositionDeletePartitionInput {
                data_file_path: file.path,
                partition_path,
                null_fingerprint,
                partition_spec_id,
                descriptor,
                existing_deletion_vector_payload,
            });
        }
        partitions.sort_by(|left, right| left.data_file_path.cmp(&right.data_file_path));
        encode_write_handle(&IcebergWriteHandleInput {
            mode: if deletion_vectors {
                IcebergWriteHandleMode::DeletionVectors
            } else {
                IcebergWriteHandleMode::PositionDeletes
            },
            table_location: open_metadata.location().to_string(),
            data_location: data_location(open_metadata),
            target_partition_spec_id: open_metadata.default_partition_spec_id(),
            target_snapshot_id: Some(snapshot_id),
            file_format: if deletion_vectors {
                IcebergFileFormat::Puffin
            } else {
                IcebergFileFormat::Parquet
            },
            report_file_format: if deletion_vectors {
                "puffin".to_string()
            } else {
                "parquet".to_string()
            },
            compression: Compression::SNAPPY,
            equality_delete_columns: Vec::new(),
            row_lineage_data: false,
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            transform_exprs: Vec::new(),
            data_input_schema: None,
            position_delete_binding: None,
            position_delete_partitions: partitions,
        })
        .map_err(|error| internal(format!("encode Iceberg row-level writer handle: {error}")))
    }

    fn control_payload(
        &self,
        request: &ConnectorWriteActivationRequest,
        plan: &IcebergWritePlanPayloadV1,
        metadata: &crate::iceberg::spec::TableMetadata,
        target_snapshot_id: Option<i64>,
        cow_recipe: Option<&crate::row_mutation_payload::IcebergCowRecipePayloadV1>,
    ) -> Result<Bytes, ConnectorError> {
        if let Some(recipe) = cow_recipe {
            let base_snapshot_id = target_snapshot_id
                .ok_or_else(|| invalid("Iceberg COW cohort requires a frozen base snapshot"))?;
            let mut hasher = Sha256::new();
            hasher.update(b"novarocks.iceberg-cow-row-ids.v1\0");
            for row_id in &recipe.matched_row_ids {
                hasher.update(row_id.to_be_bytes());
            }
            return canonical_json(
                &IcebergCowCohortControlPayloadV1 {
                    version: 1,
                    target: &plan.target,
                    target_ref: &plan.target_ref,
                    role: &recipe.role,
                    base_snapshot_id,
                    old_file: (recipe.role == "rewrite").then_some(recipe.old_file.as_str()),
                    matched_row_count: recipe.matched_row_ids.len(),
                    matched_row_digest_base64: base64_encode(hasher.finalize()),
                },
                "Iceberg COW cohort control payload",
            );
        }
        match &request.intent {
            ConnectorWriteActivationIntent::Ordinary => plan.encode(),
            ConnectorWriteActivationIntent::ManagedPublication(intent) => {
                if !matches!(&request.source, ConnectorWriteActivationSource::Prepared(_)) {
                    return Err(invalid(
                        "Iceberg managed publication requires one prepared write source",
                    ));
                }
                let provenance = super::MvProvenanceV1 {
                    provenance_version: super::MV_PROVENANCE_VERSION,
                    refresh_id: intent.refresh_id(),
                    mv_id: intent.materialization_id(),
                    token: intent.marker().to_string(),
                    technique: match intent.technique() {
                        ConnectorManagedPublicationTechnique::Full => super::RefreshTechnique::Full,
                        ConnectorManagedPublicationTechnique::Incremental => {
                            super::RefreshTechnique::Incremental
                        }
                    },
                    bases: intent
                        .bases()
                        .iter()
                        .map(|base| super::ProvenanceBase {
                            table_fqn: base.table.to_string(),
                            uuid: base.uuid.to_string(),
                            from_snapshot: base.from_version,
                            to_snapshot: base.to_version,
                        })
                        .collect(),
                    definition_fingerprint: intent.definition_fingerprint().to_string(),
                    rows: 0,
                };
                IcebergFirstRefreshWritePlanPayloadV2 {
                    version: 2,
                    target: plan.target.clone(),
                    target_ref: plan.target_ref.clone(),
                    expected_snapshot_id: target_snapshot_id,
                    staging_path: format!(
                        "{}/data/_staging/{}",
                        metadata.location().trim_end_matches('/'),
                        request.operation_id
                    ),
                    provenance_properties: provenance.to_summary_properties().map_err(invalid)?,
                }
                .encode()
            }
        }
    }

    fn load_exact_commit_table(
        &self,
        target: &ActiveTarget,
    ) -> Result<crate::iceberg::table::Table, ConnectorError> {
        self.runtime
            .control_state()
            .physical_table_cache()
            .invalidate(&target.namespace, &target.table);
        let physical = self
            .runtime
            .load_table(&target.namespace, &target.table)
            .map_err(unavailable)?;
        let metadata = physical.table.metadata();
        let snapshot =
            crate::ref_snapshot::resolve_branch_head_snapshot_id(metadata, &target.target_ref)
                .map_err(invalid)?;
        if metadata.uuid().to_string() != target.table_uuid
            || snapshot != target.base_snapshot_id
            || metadata.current_schema_id() != target.schema_id
            || metadata.default_partition_spec_id() != target.partition_spec_id
            || metadata.location() != target.location
        {
            return Err(invalid(
                "Iceberg write target no longer matches its exact sealed preparation",
            ));
        }
        Ok(physical.table)
    }

    fn decode_commit_cohorts(
        &self,
        request: &ConnectorWriteCommitRequest,
        active: &ActiveOperation,
        metadata: &crate::iceberg::spec::TableMetadata,
    ) -> Result<Vec<DecodedCohort>, ConnectorError> {
        let descriptors = request
            .sealed()
            .cohorts()
            .iter()
            .map(|descriptor| (descriptor.cohort_id(), descriptor))
            .collect::<HashMap<_, _>>();
        let mut decoded = Vec::with_capacity(request.cohorts().len());
        for completion in request.cohorts() {
            let descriptor = descriptors.get(&completion.cohort_id()).ok_or_else(|| {
                invalid("Iceberg commit completion has no sealed cohort descriptor")
            })?;
            let cohort = active.cohorts.get(&completion.cohort_id()).ok_or_else(|| {
                not_found("Iceberg commit completion has no activated cohort service")
            })?;
            if cohort.stable_digest != Some(descriptor.planning_digest()) {
                return Err(invalid(
                    "Iceberg sealed cohort planning digest does not match its activated service",
                ));
            }
            let accepted = completion.accepted().ok_or_else(|| {
                invalid("Iceberg commit requires one accepted attempt for every cohort")
            })?;
            if accepted.control_payload() != &cohort.control_payload {
                return Err(invalid(
                    "Iceberg accepted attempt has a foreign cohort control payload",
                ));
            }
            let mut files = Vec::new();
            for staged in accepted.reports() {
                staged.validate()?;
                if staged.state() != novarocks_spi::connector::ConnectorWriterTerminalState::Staged
                {
                    return Err(invalid(
                        "Iceberg commit received a non-staged accepted writer report",
                    ));
                }
                let reports = crate::write_codec::decode_writer_reports(staged.payload(), metadata)
                    .map_err(|error| invalid(format!("decode Iceberg staged report: {error}")))?;
                let converter = IcebergCommitCollector::new(
                    CommitOpKind::FastAppend,
                    crate::iceberg::TableIdent::from_strs([
                        active.target.namespace.as_str(),
                        active.target.table.as_str(),
                    ])
                    .map_err(|error| invalid(format!("build Iceberg table identity: {error}")))?,
                    active.target.base_snapshot_id,
                    metadata.last_sequence_number(),
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().clone(),
                    data_location(metadata),
                );
                for report in reports {
                    files.push(converter.convert_writer_report(report).map_err(invalid)?);
                }
            }
            decoded.push(DecodedCohort {
                cohort_id: completion.cohort_id(),
                intent: descriptor.intent(),
                files,
            });
        }
        Ok(decoded)
    }

    fn execute_commit(
        &self,
        request: &ConnectorWriteCommitRequest,
        active: &ActiveOperation,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, CommitServiceError> {
        let table = self
            .load_exact_commit_table(&active.target)
            .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
        let metadata = table.metadata().clone();
        let decoded = self
            .decode_commit_cohorts(request, active, &metadata)
            .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
        let staged_data_rows = decoded
            .iter()
            .flat_map(|cohort| &cohort.files)
            .filter(|file| file.content == crate::iceberg::spec::DataContentType::Data)
            .fold(0_u64, |total, file| total.saturating_add(file.record_count));

        if staged_data_rows == 0
            && matches!(
                &active.activation_intent,
                ConnectorWriteActivationIntent::ManagedPublication(intent)
                    if intent.empty_input()
                        == ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit
            )
        {
            let files = decoded
                .iter()
                .flat_map(|cohort| cohort.files.iter().cloned())
                .collect::<Vec<_>>();
            let cleanup = self
                .cleanup_files(&files)
                .map_err(CommitServiceError::invalid_input)?;
            return Err(CommitServiceError::known_uncommitted(
                "managed Iceberg publication produced empty input".to_string(),
                cleanup,
            ));
        }
        if decoded.iter().all(|cohort| cohort.files.is_empty())
            && matches!(
                active.activation_intent,
                ConnectorWriteActivationIntent::Ordinary
            )
        {
            return Err(CommitServiceError::invalid_input(
                "known-empty Iceberg writes must terminate through provider abort".to_string(),
            ));
        }

        let (op_kind, cow_update_rewrite) = commit_shape(active, &decoded)?;
        let selected_rewrite = active.distributed_rewrite.as_ref().map(|rewrite| {
            super::selected_rewrite::SelectedRewriteFiles {
                kind: match rewrite.kind {
                    IcebergDistributedRewriteKind::Data => {
                        super::selected_rewrite::SelectedRewriteKind::Data
                    }
                    IcebergDistributedRewriteKind::PositionDeletes => {
                        super::selected_rewrite::SelectedRewriteKind::PositionDeletes
                    }
                },
                data_paths: rewrite.data_paths.clone(),
                delete_paths: rewrite.delete_paths.clone(),
            }
        });
        let table_ident = crate::iceberg::TableIdent::from_strs([
            active.target.namespace.as_str(),
            active.target.table.as_str(),
        ])
        .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
        let collector = Arc::new(
            IcebergCommitCollector::new(
                op_kind,
                table_ident,
                active.target.base_snapshot_id,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!(
                    "{}/_staging/{}",
                    data_location(&metadata).trim_end_matches('/'),
                    request.operation_id()
                ),
            )
            .with_table_metadata(metadata.clone()),
        );
        for cohort in &decoded {
            if op_kind == CommitOpKind::RowDeltaDvFromFiles
                && cohort.intent == novarocks_spi::connector::ConnectorWriteIntent::Append
            {
                collector.inject_appended_files(cohort.files.clone());
            } else {
                collector.inject_written_files(cohort.files.clone());
            }
        }
        let snapshot_properties = self.snapshot_properties(request, active, staged_data_rows)?;
        let binding = self.runtime.resources().planning_binding();
        let access = binding
            .resolve_access(metadata.location())
            .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
        let fs = access.operator();
        let cleanup_access = access.clone();
        let cleanup_path_mapper = Some(Arc::new(move |path: &str| {
            cleanup_access
                .bind_location(path, novarocks_fs::FileIdentity::new(path, 0, None))
                .map(|file| file.operator_relative_path().to_string())
                .unwrap_or_else(|_| path.to_string())
        }) as super::CleanupPathMapper);
        let catalog = Arc::clone(self.runtime.catalog());
        let input = RunInput {
            collector,
            catalog,
            table: table.clone(),
            fs,
            file_io: table.file_io().clone(),
            cleanup_path_mapper,
            cow_update_rewrite,
            selected_rewrite,
            target_ref: active.target.target_ref.clone(),
            snapshot_properties,
        };
        let result = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { run_iceberg_commit(input).await })
            .map_err(|error| {
                CommitServiceError::known_uncommitted(error, super::CleanupAttempt::not_attempted())
            })??;
        let resulting_row_count = if matches!(
            active.activation_intent,
            ConnectorWriteActivationIntent::ManagedPublication(_)
        ) {
            table_snapshot_row_count(&self.runtime, &active.target, result.new_snapshot_id)?
        } else {
            None
        };
        let receipt = crate::write_codec::connector_write_receipt(
            result.new_snapshot_id,
            resulting_row_count,
        )
        .map_err(CommitServiceError::invalid_input)?;
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        })
    }

    fn snapshot_properties(
        &self,
        request: &ConnectorWriteCommitRequest,
        active: &ActiveOperation,
        rows: u64,
    ) -> Result<BTreeMap<String, String>, CommitServiceError> {
        let mut properties = managed_snapshot_properties(&active.activation_intent, rows)?;
        let marker = self.operation_marker(
            request.operation_id(),
            active,
            request.sealed().digest(),
            request.aggregate_digest(),
        );
        let encoded = serde_json::to_string(&marker).map_err(|error| {
            CommitServiceError::invalid_input(format!(
                "encode Iceberg write operation marker: {error}"
            ))
        })?;
        if properties
            .insert(ICEBERG_WRITE_OPERATION_MARKER_PROPERTY.to_string(), encoded)
            .is_some()
        {
            return Err(CommitServiceError::invalid_input(
                "Iceberg write operation marker conflicts with managed snapshot properties"
                    .to_string(),
            ));
        }
        Ok(properties)
    }

    fn operation_marker(
        &self,
        operation_id: ConnectorWriteOperationId,
        active: &ActiveOperation,
        cohort_set_digest: [u8; 32],
        aggregate_digest: [u8; 32],
    ) -> IcebergWriteOperationMarkerV1 {
        IcebergWriteOperationMarkerV1 {
            version: ICEBERG_WRITE_OPERATION_MARKER_VERSION,
            instance_id: self.key.instance_id.as_str().to_string(),
            incarnation_base64: base64_encode(self.key.incarnation.to_bytes()),
            operation_id_base64: base64_encode(operation_id.to_bytes()),
            target_ref: active.target.target_ref.clone(),
            cohort_set_digest_base64: base64_encode(cohort_set_digest),
            aggregate_digest_base64: base64_encode(aggregate_digest),
        }
    }

    fn invalidate_target_caches(&self, target: &ActiveTarget) {
        self.runtime
            .control_state()
            .invalidate_table_cache(&target.namespace, &target.table);
    }

    fn cleanup_files(&self, files: &[WrittenFile]) -> Result<super::CleanupAttempt, String> {
        if files.is_empty() {
            return Ok(super::CleanupAttempt::completed(Vec::new()));
        }
        let binding = self.runtime.resources().planning_binding();
        let access = binding
            .resolve_access_for_locations(files.iter().map(|file| file.path.as_str()))
            .map_err(|error| error.to_string())?;
        let paths = access
            .operator_relative_paths()
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let operator = access.operator();
        let cleanup = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                let abort = super::AbortLog::new();
                for path in paths {
                    abort.record_data_file(path);
                }
                abort.cleanup(&operator).await
            })?;
        Ok(super::CleanupAttempt::from_cleanup_errors(&cleanup))
    }

    /// Seal provider recovery facts into the exact-generation SPI envelope.
    /// The provider commit action calls this only after it has classified an
    /// external commit result as uncertain.
    pub fn encode_commit_unknown_evidence(
        &self,
        request: &ConnectorWriteCommitRequest,
        recovery: RecoveryEvidence,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let payload = canonical_json(
            &IcebergWriteReconcileEvidenceV1 {
                version: ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION,
                operation_id_base64: base64_encode(request.operation_id().to_bytes()),
                cohort_set_digest_base64: base64_encode(request.sealed().digest()),
                aggregate_digest_base64: base64_encode(request.aggregate_digest()),
                recovery: IcebergRecoveryEvidenceV1 {
                    table_ident: recovery.table_ident,
                    op_kind: format!("{:?}", recovery.op_kind),
                    base_snapshot_id: recovery.base_snapshot_id,
                    base_sequence_number: recovery.base_sequence_number,
                    staging_dir: recovery.staging_dir,
                },
            },
            "Iceberg write reconciliation evidence",
        )?;
        ExternalMutationEvidence::try_new(
            ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.key.incarnation,
            ConnectorMutationOperationId::from_bytes(request.operation_id().to_bytes()),
            ICEBERG_WRITE_OPERATION_KIND,
            payload,
        )
    }

    fn decode_evidence(
        &self,
        evidence: &ExternalMutationEvidence,
    ) -> Result<IcebergWriteReconcileEvidenceV1, ConnectorError> {
        if evidence.schema_version() != ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION
            || evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.key.incarnation
            || evidence.operation_kind() != ICEBERG_WRITE_OPERATION_KIND
        {
            return Err(invalid(
                "Iceberg write reconciliation evidence has a foreign connector generation",
            ));
        }
        let decoded: IcebergWriteReconcileEvidenceV1 =
            serde_json::from_slice(evidence.provider_payload()).map_err(|error| {
                invalid(format!(
                    "decode Iceberg write reconciliation evidence: {error}"
                ))
            })?;
        if decoded.version != ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION
            || canonical_json(&decoded, "Iceberg write reconciliation evidence")?.as_ref()
                != evidence.provider_payload().as_ref()
        {
            return Err(invalid(
                "Iceberg write reconciliation evidence is not canonical v2",
            ));
        }
        let operation_id = decode_fixed::<16>(&decoded.operation_id_base64, "operation id")?;
        if evidence.operation_id() != ConnectorMutationOperationId::from_bytes(operation_id) {
            return Err(invalid(
                "Iceberg write reconciliation operation ID mismatch",
            ));
        }
        decode_fixed::<32>(&decoded.cohort_set_digest_base64, "cohort set digest")?;
        decode_fixed::<32>(&decoded.aggregate_digest_base64, "aggregate digest")?;
        Ok(decoded)
    }
}

impl ConnectorWriteControl for IcebergWriteControl {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn prepare_write(
        &self,
        request: ConnectorWritePreparationRequest,
    ) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
        super::write_preparation::prepare_write(request, &self.key)
    }

    fn prepare_row_mutation(
        &self,
        request: ConnectorRowMutationPreparationRequest,
    ) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError> {
        super::row_mutation_preparation::prepare_row_mutation(request, &self.key)
    }

    fn activate_row_mutation(
        &self,
        request: ConnectorRowMutationActivationRequest,
    ) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
        super::row_mutation_activation::activate_row_mutation(request, &self.key)
    }

    fn activate_write(
        &self,
        request: ConnectorWriteActivationRequest,
    ) -> Result<ConnectorWriteActivation, ConnectorError> {
        validate_context(&request.context)?;
        request.validate(&self.key)?;
        {
            let operations = self.operations.lock().map_err(operation_lock_error)?;
            if matches!(
                operations.get(&request.operation_id),
                Some(OperationState::KnownUncommitted(_))
            ) {
                return Err(invalid(
                    "Iceberg write operation cannot be reactivated after a known terminal outcome",
                ));
            }
        }
        let activation = self.activations.activate(&self.key, &request)?;
        let active = match self.build_active_operation(&request, &activation) {
            Ok(active) => active,
            Err(error) => {
                self.activations.release(request.operation_id)?;
                return Err(error);
            }
        };
        let mut operations = self.operations.lock().map_err(operation_lock_error)?;
        match operations.get(&request.operation_id) {
            Some(OperationState::Active(existing))
                if existing.activation_digest == activation.digest() =>
            {
                Ok(activation)
            }
            Some(_) => {
                drop(operations);
                self.activations.release(request.operation_id)?;
                Err(invalid(
                    "Iceberg write operation has a conflicting generation-local service",
                ))
            }
            None => {
                operations.insert(request.operation_id, OperationState::Active(active));
                Ok(activation)
            }
        }
    }

    fn plan_write(
        &self,
        request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlan, ConnectorError> {
        validate_context(&request.context)?;
        request.validate(&self.key)?;
        let stable_digest = request.stable_digest(&self.key)?;
        let attempt_digest = planning_attempt_digest(&request);
        let mut operations = self.operations.lock().map_err(operation_lock_error)?;
        let OperationState::Active(operation) = operations
            .get_mut(&request.operation_id)
            .ok_or_else(|| not_found("Iceberg write operation has no activated service"))?
        else {
            return Err(invalid(
                "Iceberg write operation cannot be planned after a known terminal outcome",
            ));
        };
        if operation.activation_digest != request.activation.activation_digest() {
            return Err(invalid(
                "Iceberg planning activation does not match its reserved service",
            ));
        }
        let cohort = operation
            .cohorts
            .get_mut(&request.cohort_id)
            .ok_or_else(|| not_found("Iceberg write operation has no activated cohort service"))?;
        if cohort.preparation_digest != request.activation.preparation().digest() {
            return Err(invalid(
                "Iceberg planning preparation does not match its activated cohort service",
            ));
        }
        match cohort.stable_digest {
            Some(existing) if existing != stable_digest => {
                return Err(invalid(
                    "Iceberg write cohort was replanned with different stable inputs",
                ));
            }
            None => cohort.stable_digest = Some(stable_digest),
            Some(_) => {}
        }
        if let Some(cached) = cohort.attempts.get(&request.execution_id) {
            return if cached.attempt_digest == attempt_digest {
                Ok(cached.plan.clone())
            } else {
                Err(invalid(
                    "Iceberg write execution attempt was replayed with different writer inputs",
                ))
            };
        }
        let writer_payloads = routed_writer_payloads_for_manifest(
            &cohort.writer_payload,
            cohort.routed_writer_payloads.as_deref(),
            &request.expected_writers,
        )?;
        let handles = request
            .expected_writers
            .iter()
            .cloned()
            .zip(writer_payloads)
            .map(|(writer, payload)| {
                ConnectorWriterHandle::try_new(
                    self.key.clone(),
                    writer,
                    ICEBERG_WRITE_PAYLOAD_VERSION,
                    payload,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let plan = ConnectorWritePlan::try_new(
            self.key.clone(),
            request.operation_id,
            request.cohort_id,
            request.execution_id,
            handles,
            cohort.control_payload.clone(),
        )?;
        cohort.attempts.insert(
            request.execution_id,
            CachedPlan {
                attempt_digest,
                plan: plan.clone(),
            },
        );
        Ok(plan)
    }

    fn commit(
        &self,
        request: ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        validate_context(&request.context)?;
        self.ensure_owner(request.owner())?;
        let operation_id = request.operation_id();
        let active = {
            let mut operations = self.operations.lock().map_err(operation_lock_error)?;
            match operations.get(&operation_id).cloned() {
                Some(OperationState::Active(active)) => {
                    operations.insert(
                        operation_id,
                        OperationState::Committing(CommittingOperation {
                            cohort_set_digest: request.sealed().digest(),
                            aggregate_digest: request.aggregate_digest(),
                        }),
                    );
                    active
                }
                Some(OperationState::Committing(record)) => {
                    ensure_terminal_digest(
                        request.sealed().digest(),
                        request.aggregate_digest(),
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    return Err(unavailable(
                        "Iceberg write commit for this sealed operation is already in progress",
                    ));
                }
                Some(OperationState::KnownCommitted(record)) => {
                    ensure_terminal_digest(
                        request.sealed().digest(),
                        request.aggregate_digest(),
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    return Ok(record.outcome.clone());
                }
                Some(OperationState::CommitUnknown(record)) => {
                    ensure_terminal_digest(
                        request.sealed().digest(),
                        request.aggregate_digest(),
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    return Ok(record.outcome.clone());
                }
                Some(OperationState::KnownUncommitted(record)) => {
                    ensure_terminal_digest(
                        request.sealed().digest(),
                        request.aggregate_digest(),
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    return Ok(ExternalMutationOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            "Iceberg write operation is already known uncommitted",
                        ),
                    });
                }
                None => {
                    return Err(not_found(
                        "Iceberg write commit has no activated operation service",
                    ));
                }
            }
        };
        let (outcome, known_uncommitted_finalization) = match self.execute_commit(&request, &active)
        {
            Ok(outcome) => (outcome, None),
            Err(CommitServiceError::InvalidInput { message }) => {
                let mut operations = self.operations.lock().map_err(operation_lock_error)?;
                if matches!(
                    operations.get(&operation_id),
                    Some(OperationState::Committing(record))
                        if record.cohort_set_digest == request.sealed().digest()
                            && record.aggregate_digest == request.aggregate_digest()
                ) {
                    operations.insert(operation_id, OperationState::Active(active.clone()));
                }
                return Err(invalid(message));
            }
            Err(CommitServiceError::KnownUncommitted { message, cleanup }) => (
                ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        format!(
                            "{message}; staged cleanup attempted={}, errors={}",
                            cleanup.attempted, cleanup.error_count
                        ),
                    ),
                },
                Some(cleanup_finalization(&cleanup)),
            ),
            Err(CommitServiceError::Unknown { message, evidence }) => (
                ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        message,
                    ),
                    evidence: self.encode_commit_unknown_evidence(&request, evidence)?,
                },
                None,
            ),
            Err(CommitServiceError::FinalizeFailedKnownCommitted {
                outcome: Some(committed),
                finalize_error,
                ..
            }) => (
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: crate::write_codec::connector_write_receipt(
                        committed.new_snapshot_id,
                        None,
                    )
                    .map_err(internal)?,
                    finalization: ExternalMutationFinalization::Failed(
                        ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Internal,
                            finalize_error,
                        ),
                    ),
                },
                None,
            ),
            Err(CommitServiceError::FinalizeFailedKnownCommitted {
                outcome: None,
                finalize_error,
                evidence,
            }) => (
                ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Internal,
                        finalize_error,
                    ),
                    evidence: self.encode_commit_unknown_evidence(&request, evidence)?,
                },
                None,
            ),
        };
        self.invalidate_target_caches(&active.target);
        let mut operations = self.operations.lock().map_err(operation_lock_error)?;
        let state = match &outcome {
            ExternalMutationOutcome::KnownCommitted { .. } => {
                OperationState::KnownCommitted(KnownCommittedOperation {
                    cohort_set_digest: request.sealed().digest(),
                    aggregate_digest: request.aggregate_digest(),
                    outcome: outcome.clone(),
                })
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {
                OperationState::KnownUncommitted(KnownUncommittedOperation {
                    cohort_set_digest: request.sealed().digest(),
                    aggregate_digest: request.aggregate_digest(),
                    outcome: ConnectorWriteAbortOutcome::KnownUncommitted {
                        cleanup: known_uncommitted_finalization.clone().ok_or_else(|| {
                            internal(
                                "Iceberg known-uncommitted commit is missing cleanup finalization",
                            )
                        })?,
                    },
                })
            }
            ExternalMutationOutcome::CommitUnknown { .. } => {
                OperationState::CommitUnknown(CommitUnknownOperation {
                    cohort_set_digest: request.sealed().digest(),
                    aggregate_digest: request.aggregate_digest(),
                    outcome: outcome.clone(),
                    active: active.clone(),
                    request: request.clone(),
                })
            }
        };
        operations.insert(operation_id, state);
        drop(operations);
        if !matches!(outcome, ExternalMutationOutcome::CommitUnknown { .. }) {
            self.activations.release(operation_id)?;
        }
        Ok(outcome)
    }

    fn abort(
        &self,
        request: ConnectorWriteAbortRequest,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        validate_context(&request.context)?;
        self.ensure_owner(&request.owner)?;
        let operation_id = request.operation_id();
        let active = {
            let operations = self.operations.lock().map_err(operation_lock_error)?;
            match operations.get(&operation_id) {
                Some(OperationState::KnownUncommitted(record)) => {
                    if record.cohort_set_digest != request.sealed.digest()
                        || record.aggregate_digest != request.aggregate_digest
                    {
                        return Err(invalid(
                            "Iceberg write operation was aborted with a different sealed aggregate",
                        ));
                    }
                    let outcome = record.outcome.clone();
                    self.activations.release(operation_id)?;
                    return Ok(outcome);
                }
                Some(OperationState::Active(operation)) => {
                    for descriptor in request.sealed.cohorts() {
                        let cohort =
                            operation
                                .cohorts
                                .get(&descriptor.cohort_id())
                                .ok_or_else(|| {
                                    not_found("Iceberg sealed cohort has no activated service")
                                })?;
                        if cohort
                            .stable_digest
                            .is_some_and(|digest| digest != descriptor.planning_digest())
                        {
                            return Err(invalid(
                                "Iceberg sealed cohort planning digest does not match its frozen parent",
                            ));
                        }
                    }
                    operation.clone()
                }
                Some(OperationState::Committing(record)) => {
                    ensure_terminal_digest(
                        request.sealed.digest(),
                        request.aggregate_digest,
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    return Err(unavailable(
                        "Iceberg write abort cannot race an in-progress external commit",
                    ));
                }
                Some(OperationState::KnownCommitted(record)) => {
                    if record.cohort_set_digest != request.sealed.digest()
                        || record.aggregate_digest != request.aggregate_digest
                    {
                        return Err(invalid(
                            "Iceberg write operation was aborted with a different sealed aggregate",
                        ));
                    }
                    let ExternalMutationOutcome::KnownCommitted {
                        receipt,
                        finalization,
                        ..
                    } = &record.outcome
                    else {
                        return Err(internal("Iceberg committed operation state is corrupt"));
                    };
                    return Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                        receipt: receipt.clone(),
                        finalization: finalization.clone(),
                    });
                }
                Some(OperationState::CommitUnknown(record)) => {
                    if record.cohort_set_digest != request.sealed.digest()
                        || record.aggregate_digest != request.aggregate_digest
                    {
                        return Err(invalid(
                            "Iceberg write operation was aborted with a different sealed aggregate",
                        ));
                    }
                    let ExternalMutationOutcome::CommitUnknown { failure, evidence } =
                        &record.outcome
                    else {
                        return Err(internal(
                            "Iceberg commit-unknown operation state is corrupt",
                        ));
                    };
                    return Ok(ConnectorWriteAbortOutcome::CommitUnknown {
                        failure: failure.clone(),
                        evidence: evidence.clone(),
                    });
                }
                None => {
                    return Err(not_found(
                        "Iceberg write abort has no activated operation service",
                    ));
                }
            }
        };
        let cleanup = if request.cohorts.is_empty() {
            super::CleanupAttempt::completed(Vec::new())
        } else {
            let physical = self
                .runtime
                .load_table(&active.target.namespace, &active.target.table)
                .map_err(unavailable)?;
            let metadata = physical.table.metadata();
            let converter = IcebergCommitCollector::new(
                CommitOpKind::FastAppend,
                crate::iceberg::TableIdent::from_strs([
                    active.target.namespace.as_str(),
                    active.target.table.as_str(),
                ])
                .map_err(|error| invalid(format!("build Iceberg table identity: {error}")))?,
                active.target.base_snapshot_id,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                data_location(metadata),
            );
            let mut files = Vec::new();
            for completion in &request.cohorts {
                let cohort = active
                    .cohorts
                    .get(&completion.cohort_id())
                    .ok_or_else(|| not_found("Iceberg abort cohort has no activated service"))?;
                for attempt in completion
                    .accepted()
                    .into_iter()
                    .chain(completion.superseded())
                {
                    if attempt.control_payload() != &cohort.control_payload {
                        return Err(invalid(
                            "Iceberg abort attempt has a foreign cohort control payload",
                        ));
                    }
                    for staged in attempt.reports() {
                        staged.validate()?;
                        for report in
                            crate::write_codec::decode_writer_reports(staged.payload(), metadata)
                                .map_err(invalid)?
                        {
                            files.push(converter.convert_writer_report(report).map_err(invalid)?);
                        }
                    }
                }
            }
            self.cleanup_files(&files).map_err(internal)?
        };
        let outcome = ConnectorWriteAbortOutcome::KnownUncommitted {
            cleanup: cleanup_finalization(&cleanup),
        };
        self.invalidate_target_caches(&active.target);
        let mut operations = self.operations.lock().map_err(operation_lock_error)?;
        operations.insert(
            operation_id,
            OperationState::KnownUncommitted(KnownUncommittedOperation {
                cohort_set_digest: request.sealed.digest(),
                aggregate_digest: request.aggregate_digest,
                outcome: outcome.clone(),
            }),
        );
        drop(operations);
        self.activations.release(operation_id)?;
        Ok(outcome)
    }

    fn reconcile(
        &self,
        request: ConnectorWriteReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        validate_context(&request.context)?;
        self.ensure_owner(&request.owner)?;
        let evidence = self.decode_evidence(&request.evidence)?;
        let operation_id = ConnectorWriteOperationId::from_bytes(decode_fixed::<16>(
            &evidence.operation_id_base64,
            "operation id",
        )?);
        if operation_id != request.operation_id
            || decode_fixed::<32>(&evidence.cohort_set_digest_base64, "cohort set digest")?
                != request.cohort_set_digest
            || decode_fixed::<32>(&evidence.aggregate_digest_base64, "aggregate digest")?
                != request.aggregate_digest
        {
            return Err(invalid(
                "Iceberg write reconciliation request does not match its evidence",
            ));
        }
        let unknown = {
            let operations = self.operations.lock().map_err(operation_lock_error)?;
            match operations.get(&operation_id) {
                Some(OperationState::KnownCommitted(record)) => {
                    ensure_terminal_digest(
                        request.cohort_set_digest,
                        request.aggregate_digest,
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    return Ok(record.outcome.clone());
                }
                Some(OperationState::KnownUncommitted(record)) => {
                    ensure_terminal_digest(
                        request.cohort_set_digest,
                        request.aggregate_digest,
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    return Ok(ExternalMutationOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            "Iceberg write operation is known uncommitted after reconciliation",
                        ),
                    });
                }
                Some(OperationState::CommitUnknown(record)) => {
                    ensure_terminal_digest(
                        request.cohort_set_digest,
                        request.aggregate_digest,
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    record.clone()
                }
                Some(OperationState::Active(_)) => {
                    return Err(invalid(
                        "Iceberg write reconciliation requires a prior commit-unknown outcome",
                    ));
                }
                Some(OperationState::Committing(record)) => {
                    ensure_terminal_digest(
                        request.cohort_set_digest,
                        request.aggregate_digest,
                        record.cohort_set_digest,
                        record.aggregate_digest,
                    )?;
                    return Err(unavailable(
                        "Iceberg write reconciliation cannot race an in-progress external commit",
                    ));
                }
                None => {
                    return Err(not_found(
                        "Iceberg write reconciliation has no generation-local operation",
                    ));
                }
            }
        };
        self.invalidate_target_caches(&unknown.active.target);
        let ident = crate::iceberg::TableIdent::from_strs([
            unknown.active.target.namespace.as_str(),
            unknown.active.target.table.as_str(),
        ])
        .map_err(|error| invalid(format!("build Iceberg table identity: {error}")))?;
        let catalog = Arc::clone(self.runtime.catalog());
        let table = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { catalog.load_table(&ident).await })
            .map_err(unavailable)?
            .map_err(|error| unavailable(error.to_string()))?;
        if table.metadata().uuid().to_string() != unknown.active.target.table_uuid {
            return Err(corrupt(
                "Iceberg write reconciliation loaded a different physical table UUID",
            ));
        }
        let expected_marker = self.operation_marker(
            operation_id,
            &unknown.active,
            request.cohort_set_digest,
            request.aggregate_digest,
        );
        let matched_snapshot = find_operation_marker_snapshot(
            table.metadata().snapshots().map(Arc::as_ref),
            &expected_marker,
        )?;
        let (outcome, known_uncommitted_finalization) = if let Some(snapshot) = matched_snapshot {
            let row_count = match &unknown.active.activation_intent {
                ConnectorWriteActivationIntent::ManagedPublication(expected) => {
                    let provenance = super::MvProvenanceV1::from_snapshot_summary(snapshot)
                        .map_err(corrupt)?
                        .ok_or_else(|| {
                            corrupt("Iceberg managed publication snapshot is missing provenance")
                        })?;
                    if !managed_provenance_matches(expected, &provenance) {
                        return Err(corrupt(
                            "Iceberg managed publication snapshot provenance does not match its operation marker",
                        ));
                    }
                    Some(u64::try_from(provenance.rows).map_err(|_| {
                        corrupt("Iceberg managed publication snapshot has a negative row count")
                    })?)
                }
                ConnectorWriteActivationIntent::Ordinary => None,
            };
            (
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: crate::write_codec::connector_write_receipt(
                        snapshot.snapshot_id(),
                        row_count,
                    )
                    .map_err(internal)?,
                    finalization: ExternalMutationFinalization::Complete,
                },
                None,
            )
        } else {
            let decoded =
                self.decode_commit_cohorts(&unknown.request, &unknown.active, table.metadata())?;
            let files = decoded
                .into_iter()
                .flat_map(|cohort| cohort.files)
                .collect::<Vec<_>>();
            let cleanup = self.cleanup_files(&files).map_err(internal)?;
            (
                ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        format!(
                            "Iceberg write operation marker is absent; staged cleanup errors={}",
                            cleanup.error_count
                        ),
                    ),
                },
                Some(cleanup_finalization(&cleanup)),
            )
        };
        let mut operations = self.operations.lock().map_err(operation_lock_error)?;
        match &outcome {
            ExternalMutationOutcome::KnownCommitted { .. } => {
                operations.insert(
                    operation_id,
                    OperationState::KnownCommitted(KnownCommittedOperation {
                        cohort_set_digest: request.cohort_set_digest,
                        aggregate_digest: request.aggregate_digest,
                        outcome: outcome.clone(),
                    }),
                );
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {
                operations.insert(
                    operation_id,
                    OperationState::KnownUncommitted(KnownUncommittedOperation {
                        cohort_set_digest: request.cohort_set_digest,
                        aggregate_digest: request.aggregate_digest,
                        outcome: ConnectorWriteAbortOutcome::KnownUncommitted {
                            cleanup: known_uncommitted_finalization.clone().ok_or_else(|| {
                                internal(
                                    "Iceberg reconciled known-uncommitted operation is missing cleanup finalization",
                                )
                            })?,
                        },
                    }),
                );
            }
            ExternalMutationOutcome::CommitUnknown { .. } => unreachable!(),
        }
        drop(operations);
        self.activations.release(operation_id)?;
        Ok(outcome)
    }
}

fn commit_shape(
    active: &ActiveOperation,
    cohorts: &[DecodedCohort],
) -> Result<(CommitOpKind, Option<CowUpdateRewriteSet>), CommitServiceError> {
    if active.distributed_rewrite.is_some() {
        return Ok((CommitOpKind::SelectedRewrite, None));
    }
    if let ConnectorWriteActivationSource::RowMutation(plan) = &active.activation_source
        && let Some((_, recipes)) = plan.copy_on_write()
    {
        let files_by_cohort = cohorts
            .iter()
            .map(|cohort| (cohort.cohort_id, cohort.files.clone()))
            .collect::<HashMap<_, _>>();
        let mut touched_data_files = Vec::new();
        let mut appended_files = Vec::new();
        let mut updated_row_ids = BTreeSet::new();
        for recipe in recipes {
            let decoded = crate::row_mutation_payload::decode_cow_recipe(recipe.payload())
                .map_err(CommitServiceError::invalid_input)?;
            let files = files_by_cohort.get(&recipe.cohort_id()).ok_or_else(|| {
                CommitServiceError::invalid_input(
                    "Iceberg COW recipe has no accepted cohort completion".to_string(),
                )
            })?;
            match decoded.role.as_str() {
                "rewrite" => {
                    updated_row_ids.extend(decoded.matched_row_ids.iter().copied());
                    touched_data_files.push(CowUpdateTouchedFile {
                        old_file: decoded.old_file,
                        new_files: files.iter().map(|file| file.path.clone()).collect(),
                        row_ids: decoded.matched_row_ids,
                    });
                }
                "append" => appended_files.extend(files.iter().cloned()),
                _ => {
                    return Err(CommitServiceError::invalid_input(
                        "Iceberg COW recipe has an unknown role".to_string(),
                    ));
                }
            }
        }
        let base_snapshot_id = active.target.base_snapshot_id.ok_or_else(|| {
            CommitServiceError::invalid_input(
                "Iceberg COW commit requires a frozen base snapshot".to_string(),
            )
        })?;
        return Ok((
            CommitOpKind::CowUpdate,
            Some(CowUpdateRewriteSet {
                base_snapshot_id,
                target_table_uuid: active.target.table_uuid.clone(),
                updated_row_ids: updated_row_ids.into_iter().collect(),
                touched_data_files,
                appended_files,
            }),
        ));
    }

    let intents = cohorts
        .iter()
        .map(|cohort| cohort.intent)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let kind = match intents.as_slice() {
        [novarocks_spi::connector::ConnectorWriteIntent::Append] => CommitOpKind::FastAppend,
        [novarocks_spi::connector::ConnectorWriteIntent::Overwrite] => CommitOpKind::Overwrite,
        [novarocks_spi::connector::ConnectorWriteIntent::PartitionOverwrite] => {
            CommitOpKind::OverwritePartitions
        }
        [novarocks_spi::connector::ConnectorWriteIntent::RowDelta] => {
            if cohorts
                .iter()
                .flat_map(|cohort| &cohort.files)
                .any(|file| file.format == crate::iceberg::spec::DataFileFormat::Puffin)
            {
                CommitOpKind::RowDeltaDvFromFiles
            } else {
                CommitOpKind::RowDelta
            }
        }
        [
            novarocks_spi::connector::ConnectorWriteIntent::Append,
            novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
        ] => CommitOpKind::RowDeltaDvFromFiles,
        _ => {
            return Err(CommitServiceError::invalid_input(
                "Iceberg operation has an unsupported sealed cohort intent combination".to_string(),
            ));
        }
    };
    Ok((kind, None))
}

fn managed_snapshot_properties(
    intent: &ConnectorWriteActivationIntent,
    rows: u64,
) -> Result<BTreeMap<String, String>, CommitServiceError> {
    let ConnectorWriteActivationIntent::ManagedPublication(intent) = intent else {
        return Ok(BTreeMap::new());
    };
    let rows = i64::try_from(rows).map_err(|_| {
        CommitServiceError::invalid_input(
            "Iceberg managed publication row count exceeds i64".to_string(),
        )
    })?;
    super::MvProvenanceV1 {
        provenance_version: super::MV_PROVENANCE_VERSION,
        refresh_id: intent.refresh_id(),
        mv_id: intent.materialization_id(),
        token: intent.marker().to_string(),
        technique: match intent.technique() {
            ConnectorManagedPublicationTechnique::Full => super::RefreshTechnique::Full,
            ConnectorManagedPublicationTechnique::Incremental => {
                super::RefreshTechnique::Incremental
            }
        },
        bases: intent
            .bases()
            .iter()
            .map(|base| super::ProvenanceBase {
                table_fqn: base.table.to_string(),
                uuid: base.uuid.to_string(),
                from_snapshot: base.from_version,
                to_snapshot: base.to_version,
            })
            .collect(),
        definition_fingerprint: intent.definition_fingerprint().to_string(),
        rows,
    }
    .to_summary_properties()
    .map_err(CommitServiceError::invalid_input)
}

fn managed_provenance_matches(
    expected: &novarocks_spi::connector::ConnectorManagedPublicationIntent,
    actual: &super::MvProvenanceV1,
) -> bool {
    let technique = match expected.technique() {
        ConnectorManagedPublicationTechnique::Full => super::RefreshTechnique::Full,
        ConnectorManagedPublicationTechnique::Incremental => super::RefreshTechnique::Incremental,
    };
    let bases = expected
        .bases()
        .iter()
        .map(|base| super::ProvenanceBase {
            table_fqn: base.table.to_string(),
            uuid: base.uuid.to_string(),
            from_snapshot: base.from_version,
            to_snapshot: base.to_version,
        })
        .collect::<Vec<_>>();
    actual.provenance_version == super::MV_PROVENANCE_VERSION
        && actual.refresh_id == expected.refresh_id()
        && actual.mv_id == expected.materialization_id()
        && actual.token == expected.marker()
        && actual.technique == technique
        && actual.bases == bases
        && actual.definition_fingerprint == expected.definition_fingerprint()
}

fn operation_marker_from_snapshot(
    snapshot: &crate::iceberg::spec::Snapshot,
) -> Result<Option<IcebergWriteOperationMarkerV1>, ConnectorError> {
    let Some(raw) = snapshot
        .summary()
        .additional_properties
        .get(ICEBERG_WRITE_OPERATION_MARKER_PROPERTY)
    else {
        return Ok(None);
    };
    let marker: IcebergWriteOperationMarkerV1 = serde_json::from_str(raw)
        .map_err(|error| corrupt(format!("decode Iceberg write operation marker: {error}")))?;
    if marker.version != ICEBERG_WRITE_OPERATION_MARKER_VERSION {
        return Err(corrupt(format!(
            "Iceberg write operation marker has unsupported version {}",
            marker.version
        )));
    }
    novarocks_spi::connector::ConnectorInstanceId::parse(&marker.instance_id)
        .map_err(|error| corrupt(format!("invalid Iceberg marker instance ID: {error}")))?;
    if marker.target_ref.trim().is_empty() {
        return Err(corrupt(
            "Iceberg write operation marker has an empty target ref",
        ));
    }
    decode_marker_fixed::<16>(&marker.incarnation_base64, "incarnation")?;
    decode_marker_fixed::<16>(&marker.operation_id_base64, "operation id")?;
    decode_marker_fixed::<32>(&marker.cohort_set_digest_base64, "cohort set digest")?;
    decode_marker_fixed::<32>(&marker.aggregate_digest_base64, "aggregate digest")?;
    let canonical = serde_json::to_string(&marker)
        .map_err(|error| corrupt(format!("encode Iceberg write operation marker: {error}")))?;
    if canonical != *raw {
        return Err(corrupt(
            "Iceberg write operation marker is not canonically encoded",
        ));
    }
    Ok(Some(marker))
}

fn find_operation_marker_snapshot<'a>(
    snapshots: impl IntoIterator<Item = &'a crate::iceberg::spec::Snapshot>,
    expected: &IcebergWriteOperationMarkerV1,
) -> Result<Option<&'a crate::iceberg::spec::Snapshot>, ConnectorError> {
    let mut matched = None;
    for snapshot in snapshots {
        let Some(marker) = operation_marker_from_snapshot(snapshot)? else {
            continue;
        };
        if marker == *expected && matched.replace(snapshot).is_some() {
            return Err(corrupt(
                "Iceberg write operation marker matches multiple snapshots",
            ));
        }
    }
    Ok(matched)
}

fn table_snapshot_row_count(
    runtime: &Arc<IcebergControlRuntime>,
    target: &ActiveTarget,
    snapshot_id: i64,
) -> Result<Option<u64>, CommitServiceError> {
    let ident =
        crate::iceberg::TableIdent::from_strs([target.namespace.as_str(), target.table.as_str()])
            .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
    let catalog = Arc::clone(runtime.catalog());
    let table = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.load_table(&ident).await })
        .map_err(|error| {
            CommitServiceError::finalize_failed_known_committed(
                Some(CommitOutcome {
                    new_snapshot_id: snapshot_id,
                    written_manifest_paths: Vec::new(),
                }),
                error,
                RecoveryEvidence {
                    table_ident: format!("{}.{}", target.namespace, target.table),
                    op_kind: CommitOpKind::FastAppend,
                    base_snapshot_id: target.base_snapshot_id,
                    base_sequence_number: 0,
                    staging_dir: target.location.clone(),
                },
            )
        })?
        .map_err(|error| {
            CommitServiceError::finalize_failed_known_committed(
                Some(CommitOutcome {
                    new_snapshot_id: snapshot_id,
                    written_manifest_paths: Vec::new(),
                }),
                error.to_string(),
                RecoveryEvidence {
                    table_ident: format!("{}.{}", target.namespace, target.table),
                    op_kind: CommitOpKind::FastAppend,
                    base_snapshot_id: target.base_snapshot_id,
                    base_sequence_number: 0,
                    staging_dir: target.location.clone(),
                },
            )
        })?;
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| {
            CommitServiceError::finalize_failed_known_committed(
                Some(CommitOutcome {
                    new_snapshot_id: snapshot_id,
                    written_manifest_paths: Vec::new(),
                }),
                "committed Iceberg snapshot is absent during managed row-count projection"
                    .to_string(),
                RecoveryEvidence {
                    table_ident: format!("{}.{}", target.namespace, target.table),
                    op_kind: CommitOpKind::FastAppend,
                    base_snapshot_id: target.base_snapshot_id,
                    base_sequence_number: 0,
                    staging_dir: target.location.clone(),
                },
            )
        })?;
    let rows = snapshot
        .summary()
        .additional_properties
        .get("total-records")
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|error| CommitServiceError::invalid_input(error.to_string()))?;
    Ok(rows)
}

fn data_writer_payload(
    metadata: &crate::iceberg::spec::TableMetadata,
    target_snapshot_id: Option<i64>,
    row_lineage_data: bool,
) -> Result<Bytes, ConnectorError> {
    let partition_spec = metadata.default_partition_spec();
    let mut partition_source_column_names = Vec::with_capacity(partition_spec.fields().len());
    let mut partition_column_names = Vec::with_capacity(partition_spec.fields().len());
    let mut transform_exprs = Vec::with_capacity(partition_spec.fields().len());
    for field in partition_spec.fields() {
        let source = metadata
            .current_schema()
            .field_by_id(field.source_id)
            .ok_or_else(|| {
                corrupt(format!(
                    "Iceberg writer partition field {} has unknown source column {}",
                    field.name, field.source_id
                ))
            })?;
        partition_source_column_names.push(source.name.clone());
        partition_column_names.push(field.name.clone());
        transform_exprs.push(field.transform.to_string());
    }
    let mut fields = crate::schema_facts::iceberg_schema_def(metadata.current_schema()).fields;
    if row_lineage_data {
        fields.extend([
            IcebergSchemaFieldDef {
                field_id: ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                name: ICEBERG_ROW_ID_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
            IcebergSchemaFieldDef {
                field_id: ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                name: ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
        ]);
    }
    encode_write_handle(&IcebergWriteHandleInput {
        mode: IcebergWriteHandleMode::Data,
        table_location: metadata.location().to_string(),
        data_location: data_location(metadata),
        target_partition_spec_id: metadata.default_partition_spec_id(),
        target_snapshot_id,
        file_format: IcebergFileFormat::Parquet,
        report_file_format: "parquet".to_string(),
        compression: Compression::SNAPPY,
        equality_delete_columns: Vec::new(),
        row_lineage_data,
        partition_source_column_names,
        partition_column_names,
        transform_exprs,
        data_input_schema: Some(crate::scan_model::IcebergSchemaDef { fields }),
        position_delete_binding: None,
        position_delete_partitions: Vec::new(),
    })
    .map_err(|error| internal(format!("encode Iceberg data writer handle: {error}")))
}

fn equality_delete_writer_payload(
    metadata: &crate::iceberg::spec::TableMetadata,
    target_snapshot_id: Option<i64>,
    fields: &[novarocks_spi::connector::ConnectorWriteFieldBinding],
) -> Result<Bytes, ConnectorError> {
    if fields.is_empty() {
        return Err(invalid(
            "Iceberg equality-delete write requires at least one equality field",
        ));
    }
    if !metadata.default_partition_spec().is_unpartitioned() {
        return Err(unsupported(
            "Iceberg equality-delete writer supports only unpartitioned tables",
        ));
    }
    let schema = metadata.current_schema();
    let equality_delete_columns = fields
        .iter()
        .map(|binding| {
            let field = binding.field();
            let iceberg_field = schema
                .as_struct()
                .fields()
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(field.name()))
                .ok_or_else(|| {
                    invalid(format!(
                        "Iceberg equality-delete field `{}` is absent from the frozen schema",
                        field.name()
                    ))
                })?;
            Ok(EqualityDeleteColumn {
                name: field.name().to_string(),
                field_id: iceberg_field.id,
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    encode_write_handle(&IcebergWriteHandleInput {
        mode: IcebergWriteHandleMode::EqualityDeletes,
        table_location: metadata.location().to_string(),
        data_location: data_location(metadata),
        target_partition_spec_id: metadata.default_partition_spec_id(),
        target_snapshot_id,
        file_format: IcebergFileFormat::Parquet,
        report_file_format: "parquet".to_string(),
        compression: Compression::SNAPPY,
        equality_delete_columns,
        row_lineage_data: false,
        partition_source_column_names: Vec::new(),
        partition_column_names: Vec::new(),
        transform_exprs: Vec::new(),
        data_input_schema: None,
        position_delete_binding: None,
        position_delete_partitions: Vec::new(),
    })
    .map_err(|error| {
        internal(format!(
            "encode Iceberg equality-delete writer handle: {error}"
        ))
    })
}

fn validate_position_delete_shape(
    metadata: &crate::iceberg::spec::TableMetadata,
    identity_fields: &[novarocks_spi::connector::ConnectorWriteFieldBinding],
    partition_source_fields: &[novarocks_spi::connector::ConnectorWriteFieldBinding],
) -> Result<(), ConnectorError> {
    use arrow::datatypes::DataType;

    if identity_fields.len() != 2 {
        return Err(invalid(
            "Iceberg row-level write requires exactly file-path and position identity fields",
        ));
    }
    let file = identity_fields[0].field();
    let position = identity_fields[1].field();
    if !file.name().eq_ignore_ascii_case("_file")
        || file.data_type() != &DataType::Utf8
        || file.is_nullable()
        || !position.name().eq_ignore_ascii_case("_pos")
        || position.data_type() != &DataType::Int64
        || position.is_nullable()
    {
        return Err(invalid(
            "Iceberg row-level write identity must be non-null `_file` UTF-8 followed by non-null `_pos` INT64",
        ));
    }
    let partition_fields = metadata.default_partition_spec().fields();
    if partition_source_fields.len() != partition_fields.len() {
        return Err(invalid(format!(
            "Iceberg row-level write has {} partition source fields but the frozen table requires {}",
            partition_source_fields.len(),
            partition_fields.len()
        )));
    }
    for (partition, binding) in partition_fields.iter().zip(partition_source_fields) {
        let source = metadata
            .current_schema()
            .field_by_id(partition.source_id)
            .ok_or_else(|| {
                corrupt(format!(
                    "Iceberg partition field {} references unknown source column {}",
                    partition.name, partition.source_id
                ))
            })?;
        if !binding.field().name().eq_ignore_ascii_case(&source.name) {
            return Err(invalid(format!(
                "Iceberg row-level partition source `{}` does not match frozen source `{}`",
                binding.field().name(),
                source.name
            )));
        }
    }
    Ok(())
}

fn data_location(metadata: &crate::iceberg::spec::TableMetadata) -> String {
    metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", metadata.location().trim_end_matches('/')))
}

fn cleanup_finalization(cleanup: &super::CleanupAttempt) -> ExternalMutationFinalization {
    if cleanup.error_count == 0 {
        ExternalMutationFinalization::Complete
    } else {
        ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Internal,
            format!(
                "Iceberg staged cleanup attempted={} and completed with {} error(s): {}",
                cleanup.attempted,
                cleanup.error_count,
                cleanup.error_paths.join(", ")
            ),
        ))
    }
}

fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn ensure_terminal_digest(
    cohort_set_digest: [u8; 32],
    aggregate_digest: [u8; 32],
    expected_cohort_set_digest: [u8; 32],
    expected_aggregate_digest: [u8; 32],
) -> Result<(), ConnectorError> {
    if cohort_set_digest != expected_cohort_set_digest
        || aggregate_digest != expected_aggregate_digest
    {
        return Err(invalid(
            "Iceberg terminal write replay uses a different sealed aggregate",
        ));
    }
    Ok(())
}

fn planning_attempt_digest(request: &ConnectorWritePlanningRequest) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg-write-attempt-plan.v1\0");
    hasher.update(request.operation_id.to_bytes());
    hasher.update(request.cohort_id.to_bytes());
    hasher.update(request.execution_id.query_id());
    hasher.update(request.execution_id.attempt_id().to_be_bytes());
    for writer in request.expected_writers.iter().collect::<BTreeSet<_>>() {
        hasher.update(writer.operation_id().to_bytes());
        hasher.update(writer.cohort_id().to_bytes());
        hasher.update(writer.execution_id().query_id());
        hasher.update(writer.execution_id().attempt_id().to_be_bytes());
        hasher.update(writer.fragment_instance_id());
        hasher.update(writer.fragment_id().to_be_bytes());
        hasher.update(writer.backend_num().to_be_bytes());
        hasher.update(writer.sink_ordinal().to_be_bytes());
        hasher.update(writer.binding_key().instance_id.as_str().as_bytes());
        hasher.update(writer.binding_key().incarnation.to_bytes());
    }
    hasher.finalize().into()
}

fn routed_writer_payloads_for_manifest(
    default_payload: &Bytes,
    routed_payloads: Option<&[Bytes]>,
    writers: &[novarocks_spi::connector::ConnectorWriterIdentity],
) -> Result<Vec<Bytes>, ConnectorError> {
    let Some(routed_payloads) = routed_payloads else {
        return Ok(vec![default_payload.clone(); writers.len()]);
    };
    let fragments = writers
        .iter()
        .map(|writer| writer.fragment_id())
        .collect::<BTreeSet<_>>();
    if fragments.len() != routed_payloads.len() {
        return Err(invalid(format!(
            "managed Iceberg routed write expected {} terminal fragments, observed {}",
            routed_payloads.len(),
            fragments.len()
        )));
    }
    let by_fragment = fragments
        .into_iter()
        .zip(routed_payloads.iter().cloned())
        .collect::<BTreeMap<_, _>>();
    writers
        .iter()
        .map(|writer| {
            by_fragment
                .get(&writer.fragment_id())
                .cloned()
                .ok_or_else(|| corrupt("managed Iceberg routed write lost a fragment payload"))
        })
        .collect()
}

fn canonical_json<T: Serialize>(value: &T, subject: &str) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| internal(format!("encode {subject}: {error}")))
}

fn base64_encode(bytes: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decode_fixed<const N: usize>(value: &str, subject: &str) -> Result<[u8; N], ConnectorError> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| invalid(format!("decode Iceberg write {subject}: {error}")))?
        .try_into()
        .map_err(|_| invalid(format!("Iceberg write {subject} has invalid length")))
}

fn decode_marker_fixed<const N: usize>(
    value: &str,
    subject: &str,
) -> Result<[u8; N], ConnectorError> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| corrupt(format!("decode Iceberg marker {subject}: {error}")))?
        .try_into()
        .map_err(|_| corrupt(format!("Iceberg marker {subject} has invalid length")))
}

fn operation_lock_error<T>(error: std::sync::PoisonError<T>) -> ConnectorError {
    internal(format!("Iceberg write operation table lock: {error}"))
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

fn not_found(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message).with_retryable_before_progress()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field};
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        CONNECTOR_WRITE_CONTRACT_VERSION, ConnectorCancellation, ConnectorInstanceId,
        ConnectorManagedPublicationIntent, ConnectorProviderId, ConnectorSealedWriteCohortSet,
        ConnectorStagedPublicationBaseFact, ConnectorStagedReport, ConnectorStagedReportSummary,
        ConnectorTableHandle, ConnectorWriteAttemptCompletion, ConnectorWriteBaseVersion,
        ConnectorWriteCohortCompletion, ConnectorWriteCohortDescriptor, ConnectorWriteFieldBinding,
        ConnectorWriteFieldToken, ConnectorWriteIntent, ConnectorWriteOperationCompletion,
        ConnectorWritePreparation, ConnectorWriteTargetRef, ConnectorWriterIdentity,
        ConnectorWriterTerminalState,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::control_provider::IcebergTablePayload;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use crate::iceberg::{NamespaceIdent, TableCreation};
    use crate::resources::IcebergControlResources;
    use crate::scan_model::IcebergTableInfo;

    use super::*;

    #[derive(Default)]
    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("context")
    }

    fn control() -> (tokio::runtime::Runtime, IcebergWriteControl) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let resources = IcebergControlResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let control = IcebergWriteControl::new(
            descriptor,
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
            runtime,
        );
        (executor, control)
    }

    fn control_with_empty_table() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergWriteControl,
        crate::iceberg::table::Table,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let resources = IcebergControlResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let control = IcebergWriteControl::new(
            descriptor,
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
            Arc::clone(&runtime),
        );
        let catalog = Arc::clone(runtime.catalog());
        let table = executor.block_on(async move {
            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("create namespace");
            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("schema");
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("t".to_string())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .build(),
                )
                .await
                .expect("create table")
        });
        (executor, warehouse, control, table)
    }

    fn preparation(owner: &ConnectorExecutionBindingKey, marker: u8) -> ConnectorWritePreparation {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema.clone(),
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        preparation_for_metadata(owner, &metadata, ConnectorWriteIntent::Append, marker)
    }

    fn preparation_for_metadata(
        owner: &ConnectorExecutionBindingKey,
        metadata: &crate::iceberg::spec::TableMetadata,
        intent: ConnectorWriteIntent,
        marker: u8,
    ) -> ConnectorWritePreparation {
        let schema = metadata.current_schema();
        let table_info = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: Some(metadata.uuid().to_string()),
            current_snapshot_id: metadata
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            schema_id: metadata.current_schema_id(),
            location: metadata.location().to_string(),
            schema: crate::schema_facts::iceberg_schema_def(schema),
            serialized_metadata: Some(serde_json::to_string(&metadata).expect("metadata JSON")),
            serialized_metadata_rows: None,
        };
        let payload = IcebergTablePayload {
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_info: Some(table_info),
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
        };
        ConnectorWritePreparation::try_new(
            owner.clone(),
            ConnectorTableHandle::try_new(
                owner.instance_id.clone(),
                Bytes::from(serde_json::to_vec(&payload).expect("table payload")),
            )
            .expect("table handle"),
            ConnectorWriteTargetRef::main(),
            intent,
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).expect("base"),
            ConnectorWriteInputShape::Data {
                fields: vec![ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([marker; 32]),
                    Field::new("value", DataType::Int64, true),
                )],
            },
            Bytes::from(vec![marker]),
        )
        .expect("preparation")
    }

    fn activation_request(
        owner: &ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        marker: u8,
    ) -> ConnectorWriteActivationRequest {
        ConnectorWriteActivationRequest {
            operation_id,
            source: novarocks_spi::connector::ConnectorWriteActivationSource::Prepared(
                preparation(owner, marker),
            ),
            intent: ConnectorWriteActivationIntent::Ordinary,
            context: context(),
        }
    }

    fn snapshot_with_operation_marker(snapshot_id: i64, marker: String) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list(format!("file:/tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: [(
                    ICEBERG_WRITE_OPERATION_MARKER_PROPERTY.to_string(),
                    marker,
                )]
                .into_iter()
                .collect(),
            })
            .with_schema_id(0)
            .build()
    }

    #[test]
    fn activation_and_planning_are_generation_local_and_idempotent() {
        let (_executor, control) = control();
        let owner = control.binding_key().clone();
        let operation_id = ConnectorWriteOperationId::new();
        let activation_request = activation_request(&owner, operation_id, 1);
        let first = control
            .activate_write(activation_request.clone())
            .expect("activate");
        let replay = control
            .activate_write(activation_request)
            .expect("idempotent activation");
        assert_eq!(first.digest(), replay.digest());

        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let execution_id = ConnectorWriteExecutionId::new([9; 16], 3);
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            cohort_id,
            execution_id,
            [4; 16],
            5,
            6,
            0,
            owner.clone(),
        );
        let request = ConnectorWritePlanningRequest {
            operation_id,
            cohort_id,
            execution_id,
            activation: first.cohort(cohort_id).expect("cohort"),
            expected_writers: vec![writer],
            context: context(),
        };
        let plan = control.plan_write(request.clone()).expect("plan");
        let replayed = control.plan_write(request).expect("replay plan");
        assert_eq!(plan, replayed);
        assert_eq!(plan.handles().len(), 1);
        assert_eq!(
            crate::write_codec::decode_write_handle(plan.handles()[0].payload())
                .expect("writer handle")
                .mode,
            IcebergWriteHandleMode::Data
        );
        assert_eq!(
            IcebergWritePlanPayloadV1::decode(plan.control_payload())
                .expect("control payload")
                .target,
            "ice.db.t"
        );
    }

    #[test]
    fn routed_payloads_bind_to_sorted_terminal_fragments() {
        let (_executor, control) = control();
        let owner = control.binding_key().clone();
        let operation_id = ConnectorWriteOperationId::new();
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let execution_id = ConnectorWriteExecutionId::new([7; 16], 1);
        let writer = |fragment_id, backend_num, marker| {
            ConnectorWriterIdentity::new(
                operation_id,
                cohort_id,
                execution_id,
                [marker; 16],
                fragment_id,
                backend_num,
                0,
                owner.clone(),
            )
        };
        let writers = vec![writer(9, 0, 1), writer(3, 0, 2), writer(3, 1, 3)];
        let payloads = routed_writer_payloads_for_manifest(
            &Bytes::from_static(b"unused"),
            Some(&[Bytes::from_static(b"delete"), Bytes::from_static(b"data")]),
            &writers,
        )
        .expect("route payloads");
        assert_eq!(
            payloads,
            vec![
                Bytes::from_static(b"data"),
                Bytes::from_static(b"delete"),
                Bytes::from_static(b"delete"),
            ]
        );
    }

    #[test]
    fn staged_completion_is_exact_and_known_terminal_releases_reservation() {
        let (_executor, control) = control();
        let owner = control.binding_key().clone();
        let operation_id = ConnectorWriteOperationId::new();
        let activation = control
            .activate_write(activation_request(&owner, operation_id, 1))
            .expect("activate");
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let execution_id = ConnectorWriteExecutionId::new([3; 16], 1);
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            cohort_id,
            execution_id,
            [4; 16],
            5,
            6,
            0,
            owner.clone(),
        );
        let planning = ConnectorWritePlanningRequest {
            operation_id,
            cohort_id,
            execution_id,
            activation: activation.cohort(cohort_id).expect("cohort"),
            expected_writers: vec![writer.clone()],
            context: context(),
        };
        let planning_digest = planning.stable_digest(&owner).expect("planning digest");
        let plan = control.plan_write(planning).expect("plan");
        let report = ConnectorStagedReport::try_new(
            writer,
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from_static(b"opaque-staged-create-report"),
        )
        .expect("report");
        let attempt = ConnectorWriteAttemptCompletion::try_new(
            owner.clone(),
            operation_id,
            cohort_id,
            execution_id,
            [8; 32],
            vec![report],
            plan.control_payload().clone(),
        )
        .expect("attempt");
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![ConnectorWriteCohortDescriptor::new(
                cohort_id,
                ConnectorWriteIntent::Append,
                planning_digest,
            )],
        )
        .expect("sealed");
        let completion = ConnectorWriteOperationCompletion::try_new(
            owner.clone(),
            sealed,
            vec![
                ConnectorWriteCohortCompletion::try_new(cohort_id, Some(attempt), Vec::new())
                    .expect("cohort completion"),
            ],
        )
        .expect("completion");
        control
            .validate_staged_completion(&completion)
            .expect("exact staged completion");
        control
            .finish_staged_terminal(operation_id)
            .expect("known terminal");
        assert_eq!(
            control
                .validate_staged_completion(&completion)
                .expect_err("released operation")
                .kind(),
            ConnectorErrorKind::NotFound
        );
        control
            .activate_write(activation_request(&owner, operation_id, 1))
            .expect("released reservation can be reused");
    }

    #[test]
    fn known_empty_abort_releases_reservation_and_is_idempotent() {
        let (_executor, control) = control();
        let owner = control.binding_key().clone();
        let operation_id = ConnectorWriteOperationId::new();
        let activation = control
            .activate_write(activation_request(&owner, operation_id, 1))
            .expect("activate");
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let stable_digest = ConnectorWritePlanningRequest {
            operation_id,
            cohort_id,
            execution_id: ConnectorWriteExecutionId::new([1; 16], 0),
            activation: activation.cohort(cohort_id).expect("cohort"),
            expected_writers: Vec::new(),
            context: context(),
        }
        .stable_digest(&owner)
        .expect("stable digest");
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![ConnectorWriteCohortDescriptor::new(
                cohort_id,
                ConnectorWriteIntent::Append,
                stable_digest,
            )],
        )
        .expect("sealed");
        let request =
            ConnectorWriteAbortRequest::try_new(owner.clone(), sealed, Vec::new(), context())
                .expect("abort request");
        let first = control.abort(request.clone()).expect("abort");
        let replay = control.abort(request).expect("abort replay");
        assert_eq!(first, replay);

        let conflicting = activation_request(&owner, operation_id, 2);
        control
            .activations
            .activate(&owner, &conflicting)
            .expect("reservation was released");
        control
            .activations
            .release(operation_id)
            .expect("release probe reservation");
    }

    #[test]
    fn operation_marker_is_canonical_and_binds_exact_generation_and_aggregate() {
        let (_executor, control) = control();
        let owner = control.binding_key().clone();
        let operation_id = ConnectorWriteOperationId::from_bytes([3; 16]);
        control
            .activate_write(activation_request(&owner, operation_id, 1))
            .expect("activate");
        let active = {
            let operations = control.operations.lock().expect("operation table");
            let OperationState::Active(active) =
                operations.get(&operation_id).expect("active operation")
            else {
                panic!("expected active operation");
            };
            active.clone()
        };
        let marker = control.operation_marker(operation_id, &active, [4; 32], [5; 32]);
        let encoded = serde_json::to_string(&marker).expect("marker JSON");
        let snapshot = snapshot_with_operation_marker(8, encoded);
        assert_eq!(
            operation_marker_from_snapshot(&snapshot).expect("decode marker"),
            Some(marker)
        );
    }

    #[test]
    fn malformed_operation_marker_is_corrupt_data() {
        let snapshot = snapshot_with_operation_marker(8, "{\"version\":1}".to_string());
        let error = operation_marker_from_snapshot(&snapshot).expect_err("corrupt marker");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn duplicate_operation_marker_matches_are_corrupt_data() {
        let (_executor, control) = control();
        let owner = control.binding_key().clone();
        let operation_id = ConnectorWriteOperationId::from_bytes([3; 16]);
        control
            .activate_write(activation_request(&owner, operation_id, 1))
            .expect("activate");
        let active = {
            let operations = control.operations.lock().expect("operation table");
            let OperationState::Active(active) =
                operations.get(&operation_id).expect("active operation")
            else {
                panic!("expected active operation");
            };
            active.clone()
        };
        let marker = control.operation_marker(operation_id, &active, [4; 32], [5; 32]);
        let raw = serde_json::to_string(&marker).expect("marker JSON");
        let first = snapshot_with_operation_marker(8, raw.clone());
        let second = snapshot_with_operation_marker(9, raw);
        let error = find_operation_marker_snapshot([&first, &second], &marker)
            .expect_err("duplicate marker");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn terminal_tombstones_are_bounded_without_evicting_active_operations() {
        let (_executor, control) = control();
        let owner = control.binding_key().clone();
        let active_operation_id = ConnectorWriteOperationId::from_bytes([9; 16]);
        control
            .activate_write(activation_request(&owner, active_operation_id, 1))
            .expect("activate");
        let first_terminal_id = ConnectorWriteOperationId::from_bytes(0_u128.to_be_bytes());
        let last_terminal_id = ConnectorWriteOperationId::from_bytes(
            (MAX_ICEBERG_WRITE_TERMINAL_TOMBSTONES as u128).to_be_bytes(),
        );
        let mut operations = control.operations.lock().expect("operation table");
        for ordinal in 0..=MAX_ICEBERG_WRITE_TERMINAL_TOMBSTONES {
            operations.insert(
                ConnectorWriteOperationId::from_bytes((ordinal as u128).to_be_bytes()),
                OperationState::KnownUncommitted(KnownUncommittedOperation {
                    cohort_set_digest: [1; 32],
                    aggregate_digest: [2; 32],
                    outcome: ConnectorWriteAbortOutcome::KnownUncommitted {
                        cleanup: ExternalMutationFinalization::Complete,
                    },
                }),
            );
        }
        assert!(operations.get(&first_terminal_id).is_none());
        assert!(operations.get(&last_terminal_id).is_some());
        assert!(matches!(
            operations.get(&active_operation_id),
            Some(OperationState::Active(_))
        ));
        assert_eq!(
            operations.terminal_order.len(),
            MAX_ICEBERG_WRITE_TERMINAL_TOMBSTONES
        );
    }

    #[test]
    fn cleanup_failure_finalization_preserves_error_paths() {
        let finalization = cleanup_finalization(&crate::commit::CleanupAttempt::completed(vec![
            "data/staged-a.parquet".to_string(),
            "metadata/staged-b.avro".to_string(),
        ]));
        let ExternalMutationFinalization::Failed(failure) = finalization else {
            panic!("expected failed cleanup finalization");
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Internal);
        assert!(failure.message().contains("2 error(s)"));
        assert!(failure.message().contains("data/staged-a.parquet"));
        assert!(failure.message().contains("metadata/staged-b.avro"));
    }

    #[test]
    fn managed_empty_overwrite_commits_real_snapshot_with_operation_marker() {
        let (_executor, _warehouse, control, table) = control_with_empty_table();
        let owner = control.binding_key().clone();
        let operation_id = ConnectorWriteOperationId::from_bytes([12; 16]);
        let managed = ConnectorManagedPublicationIntent::try_new(
            41,
            7,
            "refresh-41",
            ConnectorManagedPublicationTechnique::Full,
            vec![ConnectorStagedPublicationBaseFact {
                table: Arc::from("ice.db.base"),
                uuid: Arc::from("base-uuid"),
                from_version: None,
                to_version: 1,
            }],
            "definition-fingerprint",
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        )
        .expect("managed intent");
        let activation = control
            .activate_write(ConnectorWriteActivationRequest {
                operation_id,
                source: ConnectorWriteActivationSource::Prepared(preparation_for_metadata(
                    &owner,
                    table.metadata(),
                    ConnectorWriteIntent::Overwrite,
                    6,
                )),
                intent: ConnectorWriteActivationIntent::ManagedPublication(managed),
                context: context(),
            })
            .expect("activate");
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let execution_id = ConnectorWriteExecutionId::new([13; 16], 1);
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            cohort_id,
            execution_id,
            [14; 16],
            1,
            2,
            0,
            owner.clone(),
        );
        let planning = ConnectorWritePlanningRequest {
            operation_id,
            cohort_id,
            execution_id,
            activation: activation.cohort(cohort_id).expect("cohort"),
            expected_writers: vec![writer.clone()],
            context: context(),
        };
        let planning_digest = planning.stable_digest(&owner).expect("planning digest");
        let plan = control.plan_write(planning).expect("plan write");
        let report = ConnectorStagedReport::try_new(
            writer,
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            crate::write_codec::encode_writer_reports(&[], table.metadata())
                .expect("empty report payload"),
        )
        .expect("staged report");
        let accepted = ConnectorWriteAttemptCompletion::try_new(
            owner.clone(),
            operation_id,
            cohort_id,
            execution_id,
            [15; 32],
            vec![report],
            plan.control_payload().clone(),
        )
        .expect("attempt completion");
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![ConnectorWriteCohortDescriptor::new(
                cohort_id,
                ConnectorWriteIntent::Overwrite,
                planning_digest,
            )],
        )
        .expect("sealed cohorts");
        let cohort_set_digest = sealed.digest();
        let completion = ConnectorWriteOperationCompletion::try_new(
            owner.clone(),
            sealed,
            vec![
                ConnectorWriteCohortCompletion::try_new(cohort_id, Some(accepted), Vec::new())
                    .expect("cohort completion"),
            ],
        )
        .expect("operation completion");
        let aggregate_digest = completion.aggregate_digest();
        let outcome = control
            .commit(ConnectorWriteCommitRequest {
                completion,
                context: context(),
            })
            .expect("commit empty overwrite");
        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
            panic!("expected known committed outcome");
        };
        let snapshot_id = receipt
            .committed_version()
            .expect("committed version")
            .snapshot_id()
            .expect("snapshot id");
        let loaded = control
            .runtime
            .load_table("db", "t")
            .expect("reload committed table");
        let snapshot = loaded
            .table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .expect("committed snapshot");
        let marker = operation_marker_from_snapshot(snapshot)
            .expect("decode operation marker")
            .expect("operation marker");
        assert_eq!(
            decode_marker_fixed::<16>(&marker.operation_id_base64, "operation id")
                .expect("operation id"),
            operation_id.to_bytes()
        );
        assert_eq!(
            decode_marker_fixed::<32>(&marker.cohort_set_digest_base64, "cohort digest")
                .expect("cohort digest"),
            cohort_set_digest
        );
        assert_eq!(
            decode_marker_fixed::<32>(&marker.aggregate_digest_base64, "aggregate digest")
                .expect("aggregate digest"),
            aggregate_digest
        );
        assert!(
            crate::commit::MvProvenanceV1::from_snapshot_summary(snapshot)
                .expect("decode provenance")
                .is_some()
        );
        let conflicting = activation_request(&owner, operation_id, 99);
        control
            .activations
            .activate(&owner, &conflicting)
            .expect("known terminal released activation reservation");
        control
            .activations
            .release(operation_id)
            .expect("release probe reservation");
    }
}
