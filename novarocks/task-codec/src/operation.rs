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

//! Operation, batch, and receipt codec.
//!
//! Every operation carries its own envelope and produces its own receipt. A
//! batch is a transport convenience and nothing more: it gives its items no
//! atomicity, no shared revision, and no batch-global success, and this codec
//! never collapses their outcomes.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use novarocks_execution_contract::task_execution::context_convergence::{
    QueryContextConvergenceCursor, QueryContextConvergenceReceipt,
};
use novarocks_execution_contract::task_execution::creation::{FrozenBytes, TaskCreationInput};
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::domain::{
    CodecOwnedContent, CredentialEpoch, CredentialLeaseId, DomainProgression, DomainVersion,
    EdgeOpenVersion, ExchangeEdgeId,
};
use novarocks_execution_contract::task_execution::identity::{
    AdmissionTicketId, QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::lease::LeaseValidFor;
use novarocks_execution_contract::task_execution::operation::{
    AbortQueryContext, AcquireQueryContextAdmissionTicket, AdvanceQueryContextDomain, CancelTask,
    CreateTask, CreateTaskReceipt, EstablishQueryContext, FetchTaskDynamicFilters,
    GetFinalTaskInfo, MaxWait, OperationEnvelope, OperationKind, OperationOutcome,
    QueryContextAdmissionTicketReceipt, QueryContextDomainReceipt, QueryContextReceipt,
    QuiesceQueryContext, QuiesceQueryContextReceipt, ReleaseOutcome, ReleaseQueryContext,
    RenewQueryExecutionLease, ResultByteLimit, ResultPacketSequence, TaskDomainReceipt,
    UpdateQueryContext, UpdateTask, UpdateTaskReceipt,
};
use novarocks_execution_contract::task_execution::status::{TaskStatus, TaskStatusCursor};
use novarocks_execution_contract::task_execution::task_convergence::{
    TaskConvergenceCursor, TaskConvergenceReceipt, TaskConvergenceVersion,
};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_proto_models::novarocks;
use novarocks_types::NativeCompatibilityId;
use prost::Message;

use crate::creation::decode_task_assignment;
use crate::descriptor::decode_task_descriptor;
use crate::domain::{
    DecodedQueryContextDomain, DecodedTaskDomain, MAX_DOMAIN_UPDATES, WireContent,
    decode_credential_domain, decode_plan_node_split_receipt, decode_query_context_domain,
    decode_task_domain,
};
use crate::resource_preflight::{check_creation_metadata, check_frozen_fragment};

/// Domain separation tags for the shared facts an establish installs.
/// They are distinct from the tags an advance uses, because the same content
/// arriving as an install and as a rotation is not the same operation.
pub const ESTABLISH_CATALOG_DOMAIN_TAG: &[u8] =
    b"novarocks.task_execution.establish.catalog_binding.v1";
pub const ESTABLISH_FILTER_DOMAIN_TAG: &[u8] =
    b"novarocks.task_execution.establish.initial_runtime_filter.v1";
pub const ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG: &[u8] =
    b"novarocks.task_execution.establish.query_options.v1";

/// Process-wide decoded-message ceiling configured on the Native FE client.
pub const NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Largest root-result payload the current Native gRPC envelope can carry.
///
/// Both Native client and server configure a 64 MiB decoded-message ceiling.
/// The payload stays below it by a fixed envelope reserve so protobuf tags,
/// lengths, sequence and status fields cannot turn a legal payload into an
/// oversized decoded message.
pub const MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES: u64 =
    (NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES - 1024) as u64;
use crate::identity::{
    decode_admission_ticket_id, decode_query_context_ref, decode_task_identity,
    decode_task_operation_id, encode_admission_ticket_id, encode_query_context_ref,
    encode_task_identity, encode_task_operation_id,
};
use crate::lease::decode_duration_millis;
use crate::status::{
    decode_abort_cause, decode_cancel_reason, encode_abort_cause, encode_cancel_reason,
};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_proto_codec::{FieldPath, ProtocolError};

use crate::{TransportBudget, invalid, invalid_enum, missing, out_of_range};

/// Decodes an operation envelope.
pub fn decode_envelope(
    src: &novarocks::TaskOperationEnvelope,
    kind: OperationKind,
    path: FieldPath,
) -> Result<OperationEnvelope, ProtocolError> {
    let operation_id = src.operation_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("operation_id"),
            "operation envelope requires an operation id",
        )
    })?;
    let operation_id = decode_task_operation_id(operation_id, path.clone().field("operation_id"))?;
    let max_wait = decode_duration_millis(
        src.max_wait_millis,
        path.clone().field("max_wait_millis"),
        MaxWait::MAX_REPRESENTABLE,
    )?;
    let max_wait = MaxWait::new(max_wait)
        .map_err(|error| invalid(path.field("max_wait_millis"), error.to_string()))?;
    Ok(OperationEnvelope::new(operation_id, kind, max_wait))
}

pub fn encode_envelope(value: OperationEnvelope) -> novarocks::TaskOperationEnvelope {
    novarocks::TaskOperationEnvelope {
        operation_id: Some(encode_task_operation_id(value.operation_id())),
        max_wait_millis: value.max_wait().get().as_millis() as u64,
    }
}

/// A decoded create request.
///
/// The neutral request carries everything the lifecycle owner decides on:
/// the exact context, the descriptor and the initial domains. The creation
/// input is the short-lived remainder, moved to whichever backend owner wins
/// this identity's creation and otherwise dropped unread.
pub struct DecodedCreateTask {
    request: CreateTask,
    input: TaskCreationInput,
    initial_domains: Vec<DecodedTaskDomain>,
    retained_bytes: usize,
}

impl DecodedCreateTask {
    pub const fn request(&self) -> &CreateTask {
        &self.request
    }

    pub const fn descriptor(&self) -> &TaskDescriptor {
        self.request.descriptor()
    }

    /// The creation input, for the owner that will move it to its winner.
    pub const fn input(&self) -> &TaskCreationInput {
        &self.input
    }

    pub fn initial_domains(&self) -> &[DecodedTaskDomain] {
        &self.initial_domains
    }

    /// The neutral request and the creation input it travels with.
    pub fn into_parts(self) -> (CreateTask, TaskCreationInput) {
        (self.request, self.input)
    }

    /// The exact encoded Create item size, before ownership moves to Worker.
    pub fn into_parts_with_retained_bytes(self) -> (CreateTask, TaskCreationInput, usize) {
        (self.request, self.input, self.retained_bytes)
    }
}

/// Checks the private canonical carrier before its codec owner allocates it.
fn check_canonical_assignment_size(
    assignment: &novarocks::TaskAssignment,
    frozen_bytes: usize,
    budget: TransportBudget,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if frozen_bytes
        .checked_add(assignment.encoded_len())
        .is_none_or(|total| total > budget.max_descriptor_encoded_bytes())
    {
        return Err(out_of_range(
            path,
            "canonical creation plan carriers exceed the hard limit",
        ));
    }
    Ok(())
}

/// A decoded update request, with its typed domain content retained.
pub struct DecodedUpdateTask {
    request: UpdateTask,
    domains: Vec<DecodedTaskDomain>,
}

impl DecodedUpdateTask {
    pub const fn request(&self) -> &UpdateTask {
        &self.request
    }

    pub fn domains(&self) -> &[DecodedTaskDomain] {
        &self.domains
    }
}

/// A decoded establish request.
pub struct DecodedEstablishQueryContext {
    context: QueryContextRef,
    envelope: OperationEnvelope,
    admission_ticket_id: AdmissionTicketId,
    catalog_set: Arc<WireContent<novarocks_proto_models::catalog::CatalogSet>>,
    initial_runtime_filter: Arc<WireContent<novarocks::RuntimeFilterContribution>>,
    query_options: Arc<WireContent<novarocks::QueryOptions>>,
    initial_credential: DecodedQueryContextDomain,
    initial_lease_valid_for: LeaseValidFor,
    native_compatibility_id: NativeCompatibilityId,
}

impl DecodedEstablishQueryContext {
    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn envelope(&self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn admission_ticket_id(&self) -> AdmissionTicketId {
        self.admission_ticket_id
    }

    /// The catalog binding, for the backend's shared-fact owner.
    pub fn catalog_set(&self) -> &novarocks_proto_models::catalog::CatalogSet {
        self.catalog_set.wire()
    }

    /// The initial runtime filter, for the backend's shared-fact owner.
    pub fn initial_runtime_filter(&self) -> &novarocks::RuntimeFilterContribution {
        self.initial_runtime_filter.wire()
    }

    /// The exact query options frozen for this query context.
    pub fn query_options(&self) -> &novarocks::QueryOptions {
        self.query_options.wire()
    }

    /// The credential rotation, whose material stays behind its confidential
    /// handle.
    pub const fn credential(&self) -> &DecodedQueryContextDomain {
        &self.initial_credential
    }

    pub const fn initial_credential(&self) -> &DecodedQueryContextDomain {
        &self.initial_credential
    }

    pub const fn initial_lease_valid_for(&self) -> LeaseValidFor {
        self.initial_lease_valid_for
    }

    pub const fn native_compatibility_id(&self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }
}

/// A decoded query-context command.
pub enum DecodedUpdateQueryContext {
    Establish(DecodedEstablishQueryContext),
    AdvanceDomain {
        context: QueryContextRef,
        envelope: OperationEnvelope,
        domain: DecodedQueryContextDomain,
    },
    RenewLease {
        context: QueryContextRef,
        envelope: OperationEnvelope,
        sequence: novarocks_execution_contract::task_execution::lease::LeaseSequence,
        valid_for: LeaseValidFor,
    },
}

impl DecodedUpdateQueryContext {
    /// The neutral command the backend's own owner consumes.
    ///
    /// The typed content stays reachable through the decoded form; this is the
    /// projection that carries only what the neutral contract defines, which
    /// is what keeps the owner from ever naming a wire type.
    pub fn as_neutral(&self) -> Option<UpdateQueryContext> {
        match self {
            Self::Establish(request) => {
                let DecodedQueryContextDomain::Credential { update, .. } =
                    &request.initial_credential
                else {
                    // The decoder only ever builds this variant from a
                    // credential domain, so anything else is a codec bug
                    // rather than a wire condition.
                    return None;
                };
                Some(UpdateQueryContext::Establish(EstablishQueryContext::new(
                    request.envelope().operation_id(),
                    request.context(),
                    request.admission_ticket_id(),
                    Arc::clone(&request.catalog_set) as Arc<dyn CodecOwnedContent>,
                    Arc::clone(&request.initial_runtime_filter) as Arc<dyn CodecOwnedContent>,
                    Arc::clone(&request.query_options) as Arc<dyn CodecOwnedContent>,
                    update.clone(),
                    request.initial_lease_valid_for(),
                )))
            }
            Self::AdvanceDomain {
                context,
                envelope,
                domain,
            } => Some(UpdateQueryContext::AdvanceDomain(
                AdvanceQueryContextDomain::new(
                    envelope.operation_id(),
                    *context,
                    domain.as_neutral(),
                ),
            )),
            Self::RenewLease {
                context,
                envelope,
                sequence,
                valid_for,
            } => Some(UpdateQueryContext::RenewLease(
                RenewQueryExecutionLease::new(
                    envelope.operation_id(),
                    *context,
                    *sequence,
                    *valid_for,
                ),
            )),
        }
    }

    pub const fn context(&self) -> QueryContextRef {
        match self {
            Self::Establish(request) => request.context(),
            Self::AdvanceDomain { context, .. } | Self::RenewLease { context, .. } => *context,
        }
    }

    pub const fn envelope(&self) -> OperationEnvelope {
        match self {
            Self::Establish(request) => request.envelope(),
            Self::AdvanceDomain { envelope, .. } | Self::RenewLease { envelope, .. } => *envelope,
        }
    }

    /// Only an establish may create a context.
    pub const fn may_create(&self) -> bool {
        matches!(self, Self::Establish(_))
    }
}

/// One decoded operation from a batch.
pub enum DecodedOperation {
    AcquireQueryContextAdmissionTicket(AcquireQueryContextAdmissionTicket),
    CreateTask(DecodedCreateTask),
    UpdateTask(DecodedUpdateTask),
    UpdateQueryContext(DecodedUpdateQueryContext),
    CancelTask(CancelTask),
    AbortQueryContext(AbortQueryContext),
    QuiesceQueryContext(QuiesceQueryContext),
    ReleaseQueryContext(ReleaseQueryContext),
}

/// Renders only the operation kind and its envelope.
///
/// A decoded operation can transitively hold confidential credential
/// material, so this deliberately renders nothing but the shape.
impl fmt::Debug for DecodedOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedOperation")
            .field("kind", &self.kind())
            .field("operation_id", &self.envelope().operation_id())
            .finish()
    }
}

impl DecodedOperation {
    pub const fn envelope(&self) -> OperationEnvelope {
        match self {
            Self::AcquireQueryContextAdmissionTicket(request) => request.envelope(),
            Self::CreateTask(request) => request.request.envelope(),
            Self::UpdateTask(request) => request.request.envelope(),
            Self::UpdateQueryContext(request) => request.envelope(),
            Self::CancelTask(request) => request.envelope(),
            Self::AbortQueryContext(request) => request.envelope(),
            Self::QuiesceQueryContext(request) => request.envelope(),
            Self::ReleaseQueryContext(request) => request.envelope(),
        }
    }

    pub const fn kind(&self) -> OperationKind {
        self.envelope().kind()
    }
}

fn decode_task_domains(
    src: &[novarocks::TaskDomainUpdate],
    path: FieldPath,
) -> Result<Vec<DecodedTaskDomain>, ProtocolError> {
    if src.len() > MAX_DOMAIN_UPDATES {
        return Err(out_of_range(
            path,
            "domain update count exceeds the hard limit",
        ));
    }
    let mut domains = Vec::with_capacity(src.len());
    for (index, domain) in src.iter().enumerate() {
        domains.push(decode_task_domain(domain, path.clone().index(index))?);
    }
    Ok(domains)
}

/// Decodes one operation from a batch.
pub fn decode_operation(
    src: &novarocks::TaskOperation,
    path: FieldPath,
) -> Result<DecodedOperation, ProtocolError> {
    let envelope_src = src.envelope.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("envelope"),
            "an operation requires an envelope",
        )
    })?;
    let operation = src
        .operation
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "an operation requires a command"))?;

    match operation {
        novarocks::task_operation::Operation::AcquireQueryContextAdmissionTicket(acquire) => {
            let acquire_path = path.clone().field("acquire_query_context_admission_ticket");
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::AcquireQueryContextAdmissionTicket,
                path.field("envelope"),
            )?;
            let context = acquire.query_context.as_ref().ok_or_else(|| {
                missing(
                    acquire_path.clone().field("query_context"),
                    "admission ticket acquisition requires a query context reference",
                )
            })?;
            let context =
                decode_query_context_ref(context, acquire_path.clone().field("query_context"))?;
            let valid_for = decode_duration_millis(
                acquire.valid_for_millis,
                acquire_path.clone().field("valid_for_millis"),
                LeaseValidFor::MAX_REPRESENTABLE,
            )?;
            let valid_for = LeaseValidFor::new(valid_for).map_err(|error| {
                invalid(
                    acquire_path.clone().field("valid_for_millis"),
                    error.to_string(),
                )
            })?;
            let native_compatibility_id =
                acquire.native_compatibility_id.as_ref().ok_or_else(|| {
                    missing(
                        acquire_path.clone().field("native_compatibility_id"),
                        "admission ticket acquisition requires a native compatibility identity",
                    )
                })?;
            let native_compatibility_id = NativeCompatibilityId::try_from_slice(
                &native_compatibility_id.value,
            )
            .map_err(|error| {
                invalid(
                    acquire_path.field("native_compatibility_id").field("value"),
                    error.to_string(),
                )
            })?;
            let admission_epoch_capability =
                acquire.admission_epoch_capability.as_ref().ok_or_else(|| {
                    missing(
                        acquire_path.clone().field("admission_epoch_capability"),
                        "admission ticket acquisition requires an admission epoch capability",
                    )
                })?;
            let admission_epoch_capability = crate::identity::decode_admission_epoch_capability(
                admission_epoch_capability,
                acquire_path.field("admission_epoch_capability"),
            )?;
            Ok(DecodedOperation::AcquireQueryContextAdmissionTicket(
                AcquireQueryContextAdmissionTicket::new(
                    envelope.operation_id(),
                    context,
                    valid_for,
                    native_compatibility_id,
                    admission_epoch_capability,
                ),
            ))
        }
        novarocks::task_operation::Operation::CreateTask(create) => {
            let create_path = path.clone().field("create_task");
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::CreateTask,
                path.field("envelope"),
            )?;
            check_frozen_fragment(create.frozen_fragment.as_ref()).map_err(|error| {
                out_of_range(
                    create_path.clone().field("frozen_fragment"),
                    error.to_string(),
                )
            })?;
            check_creation_metadata(
                create.creation_metadata.as_ref(),
                create.frozen_fragment.len(),
            )
            .map_err(|error| {
                out_of_range(
                    create_path.clone().field("creation_metadata"),
                    error.to_string(),
                )
            })?;
            // The static fragment is bounded above but deliberately not
            // decoded here: only the backend that wins this identity's
            // creation interprets it.
            // Each retained item owns only its bounded fields. Prost Bytes can
            // otherwise keep a whole multi-item request allocation alive.
            let metadata = novarocks::CreationMetadata::decode(
                prost::bytes::Bytes::copy_from_slice(create.creation_metadata.as_ref()),
            )
            .map_err(|error| {
                invalid(
                    create_path.clone().field("creation_metadata"),
                    format!("invalid creation metadata: {error}"),
                )
            })?;
            let context = metadata.query_context.as_ref().ok_or_else(|| {
                missing(
                    create_path
                        .clone()
                        .field("creation_metadata")
                        .field("query_context"),
                    "create requires a query context reference",
                )
            })?;
            let context = decode_query_context_ref(
                context,
                create_path
                    .clone()
                    .field("creation_metadata")
                    .field("query_context"),
            )?;
            let descriptor = metadata.descriptor.as_ref().ok_or_else(|| {
                missing(
                    create_path
                        .clone()
                        .field("creation_metadata")
                        .field("descriptor"),
                    "create requires a descriptor",
                )
            })?;
            let descriptor = decode_task_descriptor(
                descriptor,
                create_path
                    .clone()
                    .field("creation_metadata")
                    .field("descriptor"),
            )?;
            let assignment = metadata.assignment.ok_or_else(|| {
                missing(
                    create_path
                        .clone()
                        .field("creation_metadata")
                        .field("assignment"),
                    "create requires a task assignment",
                )
            })?;
            check_canonical_assignment_size(
                &assignment,
                create.frozen_fragment.len(),
                TransportBudget::DEFAULT,
                create_path
                    .clone()
                    .field("creation_metadata")
                    .field("assignment"),
            )?;
            let assignment = decode_task_assignment(
                assignment,
                &descriptor,
                create_path
                    .clone()
                    .field("creation_metadata")
                    .field("assignment"),
            )?;
            let initial_domains = decode_task_domains(
                &metadata.initial_domains,
                create_path
                    .clone()
                    .field("creation_metadata")
                    .field("initial_domains"),
            )?;
            let request = CreateTask::try_new(
                envelope.operation_id(),
                context,
                descriptor,
                initial_domains
                    .iter()
                    .map(DecodedTaskDomain::as_neutral)
                    .collect(),
            )
            .map_err(|error| invalid(create_path, error.to_string()))?;
            let input = TaskCreationInput::new(
                FrozenBytes::freeze(prost::bytes::Bytes::copy_from_slice(
                    create.frozen_fragment.as_ref(),
                )),
                Box::new(assignment),
            );
            Ok(DecodedOperation::CreateTask(DecodedCreateTask {
                request,
                input,
                initial_domains,
                retained_bytes: src.encoded_len(),
            }))
        }
        novarocks::task_operation::Operation::UpdateTask(update) => {
            let update_path = path.clone().field("update_task");
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::UpdateTask,
                path.field("envelope"),
            )?;
            let identity = decode_identity_field(
                update.identity.as_ref(),
                update_path.clone(),
                "update requires a task identity",
            )?;
            let domains =
                decode_task_domains(&update.domains, update_path.clone().field("domains"))?;
            let request = UpdateTask::try_new(
                envelope.operation_id(),
                identity,
                domains.iter().map(DecodedTaskDomain::as_neutral).collect(),
            )
            .map_err(|error| invalid(update_path, error.to_string()))?;
            Ok(DecodedOperation::UpdateTask(DecodedUpdateTask {
                request,
                domains,
            }))
        }
        novarocks::task_operation::Operation::UpdateQueryContext(context) => {
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::UpdateQueryContext,
                path.clone().field("envelope"),
            )?;
            Ok(DecodedOperation::UpdateQueryContext(
                decode_update_query_context(context, envelope, path.field("update_query_context"))?,
            ))
        }
        novarocks::task_operation::Operation::CancelTask(cancel) => {
            let cancel_path = path.clone().field("cancel_task");
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::CancelTask,
                path.field("envelope"),
            )?;
            let identity = decode_identity_field(
                cancel.identity.as_ref(),
                cancel_path.clone(),
                "cancel requires a task identity",
            )?;
            let reason = decode_cancel_reason(cancel.reason, cancel_path.field("reason"))?;
            Ok(DecodedOperation::CancelTask(CancelTask::new(
                envelope.operation_id(),
                identity,
                reason,
            )))
        }
        novarocks::task_operation::Operation::AbortQueryContext(abort) => {
            let abort_path = path.clone().field("abort_query_context");
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::AbortQueryContext,
                path.field("envelope"),
            )?;
            let context = abort.query_context.as_ref().ok_or_else(|| {
                missing(
                    abort_path.clone().field("query_context"),
                    "abort requires a query context reference",
                )
            })?;
            let context =
                decode_query_context_ref(context, abort_path.clone().field("query_context"))?;
            let cause = decode_abort_cause(abort.cause, abort_path.field("cause"))?;
            Ok(DecodedOperation::AbortQueryContext(AbortQueryContext::new(
                envelope.operation_id(),
                context,
                cause,
            )))
        }
        novarocks::task_operation::Operation::ReleaseQueryContext(release) => {
            let release_path = path.clone().field("release_query_context");
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::ReleaseQueryContext,
                path.field("envelope"),
            )?;
            let context = release.query_context.as_ref().ok_or_else(|| {
                missing(
                    release_path.field("query_context"),
                    "release requires a query context reference",
                )
            })?;
            let context = decode_query_context_ref(context, release_path)?;
            Ok(DecodedOperation::ReleaseQueryContext(
                ReleaseQueryContext::new(envelope.operation_id(), context),
            ))
        }
        novarocks::task_operation::Operation::QuiesceQueryContext(quiesce) => {
            let request_path = path.clone().field("quiesce_query_context");
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::QuiesceQueryContext,
                path.field("envelope"),
            )?;
            let context = quiesce.query_context.as_ref().ok_or_else(|| {
                missing(
                    request_path.clone().field("query_context"),
                    "quiesce requires a query context reference",
                )
            })?;
            let context = decode_query_context_ref(context, request_path.field("query_context"))?;
            Ok(DecodedOperation::QuiesceQueryContext(
                QuiesceQueryContext::new(envelope.operation_id(), context),
            ))
        }
    }
}

fn decode_identity_field(
    src: Option<&novarocks::TaskIdentity>,
    path: FieldPath,
    detail: &'static str,
) -> Result<TaskIdentity, ProtocolError> {
    let identity = src.ok_or_else(|| missing(path.clone().field("identity"), detail))?;
    crate::identity::decode_task_identity(identity, path.field("identity"))
}

fn decode_update_query_context(
    src: &novarocks::UpdateQueryContextRequest,
    envelope: OperationEnvelope,
    path: FieldPath,
) -> Result<DecodedUpdateQueryContext, ProtocolError> {
    let command = src.command.as_ref().ok_or_else(|| {
        missing(
            path.clone(),
            "update query context requires exactly one command",
        )
    })?;
    match command {
        novarocks::update_query_context_request::Command::Establish(establish) => {
            let establish_path = path.field("establish");
            let context = establish.query_context.as_ref().ok_or_else(|| {
                missing(
                    establish_path.clone().field("query_context"),
                    "establish requires a query context reference",
                )
            })?;
            let context =
                decode_query_context_ref(context, establish_path.clone().field("query_context"))?;
            let admission_ticket_id = establish.admission_ticket_id.as_ref().ok_or_else(|| {
                missing(
                    establish_path.clone().field("admission_ticket_id"),
                    "establish requires an admission ticket id",
                )
            })?;
            let admission_ticket_id = decode_admission_ticket_id(
                admission_ticket_id,
                establish_path.clone().field("admission_ticket_id"),
            )?;
            let catalog_set = establish.catalog_set.clone().ok_or_else(|| {
                missing(
                    establish_path.clone().field("catalog_set"),
                    "establish requires a catalog set",
                )
            })?;
            let catalog_set = Arc::new(WireContent::new(ESTABLISH_CATALOG_DOMAIN_TAG, catalog_set));
            let initial_runtime_filter =
                establish.initial_runtime_filter.clone().ok_or_else(|| {
                    missing(
                        establish_path.clone().field("initial_runtime_filter"),
                        "establish requires an initial runtime filter",
                    )
                })?;
            let initial_runtime_filter = Arc::new(WireContent::new(
                ESTABLISH_FILTER_DOMAIN_TAG,
                initial_runtime_filter,
            ));
            let query_options = establish.query_options.ok_or_else(|| {
                missing(
                    establish_path.clone().field("query_options"),
                    "establish requires query options",
                )
            })?;
            novarocks_proto_codec::lifecycle::QueryOptions::parse(query_options).map_err(
                |error| {
                    invalid(
                        establish_path.clone().field("query_options"),
                        error.detail(),
                    )
                },
            )?;
            let query_options = Arc::new(WireContent::new(
                ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
                query_options,
            ));
            let credential = establish.initial_credential.as_ref().ok_or_else(|| {
                missing(
                    establish_path.clone().field("initial_credential"),
                    "establish requires an initial credential",
                )
            })?;
            let initial_credential = decode_credential_domain(
                credential,
                establish_path.clone().field("initial_credential"),
            )?;
            let lease = establish.initial_lease.as_ref().ok_or_else(|| {
                missing(
                    establish_path.clone().field("initial_lease"),
                    "establish requires an initial lease",
                )
            })?;
            // The initial lease is the only one that may carry sequence zero,
            // and it may carry nothing else.
            if lease.sequence != 0 {
                return Err(invalid(
                    establish_path
                        .clone()
                        .field("initial_lease")
                        .field("sequence"),
                    "an initial lease must carry sequence zero",
                ));
            }
            let valid_for = decode_duration_millis(
                lease.valid_for_millis,
                establish_path
                    .clone()
                    .field("initial_lease")
                    .field("valid_for_millis"),
                LeaseValidFor::MAX_REPRESENTABLE,
            )?;
            let initial_lease_valid_for = LeaseValidFor::new(valid_for).map_err(|error| {
                invalid(
                    establish_path
                        .field("initial_lease")
                        .field("valid_for_millis"),
                    error.to_string(),
                )
            })?;
            let native_compatibility_id =
                establish.native_compatibility_id.as_ref().ok_or_else(|| {
                    missing(
                        establish_path.clone().field("native_compatibility_id"),
                        "establish requires a native compatibility identity",
                    )
                })?;
            let native_compatibility_id = NativeCompatibilityId::try_from_slice(
                &native_compatibility_id.value,
            )
            .map_err(|error| {
                invalid(
                    establish_path
                        .clone()
                        .field("native_compatibility_id")
                        .field("value"),
                    error.to_string(),
                )
            })?;
            Ok(DecodedUpdateQueryContext::Establish(
                DecodedEstablishQueryContext {
                    context,
                    envelope,
                    admission_ticket_id,
                    catalog_set,
                    initial_runtime_filter,
                    query_options,
                    initial_credential,
                    initial_lease_valid_for,
                    native_compatibility_id,
                },
            ))
        }
        novarocks::update_query_context_request::Command::AdvanceDomain(advance) => {
            let advance_path = path.field("advance_domain");
            let context = advance.query_context.as_ref().ok_or_else(|| {
                missing(
                    advance_path.clone().field("query_context"),
                    "advance requires a query context reference",
                )
            })?;
            let context =
                decode_query_context_ref(context, advance_path.clone().field("query_context"))?;
            let domain = advance.domain.as_ref().ok_or_else(|| {
                missing(
                    advance_path.clone().field("domain"),
                    "advance requires a domain",
                )
            })?;
            let domain = decode_query_context_domain(domain, advance_path.field("domain"))?;
            Ok(DecodedUpdateQueryContext::AdvanceDomain {
                context,
                envelope,
                domain,
            })
        }
        novarocks::update_query_context_request::Command::RenewLease(renew) => {
            let renew_path = path.field("renew_lease");
            let context = renew.query_context.as_ref().ok_or_else(|| {
                missing(
                    renew_path.clone().field("query_context"),
                    "renew requires a query context reference",
                )
            })?;
            let context =
                decode_query_context_ref(context, renew_path.clone().field("query_context"))?;
            let lease = renew.lease.as_ref().ok_or_else(|| {
                missing(
                    renew_path.clone().field("lease"),
                    "renew requires a lease grant",
                )
            })?;
            let grant = crate::lease::decode_lease_grant(lease, renew_path.field("lease"))?;
            Ok(DecodedUpdateQueryContext::RenewLease {
                context,
                envelope,
                sequence: grant.sequence(),
                valid_for: grant.valid_for(),
            })
        }
    }
}

/// Decodes a per-backend operation batch.
pub fn decode_operation_batch(
    src: &novarocks::ApplyTaskOperationsRequest,
    budget: TransportBudget,
    path: FieldPath,
) -> Result<Vec<DecodedOperation>, ProtocolError> {
    let encoded_len = src.encoded_len();
    if !budget.batch_fits(src.operations.len(), encoded_len) {
        return Err(out_of_range(
            path.clone().field("operations"),
            "operation batch exceeds its item or byte budget",
        ));
    }
    let mut operations = Vec::with_capacity(src.operations.len());
    for (index, operation) in src.operations.iter().enumerate() {
        operations.push(decode_operation(
            operation,
            path.clone().field("operations").index(index),
        )?);
    }
    Ok(operations)
}

/// Decodes the ordinary method and refuses control work sent through its
/// shared executor. The same operation cannot silently lose its control lane.
pub fn decode_ordinary_operation_batch(
    src: &novarocks::ApplyTaskOperationsRequest,
    budget: TransportBudget,
    path: FieldPath,
) -> Result<Vec<DecodedOperation>, ProtocolError> {
    let operations = decode_operation_batch(src, budget, path.clone())?;
    if operations.iter().any(is_small_control) {
        return Err(invalid(
            path.field("operations"),
            "small control operation requires the control method",
        ));
    }
    Ok(operations)
}

/// Validates an ordinary batch before any operation is applied, allowing the
/// Worker to supply an exact refusal for selected expired Establish items.
///
/// A skipped item still has its formal envelope, context, ticket identity, and
/// ordinary-method shape checked here. Only its potentially large Establish
/// content is left undecoded. The caller must have an owner-authored receipt
/// for every skipped position; this codec never decides whether to skip.
pub fn decode_ordinary_operation_batch_with_skip(
    src: &novarocks::ApplyTaskOperationsRequest,
    budget: TransportBudget,
    skip: &[bool],
    path: FieldPath,
) -> Result<Vec<Option<DecodedOperation>>, ProtocolError> {
    if skip.len() != src.operations.len() {
        return Err(invalid(
            path.field("operations"),
            "skip count does not match batch",
        ));
    }
    if !budget.batch_fits(src.operations.len(), src.encoded_len()) {
        return Err(out_of_range(
            path.field("operations"),
            "operation batch exceeds its item or byte budget",
        ));
    }
    let mut operations = Vec::with_capacity(src.operations.len());
    for (index, operation) in src.operations.iter().enumerate() {
        let item_path = path.clone().field("operations").index(index);
        if skip[index] {
            let envelope_src = operation.envelope.as_ref().ok_or_else(|| {
                missing(
                    item_path.clone().field("envelope"),
                    "an operation requires an envelope",
                )
            })?;
            decode_envelope(
                envelope_src,
                OperationKind::UpdateQueryContext,
                item_path.clone().field("envelope"),
            )?;
            let Some(novarocks::task_operation::Operation::UpdateQueryContext(update)) =
                operation.operation.as_ref()
            else {
                return Err(invalid(item_path, "only Establish may skip deep decode"));
            };
            let Some(novarocks::update_query_context_request::Command::Establish(establish)) =
                update.command.as_ref()
            else {
                return Err(invalid(item_path, "only Establish may skip deep decode"));
            };
            let context = establish.query_context.as_ref().ok_or_else(|| {
                missing(
                    item_path
                        .clone()
                        .field("update_query_context")
                        .field("establish")
                        .field("query_context"),
                    "establish requires a query context reference",
                )
            })?;
            decode_query_context_ref(
                context,
                item_path
                    .clone()
                    .field("update_query_context")
                    .field("establish")
                    .field("query_context"),
            )?;
            let ticket = establish.admission_ticket_id.as_ref().ok_or_else(|| {
                missing(
                    item_path
                        .clone()
                        .field("update_query_context")
                        .field("establish")
                        .field("admission_ticket_id"),
                    "establish requires an admission ticket id",
                )
            })?;
            decode_admission_ticket_id(
                ticket,
                item_path
                    .field("update_query_context")
                    .field("establish")
                    .field("admission_ticket_id"),
            )?;
            operations.push(None);
        } else {
            let decoded = decode_operation(operation, item_path)?;
            operations.push(Some(decoded));
        }
    }
    if operations.iter().flatten().any(is_small_control) {
        return Err(invalid(
            path.field("operations"),
            "small control operation requires the control method",
        ));
    }
    Ok(operations)
}

fn is_small_control(operation: &DecodedOperation) -> bool {
    match operation {
        DecodedOperation::AcquireQueryContextAdmissionTicket(_)
        | DecodedOperation::CreateTask(_)
        | DecodedOperation::UpdateTask(_) => false,
        DecodedOperation::UpdateQueryContext(command) => match command {
            DecodedUpdateQueryContext::Establish(_)
            | DecodedUpdateQueryContext::AdvanceDomain { .. } => false,
            DecodedUpdateQueryContext::RenewLease { .. } => true,
        },
        DecodedOperation::CancelTask(_)
        | DecodedOperation::AbortQueryContext(_)
        | DecodedOperation::QuiesceQueryContext(_)
        | DecodedOperation::ReleaseQueryContext(_) => true,
    }
}

/// Decodes the closed small-control method via the one authoritative typed
/// operation decoder. Its schema cannot carry creation or a domain advance.
pub fn decode_control_operation_batch(
    src: &novarocks::ApplyTaskControlOperationsRequest,
    budget: TransportBudget,
    path: FieldPath,
) -> Result<Vec<DecodedOperation>, ProtocolError> {
    if !budget.batch_fits(src.operations.len(), src.encoded_len()) {
        return Err(out_of_range(
            path.clone().field("operations"),
            "control batch exceeds its item or byte budget",
        ));
    }
    let mut operations = Vec::with_capacity(src.operations.len());
    for (index, control) in src.operations.iter().enumerate() {
        let control_path = path.clone().field("operations").index(index);
        let command = control.control.as_ref().ok_or_else(|| {
            missing(
                control_path.clone().field("control"),
                "control command is required",
            )
        })?;
        let operation = match command {
            novarocks::task_control_operation::Control::RenewLease(request) => {
                novarocks::task_operation::Operation::UpdateQueryContext(
                    novarocks::UpdateQueryContextRequest {
                        command: Some(
                            novarocks::update_query_context_request::Command::RenewLease(
                                request.clone(),
                            ),
                        ),
                    },
                )
            }
            novarocks::task_control_operation::Control::CancelTask(request) => {
                novarocks::task_operation::Operation::CancelTask(request.clone())
            }
            novarocks::task_control_operation::Control::AbortQueryContext(request) => {
                novarocks::task_operation::Operation::AbortQueryContext(request.clone())
            }
            novarocks::task_control_operation::Control::ReleaseQueryContext(request) => {
                novarocks::task_operation::Operation::ReleaseQueryContext(request.clone())
            }
            novarocks::task_control_operation::Control::QuiesceQueryContext(request) => {
                novarocks::task_operation::Operation::QuiesceQueryContext(request.clone())
            }
        };
        operations.push(decode_operation(
            &novarocks::TaskOperation {
                envelope: control.envelope.clone(),
                operation: Some(operation),
            },
            control_path,
        )?);
    }
    Ok(operations)
}

fn decode_outcome(value: i32, path: FieldPath) -> Result<OperationOutcome, ProtocolError> {
    match novarocks::TaskOperationOutcome::try_from(value) {
        Ok(novarocks::TaskOperationOutcome::Accepted) => Ok(OperationOutcome::Accepted),
        Ok(novarocks::TaskOperationOutcome::Idempotent) => Ok(OperationOutcome::Idempotent),
        Ok(novarocks::TaskOperationOutcome::OperationTimedOut) => {
            Ok(OperationOutcome::OperationTimedOut)
        }
        Ok(novarocks::TaskOperationOutcome::IdentityMismatch) => {
            Ok(OperationOutcome::IdentityMismatch)
        }
        Ok(novarocks::TaskOperationOutcome::CompatibilityMismatch) => {
            Ok(OperationOutcome::CompatibilityMismatch)
        }
        Ok(novarocks::TaskOperationOutcome::ContextNotEstablished) => {
            Ok(OperationOutcome::ContextNotEstablished)
        }
        Ok(novarocks::TaskOperationOutcome::ContextConflict) => {
            Ok(OperationOutcome::ContextConflict)
        }
        Ok(novarocks::TaskOperationOutcome::DomainConflict) => Ok(OperationOutcome::DomainConflict),
        Ok(novarocks::TaskOperationOutcome::LeaseExpired) => Ok(OperationOutcome::LeaseExpired),
        Ok(novarocks::TaskOperationOutcome::ReleaseNotReady) => {
            Ok(OperationOutcome::ReleaseNotReady)
        }
        Ok(novarocks::TaskOperationOutcome::ContextTerminalReceipt) => {
            Ok(OperationOutcome::ContextTerminalReceipt)
        }
        Ok(novarocks::TaskOperationOutcome::InvalidStateOrRequest) => {
            Ok(OperationOutcome::InvalidStateOrRequest)
        }
        Ok(novarocks::TaskOperationOutcome::TerminalRejected) => {
            Ok(OperationOutcome::TerminalRejected)
        }
        Ok(novarocks::TaskOperationOutcome::Gone) => Ok(OperationOutcome::Gone),
        Ok(novarocks::TaskOperationOutcome::ResourceExhausted) => {
            Ok(OperationOutcome::ResourceExhausted)
        }
        Ok(novarocks::TaskOperationOutcome::NotReady) => Ok(OperationOutcome::NotReady),
        Ok(novarocks::TaskOperationOutcome::PreparationBusy) => {
            Ok(OperationOutcome::PreparationBusy)
        }
        Ok(novarocks::TaskOperationOutcome::AdmissionTicketStillActive) => {
            Ok(OperationOutcome::AdmissionTicketStillActive)
        }
        Ok(novarocks::TaskOperationOutcome::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "operation outcome must be a known non-default value",
        )),
    }
}

/// Encodes a Worker-produced receipt verdict.
///
/// The shared outcome set contains only Worker verdicts, so every value has
/// one exact wire representation.
fn encode_outcome(value: OperationOutcome) -> i32 {
    let encoded = match value {
        OperationOutcome::Accepted => novarocks::TaskOperationOutcome::Accepted,
        OperationOutcome::Idempotent => novarocks::TaskOperationOutcome::Idempotent,
        OperationOutcome::OperationTimedOut => novarocks::TaskOperationOutcome::OperationTimedOut,
        OperationOutcome::IdentityMismatch => novarocks::TaskOperationOutcome::IdentityMismatch,
        OperationOutcome::CompatibilityMismatch => {
            novarocks::TaskOperationOutcome::CompatibilityMismatch
        }
        OperationOutcome::ContextNotEstablished => {
            novarocks::TaskOperationOutcome::ContextNotEstablished
        }
        OperationOutcome::ContextConflict => novarocks::TaskOperationOutcome::ContextConflict,
        OperationOutcome::DomainConflict => novarocks::TaskOperationOutcome::DomainConflict,
        OperationOutcome::LeaseExpired => novarocks::TaskOperationOutcome::LeaseExpired,
        OperationOutcome::ReleaseNotReady => novarocks::TaskOperationOutcome::ReleaseNotReady,
        OperationOutcome::ContextTerminalReceipt => {
            novarocks::TaskOperationOutcome::ContextTerminalReceipt
        }
        OperationOutcome::InvalidStateOrRequest => {
            novarocks::TaskOperationOutcome::InvalidStateOrRequest
        }
        OperationOutcome::TerminalRejected => novarocks::TaskOperationOutcome::TerminalRejected,
        OperationOutcome::Gone => novarocks::TaskOperationOutcome::Gone,
        OperationOutcome::ResourceExhausted => novarocks::TaskOperationOutcome::ResourceExhausted,
        OperationOutcome::NotReady => novarocks::TaskOperationOutcome::NotReady,
        OperationOutcome::PreparationBusy => novarocks::TaskOperationOutcome::PreparationBusy,
        OperationOutcome::AdmissionTicketStillActive => {
            novarocks::TaskOperationOutcome::AdmissionTicketStillActive
        }
    };
    encoded as i32
}

fn decode_context_state(value: i32, path: FieldPath) -> Result<QueryContextState, ProtocolError> {
    match novarocks::QueryContextState::try_from(value) {
        Ok(novarocks::QueryContextState::Establishing) => Ok(QueryContextState::Establishing),
        Ok(novarocks::QueryContextState::Active) => Ok(QueryContextState::Active),
        Ok(novarocks::QueryContextState::Quiescing) => Ok(QueryContextState::Quiescing),
        Ok(novarocks::QueryContextState::Releasing) => Ok(QueryContextState::Releasing),
        Ok(novarocks::QueryContextState::Aborting) => Ok(QueryContextState::Aborting),
        Ok(novarocks::QueryContextState::TerminalRetained) => {
            Ok(QueryContextState::TerminalRetained)
        }
        Ok(novarocks::QueryContextState::Gone) => Ok(QueryContextState::Gone),
        Ok(novarocks::QueryContextState::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "query context state must be a known non-default value",
        )),
    }
}

/// Encodes a query context state.
///
/// `Absent` deliberately has no wire representation: a backend never reports a
/// context it does not have as a state, it reports it as an outcome. Returning
/// `None` keeps that from being silently encoded as `Establishing`.
fn encode_context_state(value: QueryContextState) -> Option<i32> {
    let encoded = match value {
        QueryContextState::Absent => return None,
        QueryContextState::Establishing => novarocks::QueryContextState::Establishing,
        QueryContextState::Active => novarocks::QueryContextState::Active,
        QueryContextState::Quiescing => novarocks::QueryContextState::Quiescing,
        QueryContextState::Releasing => novarocks::QueryContextState::Releasing,
        QueryContextState::Aborting => novarocks::QueryContextState::Aborting,
        QueryContextState::TerminalRetained => novarocks::QueryContextState::TerminalRetained,
        QueryContextState::Gone => novarocks::QueryContextState::Gone,
    };
    Some(encoded as i32)
}

fn decode_release_outcome(value: i32, path: FieldPath) -> Result<ReleaseOutcome, ProtocolError> {
    match novarocks::ReleaseQueryContextOutcome::try_from(value) {
        Ok(novarocks::ReleaseQueryContextOutcome::Released) => Ok(ReleaseOutcome::Released),
        Ok(novarocks::ReleaseQueryContextOutcome::NotReady) => Ok(ReleaseOutcome::NotReady),
        Ok(novarocks::ReleaseQueryContextOutcome::AlreadyTerminal) => {
            Ok(ReleaseOutcome::AlreadyTerminal)
        }
        Ok(novarocks::ReleaseQueryContextOutcome::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "release outcome must be a known non-default value",
        )),
    }
}

fn encode_release_outcome(value: ReleaseOutcome) -> i32 {
    let encoded = match value {
        ReleaseOutcome::Released => novarocks::ReleaseQueryContextOutcome::Released,
        ReleaseOutcome::NotReady => novarocks::ReleaseQueryContextOutcome::NotReady,
        ReleaseOutcome::AlreadyTerminal => novarocks::ReleaseQueryContextOutcome::AlreadyTerminal,
    };
    encoded as i32
}

/// The outcome and identity of one receipt, without its acknowledgement body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptHeader {
    operation_id: TaskOperationId,
    outcome: OperationOutcome,
    /// The backend's own account of a refusal, already validated as safe to
    /// leave the process.
    ///
    /// Kept rather than discarded: this decoder was validating the field and
    /// then dropping it, so a refusal reached the client as an outcome name
    /// with the reason it had in hand thrown away.
    detail: Option<novarocks_execution_contract::task_execution::status::SafeDetail>,
}

impl ReceiptHeader {
    pub const fn new(operation_id: TaskOperationId, outcome: OperationOutcome) -> Self {
        Self {
            detail: None,
            operation_id,
            outcome,
        }
    }

    pub const fn operation_id(&self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn outcome(&self) -> OperationOutcome {
        self.outcome
    }

    /// The backend's own reason for a refusal, if it gave one.
    pub const fn detail(
        &self,
    ) -> Option<&novarocks_execution_contract::task_execution::status::SafeDetail> {
        self.detail.as_ref()
    }
}

/// Decodes the header of one operation receipt.
pub fn decode_receipt_header(
    src: &novarocks::TaskOperationReceipt,
    path: FieldPath,
) -> Result<ReceiptHeader, ProtocolError> {
    let operation_id = src.operation_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("operation_id"),
            "a receipt requires an operation id",
        )
    })?;
    let operation_id = decode_task_operation_id(operation_id, path.clone().field("operation_id"))?;
    let outcome = decode_outcome(src.outcome, path.clone().field("outcome"))?;
    let detail =
        crate::status::decode_safe_detail(&src.safe_detail, path.clone().field("safe_detail"))?;
    if let Some(field_path) = src.safe_field_path.as_deref()
        && field_path.len()
            > novarocks_execution_contract::task_execution::status::SAFE_FIELD_PATH_MAX_BYTES
    {
        return Err(out_of_range(
            path.field("safe_field_path"),
            "safe field path exceeds the redacted text limit",
        ));
    }
    Ok(ReceiptHeader {
        operation_id,
        outcome,
        // Only a refusal has anything to explain; an applied operation's
        // detail is empty by contract and carrying it would invite a reader
        // to look for meaning that is not there.
        detail: (!detail.as_str().is_empty()).then_some(detail),
    })
}

/// Decodes a batch response, which must carry exactly one receipt per request
/// item, in request order.
pub fn decode_receipt_batch(
    src: &novarocks::ApplyTaskOperationsResponse,
    expected: &[TaskOperationId],
    path: FieldPath,
) -> Result<Vec<ReceiptHeader>, ProtocolError> {
    if src.receipts.len() != expected.len() {
        return Err(invalid(
            path.clone().field("receipts"),
            "a batch response must carry exactly one receipt per request item",
        ));
    }
    let mut receipts = Vec::with_capacity(src.receipts.len());
    for (index, receipt) in src.receipts.iter().enumerate() {
        let receipt_path = path.clone().field("receipts").index(index);
        let header = decode_receipt_header(receipt, receipt_path.clone())?;
        if header.operation_id() != expected[index] {
            return Err(invalid(
                receipt_path.field("operation_id"),
                "a batch receipt does not correspond to its request item",
            ));
        }
        receipts.push(header);
    }
    Ok(receipts)
}

/// Decodes a dynamic filter read request.
pub fn decode_fetch_dynamic_filters(
    src: &novarocks::FetchTaskDynamicFiltersRequest,
    operation_id: TaskOperationId,
    path: FieldPath,
) -> Result<FetchTaskDynamicFilters, ProtocolError> {
    let identity = decode_identity_field(
        src.identity.as_ref(),
        path.clone(),
        "a dynamic filter read requires a task identity",
    )?;
    // Version zero means nothing has been acknowledged yet, which is a legal
    // starting state rather than a malformed request.
    let acknowledged = DomainVersion::new(src.acknowledged_version).ok();
    Ok(FetchTaskDynamicFilters::new(
        operation_id,
        identity,
        acknowledged,
    ))
}

/// Decodes a final info read request.
pub fn decode_get_final_task_info(
    src: &novarocks::GetFinalTaskInfoRequest,
    operation_id: TaskOperationId,
    path: FieldPath,
) -> Result<GetFinalTaskInfo, ProtocolError> {
    let identity = decode_identity_field(
        src.identity.as_ref(),
        path,
        "a final info read requires a task identity",
    )?;
    Ok(GetFinalTaskInfo::new(operation_id, identity))
}

/// Decodes a root result poll request.
///
/// The root task identity is the whole point of this shape: unlike the
/// fragment-instance-addressed form it replaces, it is fenced against an exact
/// task and backend process.
pub fn decode_fetch_task_result(
    src: &novarocks::FetchTaskResultRequest,
    path: FieldPath,
) -> Result<
    (
        TaskIdentity,
        Duration,
        Option<ResultPacketSequence>,
        ResultByteLimit,
    ),
    ProtocolError,
> {
    let root = src.root_task.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("root_task"),
            "a result poll requires a root task identity",
        )
    })?;
    let root = crate::identity::decode_task_identity(root, path.clone().field("root_task"))?;
    let max_wait = decode_duration_millis(
        src.max_wait_millis,
        path.clone().field("max_wait_millis"),
        MaxWait::MAX_REPRESENTABLE,
    )?;
    let acknowledged = src
        .acknowledged_packet_sequence
        .map(ResultPacketSequence::new);
    let max_result_bytes = ResultByteLimit::new(src.max_result_bytes)
        .map_err(|error| invalid(path.clone().field("max_result_bytes"), error.to_string()))?;
    if max_result_bytes.get() > MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES {
        return Err(out_of_range(
            path.field("max_result_bytes"),
            format!(
                "max_result_bytes {} exceeds the Native payload limit {}",
                max_result_bytes.get(),
                MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES
            ),
        ));
    }
    Ok((root, max_wait, acknowledged, max_result_bytes))
}

/// Decodes one server-reported operation outcome.
///
/// A read whose response reports why it has nothing to return needs this: the
/// category is the answer, and a caller must not have to read a message string
/// to learn it.
pub fn decode_operation_outcome(
    value: i32,
    path: FieldPath,
) -> Result<OperationOutcome, ProtocolError> {
    decode_outcome(value, path)
}

/// Encodes a dynamic filter read request.
///
/// `acknowledged` is the reader's own cursor, and version zero is its "nothing
/// acknowledged yet" -- a published `DomainVersion` is nonzero, so the two
/// cannot collide.
pub fn encode_fetch_dynamic_filters(
    request: FetchTaskDynamicFilters,
) -> novarocks::FetchTaskDynamicFiltersRequest {
    novarocks::FetchTaskDynamicFiltersRequest {
        identity: Some(crate::identity::encode_task_identity(request.identity())),
        acknowledged_version: request.acknowledged_version().map_or(0, DomainVersion::get),
    }
}

/// Encodes a final info read request.
pub fn encode_get_final_task_info(identity: TaskIdentity) -> novarocks::GetFinalTaskInfoRequest {
    novarocks::GetFinalTaskInfoRequest {
        identity: Some(crate::identity::encode_task_identity(identity)),
    }
}

/// Encodes a root result poll request.
pub fn encode_fetch_task_result(
    root_task: TaskIdentity,
    max_wait: MaxWait,
    acknowledged: Option<ResultPacketSequence>,
    max_result_bytes: ResultByteLimit,
) -> novarocks::FetchTaskResultRequest {
    assert!(
        max_result_bytes.get() <= MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES,
        "root result byte limit must fit the Native gRPC response"
    );
    novarocks::FetchTaskResultRequest {
        root_task: Some(crate::identity::encode_task_identity(root_task)),
        // `MaxWait` is already bounded by `MAX_REPRESENTABLE`, so this cannot
        // narrow a wait the caller asked for.
        max_wait_millis: u64::try_from(max_wait.get().as_millis()).unwrap_or(u64::MAX),
        acknowledged_packet_sequence: acknowledged.map(ResultPacketSequence::get),
        max_result_bytes: max_result_bytes.get(),
    }
}

/// Encodes the credential receipt of one query context.
///
/// Only the accepted epoch is reported: no credential material, and no digest
/// of any, ever appears in a receipt.
pub fn encode_credential_receipt(
    lease_id: novarocks_execution_contract::task_execution::domain::CredentialLeaseId,
    epoch: CredentialEpoch,
) -> novarocks::QueryContextCredentialReceipt {
    novarocks::QueryContextCredentialReceipt {
        lease_id: lease_id.get(),
        accepted_epoch: epoch.get(),
    }
}

/// Encodes a release acknowledgement.
///
/// `runtime_filter` is the releasing backend's own sealed runtime-filter
/// observation. It is passed already-validated rather than assembled here:
/// the projection belongs to the participant's owner, and this codec's job is
/// to carry the value it produced without inventing an empty one for a
/// release that had nothing to seal.
pub fn encode_quiesce_ack(
    value: &QuiesceQueryContextReceipt,
) -> Option<novarocks::QuiesceQueryContextAck> {
    Some(novarocks::QuiesceQueryContextAck {
        query_context: Some(encode_query_context_ref(value.context())),
        fence_version: value.fence_version(),
        accepted_tasks: value
            .accepted_tasks()
            .iter()
            .copied()
            .map(encode_task_identity)
            .collect(),
        state: encode_context_state(value.state())?,
    })
}

pub fn decode_quiesce_ack(
    src: &novarocks::QuiesceQueryContextAck,
    path: FieldPath,
) -> Result<QuiesceQueryContextReceipt, ProtocolError> {
    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "quiesce acknowledgement requires a query context reference",
        )
    })?;
    let context = decode_query_context_ref(context, path.clone().field("query_context"))?;
    if src.fence_version == 0 {
        return Err(out_of_range(
            path.clone().field("fence_version"),
            "quiesce fence version must be positive",
        ));
    }
    if src.accepted_tasks.len() > TransportBudget::DEFAULT.max_tasks_per_context() {
        return Err(out_of_range(
            path.clone().field("accepted_tasks"),
            "quiesce membership exceeds the context task bound",
        ));
    }
    let tasks = src
        .accepted_tasks
        .iter()
        .enumerate()
        .map(|(index, value)| {
            decode_task_identity(value, path.clone().field("accepted_tasks").index(index))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if tasks.iter().any(|identity| {
        identity.query_execution_id() != context.query_execution_id()
            || identity.backend_process_id() != context.backend_process_id()
    }) {
        return Err(invalid(
            path.clone().field("accepted_tasks"),
            "quiesce task identity belongs to another context",
        ));
    }
    if tasks.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(
            path.clone().field("accepted_tasks"),
            "quiesce membership must be sorted and unique",
        ));
    }
    let state = decode_context_state(src.state, path.clone().field("state"))?;
    if !matches!(
        state,
        QueryContextState::Quiescing
            | QueryContextState::Releasing
            | QueryContextState::Aborting
            | QueryContextState::TerminalRetained
    ) {
        return Err(invalid(
            path.field("state"),
            "quiesce acknowledgement requires a closing state",
        ));
    }
    Ok(QuiesceQueryContextReceipt::new(
        context,
        src.fence_version,
        tasks,
        state,
    ))
}

pub fn encode_release_ack(
    context: QueryContextRef,
    outcome: ReleaseOutcome,
    state: QueryContextState,
    runtime_filter: Option<&QueryTerminalProfileContributionTelemetry>,
) -> Option<novarocks::ReleaseQueryContextAck> {
    Some(novarocks::ReleaseQueryContextAck {
        query_context: Some(encode_query_context_ref(context)),
        outcome: encode_release_outcome(outcome),
        state: encode_context_state(state)?,
        termination_cause: None,
        runtime_filter: runtime_filter.map(|value| value.as_proto().clone()),
    })
}

/// Everything one release acknowledgement states.
///
/// A struct rather than a tuple because each field answers a different
/// question and two of them are optional: a positional read would let a caller
/// silently swap the termination cause for the sealed contribution.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedReleaseAck {
    pub context: QueryContextRef,
    pub outcome: ReleaseOutcome,
    pub state: QueryContextState,
    /// Why the context was terminated, when a release answered on one that was
    /// terminated instead of released.
    pub termination_cause: Option<novarocks_execution_contract::task_execution::status::AbortCause>,
    /// The backend's sealed runtime-filter observation. Absent means the
    /// release held no participant to seal.
    pub runtime_filter: Option<QueryTerminalProfileContributionTelemetry>,
}

/// Decodes a release acknowledgement, returning the termination cause the
/// backend reported rather than validating and discarding it.
///
/// A release that comes back on a context that was terminated instead of
/// released carries the only statement of why.
pub fn decode_release_ack(
    src: &novarocks::ReleaseQueryContextAck,
    path: FieldPath,
) -> Result<DecodedReleaseAck, ProtocolError> {
    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "a release acknowledgement requires a query context reference",
        )
    })?;
    let context = decode_query_context_ref(context, path.clone().field("query_context"))?;
    let outcome = decode_release_outcome(src.outcome, path.clone().field("outcome"))?;
    let state = decode_context_state(src.state, path.clone().field("state"))?;
    let cause = src
        .termination_cause
        .map(|cause| decode_abort_cause(cause, path.clone().field("termination_cause")))
        .transpose()?;
    // Validated here rather than accepted verbatim: a contribution that does
    // not satisfy the terminal contract is a protocol error on the release
    // that carried it, not a profile the frontend discovers is malformed
    // several hops later.
    let runtime_filter = src
        .runtime_filter
        .as_ref()
        .map(|telemetry| {
            QueryTerminalProfileContributionTelemetry::parse(telemetry.clone()).map_err(|error| {
                invalid(
                    path.clone().field("runtime_filter"),
                    format!(
                        "a release acknowledgement carries an invalid runtime-filter \
                         contribution: {error}"
                    ),
                )
            })
        })
        .transpose()?;
    Ok(DecodedReleaseAck {
        context,
        outcome,
        state,
        termination_cause: cause,
        runtime_filter,
    })
}

/// Encodes a cancel reason back onto the wire, for a receipt or a status.
pub fn encode_cancel_reason_field(
    value: novarocks_execution_contract::task_execution::status::CancelReason,
) -> i32 {
    encode_cancel_reason(value)
}

/// Encodes an abort cause back onto the wire.
pub fn encode_abort_cause_field(
    value: novarocks_execution_contract::task_execution::status::AbortCause,
) -> i32 {
    encode_abort_cause(value)
}

/// Encodes one Worker receipt verdict back onto the wire.
pub fn encode_operation_outcome(value: OperationOutcome) -> i32 {
    encode_outcome(value)
}

/// Encodes a query context state back onto the wire, or `None` for `Absent`.
pub fn encode_query_context_state(value: QueryContextState) -> Option<i32> {
    encode_context_state(value)
}

// The frontend side of the operation surface: building requests, and reading
// the acknowledgements back.
//
// These are separate from the decoders above because the two roles use
// opposite halves. A backend decodes an operation and encodes a receipt; a
// frontend encodes an operation and decodes a receipt. Keeping both halves in
// one module is what makes it impossible for them to drift apart.

/// Encodes one create operation from its two frozen carriers.
///
/// Both carriers are reused as they were frozen: this clones two shared byte
/// handles and encodes only the envelope, so a resend never walks or
/// re-encodes the plan or the metadata.
pub fn encode_create_task(
    envelope: OperationEnvelope,
    frozen_fragment: &FrozenBytes,
    creation_metadata: &FrozenBytes,
) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(envelope)),
        operation: Some(novarocks::task_operation::Operation::CreateTask(
            novarocks::CreateTaskRequest {
                frozen_fragment: frozen_fragment.to_bytes(),
                creation_metadata: creation_metadata.to_bytes(),
            },
        )),
    }
}

pub fn encode_update_task(
    request: &UpdateTask,
    domains: Vec<novarocks::TaskDomainUpdate>,
) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::UpdateTask(
            novarocks::UpdateTaskRequest {
                identity: Some(crate::identity::encode_task_identity(request.identity())),
                domains,
            },
        )),
    }
}

pub fn encode_cancel_task(request: CancelTask) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::CancelTask(
            novarocks::CancelTaskRequest {
                identity: Some(crate::identity::encode_task_identity(request.identity())),
                reason: encode_cancel_reason(request.reason()),
            },
        )),
    }
}

pub fn encode_abort_query_context(request: AbortQueryContext) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::AbortQueryContext(
            novarocks::AbortQueryContextRequest {
                query_context: Some(encode_query_context_ref(request.context())),
                cause: encode_abort_cause(request.cause()),
            },
        )),
    }
}

pub fn encode_release_query_context(request: ReleaseQueryContext) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::ReleaseQueryContext(
            novarocks::ReleaseQueryContextRequest {
                query_context: Some(encode_query_context_ref(request.context())),
            },
        )),
    }
}

pub fn encode_quiesce_query_context(request: QuiesceQueryContext) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::QuiesceQueryContext(
            novarocks::QuiesceQueryContextRequest {
                query_context: Some(encode_query_context_ref(request.context())),
            },
        )),
    }
}

/// Encodes one worker admission ticket acquisition.
pub fn encode_acquire_query_context_admission_ticket(
    request: AcquireQueryContextAdmissionTicket,
) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(
            novarocks::task_operation::Operation::AcquireQueryContextAdmissionTicket(
                novarocks::AcquireQueryContextAdmissionTicketRequest {
                    query_context: Some(encode_query_context_ref(request.context())),
                    valid_for_millis: request.valid_for().get().as_millis() as u64,
                    native_compatibility_id: Some(novarocks::NativeCompatibilityId {
                        value: request.native_compatibility_id().as_bytes().to_vec(),
                    }),
                    admission_epoch_capability: Some(
                        crate::identity::encode_admission_epoch_capability(
                            request.admission_epoch_capability(),
                        ),
                    ),
                },
            ),
        ),
    }
}

/// Encodes one lease renewal.
pub fn encode_renew_lease(request: &RenewQueryExecutionLease) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::UpdateQueryContext(
            novarocks::UpdateQueryContextRequest {
                command: Some(
                    novarocks::update_query_context_request::Command::RenewLease(
                        novarocks::RenewQueryExecutionLeaseRequest {
                            query_context: Some(encode_query_context_ref(request.context())),
                            lease: Some(crate::lease::encode_lease_grant(
                                crate::lease::LeaseGrant::new(
                                    request.sequence(),
                                    request.valid_for(),
                                ),
                            )),
                        },
                    ),
                ),
            },
        )),
    }
}

/// Encodes one shared-domain advance.
pub fn encode_advance_query_context_domain(
    request: &AdvanceQueryContextDomain,
    domain: novarocks::QueryContextDomainUpdate,
) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::UpdateQueryContext(
            novarocks::UpdateQueryContextRequest {
                command: Some(
                    novarocks::update_query_context_request::Command::AdvanceDomain(
                        novarocks::AdvanceQueryContextDomainRequest {
                            query_context: Some(encode_query_context_ref(request.context())),
                            domain: Some(domain),
                        },
                    ),
                ),
            },
        )),
    }
}

/// Encodes one establish.
pub fn encode_establish_query_context(
    request: &EstablishQueryContext,
    catalog_set: novarocks_proto_models::catalog::CatalogSet,
    initial_runtime_filter: novarocks::RuntimeFilterContribution,
    initial_credential: novarocks::QueryContextCredentialDomain,
    query_options: novarocks::QueryOptions,
    native_compatibility_id: NativeCompatibilityId,
) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::UpdateQueryContext(
            novarocks::UpdateQueryContextRequest {
                command: Some(novarocks::update_query_context_request::Command::Establish(
                    novarocks::EstablishQueryContextRequest {
                        query_context: Some(encode_query_context_ref(request.context())),
                        catalog_set: Some(catalog_set),
                        initial_runtime_filter: Some(initial_runtime_filter),
                        initial_credential: Some(initial_credential),
                        initial_lease: Some(crate::lease::encode_lease_grant(
                            crate::lease::LeaseGrant::new(
                                request.initial_lease_sequence(),
                                request.initial_lease_valid_for(),
                            ),
                        )),
                        query_options: Some(query_options),
                        native_compatibility_id: Some(novarocks::NativeCompatibilityId {
                            value: native_compatibility_id.as_bytes().to_vec(),
                        }),
                        admission_ticket_id: Some(encode_admission_ticket_id(
                            request.admission_ticket_id(),
                        )),
                    },
                )),
            },
        )),
    }
}

/// Bundles operations into one per-backend batch, refusing anything that
/// exceeds the transport budget before it reaches the wire.
pub fn encode_operation_batch(
    operations: Vec<novarocks::TaskOperation>,
    budget: TransportBudget,
) -> Result<novarocks::ApplyTaskOperationsRequest, ProtocolError> {
    let request = novarocks::ApplyTaskOperationsRequest { operations };
    let encoded_len = request.encoded_len();
    if !budget.batch_fits(request.operations.len(), encoded_len) {
        return Err(out_of_range(
            FieldPath::root("apply_task_operations").field("operations"),
            "operation batch exceeds its item or byte budget",
        ));
    }
    Ok(request)
}

/// Encodes the closed control batch from typed FE operations. Refuses any
/// operation whose schema could carry plan or large domain content.
pub fn encode_control_operation_batch(
    operations: Vec<novarocks::TaskOperation>,
    budget: TransportBudget,
) -> Result<novarocks::ApplyTaskControlOperationsRequest, ProtocolError> {
    let path = FieldPath::root("apply_task_control_operations").field("operations");
    let mut controls = Vec::with_capacity(operations.len());
    for (index, operation) in operations.into_iter().enumerate() {
        let control = match operation.operation {
            Some(novarocks::task_operation::Operation::UpdateQueryContext(update)) => {
                match update.command {
                    Some(novarocks::update_query_context_request::Command::RenewLease(request)) => {
                        novarocks::task_control_operation::Control::RenewLease(request)
                    }
                    Some(novarocks::update_query_context_request::Command::Establish(_))
                    | Some(novarocks::update_query_context_request::Command::AdvanceDomain(_))
                    | None => {
                        return Err(invalid(
                            path.clone().index(index),
                            "only lease renewal may use the control method",
                        ));
                    }
                }
            }
            Some(novarocks::task_operation::Operation::CancelTask(request)) => {
                novarocks::task_control_operation::Control::CancelTask(request)
            }
            Some(novarocks::task_operation::Operation::AbortQueryContext(request)) => {
                novarocks::task_control_operation::Control::AbortQueryContext(request)
            }
            Some(novarocks::task_operation::Operation::ReleaseQueryContext(request)) => {
                novarocks::task_control_operation::Control::ReleaseQueryContext(request)
            }
            Some(novarocks::task_operation::Operation::QuiesceQueryContext(request)) => {
                novarocks::task_control_operation::Control::QuiesceQueryContext(request)
            }
            Some(novarocks::task_operation::Operation::AcquireQueryContextAdmissionTicket(_))
            | Some(novarocks::task_operation::Operation::CreateTask(_))
            | Some(novarocks::task_operation::Operation::UpdateTask(_))
            | None => {
                return Err(invalid(
                    path.clone().index(index),
                    "operation is not a small control command",
                ));
            }
        };
        controls.push(novarocks::TaskControlOperation {
            envelope: operation.envelope,
            control: Some(control),
        });
    }
    let request = novarocks::ApplyTaskControlOperationsRequest {
        operations: controls,
    };
    if !budget.batch_fits(request.operations.len(), request.encoded_len()) {
        return Err(out_of_range(
            path,
            "control batch exceeds its item or byte budget",
        ));
    }
    Ok(request)
}

/// Encodes the progression of one *accepted* domain.
///
/// Returns `None` for a conflict, which has no value on the wire because a
/// conflicting domain refuses its whole operation and carries no
/// acknowledgement body at all. Folding it into `APPLY` would report a refusal
/// as an application, so this refuses instead.
fn encode_accepted_progression(value: DomainProgression) -> Option<i32> {
    let encoded = match value {
        DomainProgression::Apply => novarocks::AcceptedDomainProgression::Apply,
        DomainProgression::Idempotent => novarocks::AcceptedDomainProgression::Idempotent,
        DomainProgression::Older => novarocks::AcceptedDomainProgression::Older,
        DomainProgression::Conflict(_) => return None,
    };
    Some(encoded as i32)
}

/// Decodes the progression of one accepted domain.
fn decode_accepted_progression(
    value: i32,
    path: FieldPath,
) -> Result<DomainProgression, ProtocolError> {
    match novarocks::AcceptedDomainProgression::try_from(value) {
        Ok(novarocks::AcceptedDomainProgression::Apply) => Ok(DomainProgression::Apply),
        Ok(novarocks::AcceptedDomainProgression::Idempotent) => Ok(DomainProgression::Idempotent),
        Ok(novarocks::AcceptedDomainProgression::Older) => Ok(DomainProgression::Older),
        Ok(novarocks::AcceptedDomainProgression::Unspecified) => Err(invalid_enum(
            path,
            "an accepted domain receipt requires a progression",
        )),
        Err(_) => Err(invalid_enum(
            path,
            "unknown accepted domain progression value",
        )),
    }
}

/// Reads the accepted version of one scalar-versioned domain receipt.
///
/// Version zero means nothing has been accepted yet, which is a legal state
/// rather than a malformed receipt.
fn decode_scalar_receipt(src: &novarocks::ScalarDomainReceipt) -> Option<DomainVersion> {
    DomainVersion::new(src.accepted_version).ok()
}

/// Decodes one task-scoped domain receipt.
pub fn decode_task_domain_receipt(
    src: &novarocks::TaskDomainReceipt,
    path: FieldPath,
) -> Result<TaskDomainReceipt, ProtocolError> {
    let progression =
        decode_accepted_progression(src.progression, path.clone().field("progression"))?;
    let receipt = src.receipt.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("receipt"),
            "a task domain receipt requires a typed body",
        )
    })?;
    match receipt {
        novarocks::task_domain_receipt::Receipt::SplitAssignment(split) => {
            let node_path = path.field("split_assignment").field("nodes");
            let mut nodes = Vec::with_capacity(split.nodes.len());
            for (index, node) in split.nodes.iter().enumerate() {
                nodes.push(decode_plan_node_split_receipt(
                    node,
                    node_path.clone().index(index),
                )?);
            }
            Ok(TaskDomainReceipt::SplitAssignment { nodes, progression })
        }
        novarocks::task_domain_receipt::Receipt::DynamicFilter(scalar) => {
            Ok(TaskDomainReceipt::TaskDynamicFilter {
                accepted_version: decode_scalar_receipt(scalar),
                progression,
            })
        }
        novarocks::task_domain_receipt::Receipt::OpenExchangeEdges(edges) => {
            let receipt_path = path.field("open_exchange_edges");
            let accepted_version =
                EdgeOpenVersion::new(edges.accepted_version).map_err(|error| {
                    invalid(
                        receipt_path.clone().field("accepted_version"),
                        error.to_string(),
                    )
                })?;
            let edge_path = receipt_path.field("opened_edge_ids");
            let mut opened = Vec::with_capacity(edges.opened_edge_ids.len());
            for (index, edge) in edges.opened_edge_ids.iter().enumerate() {
                opened.push(
                    ExchangeEdgeId::new(*edge).map_err(|error| {
                        invalid(edge_path.clone().index(index), error.to_string())
                    })?,
                );
            }
            Ok(TaskDomainReceipt::OpenExchangeEdges {
                accepted_version,
                opened,
                progression,
            })
        }
        novarocks::task_domain_receipt::Receipt::CloseExchangeDestination(scalar) => {
            let accepted_version =
                DomainVersion::new(scalar.accepted_version).map_err(|error| {
                    invalid(
                        path.field("close_exchange_destination")
                            .field("accepted_version"),
                        error.to_string(),
                    )
                })?;
            Ok(TaskDomainReceipt::CloseExchangeDestination {
                accepted_version,
                progression,
            })
        }
    }
}

/// Decodes one query-context domain receipt.
pub fn decode_query_context_domain_receipt(
    src: &novarocks::QueryContextDomainReceipt,
    path: FieldPath,
) -> Result<QueryContextDomainReceipt, ProtocolError> {
    let progression =
        decode_accepted_progression(src.progression, path.clone().field("progression"))?;
    let receipt = src.receipt.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("receipt"),
            "a query context domain receipt requires a typed body",
        )
    })?;
    match receipt {
        novarocks::query_context_domain_receipt::Receipt::CatalogBinding(scalar) => {
            Ok(QueryContextDomainReceipt::CatalogBinding {
                accepted_version: decode_scalar_receipt(scalar),
                progression,
            })
        }
        novarocks::query_context_domain_receipt::Receipt::SharedDynamicFilter(scalar) => {
            Ok(QueryContextDomainReceipt::SharedDynamicFilter {
                accepted_version: decode_scalar_receipt(scalar),
                progression,
            })
        }
        novarocks::query_context_domain_receipt::Receipt::Credential(credential) => {
            let credential_path = path.field("credential");
            let accepted_epoch =
                CredentialEpoch::new(credential.accepted_epoch).map_err(|error| {
                    invalid(
                        credential_path.clone().field("accepted_epoch"),
                        error.to_string(),
                    )
                })?;
            Ok(QueryContextDomainReceipt::Credential {
                lease_id: CredentialLeaseId::new(credential.lease_id),
                accepted_epoch,
                progression,
            })
        }
    }
}

/// Returns `None` when the receipt holds a conflict, which no acknowledgement
/// body can carry: a conflicting domain refuses its whole operation instead.
pub fn encode_task_domain_receipt(
    value: &TaskDomainReceipt,
) -> Option<novarocks::TaskDomainReceipt> {
    use novarocks_execution_contract::task_execution::operation::TaskDomainReceipt as Receipt;
    let progression = encode_accepted_progression(match value {
        Receipt::SplitAssignment { progression, .. }
        | Receipt::TaskDynamicFilter { progression, .. }
        | Receipt::OpenExchangeEdges { progression, .. }
        | Receipt::CloseExchangeDestination { progression, .. } => *progression,
    })?;
    let receipt = match value {
        Receipt::SplitAssignment { nodes, .. } => {
            novarocks::task_domain_receipt::Receipt::SplitAssignment(
                novarocks::TaskSplitAssignmentReceipt {
                    nodes: nodes
                        .iter()
                        .copied()
                        .map(crate::domain::encode_plan_node_split_receipt)
                        .collect(),
                },
            )
        }
        Receipt::TaskDynamicFilter {
            accepted_version, ..
        } => {
            novarocks::task_domain_receipt::Receipt::DynamicFilter(novarocks::ScalarDomainReceipt {
                accepted_version: accepted_version.map_or(0, DomainVersion::get),
            })
        }
        Receipt::OpenExchangeEdges {
            accepted_version,
            opened,
            ..
        } => novarocks::task_domain_receipt::Receipt::OpenExchangeEdges(
            novarocks::OpenExchangeEdgesReceipt {
                opened_edge_ids: opened.iter().map(|edge| edge.get()).collect(),
                accepted_version: accepted_version.get(),
            },
        ),
        Receipt::CloseExchangeDestination {
            accepted_version, ..
        } => novarocks::task_domain_receipt::Receipt::CloseExchangeDestination(
            novarocks::ScalarDomainReceipt {
                accepted_version: accepted_version.get(),
            },
        ),
    };
    Some(novarocks::TaskDomainReceipt {
        receipt: Some(receipt),
        progression,
    })
}

/// Encodes one query-context domain receipt.
/// Returns `None` when the receipt holds a conflict, for the same reason its
/// task-scoped sibling does.
pub fn encode_query_context_domain_receipt(
    value: &QueryContextDomainReceipt,
) -> Option<novarocks::QueryContextDomainReceipt> {
    use novarocks_execution_contract::task_execution::operation::QueryContextDomainReceipt as Receipt;
    let progression = encode_accepted_progression(match value {
        Receipt::CatalogBinding { progression, .. }
        | Receipt::SharedDynamicFilter { progression, .. }
        | Receipt::Credential { progression, .. } => *progression,
    })?;
    let receipt = match value {
        Receipt::CatalogBinding {
            accepted_version, ..
        } => novarocks::query_context_domain_receipt::Receipt::CatalogBinding(
            novarocks::ScalarDomainReceipt {
                accepted_version: accepted_version.map_or(0, DomainVersion::get),
            },
        ),
        Receipt::SharedDynamicFilter {
            accepted_version, ..
        } => novarocks::query_context_domain_receipt::Receipt::SharedDynamicFilter(
            novarocks::ScalarDomainReceipt {
                accepted_version: accepted_version.map_or(0, DomainVersion::get),
            },
        ),
        Receipt::Credential {
            lease_id,
            accepted_epoch,
            ..
        } => novarocks::query_context_domain_receipt::Receipt::Credential(
            novarocks::QueryContextCredentialReceipt {
                lease_id: lease_id.get(),
                accepted_epoch: accepted_epoch.get(),
            },
        ),
    };
    Some(novarocks::QueryContextDomainReceipt {
        receipt: Some(receipt),
        progression,
    })
}

/// Encodes a create acknowledgement.
///
/// Returns `None` when any accepted domain holds a progression the wire cannot
/// carry, so a conflict can never be shipped as part of an applied create.
pub fn encode_create_task_ack(value: &CreateTaskReceipt) -> Option<novarocks::CreateTaskAck> {
    Some(novarocks::CreateTaskAck {
        identity: Some(crate::identity::encode_task_identity(value.identity())),
        accepted_domains: encode_task_domain_receipts(value.domains())?,
        current_status: Some(crate::status::encode_task_status(value.current_status())),
    })
}

/// Encodes an update acknowledgement.
///
/// Returns `None` for the same reason a create acknowledgement does.
pub fn encode_update_task_ack(value: &UpdateTaskReceipt) -> Option<novarocks::UpdateTaskAck> {
    Some(novarocks::UpdateTaskAck {
        identity: Some(crate::identity::encode_task_identity(value.identity())),
        accepted_domains: encode_task_domain_receipts(value.domains())?,
    })
}

/// Encodes every task domain receipt, or nothing if one of them cannot be
/// represented. A partially encoded list would silently shorten an
/// acknowledgement, which reads as "that domain was never in the request".
fn encode_task_domain_receipts(
    values: &[TaskDomainReceipt],
) -> Option<Vec<novarocks::TaskDomainReceipt>> {
    values.iter().map(encode_task_domain_receipt).collect()
}

/// Decodes a create acknowledgement against the identity the caller sent.
///
/// The expected identity is the caller's, not the message's: a receipt that
/// agrees with itself proves nothing, and a create acknowledgement addressed
/// to a different task must not be read as this task's proof of installation.
pub fn decode_create_task_ack(
    src: &novarocks::CreateTaskAck,
    expected: TaskIdentity,
    path: FieldPath,
) -> Result<CreateTaskReceipt, ProtocolError> {
    let identity = decode_identity_field(
        src.identity.as_ref(),
        path.clone(),
        "a create acknowledgement requires a task identity",
    )?;
    if identity != expected {
        return Err(invalid(
            path.clone().field("identity"),
            "a create acknowledgement names a different task than the request",
        ));
    }
    let domains = decode_task_domain_receipts(&src.accepted_domains, path.clone())?;
    let status = src.current_status.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("current_status"),
            "a create acknowledgement requires the task's current status",
        )
    })?;
    let status_path = path.field("current_status");
    let status = crate::status::decode_task_status(status, status_path.clone())?;
    if status.identity() != expected {
        return Err(invalid(
            status_path.field("identity"),
            "a create acknowledgement carries the status of a different task",
        ));
    }
    Ok(CreateTaskReceipt::new(identity, domains, status))
}

/// Decodes an update acknowledgement against the identity the caller sent.
pub fn decode_update_task_ack(
    src: &novarocks::UpdateTaskAck,
    expected: TaskIdentity,
    path: FieldPath,
) -> Result<UpdateTaskReceipt, ProtocolError> {
    let identity = decode_identity_field(
        src.identity.as_ref(),
        path.clone(),
        "an update acknowledgement requires a task identity",
    )?;
    if identity != expected {
        return Err(invalid(
            path.clone().field("identity"),
            "an update acknowledgement names a different task than the request",
        ));
    }
    let domains = decode_task_domain_receipts(&src.accepted_domains, path)?;
    Ok(UpdateTaskReceipt::new(identity, domains))
}

/// Decodes an admission-ticket acknowledgement against the exact request.
///
/// An opaque ticket id proves nothing by itself. The repeated context and
/// requested validity must match before the caller may use the ticket to
/// establish a query context.
pub fn decode_query_context_admission_ticket_ack(
    src: &novarocks::QueryContextAdmissionTicketAck,
    expected: AcquireQueryContextAdmissionTicket,
    path: FieldPath,
) -> Result<QueryContextAdmissionTicketReceipt, ProtocolError> {
    let ticket_id = src.ticket_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("ticket_id"),
            "an admission ticket acknowledgement requires a ticket id",
        )
    })?;
    let ticket_id = decode_admission_ticket_id(ticket_id, path.clone().field("ticket_id"))?;

    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "an admission ticket acknowledgement requires a context reference",
        )
    })?;
    let context = decode_query_context_ref(context, path.clone().field("query_context"))?;
    if context != expected.context() {
        return Err(invalid(
            path.clone().field("query_context"),
            "an admission ticket acknowledgement names a different context than the request",
        ));
    }

    let valid_for = decode_duration_millis(
        src.valid_for_millis,
        path.clone().field("valid_for_millis"),
        LeaseValidFor::MAX_REPRESENTABLE,
    )?;
    let valid_for = LeaseValidFor::new(valid_for)
        .map_err(|error| invalid(path.clone().field("valid_for_millis"), error.to_string()))?;
    if valid_for != expected.valid_for() {
        return Err(invalid(
            path.field("valid_for_millis"),
            "an admission ticket acknowledgement reports a different validity than the request",
        ));
    }

    Ok(QueryContextAdmissionTicketReceipt::new(
        ticket_id, context, valid_for,
    ))
}

fn decode_task_domain_receipts(
    src: &[novarocks::TaskDomainReceipt],
    path: FieldPath,
) -> Result<Vec<TaskDomainReceipt>, ProtocolError> {
    let domain_path = path.field("accepted_domains");
    if src.len() > MAX_DOMAIN_UPDATES {
        return Err(out_of_range(
            domain_path,
            "an acknowledgement reports more domains than one operation may carry",
        ));
    }
    let mut domains = Vec::with_capacity(src.len());
    for (index, domain) in src.iter().enumerate() {
        domains.push(decode_task_domain_receipt(
            domain,
            domain_path.clone().index(index),
        )?);
    }
    Ok(domains)
}

/// Decodes a query-context acknowledgement against the context the caller
/// sent, returning the receipt and the termination cause the backend reported.
///
/// The cause is returned rather than validated and dropped: it is the only
/// statement of why a context the frontend still believed in is gone.
pub fn decode_query_context_ack(
    src: &novarocks::QueryContextAck,
    expected: QueryContextRef,
    path: FieldPath,
) -> Result<
    (
        QueryContextReceipt,
        Option<novarocks_execution_contract::task_execution::status::AbortCause>,
    ),
    ProtocolError,
> {
    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "a query context acknowledgement requires a context reference",
        )
    })?;
    let context = decode_query_context_ref(context, path.clone().field("query_context"))?;
    if context != expected {
        return Err(invalid(
            path.clone().field("query_context"),
            "a query context acknowledgement names a different context than the request",
        ));
    }
    let state = decode_context_state(src.state, path.clone().field("state"))?;
    let mut receipt = QueryContextReceipt::new(context, state);
    if let Some(lease) = src.lease.as_ref() {
        receipt = receipt.with_lease(crate::lease::decode_lease_receipt(
            lease,
            path.clone().field("lease"),
        )?);
    }
    let domain_path = path.clone().field("accepted_domains");
    if src.accepted_domains.len() > MAX_DOMAIN_UPDATES {
        return Err(out_of_range(
            domain_path,
            "an acknowledgement reports more domains than one operation may carry",
        ));
    }
    let mut domains = Vec::with_capacity(src.accepted_domains.len());
    for (index, domain) in src.accepted_domains.iter().enumerate() {
        domains.push(decode_query_context_domain_receipt(
            domain,
            domain_path.clone().index(index),
        )?);
    }
    receipt = receipt.with_domains(domains);
    let cause = src
        .termination_cause
        .map(|cause| decode_abort_cause(cause, path.field("termination_cause")))
        .transpose()?;
    Ok((receipt, cause))
}

/// Encodes a query-context acknowledgement.
///
/// Returns `None` when the receipt reports the one state that has no wire
/// representation.
pub fn encode_query_context_ack(
    value: &novarocks_execution_contract::task_execution::operation::QueryContextReceipt,
    termination_cause: Option<novarocks_execution_contract::task_execution::status::AbortCause>,
) -> Option<novarocks::QueryContextAck> {
    Some(novarocks::QueryContextAck {
        query_context: Some(encode_query_context_ref(value.context())),
        state: encode_context_state(value.state())?,
        lease: value.lease().map(crate::lease::encode_lease_receipt),
        accepted_domains: value
            .domains()
            .iter()
            .map(encode_query_context_domain_receipt)
            .collect::<Option<Vec<_>>>()?,
        termination_cause: termination_cause.map(encode_abort_cause),
    })
}

/// Encodes the exact worker admission grant returned for one acquisition.
pub fn encode_query_context_admission_ticket_ack(
    value: QueryContextAdmissionTicketReceipt,
) -> novarocks::QueryContextAdmissionTicketAck {
    novarocks::QueryContextAdmissionTicketAck {
        ticket_id: Some(encode_admission_ticket_id(value.ticket_id())),
        query_context: Some(encode_query_context_ref(value.context())),
        valid_for_millis: value.valid_for().get().as_millis() as u64,
    }
}

/// Encodes one Worker operation receipt.
pub fn encode_receipt(
    operation_id: TaskOperationId,
    outcome: OperationOutcome,
    safe_detail: &str,
    ack: Option<novarocks::task_operation_receipt::Ack>,
) -> novarocks::TaskOperationReceipt {
    novarocks::TaskOperationReceipt {
        operation_id: Some(encode_task_operation_id(operation_id)),
        outcome: encode_outcome(outcome),
        safe_detail: safe_detail.to_owned(),
        safe_field_path: None,
        ack,
    }
}

/// Encodes one status stream event.
pub fn encode_status_event(
    value: &novarocks_execution_contract::task_execution::status::TaskStatus,
) -> novarocks::TaskStatusStreamEvent {
    novarocks::TaskStatusStreamEvent {
        event: Some(novarocks::task_status_stream_event::Event::TaskStatus(
            crate::status::encode_task_status(value),
        )),
        source_revision: None,
    }
}

/// Encodes one worker query-context convergence event.
pub fn encode_context_convergence_event(
    value: novarocks_execution_contract::task_execution::context_convergence::QueryContextConvergenceReceipt,
) -> novarocks::TaskStatusStreamEvent {
    novarocks::TaskStatusStreamEvent {
        event: Some(
            novarocks::task_status_stream_event::Event::ContextConvergence(
                crate::context_convergence::encode_query_context_convergence_receipt(value),
            ),
        ),
        source_revision: None,
    }
}

/// Encodes a task-gone event.
pub fn encode_task_gone_event(identity: TaskIdentity) -> novarocks::TaskStatusStreamEvent {
    novarocks::TaskStatusStreamEvent {
        event: Some(novarocks::task_status_stream_event::Event::TaskGone(
            novarocks::TaskGone {
                identity: Some(crate::identity::encode_task_identity(identity)),
            },
        )),
        source_revision: None,
    }
}

/// Decodes one status stream event.
pub fn decode_status_event(
    src: &novarocks::TaskStatusStreamEvent,
    path: FieldPath,
) -> Result<StatusStreamEvent, ProtocolError> {
    if src.source_revision.is_some() {
        return Err(invalid(
            path.clone().field("source_revision"),
            "covered observations require the covered decoder",
        ));
    }
    let event = src
        .event
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "a status event requires a body"))?;
    match event {
        novarocks::task_status_stream_event::Event::TaskStatus(status) => {
            Ok(StatusStreamEvent::Status(
                crate::status::decode_task_status(status, path.field("task_status"))?,
            ))
        }
        novarocks::task_status_stream_event::Event::TaskGone(gone) => {
            let gone_path = path.field("task_gone");
            let identity = decode_identity_field(
                gone.identity.as_ref(),
                gone_path,
                "a task-gone event requires a task identity",
            )?;
            Ok(StatusStreamEvent::Gone(identity))
        }
        novarocks::task_status_stream_event::Event::ContextConvergence(_) => Err(invalid(
            path.field("context_convergence"),
            "context convergence events require the convergence-aware decoder",
        )),
        _ => Err(invalid(
            path,
            "covered observations require the covered decoder",
        )),
    }
}

/// One observed status event.
#[derive(Clone, Debug)]
pub enum StatusStreamEvent {
    Status(novarocks_execution_contract::task_execution::status::TaskStatus),
    Gone(TaskIdentity),
}

/// One event from a convergence-aware task-status subscription.
#[derive(Clone, Debug)]
pub enum ContextAwareStatusStreamEvent {
    Status(novarocks_execution_contract::task_execution::status::TaskStatus),
    Gone(TaskIdentity),
    ContextConvergence(
        novarocks_execution_contract::task_execution::context_convergence::QueryContextConvergenceReceipt,
    ),
}

/// Decodes one task or query-context observation from a convergence-aware stream.
pub fn decode_context_aware_status_event(
    src: &novarocks::TaskStatusStreamEvent,
    path: FieldPath,
) -> Result<ContextAwareStatusStreamEvent, ProtocolError> {
    if src.source_revision.is_some() {
        return Err(invalid(
            path.clone().field("source_revision"),
            "covered observations require the covered decoder",
        ));
    }
    let event = src
        .event
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "a status event requires a body"))?;
    match event {
        novarocks::task_status_stream_event::Event::TaskStatus(status) => {
            Ok(ContextAwareStatusStreamEvent::Status(
                crate::status::decode_task_status(status, path.field("task_status"))?,
            ))
        }
        novarocks::task_status_stream_event::Event::TaskGone(gone) => {
            let gone_path = path.field("task_gone");
            let identity = decode_identity_field(
                gone.identity.as_ref(),
                gone_path,
                "a task-gone event requires a task identity",
            )?;
            Ok(ContextAwareStatusStreamEvent::Gone(identity))
        }
        novarocks::task_status_stream_event::Event::ContextConvergence(receipt) => {
            Ok(ContextAwareStatusStreamEvent::ContextConvergence(
                crate::context_convergence::decode_query_context_convergence_receipt(
                    receipt,
                    path.field("context_convergence"),
                )?,
            ))
        }
        _ => Err(invalid(
            path,
            "covered observations require the covered decoder",
        )),
    }
}

/// Encodes a status subscription request.
pub fn encode_subscribe_task_status(
    context: QueryContextRef,
    cursors: &[novarocks_execution_contract::task_execution::status::TaskStatusCursor],
) -> Result<novarocks::SubscribeTaskStatusRequest, ProtocolError> {
    if cursors.len() > crate::status::MAX_SUBSCRIPTION_CURSORS {
        return Err(out_of_range(
            FieldPath::root("subscribe_task_status").field("cursors"),
            "cursor count exceeds the hard limit",
        ));
    }
    Ok(novarocks::SubscribeTaskStatusRequest {
        query_context: Some(encode_query_context_ref(context)),
        cursors: cursors
            .iter()
            .copied()
            .map(crate::status::encode_task_status_cursor)
            .collect(),
        context_convergence_cursor: None,
        generation: 0,
        task_convergence_cursors: Vec::new(),
        required_identities: Vec::new(),
        quiesce_cursor: None,
    })
}

/// Encodes a status subscription with an optional exact context cursor.
pub fn encode_context_aware_subscribe_task_status(
    context: QueryContextRef,
    cursors: &[novarocks_execution_contract::task_execution::status::TaskStatusCursor],
    context_convergence_cursor: Option<
        novarocks_execution_contract::task_execution::context_convergence::QueryContextConvergenceCursor,
    >,
) -> Result<novarocks::SubscribeTaskStatusRequest, ProtocolError> {
    let mut request = encode_subscribe_task_status(context, cursors)?;
    request.context_convergence_cursor = context_convergence_cursor
        .map(crate::context_convergence::encode_query_context_convergence_cursor);
    Ok(request)
}

/// Decodes a status subscription request.
pub fn decode_subscribe_task_status(
    src: &novarocks::SubscribeTaskStatusRequest,
    path: FieldPath,
) -> Result<
    (
        QueryContextRef,
        Vec<novarocks_execution_contract::task_execution::status::TaskStatusCursor>,
    ),
    ProtocolError,
> {
    if src.context_convergence_cursor.is_some() {
        return Err(invalid(
            path.clone().field("context_convergence_cursor"),
            "context convergence cursor requires the convergence-aware decoder",
        ));
    }
    let (context, cursors, _) = decode_context_aware_subscribe_task_status(src, path)?;
    Ok((context, cursors))
}

/// Decodes a status subscription and preserves its exact context cursor.
pub fn decode_context_aware_subscribe_task_status(
    src: &novarocks::SubscribeTaskStatusRequest,
    path: FieldPath,
) -> Result<
    (
        QueryContextRef,
        Vec<novarocks_execution_contract::task_execution::status::TaskStatusCursor>,
        Option<
            novarocks_execution_contract::task_execution::context_convergence::QueryContextConvergenceCursor,
        >,
    ),
    ProtocolError,
>{
    if src.generation != 0
        || !src.task_convergence_cursors.is_empty()
        || !src.required_identities.is_empty()
        || src.quiesce_cursor.is_some()
    {
        return Err(invalid(
            path.clone(),
            "covered subscription fields require the covered decoder",
        ));
    }
    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "a subscription requires a query context reference",
        )
    })?;
    let context = decode_query_context_ref(context, path.clone().field("query_context"))?;
    if src.cursors.len() > crate::status::MAX_SUBSCRIPTION_CURSORS {
        return Err(out_of_range(
            path.clone().field("cursors"),
            "cursor count exceeds the hard limit",
        ));
    }
    let mut cursors = Vec::with_capacity(src.cursors.len());
    for (index, cursor) in src.cursors.iter().enumerate() {
        cursors.push(crate::status::decode_task_status_cursor(
            cursor,
            path.clone().field("cursors").index(index),
        )?);
    }
    let context_convergence_cursor = src
        .context_convergence_cursor
        .as_ref()
        .map(|cursor| {
            crate::context_convergence::decode_query_context_convergence_cursor(
                cursor,
                path.field("context_convergence_cursor"),
            )
        })
        .transpose()?;
    Ok((context, cursors, context_convergence_cursor))
}

/// The optional quiesce fence already observed by one reconnecting FE.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct QuiesceObservationCursor {
    pub context: QueryContextRef,
    pub fence_version: Option<NonZeroU64>,
}

/// A validated covered subscription request. Future versions are checked by
/// the Worker source against its latest retained facts after decoding.
#[derive(Clone, Debug)]
pub struct DecodedCoveredSubscription {
    pub context: QueryContextRef,
    pub generation: NonZeroU64,
    pub status_cursors: Vec<TaskStatusCursor>,
    pub task_convergence_cursors: Vec<TaskConvergenceCursor>,
    pub context_cursor: Option<QueryContextConvergenceCursor>,
    pub quiesce_cursor: Option<QuiesceObservationCursor>,
    pub required_identities: Vec<TaskIdentity>,
}

fn check_covered_identity(
    context: QueryContextRef,
    identity: TaskIdentity,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    identity
        .verify_query_context(context)
        .map_err(|error| invalid(path, error.to_string()))
}

/// Decodes the covered mode of the existing subscription RPC.
pub fn decode_covered_subscribe_task_status(
    src: &novarocks::SubscribeTaskStatusRequest,
    path: FieldPath,
) -> Result<DecodedCoveredSubscription, ProtocolError> {
    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "subscription requires a context",
        )
    })?;
    let context = decode_query_context_ref(context, path.clone().field("query_context"))?;
    let generation = NonZeroU64::new(src.generation).ok_or_else(|| {
        invalid(
            path.clone().field("generation"),
            "covered generation must be nonzero",
        )
    })?;
    let bound = crate::status::MAX_SUBSCRIPTION_CURSORS;
    if src.cursors.len() > bound
        || src.task_convergence_cursors.len() > bound
        || src.required_identities.len() > bound
    {
        return Err(out_of_range(
            path.clone(),
            "covered subscription exceeds task bound",
        ));
    }
    let mut union = BTreeSet::new();
    let mut status_seen = BTreeSet::new();
    let mut status_cursors = Vec::with_capacity(src.cursors.len());
    for (index, wire) in src.cursors.iter().enumerate() {
        let item_path = path.clone().field("cursors").index(index);
        let cursor = crate::status::decode_task_status_cursor(wire, item_path.clone())?;
        check_covered_identity(context, cursor.identity(), item_path.clone())?;
        if !status_seen.insert(cursor.identity()) {
            return Err(invalid(item_path, "duplicate task status cursor"));
        }
        union.insert(cursor.identity());
        status_cursors.push(cursor);
    }
    let mut convergence_seen = BTreeSet::new();
    let mut task_convergence_cursors = Vec::with_capacity(src.task_convergence_cursors.len());
    for (index, wire) in src.task_convergence_cursors.iter().enumerate() {
        let item_path = path.clone().field("task_convergence_cursors").index(index);
        let identity = wire.identity.as_ref().ok_or_else(|| {
            missing(
                item_path.clone().field("identity"),
                "convergence cursor requires identity",
            )
        })?;
        let identity = decode_task_identity(identity, item_path.clone().field("identity"))?;
        check_covered_identity(context, identity, item_path.clone())?;
        if !convergence_seen.insert(identity) {
            return Err(invalid(item_path, "duplicate task convergence cursor"));
        }
        let cursor = if wire.current_version == 0 {
            TaskConvergenceCursor::unobserved(identity)
        } else {
            let version = TaskConvergenceVersion::new(wire.current_version).map_err(|error| {
                invalid(
                    item_path.clone().field("current_version"),
                    error.to_string(),
                )
            })?;
            TaskConvergenceCursor::at(identity, version)
        };
        union.insert(identity);
        task_convergence_cursors.push(cursor);
    }
    let mut required_seen = BTreeSet::new();
    let mut required_identities = Vec::with_capacity(src.required_identities.len());
    for (index, wire) in src.required_identities.iter().enumerate() {
        let item_path = path.clone().field("required_identities").index(index);
        let identity = decode_task_identity(wire, item_path.clone())?;
        check_covered_identity(context, identity, item_path.clone())?;
        if !required_seen.insert(identity) {
            return Err(invalid(item_path, "duplicate required identity"));
        }
        union.insert(identity);
        required_identities.push(identity);
    }
    if union.len() > bound {
        return Err(out_of_range(
            path.clone(),
            "covered target union exceeds task bound",
        ));
    }
    let context_cursor = src
        .context_convergence_cursor
        .as_ref()
        .map(|wire| {
            crate::context_convergence::decode_query_context_convergence_cursor(
                wire,
                path.clone().field("context_convergence_cursor"),
            )
        })
        .transpose()?;
    if context_cursor.is_some_and(|cursor| cursor.context() != context) {
        return Err(invalid(
            path.clone().field("context_convergence_cursor"),
            "context convergence cursor names another context",
        ));
    }
    let quiesce_cursor = src
        .quiesce_cursor
        .as_ref()
        .map(|wire| {
            let cursor_path = path.clone().field("quiesce_cursor");
            let cursor_context = wire.query_context.as_ref().ok_or_else(|| {
                missing(
                    cursor_path.clone().field("query_context"),
                    "quiesce cursor requires context",
                )
            })?;
            let cursor_context = decode_query_context_ref(
                cursor_context,
                cursor_path.clone().field("query_context"),
            )?;
            if cursor_context != context {
                return Err(invalid(cursor_path, "quiesce cursor names another context"));
            }
            Ok(QuiesceObservationCursor {
                context: cursor_context,
                fence_version: NonZeroU64::new(wire.fence_version),
            })
        })
        .transpose()?;
    Ok(DecodedCoveredSubscription {
        context,
        generation,
        status_cursors,
        task_convergence_cursors,
        context_cursor,
        quiesce_cursor,
        required_identities,
    })
}

/// Encodes a typed covered request and revalidates its identity relationships.
pub fn encode_covered_subscribe_task_status(
    request: &DecodedCoveredSubscription,
) -> Result<novarocks::SubscribeTaskStatusRequest, ProtocolError> {
    let wire = novarocks::SubscribeTaskStatusRequest {
        query_context: Some(encode_query_context_ref(request.context)),
        cursors: request
            .status_cursors
            .iter()
            .copied()
            .map(crate::status::encode_task_status_cursor)
            .collect(),
        context_convergence_cursor: request
            .context_cursor
            .map(crate::context_convergence::encode_query_context_convergence_cursor),
        generation: request.generation.get(),
        task_convergence_cursors: request
            .task_convergence_cursors
            .iter()
            .map(|cursor| novarocks::TaskConvergenceCursor {
                identity: Some(encode_task_identity(cursor.identity())),
                current_version: cursor
                    .current_version()
                    .map_or(0, TaskConvergenceVersion::get),
            })
            .collect(),
        required_identities: request
            .required_identities
            .iter()
            .copied()
            .map(encode_task_identity)
            .collect(),
        quiesce_cursor: request
            .quiesce_cursor
            .map(|cursor| novarocks::QuiesceQueryContextCursor {
                query_context: Some(encode_query_context_ref(cursor.context)),
                fence_version: cursor.fence_version.map_or(0, NonZeroU64::get),
            }),
    };
    decode_covered_subscribe_task_status(&wire, FieldPath::root("covered_subscribe_task_status"))?;
    Ok(wire)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CoveredCatchUpComplete {
    pub generation: u64,
    pub initial_cut: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CoveredObservationBookmark {
    pub generation: u64,
    pub sequence: u64,
    pub covered_prefix: u64,
    pub source_cut: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoveredStatusStreamFact {
    Status(TaskStatus),
    Gone(TaskIdentity),
    ContextConvergence(QueryContextConvergenceReceipt),
    TaskConvergence(TaskConvergenceReceipt),
    Quiesce(QuiesceQueryContextReceipt),
    StatusUnchanged(TaskIdentity),
    TaskConvergenceUnchanged(TaskIdentity),
    Unknown(TaskIdentity),
    CatchUpComplete(CoveredCatchUpComplete),
    Bookmark(CoveredObservationBookmark),
}

/// `source_revision` is present only for a live fact. A catch-up fact has no
/// revision of its own; CatchUpComplete and Bookmark carry their explicit cut.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoveredStatusStreamEvent {
    pub fact: CoveredStatusStreamFact,
    pub source_revision: Option<u64>,
}

fn decode_covered_identity(
    wire: Option<&novarocks::TaskIdentity>,
    path: FieldPath,
) -> Result<TaskIdentity, ProtocolError> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "observation requires task identity"))?;
    decode_task_identity(wire, path)
}

/// Decodes a covered observation without accepting zero/default evidence.
pub fn decode_covered_status_event(
    src: &novarocks::TaskStatusStreamEvent,
    path: FieldPath,
) -> Result<CoveredStatusStreamEvent, ProtocolError> {
    let event = src
        .event
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "covered observation requires a body"))?;
    if src.source_revision == Some(0) {
        return Err(invalid(
            path.clone().field("source_revision"),
            "live revision must be nonzero",
        ));
    }
    let fact = match event {
        novarocks::task_status_stream_event::Event::TaskStatus(value) => {
            CoveredStatusStreamFact::Status(crate::status::decode_task_status(
                value,
                path.clone().field("task_status"),
            )?)
        }
        novarocks::task_status_stream_event::Event::TaskGone(value) => {
            CoveredStatusStreamFact::Gone(decode_covered_identity(
                value.identity.as_ref(),
                path.clone().field("task_gone").field("identity"),
            )?)
        }
        novarocks::task_status_stream_event::Event::ContextConvergence(value) => {
            CoveredStatusStreamFact::ContextConvergence(
                crate::context_convergence::decode_query_context_convergence_receipt(
                    value,
                    path.clone().field("context_convergence"),
                )?,
            )
        }
        novarocks::task_status_stream_event::Event::TaskConvergence(value) => {
            let item_path = path.clone().field("task_convergence");
            let identity = decode_covered_identity(
                value.identity.as_ref(),
                item_path.clone().field("identity"),
            )?;
            let version = TaskConvergenceVersion::new(value.version)
                .map_err(|error| invalid(item_path.field("version"), error.to_string()))?;
            CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                identity, version,
            ))
        }
        novarocks::task_status_stream_event::Event::Quiesce(value) => {
            let ack = novarocks::QuiesceQueryContextAck {
                query_context: value.query_context.clone(),
                fence_version: value.fence_version,
                accepted_tasks: value.accepted_tasks.clone(),
                state: value.state,
            };
            CoveredStatusStreamFact::Quiesce(decode_quiesce_ack(
                &ack,
                path.clone().field("quiesce"),
            )?)
        }
        novarocks::task_status_stream_event::Event::TaskStatusUnchanged(value) => {
            CoveredStatusStreamFact::StatusUnchanged(decode_covered_identity(
                value.identity.as_ref(),
                path.clone()
                    .field("task_status_unchanged")
                    .field("identity"),
            )?)
        }
        novarocks::task_status_stream_event::Event::TaskConvergenceUnchanged(value) => {
            CoveredStatusStreamFact::TaskConvergenceUnchanged(decode_covered_identity(
                value.identity.as_ref(),
                path.clone()
                    .field("task_convergence_unchanged")
                    .field("identity"),
            )?)
        }
        novarocks::task_status_stream_event::Event::TaskUnknown(value) => {
            CoveredStatusStreamFact::Unknown(decode_covered_identity(
                value.identity.as_ref(),
                path.clone().field("task_unknown").field("identity"),
            )?)
        }
        novarocks::task_status_stream_event::Event::CatchUpComplete(value) => {
            if value.generation == 0 {
                return Err(invalid(
                    path.clone().field("catch_up_complete").field("generation"),
                    "catch-up generation must be nonzero",
                ));
            }
            CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                generation: value.generation,
                initial_cut: value.initial_cut,
            })
        }
        novarocks::task_status_stream_event::Event::Bookmark(value) => {
            if value.generation == 0
                || value.sequence == 0
                || value.covered_prefix > value.source_cut
            {
                return Err(invalid(
                    path.clone().field("bookmark"),
                    "bookmark requires nonzero generation and sequence, with covered prefix at or below cut",
                ));
            }
            CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: value.generation,
                sequence: value.sequence,
                covered_prefix: value.covered_prefix,
                source_cut: value.source_cut,
            })
        }
    };
    if matches!(
        fact,
        CoveredStatusStreamFact::StatusUnchanged(_)
            | CoveredStatusStreamFact::TaskConvergenceUnchanged(_)
            | CoveredStatusStreamFact::Unknown(_)
            | CoveredStatusStreamFact::CatchUpComplete(_)
            | CoveredStatusStreamFact::Bookmark(_)
    ) && src.source_revision.is_some()
    {
        return Err(invalid(
            path.field("source_revision"),
            "catch-up marker and bookmark frames cannot carry a live revision",
        ));
    }
    Ok(CoveredStatusStreamEvent {
        fact,
        source_revision: src.source_revision,
    })
}

/// Encodes a covered frame and validates the result through the same decoder.
pub fn encode_covered_status_event(
    value: &CoveredStatusStreamEvent,
) -> Result<novarocks::TaskStatusStreamEvent, ProtocolError> {
    use novarocks::task_status_stream_event::Event;
    let event = match &value.fact {
        CoveredStatusStreamFact::Status(status) => {
            Event::TaskStatus(crate::status::encode_task_status(status))
        }
        CoveredStatusStreamFact::Gone(identity) => Event::TaskGone(novarocks::TaskGone {
            identity: Some(encode_task_identity(*identity)),
        }),
        CoveredStatusStreamFact::ContextConvergence(receipt) => Event::ContextConvergence(
            crate::context_convergence::encode_query_context_convergence_receipt(*receipt),
        ),
        CoveredStatusStreamFact::TaskConvergence(receipt) => {
            Event::TaskConvergence(novarocks::TaskConvergenceReceipt {
                identity: Some(encode_task_identity(receipt.identity())),
                version: receipt.version().get(),
            })
        }
        CoveredStatusStreamFact::Quiesce(receipt) => {
            let ack = encode_quiesce_ack(receipt).ok_or_else(|| {
                invalid(
                    FieldPath::root("covered_status_event").field("quiesce"),
                    "quiesce requires a closing state",
                )
            })?;
            Event::Quiesce(novarocks::QuiesceQueryContextObservation {
                query_context: ack.query_context,
                fence_version: ack.fence_version,
                accepted_tasks: ack.accepted_tasks,
                state: ack.state,
            })
        }
        CoveredStatusStreamFact::StatusUnchanged(identity) => {
            Event::TaskStatusUnchanged(novarocks::TaskStatusUnchanged {
                identity: Some(encode_task_identity(*identity)),
            })
        }
        CoveredStatusStreamFact::TaskConvergenceUnchanged(identity) => {
            Event::TaskConvergenceUnchanged(novarocks::TaskConvergenceUnchanged {
                identity: Some(encode_task_identity(*identity)),
            })
        }
        CoveredStatusStreamFact::Unknown(identity) => Event::TaskUnknown(novarocks::TaskUnknown {
            identity: Some(encode_task_identity(*identity)),
        }),
        CoveredStatusStreamFact::CatchUpComplete(value) => {
            Event::CatchUpComplete(novarocks::CatchUpComplete {
                generation: value.generation,
                initial_cut: value.initial_cut,
            })
        }
        CoveredStatusStreamFact::Bookmark(value) => {
            Event::Bookmark(novarocks::ObservationBookmark {
                generation: value.generation,
                sequence: value.sequence,
                covered_prefix: value.covered_prefix,
                source_cut: value.source_cut,
            })
        }
    };
    let wire = novarocks::TaskStatusStreamEvent {
        event: Some(event),
        source_revision: value.source_revision,
    };
    decode_covered_status_event(&wire, FieldPath::root("covered_status_event"))?;
    Ok(wire)
}

#[cfg(test)]
mod covered_wire_tests {
    use super::*;
    use novarocks_execution_contract::task_execution::context_convergence::{
        QueryContextConvergenceState, QueryContextConvergenceVersion,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn context_and_task() -> (QueryContextRef, TaskIdentity) {
        let execution =
            QueryExecutionId::new(QueryId::new(13, 14), AttemptId::new(1).unwrap()).unwrap();
        let backend = BackendProcessId::new_v7();
        (
            QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend),
            TaskIdentity::new(
                execution,
                StageId::new(1).unwrap(),
                TaskId::new(2).unwrap(),
                backend,
            ),
        )
    }

    fn request(context: QueryContextRef, task: TaskIdentity) -> DecodedCoveredSubscription {
        DecodedCoveredSubscription {
            context,
            generation: NonZeroU64::new(7).unwrap(),
            status_cursors: vec![TaskStatusCursor::unobserved(task)],
            task_convergence_cursors: vec![TaskConvergenceCursor::unobserved(task)],
            context_cursor: Some(QueryContextConvergenceCursor::unobserved(context)),
            quiesce_cursor: Some(QuiesceObservationCursor {
                context,
                fence_version: None,
            }),
            required_identities: vec![task],
        }
    }

    #[test]
    fn covered_request_roundtrip_and_hybrid_rejection() {
        let (context, task) = context_and_task();
        let wire = encode_covered_subscribe_task_status(&request(context, task)).unwrap();
        let decoded =
            decode_covered_subscribe_task_status(&wire, FieldPath::root("covered_request"))
                .unwrap();
        assert_eq!(decoded.generation.get(), 7);
        assert_eq!(decoded.status_cursors[0].identity(), task);
        assert_eq!(decoded.task_convergence_cursors[0].identity(), task);
        assert_eq!(decoded.quiesce_cursor.unwrap().context, context);
        assert!(
            decode_context_aware_subscribe_task_status(&wire, FieldPath::root("legacy_request"),)
                .is_err()
        );
        let mut zero = wire.clone();
        zero.generation = 0;
        assert!(decode_covered_subscribe_task_status(&zero, FieldPath::root("zero")).is_err());
        let legacy = encode_subscribe_task_status(context, &[]).unwrap();
        assert!(decode_covered_subscribe_task_status(&legacy, FieldPath::root("legacy")).is_err());
    }

    #[test]
    fn covered_request_rejects_identity_context_and_duplicate_targets() {
        let (context, task) = context_and_task();
        let (other_context, other_task) = context_and_task();
        let mut wire = encode_covered_subscribe_task_status(&request(context, task)).unwrap();
        wire.required_identities
            .push(encode_task_identity(other_task));
        assert!(decode_covered_subscribe_task_status(&wire, FieldPath::root("mismatch")).is_err());
        wire.required_identities.pop();
        wire.required_identities.push(encode_task_identity(task));
        assert!(decode_covered_subscribe_task_status(&wire, FieldPath::root("duplicate")).is_err());
        wire.required_identities.pop();
        wire.quiesce_cursor.as_mut().unwrap().query_context =
            Some(encode_query_context_ref(other_context));
        assert!(
            decode_covered_subscribe_task_status(&wire, FieldPath::root("quiesce_mismatch"))
                .is_err()
        );
    }

    #[test]
    fn covered_frame_roundtrip_and_nonzero_rejections() {
        let (context, task) = context_and_task();
        let frames = [
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Status(TaskStatus::created(task)),
                source_revision: Some(1),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::TaskConvergence(
                    TaskConvergenceReceipt::actual_stopped(task, TaskConvergenceVersion::FIRST),
                ),
                source_revision: Some(2),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::ContextConvergence(
                    QueryContextConvergenceReceipt::new(
                        context,
                        QueryContextConvergenceVersion::FIRST,
                        QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                    ),
                ),
                source_revision: Some(3),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Quiesce(QuiesceQueryContextReceipt::new(
                    context,
                    1,
                    vec![task],
                    QueryContextState::Quiescing,
                )),
                source_revision: Some(4),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::StatusUnchanged(task),
                source_revision: None,
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::TaskConvergenceUnchanged(task),
                source_revision: None,
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Unknown(task),
                source_revision: None,
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Gone(task),
                source_revision: Some(5),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 7,
                    initial_cut: 4,
                }),
                source_revision: None,
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                    generation: 7,
                    sequence: 1,
                    covered_prefix: 4,
                    source_cut: 5,
                }),
                source_revision: None,
            },
        ];
        for frame in &frames {
            let wire = encode_covered_status_event(frame).unwrap();
            let decoded = decode_covered_status_event(&wire, FieldPath::root("frame")).unwrap();
            assert_eq!(&decoded, frame);
        }
        let mut zero_revision = encode_covered_status_event(&frames[0]).unwrap();
        zero_revision.source_revision = Some(0);
        assert!(decode_covered_status_event(&zero_revision, FieldPath::root("zero_rev")).is_err());
        let mut bad_bookmark = encode_covered_status_event(frames.last().unwrap()).unwrap();
        bad_bookmark.source_revision = Some(6);
        assert!(
            decode_covered_status_event(&bad_bookmark, FieldPath::root("bad_bookmark")).is_err()
        );
        let bad_prefix = CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: 7,
                sequence: 1,
                covered_prefix: 6,
                source_cut: 5,
            }),
            source_revision: None,
        };
        assert!(encode_covered_status_event(&bad_prefix).is_err());
    }
}

#[cfg(test)]
mod canonical_assignment_size_tests {
    use super::*;

    #[test]
    fn noncanonical_unpacked_single_value_cannot_expand_past_plan_carrier_bound() {
        // Legal unpacked field 3 uses two bytes; canonical packed encoding needs three.
        let raw_assignment = [0x18, 0x07];
        let assignment = novarocks::TaskAssignment::decode(raw_assignment.as_slice()).unwrap();
        let budget = TransportBudget::new(
            1,
            16,
            8,
            2,
            32,
            4,
            64,
            1,
            1,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert!(6 + raw_assignment.len() <= budget.max_descriptor_encoded_bytes());
        assert_eq!(assignment.encoded_len(), 3);
        let path = FieldPath::root("assignment");
        assert_eq!(
            check_canonical_assignment_size(&assignment, 6, budget, path.clone())
                .unwrap_err()
                .kind(),
            novarocks_proto_codec::ProtocolErrorKind::OutOfRange
        );
        assert!(check_canonical_assignment_size(&assignment, 5, budget, path.clone()).is_ok());
        assert!(check_canonical_assignment_size(&assignment, usize::MAX, budget, path).is_err());
    }
}
