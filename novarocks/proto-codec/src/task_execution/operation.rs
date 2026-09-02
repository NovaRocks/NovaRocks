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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::task_execution::descriptor::TaskDescriptor;
use novarocks_execution::task_execution::domain::{
    CodecOwnedContent, CredentialEpoch, CredentialLeaseId, DomainProgression, DomainVersion,
    ExchangeEdgeId,
};
use novarocks_execution::task_execution::identity::{
    QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution::task_execution::lease::LeaseValidFor;
use novarocks_execution::task_execution::operation::{
    AbortQueryContext, AdvanceQueryContextDomain, CancelTask, CreateTask, CreateTaskReceipt,
    EstablishQueryContext, FetchTaskDynamicFilters, GetFinalTaskInfo, MaxWait, OperationEnvelope,
    OperationKind, OperationOutcome, QueryContextDomainReceipt, QueryContextReceipt,
    ReleaseOutcome, ReleaseQueryContext, RenewQueryExecutionLease, TaskDomainReceipt,
    TransportBudget, UpdateQueryContext, UpdateTask, UpdateTaskReceipt,
};
use novarocks_execution::task_execution::transition::QueryContextState;
use novarocks_proto_models::novarocks;
use prost::Message;

use crate::task_execution::descriptor::{WireFragmentPlan, decode_task_descriptor};
use crate::task_execution::domain::{
    DecodedQueryContextDomain, DecodedTaskDomain, MAX_DOMAIN_UPDATES, WireContent,
    decode_credential_domain, decode_plan_node_split_receipt, decode_query_context_domain,
    decode_task_domain,
};

/// Domain separation tags for the two shared facts an establish installs.
/// They are distinct from the tags an advance uses, because the same content
/// arriving as an install and as a rotation is not the same operation.
const ESTABLISH_CATALOG_DOMAIN_TAG: &[u8] =
    b"novarocks.task_execution.establish.catalog_binding.v1";
const ESTABLISH_FILTER_DOMAIN_TAG: &[u8] =
    b"novarocks.task_execution.establish.initial_runtime_filter.v1";
use crate::task_execution::identity::{
    decode_query_context_ref, decode_task_operation_id, encode_query_context_ref,
    encode_task_operation_id,
};
use crate::task_execution::lease::decode_duration_millis;
use crate::task_execution::status::{
    decode_abort_cause, decode_cancel_reason, encode_abort_cause, encode_cancel_reason,
};
use crate::task_execution::{invalid, invalid_enum, missing, out_of_range};
use crate::{FieldPath, ProtocolError};

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

/// A decoded create request, with its typed plan and domain content retained.
pub struct DecodedCreateTask {
    request: CreateTask,
    fragment: Arc<WireFragmentPlan>,
    initial_domains: Vec<DecodedTaskDomain>,
}

impl DecodedCreateTask {
    pub const fn request(&self) -> &CreateTask {
        &self.request
    }

    pub const fn descriptor(&self) -> &TaskDescriptor {
        self.request.descriptor()
    }

    /// The typed plan, for the backend's own plan decoder.
    pub fn fragment(&self) -> &Arc<WireFragmentPlan> {
        &self.fragment
    }

    pub fn initial_domains(&self) -> &[DecodedTaskDomain] {
        &self.initial_domains
    }
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
    catalog_set: Arc<WireContent<novarocks_proto_models::catalog::CatalogSet>>,
    initial_runtime_filter: Arc<WireContent<novarocks::RuntimeFilterContribution>>,
    initial_credential: DecodedQueryContextDomain,
    initial_lease_valid_for: LeaseValidFor,
}

impl DecodedEstablishQueryContext {
    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn envelope(&self) -> OperationEnvelope {
        self.envelope
    }

    /// The catalog binding, for the backend's shared-fact owner.
    pub fn catalog_set(&self) -> &novarocks_proto_models::catalog::CatalogSet {
        self.catalog_set.wire()
    }

    /// The initial runtime filter, for the backend's shared-fact owner.
    pub fn initial_runtime_filter(&self) -> &novarocks::RuntimeFilterContribution {
        self.initial_runtime_filter.wire()
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
        sequence: novarocks_execution::task_execution::lease::LeaseSequence,
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
                    Arc::clone(&request.catalog_set) as Arc<dyn CodecOwnedContent>,
                    Arc::clone(&request.initial_runtime_filter) as Arc<dyn CodecOwnedContent>,
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
    CreateTask(DecodedCreateTask),
    UpdateTask(DecodedUpdateTask),
    UpdateQueryContext(DecodedUpdateQueryContext),
    CancelTask(CancelTask),
    AbortQueryContext(AbortQueryContext),
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
            Self::CreateTask(request) => request.request.envelope(),
            Self::UpdateTask(request) => request.request.envelope(),
            Self::UpdateQueryContext(request) => request.envelope(),
            Self::CancelTask(request) => request.envelope(),
            Self::AbortQueryContext(request) => request.envelope(),
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
        novarocks::task_operation::Operation::CreateTask(create) => {
            let create_path = path.clone().field("create_task");
            let envelope = decode_envelope(
                envelope_src,
                OperationKind::CreateTask,
                path.field("envelope"),
            )?;
            let context = create.query_context.as_ref().ok_or_else(|| {
                missing(
                    create_path.clone().field("query_context"),
                    "create requires a query context reference",
                )
            })?;
            let context =
                decode_query_context_ref(context, create_path.clone().field("query_context"))?;
            let descriptor = create.descriptor.as_ref().ok_or_else(|| {
                missing(
                    create_path.clone().field("descriptor"),
                    "create requires a descriptor",
                )
            })?;
            let (descriptor, fragment) =
                decode_task_descriptor(descriptor, create_path.clone().field("descriptor"))?;
            let initial_domains = decode_task_domains(
                &create.initial_domains,
                create_path.clone().field("initial_domains"),
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
            Ok(DecodedOperation::CreateTask(DecodedCreateTask {
                request,
                fragment,
                initial_domains,
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
    }
}

fn decode_identity_field(
    src: Option<&novarocks::TaskIdentity>,
    path: FieldPath,
    detail: &'static str,
) -> Result<TaskIdentity, ProtocolError> {
    let identity = src.ok_or_else(|| missing(path.clone().field("identity"), detail))?;
    crate::task_execution::identity::decode_task_identity(identity, path.field("identity"))
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
            Ok(DecodedUpdateQueryContext::Establish(
                DecodedEstablishQueryContext {
                    context,
                    envelope,
                    catalog_set,
                    initial_runtime_filter,
                    initial_credential,
                    initial_lease_valid_for,
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
            let grant =
                crate::task_execution::lease::decode_lease_grant(lease, renew_path.field("lease"))?;
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
        Ok(novarocks::TaskOperationOutcome::CreateConflict) => Ok(OperationOutcome::CreateConflict),
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
        Ok(novarocks::TaskOperationOutcome::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "operation outcome must be a known non-default value",
        )),
    }
}

/// Encodes a server-reported outcome.
///
/// Four neutral categories have no wire representation on purpose. Two of
/// them are what a client concludes when no receipt arrives at all, and two
/// are exchange ingress rejections that travel on the data plane, not in an
/// operation receipt. Returning `None` for those is what stops a caller from
/// silently shipping one under a category that means something else.
fn encode_outcome(value: OperationOutcome) -> Option<i32> {
    let encoded = match value {
        OperationOutcome::Accepted => novarocks::TaskOperationOutcome::Accepted,
        OperationOutcome::Idempotent => novarocks::TaskOperationOutcome::Idempotent,
        OperationOutcome::OperationTimedOut => novarocks::TaskOperationOutcome::OperationTimedOut,
        OperationOutcome::IdentityMismatch => novarocks::TaskOperationOutcome::IdentityMismatch,
        OperationOutcome::CreateConflict => novarocks::TaskOperationOutcome::CreateConflict,
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
        OperationOutcome::RetryableTransportUnknown
        | OperationOutcome::RetryableObservationLoss
        | OperationOutcome::NormalDestinationCanceled
        | OperationOutcome::DestinationFailure => return None,
    };
    Some(encoded as i32)
}

fn decode_context_state(value: i32, path: FieldPath) -> Result<QueryContextState, ProtocolError> {
    match novarocks::QueryContextState::try_from(value) {
        Ok(novarocks::QueryContextState::Establishing) => Ok(QueryContextState::Establishing),
        Ok(novarocks::QueryContextState::Active) => Ok(QueryContextState::Active),
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
}

impl ReceiptHeader {
    pub const fn new(operation_id: TaskOperationId, outcome: OperationOutcome) -> Self {
        Self {
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
    crate::task_execution::status::decode_safe_detail(
        &src.safe_detail,
        path.clone().field("safe_detail"),
    )?;
    if let Some(field_path) = src.safe_field_path.as_deref()
        && field_path.len() > novarocks_execution::task_execution::status::SAFE_FIELD_PATH_MAX_BYTES
    {
        return Err(out_of_range(
            path.field("safe_field_path"),
            "safe field path exceeds the redacted text limit",
        ));
    }
    Ok(ReceiptHeader::new(operation_id, outcome))
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
) -> Result<(TaskIdentity, Duration), ProtocolError> {
    let root = src.root_task.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("root_task"),
            "a result poll requires a root task identity",
        )
    })?;
    let root = crate::task_execution::identity::decode_task_identity(
        root,
        path.clone().field("root_task"),
    )?;
    let max_wait = decode_duration_millis(
        src.max_wait_millis,
        path.field("max_wait_millis"),
        MaxWait::MAX_REPRESENTABLE,
    )?;
    Ok((root, max_wait))
}

/// Encodes the credential receipt of one query context.
///
/// Only the accepted epoch is reported: no credential material, and no digest
/// of any, ever appears in a receipt.
pub fn encode_credential_receipt(
    lease_id: novarocks_execution::task_execution::domain::CredentialLeaseId,
    epoch: CredentialEpoch,
) -> novarocks::QueryContextCredentialReceipt {
    novarocks::QueryContextCredentialReceipt {
        lease_id: lease_id.get(),
        accepted_epoch: epoch.get(),
    }
}

/// Encodes a release acknowledgement.
pub fn encode_release_ack(
    context: QueryContextRef,
    outcome: ReleaseOutcome,
    state: QueryContextState,
) -> Option<novarocks::ReleaseQueryContextAck> {
    Some(novarocks::ReleaseQueryContextAck {
        query_context: Some(encode_query_context_ref(context)),
        outcome: encode_release_outcome(outcome),
        state: encode_context_state(state)?,
        termination_cause: None,
    })
}

/// Decodes a release acknowledgement, returning the termination cause the
/// backend reported rather than validating and discarding it.
///
/// A release that comes back on a context that was terminated instead of
/// released carries the only statement of why.
pub fn decode_release_ack(
    src: &novarocks::ReleaseQueryContextAck,
    path: FieldPath,
) -> Result<
    (
        QueryContextRef,
        ReleaseOutcome,
        QueryContextState,
        Option<novarocks_execution::task_execution::status::AbortCause>,
    ),
    ProtocolError,
> {
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
        .map(|cause| decode_abort_cause(cause, path.field("termination_cause")))
        .transpose()?;
    Ok((context, outcome, state, cause))
}

/// Encodes a cancel reason back onto the wire, for a receipt or a status.
pub fn encode_cancel_reason_field(
    value: novarocks_execution::task_execution::status::CancelReason,
) -> i32 {
    encode_cancel_reason(value)
}

/// Encodes an abort cause back onto the wire.
pub fn encode_abort_cause_field(
    value: novarocks_execution::task_execution::status::AbortCause,
) -> i32 {
    encode_abort_cause(value)
}

/// Encodes an operation outcome back onto the wire, or `None` when the
/// category has no wire representation.
pub fn encode_operation_outcome(value: OperationOutcome) -> Option<i32> {
    encode_outcome(value)
}

/// Encodes a query context state back onto the wire, or `None` for `Absent`.
pub fn encode_query_context_state(value: QueryContextState) -> Option<i32> {
    encode_context_state(value)
}

/// The frontend side of the operation surface: building requests, and reading
/// the acknowledgements back.
///
/// These are separate from the decoders above because the two roles use
/// opposite halves. A backend decodes an operation and encodes a receipt; a
/// frontend encodes an operation and decodes a receipt. Keeping both halves in
/// one module is what makes it impossible for them to drift apart.
/// Encodes one task-scoped domain change from its neutral form.
///
/// The split and filter variants need their typed content, which the neutral
/// form holds only a fingerprint of, so the frontend passes the content it
/// already has rather than reconstructing it.
pub fn encode_create_task(
    request: &CreateTask,
    fragment: &WireFragmentPlan,
    initial_domains: Vec<novarocks::TaskDomainUpdate>,
) -> novarocks::TaskOperation {
    novarocks::TaskOperation {
        envelope: Some(encode_envelope(request.envelope())),
        operation: Some(novarocks::task_operation::Operation::CreateTask(
            novarocks::CreateTaskRequest {
                query_context: Some(encode_query_context_ref(request.context())),
                descriptor: Some(crate::task_execution::descriptor::encode_task_descriptor(
                    request.descriptor(),
                    fragment,
                )),
                initial_domains,
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
                identity: Some(crate::task_execution::identity::encode_task_identity(
                    request.identity(),
                )),
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
                identity: Some(crate::task_execution::identity::encode_task_identity(
                    request.identity(),
                )),
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
                            lease: Some(crate::task_execution::lease::encode_lease_grant(
                                crate::task_execution::lease::LeaseGrant::new(
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
    native_compatibility_id: Option<novarocks::NativeCompatibilityId>,
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
                        initial_lease: Some(crate::task_execution::lease::encode_lease_grant(
                            crate::task_execution::lease::LeaseGrant::new(
                                request.initial_lease_sequence(),
                                request.initial_lease_valid_for(),
                            ),
                        )),
                        query_options: Some(query_options),
                        native_compatibility_id,
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

/// Encodes one task-domain receipt.

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
            let edge_path = path.field("open_exchange_edges").field("opened_edge_ids");
            let mut opened = Vec::with_capacity(edges.opened_edge_ids.len());
            for (index, edge) in edges.opened_edge_ids.iter().enumerate() {
                opened.push(
                    ExchangeEdgeId::new(*edge).map_err(|error| {
                        invalid(edge_path.clone().index(index), error.to_string())
                    })?,
                );
            }
            Ok(TaskDomainReceipt::OpenExchangeEdges {
                opened,
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
    use novarocks_execution::task_execution::operation::TaskDomainReceipt as Receipt;
    let progression = encode_accepted_progression(match value {
        Receipt::SplitAssignment { progression, .. }
        | Receipt::TaskDynamicFilter { progression, .. }
        | Receipt::OpenExchangeEdges { progression, .. } => *progression,
    })?;
    let receipt = match value {
        Receipt::SplitAssignment { nodes, .. } => {
            novarocks::task_domain_receipt::Receipt::SplitAssignment(
                novarocks::TaskSplitAssignmentReceipt {
                    nodes: nodes
                        .iter()
                        .copied()
                        .map(crate::task_execution::domain::encode_plan_node_split_receipt)
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
        Receipt::OpenExchangeEdges { opened, .. } => {
            novarocks::task_domain_receipt::Receipt::OpenExchangeEdges(
                novarocks::OpenExchangeEdgesReceipt {
                    opened_edge_ids: opened.iter().map(|edge| edge.get()).collect(),
                },
            )
        }
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
    use novarocks_execution::task_execution::operation::QueryContextDomainReceipt as Receipt;
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
        identity: Some(crate::task_execution::identity::encode_task_identity(
            value.identity(),
        )),
        accepted_domains: encode_task_domain_receipts(value.domains())?,
        current_status: Some(crate::task_execution::status::encode_task_status(
            value.current_status(),
        )),
    })
}

/// Encodes an update acknowledgement.
///
/// Returns `None` for the same reason a create acknowledgement does.
pub fn encode_update_task_ack(value: &UpdateTaskReceipt) -> Option<novarocks::UpdateTaskAck> {
    Some(novarocks::UpdateTaskAck {
        identity: Some(crate::task_execution::identity::encode_task_identity(
            value.identity(),
        )),
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
    let status = crate::task_execution::status::decode_task_status(status, status_path.clone())?;
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
        Option<novarocks_execution::task_execution::status::AbortCause>,
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
        receipt = receipt.with_lease(crate::task_execution::lease::decode_lease_receipt(
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
    value: &novarocks_execution::task_execution::operation::QueryContextReceipt,
    termination_cause: Option<novarocks_execution::task_execution::status::AbortCause>,
) -> Option<novarocks::QueryContextAck> {
    Some(novarocks::QueryContextAck {
        query_context: Some(encode_query_context_ref(value.context())),
        state: encode_context_state(value.state())?,
        lease: value
            .lease()
            .map(crate::task_execution::lease::encode_lease_receipt),
        accepted_domains: value
            .domains()
            .iter()
            .map(encode_query_context_domain_receipt)
            .collect::<Option<Vec<_>>>()?,
        termination_cause: termination_cause.map(encode_abort_cause),
    })
}

/// Encodes one operation receipt.
///
/// Returns `None` when the outcome has no wire representation, which is the
/// only way a caller can be stopped from shipping a client-only category as
/// something a backend claimed.
pub fn encode_receipt(
    operation_id: TaskOperationId,
    outcome: OperationOutcome,
    safe_detail: &str,
    ack: Option<novarocks::task_operation_receipt::Ack>,
) -> Option<novarocks::TaskOperationReceipt> {
    Some(novarocks::TaskOperationReceipt {
        operation_id: Some(encode_task_operation_id(operation_id)),
        outcome: encode_outcome(outcome)?,
        safe_detail: safe_detail.to_owned(),
        safe_field_path: None,
        ack,
    })
}

/// Encodes one status stream event.
pub fn encode_status_event(
    value: &novarocks_execution::task_execution::status::TaskStatus,
) -> novarocks::TaskStatusStreamEvent {
    novarocks::TaskStatusStreamEvent {
        event: Some(novarocks::task_status_stream_event::Event::TaskStatus(
            crate::task_execution::status::encode_task_status(value),
        )),
    }
}

/// Encodes a task-gone event.
pub fn encode_task_gone_event(identity: TaskIdentity) -> novarocks::TaskStatusStreamEvent {
    novarocks::TaskStatusStreamEvent {
        event: Some(novarocks::task_status_stream_event::Event::TaskGone(
            novarocks::TaskGone {
                identity: Some(crate::task_execution::identity::encode_task_identity(
                    identity,
                )),
            },
        )),
    }
}

/// Decodes one status stream event.
pub fn decode_status_event(
    src: &novarocks::TaskStatusStreamEvent,
    path: FieldPath,
) -> Result<StatusStreamEvent, ProtocolError> {
    let event = src
        .event
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "a status event requires a body"))?;
    match event {
        novarocks::task_status_stream_event::Event::TaskStatus(status) => Ok(
            StatusStreamEvent::Status(crate::task_execution::status::decode_task_status(
                status,
                path.field("task_status"),
            )?),
        ),
        novarocks::task_status_stream_event::Event::TaskGone(gone) => {
            let gone_path = path.field("task_gone");
            let identity = decode_identity_field(
                gone.identity.as_ref(),
                gone_path,
                "a task-gone event requires a task identity",
            )?;
            Ok(StatusStreamEvent::Gone(identity))
        }
    }
}

/// One observed status event.
#[derive(Clone, Debug)]
pub enum StatusStreamEvent {
    Status(novarocks_execution::task_execution::status::TaskStatus),
    Gone(TaskIdentity),
}

/// Encodes a status subscription request.
pub fn encode_subscribe_task_status(
    context: QueryContextRef,
    cursors: &[novarocks_execution::task_execution::status::TaskStatusCursor],
) -> Result<novarocks::SubscribeTaskStatusRequest, ProtocolError> {
    if cursors.len() > crate::task_execution::status::MAX_SUBSCRIPTION_CURSORS {
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
            .map(crate::task_execution::status::encode_task_status_cursor)
            .collect(),
    })
}

/// Decodes a status subscription request.
pub fn decode_subscribe_task_status(
    src: &novarocks::SubscribeTaskStatusRequest,
    path: FieldPath,
) -> Result<
    (
        QueryContextRef,
        Vec<novarocks_execution::task_execution::status::TaskStatusCursor>,
    ),
    ProtocolError,
> {
    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "a subscription requires a query context reference",
        )
    })?;
    let context = decode_query_context_ref(context, path.clone().field("query_context"))?;
    if src.cursors.len() > crate::task_execution::status::MAX_SUBSCRIPTION_CURSORS {
        return Err(out_of_range(
            path.clone().field("cursors"),
            "cursor count exceeds the hard limit",
        ));
    }
    let mut cursors = Vec::with_capacity(src.cursors.len());
    for (index, cursor) in src.cursors.iter().enumerate() {
        cursors.push(crate::task_execution::status::decode_task_status_cursor(
            cursor,
            path.clone().field("cursors").index(index),
        )?);
    }
    Ok((context, cursors))
}
