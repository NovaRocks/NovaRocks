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

//! The execution-side ports the task protocol owner drives.
//!
//! The owner in [`super::registry`] owns linearization, transactions, status,
//! and retention; it owns no plan decoding, no receiver allocation, and no
//! pipeline. Those are exactly the steps that can be slow or can fail, so they
//! are named here as narrow ports with an explicit undo for every install.
//!
//! Each install has a matching remove because the creation transaction's
//! rollback calls them in reverse: a port whose effect cannot be undone could
//! not take part in an atomic creation.

use std::fmt;
use std::sync::Arc;

use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_execution_contract::task_execution::identity::QueryContextRef;
use novarocks_execution_contract::task_execution::operation::{
    CredentialUpdate, QueryContextDomainUpdate, TaskDomainUpdate,
};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory,
};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;

use super::status::TaskStatusReporter;

/// A bounded, already-redacted rejection from an execution-side port.
///
/// It is a [`TaskFailure`] by construction so that a port rejection reaching a
/// task's status can never be widened into free-form text or a different
/// category on the way.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostRejection {
    failure: TaskFailure,
    /// This rejection is "the plan node stopped taking input", which a caller
    /// holding a replay may treat as moot rather than illegal. It is a
    /// separate flag rather than a category because the category is what
    /// reaches a task's status, and this distinction must not widen that
    /// vocabulary.
    closed_queue: bool,
}

impl HostRejection {
    pub fn new(category: TaskFailureCategory, detail: impl AsRef<str>) -> Self {
        Self {
            failure: TaskFailure::new(category, SafeDetail::truncating(detail.as_ref())),
            closed_queue: false,
        }
    }

    /// The same rejection, marked as caused by a closed plan-node queue.
    pub fn from_closed_queue(category: TaskFailureCategory, detail: impl AsRef<str>) -> Self {
        Self {
            closed_queue: true,
            ..Self::new(category, detail)
        }
    }

    pub const fn is_closed_queue(&self) -> bool {
        self.closed_queue
    }

    pub const fn failure(&self) -> &TaskFailure {
        &self.failure
    }

    pub const fn category(&self) -> TaskFailureCategory {
        self.failure.category()
    }

    pub const fn detail(&self) -> &SafeDetail {
        self.failure.detail()
    }
}

impl fmt::Display for HostRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.failure.fmt(formatter)
    }
}

impl std::error::Error for HostRejection {}

/// The shared facts one establish installs, handed over as a single unit.
///
/// They are passed together because they become observable together: the
/// context reaches `Active` only once all four are materialized, so a host
/// never publishes a catalog binding a query's credential cannot yet read.
pub struct SharedFactsRequest<'a> {
    context: QueryContextRef,
    catalog_binding: &'a Arc<dyn CodecOwnedContent>,
    initial_runtime_filter: &'a Arc<dyn CodecOwnedContent>,
    query_options: &'a Arc<dyn CodecOwnedContent>,
    initial_credential: &'a CredentialUpdate,
}

impl<'a> SharedFactsRequest<'a> {
    pub const fn new(
        context: QueryContextRef,
        catalog_binding: &'a Arc<dyn CodecOwnedContent>,
        initial_runtime_filter: &'a Arc<dyn CodecOwnedContent>,
        query_options: &'a Arc<dyn CodecOwnedContent>,
        initial_credential: &'a CredentialUpdate,
    ) -> Self {
        Self {
            context,
            catalog_binding,
            initial_runtime_filter,
            query_options,
            initial_credential,
        }
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn catalog_binding(&self) -> &'a Arc<dyn CodecOwnedContent> {
        self.catalog_binding
    }

    pub const fn initial_runtime_filter(&self) -> &'a Arc<dyn CodecOwnedContent> {
        self.initial_runtime_filter
    }

    pub const fn query_options(&self) -> &'a Arc<dyn CodecOwnedContent> {
        self.query_options
    }

    pub const fn initial_credential(&self) -> &'a CredentialUpdate {
        self.initial_credential
    }
}

/// The query-context side of execution.
///
/// `materialize` runs while the context is `Establishing`, outside the owner's
/// lock and already racing the sequence-zero lease. Whatever it installs must
/// be undone by `release`, because an establish that loses that race rolls
/// back rather than reaching `Active` late.
/// What a query context's tear-down sealed, for the release that reports it.
///
/// A release that installed no participant carries `None`. That is a
/// different statement from an empty contribution, and the two must not be
/// collapsed: the frontend distinguishes "this query had no runtime filter on
/// this backend" from "this backend observed no runtime-filter activity".
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReleasedContextEvidence {
    runtime_filter: Option<QueryTerminalProfileContributionTelemetry>,
}

impl ReleasedContextEvidence {
    /// Evidence from a tear-down that found no participant to seal.
    pub const fn none() -> Self {
        Self {
            runtime_filter: None,
        }
    }

    pub const fn with_runtime_filter(
        runtime_filter: QueryTerminalProfileContributionTelemetry,
    ) -> Self {
        Self {
            runtime_filter: Some(runtime_filter),
        }
    }

    pub const fn runtime_filter(&self) -> Option<&QueryTerminalProfileContributionTelemetry> {
        self.runtime_filter.as_ref()
    }
}

pub trait QueryContextHost: Send + Sync {
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection>;

    /// Undoes everything `materialize` installed. Called for a rollback and
    /// for a normal release, so it must be idempotent.
    ///
    /// Returns the terminal evidence the tear-down sealed. A release is the
    /// only point at which the runtime-filter observation is a complete fact
    /// -- every local task is a terminal record and nothing further will be
    /// observed -- so the seal happens here and its result is handed back
    /// rather than left inside the host for a later reader to go looking for.
    fn release(&self, context: QueryContextRef) -> ReleasedContextEvidence;

    /// Applies a shared-domain advance the owner already classified as
    /// applicable.
    fn advance_shared_domain(
        &self,
        context: QueryContextRef,
        domain: &QueryContextDomainUpdate,
    ) -> Result<(), HostRejection>;
}

/// A submitted, runnable task.
///
/// The handle is deliberately narrow: the owner publishes status and decides
/// terminal outcomes, so a running task can only be asked to stand down.
pub trait RunnableTask: fmt::Debug + Send + Sync {
    fn cancel(&self, reason: CancelReason);

    fn abort(&self, cause: AbortCause);
}

/// The task side of execution.
///
/// The three install steps are called in exactly the order the creation
/// transaction commits them, and `submit_runnable` is last: it is the only one
/// that can start a thread, so a failure in any earlier step is reported
/// before a worker exists to clean up.
pub trait TaskExecutionHost: Send + Sync {
    /// Closes data-plane admission for every task of this exact query
    /// execution. The registry calls this while it linearizes context
    /// termination, before any per-task capability can be withdrawn.
    fn close_context_admission(&self, context: QueryContextRef);

    /// Reclaims the compact context fence after the registry has forgotten
    /// the context itself. No task capability for the execution may remain.
    fn forget_context_admission(&self, context: QueryContextRef);

    fn install_receiver(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection>;

    fn remove_receiver(&self, descriptor: &TaskDescriptor);

    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection>;

    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor);

    fn submit_runnable(
        &self,
        descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection>;

    /// Applies a task-scoped domain advance the owner already classified as
    /// applicable.
    ///
    /// Returns how many splits the plan node still holds after the offer, for
    /// the domains that have a queue. The sender reads it as backpressure, so
    /// it must be measured rather than defaulted: reporting zero for a domain
    /// that never counted says the task is idle and invites the sender to keep
    /// filling a queue that is already full. `None` means this domain has no
    /// queue to report, which is a different statement from an empty one.
    fn apply_task_domain(
        &self,
        descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
    ) -> Result<Option<u64>, HostRejection>;
}

/// One task's readable dynamic filter domain.
///
/// The payload stays behind [`CodecOwnedContent`]: the owner retains what a
/// task published and hands it back, never inspecting or re-encoding it.
#[derive(Clone, Debug)]
pub struct TaskDynamicFilterRead {
    version: DomainVersion,
    payload: Arc<dyn CodecOwnedContent>,
}

impl TaskDynamicFilterRead {
    pub const fn new(version: DomainVersion, payload: Arc<dyn CodecOwnedContent>) -> Self {
        Self { version, payload }
    }

    pub const fn version(&self) -> DomainVersion {
        self.version
    }

    pub fn payload(&self) -> &Arc<dyn CodecOwnedContent> {
        &self.payload
    }
}
