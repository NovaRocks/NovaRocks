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

//! Transport-neutral execution ports driven by the Worker lifecycle owner.

use std::fmt;
use std::sync::Arc;

use novarocks_execution_contract::task_execution::creation::{
    PreparedTaskFacts, TaskCreationInput,
};
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::domain::CodecOwnedContent;
use novarocks_execution_contract::task_execution::identity::QueryContextRef;
use novarocks_execution_contract::task_execution::operation::{CredentialUpdate, TaskDomainUpdate};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory,
};

use crate::RuntimeFilterReleaseObservation;
use crate::TaskStatusReporter;

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

/// The opaque terminal evidence one query-context release sealed.
///
/// The Worker owns the lifetime of this fact: a context keeps it after shared
/// facts are released so the transport adapter can include it in the matching
/// release acknowledgement.  It deliberately does not name the generated
/// message.  Only the role-local adapter that produced the content may recover
/// and encode that representation.
#[derive(Clone)]
pub struct ReleasedContextEvidence {
    runtime_filter: Option<Arc<dyn CodecOwnedContent>>,
    runtime_filter_observation: RuntimeFilterReleaseObservation,
}

impl Default for ReleasedContextEvidence {
    fn default() -> Self {
        Self::none()
    }
}

impl fmt::Debug for ReleasedContextEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleasedContextEvidence")
            .field(
                "runtime_filter_fingerprint",
                &self
                    .runtime_filter
                    .as_ref()
                    .map(|content| content.fingerprint()),
            )
            .field(
                "runtime_filter_encoded_len",
                &self
                    .runtime_filter
                    .as_ref()
                    .map(|content| content.encoded_len()),
            )
            .field(
                "runtime_filter_observation",
                &self.runtime_filter_observation,
            )
            .finish()
    }
}

impl ReleasedContextEvidence {
    /// Evidence from a tear-down that found no participant to seal.
    pub const fn none() -> Self {
        Self {
            runtime_filter: None,
            runtime_filter_observation: RuntimeFilterReleaseObservation::Absent,
        }
    }

    /// Sealed runtime-filter evidence and its Worker-owned classification.
    ///
    /// A caller may not attach an `Absent` classification to present content:
    /// that would make the retained value and the lifecycle marker contradict
    /// one another.
    pub fn with_runtime_filter(
        runtime_filter: Arc<dyn CodecOwnedContent>,
        runtime_filter_observation: RuntimeFilterReleaseObservation,
    ) -> Self {
        assert!(
            !matches!(
                runtime_filter_observation,
                RuntimeFilterReleaseObservation::Absent
            ),
            "present runtime-filter evidence must not be classified absent"
        );
        Self {
            runtime_filter: Some(runtime_filter),
            runtime_filter_observation,
        }
    }

    pub fn runtime_filter(&self) -> Option<&Arc<dyn CodecOwnedContent>> {
        self.runtime_filter.as_ref()
    }

    pub const fn runtime_filter_observation(&self) -> RuntimeFilterReleaseObservation {
        self.runtime_filter_observation
    }
}

/// The query-context side of execution.
///
/// `materialize` runs while the context is `Establishing`, outside the owner's
/// lock and already racing the sequence-zero lease. Whatever it installs must
/// be undone by `release`, because an establish that loses that race rolls
/// back rather than reaching `Active` late.
pub trait QueryContextHost: Send + Sync {
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection>;

    /// Undoes everything `materialize` installed. Called for a rollback and
    /// for a normal release, so it must be idempotent.
    ///
    /// Returns the terminal evidence the tear-down sealed. A release is the
    /// only point at which the runtime-filter observation is complete -- every
    /// local task is a terminal record and nothing further will be observed --
    /// so the seal happens here and its result is handed back rather than left
    /// inside the host for a later reader to go looking for.
    fn release(&self, context: QueryContextRef) -> ReleasedContextEvidence;

    /// Applies a shared-domain advance the owner already classified as
    /// applicable.
    fn advance_shared_domain(
        &self,
        context: QueryContextRef,
        domain: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection>;
}

/// A submitted, runnable task.
///
/// The handle is deliberately narrow: the owner publishes status and decides
/// terminal outcomes. The registry opens its completion gate at creation
/// commit and may otherwise only ask the running task to stand down.
pub trait RunnableTask: fmt::Debug + Send + Sync {
    /// Opens completion processing after the registry has installed the task
    /// as a live creation. A fragment may physically stop before this call;
    /// its exact completion slot retains the fact until commit.
    fn commit_creation(&self);

    fn cancel(&self, reason: CancelReason);

    fn abort(&self, cause: AbortCause);
}

/// The task side of execution.
///
/// The three install steps are called in exactly the order the creation
/// transaction commits them, and `submit_runnable` is last: it is the only one
/// that starts executable work, so a failure in any earlier step is reported
/// before a worker exists to clean up.
pub trait TaskExecutionHost: Send + Sync {
    /// Closes data-plane admission for every task of this exact query
    /// execution. The registry calls this while it linearizes context
    /// termination, before any per-task capability can be withdrawn.
    fn close_context_admission(&self, context: QueryContextRef);

    /// Releases an idle execution context after every task has physically
    /// converged and no new task can be admitted for this exact attempt.
    fn retire_context_execution(&self, context: QueryContextRef);

    /// Reclaims the compact context fence after the registry has forgotten
    /// the context itself. No task capability for the execution may remain.
    fn forget_context_admission(&self, context: QueryContextRef);

    /// Prepares the task a creation winner owns, from the input its create
    /// request carried.
    ///
    /// This is the only call that interprets a task's static plan. It proves
    /// the plan and the assignment against the descriptor before anything is
    /// published, prepares a dormant runtime, and returns what it proved.
    /// A refusal returns only after this host has undone everything it
    /// prepared: the owner has not yet recorded an installed receiver, so it
    /// cannot undo anything on the host's behalf.
    fn install_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
    ) -> Result<PreparedTaskFacts, HostRejection>;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_port_rejection_stays_bounded_and_preserves_the_closed_queue_fact() {
        let rejection =
            HostRejection::from_closed_queue(TaskFailureCategory::Protocol, "x".repeat(8 * 1024));

        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert!(rejection.is_closed_queue());
        assert!(rejection.detail().as_str().len() < 8 * 1024);
    }
}
