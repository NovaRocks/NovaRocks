// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Feature-gated integration harness for Native role-adapter tests.
//!
//! The harness exposes observed behavior while retaining every registry,
//! actor, attempt, output, join, and retirement authority inside the query
//! application crate. Production adapters cannot use this module because the
//! production dependency does not enable `test-support`.

use novarocks_execution_contract::{AcquireQueryContextAdmissionTicket, QueryContextRef};
use std::time::Instant;
use tokio::runtime::Handle;

use crate::coordination::{
    AdmissionIssueReceipt, AdmissionIssueSettlement, AttemptActivationIdentity,
    ContextStandDownSnapshot, LogicalExecutionActor, LogicalExecutionActorConfig,
    LogicalExecutionActorError, LogicalExecutionActorSnapshot, LogicalExecutionJoinReadiness,
    LogicalExecutionOutputTransfer, LogicalExecutionRegistration, LogicalExecutionRuntimeRegistry,
    LogicalExecutionRuntimeRegistryError, LogicalExecutionRuntimeRegistryHandle,
    LogicalExecutionRuntimeShutdownError, RunningAttemptPermit,
};

/// Complete Query Application ownership for one actor-driven integration test.
#[must_use = "the test logical execution must converge and call finish"]
pub struct LogicalExecutionTestHarness {
    registry_owner: Option<LogicalExecutionRuntimeRegistry>,
    registry: LogicalExecutionRuntimeRegistryHandle,
    registration: Option<LogicalExecutionRegistration>,
    actor: Option<LogicalExecutionActor>,
    initial: Option<crate::coordination::AttemptInstantiationPermit>,
    running: Option<RunningAttemptPermit>,
    output: Option<LogicalExecutionOutputTransfer>,
}

impl std::fmt::Debug for LogicalExecutionTestHarness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogicalExecutionTestHarness")
            .field(
                "initial_execution",
                &self
                    .registration
                    .as_ref()
                    .map(LogicalExecutionRegistration::initial_execution),
            )
            .field("initial_pending", &self.initial.is_some())
            .field("running", &self.running.is_some())
            .finish_non_exhaustive()
    }
}

impl LogicalExecutionTestHarness {
    pub fn install(
        runtime: Handle,
        config: LogicalExecutionActorConfig,
        initial_execution: novarocks_types::QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> Result<Self, LogicalExecutionRuntimeRegistryError> {
        let registry_owner = LogicalExecutionRuntimeRegistry::new(runtime);
        let registry = registry_owner.handle();
        let (registration, initial, output) = registry
            .reserve(initial_execution, contexts)?
            .spawn_and_install(config)?
            .into_parts();
        let actor = registry.actor(&registration)?;
        Ok(Self {
            registry_owner: Some(registry_owner),
            registry,
            registration: Some(registration),
            actor: Some(actor),
            initial: Some(initial),
            running: None,
            output: Some(output),
        })
    }

    pub async fn activate_initial(
        &mut self,
    ) -> Result<AttemptActivationIdentity, LogicalExecutionActorError> {
        let initial = self
            .initial
            .take()
            .expect("the initial attempt may be activated only once");
        let running = self.actor().activate(initial.ready()).await?;
        let identity = running.identity();
        self.running = Some(running);
        Ok(identity)
    }

    /// Borrows the same narrow Native drive authority production active
    /// attempts receive. The running permit remains owned by this harness.
    pub fn native_attempt_drive(&self) -> crate::coordination::NativeAttemptDrive {
        crate::coordination::NativeAttemptDrive::new(
            self.running
                .as_ref()
                .expect("the test attempt must be activated before Native drive"),
        )
    }

    /// Abandons the active attempt so tests can drive its real stand-down and
    /// registry-shutdown path after exercising a borrowed Native drive.
    pub fn abandon_running_attempt(&mut self) {
        drop(
            self.running
                .take()
                .expect("the test attempt must be activated before abandonment"),
        );
    }

    /// Begins the exact admission issue used by Native Abort adapter tests and
    /// abandons the running permit so the actor must supervise stand-down.
    pub async fn begin_admission_issue_and_abandon(
        &mut self,
        request: AcquireQueryContextAdmissionTicket,
    ) -> Result<(AttemptActivationIdentity, AdmissionIssueReceipt), LogicalExecutionActorError>
    {
        let running = self
            .running
            .take()
            .expect("the test attempt must be activated before admission");
        let activation = running.identity();
        let result = running.begin_admission_issue(request).await;
        match result {
            Ok(receipt) => {
                drop(running);
                Ok((activation, receipt))
            }
            Err(error) => {
                self.running = Some(running);
                Err(error)
            }
        }
    }

    pub async fn settle_late_admission_issue(
        &self,
        activation: AttemptActivationIdentity,
        receipt: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
    ) -> Result<crate::coordination::AdmissionIssueDisposition, LogicalExecutionActorError> {
        self.actor()
            .settle_late_admission_issue(activation, receipt, settlement)
            .await
    }

    pub async fn stand_down_snapshot(
        &self,
        context: QueryContextRef,
    ) -> Result<Option<ContextStandDownSnapshot>, LogicalExecutionActorError> {
        self.actor().stand_down_snapshot(context).await
    }

    pub async fn actor_snapshot(
        &self,
    ) -> Result<LogicalExecutionActorSnapshot, LogicalExecutionActorError> {
        self.actor().snapshot().await
    }

    pub async fn join_readiness(
        &self,
    ) -> Result<LogicalExecutionJoinReadiness, LogicalExecutionRuntimeRegistryError> {
        self.registry.join_readiness(self.registration()).await
    }

    pub async fn observe_worker_stopped_and_context_fenced(
        &self,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.registry
            .observe_worker_stopped_and_context_fenced(self.registration(), context)
            .await
    }

    pub async fn observe_worker_process_replaced(
        &self,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.registry
            .observe_worker_process_replaced(self.registration(), context)
            .await
    }

    /// Starts bounded registry shutdown while retaining the owner in `self`.
    /// A timeout, runtime error, or cancellation of this future leaves the
    /// harness available for an exact retry with a later deadline.
    pub async fn finish_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionRuntimeShutdownError> {
        drop(self.output.take());
        drop(self.running.take());
        drop(self.initial.take());
        drop(self.actor.take());
        drop(self.registration.take());
        self.registry_owner
            .as_mut()
            .expect("the test registry owner may be shut down only once")
            .shutdown_until(deadline)
            .await
    }

    fn actor(&self) -> &LogicalExecutionActor {
        self.actor
            .as_ref()
            .expect("the test actor remains owned until finish")
    }

    fn registration(&self) -> &LogicalExecutionRegistration {
        self.registration
            .as_ref()
            .expect("the test registration remains owned until finish")
    }
}
