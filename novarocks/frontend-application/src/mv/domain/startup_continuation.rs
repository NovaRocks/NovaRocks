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

//! Reopening this deployment's own targets after a restart.
//!
//! A restarted process closes management on every target its deployment owns
//! but a previous incarnation wrote. Retiring those barriers one operator
//! statement at a time does not scale, so the deployment may instead state
//! once, in a file, that the previous writer is isolated. This pass is what
//! turns that statement into readmissions.
//!
//! It is a pass rather than a startup step because neither input is ready at
//! any single moment. The statement can be written after this process is
//! already serving -- an orchestration that reads this incarnation out of
//! `novarocks_mv_management_status` first has to -- and the remote-effect
//! window measured from the declared isolation usually has not elapsed yet
//! when it arrives. So the pass runs on the maintenance tick, does what it
//! can, and leaves the rest closed.
//!
//! Nothing here decides what the old writer's effects did. A permit only
//! allows the exact re-observation that finds out.

use std::future::Future;

use novarocks_mv_application::management::{
    ManagementClock, ManagementContinuationService, ManagementEntrance, StartupContinuation,
    StartupIsolationEvidence, SystemManagementClock,
};
use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};

use crate::mv::startup_isolation_file::StartupIsolationSource;

/// What one pass did, as an operator needs to see it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct StartupContinuationReport {
    pub(crate) readmitted: usize,
    pub(crate) waiting: usize,
    pub(crate) operator_only: usize,
    pub(crate) not_covered: usize,
    pub(crate) failed: usize,
}

impl StartupContinuationReport {
    /// Whether this pass found anything the statement still has to act on. A
    /// pass that found nothing is the steady state and says nothing.
    pub(crate) const fn is_silent(&self) -> bool {
        self.readmitted == 0
            && self.waiting == 0
            && self.operator_only == 0
            && self.not_covered == 0
            && self.failed == 0
    }
}

fn accept_declaration(
    continuation: &ManagementContinuationService,
    source: &StartupIsolationSource,
    declaration: novarocks_mv_application::management::StartupIsolationDeclaration,
    clock: &dyn ManagementClock,
) -> Option<StartupIsolationEvidence> {
    let now = match clock.now() {
        Ok(now) => now,
        Err(error) => {
            tracing::warn!(%error, "MV startup continuation could not read the clock");
            return None;
        }
    };
    match StartupIsolationEvidence::try_accept(
        declaration,
        continuation.local_owner(),
        continuation.local_incarnation(),
        source.launch_nonce(),
        now,
    ) {
        Ok(evidence) => Some(evidence),
        Err(error) => {
            tracing::warn!(
                %error,
                path = %source.path().display(),
                "MV startup isolation evidence does not apply to this process; management stays closed"
            );
            None
        }
    }
}

/// Named storage in the original maintenance event. The deployment file job,
/// its genuine QB receipt, and each accepted Current registration outlive any
/// borrowed wait. No callback owns this receiving or its Root.
#[derive(Default)]
pub(crate) struct StartupContinuationReceiving {
    input: Option<StartupIsolationSource>,
    responsibility: Option<crate::task_execution::blocking_io::ConnectorBlockingIoResponsibility>,
    job: Option<
        crate::task_execution::blocking_io::ConnectorOperationJob<
            Result<
                Option<novarocks_mv_application::management::StartupIsolationDeclaration>,
                String,
            >,
        >,
    >,
    raw: Option<
        Result<
            Result<
                Option<novarocks_mv_application::management::StartupIsolationDeclaration>,
                String,
            >,
            crate::task_execution::blocking_io::ConnectorBlockingIoError,
        >,
    >,
    failures: Vec<novarocks_mv_application::readiness::MvProjectionError>,
    registrations: Vec<std::sync::Arc<super::staged_create::CurrentProjectionRegistration>>,
}
impl StartupContinuationReceiving {
    pub(crate) fn prepare_file_in(
        &mut self,
        source: &StartupIsolationSource,
        blocking: &crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        scope: &novarocks_workload_control::WorkScope,
        window: &novarocks_workload_control::ResultWindowAlias,
    ) -> Result<(), novarocks_workload_control::WorkError> {
        if self.job.is_some() || self.raw.is_some() {
            return Err(novarocks_workload_control::WorkError::Conflict);
        }
        self.responsibility = Some(
            crate::task_execution::blocking_io::ConnectorBlockingIoResponsibility::admit(
                scope, window,
            )?,
        );
        self.input = Some(source.clone());
        blocking.prepare_blocking_owned_in(
            self.responsibility.as_ref().unwrap(),
            &mut self.input,
            |original, _| Some(original.read()),
            &mut self.job,
        )
    }
    pub(crate) fn poll_progress(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        if self.raw.is_none()
            && let Some(job) = self.job.as_mut()
        {
            match job.poll_progress(cx) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => self.raw = Some(Err(error)),
                std::task::Poll::Ready(Ok(())) => self.raw = job.try_take(),
            }
        }
        std::task::Poll::Ready(())
    }
    pub(crate) fn poll_closed(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        std::task::ready!(self.poll_progress(cx));
        let mut closed = true;
        for registration in &self.registrations {
            closed &= registration.poll_registration_closed(cx).is_ready();
        }
        if closed {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.registrations
            .iter()
            .find_map(|source| source.local_health_failure())
    }
}

/// Preserve the actual connector cause while the original maintenance Root
/// retains deployment evidence, continuation policy and exact readmission.
fn startup_connector_failure(
    error: novarocks_spi::connector::ConnectorError,
) -> novarocks_mv_application::readiness::MvProjectionError {
    let cause = super::readiness::connector_failure_cause(error);
    novarocks_mv_application::readiness::MvProjectionError::new(
        novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,
        cause.detail(),
    )
    .with_original_failure(cause)
}

pub(crate) async fn run_startup_continuation_pass_async_in(
    entrance: &ManagementEntrance,
    continuation: &ManagementContinuationService,
    readiness: &super::readiness::MvReadinessExecution<'_>,
    connector_control: &dyn ConnectorControlResolver,
    source: &StartupIsolationSource,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    factory: &super::staged_create::CurrentProjectionMetadataCpu,
    receiving: &mut StartupContinuationReceiving,
) -> StartupContinuationReport {
    std::future::poll_fn(|cx| receiving.poll_progress(cx)).await;
    let declaration = match receiving.raw.as_ref() {
        Some(Ok(Ok(Some(declaration)))) => declaration.clone(),
        Some(Ok(Ok(None))) => return StartupContinuationReport::default(),
        Some(Ok(Err(error))) => {
            tracing::warn!(%error, "MV startup isolation evidence could not be read");
            return StartupContinuationReport::default();
        }
        Some(Err(error)) => {
            tracing::warn!(%error, "MV startup isolation evidence original file job failed");
            return StartupContinuationReport::default();
        }
        None => unreachable!("original file result precedes pass policy"),
    };
    let clock = SystemManagementClock;
    let Some(evidence) = accept_declaration(continuation, source, declaration, &clock) else {
        return StartupContinuationReport::default();
    };
    let projections = match readiness
        .candidate_reader()
        .list_candidate_definitions_async()
        .await
    {
        Ok(projections) => projections,
        Err(error) => {
            tracing::warn!(%error, "skipping MV startup continuation because the Accelerator is unavailable");
            return StartupContinuationReport::default();
        }
    };
    let mut report = StartupContinuationReport::default();
    for projection in projections {
        let revision = projection.facts.source_revision();
        match continuation.resume_target_on_startup_isolation(&revision.target, &evidence, &clock) {
            StartupContinuation::Nothing => {}
            StartupContinuation::NotCovered => report.not_covered += 1,
            StartupContinuation::OperatorOnly => report.operator_only += 1,
            StartupContinuation::Waiting { deadline } => {
                report.waiting += 1;
                tracing::debug!(mv_target = %format!("{}.{}.{}", revision.target.instance_id.as_str(), revision.target.namespace, revision.target.table), deadline = deadline.as_unix_millis(), "MV startup continuation is waiting for the guaranteed remote window");
            }
            StartupContinuation::Ready {
                previous_incarnation,
                permits,
            } => {
                // Registration custody is installed before constructing or
                // polling its borrowed foreign future; Unknown is retained.
                receiving.registrations.push(std::sync::Arc::new(
                    super::staged_create::CurrentProjectionRegistration::new(),
                ));
                let registration = receiving.registrations.last().unwrap().clone();
                let result = async {
                    let instance_id =
                        ConnectorInstanceId::parse(revision.target.instance_id.as_str())
                            .map_err(|error| novarocks_mv_application::readiness::MvProjectionError::new(novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict, error.to_string()))?;
                    let lease = connector_control
                        .acquire_current(&instance_id)
                        .map_err(startup_connector_failure)?;
                    let catalog = lease
                        .binding()
                        .catalog_handle()
                        .map_err(startup_connector_failure)?
                        .clone();
                    let mut original = Box::pin(super::staged_create::readmit_declared_target_async_in(
                        super::staged_create::CurrentProjectionServices {
                            entrance,
                            readiness,
                            connector_control,
                            metadata_cpu: factory,
                            registration: registration.as_ref(),
                        },
                        catalog,
                        novarocks_mv_application::product::MvTarget::from_parts(
                            Some(revision.target.instance_id.as_str()),
                            &revision.target.namespace,
                            &revision.target.table,
                        ),
                        uuid::Uuid::now_v7(),
                        previous_incarnation,
                        permits,
                        context.clone(),
                    ));
                    std::future::poll_fn(|cx| {
                        if let Some(health) = registration.local_health_failure() { std::panic::panic_any(health); }
                        let polled = original.as_mut().poll(cx);
                        if let Some(health) = registration.local_health_failure() { std::panic::panic_any(health); }
                        polled
                    }).await
                }
                .await;
                match result {
                    Ok(()) => report.readmitted += 1,
                    Err(error) => {
                        report.failed += 1;
                        tracing::warn!(mv_target = %format!("{}.{}.{}", revision.target.instance_id.as_str(), revision.target.namespace, revision.target.table), %error, "MV startup continuation could not readmit a target its statement covers");
                        // Keep the actual typed source until this pass's
                        // registration, CPU, SS and physical footer close.
                        receiving.failures.push(error);
                    }
                }
            }
            StartupContinuation::Refused(error) => {
                report.failed += 1;
                tracing::warn!(mv_target = %format!("{}.{}.{}", revision.target.instance_id.as_str(), revision.target.namespace, revision.target.table), %error, "MV startup continuation refused a target its statement named");
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_that_found_nothing_says_nothing() {
        assert!(StartupContinuationReport::default().is_silent());
        assert!(
            !StartupContinuationReport {
                waiting: 1,
                ..StartupContinuationReport::default()
            }
            .is_silent()
        );
    }
}
