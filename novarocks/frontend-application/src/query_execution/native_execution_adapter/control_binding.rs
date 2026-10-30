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

//! Same original Native projection's bounded control-binding phase.

use novarocks_resource_contract::{DependencyFactError, SelectedNetworkOrigin};
use novarocks_workload_control::joint_admission::{
    ControlBindingProposal, InternalManifestBinding, JointAdmission, JointGrantedBatch,
    JointProposalSlotWait, JointTryBegin, RootStageAdmission, RootStageProposal, RootStageReady,
};
use novarocks_workload_control::{ResultWindowAlias, WorkError, WorkScope};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone, Copy, Default)]
enum Phase {
    #[default]
    Root,
    Execution,
    Bound,
    Closed,
}

/// Inline in the original dormant projection, never in the polled F.
/// The same original slot retains construction, reply and first refusal.
#[derive(Default)]
pub(super) struct NativeControlBinding {
    phase: Phase,
    proposal: Option<ControlBindingProposal>,
    admission: Option<JointAdmission>,
    ready: Option<JointGrantedBatch>,
    internal_proposal: Option<RootStageProposal>,
    internal_admission: Option<RootStageAdmission>,
    internal_ready: Option<RootStageReady>,
    wait: Option<JointProposalSlotWait>,
    failure: Option<WorkError>,
    polling: bool,
}
impl NativeControlBinding {
    pub(super) fn poll<'a>(
        &mut self,
        window: &ResultWindowAlias,
        execution: &WorkScope,
        origins: impl Iterator<Item = Result<SelectedNetworkOrigin<'a>, DependencyFactError>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        if let Some(error) = &self.failure {
            return Poll::Ready(Err(error.clone()));
        }
        if self.polling {
            return Poll::Ready(Err(WorkError::Conflict));
        }
        // Callback unwind leaves this exact source poisoned, never a fresh F.
        self.polling = true;
        let result = self.poll_original(window, execution, origins, cx);
        self.polling = false;
        if let Poll::Ready(Err(error)) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
    fn poll_original<'a>(
        &mut self,
        window: &ResultWindowAlias,
        execution: &WorkScope,
        origins: impl Iterator<Item = Result<SelectedNetworkOrigin<'a>, DependencyFactError>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        // Both checks use the original alias's exact Inner + execution WorkId.
        // The execution borrow was captured from the same capacity at creation.
        if !window.is_for_scope(execution) {
            return Poll::Ready(Err(WorkError::ForeignAuthority));
        }
        if let Some(binding) = window.internal_manifest_binding()? {
            return self.poll_internal(binding, origins, cx);
        }
        let same_scope = window.is_for_scope(window.root_scope());
        let scope = match self.phase {
            Phase::Root => window.root_scope(),
            Phase::Execution => execution,
            Phase::Bound => return Poll::Ready(Ok(())),
            Phase::Closed => return Poll::Ready(Err(WorkError::Released)),
        };
        if let Some(wait) = self.wait.as_mut() {
            match Pin::new(wait).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => drop(self.wait.take()),
            }
        }
        if self.admission.is_none() && self.ready.is_none() {
            match scope.try_begin_control_binding()? {
                JointTryBegin::Full(signal) => {
                    // One fixed original source position; no per-request wait
                    // allocation occurs before selecting that paid position.
                    self.wait = Some(signal.into_wait());
                    match Pin::new(self.wait.as_mut().unwrap()).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(())) => {
                            drop(self.wait.take());
                            // A real marker/free observation asks the same
                            // source to retry; it does not mint a Q ticket.
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                }
                JointTryBegin::Started(proposal) => self.proposal = Some(proposal),
            }
            for origin in origins {
                let selected =
                    origin.map_err(|_| WorkError::InvalidConfig("Native selected origin"))?;
                self.proposal
                    .as_mut()
                    .unwrap()
                    .push_selected_network(selected)?;
            }
            self.proposal.as_mut().unwrap().end_change()?;
            self.admission = Some(
                self.proposal
                    .take()
                    .unwrap()
                    .submit()
                    .map_err(|e| e.error)?,
            );
        }
        if let Some(admission) = self.admission.as_mut() {
            match Pin::new(admission).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.error)),
                Poll::Ready(Ok(ready)) => self.ready = Some(ready),
            }
            drop(self.admission.take());
        }
        let ready = self.ready.as_mut().expect("same original control reply");
        if !ready.new_claims.is_empty() {
            return Poll::Ready(Err(WorkError::Conflict));
        }
        let notification = ready
            .adjustment
            .as_mut()
            .ok_or(WorkError::Conflict)?
            .install_control_scope_in(scope)?;
        self.phase = match self.phase {
            Phase::Root if !same_scope => Phase::Execution,
            Phase::Root | Phase::Execution => Phase::Bound,
            Phase::Bound | Phase::Closed => unreachable!("one original control phase"),
        };
        // The exact scope and phase are stable before either wake or Q-body
        // destruction; an unwind retains the same reply in this source.
        notification.notify();
        drop(self.ready.take());
        if matches!(self.phase, Phase::Bound) {
            Poll::Ready(Ok(()))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
    fn poll_internal<'a>(
        &mut self,
        binding: &InternalManifestBinding,
        origins: impl Iterator<Item = Result<SelectedNetworkOrigin<'a>, DependencyFactError>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        match self.phase {
            Phase::Bound => return Poll::Ready(Ok(())),
            Phase::Closed => return Poll::Ready(Err(WorkError::Released)),
            Phase::Root => {}
            Phase::Execution => return Poll::Ready(Err(WorkError::Conflict)),
        }
        if self.proposal.is_some() || self.admission.is_some() || self.ready.is_some() {
            return Poll::Ready(Err(WorkError::Conflict));
        }
        if let Some(wait) = self.wait.as_mut() {
            match Pin::new(wait).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => drop(self.wait.take()),
            }
        }
        if self.internal_admission.is_none() && self.internal_ready.is_none() {
            if self.internal_proposal.is_none() {
                match binding.try_begin()? {
                    JointTryBegin::Full(signal) => {
                        self.wait = Some(signal.into_wait());
                        match Pin::new(self.wait.as_mut().unwrap()).poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                            Poll::Ready(Ok(())) => {
                                drop(self.wait.take());
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                    }
                    JointTryBegin::Started(proposal) => self.internal_proposal = Some(proposal),
                }
            }
            let proposal = self.internal_proposal.as_mut().unwrap();
            for origin in origins {
                proposal.push_payload_selected_network(origin.map_err(|_| WorkError::Conflict)?)?;
            }
            proposal.end_payload()?;
            proposal.end_stage()?;
            let admission = self
                .internal_proposal
                .take()
                .unwrap()
                .submit()
                .map_err(|failure| failure.error)?;
            self.internal_admission = Some(admission);
        }
        if self.internal_ready.is_none() {
            match Pin::new(self.internal_admission.as_mut().unwrap()).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(failure)) => return Poll::Ready(Err(failure.error)),
                Poll::Ready(Ok(ready)) => self.internal_ready = Some(ready),
            }
            drop(self.internal_admission.take());
        }
        let notification = self
            .internal_ready
            .as_mut()
            .unwrap()
            .install_binding_in(binding)?;
        self.phase = Phase::Bound;
        drop(self.internal_ready.take());
        notification.notify();
        Poll::Ready(Ok(()))
    }
    pub(super) fn close(&mut self) {
        self.phase = Phase::Closed;
        // These are the original bounded ledger withdrawal authors. Original
        // pending/reclaim pins stay in that ledger until its same driver reaps;
        // this method does not claim whole-WC drain or external physical exit.
        drop(self.wait.take());
        drop(self.ready.take());
        drop(self.admission.take());
        drop(self.internal_proposal.take());
        drop(self.internal_admission.take());
        drop(self.internal_ready.take());
        drop(self.proposal.take());
        self.phase = Phase::Closed;
    }
}

#[cfg(test)]
pub(super) fn metadata_layouts() -> [std::alloc::Layout; 2] {
    [
        std::alloc::Layout::new::<NativeControlBinding>(),
        JointProposalSlotWait::metadata_layouts().1,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_workload_control::joint_admission::*;
    use novarocks_workload_control::{
        ControlStorageConfig, PollRoleSourceBacking, ResultCapacityConfig, ResultWindowClass,
        WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use std::{num::NonZeroUsize, task::Waker};

    fn fixture() -> (
        WorkloadControl,
        JointProgressDriver,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let count = 8;
        let storage = ControlStorageConfig::new(count, &[]).with_process_source_backing(
            PollRoleSourceBacking::from_layouts(&metadata_layouts(), &[]).unwrap(),
        );
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        (control, driver, capacity)
    }
    fn drive(driver: &mut JointProgressDriver) {
        for _ in 0..100_000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("original finite driver did not become idle");
    }
    fn bind(
        source: &mut NativeControlBinding,
        window: &ResultWindowAlias,
        scope: &WorkScope,
        endpoints: &[novarocks_execution::runtime::endpoint::RuntimeEndpoint; 2],
        driver: &mut JointProgressDriver,
    ) {
        let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
        for _ in 0..100_000 {
            match source.poll(
                window,
                scope,
                endpoints
                    .iter()
                    .map(|endpoint| transport.selected_network_origin(endpoint.native_endpoint())),
                &mut Context::from_waker(Waker::noop()),
            ) {
                Poll::Ready(Ok(())) => {
                    drive(driver);
                    return;
                }
                Poll::Ready(Err(error)) => panic!("original production binding failed: {error:?}"),
                Poll::Pending => drive(driver),
            }
        }
        panic!("original production binding did not finish");
    }
    #[test]
    fn original_projection_binding_precedes_native_source_capture_with_other_builders_live() {
        let (control, mut driver, capacity) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let window_grant = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        let window = window_grant.retain_alias();
        let authority = window.native_action_authority().unwrap();
        // Quote the original replay/creation headers using their actual
        // Native author; these quantities alone confer no source authority.
        let quote = crate::native::task_transport::queued_creation_quote(0).unwrap();
        assert!(
            matches!(
                authority.begin_queued_input(JointLane::Foreground, quote),
                Err(failure) if failure.error == WorkError::Conflict
            ),
            "Native source capture rejects this exact unbound Root"
        );
        let endpoints = [
            novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 19010)
                .unwrap(),
            novarocks_execution::runtime::endpoint::RuntimeEndpoint::new(
                "control.test.invalid",
                19061,
            )
            .unwrap(),
        ];
        let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
        let first = scope.begin_control_binding().unwrap();
        let second = scope.begin_control_binding().unwrap();
        let mut binding = NativeControlBinding::default();
        assert!(
            binding
                .poll(
                    &window,
                    &scope,
                    endpoints
                        .iter()
                        .map(|endpoint| transport
                            .selected_network_origin(endpoint.native_endpoint())),
                    &mut Context::from_waker(Waker::noop())
                )
                .is_pending()
        );
        assert!(
            binding.wait.is_none(),
            "unsubmitted builders do not impose a proposal quota"
        );
        assert!(
            binding.admission.is_some(),
            "same original binding request is retained"
        );
        bind(&mut binding, &window, &scope, &endpoints, &mut driver);
        let original = authority
            .begin_queued_input(JointLane::Foreground, quote)
            .unwrap();
        drop(original);
        drop((first, second));
        drive(&mut driver);
        assert!(binding.wait.is_none() && binding.ready.is_none() && binding.admission.is_none());
        // The separate execution path uses the same exact capacity association,
        // not numeric equality or a second Root/result permit.
        let mut child_admission = scope.request_joint_child().unwrap();
        drive(&mut driver);
        let mut reply =
            match Pin::new(&mut child_admission).poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(Ok(reply)) => reply,
                _ => panic!("actual same-root child must grant"),
            };
        let mut child = None;
        reply
            .adjustment
            .as_mut()
            .unwrap()
            .install_child_in(&scope, &mut child)
            .unwrap()
            .notify();
        drop(reply);
        let child = child.unwrap();
        let child_scope = child.scope();
        let child_window = window.for_child(&child_scope).unwrap();
        assert!(!child_window.is_for_scope(child_window.root_scope()));
        let mut child_binding = NativeControlBinding::default();
        drive(&mut driver);
        bind(
            &mut child_binding,
            &child_window,
            &child_scope,
            &endpoints,
            &mut driver,
        );
        assert!(matches!(child_binding.phase, Phase::Bound));
        let original = child_window
            .native_action_authority()
            .unwrap()
            .begin_queued_input(JointLane::Foreground, quote)
            .unwrap();
        drop(original);
        drive(&mut driver);
        child_binding.close();
        drop(child_window);
        child.complete();
        binding.close();
        drop(window);
        drop(window_grant);
        root.business.release();
        root.owner.complete();
        drive(&mut driver);
        control.close_admission();
        assert!(
            control.shutdown().is_ok(),
            "same original driver/owner must actually drain"
        );
    }
    #[cfg(novarocks_dev_controls)]
    fn poll_manifest(
        binding: &mut NativeControlBinding,
        window: &ResultWindowAlias,
        scope: &WorkScope,
        manifest: &crate::query_execution::artifact::TaskManifestBinding,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
        binding.poll(
            window,
            scope,
            manifest.selected_network_origins(&transport),
            cx,
        )
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn successor_b_actual_origins_wait_for_original_positive_binding_and_exclude_eligible_c() {
        use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId};
        let (control, mut driver, capacity) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let original_grant = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        let window = original_grant.retain_alias();
        let processes = [
            BackendProcessId::new_v7(),
            BackendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        ];
        let first =
            QueryExecutionId::new(QueryId::new(0x77, 0x11), AttemptId::new(1).unwrap()).unwrap();
        let successor =
            QueryExecutionId::new(first.query_id(), AttemptId::new(2).unwrap()).unwrap();
        let (a, _) =
            crate::query_execution::artifact::TaskManifestBinding::qualification_fixture_for_test(
                first, &processes, 0,
            );
        let (b, schedule) =
            crate::query_execution::artifact::TaskManifestBinding::qualification_fixture_for_test(
                successor, &processes, 1,
            );
        let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
        let origins = |manifest: &crate::query_execution::artifact::TaskManifestBinding| {
            manifest
                .selected_network_origins(&transport)
                .map(|origin| origin.unwrap().into_owned().as_str().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            origins(&a),
            [
                "http://127.0.0.1:19010",
                "http://control-0.test.invalid:19061"
            ]
        );
        assert_eq!(
            origins(&b),
            [
                "http://127.0.0.1:19011",
                "http://control-1.test.invalid:19061"
            ]
        );
        assert!(b.eligible_backends().contains(&processes[2]));
        assert_eq!(b.contexts().len(), 1);
        assert!(
            schedule
                .contexts()
                .iter()
                .all(|context| context.backend_process_id() == processes[1])
        );
        let mut initial = NativeControlBinding::default();
        for _ in 0..100_000 {
            match poll_manifest(
                &mut initial,
                &window,
                &scope,
                &a,
                &mut Context::from_waker(Waker::noop()),
            ) {
                Poll::Ready(result) => {
                    result.unwrap();
                    break;
                }
                Poll::Pending => drive(&mut driver),
            }
        }
        assert!(
            matches!(initial.phase, Phase::Bound),
            "finite original binding must finish"
        );
        drive(&mut driver);
        let before = capacity.snapshot();
        let unsubmitted = [
            scope.begin_control_binding().unwrap(),
            scope.begin_control_binding().unwrap(),
        ];
        let mut replacement = NativeControlBinding::default();
        assert!(
            poll_manifest(
                &mut replacement,
                &window,
                &scope,
                &b,
                &mut Context::from_waker(Waker::noop())
            )
            .is_pending()
        );
        assert!(replacement.wait.is_none() && replacement.admission.is_some());
        assert!(!matches!(replacement.phase, Phase::Bound));
        assert_eq!(capacity.snapshot(), before);
        assert!(
            poll_manifest(
                &mut replacement,
                &window,
                &scope,
                &b,
                &mut Context::from_waker(Waker::noop())
            )
            .is_pending()
        );
        assert!(replacement.admission.is_some());
        drive(&mut driver);
        assert!(matches!(
            poll_manifest(
                &mut replacement,
                &window,
                &scope,
                &b,
                &mut Context::from_waker(Waker::noop())
            ),
            Poll::Ready(Ok(()))
        ));
        assert!(matches!(replacement.phase, Phase::Bound));
        drop(unsubmitted);
        // Original Client position/bytes were never replaced during A -> B.
        assert_eq!(capacity.snapshot(), before);
        replacement.close();
        initial.close();
        drop((window, original_grant));
        root.business.release();
        root.owner.complete();
        drive(&mut driver);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    struct BindingNotificationPanic;
    struct ThrowBindingWake;
    impl std::task::Wake for ThrowBindingWake {
        fn wake(self: std::sync::Arc<Self>) {
            std::panic::panic_any(BindingNotificationPanic);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            std::panic::panic_any(BindingNotificationPanic);
        }
    }

    #[test]
    fn installed_binding_notify_panic_keeps_original_receipt_and_refuses_reentry() {
        let (control, mut driver, capacity) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let grant = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        let window = grant.retain_alias();
        let endpoints = [
            novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 19010)
                .unwrap(),
            novarocks_execution::runtime::endpoint::RuntimeEndpoint::new(
                "control.test.invalid",
                19061,
            )
            .unwrap(),
        ];
        let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
        let mut binding = NativeControlBinding::default();
        assert!(
            binding
                .poll(
                    &window,
                    &scope,
                    endpoints
                        .iter()
                        .map(|e| transport.selected_network_origin(e.native_endpoint())),
                    &mut Context::from_waker(Waker::noop())
                )
                .is_pending()
        );
        drive(&mut driver);
        let Poll::Ready(Ok(reply)) = Pin::new(binding.admission.as_mut().unwrap())
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("original receipt must arrive");
        };
        binding.ready = Some(reply);
        drop(binding.admission.take());
        drive(&mut driver);
        let after = control.progress_revision();
        let mut observed = Box::pin(control.wait_progress(after));
        let waker = Waker::from(std::sync::Arc::new(ThrowBindingWake));
        assert!(
            observed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let original = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            binding.poll(
                &window,
                &scope,
                endpoints
                    .iter()
                    .map(|e| transport.selected_network_origin(e.native_endpoint())),
                &mut Context::from_waker(Waker::noop()),
            )
        }))
        .expect_err("the actual committed notification invokes the registered throwing waker");
        assert!(original.is::<BindingNotificationPanic>());
        assert!(matches!(binding.phase, Phase::Bound));
        assert!(binding.polling && binding.ready.is_some());
        assert!(matches!(
            binding.poll(
                &window,
                &scope,
                endpoints
                    .iter()
                    .map(|e| transport.selected_network_origin(e.native_endpoint())),
                &mut Context::from_waker(Waker::noop())
            ),
            Poll::Ready(Err(WorkError::Conflict))
        ));
        drop((observed, original));
        binding.close();
        drop((window, grant));
        root.business.release();
        root.owner.complete();
        drive(&mut driver);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }
}
