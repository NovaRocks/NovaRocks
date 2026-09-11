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

//! Frontend implementation of the process-wide Query Application Native-open
//! boundary.

use novarocks_query_application::api::{
    ActivatedNativeAttempt, ActiveNativeAttemptOwner, CancellationView, DormantNativeAttemptOwner,
    LogicalExecutionNativePort, LogicalNativeOpenFuture, LogicalNativeOpenRequest,
    NativeAttemptActivationFailure, NativeAttemptActivationFuture, NativeAttemptConvergenceFuture,
    NativeAttemptPreparationError, NativeAttemptPreparationFailure, NativeAttemptPreparationFuture,
    NativeAttemptPreparationPort, NativeAttemptPreparationRequest, NativeAttemptRunFuture,
    QueryExecutionError, QueryExecutionErrorKind,
};
use novarocks_query_application::coordination::{
    AttemptFailureClass, AttemptSchedule, NativeAttemptDrive,
};

use crate::common::backend_topology::BackendTopologyService;
use crate::query_execution::artifact::{
    ManifestBoundNativeAttemptInputs, PreparedDistributedAttemptTemplate,
    SnapshotBoundDormantAttemptInputs,
};

/// Stateless process adapter which consumes the exact Native seed already
/// sealed into an executable Query Application request.
///
/// Attempt-specific state belongs to the move-only preparation port carried by
/// that seed. Keeping this adapter stateless lets the process host share one
/// narrow handle without becoming an alternate execution owner.
#[derive(Debug, Default)]
pub(crate) struct FrontendLogicalExecutionNativePort;

impl LogicalExecutionNativePort for FrontendLogicalExecutionNativePort {
    fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
        Box::pin(async move { request.bind().map_err(Into::into) })
    }
}

/// Frontend-local Task protocol and transport behavior used by the fixed
/// snapshot-owning dormant adapter.
///
/// The behavior only borrows the manifest already bound by the adapter. It
/// cannot provide eligible backend identities, substitute a topology snapshot,
/// or move attempt leases into an activation future. The adapter transfers the
/// manifest only after activation returns an active behavior successfully.
pub(crate) trait FrontendActiveAttemptBehavior<M = ManifestBoundNativeAttemptInputs>:
    std::fmt::Debug + Send + 'static
{
    fn run<'a>(
        &'a mut self,
        inputs: &'a mut M,
        drive: &'a NativeAttemptDrive,
        cancellation: CancellationView,
    ) -> NativeAttemptRunFuture<'a>;

    fn converge<'a>(
        &'a mut self,
        inputs: &'a mut M,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a>;
}

pub(crate) trait FrontendDormantAttemptBehavior<M = ManifestBoundNativeAttemptInputs>:
    std::fmt::Debug + Send + 'static
{
    type ActiveBehavior: FrontendActiveAttemptBehavior<M>;

    fn activate<'a>(
        &'a mut self,
        inputs: &'a mut M,
        cancellation: CancellationView,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Self::ActiveBehavior, NativeAttemptActivationFailure>,
                > + Send
                + 'a,
        >,
    >;

    fn converge<'a>(
        &'a mut self,
        inputs: &'a mut M,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a>;
}

/// Frontend-local constructor for Task protocol and transport behavior. The
/// adapter itself remains the only constructor of the complete dormant owner.
pub(crate) trait FrontendDormantAttemptFactory: std::fmt::Debug + Send + 'static {
    type Behavior: FrontendDormantAttemptBehavior<ManifestBoundNativeAttemptInputs>;

    fn create(&mut self) -> Result<Self::Behavior, NativeAttemptPreparationError>;
}

trait RetainedAttemptInputs: std::fmt::Debug + Send + 'static {
    type Manifest: std::fmt::Debug + Send + 'static;

    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId];

    fn bind_manifest(
        self,
        schedule: &AttemptSchedule,
    ) -> Result<Self::Manifest, NativeAttemptActivationFailure>;
}

impl RetainedAttemptInputs for SnapshotBoundDormantAttemptInputs {
    type Manifest = ManifestBoundNativeAttemptInputs;

    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        self.eligible_backends()
    }

    fn bind_manifest(
        self,
        schedule: &AttemptSchedule,
    ) -> Result<Self::Manifest, NativeAttemptActivationFailure> {
        self.bind_manifest(schedule).map_err(|error| {
            NativeAttemptActivationFailure::new(attempt_runtime_failure(
                AttemptFailureClass::ContractViolation,
                QueryExecutionErrorKind::InvalidRequest,
                error.to_string(),
            ))
        })
    }
}

/// Fixed dormant owner which keeps topology authority in the adapter instead
/// of delegating it to a replaceable factory implementation.
struct SnapshotBoundDormantAttemptOwner<B, I = SnapshotBoundDormantAttemptInputs>
where
    I: RetainedAttemptInputs,
{
    dormant_inputs: Option<I>,
    manifest_inputs: Option<I::Manifest>,
    behavior: B,
}

impl<B, I> std::fmt::Debug for SnapshotBoundDormantAttemptOwner<B, I>
where
    B: std::fmt::Debug,
    I: RetainedAttemptInputs,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotBoundDormantAttemptOwner")
            .field("dormant_inputs", &self.dormant_inputs)
            .field("manifest_inputs", &self.manifest_inputs)
            .field("behavior", &self.behavior)
            .finish()
    }
}

impl<B, I> SnapshotBoundDormantAttemptOwner<B, I>
where
    I: RetainedAttemptInputs,
{
    fn new(inputs: I, behavior: B) -> Self {
        Self {
            dormant_inputs: Some(inputs),
            manifest_inputs: None,
            behavior,
        }
    }

    #[cfg(test)]
    fn from_manifest(inputs: I::Manifest, behavior: B) -> Self {
        Self {
            dormant_inputs: None,
            manifest_inputs: Some(inputs),
            behavior,
        }
    }

    fn retained_eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        self.dormant_inputs
            .as_ref()
            .expect("a dormant Native attempt retains inputs until its sole activation")
            .eligible_backends()
    }

    fn bind_manifest(
        &mut self,
        schedule: &AttemptSchedule,
    ) -> Result<(), NativeAttemptActivationFailure> {
        if self.manifest_inputs.is_some() {
            return Ok(());
        }
        let inputs = self
            .dormant_inputs
            .take()
            .expect("Query Application activates a dormant Native attempt only once");
        self.manifest_inputs = Some(inputs.bind_manifest(schedule)?);
        Ok(())
    }

    fn activate_fixed_retained<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        SnapshotBoundActiveAttemptOwner<I::Manifest, B::ActiveBehavior>,
                        NativeAttemptActivationFailure,
                    >,
                > + Send
                + 'a,
        >,
    >
    where
        B: FrontendDormantAttemptBehavior<I::Manifest>,
    {
        let behavior = &mut self.behavior;
        let manifest_inputs = &mut self.manifest_inputs;
        Box::pin(async move {
            let active_behavior = behavior
                .activate(
                    manifest_inputs
                        .as_mut()
                        .expect("activation retains its manifest inputs"),
                    cancellation,
                )
                .await?;
            let inputs = manifest_inputs
                .take()
                .expect("successful activation transfers the retained manifest once");
            Ok(SnapshotBoundActiveAttemptOwner {
                inputs,
                behavior: active_behavior,
            })
        })
    }

    fn activate_retained<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeAttemptActivationFuture<'a>
    where
        B: FrontendDormantAttemptBehavior<I::Manifest>,
    {
        let activation = self.activate_fixed_retained(cancellation);
        Box::pin(async move { activation.await.map(ActivatedNativeAttempt::new) })
    }

    fn converge_retained<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a>
    where
        B: FrontendDormantAttemptBehavior<I::Manifest>,
    {
        match self.manifest_inputs.as_mut() {
            Some(inputs) => self.behavior.converge(inputs, cancellation),
            None => Box::pin(async {}),
        }
    }
}

struct SnapshotBoundActiveAttemptOwner<M, B> {
    inputs: M,
    behavior: B,
}

impl<M, B> std::fmt::Debug for SnapshotBoundActiveAttemptOwner<M, B>
where
    M: std::fmt::Debug,
    B: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotBoundActiveAttemptOwner")
            .field("inputs", &self.inputs)
            .field("behavior", &self.behavior)
            .finish()
    }
}

impl<M, B> ActiveNativeAttemptOwner for SnapshotBoundActiveAttemptOwner<M, B>
where
    M: std::fmt::Debug + Send + 'static,
    B: FrontendActiveAttemptBehavior<M>,
{
    fn run<'a>(
        &'a mut self,
        drive: &'a NativeAttemptDrive,
        cancellation: CancellationView,
    ) -> NativeAttemptRunFuture<'a> {
        self.behavior.run(&mut self.inputs, drive, cancellation)
    }

    fn converge<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.behavior.converge(&mut self.inputs, cancellation)
    }
}

impl<B> DormantNativeAttemptOwner for SnapshotBoundDormantAttemptOwner<B>
where
    B: FrontendDormantAttemptBehavior,
{
    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        self.retained_eligible_backends()
    }

    fn activate<'a>(
        &'a mut self,
        schedule: &'a AttemptSchedule,
        cancellation: CancellationView,
    ) -> NativeAttemptActivationFuture<'a> {
        if let Err(error) = self.bind_manifest(schedule) {
            return Box::pin(async move { Err(error) });
        }
        self.activate_retained(cancellation)
    }

    fn converge<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.converge_retained(cancellation)
    }
}

/// Per-logical-execution Frontend adapter for Native attempt preparation.
///
/// Each call captures membership once, derives the Query Application placement
/// inputs from the owner that retains that capture, and returns a dormant owner
/// which must consume the same capture at activation. Recovery calls reuse the
/// immutable template while capturing a new snapshot for the new attempt.
pub(crate) struct FrontendNativeAttemptPreparationPort<F> {
    template: PreparedDistributedAttemptTemplate,
    topology: BackendTopologyService,
    dormant_factory: F,
}

impl<F> std::fmt::Debug for FrontendNativeAttemptPreparationPort<F>
where
    F: FrontendDormantAttemptFactory,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrontendNativeAttemptPreparationPort")
            .field("dormant_factory", &self.dormant_factory)
            .finish_non_exhaustive()
    }
}

impl<F> FrontendNativeAttemptPreparationPort<F>
where
    F: FrontendDormantAttemptFactory,
{
    pub(crate) fn new(
        template: PreparedDistributedAttemptTemplate,
        topology: BackendTopologyService,
        dormant_factory: F,
    ) -> Self {
        Self {
            template,
            topology,
            dormant_factory,
        }
    }
}

impl<F> NativeAttemptPreparationPort for FrontendNativeAttemptPreparationPort<F>
where
    F: FrontendDormantAttemptFactory,
{
    fn prepare(
        &mut self,
        request: NativeAttemptPreparationRequest,
    ) -> NativeAttemptPreparationFuture {
        let prepared = (|| {
            let snapshot = self.topology.snapshot().map_err(|error| {
                attempt_failure(
                    AttemptFailureClass::RecoverableInfrastructure,
                    QueryExecutionErrorKind::Failed,
                    format!("failed to capture Native attempt topology: {error}"),
                )
            })?;
            let scan_work = self.template.native_scan_work_facts().map_err(|error| {
                attempt_failure(
                    AttemptFailureClass::ContractViolation,
                    QueryExecutionErrorKind::InvalidRequest,
                    error.to_string(),
                )
            })?;
            let inputs =
                SnapshotBoundDormantAttemptInputs::capture(&self.template, &request, snapshot)
                    .map_err(|error| {
                        attempt_failure(
                            AttemptFailureClass::ContractViolation,
                            QueryExecutionErrorKind::InvalidRequest,
                            error.to_string(),
                        )
                    })?;
            let behavior = self.dormant_factory.create()?;
            let owner = SnapshotBoundDormantAttemptOwner::new(inputs, behavior);
            request
                .bind(scan_work, owner)
                .map_err(NativeAttemptPreparationError::from)
        })();
        Box::pin(async move { prepared })
    }
}

fn attempt_failure(
    class: AttemptFailureClass,
    kind: QueryExecutionErrorKind,
    message: impl Into<std::sync::Arc<str>>,
) -> NativeAttemptPreparationError {
    attempt_runtime_failure(class, kind, message).into()
}

fn attempt_runtime_failure(
    class: AttemptFailureClass,
    kind: QueryExecutionErrorKind,
    message: impl Into<std::sync::Arc<str>>,
) -> NativeAttemptPreparationFailure {
    NativeAttemptPreparationFailure::new(class, QueryExecutionError::new(kind, message))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use futures::FutureExt;
    use novarocks_query_application::api::{
        ActiveNativeAttemptOwner, CancellationView, NativeAttemptActivationFailure,
        NativeAttemptConvergenceFuture, NativeAttemptPreparationError, NativeAttemptRunFuture,
        NativeAttemptTerminal,
    };
    use novarocks_query_application::coordination::{AttemptSchedule, NativeAttemptDrive};
    use novarocks_types::identity::BackendProcessId;
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    use super::{
        FrontendActiveAttemptBehavior, FrontendDormantAttemptBehavior,
        FrontendDormantAttemptFactory, RetainedAttemptInputs, SnapshotBoundDormantAttemptOwner,
        attempt_runtime_failure,
    };
    use crate::query_execution::artifact::ManifestBoundNativeAttemptInputs;

    #[derive(Debug)]
    struct CapturedInputs {
        eligible_backends: Vec<BackendProcessId>,
    }

    impl RetainedAttemptInputs for CapturedInputs {
        type Manifest = TestManifest;

        fn eligible_backends(&self) -> &[BackendProcessId] {
            &self.eligible_backends
        }

        fn bind_manifest(
            self,
            _schedule: &AttemptSchedule,
        ) -> Result<Self::Manifest, NativeAttemptActivationFailure> {
            Ok(TestManifest {
                marker: 73,
                identity: Box::new(91),
            })
        }
    }

    #[derive(Debug)]
    struct AdversarialBehavior {
        replacement_backends: Vec<BackendProcessId>,
    }

    #[derive(Debug)]
    struct NeverActiveBehavior;

    impl FrontendActiveAttemptBehavior for NeverActiveBehavior {
        fn run<'a>(
            &'a mut self,
            _inputs: &'a mut ManifestBoundNativeAttemptInputs,
            _drive: &'a NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { panic!("adversarial eligibility test must not run") })
        }

        fn converge<'a>(
            &'a mut self,
            _inputs: &'a mut ManifestBoundNativeAttemptInputs,
            _cancellation: CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    impl FrontendDormantAttemptBehavior for AdversarialBehavior {
        type ActiveBehavior = NeverActiveBehavior;

        fn activate<'a>(
            &'a mut self,
            _inputs: &'a mut ManifestBoundNativeAttemptInputs,
            _cancellation: CancellationView,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Self::ActiveBehavior, NativeAttemptActivationFailure>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async { panic!("adversarial eligibility test must not activate") })
        }

        fn converge<'a>(
            &'a mut self,
            _inputs: &'a mut ManifestBoundNativeAttemptInputs,
            _cancellation: CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    #[derive(Debug)]
    struct AdversarialFactory {
        replacement_backend: BackendProcessId,
    }

    impl FrontendDormantAttemptFactory for AdversarialFactory {
        type Behavior = AdversarialBehavior;

        fn create(&mut self) -> Result<Self::Behavior, NativeAttemptPreparationError> {
            Ok(AdversarialBehavior {
                replacement_backends: vec![self.replacement_backend],
            })
        }
    }

    #[test]
    fn adversarial_factory_cannot_replace_snapshot_derived_eligible_backends() {
        let captured = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let mut factory = AdversarialFactory {
            replacement_backend: replacement,
        };
        let owner = SnapshotBoundDormantAttemptOwner::new(
            CapturedInputs {
                eligible_backends: vec![captured],
            },
            factory.create().expect("adversarial behavior"),
        );

        assert_eq!(owner.retained_eligible_backends(), &[captured]);
        assert_eq!(owner.behavior.replacement_backends, vec![replacement]);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct TestManifest {
        marker: u8,
        identity: Box<u8>,
    }

    #[derive(Clone, Copy, Debug)]
    enum ActivationMode {
        Pending,
        Error,
        Panic,
        Success,
    }

    #[derive(Debug)]
    struct LifecycleBehavior {
        mode: ActivationMode,
        convergence_observed: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct TestActiveBehavior {
        identity_address: usize,
        run_observed: Arc<AtomicBool>,
        convergence_observed: Arc<AtomicBool>,
    }

    impl TestActiveBehavior {
        fn observe_run_inputs(&self, inputs: &mut TestManifest) {
            assert_eq!(inputs.marker, 73);
            assert_eq!(
                inputs.identity.as_ref() as *const u8 as usize,
                self.identity_address
            );
            self.run_observed.store(true, Ordering::SeqCst);
        }
    }

    impl FrontendActiveAttemptBehavior<TestManifest> for TestActiveBehavior {
        fn run<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _drive: &'a NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            self.observe_run_inputs(inputs);
            Box::pin(async { NativeAttemptTerminal::Completed })
        }

        fn converge<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _cancellation: CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            assert_eq!(inputs.marker, 73);
            assert_eq!(
                inputs.identity.as_ref() as *const u8 as usize,
                self.identity_address
            );
            let observed = Arc::clone(&self.convergence_observed);
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
            })
        }
    }

    impl FrontendDormantAttemptBehavior<TestManifest> for LifecycleBehavior {
        type ActiveBehavior = TestActiveBehavior;

        fn activate<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _cancellation: CancellationView,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Self::ActiveBehavior, NativeAttemptActivationFailure>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                assert_eq!(inputs.marker, 73);
                match self.mode {
                    ActivationMode::Pending => std::future::pending().await,
                    ActivationMode::Error => Err(NativeAttemptActivationFailure::new(
                        attempt_runtime_failure(
                            super::AttemptFailureClass::RecoverableInfrastructure,
                            super::QueryExecutionErrorKind::Failed,
                            "activation failed",
                        ),
                    )),
                    ActivationMode::Panic => panic!("activation poll panic"),
                    ActivationMode::Success => Ok(TestActiveBehavior {
                        identity_address: inputs.identity.as_ref() as *const u8 as usize,
                        run_observed: Arc::new(AtomicBool::new(false)),
                        convergence_observed: Arc::clone(&self.convergence_observed),
                    }),
                }
            })
        }

        fn converge<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _cancellation: CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            assert_eq!(inputs.marker, 73);
            let observed = Arc::clone(&self.convergence_observed);
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
            })
        }
    }

    fn governed_cancellation() -> (WorkloadControl, RootWork, CancellationView) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: 1 << 18,
            },
        )
        .expect("valid workload control");
        control.mark_ready().expect("workload control ready");
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query work admitted");
        let cancellation = root
            .owner
            .scope()
            .cancellation()
            .expect("active work cancellation view");
        (control, root, cancellation)
    }

    fn lifecycle_owner(
        mode: ActivationMode,
    ) -> (
        SnapshotBoundDormantAttemptOwner<LifecycleBehavior, CapturedInputs>,
        Arc<AtomicBool>,
    ) {
        let convergence_observed = Arc::new(AtomicBool::new(false));
        (
            SnapshotBoundDormantAttemptOwner::from_manifest(
                TestManifest {
                    marker: 73,
                    identity: Box::new(91),
                },
                LifecycleBehavior {
                    mode,
                    convergence_observed: Arc::clone(&convergence_observed),
                },
            ),
            convergence_observed,
        )
    }

    async fn assert_retained_and_converges(
        owner: &mut SnapshotBoundDormantAttemptOwner<LifecycleBehavior, CapturedInputs>,
        convergence_observed: &AtomicBool,
        cancellation: CancellationView,
    ) {
        assert_eq!(owner.manifest_inputs.as_ref().unwrap().marker, 73);
        owner.converge_retained(cancellation).await;
        assert!(convergence_observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_activation_future_retains_manifest_for_convergence() {
        let (_control, root, cancellation) = governed_cancellation();
        let (mut owner, convergence_observed) = lifecycle_owner(ActivationMode::Pending);
        let mut activation = owner.activate_retained(cancellation.clone());
        assert!(
            tokio::time::timeout(Duration::from_millis(1), &mut activation)
                .await
                .is_err()
        );
        root.owner.cancel(CancellationReason::ClientDisconnected);
        drop(activation);

        assert_retained_and_converges(&mut owner, &convergence_observed, cancellation).await;
    }

    #[tokio::test]
    async fn activation_error_retains_manifest_for_convergence() {
        let (_control, _root, cancellation) = governed_cancellation();
        let (mut owner, convergence_observed) = lifecycle_owner(ActivationMode::Error);
        assert!(owner.activate_retained(cancellation.clone()).await.is_err());

        assert_retained_and_converges(&mut owner, &convergence_observed, cancellation).await;
    }

    #[tokio::test]
    async fn activation_poll_panic_retains_manifest_for_convergence() {
        let (_control, _root, cancellation) = governed_cancellation();
        let (mut owner, convergence_observed) = lifecycle_owner(ActivationMode::Panic);
        let outcome = std::panic::AssertUnwindSafe(owner.activate_retained(cancellation.clone()))
            .catch_unwind()
            .await;
        assert!(outcome.is_err());

        assert_retained_and_converges(&mut owner, &convergence_observed, cancellation).await;
    }

    #[tokio::test]
    async fn successful_activation_transfers_one_manifest_to_the_fixed_active_owner() {
        let (_control, _root, cancellation) = governed_cancellation();
        let (mut dormant, convergence_observed) = lifecycle_owner(ActivationMode::Success);
        let identity_address =
            dormant.manifest_inputs.as_ref().unwrap().identity.as_ref() as *const u8 as usize;
        let mut active = dormant
            .activate_fixed_retained(cancellation.clone())
            .await
            .expect("activation succeeds");

        assert!(dormant.manifest_inputs.is_none());
        assert_eq!(active.inputs.marker, 73);
        assert_eq!(
            active.inputs.identity.as_ref() as *const u8 as usize,
            identity_address
        );
        active.behavior.observe_run_inputs(&mut active.inputs);
        assert!(active.behavior.run_observed.load(Ordering::SeqCst));
        ActiveNativeAttemptOwner::converge(&mut active, cancellation).await;
        assert!(convergence_observed.load(Ordering::SeqCst));
    }
}
