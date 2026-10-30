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

//! Event-driven initialization of one exact distributed attempt.
//!
//! The initializer owns the immutable logical artifacts while it reacquires
//! attempt access and opens split sources. Each synchronous Connector call is
//! one supervised process job, with no second capacity wait after query
//! admission. Only
//! [`AttemptReady`] can hand the artifacts,
//! source plan, and sealed credential leases to task-round construction.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use crate::query_execution::artifact::FragmentId;
use novarocks_execution_contract::failure::SharedFailureCause;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_spi::connector::read_stack::ConnectorReadBinding;
use novarocks_spi::connector::{ConnectorError, ConnectorRequestContext, ConnectorRequestScope};

use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::artifact::{PreparedDistributedQuery, ValidatedFragmentSchedule};
use crate::query_execution::completion::QueryAttemptReservation;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle_plan::{
    AttemptCredentialLeaseCollector, QueryCredentialLeases,
};
use crate::query_execution::split_assignment_round::{
    OpenRoundSplitSources, OpenedRoundSplitSource, RoundSplitAssignmentPlan,
    RoundSplitSourceRecipe, assignment_targets,
};
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
use crate::task_execution::blocking_io::{
    ConnectorBlockingIoAdmission, ConnectorBlockingIoError, ConnectorBlockingIoResponsibility,
    ConnectorBlockingIoSupervisor, ConnectorOperationJob as ConnectorBlockingIoJob,
};
use crate::task_protocol_config::TaskUpdateRetryPolicy;
use novarocks_query_application::api::StatementNativeHealthFailure;
use novarocks_workload_control::{ResultWindowAlias, WorkScope};

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

fn failed_connector(error: ConnectorError) -> DistributedQueryError {
    DistributedQueryError::from_source(
        DistributedQueryErrorKind::Failed,
        SharedFailureCause::new(error),
    )
}

/// Credential material stays under its planning collector until every split
/// source of this exact attempt is open.
pub(crate) enum RoundCredentialLeaseSource {
    Fresh {
        collector: Arc<AttemptCredentialLeaseCollector>,
        request_scope: ConnectorRequestScope,
    },
    Reservation {
        reservation: Option<QueryAttemptReservation>,
        observed_collected: Option<Arc<AtomicBool>>,
    },
}

impl RoundCredentialLeaseSource {
    /// A component fixture uses the original one-shot Native drain implementation.
    #[cfg(all(test, novarocks_dev_controls))]
    pub(crate) fn test_into_credential_leases(
        self,
    ) -> Result<QueryCredentialLeases, DistributedQueryError> {
        let mut source = self;
        source.drain_credential_leases()
    }

    pub(crate) fn fresh(execution: QueryExecutionId) -> Self {
        Self::Fresh {
            collector: AttemptCredentialLeaseCollector::new(execution),
            request_scope: ConnectorRequestScope::new(),
        }
    }

    pub(crate) fn connector_request_context(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorRequestContext, String> {
        match self {
            Self::Fresh {
                collector,
                request_scope,
            } => {
                let context = context
                    .with_request_scope(request_scope.clone())
                    .with_storage_resolver(collector.storage_resolver())
                    .with_vended_credential_lease_sink(collector.sink());
                Ok(context)
            }
            Self::Reservation { reservation, .. } => reservation
                .as_ref()
                .expect("attempt reservation exists before credential sealing")
                .connector_request_context(
                    context,
                    #[cfg(novarocks_dev_controls)]
                    None,
                ),
        }
    }

    fn publish_observed(&self) {
        if let Self::Reservation {
            observed_collected: Some(observed_collected),
            reservation: Some(reservation),
        } = self
            && reservation.has_collected_credential_leases()
        {
            observed_collected.store(true, Ordering::Release);
        }
    }

    fn drain_credential_leases(&mut self) -> Result<QueryCredentialLeases, DistributedQueryError> {
        self.publish_observed();
        match self {
            Self::Fresh { collector, .. } => collector.drain_credential_leases(),
            Self::Reservation { reservation, .. } => reservation
                .as_ref()
                .expect("original attempt reservation remains installed")
                .drain_credential_leases(),
        }
    }
}

impl Drop for RoundCredentialLeaseSource {
    fn drop(&mut self) {
        self.publish_observed();
        if let Self::Fresh { collector, .. } = self {
            // A terminal holder may keep the collector alive after this source
            // exits. Close its production rights without claiming actual exit.
            let _ = collector.seal_for_terminal();
        }
        // A still-owned Reservation seals through its own Drop; a consumed
        // NativeDrained collector keeps the same immutable storage table.
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AttemptSourceIdentity {
    execution_id: QueryExecutionId,
    fragment_id: FragmentId,
    plan_node_id: i32,
    generation: ConnectorReadBinding,
}

struct AttemptSourceRecipe<R> {
    identity: AttemptSourceIdentity,
    recipe: R,
}

struct OpenedAttemptSource<O> {
    identity: AttemptSourceIdentity,
    opened: O,
}

type AttemptSourceOpenOutcome<O> =
    Result<Result<OpenedAttemptSource<O>, DistributedQueryError>, ConnectorBlockingIoError>;

trait SerialAttemptInitialization: Send + Sized + 'static {
    type Ready: Send + 'static;
    type Recipe: Send + 'static;
    type Opened: Send + 'static;
    type Opening: Send + 'static;

    fn execution_id(&self) -> QueryExecutionId;

    fn next_source_recipe(
        &mut self,
    ) -> Result<Option<AttemptSourceRecipe<Self::Recipe>>, DistributedQueryError>;

    fn begin_open(
        recipe: AttemptSourceRecipe<Self::Recipe>,
        supervisor: &ConnectorBlockingIoSupervisor,
    ) -> Self::Opening;
    fn poll_open_in(
        opening: &mut Self::Opening,
        cx: &mut std::task::Context<'_>,
        outcome: &mut Option<AttemptSourceOpenOutcome<Self::Opened>>,
    ) -> std::task::Poll<()>;

    /// Raw health is separate from a business outcome and never proves exit.
    fn opening_health(_: &Self::Opening) -> Option<StatementNativeHealthFailure> {
        None
    }

    fn accept_opened(&mut self, opened: Self::Opened) -> Result<(), DistributedQueryError>;

    fn finish_in(&mut self, ready: &mut Option<Self::Ready>) -> Result<(), DistributedQueryError>;

    fn close_in(&mut self, job: &mut Option<ConnectorBlockingIoJob<()>>);
}

#[derive(Clone)]
struct AttemptInitializationLifecycle {
    deadline: Instant,
    cancellation: novarocks_workload_control::CancellationView,
}

impl AttemptInitializationLifecycle {
    fn governed(
        deadline: Instant,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }

    fn check(&self) -> Result<(), DistributedQueryError> {
        if self.cancellation.reason().is_some() {
            return Err(failed("query cancelled while initializing its attempt"));
        }
        if Instant::now() >= self.deadline {
            return Err(failed(
                "query deadline elapsed while initializing its attempt",
            ));
        }
        Ok(())
    }
}

/// Original serial initialization frame. Every await borrows these same fields;
/// cancellation never transfers the only job, opened result, or close receipt
/// into the transient polling future.
struct InitializationProgress<S: SerialAttemptInitialization> {
    state: S,
    supervisor: ConnectorBlockingIoSupervisor,
    lifecycle: AttemptInitializationLifecycle,
    expected: Option<AttemptSourceIdentity>,
    opening: Option<S::Opening>,
    outcome: Option<AttemptSourceOpenOutcome<S::Opened>>,
    close_job: Option<ConnectorBlockingIoJob<()>>,
    close_outcome: Option<Result<(), ConnectorBlockingIoError>>,
    failure: Option<DistributedQueryError>,
    source_failure: Option<DistributedQueryError>,
    ready: Option<S::Ready>,
    closing: bool,
}

impl<S: SerialAttemptInitialization> InitializationProgress<S> {
    fn new(
        supervisor: ConnectorBlockingIoSupervisor,
        lifecycle: AttemptInitializationLifecycle,
        state: S,
    ) -> Self {
        Self {
            state,
            supervisor,
            lifecycle,
            expected: None,
            opening: None,
            outcome: None,
            close_job: None,
            close_outcome: None,
            failure: None,
            source_failure: None,
            ready: None,
            closing: false,
        }
    }

    async fn initialize(&mut self) -> Result<(), DistributedQueryError> {
        if self.closing {
            return Err(failed("attempt initialization already entered close"));
        }
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.advance().await;
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    async fn advance(&mut self) -> Result<(), DistributedQueryError> {
        loop {
            self.lifecycle.check()?;
            if self.ready.is_some() {
                return Ok(());
            }
            if self.opening.is_none() {
                let Some(recipe) = self.state.next_source_recipe()? else {
                    return self.state.finish_in(&mut self.ready);
                };
                self.expected = Some(recipe.identity.clone());
                if recipe.identity.execution_id != self.state.execution_id() {
                    return Err(failed(
                        "attempt initializer produced a source recipe for another execution",
                    ));
                }
                self.opening = Some(S::begin_open(recipe, &self.supervisor));
            }
            if self.outcome.is_none() {
                tokio::select! {
                    () = std::future::poll_fn(|cx|S::poll_open_in(self.opening.as_mut().unwrap(),cx,&mut self.outcome)) => {}
                    _ = self.lifecycle.cancellation.cancelled() => {
                        return Err(failed("query cancelled while opening an attempt split source"));
                    }
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(self.lifecycle.deadline)) => {
                        return Err(failed("query deadline elapsed while opening an attempt split source"));
                    }
                }
            }
            if S::opening_health(self.opening.as_ref().unwrap()).is_some() {
                return Err(failed("original source preparation CPU exit is unproven"));
            }
            // Keep the actual outcome installed through all fallible checks.
            self.lifecycle.check()?;
            let opened = match self.outcome.as_ref().unwrap() {
                Ok(Ok(opened)) => opened,
                Ok(Err(error)) => return Err(error.clone()),
                Err(error) => return Err(failed(error.to_string())),
            };
            let expected = self.expected.as_ref().unwrap();
            if opened.identity != *expected {
                return Err(failed(format!(
                    "attempt initializer received a source from another identity: expected={expected:?}, actual={:?}",
                    opened.identity
                )));
            }
            let Some(Ok(Ok(opened))) = self.outcome.take() else {
                unreachable!()
            };
            // Production acceptance is a pure transfer into the original
            // pre-reserved source Vec. It invokes no provider callback.
            self.state.accept_opened(opened.opened)?;
            self.opening = None;
            self.expected = None;
        }
    }

    /// Observe actual original worker joins and source close. A cleanup panic
    /// stays in its exact outcome and prevents a false successful convergence.
    async fn close(&mut self) -> Result<(), StatementNativeHealthFailure> {
        self.closing = true;
        // Close already accepted sources before waiting for a late provider.
        self.close_accepted().await?;
        if let Some(opening) = &mut self.opening
            && self.outcome.is_none()
        {
            std::future::poll_fn(|cx| {
                let polled = S::poll_open_in(opening, cx, &mut self.outcome);
                if let Some(health) = S::opening_health(opening) {
                    return std::task::Poll::Ready(Err(health));
                }
                polled.map(Ok)
            })
            .await?;
        }
        if let Some(Err(error)) = &self.outcome {
            return Err(StatementNativeHealthFailure::actual_exit_unproven(
                "attempt source-open worker failed before a proven source exit",
                error.health_cause(),
            ));
        }
        match self.outcome.take() {
            Some(Ok(Ok(opened))) => {
                // A rejected identity is never published, but still needs its
                // real source close under this same original owner.
                if let Err(error) = self.state.accept_opened(opened.opened) {
                    self.source_failure = Some(error);
                }
            }
            Some(Ok(Err(error))) => {
                self.source_failure = Some(error);
            }
            Some(Err(_)) => unreachable!("failed worker stays in its original outcome"),
            None => {}
        }
        self.opening = None;
        self.expected = None;
        self.close_accepted().await
    }

    async fn close_accepted(&mut self) -> Result<(), StatementNativeHealthFailure> {
        if self.close_job.is_none() {
            self.state.close_in(&mut self.close_job);
        }
        if let Some(job) = &mut self.close_job {
            if self.close_outcome.is_none() {
                job.finish_in(&mut self.close_outcome).await;
            }
            check_source_close(job, self.close_outcome.as_ref().unwrap())?;
            self.close_job = None;
            self.close_outcome = None;
        }
        Ok(())
    }
}

pub(super) fn check_source_close(
    job: &ConnectorBlockingIoJob<()>,
    outcome: &Result<(), ConnectorBlockingIoError>,
) -> Result<(), StatementNativeHealthFailure> {
    if let Some(error) = job.source_close_failure() {
        // Primary typed error was installed in the original job before any
        // later source Drop/callback. A secondary JoinError stays in outcome.
        return Err(StatementNativeHealthFailure::actual_exit_unproven(
            "attempt source-close returned an original provider failure",
            error,
        ));
    }
    match outcome {
        Ok(()) => Ok(()),
        Err(error) => Err(StatementNativeHealthFailure::actual_exit_unproven(
            "attempt source-close worker failed before a proven exit",
            error.health_cause(),
        )),
    }
}

struct ProductionInitializationState {
    work_scope: WorkScope,
    result_window: ResultWindowAlias,
    source_cohort:
        crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    execution_id: QueryExecutionId,
    artifacts: Option<PreparedDistributedQuery>,
    schedule: Option<ValidatedFragmentSchedule>,
    scan_keys: Vec<(FragmentId, i32)>,
    next_scan: usize,
    session: Option<novarocks_spi::connector::read_stack::ConnectorSession>,
    connector_context: ConnectorRequestContext,
    metadata_preparation:
        Option<Weak<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>>,
    cancellation: novarocks_workload_control::CancellationView,
    runtime: tokio::runtime::Handle,
    feedback: Arc<RuntimeFilterFeedbackState>,
    sources: OpenRoundSplitSources,
    retry_policy: TaskUpdateRetryPolicy,
    initial_dynamic_filter_wait_cap: Duration,
    blocking_io: ConnectorBlockingIoSupervisor,
    credential_lease_source: RoundCredentialLeaseSource,
}

struct ProductionSourceRecipe {
    admission: ConnectorBlockingIoAdmission,
    source: RoundSplitSourceRecipe,
    session: novarocks_spi::connector::read_stack::ConnectorSession,
    connector_context: ConnectorRequestContext,
    metadata_preparation:
        Option<Weak<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>>,
    cancellation: novarocks_workload_control::CancellationView,
    runtime: tokio::runtime::Handle,
    blocking_io: ConnectorBlockingIoSupervisor,
}

#[derive(Debug, Default)]
struct SourcePreparationWake(futures::task::AtomicWaker);
impl crate::task_execution::status_intake::StatusIntakeWake for SourcePreparationWake {
    fn wake(&self) {
        self.0.wake();
    }
}

struct ProductionSourceOpening {
    recipe: Option<AttemptSourceRecipe<ProductionSourceRecipe>>,
    responsibility: Option<ConnectorBlockingIoResponsibility>,
    access_future: Option<novarocks_spi::connector::ConnectorReadAttemptCapabilitiesFuture>,
    access_result:
        Option<Result<novarocks_spi::connector::ConnectorReadAttemptCapabilities, ConnectorError>>,
    capabilities: Option<novarocks_spi::connector::ConnectorReadAttemptCapabilities>,
    preparation: Option<novarocks_spi::connector::read_stack::ConnectorReadSplitPreparation>,
    preparation_wake: Arc<SourcePreparationWake>,
    raw_health: Option<StatementNativeHealthFailure>,
    open_owner: Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
    open_future: Option<novarocks_spi::connector::read_stack::ConnectorReadSplitOpenFuture>,
    open_result: Option<
        Result<
            Box<dyn novarocks_spi::connector::read_stack::ConnectorReadSplitSource>,
            ConnectorError,
        >,
    >,
    poisoned: bool,
}
fn poll_original_preparation<T>(
    future: &mut Option<futures::future::BoxFuture<'static, T>>,
    destination: &mut Option<T>,
    poisoned: &mut bool,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<()> {
    use std::task::Poll;
    if destination.is_some() {
        return Poll::Ready(());
    }
    assert!(
        !*poisoned,
        "an unwound original Provider future is never repolled"
    );
    struct Polling<'a> {
        poisoned: &'a mut bool,
        done: bool,
    }
    impl Drop for Polling<'_> {
        fn drop(&mut self) {
            if !self.done {
                *self.poisoned = true;
            }
        }
    }
    let mut mark = Polling {
        poisoned,
        done: false,
    };
    match future
        .as_mut()
        .expect("same original Provider preparation F")
        .as_mut()
        .poll(cx)
    {
        Poll::Pending => {
            mark.done = true;
            Poll::Pending
        }
        Poll::Ready(result) => {
            *destination = Some(result);
            // Original raw result/error is stable BEFORE foreign Future Drop.
            drop(future.take());
            mark.done = true;
            Poll::Ready(())
        }
    }
}
impl ProductionSourceOpening {
    fn select_metadata_preparation(&mut self) -> Result<(), ConnectorError> {
        if self.preparation.is_some() {
            return Ok(());
        }
        let recipe = &self
            .recipe
            .as_ref()
            .expect("same original source recipe")
            .recipe;
        if !recipe.source.requires_metadata_split_preparation() {
            return Ok(());
        }
        let original = recipe.metadata_preparation.as_ref().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "prepared source has no original metadata preparation source",
            )
        })?;
        let io = original.upgrade().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "original metadata source exited before split preparation",
            )
        })?;
        let admission_error = |error| {
            // Keep the original Work fact and cause; this carrier does not
            // reclassify a resource/control refusal as a Provider decision.
            ConnectorError::from_domain(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                error,
            )
        };
        let capacity = io.root_capacity().map_err(admission_error)?;
        let original = io
            .original_result_scope(&recipe.connector_context)
            .map_err(|error| {
                ConnectorError::from_domain(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    error,
                )
            })?;
        let cpu = recipe
            .blocking_io
            .provider_creator_cpu()
            .map_err(admission_error)?;
        let cpu = cpu
            .planning_metadata_cpu_port(
                capacity.scope(),
                capacity.try_window_alias().map_err(admission_error)?,
                recipe.source.planning_lease(),
                novarocks_query_application::cancellation::QueryCancellationView::governed(
                    recipe.cancellation.clone(),
                    None,
                ),
                recipe.runtime.clone(),
                self.preparation_wake.clone(),
            )
            .map_err(admission_error)?;
        self.preparation = Some(
            novarocks_spi::connector::read_stack::ConnectorReadSplitPreparation::new(cpu, original),
        );
        Ok(())
    }

    fn poll_metadata_closed(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        use std::task::Poll;
        if self.raw_health.is_some() {
            return Poll::Ready(());
        }
        let Some(preparation) = &self.preparation else {
            return Poll::Ready(());
        };
        self.poisoned = true;
        let closed = preparation.cpu().poll_closed(cx);
        let observed = match closed {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(error)) => {
                let cause = preparation
                    .cpu()
                    .local_failure()
                    .unwrap_or_else(|| Arc::new(error));
                self.raw_health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "original split preparation CPU exit is unproven",
                    cause,
                ));
                Poll::Ready(())
            }
        };
        self.poisoned = false;
        observed
    }

    fn poll_in(
        &mut self,
        cx: &mut std::task::Context<'_>,
        outcome: &mut Option<AttemptSourceOpenOutcome<OpenedRoundSplitSource>>,
    ) -> std::task::Poll<()> {
        use std::task::Poll;
        if outcome.is_some() || self.raw_health.is_some() {
            return Poll::Ready(());
        }
        assert!(
            !self.poisoned,
            "poisoned original source opening retains its actual state"
        );
        self.preparation_wake.0.register(cx.waker());
        if self.responsibility.is_none() {
            match self
                .recipe
                .as_mut()
                .unwrap()
                .recipe
                .admission
                .poll_ready(cx)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    *outcome = Some(Ok(Err(failed(error.to_string()))));
                    return Poll::Ready(());
                }
                Poll::Ready(Ok(owner)) => self.responsibility = Some(owner),
            }
        }
        if self.open_result.is_none()
            && let Err(error) = self.select_metadata_preparation()
        {
            self.open_result = Some(Err(error));
        }
        if self.capabilities.is_none() && self.open_result.is_none() {
            if self.access_future.is_none() && self.access_result.is_none() {
                let recipe = &self.recipe.as_ref().unwrap().recipe;
                self.poisoned = true;
                let access = recipe.source.begin_access(&recipe.connector_context);
                self.poisoned = false;
                match access {
                    Ok(future) => self.access_future = Some(future),
                    Err(error) => self.access_result = Some(Err(error)),
                }
            }
            std::task::ready!(poll_original_preparation(
                &mut self.access_future,
                &mut self.access_result,
                &mut self.poisoned,
                cx,
            ));
            match self.access_result.take().unwrap() {
                Ok(capabilities) => self.capabilities = Some(capabilities),
                Err(error) => self.open_result = Some(Err(error)),
            }
        }
        if self.open_future.is_none() && self.open_result.is_none() {
            let recipe = &self.recipe.as_ref().unwrap().recipe;
            self.poisoned = true;
            let future = recipe.source.begin_split_open(
                &recipe.session,
                self.capabilities.as_ref().unwrap(),
                self.preparation.clone(),
                &mut self.open_owner,
            );
            self.open_future = Some(future);
            self.poisoned = false;
        }
        if self.open_result.is_none() {
            std::task::ready!(poll_original_preparation(
                &mut self.open_future,
                &mut self.open_result,
                &mut self.poisoned,
                cx,
            ));
        }
        // Raw business result is stable before F Drop; its original Provider
        // body and packet remain installed through actual worker/forwarder exit.
        std::task::ready!(self.poll_metadata_closed(cx));
        if self.raw_health.is_some() {
            return Poll::Ready(());
        }
        let raw = match self.open_result.take().unwrap() {
            Ok(raw) => raw,
            Err(error) => {
                let node = self.recipe.as_ref().unwrap().recipe.source.plan_node_id();
                let error = error.map_message(|message| format!(
                    "typed connector scan node_id={node} cannot open its split source: {message}"
                ));
                *outcome = Some(Ok(Err(failed_connector(error))));
                return Poll::Ready(());
            }
        };
        let AttemptSourceRecipe { identity, recipe } = self.recipe.take().unwrap();
        let opened = recipe.source.finish_open(
            raw,
            self.capabilities.take().unwrap(),
            self.responsibility.take().unwrap(),
        );
        *outcome = Some(Ok(Ok(OpenedAttemptSource { identity, opened })));
        Poll::Ready(())
    }
}

impl SerialAttemptInitialization for ProductionInitializationState {
    type Ready = AttemptReady;
    type Recipe = ProductionSourceRecipe;
    type Opened = OpenedRoundSplitSource;
    type Opening = ProductionSourceOpening;

    fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    fn next_source_recipe(
        &mut self,
    ) -> Result<Option<AttemptSourceRecipe<Self::Recipe>>, DistributedQueryError> {
        if self.session.is_none() {
            self.session =
                Some(crate::query_execution::compiler::typed_connector_session().map_err(failed)?);
        }
        let Some(&(fragment_id, plan_node_id)) = self.scan_keys.get(self.next_scan) else {
            return Ok(None);
        };
        let source = RoundSplitSourceRecipe::from_artifacts(
            self.artifacts.as_ref().unwrap(),
            fragment_id,
            plan_node_id,
        )
        .map_err(failed)?;
        let identity = AttemptSourceIdentity {
            execution_id: self.execution_id,
            fragment_id: source.fragment_id(),
            plan_node_id: source.plan_node_id(),
            generation: source.generation().clone(),
        };
        Ok(Some(AttemptSourceRecipe {
            identity,
            recipe: ProductionSourceRecipe {
                admission: ConnectorBlockingIoResponsibility::request_from_cohort(
                    &self.work_scope,
                    &self.result_window,
                    self.source_cohort.clone(),
                )
                .map_err(|error| failed(error.to_string()))?,
                source,
                session: self.session.as_ref().unwrap().clone(),
                // Every source belongs to this same admitted attempt. Preserve
                // its request scope so provider-private capability caches can
                // coalesce repeated frozen-table access without crossing an
                // attempt boundary.
                connector_context: self.connector_context.clone(),
                metadata_preparation: self.metadata_preparation.clone(),
                cancellation: self.cancellation.clone(),
                runtime: self.runtime.clone(),
                blocking_io: self.blocking_io.clone(),
            },
        }))
    }

    fn begin_open(
        recipe: AttemptSourceRecipe<Self::Recipe>,
        _: &ConnectorBlockingIoSupervisor,
    ) -> Self::Opening {
        ProductionSourceOpening {
            recipe: Some(recipe),
            responsibility: None,
            access_future: None,
            access_result: None,
            capabilities: None,
            preparation: None,
            preparation_wake: Arc::new(SourcePreparationWake::default()),
            raw_health: None,
            open_owner: None,
            open_future: None,
            open_result: None,
            poisoned: false,
        }
    }
    fn poll_open_in(
        opening: &mut Self::Opening,
        cx: &mut std::task::Context<'_>,
        outcome: &mut Option<
            Result<
                Result<OpenedAttemptSource<Self::Opened>, DistributedQueryError>,
                ConnectorBlockingIoError,
            >,
        >,
    ) -> std::task::Poll<()> {
        opening.poll_in(cx, outcome)
    }

    fn opening_health(opening: &Self::Opening) -> Option<StatementNativeHealthFailure> {
        opening.raw_health.clone()
    }

    fn accept_opened(&mut self, opened: Self::Opened) -> Result<(), DistributedQueryError> {
        let source = opened.into_round_source(Arc::clone(&self.feedback));
        self.sources.push(source);
        self.next_scan += 1;
        Ok(())
    }

    fn finish_in(&mut self, ready: &mut Option<Self::Ready>) -> Result<(), DistributedQueryError> {
        if self.next_scan != self.scan_keys.len() {
            return Err(failed(
                "attempt initializer cannot become ready with unopened split sources",
            ));
        }
        let targets = assignment_targets(self.schedule.as_ref().unwrap(), &self.scan_keys);
        for scan in self.sources.scans() {
            if targets.get(&scan).is_none_or(|targets| targets.is_empty()) {
                return Err(failed(format!(
                    "typed connector scan at {scan} has no admitted task in this schedule"
                )));
            }
        }
        // No transport gate here any more. It refused to send material over a
        // readable transport; nothing sends material at all now, and what this
        // announces is a scope and an acquisition path (CAD-1 D1, C07b).
        let credential_leases = self.credential_lease_source.drain_credential_leases()?;
        // Keep the sources under their RAII owner through every fallible
        // readiness check. Once moved into the round plan, normal round
        // teardown becomes their sole close owner.
        let sources = self.sources.take_sources();
        let split_assignment_plan = (!self.scan_keys.is_empty()).then(|| {
            RoundSplitAssignmentPlan::new(
                targets,
                sources,
                self.retry_policy,
                self.initial_dynamic_filter_wait_cap,
                self.blocking_io.clone(),
            )
        });
        *ready = Some(AttemptReady {
            execution_id: self.execution_id,
            artifacts: Some(self.artifacts.take().unwrap()),
            schedule: Some(self.schedule.take().unwrap()),
            feedback: Arc::clone(&self.feedback),
            split_assignment_plan,
            credential_leases: Some(credential_leases),
            close_job: None,
            close_outcome: None,
        });
        Ok(())
    }

    fn close_in(&mut self, job: &mut Option<ConnectorBlockingIoJob<()>>) {
        self.sources.close_in(job);
    }
}

/// The only pre-round state. It owns source recipes and attempt credentials,
/// but exposes neither until every source has opened under this attempt.
pub(crate) struct AttemptInitializing {
    progress: InitializationProgress<ProductionInitializationState>,
}

impl AttemptInitializing {
    #[expect(
        clippy::too_many_arguments,
        reason = "Each input is an independently frozen fact needed before an attempt may become ready."
    )]
    fn from_lifecycle(
        execution_id: QueryExecutionId,
        work_scope: WorkScope,
        result_window: ResultWindowAlias,
        artifacts: PreparedDistributedQuery,
        schedule: ValidatedFragmentSchedule,
        retry_policy: TaskUpdateRetryPolicy,
        feedback: Arc<RuntimeFilterFeedbackState>,
        initial_dynamic_filter_wait_cap: Duration,
        connector_context: ConnectorRequestContext,
        runtime: FrontendDataRuntime,
        metadata_preparation: Option<
            Weak<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
        >,
        credential_lease_source: &mut Option<RoundCredentialLeaseSource>,
        lifecycle: AttemptInitializationLifecycle,
        source_cohort: crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    ) -> Result<Self, DistributedQueryError> {
        if schedule.execution_id() != execution_id {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "attempt initializer received a schedule for another execution",
            ));
        }
        let scan_keys = artifacts.typed_scans().collect::<Vec<_>>();
        let blocking_io = runtime.connector_blocking_io().clone();
        let cancellation = lifecycle.cancellation.clone();
        Ok(Self {
            progress: InitializationProgress::new(
                blocking_io.clone(),
                lifecycle,
                ProductionInitializationState {
                    work_scope,
                    result_window,
                    source_cohort,
                    execution_id,
                    artifacts: Some(artifacts),
                    schedule: Some(schedule),
                    sources: OpenRoundSplitSources::with_capacity(
                        scan_keys.len(),
                        blocking_io.clone(),
                    ),
                    scan_keys,
                    next_scan: 0,
                    session: None,
                    connector_context,
                    metadata_preparation,
                    cancellation,
                    runtime: runtime.handle().clone(),
                    feedback,
                    retry_policy,
                    initial_dynamic_filter_wait_cap,
                    blocking_io,
                    credential_lease_source: credential_lease_source
                        .take()
                        .expect("original credential source stays installed until pure capture"),
                },
            ),
        })
    }

    /// Build the same attempt-local source owner for the query application's
    /// governed cancellation lifetime. The schedule supplied here is already
    /// a one-way projection of the exact Task manifest; this constructor does
    /// not read topology or invoke placement policy.
    #[expect(
        clippy::too_many_arguments,
        reason = "Each input is an independently frozen fact needed before an attempt may become ready."
    )]
    pub(crate) fn new_governed_in(
        execution_id: QueryExecutionId,
        work_scope: WorkScope,
        result_window: ResultWindowAlias,
        source_cohort: crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
        artifacts: PreparedDistributedQuery,
        schedule: ValidatedFragmentSchedule,
        retry_policy: TaskUpdateRetryPolicy,
        feedback: Arc<RuntimeFilterFeedbackState>,
        initial_dynamic_filter_wait_cap: Duration,
        connector_context: ConnectorRequestContext,
        cancellation: novarocks_workload_control::CancellationView,
        runtime: FrontendDataRuntime,
        metadata_preparation: Option<
            Weak<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
        >,
        credential_lease_source: &mut Option<RoundCredentialLeaseSource>,
        destination: &mut Option<Self>,
    ) -> Result<(), DistributedQueryError> {
        assert!(
            destination.is_none(),
            "initializer has one stable destination"
        );
        let lifecycle =
            AttemptInitializationLifecycle::governed(connector_context.deadline(), cancellation);
        *destination = Some(Self::from_lifecycle(
            execution_id,
            work_scope,
            result_window,
            artifacts,
            schedule,
            retry_policy,
            feedback,
            initial_dynamic_filter_wait_cap,
            connector_context,
            runtime,
            metadata_preparation,
            credential_lease_source,
            lifecycle,
            source_cohort,
        )?);
        Ok(())
    }

    pub(crate) async fn initialize(&mut self) -> Result<(), DistributedQueryError> {
        self.progress.initialize().await
    }

    /// Pure final transfer after the borrowed initializer has installed Ready.
    pub(crate) fn take_ready_in(&mut self, ready: &mut Option<AttemptReady>) {
        assert!(ready.is_none(), "Ready has one original destination");
        *ready = self.progress.ready.take();
        assert!(
            ready.is_some(),
            "initialization installed Ready before transfer"
        );
    }

    pub(crate) async fn close(&mut self) -> Result<(), StatementNativeHealthFailure> {
        if let Some(ready) = self.progress.ready.as_mut() {
            ready.close().await?;
        }
        self.progress.close().await
    }
}

/// Move-only proof that source-open, exact generation checks, and credential
/// sealing all completed before task-round construction.
pub(crate) struct AttemptReady {
    execution_id: QueryExecutionId,
    artifacts: Option<PreparedDistributedQuery>,
    schedule: Option<ValidatedFragmentSchedule>,
    feedback: Arc<RuntimeFilterFeedbackState>,
    split_assignment_plan: Option<RoundSplitAssignmentPlan>,
    credential_leases: Option<QueryCredentialLeases>,
    close_job: Option<ConnectorBlockingIoJob<()>>,
    close_outcome: Option<Result<(), ConnectorBlockingIoError>>,
}

impl AttemptReady {
    pub(crate) fn take_preparation_parts(
        &mut self,
    ) -> (
        QueryExecutionId,
        PreparedDistributedQuery,
        ValidatedFragmentSchedule,
        Arc<RuntimeFilterFeedbackState>,
        QueryCredentialLeases,
    ) {
        (
            self.execution_id,
            self.artifacts.take().unwrap(),
            self.schedule.take().unwrap(),
            Arc::clone(&self.feedback),
            self.credential_leases.take().unwrap(),
        )
    }

    pub(crate) fn take_split_plan(&mut self) -> Option<RoundSplitAssignmentPlan> {
        self.split_assignment_plan.take()
    }

    pub(crate) async fn close(&mut self) -> Result<(), StatementNativeHealthFailure> {
        if self.close_job.is_none()
            && let Some(plan) = self.split_assignment_plan.as_mut()
        {
            plan.close_in(&mut self.close_job);
        }
        if let Some(job) = &mut self.close_job {
            if self.close_outcome.is_none() {
                job.finish_in(&mut self.close_outcome).await;
            }
            check_source_close(job, self.close_outcome.as_ref().unwrap())?;
            self.close_job = None;
            self.close_outcome = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    mod prepared_opening_tests;
    use std::future::Future;
    use std::sync::mpsc;

    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId, CredentialConsumerRole, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StorageCredentialScopePrefix,
        VendedS3CredentialLeaseContribution, VendedS3CredentialLeaseEntry,
    };
    use novarocks_types::{AttemptId, QueryId};

    use super::*;
    use novarocks_workload_control::{
        CancellationReason, RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    #[test]
    fn split_open_unavailable_retains_original_source_and_only_renders_owner_safe_detail() {
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        use novarocks_spi::connector::ConnectorErrorKind;
        use std::error::Error;

        let source = ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "cannot read Variant column statistics; static Variant hint; private fixture locator",
        )
        .with_retryable_before_progress()
        .with_cleanup_context("private cleanup locator");
        let fact = source.failure_fact();
        let original_message = source.message().as_ptr();
        let original_display = source.to_string();
        let error = failed_connector(source);
        let shared = error
            .source()
            .expect("original connector source")
            .downcast_ref::<SharedFailureCause>()
            .expect("shared source carrier");
        let carried = shared
            .downcast_ref::<ConnectorError>()
            .expect("original connector error");
        assert_eq!(carried.message().as_ptr(), original_message);
        assert_eq!(carried.to_string(), original_display);
        assert!(carried.retryable_before_progress());
        assert_eq!(carried.kind(), ConnectorErrorKind::Unavailable);
        assert_eq!(shared.failure_fact(), fact);
        assert_eq!(fact.class(), FailureClass::Dependency);
        assert_eq!(fact.reason().domain(), 4);
        assert_eq!(fact.reason().code(), 8);
        assert_eq!(error.kind(), DistributedQueryErrorKind::Failed);
        assert_eq!(error.compile_control_error(), None);
        let input = error.into_execution_failure_input();
        assert_eq!(input.fact(), fact);
        assert_eq!(
            input.detail().as_str(),
            "native attempt initialization failed in its owning domain (reason 4:8)"
        );
        let public = input.user_error();
        assert_eq!(public.code().as_str(), "query.execution.dependency");
        assert_eq!(public.message(), input.detail().as_str());
        assert!(!public.to_string().contains("Variant"));
        assert!(!public.to_string().contains("private"));
    }

    #[test]
    fn split_open_source_keeps_domain_control_fact_and_original_shared_identity() {
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        use novarocks_spi::connector::ConnectorErrorKind;
        use novarocks_type_contract::CompileControlError;
        use std::error::Error;

        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let original = SharedFailureCause::new(control);
            let fact = original.failure_fact();
            let connector =
                ConnectorError::from_domain(ConnectorErrorKind::Unavailable, original.clone());
            let error = failed_connector(connector);
            let shared = error
                .source()
                .expect("connector source")
                .downcast_ref::<SharedFailureCause>()
                .expect("shared connector source");
            let carried = shared
                .downcast_ref::<ConnectorError>()
                .expect("original connector error");
            let domain = carried
                .source()
                .expect("domain source")
                .downcast_ref::<SharedFailureCause>()
                .expect("connector domain carrier");
            assert_eq!(domain.downcast_ref::<SharedFailureCause>(), Some(&original));
            assert_eq!(shared.failure_fact(), fact);
            assert_ne!(fact.class(), FailureClass::Dependency);
            assert_eq!(error.compile_control_error(), None);
            assert_eq!(error.into_execution_failure_input().fact(), fact);
        }
    }

    fn execution_id(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(17, 23),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("valid execution identity")
    }

    fn binding(version: u8) -> ConnectorReadBinding {
        let instance = ConnectorInstanceId::try_from_canonical("attempt-initializer")
            .expect("canonical instance");
        ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("initializer-test").expect("provider id"),
                instance_id: instance.clone(),
            },
            CatalogHandle::new(instance, CatalogVersion::from_bytes([version; 32])),
        )
    }

    fn identity(execution_id: QueryExecutionId, node_id: i32) -> AttemptSourceIdentity {
        AttemptSourceIdentity {
            execution_id,
            fragment_id: node_id as u32,
            plan_node_id: node_id,
            generation: binding(node_id as u8),
        }
    }

    fn supervisor() -> (
        crate::task_execution::blocking_io::tests::OriginalExecutorsFixture,
        ConnectorBlockingIoSupervisor,
    ) {
        crate::task_execution::blocking_io::tests::original_executors(
            tokio::runtime::Handle::current(),
        )
    }

    struct ProtectedCleanup {
        supervisor: ConnectorBlockingIoSupervisor,
        closed: Option<mpsc::Sender<()>>,
    }

    struct FixtureInitializationState {
        execution_id: QueryExecutionId,
        identities: Vec<AttemptSourceIdentity>,
        recipes: Vec<Option<FixtureSourceRecipe>>,
        next: usize,
        accepted: Vec<ProtectedCleanup>,
    }

    struct FixtureSourceRecipe {
        reported_identity: Option<AttemptSourceIdentity>,
        started: Option<mpsc::Sender<()>>,
        release: Option<mpsc::Receiver<()>>,
        connector_context: Option<ConnectorRequestContext>,
        cleanup: ProtectedCleanup,
    }

    impl FixtureInitializationState {
        fn open_fixture_source(
            input: &mut Option<AttemptSourceRecipe<FixtureSourceRecipe>>,
            _: &crate::task_execution::blocking_io::ConnectorSourceCloseResult<'_>,
        ) -> Option<Result<OpenedAttemptSource<ProtectedCleanup>, DistributedQueryError>> {
            let AttemptSourceRecipe {
                identity,
                mut recipe,
            } = input.take().expect("one original fixture source recipe");
            if let Some(started) = recipe.started.take() {
                started.send(()).expect("publish source-open start");
            }
            if let Some(release) = recipe.release.take() {
                release.recv().expect("release source-open call");
            }
            drop(recipe.connector_context.take());
            let identity = recipe.reported_identity.take().unwrap_or(identity);
            Some(Ok(OpenedAttemptSource {
                identity,
                opened: recipe.cleanup,
            }))
        }

        fn close_fixture_sources(
            accepted: &mut Vec<ProtectedCleanup>,
            _: &crate::task_execution::blocking_io::ConnectorSourceCloseResult<'_>,
        ) -> Option<()> {
            for mut cleanup in accepted.drain(..) {
                if let Some(closed) = cleanup.closed.take() {
                    closed
                        .send(())
                        .expect("publish actual original source close");
                }
            }
            Some(())
        }
    }

    impl SerialAttemptInitialization for FixtureInitializationState {
        type Ready = usize;
        type Recipe = FixtureSourceRecipe;
        type Opened = ProtectedCleanup;
        type Opening = ConnectorBlockingIoJob<
            Result<OpenedAttemptSource<ProtectedCleanup>, DistributedQueryError>,
        >;

        fn execution_id(&self) -> QueryExecutionId {
            self.execution_id
        }

        fn next_source_recipe(
            &mut self,
        ) -> Result<Option<AttemptSourceRecipe<Self::Recipe>>, DistributedQueryError> {
            let Some(identity) = self.identities.get(self.next).cloned() else {
                return Ok(None);
            };
            let recipe = self.recipes[self.next]
                .take()
                .expect("fixture recipe is consumed once");
            Ok(Some(AttemptSourceRecipe { identity, recipe }))
        }

        fn begin_open(
            recipe: AttemptSourceRecipe<Self::Recipe>,
            supervisor: &ConnectorBlockingIoSupervisor,
        ) -> Self::Opening {
            supervisor.prepare_cpu_owned(Vec::new(), Some(recipe), Self::open_fixture_source)
        }
        fn poll_open_in(
            opening: &mut Self::Opening,
            cx: &mut std::task::Context<'_>,
            outcome: &mut Option<
                Result<
                    Result<OpenedAttemptSource<Self::Opened>, DistributedQueryError>,
                    ConnectorBlockingIoError,
                >,
            >,
        ) -> std::task::Poll<()> {
            match opening.poll_progress(cx) {
                std::task::Poll::Pending => std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => {
                    *outcome = Some(Err(error));
                    std::task::Poll::Ready(())
                }
                std::task::Poll::Ready(Ok(())) => {
                    *outcome = opening.try_take();
                    std::task::Poll::Ready(())
                }
            }
        }

        fn accept_opened(&mut self, opened: Self::Opened) -> Result<(), DistributedQueryError> {
            self.accepted.push(opened);
            self.next += 1;
            Ok(())
        }

        fn finish_in(
            &mut self,
            ready: &mut Option<Self::Ready>,
        ) -> Result<(), DistributedQueryError> {
            *ready = Some(self.next);
            Ok(())
        }

        fn close_in(&mut self, job: &mut Option<ConnectorBlockingIoJob<()>>) {
            assert!(job.is_none());
            if self.accepted.is_empty() {
                return;
            }
            let supervisor = self.accepted[0].supervisor.clone();
            let accepted = std::mem::take(&mut self.accepted);
            *job = Some(supervisor.prepare_cpu_retirement(
                Vec::new(),
                accepted,
                Self::close_fixture_sources,
            ));
        }
    }

    fn fixture(
        execution_id: QueryExecutionId,
        identities: Vec<AttemptSourceIdentity>,
        supervisor: ConnectorBlockingIoSupervisor,
        closed: mpsc::Sender<()>,
    ) -> FixtureInitializationState {
        let recipes = identities
            .iter()
            .map(|_| {
                Some(FixtureSourceRecipe {
                    reported_identity: None,
                    started: None,
                    release: None,
                    connector_context: None,
                    cleanup: ProtectedCleanup {
                        supervisor: supervisor.clone(),
                        closed: Some(closed.clone()),
                    },
                })
            })
            .collect();
        FixtureInitializationState {
            execution_id,
            identities,
            recipes,
            next: 0,
            accepted: Vec::new(),
        }
    }

    struct InitializationCancellationFixture {
        control: WorkloadControl,
        root: RootWork,
    }
    impl InitializationCancellationFixture {
        fn new() -> Self {
            let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
                .unwrap()
                .owner;
            control.mark_ready().unwrap();
            let root = control
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .unwrap();
            Self { control, root }
        }
        fn view(&self) -> novarocks_workload_control::CancellationView {
            self.root.owner.scope().cancellation().unwrap()
        }
        fn request(&self, reason: CancellationReason) {
            self.root
                .owner
                .cancellation_requester()
                .request(reason.clone())
                .unwrap();
            assert_eq!(self.view().reason(), Some(reason));
        }
        fn complete(self) {
            if self.view().reason().is_some() {
                let notice = self
                    .control
                    .next_control()
                    .expect("the original Cancel notification");
                assert_eq!(notice.scope().id(), self.root.owner.scope().id());
                assert!(
                    notice
                        .intents()
                        .contains(novarocks_workload_control::ControlIntent::Cancel)
                );
                notice.acknowledge();
            }
            assert!(self.control.next_control().is_none());
            self.root.owner.complete();
            self.root.business.release();
            self.control.close_admission();
            self.control.shutdown().unwrap_or_else(|failure| {
                panic!("initializer governance did not drain: {}", failure.error())
            });
        }
    }
    fn lifecycle(source: &InitializationCancellationFixture) -> AttemptInitializationLifecycle {
        AttemptInitializationLifecycle::governed(
            Instant::now() + Duration::from_secs(5),
            source.view(),
        )
    }

    struct ScopeDropCanary {
        dropped: Option<mpsc::Sender<()>>,
    }

    impl Drop for ScopeDropCanary {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                // An unwinding test drops its observer before this canary is
                // released, and a panic raised from a destructor during
                // unwinding is non-unwinding: it aborts the test binary
                // instead of reporting the assertion that actually failed.
                assert!(
                    dropped.send(()).is_ok() || std::thread::panicking(),
                    "publish request-scope drop"
                );
            }
        }
    }

    fn connector_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("valid test Connector context")
    }

    fn vended_catalog_properties() -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("catalog.vended").expect("catalog name"),
                CatalogVersion::from_bytes([0x24; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Vended,
                )
                .expect("vended binding"),
            ],
        )
        .expect("catalog properties")
    }

    fn vended_contribution() -> VendedS3CredentialLeaseContribution {
        VendedS3CredentialLeaseContribution::try_new(
            vec![
                VendedS3CredentialLeaseEntry::try_new(
                    StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/data")
                        .expect("prefix"),
                    u64::MAX,
                    SecretValue::new("access"),
                    SecretValue::new("secret"),
                    SecretValue::new("token"),
                )
                .expect("entry"),
            ],
            None,
        )
        .expect("contribution")
    }

    #[test]
    fn dropped_attempt_source_publishes_credentials_observed_before_failure() {
        let reservation =
            QueryAttemptReservation::retry(QueryId::new(17, 23), 2).expect("attempt reservation");
        let terminal = reservation.terminal_storage_resolver();
        let sink = reservation.credential_lease_sink();
        sink.offer_vended_s3_credential_lease(&vended_catalog_properties(), vended_contribution())
            .expect("credential contribution");
        let observed = Arc::new(AtomicBool::new(false));
        let source = RoundCredentialLeaseSource::Reservation {
            reservation: Some(reservation),
            observed_collected: Some(Arc::clone(&observed)),
        };

        drop(source);

        assert!(
            observed.load(Ordering::Acquire),
            "dropping a failed attempt source must publish credentials already collected"
        );
        assert!(
            sink.offer_vended_s3_credential_lease(
                &vended_catalog_properties(),
                vended_contribution(),
            )
            .is_err(),
            "terminal custody cannot keep the abandoned collection right open"
        );
        let request = novarocks_spi::connector::StorageAccessRequest::try_new(
            vended_catalog_properties().handle().clone(),
            "s3://warehouse/data/file.parquet",
        )
        .unwrap();
        assert!(
            novarocks_spi::connector::ConnectorStorageResolver::resolve_vended_s3(
                terminal.as_ref(),
                &request,
            )
            .is_ok(),
            "the original contribution stays available for terminal work"
        );
    }

    #[test]
    fn dropped_fresh_source_closes_collection_under_retained_terminal_custody() {
        let source = RoundCredentialLeaseSource::fresh(execution_id(1));
        let RoundCredentialLeaseSource::Fresh { collector, .. } = &source else {
            unreachable!("fresh source owns the original collector");
        };
        let terminal = collector.terminal_storage_resolver();
        let sink = collector.sink();
        sink.offer_vended_s3_credential_lease(&vended_catalog_properties(), vended_contribution())
            .unwrap();
        drop(source);
        assert!(
            sink.offer_vended_s3_credential_lease(
                &vended_catalog_properties(),
                vended_contribution(),
            )
            .is_err()
        );
        let request = novarocks_spi::connector::StorageAccessRequest::try_new(
            vended_catalog_properties().handle().clone(),
            "s3://warehouse/data/file.parquet",
        )
        .unwrap();
        assert!(
            novarocks_spi::connector::ConnectorStorageResolver::resolve_vended_s3(
                terminal.as_ref(),
                &request,
            )
            .is_ok()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_scan_becomes_ready_without_submitting_connector_work() {
        let (mut executors, supervisor) = supervisor();
        let execution_id = execution_id(1);
        let cancellation = InitializationCancellationFixture::new();
        let (closed, _observe_close) = mpsc::channel();
        let state = fixture(execution_id, Vec::new(), supervisor.clone(), closed);

        let mut initializing =
            InitializationProgress::new(supervisor, lifecycle(&cancellation), state);
        initializing
            .initialize()
            .await
            .expect("scanless attempt becomes ready");
        assert_eq!(initializing.ready, Some(0));
        assert!(initializing.opening.is_none());
        executors.shutdown().await;
        cancellation.complete();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_before_submission_keeps_recipe_unopened() {
        let (mut executors, supervisor) = supervisor();
        let execution_id = execution_id(1);
        let cancellation = InitializationCancellationFixture::new();
        let (started, observe_started) = mpsc::channel();
        let (closed, _observe_close) = mpsc::channel();
        let mut state = fixture(
            execution_id,
            vec![identity(execution_id, 7)],
            supervisor.clone(),
            closed,
        );
        state.recipes[0]
            .as_mut()
            .expect("first fixture recipe")
            .started = Some(started);
        cancellation.request(CancellationReason::ClientDisconnected);
        let mut initializing =
            InitializationProgress::new(supervisor, lifecycle(&cancellation), state);
        let error = initializing
            .initialize()
            .await
            .expect_err("cancelled initializer cannot become ready");
        assert!(error.message().contains("cancelled"), "{error}");
        assert!(
            observe_started.try_recv().is_err(),
            "a cancelled recipe must remain unopened"
        );
        executors.shutdown().await;
        cancellation.complete();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_during_source_open_reaps_late_success() {
        let (mut executors, supervisor) = supervisor();
        let execution_id = execution_id(1);
        let cancellation = InitializationCancellationFixture::new();
        let (started, observe_started) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let (closed, observe_close) = mpsc::channel();
        let mut state = fixture(
            execution_id,
            vec![identity(execution_id, 7)],
            supervisor.clone(),
            closed,
        );
        let recipe = state.recipes[0].as_mut().expect("first fixture recipe");
        recipe.started = Some(started);
        recipe.release = Some(released);
        let lifecycle = lifecycle(&cancellation);
        let actor = tokio::spawn(async move {
            let mut initializing = InitializationProgress::new(supervisor, lifecycle, state);
            let result = initializing.initialize().await;
            (initializing, result)
        });
        observe_started
            .recv_timeout(Duration::from_secs(2))
            .expect("source-open call starts");
        cancellation.request(CancellationReason::ClientDisconnected);

        let (mut initializing, result) = tokio::time::timeout(Duration::from_secs(1), actor)
            .await
            .expect("running source-open observes cancellation")
            .expect("initializer task");
        let error = result.expect_err("cancelled initializer cannot become ready");
        assert!(error.message().contains("cancelled"), "{error}");
        assert!(
            initializing.opening.is_some(),
            "the original open job survives cancellation"
        );
        assert!(
            observe_close.try_recv().is_err(),
            "the blocked source has not exited"
        );
        // Drop an actual pending close wait. The same stable job survives and
        // the next close borrows it; there is no second source open.
        {
            let close = initializing.close();
            tokio::pin!(close);
            std::future::poll_fn(|cx| {
                assert!(close.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
        }
        assert!(initializing.opening.is_some());
        release.send(()).expect("release late source-open success");
        tokio::time::timeout(Duration::from_secs(2), initializing.close())
            .await
            .expect("original cleanup joins")
            .expect("original source close succeeds");
        assert!(initializing.opening.is_none());
        observe_close
            .recv_timeout(Duration::from_secs(2))
            .expect("late state is reaped after actual source-open exit");
        executors.shutdown().await;
        cancellation.complete();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_closes_accepted_source_without_dropping_the_shared_attempt_scope() {
        let (mut executors, supervisor) = supervisor();
        let execution_id = execution_id(1);
        let cancellation = InitializationCancellationFixture::new();
        let (source_two_started, observe_source_two_started) = mpsc::channel();
        let (release_source_two, source_two_released) = mpsc::channel();
        let (source_one_closed, observe_source_one_closed) = mpsc::channel();
        let (source_two_closed, observe_source_two_closed) = mpsc::channel();
        let (scope_dropped, observe_scope_dropped) = mpsc::channel();
        let mut state = fixture(
            execution_id,
            vec![identity(execution_id, 7), identity(execution_id, 8)],
            supervisor.clone(),
            source_one_closed,
        );
        let attempt_context = connector_context();
        attempt_context.request_scope_extension_or_insert_with(|| ScopeDropCanary {
            dropped: Some(scope_dropped),
        });
        state.recipes[0]
            .as_mut()
            .expect("first fixture recipe")
            .connector_context = Some(attempt_context.clone());
        let source_two = state.recipes[1].as_mut().expect("second fixture recipe");
        source_two.started = Some(source_two_started);
        source_two.release = Some(source_two_released);
        source_two.connector_context = Some(attempt_context.clone());
        source_two.cleanup.closed = Some(source_two_closed);
        drop(attempt_context);
        let lifecycle = lifecycle(&cancellation);
        let actor = tokio::spawn(async move {
            let mut initializing = InitializationProgress::new(supervisor, lifecycle, state);
            let result = initializing.initialize().await;
            (initializing, result)
        });
        observe_source_two_started
            .recv_timeout(Duration::from_secs(2))
            .expect("second source starts after the first was accepted");

        cancellation.request(CancellationReason::ClientDisconnected);
        let (mut initializing, result) = tokio::time::timeout(Duration::from_secs(1), actor)
            .await
            .expect("running second source observes cancellation")
            .expect("initializer task");
        let error = result.expect_err("cancelled initializer cannot become ready");
        assert!(error.message().contains("cancelled"), "{error}");
        let close = initializing.close();
        tokio::pin!(close);
        std::future::poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        observe_source_one_closed
            .recv_timeout(Duration::from_secs(2))
            .expect("accepted first source closes before the second returns");
        assert!(
            observe_scope_dropped.try_recv().is_err(),
            "the late source retains the shared attempt request scope"
        );
        assert!(
            observe_source_two_closed.try_recv().is_err(),
            "blocked second source cannot close before its provider returns"
        );
        release_source_two
            .send(())
            .expect("release second source-open call");
        tokio::time::timeout(Duration::from_secs(2), close)
            .await
            .expect("both original close jobs join")
            .expect("both original source closes succeed");
        observe_source_two_closed
            .recv_timeout(Duration::from_secs(2))
            .expect("late second source closes independently");
        observe_scope_dropped
            .recv_timeout(Duration::from_secs(2))
            .expect("the shared attempt scope releases after the late provider return");
        executors.shutdown().await;
        cancellation.complete();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_result_from_another_generation_is_rejected_before_ready() {
        let (mut executors, supervisor) = supervisor();
        let execution_id = execution_id(1);
        let cancellation = InitializationCancellationFixture::new();
        let expected = identity(execution_id, 7);
        let (closed, observe_close) = mpsc::channel();
        let mut state = fixture(
            execution_id,
            vec![expected.clone()],
            supervisor.clone(),
            closed,
        );
        state.recipes[0]
            .as_mut()
            .expect("first fixture recipe")
            .reported_identity = Some(AttemptSourceIdentity {
            generation: binding(99),
            ..expected
        });

        let mut initializing =
            InitializationProgress::new(supervisor, lifecycle(&cancellation), state);
        let error = initializing
            .initialize()
            .await
            .expect_err("another generation cannot become ready");
        assert!(
            initializing.outcome.is_some(),
            "the rejected source remains in the original result slot"
        );
        initializing
            .close()
            .await
            .expect("the original source-close worker joins");

        assert!(error.message().contains("another identity"), "{error}");
        observe_close
            .recv_timeout(Duration::from_secs(2))
            .expect("rejected source state is reaped");
        executors.shutdown().await;
        cancellation.complete();
    }
}
