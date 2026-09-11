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
//! one ordinary process job; when that lane is full, the actor itself waits
//! for a move-only admission without spawning a helper task. Only
//! [`AttemptReady`] can hand the artifacts,
//! source plan, and sealed credential leases to task-round construction.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_spi::connector::read_stack::ConnectorReadBinding;
use novarocks_spi::connector::{ConnectorRequestContext, ConnectorRequestScope};
use novarocks_sql::plan_read::FragmentId;

use crate::common::query_cancellation::QueryCancellationView;
use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::artifact::{PreparedDistributedQuery, ValidatedFragmentSchedule};
use crate::query_execution::completion::QueryAttemptReservation;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle_plan::{
    AttemptCredentialLeaseCollector, QueryCredentialLeases,
};
use crate::query_execution::split_assignment::TaskUpdateRetryPolicy;
use crate::query_execution::split_assignment_round::{
    OpenRoundSplitSources, OpenedRoundSplitSource, RoundSplitAssignmentPlan,
    RoundSplitSourceRecipe, assignment_endpoints, assignment_targets, open_round_split_source,
};
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
use crate::task_execution::blocking_io::{
    ConnectorBlockingIoAdmission, ConnectorBlockingIoJob, ConnectorBlockingIoSupervisor,
};

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

fn connector_context_for_single_scan(context: &ConnectorRequestContext) -> ConnectorRequestContext {
    context
        .clone()
        .with_request_scope(ConnectorRequestScope::new())
}

/// Credential material stays under its planning collector until every split
/// source of this exact attempt is open.
pub(crate) enum RoundCredentialLeaseSource {
    Frozen(QueryCredentialLeases),
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
    pub(crate) fn fresh(execution: QueryExecutionId) -> Self {
        Self::Fresh {
            collector: AttemptCredentialLeaseCollector::new(execution),
            request_scope: ConnectorRequestScope::new(),
        }
    }

    pub(crate) fn connector_request_context(
        &self,
        context: ConnectorRequestContext,
    ) -> ConnectorRequestContext {
        match self {
            Self::Frozen(_) => context,
            Self::Fresh {
                collector,
                request_scope,
            } => context
                .with_request_scope(request_scope.clone())
                .with_storage_resolver(collector.storage_resolver())
                .with_vended_credential_lease_sink(collector.sink()),
            Self::Reservation { reservation, .. } => reservation
                .as_ref()
                .expect("attempt reservation exists before credential sealing")
                .connector_request_context(context),
        }
    }

    fn publish_observed(&self) {
        if let Self::Reservation {
            observed_collected: Some(observed_collected),
            reservation: Some(reservation),
        } = self
        {
            if reservation.has_collected_credential_leases() {
                observed_collected.store(true, Ordering::Release);
            }
        }
    }

    fn into_credential_leases(mut self) -> Result<QueryCredentialLeases, DistributedQueryError> {
        self.publish_observed();
        match &mut self {
            Self::Frozen(leases) => Ok(std::mem::replace(leases, QueryCredentialLeases::empty())),
            Self::Fresh { collector, .. } => collector.into_credential_leases(),
            Self::Reservation { reservation, .. } => reservation
                .take()
                .expect("attempt reservation is consumed exactly once")
                .into_credential_leases(),
        }
    }
}

impl Drop for RoundCredentialLeaseSource {
    fn drop(&mut self) {
        self.publish_observed();
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

trait SerialAttemptInitialization: Send + Sized + 'static {
    type Ready: Send + 'static;
    type Recipe: Send + 'static;
    type Opened: Send + 'static;

    fn execution_id(&self) -> QueryExecutionId;

    fn next_source_recipe(
        &mut self,
    ) -> Result<Option<AttemptSourceRecipe<Self::Recipe>>, DistributedQueryError>;

    fn open_source(
        recipe: AttemptSourceRecipe<Self::Recipe>,
    ) -> Result<OpenedAttemptSource<Self::Opened>, DistributedQueryError>;

    fn accept_opened(&mut self, opened: Self::Opened) -> Result<(), DistributedQueryError>;

    fn finish(self) -> Result<Self::Ready, DistributedQueryError>;
}

#[derive(Clone)]
struct AttemptInitializationLifecycle {
    deadline: Instant,
    cancellation: AttemptInitializationCancellation,
}

#[derive(Clone)]
enum AttemptInitializationCancellation {
    Legacy(QueryCancellationView),
    Governed(novarocks_workload_control::CancellationView),
}

impl AttemptInitializationCancellation {
    fn is_cancelled(&self) -> bool {
        match self {
            Self::Legacy(cancellation) => cancellation.is_cancelled(),
            Self::Governed(cancellation) => cancellation.reason().is_some(),
        }
    }

    async fn cancelled(&self) {
        match self {
            Self::Legacy(cancellation) => {
                cancellation.cancelled().await;
            }
            Self::Governed(cancellation) => {
                cancellation.cancelled().await;
            }
        }
    }
}

impl AttemptInitializationLifecycle {
    fn new(deadline: Instant, cancellation: QueryCancellationView) -> Self {
        Self {
            deadline,
            cancellation: AttemptInitializationCancellation::Legacy(cancellation),
        }
    }

    fn governed(
        deadline: Instant,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> Self {
        Self {
            deadline,
            cancellation: AttemptInitializationCancellation::Governed(cancellation),
        }
    }

    fn check(&self) -> Result<(), DistributedQueryError> {
        if self.cancellation.is_cancelled() {
            return Err(failed("query cancelled while initializing its attempt"));
        }
        if Instant::now() >= self.deadline {
            return Err(failed(
                "query deadline elapsed while initializing its attempt",
            ));
        }
        Ok(())
    }

    async fn acquire_ordinary(
        &self,
        supervisor: &ConnectorBlockingIoSupervisor,
    ) -> Result<ConnectorBlockingIoAdmission, DistributedQueryError> {
        self.check()?;
        tokio::select! {
            admission = supervisor.acquire_ordinary() => {
                let admission = admission.map_err(|error| failed(error.to_string()))?;
                self.check()?;
                Ok(admission)
            }
            _ = self.cancellation.cancelled() => {
                Err(failed("query cancelled while waiting for attempt source-open capacity"))
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(self.deadline)) => {
                Err(failed("query deadline elapsed while waiting for attempt source-open capacity"))
            }
        }
    }

    async fn await_job<T>(
        &self,
        job: ConnectorBlockingIoJob<T>,
    ) -> Result<T, DistributedQueryError> {
        tokio::select! {
            outcome = job.finish() => {
                let outcome = outcome.map_err(|error| failed(error.to_string()))?;
                self.check()?;
                Ok(outcome)
            }
            _ = self.cancellation.cancelled() => {
                Err(failed("query cancelled while opening an attempt split source"))
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(self.deadline)) => {
                Err(failed("query deadline elapsed while opening an attempt split source"))
            }
        }
    }
}

async fn drive_attempt_initialization<S: SerialAttemptInitialization>(
    supervisor: ConnectorBlockingIoSupervisor,
    lifecycle: AttemptInitializationLifecycle,
    mut state: S,
) -> Result<S::Ready, DistributedQueryError> {
    loop {
        lifecycle.check()?;
        let Some(recipe) = state.next_source_recipe()? else {
            return state.finish();
        };
        let expected = recipe.identity.clone();
        if expected.execution_id != state.execution_id() {
            return Err(failed(
                "attempt initializer produced a source recipe for another execution",
            ));
        }
        let admission = lifecycle.acquire_ordinary(&supervisor).await?;
        let job = supervisor.spawn_admitted_ordinary(admission, move || S::open_source(recipe));
        let opened = lifecycle.await_job(job).await??;
        if opened.identity != expected {
            return Err(failed(format!(
                "attempt initializer received a source from another identity: expected={expected:?}, actual={:?}",
                opened.identity
            )));
        }
        state.accept_opened(opened.opened)?;
        // A non-interruptible provider can return after cancellation or the
        // deadline. Reject that late success before it can become AttemptReady;
        // dropping the state reaps every opened source through protected
        // lifecycle capacity.
        lifecycle.check()?;
    }
}

struct ProductionInitializationState {
    execution_id: QueryExecutionId,
    artifacts: PreparedDistributedQuery,
    schedule: ValidatedFragmentSchedule,
    scan_keys: Vec<(FragmentId, i32)>,
    next_scan: usize,
    session: novarocks_spi::connector::read_stack::ConnectorSession,
    connector_context: ConnectorRequestContext,
    feedback: Arc<RuntimeFilterFeedbackState>,
    sources: OpenRoundSplitSources,
    retry_policy: TaskUpdateRetryPolicy,
    initial_dynamic_filter_wait_cap: Duration,
    blocking_io: ConnectorBlockingIoSupervisor,
    credential_lease_source: RoundCredentialLeaseSource,
    permits_confidential_credential_leases: bool,
}

struct ProductionSourceRecipe {
    source: RoundSplitSourceRecipe,
    session: novarocks_spi::connector::read_stack::ConnectorSession,
    connector_context: ConnectorRequestContext,
    blocking_io: ConnectorBlockingIoSupervisor,
}

impl SerialAttemptInitialization for ProductionInitializationState {
    type Ready = AttemptReady;
    type Recipe = ProductionSourceRecipe;
    type Opened = OpenedRoundSplitSource;

    fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    fn next_source_recipe(
        &mut self,
    ) -> Result<Option<AttemptSourceRecipe<Self::Recipe>>, DistributedQueryError> {
        let Some(&(fragment_id, plan_node_id)) = self.scan_keys.get(self.next_scan) else {
            return Ok(None);
        };
        let source =
            RoundSplitSourceRecipe::from_artifacts(&self.artifacts, fragment_id, plan_node_id)
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
                source,
                session: self.session.clone(),
                connector_context: connector_context_for_single_scan(&self.connector_context),
                blocking_io: self.blocking_io.clone(),
            },
        }))
    }

    fn open_source(
        recipe: AttemptSourceRecipe<Self::Recipe>,
    ) -> Result<OpenedAttemptSource<Self::Opened>, DistributedQueryError> {
        let AttemptSourceRecipe { identity, recipe } = recipe;
        let source = open_round_split_source(
            recipe.source,
            &recipe.session,
            &recipe.connector_context,
            recipe.blocking_io,
        )
        .map_err(failed)?;
        Ok(OpenedAttemptSource {
            identity,
            opened: source,
        })
    }

    fn accept_opened(&mut self, opened: Self::Opened) -> Result<(), DistributedQueryError> {
        let source = opened.into_round_source(Arc::clone(&self.feedback));
        self.sources.push(source);
        self.next_scan += 1;
        Ok(())
    }

    fn finish(self) -> Result<Self::Ready, DistributedQueryError> {
        if self.next_scan != self.scan_keys.len() {
            return Err(failed(
                "attempt initializer cannot become ready with unopened split sources",
            ));
        }
        let targets = assignment_targets(&self.schedule, &self.scan_keys);
        for plan_node_id in self.sources.plan_node_ids() {
            if targets
                .get(&plan_node_id)
                .is_none_or(|targets| targets.is_empty())
            {
                return Err(failed(format!(
                    "typed connector scan node_id={plan_node_id} has no admitted task in this schedule"
                )));
            }
        }
        let credential_leases = self.credential_lease_source.into_credential_leases()?;
        if !credential_leases.is_empty() && !self.permits_confidential_credential_leases {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "vended credential lease admission requires TLS Native transport",
            ));
        }
        // Keep the sources under their RAII owner through every fallible
        // readiness check. Once moved into the round plan, normal round
        // teardown becomes their sole close owner.
        let sources = self.sources.into_sources();
        let split_assignment_plan = (!self.scan_keys.is_empty()).then(|| {
            RoundSplitAssignmentPlan::new(
                targets,
                sources,
                self.retry_policy,
                self.initial_dynamic_filter_wait_cap,
                assignment_endpoints(&self.schedule),
                self.blocking_io,
            )
        });
        Ok(AttemptReady {
            execution_id: self.execution_id,
            artifacts: self.artifacts,
            schedule: self.schedule,
            feedback: self.feedback,
            split_assignment_plan,
            credential_leases,
        })
    }
}

/// The only pre-round state. It owns source recipes and attempt credentials,
/// but exposes neither until every source has opened under this attempt.
pub(crate) struct AttemptInitializing {
    runtime: FrontendDataRuntime,
    lifecycle: AttemptInitializationLifecycle,
    state: ProductionInitializationState,
}

impl AttemptInitializing {
    #[expect(
        clippy::too_many_arguments,
        reason = "Each input is an independently frozen fact needed before an attempt may become ready."
    )]
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        artifacts: PreparedDistributedQuery,
        schedule: ValidatedFragmentSchedule,
        retry_policy: TaskUpdateRetryPolicy,
        feedback: Arc<RuntimeFilterFeedbackState>,
        initial_dynamic_filter_wait_cap: Duration,
        connector_context: ConnectorRequestContext,
        cancellation: QueryCancellationView,
        runtime: FrontendDataRuntime,
        credential_lease_source: RoundCredentialLeaseSource,
    ) -> Result<Self, DistributedQueryError> {
        let lifecycle =
            AttemptInitializationLifecycle::new(connector_context.deadline(), cancellation);
        Self::from_lifecycle(
            execution_id,
            artifacts,
            schedule,
            retry_policy,
            feedback,
            initial_dynamic_filter_wait_cap,
            connector_context,
            runtime,
            credential_lease_source,
            lifecycle,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Each input is an independently frozen fact needed before an attempt may become ready."
    )]
    fn from_lifecycle(
        execution_id: QueryExecutionId,
        artifacts: PreparedDistributedQuery,
        schedule: ValidatedFragmentSchedule,
        retry_policy: TaskUpdateRetryPolicy,
        feedback: Arc<RuntimeFilterFeedbackState>,
        initial_dynamic_filter_wait_cap: Duration,
        connector_context: ConnectorRequestContext,
        runtime: FrontendDataRuntime,
        credential_lease_source: RoundCredentialLeaseSource,
        lifecycle: AttemptInitializationLifecycle,
    ) -> Result<Self, DistributedQueryError> {
        if schedule.execution_id() != execution_id {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "attempt initializer received a schedule for another execution",
            ));
        }
        let scan_keys = artifacts
            .typed_scans()
            .map(|(fragment_id, plan_node_id, _)| (fragment_id, plan_node_id))
            .collect::<Vec<_>>();
        let session =
            crate::query_execution::compiler::typed_connector_session().map_err(failed)?;
        let blocking_io = runtime.connector_blocking_io().clone();
        let permits_confidential_credential_leases = runtime
            .native_transport()
            .permits_confidential_credential_leases();
        Ok(Self {
            runtime,
            lifecycle,
            state: ProductionInitializationState {
                execution_id,
                artifacts,
                schedule,
                sources: OpenRoundSplitSources::with_capacity(scan_keys.len(), blocking_io.clone()),
                scan_keys,
                next_scan: 0,
                session,
                connector_context,
                feedback,
                retry_policy,
                initial_dynamic_filter_wait_cap,
                blocking_io,
                credential_lease_source,
                permits_confidential_credential_leases,
            },
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
    pub(crate) fn new_governed(
        execution_id: QueryExecutionId,
        artifacts: PreparedDistributedQuery,
        schedule: ValidatedFragmentSchedule,
        retry_policy: TaskUpdateRetryPolicy,
        feedback: Arc<RuntimeFilterFeedbackState>,
        initial_dynamic_filter_wait_cap: Duration,
        connector_context: ConnectorRequestContext,
        cancellation: novarocks_workload_control::CancellationView,
        runtime: FrontendDataRuntime,
        credential_lease_source: RoundCredentialLeaseSource,
    ) -> Result<Self, DistributedQueryError> {
        let lifecycle =
            AttemptInitializationLifecycle::governed(connector_context.deadline(), cancellation);
        Self::from_lifecycle(
            execution_id,
            artifacts,
            schedule,
            retry_policy,
            feedback,
            initial_dynamic_filter_wait_cap,
            connector_context,
            runtime,
            credential_lease_source,
            lifecycle,
        )
    }

    pub(crate) async fn initialize(self) -> Result<AttemptReady, DistributedQueryError> {
        drive_attempt_initialization(
            self.runtime.connector_blocking_io().clone(),
            self.lifecycle,
            self.state,
        )
        .await
    }
}

/// Move-only proof that source-open, exact generation checks, and credential
/// sealing all completed before task-round construction.
pub(crate) struct AttemptReady {
    execution_id: QueryExecutionId,
    artifacts: PreparedDistributedQuery,
    schedule: ValidatedFragmentSchedule,
    feedback: Arc<RuntimeFilterFeedbackState>,
    split_assignment_plan: Option<RoundSplitAssignmentPlan>,
    credential_leases: QueryCredentialLeases,
}

impl AttemptReady {
    pub(crate) fn into_parts(
        self,
    ) -> (
        QueryExecutionId,
        PreparedDistributedQuery,
        ValidatedFragmentSchedule,
        Arc<RuntimeFilterFeedbackState>,
        Option<RoundSplitAssignmentPlan>,
        QueryCredentialLeases,
    ) {
        (
            self.execution_id,
            self.artifacts,
            self.schedule,
            self.feedback,
            self.split_assignment_plan,
            self.credential_leases,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogVersion, ConnectorCancellation, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, CredentialConsumerRole,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        StorageCredentialScopePrefix, VendedS3CredentialLeaseContribution,
        VendedS3CredentialLeaseEntry,
    };
    use novarocks_types::{AttemptId, QueryId};

    use super::*;
    use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::task_execution::blocking_io::ConnectorBlockingIoBudget;

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

    fn supervisor() -> ConnectorBlockingIoSupervisor {
        ConnectorBlockingIoSupervisor::new(
            tokio::runtime::Handle::current(),
            ConnectorBlockingIoBudget::try_new(2, 1)
                .expect("one ordinary and one protected permit"),
        )
    }

    struct ProtectedCleanup {
        armed: bool,
        supervisor: ConnectorBlockingIoSupervisor,
        closed: Option<mpsc::Sender<()>>,
    }

    impl Drop for ProtectedCleanup {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            let Some(closed) = self.closed.take() else {
                return;
            };
            let _ = self.supervisor.spawn_protected(move || {
                closed.send(()).expect("publish protected cleanup");
            });
        }
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

    impl SerialAttemptInitialization for FixtureInitializationState {
        type Ready = usize;
        type Recipe = FixtureSourceRecipe;
        type Opened = ProtectedCleanup;

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

        fn open_source(
            recipe: AttemptSourceRecipe<Self::Recipe>,
        ) -> Result<OpenedAttemptSource<Self::Opened>, DistributedQueryError> {
            let AttemptSourceRecipe {
                identity,
                mut recipe,
            } = recipe;
            if let Some(started) = recipe.started.take() {
                started.send(()).expect("publish source-open start");
            }
            if let Some(release) = recipe.release.take() {
                release.recv().expect("release source-open call");
            }
            drop(recipe.connector_context.take());
            recipe.cleanup.armed = true;
            let identity = recipe.reported_identity.take().unwrap_or(identity);
            Ok(OpenedAttemptSource {
                identity,
                opened: recipe.cleanup,
            })
        }

        fn accept_opened(&mut self, opened: Self::Opened) -> Result<(), DistributedQueryError> {
            self.accepted.push(opened);
            self.next += 1;
            Ok(())
        }

        fn finish(mut self) -> Result<Self::Ready, DistributedQueryError> {
            for cleanup in &mut self.accepted {
                cleanup.armed = false;
            }
            Ok(self.next)
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
                        armed: false,
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

    fn lifecycle(source: &QueryCancellationSource) -> AttemptInitializationLifecycle {
        AttemptInitializationLifecycle::new(Instant::now() + Duration::from_secs(5), source.view())
    }

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct ScopeDropCanary {
        dropped: Option<mpsc::Sender<()>>,
    }

    impl Drop for ScopeDropCanary {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                dropped.send(()).expect("publish request-scope drop");
            }
        }
    }

    fn connector_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
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
        reservation
            .credential_lease_sink()
            .offer_vended_s3_credential_lease(&vended_catalog_properties(), vended_contribution())
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_scan_becomes_ready_without_submitting_connector_work() {
        let supervisor = supervisor();
        let execution_id = execution_id(1);
        let cancellation = QueryCancellationSource::new();
        let (closed, _observe_close) = mpsc::channel();
        let state = fixture(execution_id, Vec::new(), supervisor.clone(), closed);

        let ready = drive_attempt_initialization(supervisor, lifecycle(&cancellation), state)
            .await
            .expect("scanless attempt becomes ready");

        assert_eq!(ready, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_while_waiting_for_capacity_keeps_recipe_unopened() {
        let supervisor = supervisor();
        let (release, released) = mpsc::channel();
        let (occupied, observe_occupied) = mpsc::channel();
        let blocker = supervisor.spawn_ordinary(move || {
            occupied.send(()).expect("ordinary lane occupied");
            released.recv().expect("release ordinary lane");
        });
        observe_occupied
            .recv_timeout(Duration::from_secs(2))
            .expect("ordinary blocker starts");

        let execution_id = execution_id(1);
        let cancellation = QueryCancellationSource::new();
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
        let actor = tokio::spawn(drive_attempt_initialization(
            supervisor,
            lifecycle(&cancellation),
            state,
        ));
        tokio::task::yield_now().await;
        cancellation.request(QueryCancellationReason::ClientDisconnected);

        let error = tokio::time::timeout(Duration::from_secs(1), actor)
            .await
            .expect("capacity waiter observes cancellation")
            .expect("initializer task")
            .expect_err("cancelled initializer cannot become ready");
        assert!(error.message().contains("cancelled"), "{error}");
        assert!(
            observe_started.try_recv().is_err(),
            "a recipe waiting for admission must remain unopened"
        );
        release.send(()).expect("release blocker");
        blocker.finish().await.expect("blocker finishes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_during_source_open_reaps_late_success_through_protected_capacity() {
        let supervisor = supervisor();
        let execution_id = execution_id(1);
        let cancellation = QueryCancellationSource::new();
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
        let actor = tokio::spawn(drive_attempt_initialization(
            supervisor,
            lifecycle(&cancellation),
            state,
        ));
        observe_started
            .recv_timeout(Duration::from_secs(2))
            .expect("source-open call starts");
        cancellation.request(QueryCancellationReason::ClientDisconnected);

        let error = tokio::time::timeout(Duration::from_secs(1), actor)
            .await
            .expect("running source-open observes cancellation")
            .expect("initializer task")
            .expect_err("cancelled initializer cannot become ready");
        assert!(error.message().contains("cancelled"), "{error}");
        release.send(()).expect("release late source-open success");
        observe_close
            .recv_timeout(Duration::from_secs(2))
            .expect("late state is reaped through protected capacity");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_closes_accepted_source_before_late_source_returns() {
        let supervisor = supervisor();
        let execution_id = execution_id(1);
        let cancellation = QueryCancellationSource::new();
        let (source_two_started, observe_source_two_started) = mpsc::channel();
        let (release_source_two, source_two_released) = mpsc::channel();
        let (source_one_closed, observe_source_one_closed) = mpsc::channel();
        let (source_two_closed, observe_source_two_closed) = mpsc::channel();
        let (source_one_scope_dropped, observe_source_one_scope_dropped) = mpsc::channel();
        let (source_two_scope_dropped, observe_source_two_scope_dropped) = mpsc::channel();
        let mut state = fixture(
            execution_id,
            vec![identity(execution_id, 7), identity(execution_id, 8)],
            supervisor.clone(),
            source_one_closed,
        );
        let base_context = connector_context();
        let source_one_context = connector_context_for_single_scan(&base_context);
        source_one_context.request_scope_extension_or_insert_with(|| ScopeDropCanary {
            dropped: Some(source_one_scope_dropped),
        });
        state.recipes[0]
            .as_mut()
            .expect("first fixture recipe")
            .connector_context = Some(source_one_context);
        let source_two = state.recipes[1].as_mut().expect("second fixture recipe");
        let source_two_context = connector_context_for_single_scan(&base_context);
        source_two_context.request_scope_extension_or_insert_with(|| ScopeDropCanary {
            dropped: Some(source_two_scope_dropped),
        });
        source_two.started = Some(source_two_started);
        source_two.release = Some(source_two_released);
        source_two.connector_context = Some(source_two_context);
        source_two.cleanup.closed = Some(source_two_closed);
        let actor = tokio::spawn(drive_attempt_initialization(
            supervisor,
            lifecycle(&cancellation),
            state,
        ));
        observe_source_two_started
            .recv_timeout(Duration::from_secs(2))
            .expect("second source starts after the first was accepted");

        cancellation.request(QueryCancellationReason::ClientDisconnected);
        let error = tokio::time::timeout(Duration::from_secs(1), actor)
            .await
            .expect("running second source observes cancellation")
            .expect("initializer task")
            .expect_err("cancelled initializer cannot become ready");
        assert!(error.message().contains("cancelled"), "{error}");
        observe_source_one_closed
            .recv_timeout(Duration::from_secs(2))
            .expect("accepted first source closes before the second returns");
        observe_source_one_scope_dropped
            .recv_timeout(Duration::from_secs(2))
            .expect("the accepted first source does not retain the second source request scope");
        assert!(
            observe_source_two_closed.try_recv().is_err(),
            "blocked second source cannot close before its provider returns"
        );
        assert!(
            observe_source_two_scope_dropped.try_recv().is_err(),
            "blocked second source retains only its own request scope until its provider returns"
        );

        release_source_two
            .send(())
            .expect("release second source-open call");
        observe_source_two_closed
            .recv_timeout(Duration::from_secs(2))
            .expect("late second source closes independently");
        observe_source_two_scope_dropped
            .recv_timeout(Duration::from_secs(2))
            .expect("late second source releases its own request scope after provider return");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_result_from_another_generation_is_rejected_before_ready() {
        let supervisor = supervisor();
        let execution_id = execution_id(1);
        let cancellation = QueryCancellationSource::new();
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

        let error = drive_attempt_initialization(supervisor, lifecycle(&cancellation), state)
            .await
            .expect_err("another generation cannot become ready");

        assert!(error.message().contains("another identity"), "{error}");
        observe_close
            .recv_timeout(Duration::from_secs(2))
            .expect("rejected source state is reaped");
    }
}
