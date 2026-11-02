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

//! Immutable request state captured exactly once at statement admission.
// Design: ADR-0011 (docs/adr/ADR-0011-immutable-request-execution-context.md)

use std::{sync::Arc, time::Instant};

use crate::api::BackendTopologySnapshot;
use crate::cancellation::QueryCancellationView;
use crate::request_session::RequestSessionContext;
use novarocks_sql::compiler::{SessionOptimizerSettings, SqlFoldDependencyObserver};
use novarocks_types::ClusterRole;
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkError, WorkScope};

/// All inputs accepted at the frontend statement-admission boundary.
///
/// The contained values are moved into an immutable [`RequestContext`]; the
/// individual projection constructors intentionally remain private.
pub struct RequestAdmission {
    current_catalog: Option<String>,
    current_database: String,
    role: ClusterRole,
    topology: BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    optimizer_settings: SessionOptimizerSettings,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    presentation: novarocks_result_contract::ClientPresentationFamily,
}

impl RequestAdmission {
    pub fn new(
        current_catalog: Option<String>,
        current_database: String,
        role: ClusterRole,
        topology: BackendTopologySnapshot,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        optimizer_settings: SessionOptimizerSettings,
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
        presentation: novarocks_result_contract::ClientPresentationFamily,
    ) -> Self {
        Self {
            current_catalog,
            current_database,
            role,
            topology,
            deadline,
            cancellation,
            optimizer_settings,
            sql_semantics,
            presentation,
        }
    }
}

/// Statement-stable inputs retained while a distributed statement may need a
/// new topology round.  It deliberately owns no backend snapshot: every
/// round derives a fresh [`QueryExecutionContext`] from this immutable
/// admission boundary.
///
/// The existing [`RequestContext`] remains the narrow projection consumed by
/// compiler and coordinator code.  It is therefore impossible for a caller
/// to manufacture a later round by modifying an already prepared request.
#[derive(Clone)]
pub struct StatementAdmissionContext {
    #[cfg(novarocks_dev_controls)]
    test_statement_nomination:
        Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
    session: RequestSessionContext,
    role: ClusterRole,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    fold_dependency_observer: Option<Arc<dyn SqlFoldDependencyObserver>>,
}

impl StatementAdmissionContext {
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_statement_nomination(
        mut self,
        nomination: Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
    ) -> Result<Self, String> {
        if let Some(nomination) = &nomination {
            nomination.validate().map_err(|error| error.to_string())?;
        }
        if self.test_statement_nomination.is_some() && self.test_statement_nomination != nomination
        {
            return Err("statement admission already names a different test nomination".into());
        }
        self.test_statement_nomination = nomination;
        Ok(self)
    }

    pub fn new(
        current_catalog: Option<String>,
        current_database: String,
        role: ClusterRole,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        optimizer_settings: SessionOptimizerSettings,
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
        presentation: novarocks_result_contract::ClientPresentationFamily,
    ) -> Self {
        Self {
            #[cfg(novarocks_dev_controls)]
            test_statement_nomination: None,
            session: RequestSessionContext::new(
                current_catalog,
                current_database,
                optimizer_settings,
                sql_semantics,
                presentation,
            ),
            role,
            deadline,
            cancellation,
            fold_dependency_observer: None,
        }
    }

    /// Attach the admitted statement's observer without changing its control facts.
    pub fn with_fold_dependency_observer(
        mut self,
        observer: Arc<dyn SqlFoldDependencyObserver>,
    ) -> Self {
        self.fold_dependency_observer = Some(observer);
        self
    }

    /// Borrow the same query-owned observer; absence preserves the original path.
    pub fn fold_dependency_observer(&self) -> Option<&Arc<dyn SqlFoldDependencyObserver>> {
        self.fold_dependency_observer.as_ref()
    }

    /// Retain an already admitted observer across a topology/retry projection.
    pub fn with_optional_fold_dependency_observer(
        mut self,
        observer: Option<Arc<dyn SqlFoldDependencyObserver>>,
    ) -> Self {
        self.fold_dependency_observer = observer;
        self
    }

    /// Derive the compiler/coordinator projection for exactly one frozen
    /// topology round.  Semantic session state, deadline, and cancellation
    /// identity remain those admitted for the original statement.
    pub fn for_topology(&self, topology: BackendTopologySnapshot) -> RequestContext {
        let execution = QueryExecutionContext::new(
            self.role,
            topology,
            self.deadline,
            self.cancellation.clone(),
            self.session.optimizer_settings().clone(),
            self.session.sql_semantics().clone(),
        );
        let execution = match &self.fold_dependency_observer {
            Some(observer) => execution.with_fold_dependency_observer(Arc::clone(observer)),
            None => execution,
        };
        #[cfg(novarocks_dev_controls)]
        let execution = {
            let mut execution = execution;
            execution.test_statement_nomination = self.test_statement_nomination.clone();
            execution
        };
        RequestContext::new(self.session.clone(), execution)
    }

    pub fn session(&self) -> &RequestSessionContext {
        &self.session
    }

    pub const fn role(&self) -> ClusterRole {
        self.role
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }
}

/// Exact runtime capacity supplied by admission. It is never projected into
/// a completed plan, frozen semantic description, or native plan DTO.
#[derive(Clone)]
pub struct QueryResultCapacityBinding {
    scope: WorkScope,
    window: ResultWindowAlias,
    activity: std::sync::Arc<ResultCapacityActivity>,
}

#[derive(Default)]
struct ResultCapacityActivity {
    state: std::sync::Mutex<ResultCapacityActivityState>,
    exited: std::sync::Condvar,
    async_exited: tokio::sync::Notify,
}
#[derive(Default)]
struct ResultCapacityActivityState {
    count: usize,
    streamed: Option<StreamedComputationCustody>,
}
struct StreamedComputationCustody {
    // The sole positive streamed computation; Window/Native tails carry no pin.
    grant: Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    sealed: bool,
    producer_exited: bool,
    retiring: bool,
    retired: bool,
}
/// Private original-source observation. Only the real streaming pump/Native
/// body exit authors may close it; a Work identity or success enum cannot.
#[derive(Clone)]
pub(crate) struct StreamedComputationCutHandle {
    activity: std::sync::Weak<ResultCapacityActivity>,
    scope: WorkScope,
}
/// One inline custody reference to the existing activity allocation in the
/// original Native body. It prevents incidental capacity-alias Drop from
/// retiring the computation before the real local producer frame exits.
pub(crate) struct StreamedComputationProducer {
    activity: std::sync::Arc<ResultCapacityActivity>,
    scope: WorkScope,
}
impl StreamedComputationProducer {
    pub(crate) fn native_body_exited(&self) -> Result<(), WorkError> {
        let handle = StreamedComputationCutHandle {
            activity: std::sync::Arc::downgrade(&self.activity),
            scope: self.scope.clone(),
        };
        handle.native_body_exited()
    }
}
impl StreamedComputationCutHandle {
    pub(crate) fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.scope.id() == scope.id()
    }
    pub(crate) fn same_source(&self, other: &Self) -> bool {
        std::sync::Weak::ptr_eq(&self.activity, &other.activity)
    }
    /// Retain this SAME owned computation while its original registered
    /// consumer factory/F/Join is live. No window, alias or position is minted.
    pub(crate) fn begin_consumer_activity(&self) -> Result<ResultCapacityActivityLease, WorkError> {
        self.scope.check()?;
        let activity = self.activity.upgrade().ok_or(WorkError::Released)?;
        {
            let mut state = activity.state.lock().expect("result activity lock");
            let streamed = state.streamed.as_ref().ok_or(WorkError::Conflict)?;
            if streamed.sealed || streamed.grant.is_none() {
                return Err(WorkError::Conflict);
            }
            state.count = state
                .count
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
        }
        Ok(ResultCapacityActivityLease {
            activity,
            window: None,
            computation: None,
        })
    }
    pub(crate) fn seal_producer(&self) -> Result<(), WorkError> {
        let activity = self.activity.upgrade().ok_or(WorkError::Released)?;
        let mut state = activity.state.lock().expect("result activity lock");
        let streamed = state.streamed.as_mut().ok_or(WorkError::Conflict)?;
        streamed.sealed = true;
        Ok(())
    }
    fn producer_exited(&self) -> Result<std::sync::Arc<ResultCapacityActivity>, WorkError> {
        let activity = self.activity.upgrade().ok_or(WorkError::Released)?;
        {
            let mut state = activity.state.lock().expect("result activity lock");
            let streamed = state.streamed.as_mut().ok_or(WorkError::Conflict)?;
            streamed.sealed = true;
            streamed.producer_exited = true;
        }
        retire_streamed_if_ready(&activity);
        Ok(activity)
    }
    /// Only after the original driver/pump frame returned normally. The
    /// private pump token is checked before any positive grant can exit.
    pub(crate) async fn retire_after_pump_exit(
        &self,
        cut: super::coordination::StreamedRowsComputedCut,
    ) -> Result<(), WorkError> {
        if !cut.is_for(self) {
            return Err(WorkError::Conflict);
        }
        let activity = self.producer_exited()?;
        loop {
            let exited = activity.async_exited.notified();
            tokio::pin!(exited);
            exited.as_mut().enable();
            if activity
                .state
                .lock()
                .expect("result activity lock")
                .streamed
                .as_ref()
                .is_some_and(|streamed| streamed.retired)
            {
                return Ok(());
            }
            exited.await;
        }
    }
    /// OriginalLogicalExit invokes this only after its real local role frame
    /// and Native-body destructors have normally exited. Failure/timeout alone
    /// never calls it; held activities still keep the same grant charged.
    pub(crate) fn native_body_exited(&self) -> Result<(), WorkError> {
        self.producer_exited().map(|_| ())
    }
}
fn retire_streamed_if_ready(activity: &std::sync::Arc<ResultCapacityActivity>) {
    let grant = {
        let mut state = activity.state.lock().expect("result activity lock");
        if state.count != 0 {
            return;
        }
        let Some(streamed) = state.streamed.as_mut() else {
            return;
        };
        if !streamed.producer_exited || !streamed.sealed || streamed.retiring || streamed.retired {
            return;
        }
        streamed.retiring = true;
        streamed
            .grant
            .take()
            .expect("one original streamed computation")
    };
    // The original positive resource exits outside the activity mutex. A Drop
    // unwind leaves Retiring, never a fabricated completed retirement.
    drop(grant);
    {
        let mut state = activity.state.lock().expect("result activity lock");
        let streamed = state.streamed.as_mut().unwrap();
        streamed.retiring = false;
        streamed.retired = true;
    }
    activity.exited.notify_all();
    activity.async_exited.notify_waiters();
}

/// Actual execution ownership, independent of a cancelled waiter. Drop this
/// only after the input or unclaimed output it guards has actually exited.
pub struct ResultCapacityActivityLease {
    activity: std::sync::Arc<ResultCapacityActivity>,
    window: Option<ResultWindowAlias>,
    computation: Option<novarocks_workload_control::joint_admission::InternalComputationAlias>,
}
impl Drop for ResultCapacityActivityLease {
    fn drop(&mut self) {
        drop(self.window.take());
        drop(self.computation.take());
        let exited = {
            let mut state = self.activity.state.lock().expect("result activity lock");
            state.count = state
                .count
                .checked_sub(1)
                .expect("one result activity exit");
            state.count == 0
        };
        if exited {
            retire_streamed_if_ready(&self.activity);
            self.activity.exited.notify_all();
            self.activity.async_exited.notify_waiters();
        }
    }
}

impl QueryResultCapacityBinding {
    /// Pure geometry of the actual per-binding activity allocation. This
    /// constructs no binding, waiter, result alias or resource authority.
    pub fn fixed_backing_layout() -> Result<std::alloc::Layout, WorkError> {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            ResultCapacityActivity,
        >())
        .map_err(|_| WorkError::ArithmeticOverflow)
    }

    pub fn native_action_authority(
        &self,
    ) -> Result<novarocks_workload_control::joint_admission::NativeAsyncActionAuthority, WorkError>
    {
        self.window.native_action_authority()
    }
    /// Pure delegation from the same original Root result owner. The returned
    /// source is not a new budget, Native dispatch or physical grant.
    pub fn native_action_source(
        &self,
        lane: novarocks_workload_control::joint_admission::JointLane,
        quote: novarocks_workload_control::joint_admission::NativeAsyncActionQuote,
    ) -> Result<novarocks_workload_control::joint_admission::NativeAsyncActionSource, WorkError>
    {
        self.window.native_action_source(lane, quote)
    }
    pub fn try_new(scope: &WorkScope, window: ResultWindowAlias) -> Result<Self, WorkError> {
        if !window.is_for_scope(scope)
            || window.class() == ResultWindowClass::Closing
            || window.has_streamed_computation_stamp()
        {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        Ok(Self {
            scope: scope.clone(),
            window,
            activity: std::sync::Arc::new(ResultCapacityActivity::default()),
        })
    }
    /// Retain the original active Root's data budget without reserving any
    /// computation position or deriving Native/manifest execution authority.
    pub fn from_root_owner(
        owner: &novarocks_workload_control::WorkOwner,
    ) -> Result<Self, WorkError> {
        let window = ResultWindowAlias::from_root_owner(owner)?;
        Ok(Self {
            scope: window.root_scope().clone(),
            window,
            activity: std::sync::Arc::new(ResultCapacityActivity::default()),
        })
    }
    pub fn from_internal_payload(
        root: &WorkScope,
        execution: &WorkScope,
        grant: &novarocks_workload_control::joint_admission::RetainedPayloadGrant,
    ) -> Result<Self, WorkError> {
        // WC validates the attribution before creating the checked pin. Do
        // not repeat a fallible check that could Drop that pin in a receiver
        // lock on cancellation between the two checks.
        let window = ResultWindowAlias::from_internal_payload(root, execution, grant)?;
        Ok(Self {
            scope: execution.clone(),
            window,
            activity: std::sync::Arc::new(ResultCapacityActivity::default()),
        })
    }
    /// Only the two original positive grants can attach manifest-binding
    /// authority. Retained aliases cannot mint it from an identity.
    pub fn from_internal_computation(
        root: &WorkScope,
        stage: &novarocks_workload_control::WorkOwner,
        payload: &novarocks_workload_control::joint_admission::RetainedPayloadGrant,
        compute: &novarocks_workload_control::joint_admission::InternalComputationGrant,
    ) -> Result<Self, WorkError> {
        let window = ResultWindowAlias::from_internal_computation(root, stage, payload, compute)?;
        Ok(Self {
            scope: stage.scope(),
            window,
            activity: std::sync::Arc::new(ResultCapacityActivity::default()),
        })
    }

    /// Keep this original payload and activity allocation after its actual
    /// computation exits. This projection carries no manifest-binding power.
    pub fn into_statement_retention(mut self) -> Result<Self, WorkError> {
        self.window = self.window.into_root_retention()?;
        self.scope = self.window.root_scope().clone();
        Ok(self)
    }

    /// Move the original positive Client computation into the existing shared
    /// activity allocation. Every fallible check precedes the first owner move.
    pub fn from_owned_client_computation_in(
        root: &WorkScope,
        stage: &novarocks_workload_control::WorkOwner,
        original: &ResultWindowAlias,
        payload: &novarocks_workload_control::joint_admission::RetainedPayloadGrant,
        source: &mut Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
        target: &mut Option<Self>,
    ) -> Result<(), WorkError> {
        if target.is_some() {
            return Err(WorkError::Conflict);
        }
        let compute = source.as_ref().ok_or(WorkError::Released)?;
        let window =
            ResultWindowAlias::from_client_computation(root, stage, original, payload, compute)?;
        Self::install_owned_streamed_computation_in(stage, window, source, target);
        Ok(())
    }

    /// Transfer the same positive Internal read computation, after the original
    /// Root payload, direct stage and actual position have all been validated.
    pub fn from_owned_internal_computation_in(
        root: &WorkScope,
        stage: &novarocks_workload_control::WorkOwner,
        payload: &novarocks_workload_control::joint_admission::RetainedPayloadGrant,
        source: &mut Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
        target: &mut Option<Self>,
    ) -> Result<(), WorkError> {
        if target.is_some() {
            return Err(WorkError::Conflict);
        }
        let compute = source.as_ref().ok_or(WorkError::Released)?;
        let window =
            ResultWindowAlias::from_streamed_internal_computation(root, stage, payload, compute)?;
        Self::install_owned_streamed_computation_in(stage, window, source, target);
        Ok(())
    }

    // Both public authors validate their distinct original grants before this
    // closed move. Retention aliases cannot install an owned computation here.
    fn install_owned_streamed_computation_in(
        stage: &novarocks_workload_control::WorkOwner,
        window: ResultWindowAlias,
        source: &mut Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
        target: &mut Option<Self>,
    ) {
        let mut activity = std::sync::Arc::new(ResultCapacityActivity::default());
        let state = std::sync::Arc::get_mut(&mut activity)
            .unwrap()
            .state
            .get_mut()
            .expect("new result activity lock");
        state.streamed = Some(StreamedComputationCustody {
            grant: source.take(),
            sealed: false,
            producer_exited: false,
            retiring: false,
            retired: false,
        });
        *target = Some(Self {
            scope: stage.scope(),
            window,
            activity,
        });
    }
    pub(crate) fn streamed_computation_producer(&self) -> Option<StreamedComputationProducer> {
        let state = self.activity.state.lock().expect("result activity lock");
        state.streamed.as_ref()?;
        Some(StreamedComputationProducer {
            activity: std::sync::Arc::clone(&self.activity),
            scope: self.scope.clone(),
        })
    }
    pub(crate) fn streamed_computation_cut_handle(&self) -> Option<StreamedComputationCutHandle> {
        let state = self.activity.state.lock().expect("result activity lock");
        state.streamed.as_ref()?;
        Some(StreamedComputationCutHandle {
            activity: std::sync::Arc::downgrade(&self.activity),
            scope: self.scope.clone(),
        })
    }
    pub fn scope(&self) -> &WorkScope {
        &self.scope
    }
    pub fn root_scope(&self) -> &WorkScope {
        self.window.root_scope()
    }
    pub fn is_root_backed_internal(&self) -> bool {
        self.window.is_root_backed_internal()
    }
    pub fn try_window_alias(&self) -> Result<ResultWindowAlias, WorkError> {
        self.window.try_clone()
    }
    pub fn try_clone(&self) -> Result<Self, WorkError> {
        Ok(Self {
            scope: self.scope.clone(),
            window: self.window.try_clone()?,
            activity: std::sync::Arc::clone(&self.activity),
        })
    }
    pub fn window_alias(&self) -> ResultWindowAlias {
        self.window.clone()
    }
    pub fn class(&self) -> ResultWindowClass {
        self.window.class()
    }

    /// Register before dispatch, so cancellation cannot race registration.
    /// This creates no allowance and reuses the original physical window.
    pub fn begin_result_activity(&self) -> Result<ResultCapacityActivityLease, WorkError> {
        if self.is_root_backed_internal() {
            return Err(WorkError::Conflict);
        }
        self.scope.check()?;
        let window = self.window.try_clone()?;
        let mut state = self.activity.state.lock().expect("result activity lock");
        if state
            .streamed
            .as_ref()
            .is_some_and(|streamed| streamed.sealed)
        {
            return Err(WorkError::Conflict);
        }
        state.count = state
            .count
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(ResultCapacityActivityLease {
            activity: std::sync::Arc::clone(&self.activity),
            window: Some(window),
            computation: None,
        })
    }

    /// The positive stage pin is supplied by the original installed closed
    /// batch. Payload retention alone never creates execution activity.
    pub fn begin_internal_result_activity(
        &self,
        computation: novarocks_workload_control::joint_admission::InternalComputationAlias,
    ) -> Result<ResultCapacityActivityLease, WorkError> {
        if !self.is_root_backed_internal() || !computation.is_for_scope(&self.scope) {
            return Err(WorkError::Conflict);
        }
        self.scope.check()?;
        let window = self.window.try_clone()?;
        let mut state = self.activity.state.lock().expect("result activity lock");
        if state
            .streamed
            .as_ref()
            .is_some_and(|streamed| streamed.sealed)
        {
            return Err(WorkError::Conflict);
        }
        state.count = state
            .count
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(ResultCapacityActivityLease {
            activity: std::sync::Arc::clone(&self.activity),
            window: Some(window),
            computation: Some(computation),
        })
    }

    /// A background phase calls this after closing its producer. It waits for
    /// actual input/unclaimed-output destruction, never for cancellation.
    /// Foreground coordinator cancellation must not call this blocking fence.
    pub fn wait_result_activities_exited(&self) {
        let mut state = self.activity.state.lock().expect("result activity lock");
        while state.count != 0 {
            state = self
                .activity
                .exited
                .wait(state)
                .expect("result activity lock");
        }
    }

    /// Async observation of the same actual-input/unclaimed-output counter.
    /// Close producers before awaiting: zero is not a new admission seal.
    pub async fn result_activities_exited(&self) {
        loop {
            let exited = self.activity.async_exited.notified();
            tokio::pin!(exited);
            // Register before inspecting count. A zero transition between the
            // inspection and await is retained by this registered waiter.
            exited.as_mut().enable();
            if self
                .activity
                .state
                .lock()
                .expect("result activity lock")
                .count
                == 0
            {
                return;
            }
            exited.await;
        }
    }

    /// A nested stage keeps its parent's whole envelope, with exact child
    /// attribution. No result-capacity admission is performed here.
    pub fn for_child(&self, child: &WorkScope) -> Result<Self, WorkError> {
        Ok(Self {
            scope: child.clone(),
            window: self.window.for_child(child)?,
            activity: std::sync::Arc::clone(&self.activity),
        })
    }
}

/// Execution inputs which must remain identical from planning through native
/// coordinator submission.
///
/// CLS-R2 boundary: request admission — deciding these inputs once per
/// statement — is frontend authority and moves there. The immutable value
/// itself stays with the aggregate package because `mv` and `connector` still
/// receive it as a parameter. Those consumers leave with CLS-R3 and CLS-R5.
#[derive(Clone)]
pub struct QueryExecutionContext {
    #[cfg(novarocks_dev_controls)]
    test_statement_nomination:
        Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
    result_capacity: Option<QueryResultCapacityBinding>,
    role: ClusterRole,
    topology: BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    optimizer_settings: SessionOptimizerSettings,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    fold_dependency_observer: Option<Arc<dyn SqlFoldDependencyObserver>>,
}

impl QueryExecutionContext {
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_statement_nomination(
        mut self,
        nomination: Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
    ) -> Result<Self, String> {
        nomination.validate().map_err(|error| error.to_string())?;
        if self
            .test_statement_nomination
            .as_ref()
            .is_some_and(|existing| existing != &nomination)
        {
            return Err("admitted context has a foreign statement nomination".into());
        }
        self.test_statement_nomination = Some(nomination);
        Ok(self)
    }
    #[cfg(novarocks_dev_controls)]
    pub fn test_statement_nomination(
        &self,
    ) -> Option<&Arc<novarocks_failpoint::target_binding::StatementWorkNomination>> {
        self.test_statement_nomination.as_ref()
    }

    pub fn new(
        role: ClusterRole,
        topology: BackendTopologySnapshot,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        mut optimizer_settings: SessionOptimizerSettings,
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    ) -> Self {
        if optimizer_settings.cbo_broadcast_backend_count.is_none()
            && optimizer_settings.effective_backend_count.is_none()
            && !topology.targets().is_empty()
        {
            optimizer_settings.effective_backend_count = Some(topology.targets().len() as f64);
        }
        Self {
            #[cfg(novarocks_dev_controls)]
            test_statement_nomination: None,
            result_capacity: None,
            role,
            topology,
            deadline,
            cancellation,
            optimizer_settings,
            sql_semantics,
            fold_dependency_observer: None,
        }
    }

    /// Attach the admitted statement's observer without changing its control facts.
    pub fn with_fold_dependency_observer(
        mut self,
        observer: Arc<dyn SqlFoldDependencyObserver>,
    ) -> Self {
        self.fold_dependency_observer = Some(observer);
        self
    }

    /// Borrow the same query-owned observer; absence preserves the original path.
    pub fn fold_dependency_observer(&self) -> Option<&Arc<dyn SqlFoldDependencyObserver>> {
        self.fold_dependency_observer.as_ref()
    }

    pub fn with_result_capacity(
        mut self,
        binding: QueryResultCapacityBinding,
    ) -> Result<Self, WorkError> {
        if self.result_capacity.is_some() {
            return Err(WorkError::Conflict);
        }
        binding.scope().check()?;
        self.result_capacity = Some(binding);
        Ok(self)
    }

    /// Derive an execution view of this same admitted statement. The caller
    /// must already hold the original positive payload and actual computation
    /// grants; a payload-only alias cannot authorize a new execution stage.
    pub fn for_internal_computation(
        &self,
        binding: QueryResultCapacityBinding,
    ) -> Result<Self, WorkError> {
        let original = self.result_capacity.as_ref().ok_or(WorkError::Conflict)?;
        if !binding.is_root_backed_internal()
            || !original.window.is_for_root_scope(binding.root_scope())
            || binding.window.internal_manifest_binding()?.is_none()
            || (original.is_root_backed_internal()
                && !original.window.shares_capacity_with(&binding.window))
        {
            return Err(WorkError::Conflict);
        }
        original.root_scope().check()?;
        binding.scope().check()?;
        let mut execution = self.clone();
        execution.result_capacity = Some(binding);
        Ok(execution)
    }

    /// Continue business work under the same admitted statement and retained
    /// payload. An old computation may have exited; no new computation is
    /// authorized by this Root-only retention projection.
    pub fn for_statement_retention(
        &self,
        binding: QueryResultCapacityBinding,
    ) -> Result<Self, WorkError> {
        let original = self.result_capacity.as_ref().ok_or(WorkError::Conflict)?;
        if !binding.is_root_backed_internal()
            || !binding.window.is_for_scope(binding.root_scope())
            || !original.window.is_for_root_scope(binding.root_scope())
            || binding.window.internal_manifest_binding()?.is_some()
            || (original.is_root_backed_internal()
                && !original.window.shares_capacity_with(&binding.window))
        {
            return Err(WorkError::Conflict);
        }
        binding.scope().check()?;
        let mut execution = self.clone();
        execution.result_capacity = Some(binding);
        Ok(execution)
    }

    pub fn result_capacity(&self) -> Option<&QueryResultCapacityBinding> {
        self.result_capacity.as_ref()
    }

    pub const fn role(&self) -> ClusterRole {
        self.role
    }

    pub fn topology(&self) -> &BackendTopologySnapshot {
        &self.topology
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }

    /// Settings frozen with the request so DML and coordinator-adjacent plan
    /// construction never consult process- or thread-local session state.
    pub fn sql_semantics(&self) -> &novarocks_sql::sql_mode::SqlSemanticSettings {
        &self.sql_semantics
    }

    pub fn optimizer_settings(&self) -> &SessionOptimizerSettings {
        &self.optimizer_settings
    }
}

/// Complete immutable statement context.  The frontend application admits a
/// statement once, then consumers receive only narrow projections.
#[derive(Clone)]
pub struct RequestContext {
    session: RequestSessionContext,
    execution: QueryExecutionContext,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
impl RequestContext {
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_statement_nomination(
        mut self,
        nomination: Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
    ) -> Result<Self, String> {
        self.execution = self.execution.with_test_statement_nomination(nomination)?;
        Ok(self)
    }

    pub fn new(session: RequestSessionContext, execution: QueryExecutionContext) -> Self {
        Self { session, execution }
    }

    /// Attach only this request's diagnostic observer; session facts remain admitted.
    pub fn with_fold_dependency_observer(
        mut self,
        observer: Arc<dyn SqlFoldDependencyObserver>,
    ) -> Self {
        self.execution = self.execution.with_fold_dependency_observer(observer);
        self
    }

    /// Freeze one statement's context.
    ///
    /// The cost budget arrives inside `optimizer_settings`: the frontend query
    /// service and the MV worker each carry the value resolved from `[runtime]`
    /// at composition, so admission never reads a process-global configuration.
    /// A settings value of `None` means "no budget", which leaves optimizer
    /// costing on its profile default.
    pub fn admit(admission: RequestAdmission) -> Self {
        let statement = StatementAdmissionContext::new(
            admission.current_catalog,
            admission.current_database,
            admission.role,
            admission.deadline,
            admission.cancellation,
            admission.optimizer_settings,
            admission.sql_semantics,
            admission.presentation,
        );
        statement.for_topology(admission.topology)
    }

    /// Bind admission's runtime sidecar before source preparation begins.
    pub fn with_result_capacity(
        mut self,
        binding: QueryResultCapacityBinding,
    ) -> Result<Self, WorkError> {
        self.execution = self.execution.with_result_capacity(binding)?;
        Ok(self)
    }

    pub fn session(&self) -> &RequestSessionContext {
        &self.session
    }

    pub fn execution(&self) -> &QueryExecutionContext {
        &self.execution
    }

    pub fn preparation(&self) -> QueryPreparationContext<'_> {
        QueryPreparationContext {
            session: &self.session,
            execution: &self.execution,
        }
    }
}

/// Borrowed projection used by SQL preparation and optimizer layers.
pub struct QueryPreparationContext<'a> {
    session: &'a RequestSessionContext,
    execution: &'a QueryExecutionContext,
}

impl<'a> QueryPreparationContext<'a> {
    pub fn session(&self) -> &'a RequestSessionContext {
        self.session
    }

    pub fn execution(&self) -> &'a QueryExecutionContext {
        self.execution
    }
}

#[cfg(test)]
mod tests {
    use novarocks_execution_contract::{
        AdmissionEpochCapability, BackendProcessDescriptor, RuntimeEndpoint,
    };
    use novarocks_types::BackendProcessId;

    use super::*;
    use crate::api::LiveBackendTarget;
    use crate::cancellation::QueryCancellationSource;

    fn capacity_control() -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        use novarocks_workload_control::{
            ResourceConfig, ResultCapacityConfig, WorkloadConfig, WorkloadControl,
        };
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        (control, capacity)
    }

    #[test]
    fn runtime_capacity_is_explicit_and_survives_execution_clones() {
        use novarocks_workload_control::{WorkClass, WorkRequest};
        let (control, capacity) = capacity_control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ))
        .with_result_capacity(
            QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias())
                .unwrap(),
        )
        .unwrap();
        let execution = context.execution().clone();
        assert!(
            execution
                .clone()
                .with_result_capacity(execution.result_capacity().unwrap().clone(),)
                .is_err()
        );
        let replacement = StatementAdmissionContext::new(
            None,
            "db1".to_string(),
            execution.role(),
            execution.deadline(),
            execution.cancellation().clone(),
            execution.optimizer_settings().clone(),
            execution.sql_semantics().clone(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        )
        .for_topology(topology(8, 1));
        assert!(replacement.execution().result_capacity().is_none());
        assert_eq!(
            execution.result_capacity().unwrap().class(),
            ResultWindowClass::Internal
        );
        drop(context);
        drop(window);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(execution);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn runtime_capacity_rejects_foreign_scope_and_delegates_without_new_position() {
        use novarocks_workload_control::{WorkClass, WorkRequest};
        let (control, capacity) = capacity_control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let binding =
            QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias())
                .unwrap();
        let (foreign, _) = capacity_control();
        let other = foreign
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        assert!(
            QueryResultCapacityBinding::try_new(&other.owner.scope(), window.retain_alias())
                .is_err()
        );
        let child = root
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let delegated = binding.for_child(&child.scope()).unwrap();
        assert!(delegated.window_alias().is_for_scope(&child.scope()));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(binding);
        drop(window);
        child.complete();
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        assert!(
            QueryResultCapacityBinding::try_new(&other.owner.scope(), delegated.window_alias())
                .is_err()
        );
        drop(delegated);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        other.owner.complete();
        other.business.release();
    }

    fn topology(revision: u64, backend_count: usize) -> BackendTopologySnapshot {
        let targets = (0..backend_count)
            .map(|backend_idx| {
                LiveBackendTarget::new(
                    backend_idx,
                    BackendProcessDescriptor::try_new(
                        BackendProcessId::new_v7(),
                        RuntimeEndpoint::new(
                            "127.0.0.1",
                            9030 + i32::try_from(backend_idx).expect("fixture backend index"),
                        )
                        .expect("valid loopback endpoint"),
                        RuntimeEndpoint::new(format!("control-{backend_idx}.test.invalid"), 19061)
                            .expect("valid control endpoint"),
                        "test-deployment",
                        "test-build",
                        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                        4096,
                    )
                    .expect("valid test descriptor"),
                    AdmissionEpochCapability::try_from_bytes(
                        [u8::try_from(backend_idx + 1).expect("fixture backend index"); 16],
                    )
                    .expect("nonzero epoch"),
                )
            })
            .collect();
        BackendTopologySnapshot::try_new(revision, targets).expect("valid topology")
    }

    #[test]
    fn projections_share_one_cancellation_and_topology_identity() {
        let cancellation = QueryCancellationSource::new();
        let snapshot = BackendTopologySnapshot::try_new(
            9,
            vec![LiveBackendTarget::new(
                7,
                BackendProcessDescriptor::try_new(
                    BackendProcessId::new_v7(),
                    RuntimeEndpoint::new("127.0.0.1", 9030).expect("valid loopback endpoint"),
                    RuntimeEndpoint::new("control.test.invalid", 19061)
                        .expect("valid control endpoint"),
                    "test-deployment",
                    "test-build",
                    novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                    4096,
                )
                .expect("valid test descriptor"),
                AdmissionEpochCapability::try_from_bytes([0x61; 16]).expect("nonzero epoch"),
            )],
        )
        .expect("valid snapshot");
        let context = RequestContext::new(
            RequestSessionContext::new(
                Some("iceberg".to_string()),
                "db1".to_string(),
                SessionOptimizerSettings::default(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
            ),
            QueryExecutionContext::new(
                ClusterRole::Fe,
                snapshot,
                None,
                cancellation.view(),
                SessionOptimizerSettings::default(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            ),
        );

        assert_eq!(context.execution().topology().revision(), 9);
        assert_eq!(context.session().current_catalog(), Some("iceberg"));
        assert!(!context.execution().cancellation().is_cancelled());
        cancellation.request(crate::cancellation::QueryCancellationReason::ClientDisconnected);
        assert!(context.execution().cancellation().is_cancelled());
    }

    #[test]
    fn admission_copies_session_settings_and_preserves_deadline() {
        let deadline = Instant::now();
        let mut admitted_settings = SessionOptimizerSettings {
            enable_eliminate_agg: true,
            cbo_broadcast_backend_count: Some(3.0),
            ..SessionOptimizerSettings::default()
        };
        let context = RequestContext::new(
            RequestSessionContext::new(
                None,
                "db1".to_string(),
                admitted_settings.clone(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
            ),
            QueryExecutionContext::new(
                ClusterRole::Fe,
                BackendTopologySnapshot::empty(4),
                Some(deadline),
                QueryCancellationSource::new().view(),
                admitted_settings.clone(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            ),
        );

        admitted_settings.enable_eliminate_agg = false;
        admitted_settings.cbo_broadcast_backend_count = Some(99.0);

        assert!(context.session().optimizer_settings().enable_eliminate_agg);
        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .cbo_broadcast_backend_count,
            Some(3.0)
        );
        assert_eq!(context.execution().deadline(), Some(deadline));
        assert!(context.execution().topology().targets().is_empty());
    }

    /// Load-bearing for the sealed-description rebind: a replacement round
    /// reuses the first round's plan, so the count that shaped that plan must
    /// not be re-derived from the new topology. `effective_backend_count` is
    /// not a mere cost hint -- it gates BroadcastJoin feasibility, so a
    /// re-derived value can flip a join to shuffle and invalidate the very
    /// description the retry is rebinding.
    #[test]
    fn statement_admission_freezes_first_topology_count_across_later_rounds() {
        let first = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ));
        let statement = StatementAdmissionContext::new(
            None,
            "db1".to_string(),
            first.execution().role(),
            first.execution().deadline(),
            first.execution().cancellation().clone(),
            first.execution().optimizer_settings().clone(),
            first.execution().sql_semantics().clone(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        );
        let replacement = statement.for_topology(topology(8, 1));

        assert_eq!(
            first
                .execution()
                .optimizer_settings()
                .effective_backend_count,
            Some(3.0)
        );
        assert_eq!(
            replacement
                .execution()
                .optimizer_settings()
                .effective_backend_count,
            Some(3.0)
        );
        assert_eq!(
            first.execution().optimizer_settings(),
            replacement.execution().optimizer_settings()
        );
    }

    #[test]
    fn explicit_session_backend_count_wins_over_admitted_topology() {
        let first = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings {
                cbo_broadcast_backend_count: Some(11.0),
                ..SessionOptimizerSettings::default()
            },
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ));
        let statement = StatementAdmissionContext::new(
            None,
            "db1".to_string(),
            first.execution().role(),
            first.execution().deadline(),
            first.execution().cancellation().clone(),
            first.execution().optimizer_settings().clone(),
            first.execution().sql_semantics().clone(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        );
        let replacement = statement.for_topology(topology(8, 1));

        for context in [&first, &replacement] {
            assert_eq!(
                context
                    .execution()
                    .optimizer_settings()
                    .cbo_broadcast_backend_count,
                Some(11.0)
            );
            assert_eq!(
                context
                    .execution()
                    .optimizer_settings()
                    .effective_backend_count,
                None
            );
        }
    }

    #[test]
    fn existing_effective_backend_count_is_not_replaced_at_admission() {
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings {
                effective_backend_count: Some(5.0),
                ..SessionOptimizerSettings::default()
            },
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ));

        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .effective_backend_count,
            Some(5.0)
        );
    }

    #[test]
    fn admission_carries_the_optimizer_query_memory_budget_verbatim() {
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(7),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings {
                optimizer_query_mem_limit_bytes: Some(512.0 * 1024.0 * 1024.0),
                ..SessionOptimizerSettings::default()
            },
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ));

        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .optimizer_query_mem_limit_bytes,
            Some(512.0 * 1024.0 * 1024.0)
        );
    }

    #[test]
    fn admission_without_a_budget_leaves_costing_on_its_profile_default() {
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(7),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ));

        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .optimizer_query_mem_limit_bytes,
            None
        );
    }

    #[test]
    fn sql_mode_is_frozen_in_both_request_projections_and_topology_retries() {
        let mode = novarocks_sql::sql_mode::SqlSemanticSettings::default().with_sql_mode(
            novarocks_sql::sql_mode::SqlMode::from_assignment(
                "GROUP_CONCAT_LEGACY,ALLOW_THROW_EXCEPTION,ERROR_IF_OVERFLOW",
            ),
        );
        let first = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_owned(),
            ClusterRole::Fe,
            topology(7, 3),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
            mode.clone(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ));
        assert_eq!(first.session().sql_semantics(), &mode);
        assert_eq!(first.execution().sql_semantics(), &mode);
        let statement = StatementAdmissionContext::new(
            None,
            "db1".to_owned(),
            first.execution().role(),
            first.execution().deadline(),
            first.execution().cancellation().clone(),
            first.execution().optimizer_settings().clone(),
            first.session().sql_semantics().clone(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        );
        let replacement = statement.for_topology(topology(8, 1));
        assert_eq!(replacement.session().sql_semantics(), &mode);
        assert_eq!(replacement.execution().sql_semantics(), &mode);
    }
}

#[cfg(test)]
mod internal_root_tests;
