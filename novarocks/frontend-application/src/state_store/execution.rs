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

//! Original StateStore calls in the existing Host or statement custody.
//! Borrowers never own a physical call. The same owner polls this inventory
//! after its product future ends and keeps it through rollback and evidence.

use crate::query_execution::maintenance::io_admission::ExistingMetadataObligation;
use futures::task::AtomicWaker;
use novarocks_failure_contract::FailureProjection;
use novarocks_query_application::cpu::{
    QueryBlockingActualExitFailure, QueryBlockingExecutor, QueryBlockingFailure,
    QueryBlockingOriginal, QueryBlockingOriginalExecutor, QueryBlockingPreparedJob,
    QueryBlockingPublishRefusal, QueryBlockingReceipt, QueryBlockingReservation,
    QueryBlockingReserveRefusal,
};
use novarocks_state_store_api::{
    InstanceScope, StateStoreBlockingExit as Exit, StateStoreBlockingOrigin,
    StateStoreBlockingOriginal, StateStoreBlockingPhase as Phase,
    StateStoreBlockingRequest as Request, StateStoreBlockingSlot,
    StateStoreBlockingSourceVocabulary as Vocabulary, StateStoreError, StateStoreErrorKind,
    StateStoreExecutionContext, StateStoreExecutionFailure as Failure, StateStoreExecutionPort,
    StateStoreExecutionRequests, StateStoreReadExecution,
};
use novarocks_workload_control::WorkScope;
use std::{
    collections::BTreeMap,
    future::Future,
    ops::Bound::{Excluded, Included},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Instant,
};

/// The actual provider vocabulary comes from the selected immutable factory;
/// neither a request nor its dynamic origin can extend the allowed types.
enum Purpose {
    Opening,
    BootstrapRead {
        instance: InstanceScope,
    },
    ClosingInstance {
        instance: InstanceScope,
    },
    InstanceEvidence {
        instance: InstanceScope,
    },
    Instance {
        instance: InstanceScope,
        root: WorkScope,
    },
}
struct PhaseCall {
    original: Arc<dyn StateStoreBlockingOriginal>,
    request: Request,
}
impl QueryBlockingOriginal for PhaseCall {
    fn run_original(&self) {
        self.original.run_original_phase(self.request);
    }
}
struct Wake {
    owner: AtomicWaker,
    dirty: AtomicBool,
}
impl Wake {
    fn notify(&self) {
        self.dirty.store(true, Ordering::Release);
        self.owner.wake();
    }
}
impl std::task::Wake for Wake {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}
struct Inventory {
    accepting: bool,
    entries: BTreeMap<usize, Arc<Cell>>,
    cursor: Option<usize>,
    high: Option<usize>,
}
struct Port {
    vocabulary: Vocabulary,
    purpose: Purpose,
    executor: QueryBlockingOriginalExecutor<PhaseCall>,
    wake: Arc<Wake>,
    inventory: Mutex<Inventory>,
}
/// Unique strong owner. Moving this value from Action to its same Residual
/// preserves every original request, queue position, result and exit receipt.
pub(crate) struct StateStoreOriginalOwner {
    port: Arc<Port>,
    policy: novarocks_state_store_runtime::StateStoreRunPolicy,
    close_requested: bool,
    retry_round: usize,
    retry: Option<Pin<Box<tokio::time::Sleep>>>,
}
impl StateStoreOriginalOwner {
    pub(crate) fn opening(
        executor: &QueryBlockingExecutor,
        vocabulary: Vocabulary,
        policy: novarocks_state_store_runtime::StateStoreRunPolicy,
    ) -> Result<Self, Failure> {
        Self::new(executor, vocabulary, Purpose::Opening, policy)
    }
    /// Finite Host initialization reads of the exact installed instance.
    /// No Root is minted before serving admission is ready. The Host must
    /// retain this owner, poll it beside the bootstrap borrower, then close it.
    pub(crate) fn bootstrap_read(
        executor: &QueryBlockingExecutor,
        vocabulary: Vocabulary,
        instance: InstanceScope,
        policy: novarocks_state_store_runtime::StateStoreRunPolicy,
    ) -> Result<Self, Failure> {
        Self::new(
            executor,
            vocabulary,
            Purpose::BootstrapRead { instance },
            policy,
        )
    }
    /// Host custody for the same instance's physical destruction. This purpose
    /// accepts only the selected lifecycle author and no transaction work.
    pub(crate) fn closing_instance(
        executor: &QueryBlockingExecutor,
        vocabulary: Vocabulary,
        instance: InstanceScope,
        policy: novarocks_state_store_runtime::StateStoreRunPolicy,
    ) -> Result<Self, Failure> {
        Self::new(
            executor,
            vocabulary,
            Purpose::ClosingInstance { instance },
            policy,
        )
    }
    /// Host evidence for issued attempts of this exact installed instance.
    /// This purpose cannot open a store or begin/read/write a transaction.
    pub(crate) fn instance_evidence(
        executor: &QueryBlockingExecutor,
        vocabulary: Vocabulary,
        instance: InstanceScope,
        policy: novarocks_state_store_runtime::StateStoreRunPolicy,
    ) -> Result<Self, Failure> {
        Self::new(
            executor,
            vocabulary,
            Purpose::InstanceEvidence { instance },
            policy,
        )
    }
    /// A cadence may poll the original inventory, but never retain its owner.
    /// Host custody must already hold this owner before lending this handle.
    pub(crate) fn progress_handle(&self) -> StateStoreOriginalProgress {
        StateStoreOriginalProgress {
            port: Arc::downgrade(&self.port),
        }
    }
    pub(crate) fn for_root(
        executor: &QueryBlockingExecutor,
        vocabulary: Vocabulary,
        instance: InstanceScope,
        root: WorkScope,
        policy: novarocks_state_store_runtime::StateStoreRunPolicy,
    ) -> Result<Self, Failure> {
        root.check().map_err(failure)?;
        Self::new(
            executor,
            vocabulary,
            Purpose::Instance { instance, root },
            policy,
        )
    }
    fn new(
        executor: &QueryBlockingExecutor,
        vocabulary: Vocabulary,
        purpose: Purpose,
        policy: novarocks_state_store_runtime::StateStoreRunPolicy,
    ) -> Result<Self, Failure> {
        let executor = executor
            .original_executor::<PhaseCall>()
            .map_err(|e| failure(PhysicalFailure::Reserve(e)))?;
        Ok(Self {
            policy,
            close_requested: false,
            retry_round: 0,
            retry: None,
            port: Arc::new(Port {
                vocabulary,
                purpose,
                executor,
                wake: Arc::new(Wake {
                    owner: AtomicWaker::new(),
                    dirty: AtomicBool::new(false),
                }),
                inventory: Mutex::new(Inventory {
                    accepting: true,
                    entries: BTreeMap::new(),
                    cursor: None,
                    high: None,
                }),
            }),
        })
    }
    pub(crate) fn context(&self, deadline: Instant) -> StateStoreExecutionContext {
        let port: Arc<dyn StateStoreExecutionPort> = self.port.clone();
        StateStoreExecutionContext::new(&port, deadline)
    }
    /// The selected instance supplies its existing physical transaction limit.
    /// This view holds no source, Root, grant, or physical position.
    pub(crate) fn read_execution(
        &self,
        transaction_deadline: std::time::Duration,
    ) -> Result<StateStoreOriginalReadExecution, Failure> {
        if transaction_deadline.is_zero() {
            return Err(protocol(
                "StateStore read transaction deadline must be positive",
            ));
        }
        Ok(StateStoreOriginalReadExecution {
            progress: self.progress_handle(),
            transaction_deadline,
            statement_deadline: None,
        })
    }
    /// Root-only request factory. Host bootstrap deliberately exposes only
    /// `read_execution`; an absent statement deadline adds no artificial cap.
    pub(crate) fn execution_requests(
        &self,
        transaction_deadline: std::time::Duration,
        statement_deadline: Option<Instant>,
    ) -> Result<StateStoreOriginalRequests, Failure> {
        if !matches!(&self.port.purpose, Purpose::Instance { .. }) {
            return Err(protocol(
                "StateStore request factory requires its original Root",
            ));
        }
        let mut read = self.read_execution(transaction_deadline)?;
        read.statement_deadline = statement_deadline;
        Ok(StateStoreOriginalRequests {
            read,
            policy: self.policy,
        })
    }

    /// The same physical inventory borrows a previously registered metadata
    /// obligation. The request wrapper retains neither this owner nor MIO.
    pub(crate) fn existing_obligation_requests(
        &self,
        transaction_deadline: std::time::Duration,
        obligation: ExistingMetadataObligation,
    ) -> Result<Arc<dyn StateStoreExecutionRequests>, Failure> {
        if transaction_deadline.is_zero() {
            return Err(protocol(
                "StateStore read transaction deadline must be positive",
            ));
        }
        let control = RequestControl::ExistingObligation(obligation);
        if let Some(cause) = control.stop(&self.port, control.deadline().unwrap()) {
            return Err(cause);
        }
        if !self
            .port
            .inventory
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accepting
        {
            return Err(protocol("original StateStore request custody is closing"));
        }
        let requests: Arc<ExistingObligationRequests> =
            Arc::new_cyclic(|weak| ExistingObligationRequests {
                this: weak.clone(),
                port: Arc::downgrade(&self.port),
                control,
                transaction_deadline,
                policy: self.policy,
            });
        Ok(requests)
    }
    pub(crate) fn request_close(&self) {
        let entries = {
            let mut inventory = self
                .port
                .inventory
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            inventory.accepting = false;
            inventory.entries.values().cloned().collect::<Vec<_>>()
        };
        for entry in entries {
            entry.original.request_close();
        }
        self.port.wake.notify();
    }
    pub(crate) fn poll_progress(&self, cx: &mut Context<'_>) -> Result<bool, Failure> {
        poll_originals(&self.port, cx)
    }
    /// Observe the current physical cut after the product borrower has ended.
    /// An idle unresolved Original can still own evidence and require later
    /// close; this observation does not complete or remove that responsibility.
    pub(crate) fn poll_actual_quiescent(&self, cx: &mut Context<'_>) -> Result<bool, Failure> {
        self.poll_progress(cx)?;
        let entries = self
            .port
            .inventory
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            if !entry.actual_quiescent()? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub(crate) fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Failure>> {
        if !self.close_requested {
            self.close_requested = true;
            self.request_close();
        }
        if let Some(retry) = self.retry.as_mut()
            && retry.as_mut().poll(cx).is_ready()
        {
            self.retry.take();
            self.request_close();
        }
        match self.poll_progress(cx) {
            Ok(true) => {
                self.retry.take();
                Poll::Ready(Ok(()))
            }
            Ok(false) => {
                if self.retry.is_none() {
                    self.retry_round = self.retry_round.saturating_add(1);
                    self.retry = Some(Box::pin(tokio::time::sleep(
                        self.policy.backoff_after(self.retry_round),
                    )));
                    let _ = self.retry.as_mut().unwrap().as_mut().poll(cx);
                }
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
    pub(crate) fn local_health_failure(&self) -> Option<Failure> {
        let entries = self
            .port
            .inventory
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .values()
            .cloned()
            .collect::<Vec<_>>();
        entries.iter().find_map(|cell| {
            cell.body
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .and_then(Body::reportable_health)
        })
    }
}
/// Weak polling capability, never source custody or execution permission.
#[derive(Clone)]
pub(crate) struct StateStoreOriginalProgress {
    port: Weak<Port>,
}
impl StateStoreOriginalProgress {
    pub(crate) fn context(&self, deadline: Instant) -> Result<StateStoreExecutionContext, Failure> {
        let port = self
            .port
            .upgrade()
            .ok_or_else(|| protocol("original StateStore Host custody is unavailable"))?;
        let port: Arc<dyn StateStoreExecutionPort> = port;
        Ok(StateStoreExecutionContext::new(&port, deadline))
    }
    pub(crate) fn poll_progress(&self, cx: &mut Context<'_>) -> Result<bool, Failure> {
        let port = self
            .port
            .upgrade()
            .ok_or_else(|| protocol("original StateStore Host custody is unavailable"))?;
        poll_originals(&port, cx)
    }
}
/// Explicit per-transaction read timing for Host/Controller reads, unlike an
/// absolute statement request. The strong owner remains in its original slot.
#[derive(Clone)]
pub(crate) struct StateStoreOriginalReadExecution {
    progress: StateStoreOriginalProgress,
    transaction_deadline: std::time::Duration,
    statement_deadline: Option<Instant>,
}
impl StateStoreOriginalReadExecution {
    fn context_for(
        &self,
        timeout: std::time::Duration,
    ) -> Result<StateStoreExecutionContext, Failure> {
        let port = self
            .progress
            .port
            .upgrade()
            .ok_or_else(|| protocol("original StateStore read custody is unavailable"))?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| protocol("StateStore operation deadline is not representable"))?;
        let deadline = self
            .statement_deadline
            .map_or(deadline, |cap| cap.min(deadline));
        if let Some(cause) = port.business_stop(deadline) {
            return Err(cause);
        }
        if !port
            .inventory
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accepting
        {
            return Err(protocol("original StateStore read custody is closing"));
        }
        let port: Arc<dyn StateStoreExecutionPort> = port;
        Ok(StateStoreExecutionContext::new(&port, deadline))
    }
}
impl StateStoreReadExecution for StateStoreOriginalReadExecution {
    fn read_transaction_context(&self) -> Result<StateStoreExecutionContext, Failure> {
        self.context_for(self.transaction_deadline)
    }
}
/// Weak request view only; the same Read tail retains every physical source.
#[derive(Clone)]
pub(crate) struct StateStoreOriginalRequests {
    read: StateStoreOriginalReadExecution,
    policy: novarocks_state_store_runtime::StateStoreRunPolicy,
}
impl StateStoreReadExecution for StateStoreOriginalRequests {
    fn read_transaction_context(&self) -> Result<StateStoreExecutionContext, Failure> {
        self.read.read_transaction_context()
    }
}
impl StateStoreExecutionRequests for StateStoreOriginalRequests {
    fn write_operation_context(&self) -> Result<StateStoreExecutionContext, Failure> {
        self.read.context_for(self.policy.operation_timeout())
    }
}

/// One borrowed wrapper backs both request factories and their weak contexts.
/// Its self-reference and physical Port reference are weak; Cells retain only
/// the captured control, never a second owner or inventory.
struct ExistingObligationRequests {
    this: Weak<Self>,
    port: Weak<Port>,
    control: RequestControl,
    transaction_deadline: std::time::Duration,
    policy: novarocks_state_store_runtime::StateStoreRunPolicy,
}
impl ExistingObligationRequests {
    fn context_for(
        &self,
        timeout: std::time::Duration,
    ) -> Result<StateStoreExecutionContext, Failure> {
        let port = self
            .port
            .upgrade()
            .ok_or_else(|| protocol("original StateStore obligation custody is unavailable"))?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| protocol("StateStore operation deadline is not representable"))?
            .min(self.control.deadline().expect("same obligation control"));
        if let Some(cause) = self.control.stop(&port, deadline) {
            return Err(cause);
        }
        if !port
            .inventory
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accepting
        {
            return Err(protocol("original StateStore request custody is closing"));
        }
        let wrapper: Arc<dyn StateStoreExecutionPort> = self
            .this
            .upgrade()
            .ok_or_else(|| protocol("original StateStore request wrapper is unavailable"))?;
        Ok(StateStoreExecutionContext::new(&wrapper, deadline))
    }
}
impl StateStoreReadExecution for ExistingObligationRequests {
    fn read_transaction_context(&self) -> Result<StateStoreExecutionContext, Failure> {
        self.context_for(self.transaction_deadline)
    }
}
impl StateStoreExecutionRequests for ExistingObligationRequests {
    fn write_operation_context(&self) -> Result<StateStoreExecutionContext, Failure> {
        self.context_for(self.policy.operation_timeout())
    }
}
impl StateStoreExecutionPort for ExistingObligationRequests {
    fn bind_original_in(
        &self,
        original: &Arc<dyn StateStoreBlockingOriginal>,
        deadline: Instant,
        target: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), Failure> {
        let port = self
            .port
            .upgrade()
            .ok_or_else(|| protocol("original StateStore obligation custody is unavailable"))?;
        port.bind_original_with_control_in(original, deadline, self.control.clone(), target)
    }
    fn request_progress(&self) {
        if let Some(port) = self.port.upgrade() {
            port.wake.notify();
        }
    }
}

#[derive(Clone)]
enum RequestControl {
    Business,
    ExistingObligation(ExistingMetadataObligation),
}
impl RequestControl {
    fn deadline(&self) -> Option<Instant> {
        match self {
            Self::Business => None,
            Self::ExistingObligation(token) => Some(token.deadline()),
        }
    }
    fn stop(&self, port: &Port, deadline: Instant) -> Option<Failure> {
        match self {
            Self::Business => port.business_stop(deadline),
            Self::ExistingObligation(token) => {
                let Purpose::Instance { root, .. } = &port.purpose else {
                    return Some(protocol("metadata obligation requires the same Root port"));
                };
                token
                    .check_active(root)
                    .err()
                    .or_else(|| deadline_failure(deadline))
            }
        }
    }
    fn wait(&self, port: &Port, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let root = match (self, &port.purpose) {
            (Self::Business, Purpose::Instance { root, .. }) => Some(root.clone()),
            _ => None,
        };
        let control = self.clone();
        Box::pin(async move {
            let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
            let stopped = async move {
                match control {
                    Self::ExistingObligation(token) => token.stopped().await,
                    Self::Business => match root {
                        Some(root) => {
                            if let Ok(cancel) = root.cancellation() {
                                cancel.cancelled().await;
                            }
                        }
                        None => std::future::pending().await,
                    },
                }
            };
            tokio::select! { _ = sleep => {}, _ = stopped => {} }
        })
    }
}

fn deadline_failure(deadline: Instant) -> Option<Failure> {
    (Instant::now() >= deadline).then(|| {
        StateStoreError::new(
            StateStoreErrorKind::DeadlineExceeded,
            "state store original deadline exceeded",
        )
        .into_original_failure()
    })
}
/// Cooperative batches bound one turn's observation cost, never admission
/// or retained responsibility. All callers poll this same original inventory.
fn poll_originals(port: &Arc<Port>, cx: &mut Context<'_>) -> Result<bool, Failure> {
    port.wake.owner.register(cx.waker());
    let (batch, more) = {
        let mut inventory = port.inventory.lock().unwrap_or_else(|p| p.into_inner());
        if inventory.high.is_none() {
            port.wake.dirty.swap(false, Ordering::AcqRel);
            inventory.high = inventory.entries.last_key_value().map(|(key, _)| *key);
            inventory.cursor = None;
        }
        let Some(high) = inventory.high else {
            return Ok(true);
        };
        let batch = match inventory.cursor {
            Some(cursor) => inventory.entries.range((Excluded(cursor), Included(high))),
            None => inventory.entries.range(..=high),
        }
        .take(32)
        .map(|(key, cell)| (*key, cell.clone()))
        .collect::<Vec<_>>();
        inventory.cursor = batch.last().map(|(key, _)| *key).or(inventory.cursor);
        let more = inventory.cursor.is_some_and(|cursor| {
            inventory
                .entries
                .range((Excluded(cursor), Included(high)))
                .next()
                .is_some()
        });
        if !more {
            inventory.high = None;
            inventory.cursor = None;
        }
        (batch, more)
    };
    let owner_waker = Waker::from(port.wake.clone());
    let mut owner_cx = Context::from_waker(&owner_waker);
    let mut first_failure = None;
    for (key, cell) in batch {
        match cell.poll_original(&mut owner_cx, port) {
            Ok(true) => {
                let retired = {
                    let mut inventory = port.inventory.lock().unwrap_or_else(|p| p.into_inner());
                    inventory.entries.remove(&key)
                };
                drop(retired);
            }
            Ok(false) => {}
            Err(error) => {
                first_failure.get_or_insert(error);
            }
        }
    }
    if more || port.wake.dirty.load(Ordering::Acquire) {
        cx.waker().wake_by_ref();
    }
    if let Some(error) = first_failure {
        return Err(error);
    }
    Ok(port
        .inventory
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entries
        .is_empty())
}
fn protocol(message: &'static str) -> Failure {
    StateStoreError::new(StateStoreErrorKind::Internal, message).into_original_failure()
}
impl Port {
    fn validate(
        &self,
        original: &dyn StateStoreBlockingOriginal,
        phase: Option<Phase>,
    ) -> Result<(), Failure> {
        let actual = original.type_id();
        let origin = original.origin();
        let valid = match &self.purpose {
            Purpose::Opening => {
                actual == self.vocabulary.open_type()
                    && origin == StateStoreBlockingOrigin::Opening(self.vocabulary.provider())
                    && phase.is_none_or(|p| matches!(p, Phase::Open | Phase::Close))
            }
            Purpose::BootstrapRead { instance } => {
                actual == self.vocabulary.transaction_type()
                    && origin == StateStoreBlockingOrigin::Instance(*instance)
                    && phase.is_none_or(|p| {
                        matches!(
                            p,
                            Phase::BeginRead
                                | Phase::Get
                                | Phase::Range
                                | Phase::Rollback
                                | Phase::Close
                        )
                    })
            }
            Purpose::ClosingInstance { instance } => {
                actual == self.vocabulary.open_type()
                    && origin == StateStoreBlockingOrigin::Instance(*instance)
                    && phase.is_none_or(|p| p == Phase::Close)
            }
            Purpose::InstanceEvidence { instance } => {
                actual == self.vocabulary.evidence_type()
                    && origin == StateStoreBlockingOrigin::Instance(*instance)
                    && phase.is_none_or(|p| {
                        matches!(
                            p,
                            Phase::EvidenceRead | Phase::EvidenceRelease | Phase::Close
                        )
                    })
            }
            Purpose::Instance { instance, .. } => {
                origin == StateStoreBlockingOrigin::Instance(*instance)
                    && ((actual == self.vocabulary.transaction_type()
                        && phase.is_none_or(|p| !matches!(p, Phase::Open)))
                        || (actual == self.vocabulary.evidence_type()
                            && phase.is_none_or(|p| {
                                matches!(
                                    p,
                                    Phase::EvidenceRead | Phase::EvidenceRelease | Phase::Close
                                )
                            })))
            }
        };
        if valid {
            Ok(())
        } else {
            Err(protocol(
                "state store original does not match the selected provider purpose",
            ))
        }
    }
    fn business_stop(&self, deadline: Instant) -> Option<Failure> {
        if let Purpose::Instance { root, .. } = &self.purpose
            && let Err(error) = root.check()
        {
            return Some(failure(error));
        }
        deadline_failure(deadline)
    }
}
impl Port {
    fn bind_original_with_control_in(
        &self,
        original: &Arc<dyn StateStoreBlockingOriginal>,
        deadline: Instant,
        control: RequestControl,
        target: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), Failure> {
        let deadline = control.deadline().map_or(deadline, |cap| deadline.min(cap));
        if target.is_some() {
            return Err(protocol("state store original borrower slot is occupied"));
        }
        self.validate(original.as_ref(), original.requested().map(|r| r.phase))?;
        // A shutdown waiting deadline grants no new business work and cannot
        // revoke the existing Host's obligation to destroy its exact instance.
        if !matches!(
            self.purpose,
            Purpose::ClosingInstance { .. } | Purpose::InstanceEvidence { .. }
        ) && let Some(error) = control.stop(self, deadline)
        {
            return Err(error);
        }
        let cell = Arc::new(Cell {
            original: original.clone(),
            deadline,
            control,
            observer: AtomicWaker::new(),
            body: Mutex::new(Some(Body {
                phase: None,
                health: None,
                notification_failure: None,
                notify: false,
            })),
        });
        let key = Arc::as_ptr(original) as *const () as usize;
        let borrower = Box::new(Borrower {
            cell: Arc::downgrade(&cell),
            wake: self.wake.clone(),
            requested: None,
        });
        {
            let mut inventory = self.inventory.lock().unwrap_or_else(|p| p.into_inner());
            if !inventory.accepting {
                return Err(protocol("state store original intake is closed"));
            }
            if inventory.entries.contains_key(&key) {
                return Err(protocol("state store original is already installed"));
            }
            inventory.entries.insert(key, cell);
            *target = Some(borrower);
        }
        Ok(()) // Caller calls request_progress after stable provider installation.
    }
}
impl StateStoreExecutionPort for Port {
    fn bind_original_in(
        &self,
        original: &Arc<dyn StateStoreBlockingOriginal>,
        deadline: Instant,
        target: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), Failure> {
        self.bind_original_with_control_in(original, deadline, RequestControl::Business, target)
    }
    fn request_progress(&self) {
        self.wake.notify();
    }
}
struct Cell {
    original: Arc<dyn StateStoreBlockingOriginal>,
    deadline: Instant,
    control: RequestControl,
    observer: AtomicWaker,
    body: Mutex<Option<Body>>,
}
struct Body {
    phase: Option<PhysicalPhase>,
    health: Option<Failure>,
    notification_failure: Option<Failure>,
    notify: bool,
}
impl Body {
    fn awaiting_published_footer(&self) -> bool {
        self.phase
            .as_ref()
            .is_some_and(|phase| phase.published && phase.exit.is_none())
    }
    fn reportable_health(&self) -> Option<Failure> {
        // The QA health receiver makes this owner dormant. Keep the same raw
        // cause here until its already accepted physical footer is received.
        if self.awaiting_published_footer() {
            None
        } else {
            self.health.clone()
        }
    }
}
type StateStoreReservationFuture = Pin<
    Box<dyn Future<Output = Result<QueryBlockingReservation, QueryBlockingReserveRefusal>> + Send>,
>;

struct PhysicalPhase {
    request: Request,
    call: Arc<PhaseCall>,
    reserve: Option<StateStoreReservationFuture>,
    reservation: Option<QueryBlockingReservation>,
    prepared: Option<QueryBlockingPreparedJob>,
    receipt: Option<QueryBlockingReceipt<()>>,
    result: Option<Result<(), Failure>>,
    exit: Option<Result<Exit, Failure>>,
    published: bool,
    interrupted: bool,
    gate: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}
struct Loan<'a> {
    cell: &'a Cell,
    body: Option<Body>,
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        let mut slot = self.cell.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            slot.is_none(),
            "state store original body restored exactly once"
        );
        *slot = self.body.take();
    }
}
impl Cell {
    fn actual_quiescent(&self) -> Result<bool, Failure> {
        let Some(body) = self.body.lock().unwrap_or_else(|p| p.into_inner()).take() else {
            return Ok(false);
        };
        let mut loan = Loan {
            cell: self,
            body: Some(body),
        };
        let body = loan.body.as_ref().unwrap();
        if let Some(cause) = body.reportable_health() {
            return Err(cause);
        }
        if let Some(phase) = &body.phase {
            match &phase.exit {
                None => return Ok(false),
                Some(Err(cause)) => return Err(cause.clone()),
                Some(Ok(_)) => {}
            }
        }
        // The Source can have queued the next revision while the old phase
        // already has its footer. Never confuse that old exit with this cut.
        // Loan keeps the same receiving stable, but holds no mutex here.
        match catch_unwind(AssertUnwindSafe(|| self.original.requested())) {
            Ok(request) => Ok(request.is_none()),
            Err(payload) => {
                let cause = failure(QueryBlockingFailure::panicked(payload));
                let body = loan.body.as_mut().unwrap();
                let first = body.health.get_or_insert(cause).clone();
                Err(first)
            }
        }
    }
    fn poll_original(&self, cx: &mut Context<'_>, port: &Port) -> Result<bool, Failure> {
        let Some(body) = self.body.lock().unwrap_or_else(|p| p.into_inner()).take() else {
            return Ok(false);
        };
        let mut loan = Loan {
            cell: self,
            body: Some(body),
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            let body = loan.body.as_mut().unwrap();
            if body.health.is_none()
                && let Some(error) = self.original.local_failure()
            {
                body.health = Some(error);
                body.notify = true;
            }
            if body.health.is_some() {
                // Health forbids another phase, but cannot abandon the same
                // published call between its result and physical footer.
                if let Some(phase) = body
                    .phase
                    .as_mut()
                    .filter(|phase| phase.published && phase.exit.is_none())
                {
                    body.notify |= self.poll_receipts(cx, phase)?;
                }
                return Ok(false);
            }
            let request = self.original.requested();
            if let Some(request) = request {
                port.validate(self.original.as_ref(), Some(request.phase))?;
                let replace = match &body.phase {
                    None => true,
                    Some(old) if old.request == request => false,
                    Some(old)
                        if old.exit.as_ref().is_some_and(Result::is_ok)
                            && request.revision > old.request.revision =>
                    {
                        true
                    }
                    Some(_) => {
                        return Err(protocol(
                            "state store original advanced before its exact physical exit",
                        ));
                    }
                };
                if replace {
                    let executor = port.executor.clone();
                    let reserve = Box::pin(async move { executor.reserve().await });
                    let gate = self.control.wait(port, self.deadline);
                    body.phase = Some(PhysicalPhase {
                        request,
                        call: Arc::new(PhaseCall {
                            original: self.original.clone(),
                            request,
                        }),
                        reserve: Some(reserve),
                        reservation: None,
                        prepared: None,
                        receipt: None,
                        result: None,
                        exit: None,
                        published: false,
                        interrupted: false,
                        gate: Some(gate),
                    });
                }
            }
            if let Some(phase) = body.phase.as_mut() {
                body.notify |= self.poll_phase(cx, port, phase)?;
            }
            if let Some(error) = self.original.local_failure() {
                return Err(error);
            }
            Ok(self.original.is_complete()
                && body
                    .phase
                    .as_ref()
                    .is_none_or(|p| p.exit.as_ref().is_some_and(Result::is_ok)))
        }));
        let result = match result {
            Ok(Err(error)) => {
                let body = loan.body.as_mut().unwrap();
                if body.health.is_none() {
                    body.health = Some(error.clone());
                    body.notify = true;
                }
                Err(error)
            }
            Ok(Ok(complete)) => Ok(complete),
            Err(panic) => {
                let error: Failure = failure(QueryBlockingFailure::panicked(panic));
                let body = loan.body.as_mut().unwrap();
                if body.health.is_none() {
                    body.health = Some(error.clone());
                    body.notify = true;
                }
                Err(error)
            }
        };
        // A stored health fact is not permission to drop an accepted receipt.
        // Only the real footer (including FAILED_UNPROVEN) ends this wait.
        let body = loan.body.as_ref().unwrap();
        let result = if body.health.is_some() {
            match body.reportable_health() {
                Some(error) => Err(error),
                None => Ok(false),
            }
        } else {
            result
        };
        let notify = std::mem::take(&mut loan.body.as_mut().unwrap().notify);
        // A borrower may poll on another thread immediately after notification.
        // Restore its stable receiving fields before any foreign wake.
        drop(loan);
        if notify && let Err(panic) = catch_unwind(AssertUnwindSafe(|| self.observer.wake())) {
            let error: Failure = failure(QueryBlockingFailure::panicked(panic));
            {
                let mut slot = self.body.lock().unwrap_or_else(|p| p.into_inner());
                let body = slot.as_mut().expect("body restored before notification");
                body.health.get_or_insert_with(|| error.clone());
                assert!(
                    body.notification_failure.is_none(),
                    "failed original notification is not retried"
                );
                body.notification_failure = Some(error.clone());
            }
            let slot = self.body.lock().unwrap_or_else(|p| p.into_inner());
            return match slot.as_ref().unwrap().reportable_health() {
                Some(error) => Err(error),
                None => Ok(false),
            };
        }
        result
    }
    fn poll_phase(
        &self,
        cx: &mut Context<'_>,
        port: &Port,
        phase: &mut PhysicalPhase,
    ) -> Result<bool, Failure> {
        if phase.exit.is_some() {
            return Ok(false);
        }
        let settlement = matches!(
            phase.request.phase,
            Phase::Rollback | Phase::Close | Phase::EvidenceRead | Phase::EvidenceRelease
        );
        if !settlement {
            if let Some(gate) = phase.gate.as_mut()
                && gate.as_mut().poll(cx).is_ready()
            {
                phase.gate.take();
            }
            if let Some(error) = self
                .control
                .stop(port, self.deadline)
                .or_else(|| self.original.publication_refusal(phase.request))
            {
                if !phase.interrupted {
                    phase.interrupted = true;
                    self.original.request_interrupt();
                }
                if !phase.published {
                    return self.withdraw(phase, error);
                }
                // Accepted work retains its exact result and actual-exit
                // receipts. Interrupt is a request, never withdrawal or exit.
            }
        }
        if !phase.published {
            if phase.reservation.is_none() {
                let result = phase
                    .reserve
                    .as_mut()
                    .expect("same reservation future")
                    .as_mut()
                    .poll(cx);
                match result {
                    Poll::Pending => return Ok(false),
                    Poll::Ready(Ok(reservation)) => {
                        phase.reservation = Some(reservation);
                        phase.reserve.take();
                    }
                    Poll::Ready(Err(error)) => {
                        return self.withdraw(phase, failure(PhysicalFailure::Reserve(error)));
                    }
                }
            }
            if !settlement
                && let Some(error) = self
                    .original
                    .publication_refusal(phase.request)
                    .or_else(|| self.control.stop(port, self.deadline))
            {
                return self.withdraw(phase, error);
            }
            if phase.prepared.is_none() {
                match port.executor.prepare_in(
                    phase.reservation.as_ref().unwrap(),
                    &phase.call,
                    &mut phase.receipt,
                ) {
                    Ok(prepared) => phase.prepared = Some(prepared),
                    Err(error) => {
                        return self.withdraw(phase, failure(PhysicalFailure::Publish(error)));
                    }
                }
                // Receipt is stable before the original marks its commit dispatch.
                if !settlement
                    && let Some(error) = self
                        .original
                        .publication_refusal(phase.request)
                        .or_else(|| self.control.stop(port, self.deadline))
                {
                    return self.withdraw(phase, error);
                }
                if let Err(error) = self.original.before_publish(phase.request) {
                    return self.withdraw(phase, error);
                }
            }
            match port
                .executor
                .publish_in(&mut phase.reservation, &mut phase.prepared)
            {
                Ok(publication) => {
                    phase.published = true;
                    publication.notify();
                }
                Err(error) => {
                    return self.withdraw(phase, failure(PhysicalFailure::Publish(error)));
                }
            }
        }
        self.poll_receipts(cx, phase)
    }
    fn poll_receipts(
        &self,
        cx: &mut Context<'_>,
        phase: &mut PhysicalPhase,
    ) -> Result<bool, Failure> {
        if phase.exit.is_some() {
            return Ok(false);
        }
        let mut changed = false;
        if phase.result.is_none()
            && let Poll::Ready(result) = phase.receipt.as_mut().unwrap().poll_result(cx)
        {
            let result = result.map_err(failure);
            phase.result = Some(result.clone());
            self.original.receive_result(phase.request, result);
            changed = true;
        }
        if phase.result.is_none() {
            return Ok(changed);
        }
        match phase.receipt.as_ref().unwrap().poll_actual_exit(cx) {
            Poll::Pending => {}
            Poll::Ready(Ok(())) => {
                let exit = Ok(Exit::ActualExited);
                phase.exit = Some(exit.clone());
                self.original.receive_exit(phase.request, exit);
                changed = true;
            }
            Poll::Ready(Err(kind)) => {
                let error = failure(PhysicalFailure::Exit {
                    kind,
                    cause: phase.receipt.as_ref().unwrap().actual_exit_failure(),
                });
                phase.exit = Some(Err(error.clone()));
                self.original
                    .receive_exit(phase.request, Err(error.clone()));
                return Err(error);
            }
        }
        Ok(changed)
    }
    fn withdraw(&self, phase: &mut PhysicalPhase, error: Failure) -> Result<bool, Failure> {
        // No worker owns these unpublished fields. Only normal destruction of
        // this exact prepared call and reservation proves a normal return.
        drop(phase.prepared.take());
        drop(phase.reservation.take());
        drop(phase.reserve.take());
        phase.result = Some(Err(error.clone()));
        phase.exit = Some(Ok(Exit::NeverPublishedReturned));
        self.original.receive_result(phase.request, Err(error));
        self.original
            .receive_exit(phase.request, Ok(Exit::NeverPublishedReturned));
        Ok(true)
    }
}
struct Borrower {
    cell: Weak<Cell>,
    wake: Arc<Wake>,
    requested: Option<Request>,
}
impl Borrower {
    fn notify_request(&mut self, request: Request) {
        if self.requested != Some(request) {
            self.requested = Some(request);
            self.wake.notify();
        }
    }
}
impl StateStoreBlockingSlot for Borrower {
    fn poll_phase(&mut self, cx: &mut Context<'_>, request: Request) -> Poll<Result<(), Failure>> {
        let cell = self
            .cell
            .upgrade()
            .ok_or_else(|| protocol("state store original custody is unavailable"))?;
        cell.observer.register(cx.waker());
        let result = {
            let slot = cell.body.lock().unwrap_or_else(|p| p.into_inner());
            slot.as_ref().and_then(|body| {
                body.health.clone().map(Err).or_else(|| {
                    body.phase
                        .as_ref()
                        .filter(|phase| phase.request == request)
                        .and_then(|phase| phase.result.clone())
                })
            })
        };
        match result {
            Some(result) => Poll::Ready(result),
            None => {
                self.notify_request(request);
                Poll::Pending
            }
        }
    }
    fn poll_exit(&mut self, cx: &mut Context<'_>, request: Request) -> Poll<Result<Exit, Failure>> {
        let cell = self
            .cell
            .upgrade()
            .ok_or_else(|| protocol("state store original custody is unavailable"))?;
        cell.observer.register(cx.waker());
        let result = {
            let slot = cell.body.lock().unwrap_or_else(|p| p.into_inner());
            slot.as_ref().and_then(|body| {
                body.health.clone().map(Err).or_else(|| {
                    body.phase
                        .as_ref()
                        .filter(|phase| phase.request == request)
                        .and_then(|phase| phase.exit.clone())
                })
            })
        };
        match result {
            Some(result) => Poll::Ready(result),
            None => {
                self.notify_request(request);
                Poll::Pending
            }
        }
    }
}
#[derive(Debug)]
enum PhysicalFailure {
    Reserve(QueryBlockingReserveRefusal),
    Publish(QueryBlockingPublishRefusal),
    Exit {
        #[expect(
            dead_code,
            reason = "Preserve the exact actual-exit verdict alongside its original opaque cause in the diagnostic failure."
        )]
        kind: QueryBlockingActualExitFailure,
        cause: Option<QueryBlockingFailure>,
    },
}
impl std::fmt::Display for PhysicalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("state store original physical execution failed")
    }
}
impl std::error::Error for PhysicalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Exit {
                cause: Some(cause), ..
            } => Some(cause),
            _ => None,
        }
    }
}

fn failure<E: std::error::Error + FailureProjection + Send + Sync + 'static>(error: E) -> Failure {
    Failure::from_static_detail(error, "state store original physical execution failed")
}
impl FailureProjection for PhysicalFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_query_application::execution_error::QueryFailureReason;
        match self {
            Self::Reserve(QueryBlockingReserveRefusal::Full) => {
                QueryFailureReason::BlockingQueueFull.failure_fact()
            }
            Self::Reserve(QueryBlockingReserveRefusal::Closed)
            | Self::Publish(QueryBlockingPublishRefusal::Closed) => {
                QueryFailureReason::BlockingExecutorClosed.failure_fact()
            }
            Self::Exit {
                cause: Some(cause), ..
            } => cause.failure_fact(),
            _ => QueryFailureReason::BlockingSourceContract.failure_fact(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct StateStoreOriginalBinding {
    pub(crate) transaction_deadline: std::time::Duration,
    pub(crate) dependencies: Vec<novarocks_resource_contract::SharedDependency>,
    pub(crate) vocabulary: Vocabulary,
    pub(crate) instance: InstanceScope,
    pub(crate) executor: QueryBlockingExecutor,
    pub(crate) policy: novarocks_state_store_runtime::StateStoreRunPolicy,
}
impl StateStoreOriginalBinding {
    pub(crate) fn blocking_dependency_facts(
        &self,
    ) -> &[novarocks_resource_contract::SharedDependency] {
        &self.dependencies
    }
    pub(crate) fn for_root(&self, root: WorkScope) -> Result<StateStoreOriginalOwner, Failure> {
        StateStoreOriginalOwner::for_root(
            &self.executor,
            self.vocabulary,
            self.instance,
            root,
            self.policy,
        )
    }
}

#[cfg(test)]
#[path = "execution/tests.rs"]
mod tests;
