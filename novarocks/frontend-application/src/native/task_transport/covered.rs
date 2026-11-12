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

//! Original covered subscription ownership and stream execution.

use super::*;

/// How long a lost subscription waits before its next attempt, per failure in
/// the current run.
///
/// The error budget alone bounds the number of attempts; this bounds their
/// rate, so a flapping backend cannot spend the whole budget inside one tick.
pub(super) const RESUBSCRIBE_BACKOFF_STEP: Duration = Duration::from_millis(100);
pub(super) const COVERED_STREAM_LIVENESS_WINDOW: Duration = Duration::from_secs(5);

pub(super) fn covered_reconnect_backoff(failures: u32, jitter_sample: u64) -> Duration {
    let base_ms = 100_u64
        .saturating_mul(1_u64 << failures.saturating_sub(1).min(4))
        .min(1_000);
    let percent = 80 + jitter_sample % 41;
    Duration::from_millis(base_ms * percent / 100)
}

/// The observable state of one logical subscription.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum SubscriptionState {
    Opening,
    Live,
    /// The stream is gone and the cursors are being replayed.
    Resubscribing,
    /// The bounded resubscription budget ran out.
    BudgetExhausted,
    /// The same retry budget ended after a typed original connection loss
    /// that no subsequently accepted stream liveness repaired.
    ConnectionLossExhausted,
    /// The backend rejected the subscription in a way resubscribing cannot
    /// repair.
    Rejected,
    /// An event addressed a different backend process. An exact process
    /// replacement is fatal to the attempt and is never followed.
    ProcessMismatch,
    /// An event addressed a different query execution on the expected
    /// backend process. It cannot contribute to this participation's cursor.
    QueryMismatch,
}

impl SubscriptionState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::Live => "live",
            Self::Resubscribing => "resubscribing",
            Self::BudgetExhausted => "budget_exhausted",
            Self::ConnectionLossExhausted => "connection_loss_exhausted",
            Self::Rejected => "rejected",
            Self::ProcessMismatch => "process_mismatch",
            Self::QueryMismatch => "query_mismatch",
        }
    }

    /// Whether this state is fatal to the query attempt.
    pub(crate) const fn is_fatal(self) -> bool {
        matches!(
            self,
            Self::BudgetExhausted
                | Self::ConnectionLossExhausted
                | Self::Rejected
                | Self::ProcessMismatch
                | Self::QueryMismatch
        )
    }
}

pub(super) fn set_state(state: &Mutex<SubscriptionState>, next: SubscriptionState) {
    if let Ok(mut current) = state.lock() {
        *current = next;
    }
    observe_subscription_state(next);
}

/// The subscription loop of one participation.
///
/// A dropped stream is never a task failure: the backend process is intact and
/// still running the query, so the loop reports observation loss and
/// resubscribes from the cursors it holds. It never cancels a task, never
/// settles an operation, and never touches a lease.
pub(super) fn covered_protocol(
    site: &'static str,
) -> crate::task_execution::error::TaskExecutionError {
    crate::task_execution::error::TaskExecutionError::Schedule(site.to_owned())
}
pub(super) fn covered_registration(
    error: crate::native::subscription_owner::RegistrationError,
) -> crate::task_execution::error::TaskExecutionError {
    use crate::native::subscription_owner::RegistrationError;
    match error {
        RegistrationError::Work(error) => {
            if matches!(&error, novarocks_workload_control::WorkError::Conflict) {
                tracing::warn!(
                    "Native covered subscription source reported conflicting work facts"
                );
            }
            crate::task_execution::error::TaskExecutionError::Workload(error)
        }
        RegistrationError::Closed => covered_protocol("Native subscription source closed"),
        RegistrationError::Storage => {
            covered_protocol("Native subscription source positions exhausted")
        }
        RegistrationError::Generation => {
            covered_protocol("Native subscription generation exhausted")
        }
        RegistrationError::SpawnUnjoinable => {
            covered_protocol("Native subscription original spawn unjoinable")
        }
    }
}

pub(super) enum CoveredSourceChildren {
    LegacyIndividual,
    Cohort(crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort),
}

/// One covered observation stream per Participation. Its request is always built
/// from cursors the serial owner has applied; receiving a frame does not make
/// that frame a safe reconnect cursor.
pub(crate) struct CoveredTaskStatusSubscriber {
    pub(super) targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    pub(super) intake: Arc<ObservationIntake>,
    pub(super) error_budget: u32,
    pub(super) data_runtime: FrontendDataRuntime,
    pub(super) work_scope: novarocks_workload_control::WorkScope,
    pub(super) active: Mutex<BTreeMap<ParticipationRef, CoveredEntry>>,
    pub(super) source_children: CoveredSourceChildren,
}

impl fmt::Debug for CoveredTaskStatusSubscriber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoveredTaskStatusSubscriber")
            .field("backends", &self.targets.len())
            .field("error_budget", &self.error_budget)
            .finish()
    }
}

pub(super) enum CoveredEntry {
    Pending(crate::native::subscription_owner::SubscriptionReservation),
    Starting(crate::native::subscription_owner::SubscriptionStopGuard),
    Running(CoveredSubscription),
}
pub(super) struct CoveredSubscription {
    pub(super) state: Arc<Mutex<SubscriptionState>>,
    pub(super) applied_snapshot: Arc<Mutex<DecodedCoveredSubscription>>,
    pub(super) reconciliation: tokio::sync::watch::Sender<DecodedCoveredSubscription>,
    pub(super) task: crate::native::subscription_owner::SubscriptionStopGuard,
}

impl Drop for CoveredSubscription {
    fn drop(&mut self) {
        self.task.stop();
    }
}

impl CoveredTaskStatusSubscriber {
    /// Explicit old coordinator/fixture API; production manifest uses the cohort constructor.
    pub(crate) fn new(
        backends: &[BackendProcessDescriptor],
        intake: Arc<ObservationIntake>,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
        work_scope: novarocks_workload_control::WorkScope,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if error_budget == 0 {
            return Err(NativeTaskTransportCreationError::ZeroStatusErrorBudget);
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            error_budget,
            data_runtime,
            work_scope,
            active: Mutex::new(BTreeMap::new()),
            source_children: CoveredSourceChildren::LegacyIndividual,
        })
    }

    pub(crate) fn new_with_source_cohort(
        backends: &[BackendProcessDescriptor],
        intake: Arc<ObservationIntake>,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
        work_scope: novarocks_workload_control::WorkScope,
        source_cohort: crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if error_budget == 0 {
            return Err(NativeTaskTransportCreationError::ZeroStatusErrorBudget);
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            error_budget,
            data_runtime,
            work_scope,
            active: Mutex::new(BTreeMap::new()),
            source_children: CoveredSourceChildren::Cohort(source_cohort),
        })
    }

    /// Reserve only the existing finite source header. The SAME subscription
    /// service polls child admission; Pending never suspends the serial fold.
    /// Call before constructing covered_request or its request/watch/F captures.
    pub(crate) fn prepare(
        &self,
        participation: ParticipationRef,
    ) -> Result<bool, crate::task_execution::error::TaskExecutionError> {
        if !self
            .targets
            .contains_key(&participation.backend_process_id())
        {
            return Err(covered_protocol(
                "covered participation is outside the frozen topology",
            ));
        }
        let port = self
            .data_runtime
            .subscription_port()
            .map_err(covered_registration)?;
        let runner_wake = self.intake.progress_wake();
        let mut created = false;
        let result = {
            let mut active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            if let std::collections::btree_map::Entry::Vacant(entry) = active.entry(participation) {
                // Both constructors only record metadata under caller/Core locks.
                // The original service consumes the paid cohort outside them.
                let original = match &self.source_children {
                    CoveredSourceChildren::Cohort(cohort) => {
                        port.prepare_from_cohort(&self.work_scope, cohort.clone(), None, None)
                    }
                    CoveredSourceChildren::LegacyIndividual => {
                        #[cfg(not(test))]
                        let original = port.prepare(&self.work_scope, None, None);
                        #[cfg(test)]
                        let original = if !self.work_scope.has_responsibility_service() {
                            port.reserve(&self.work_scope, None, None)
                        } else {
                            port.prepare(&self.work_scope, None, None)
                        };
                        original
                    }
                }
                .map_err(covered_registration)?;
                entry.insert(CoveredEntry::Pending(original));
                created = true;
            }
            match active
                .get(&participation)
                .expect("same original source entry")
            {
                CoveredEntry::Pending(original) => {
                    original.ready(&runner_wake).map_err(covered_registration)
                }
                CoveredEntry::Starting(_) => Ok(false),
                CoveredEntry::Running(_) => Ok(true),
            }
        };
        // Source is stable before any callback; no active/Core lock escapes.
        if created {
            port.notify();
        }
        result
    }
    pub(crate) fn ensure(
        &self,
        request: DecodedCoveredSubscription,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        self.apply_request(request, false)
    }
    pub(crate) fn reconcile(
        &self,
        request: DecodedCoveredSubscription,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        self.apply_request(request, true)?;
        observe_resubscribe();
        Ok(())
    }
    /// Existing Running aliases only; no pending source creates an SDK request.
    pub(crate) fn update_applied_request(
        &self,
        request: DecodedCoveredSubscription,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        encode_covered_subscribe_task_status(&request)
            .map_err(|_| covered_protocol("covered subscription encoding refused"))?;
        let snapshot = {
            let active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            match active.get(&request.participation) {
                Some(CoveredEntry::Running(original)) => Arc::clone(&original.applied_snapshot),
                _ => return Err(covered_protocol("covered subscription is not running")),
            }
        };
        *snapshot
            .lock()
            .map_err(|_| covered_protocol("covered applied snapshot lock poisoned"))? = request;
        Ok(())
    }
    pub(super) fn apply_request(
        &self,
        request: DecodedCoveredSubscription,
        reconcile: bool,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        encode_covered_subscribe_task_status(&request)
            .map_err(|_| covered_protocol("covered subscription encoding refused"))?;
        let participation = request.participation;
        #[cfg(test)]
        if !self
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&participation)
        {
            // The old unbound wire fixtures called ensure directly. Production
            // serial turns always run the explicit prepare gate before captures.
            if !self.prepare(participation)? {
                return Err(covered_protocol("covered child not ready"));
            }
        }
        // Copy only existing handles. watch callbacks and old request destruction
        // are outside active; a concurrent stop cannot resurrect this incarnation.
        let running = {
            let active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            match active.get(&participation) {
                Some(CoveredEntry::Running(original)) => Some((
                    Arc::clone(&original.applied_snapshot),
                    original.reconciliation.clone(),
                )),
                _ => None,
            }
        };
        if let Some((snapshot, sender)) = running {
            *snapshot
                .lock()
                .map_err(|_| covered_protocol("covered applied snapshot lock poisoned"))? =
                request.clone();
            if reconcile {
                drop(sender.send_replace(request));
            }
            return Ok(());
        }
        let (reservation, publication) = {
            let mut active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            if !matches!(active.get(&participation), Some(CoveredEntry::Pending(_))) {
                return Err(covered_protocol(
                    "covered subscription requires prepared original child",
                ));
            }
            let Some(CoveredEntry::Pending(reservation)) = active.remove(&participation) else {
                unreachable!()
            };
            let publication = reservation.stop_view();
            let replacement = reservation.stop_view();
            active.insert(participation, CoveredEntry::Starting(replacement));
            (reservation, publication)
        };
        // Original Core/header and the Starting stop view already exist. All
        // input construction, user spawn, wake and Drop are outside active.
        let started = self.start(reservation, request)?;
        let displaced = {
            let mut active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            let same = matches!(active.get(&participation), Some(CoveredEntry::Starting(original)) if original.same_original(&publication));
            if same {
                active.insert(participation, CoveredEntry::Running(started))
            } else {
                drop(active);
                drop(started);
                drop(publication);
                return Ok(());
            }
        };
        if let Some(CoveredEntry::Starting(original)) = displaced {
            original.disarm();
        }
        publication.disarm();
        Ok(())
    }
    pub(crate) fn stop(&self, participation: ParticipationRef) {
        let original = self
            .active
            .lock()
            .ok()
            .and_then(|mut active| active.remove(&participation));
        drop(original);
    }
    pub(crate) fn state(&self, participation: ParticipationRef) -> Option<SubscriptionState> {
        let state = {
            let active = self.active.lock().ok()?;
            match active.get(&participation)? {
                CoveredEntry::Running(original) => Arc::clone(&original.state),
                CoveredEntry::Pending(_) | CoveredEntry::Starting(_) => return None,
            }
        };
        state.lock().ok().map(|state| *state)
    }
    pub(super) fn start(
        &self,
        reservation: crate::native::subscription_owner::SubscriptionReservation,
        request: DecodedCoveredSubscription,
    ) -> Result<CoveredSubscription, crate::task_execution::error::TaskExecutionError> {
        let target = self
            .targets
            .get(&request.participation.backend_process_id())
            .ok_or_else(|| {
                covered_protocol("covered participation is outside the frozen topology")
            })?;
        let publisher = Arc::new(self.intake.subscribe().map_err(|_| {
            covered_protocol("covered observation subscription capacity exhausted")
        })?);
        let state = Arc::new(Mutex::new(SubscriptionState::Opening));
        let applied_snapshot = Arc::new(Mutex::new(request.clone()));
        let (reconciliation, receiver) = tokio::sync::watch::channel(request);
        reservation.attach(Arc::clone(&publisher), Arc::clone(&state));
        let task = reservation
            .start(run_covered_subscription(
                target.client.clone(),
                receiver,
                Arc::clone(&applied_snapshot),
                publisher,
                self.error_budget,
                Arc::clone(&state),
            ))
            .map_err(covered_registration)?;
        Ok(CoveredSubscription {
            state,
            applied_snapshot,
            reconciliation,
            task,
        })
    }
}

/// Preserve the channel owner's observed IO loss before any diagnostic display.
#[derive(Debug)]
pub(super) enum CoveredSubscriptionOpenError {
    Channel(crate::native::transport::ChannelAcquisitionError),
    Rpc(tonic::Status),
}
impl CoveredSubscriptionOpenError {
    pub(super) fn fatal_status(&self) -> Option<&tonic::Status> {
        match self {
            Self::Rpc(status) if subscription_rejection_is_fatal(status) => Some(status),
            _ => None,
        }
    }
    pub(super) fn observed_connection_loss(&self) -> bool {
        match self {
            Self::Channel(error) => error.observed_connection_loss(),
            Self::Rpc(status) => crate::native::transport::is_observed_connection_loss(status),
        }
    }
}

/// Evidence belongs to this original reconnect streak, not to a status label.
#[derive(Default)]
pub(super) struct CoveredConnectionEvidence {
    pub(super) lost: bool,
}
impl CoveredConnectionEvidence {
    pub(super) fn observe_loss(&mut self, observed: bool) {
        self.lost |= observed;
    }
    pub(super) fn observe_liveness(&mut self) {
        self.lost = false;
    }
    pub(super) fn exhausted_state(&self) -> SubscriptionState {
        if self.lost {
            SubscriptionState::ConnectionLossExhausted
        } else {
            SubscriptionState::BudgetExhausted
        }
    }
}

pub(super) async fn open_covered_subscription(
    client: &Client,
    request: &DecodedCoveredSubscription,
) -> Result<tonic::Streaming<proto::TaskStatusStreamEvent>, CoveredSubscriptionOpenError> {
    let request = encode_covered_subscribe_task_status(request).map_err(|error| {
        CoveredSubscriptionOpenError::Rpc(tonic::Status::invalid_argument(error.to_string()))
    })?;
    let mut grpc = client
        .grpc_with_channel_error(NativeRpcMethod::SubscribeTaskStatus)
        .await
        .map_err(CoveredSubscriptionOpenError::Channel)?;
    grpc.subscribe_task_status(tonic::Request::new(request))
        .await
        .map(tonic::Response::into_inner)
        .map_err(CoveredSubscriptionOpenError::Rpc)
}

/// Actual same-factory F layout; does not construct a client, request or F.
#[cfg(test)]
pub(crate) fn covered_subscription_future_layout() -> std::alloc::Layout {
    type CoveredSubscriptionFactory<F> = fn(
        Client,
        tokio::sync::watch::Receiver<DecodedCoveredSubscription>,
        Arc<Mutex<DecodedCoveredSubscription>>,
        Arc<ObservationPublisher>,
        u32,
        Arc<Mutex<SubscriptionState>>,
    ) -> F;
    fn layout<F: Future<Output = ()>>(_: CoveredSubscriptionFactory<F>) -> std::alloc::Layout {
        std::alloc::Layout::new::<F>()
    }
    layout(run_covered_subscription)
}

pub(super) async fn run_covered_subscription(
    client: Client,
    mut reconciliation: tokio::sync::watch::Receiver<DecodedCoveredSubscription>,
    applied_snapshot: Arc<Mutex<DecodedCoveredSubscription>>,
    publisher: Arc<ObservationPublisher>,
    error_budget: u32,
    state: Arc<Mutex<SubscriptionState>>,
) {
    reconciliation.borrow_and_update();
    let mut applied: DecodedCoveredSubscription;
    let mut last_generation = 0_u64;
    let jitter = std::collections::hash_map::RandomState::new();
    let mut failures = 0_u32;
    let mut connection = CoveredConnectionEvidence::default();
    'subscription: loop {
        applied = match applied_snapshot.lock() {
            Ok(snapshot) => snapshot.clone(),
            Err(_) => {
                set_state(&state, SubscriptionState::Rejected);
                return;
            }
        };
        let Some(next_generation) = last_generation.checked_add(1) else {
            set_state(&state, SubscriptionState::Rejected);
            return;
        };
        let generation = applied.generation.get().max(next_generation);
        last_generation = generation;
        applied.generation = NonZeroU64::new(generation).expect("generation is nonzero");
        let requested_versions = CoveredRequestedVersions::from_request(&applied);
        let participation = applied.participation;
        let opened = tokio::select! {
            changed = reconciliation.changed() => {
                if changed.is_err() { return; }
                reconciliation.borrow_and_update();
                set_state(&state, SubscriptionState::Resubscribing);
                continue 'subscription;
            }
            opened = tokio::time::timeout(
                COVERED_STREAM_LIVENESS_WINDOW, open_covered_subscription(&client, &applied)
            ) => opened.unwrap_or_else(|_| Err(CoveredSubscriptionOpenError::Rpc(tonic::Status::deadline_exceeded(
                "covered observation subscription opening exceeded its liveness budget"
            )))),
        };
        match opened {
            Ok(mut stream) => {
                set_state(&state, SubscriptionState::Live);
                let mut observed_liveness = false;
                let mut catch_up_seen = false;
                let mut bookmark_sequence = 0_u64;
                let mut liveness_deadline =
                    tokio::time::Instant::now() + COVERED_STREAM_LIVENESS_WINDOW;
                loop {
                    // A permit is obtained before the next network read. It
                    // reserves one bounded frame even if the serial queue is
                    // full, and dropping it on EOF/error returns that credit.
                    let reservation_started = tokio::time::Instant::now();
                    let permit = tokio::select! {
                        changed = reconciliation.changed() => {
                            if changed.is_err() { return; }
                            reconciliation.borrow_and_update();
                            set_state(&state, SubscriptionState::Resubscribing);
                            continue 'subscription;
                        }
                        permit = publisher.reserve_read() => permit,
                    };
                    // Time spent stopped by local intake capacity does not
                    // count as transport silence. The stream was not read
                    // while this permit was unavailable.
                    liveness_deadline += tokio::time::Instant::now() - reservation_started;
                    let message = tokio::select! {
                        changed = reconciliation.changed() => {
                            if changed.is_err() { return; }
                            reconciliation.borrow_and_update();
                            set_state(&state, SubscriptionState::Resubscribing);
                            continue 'subscription;
                        }
                        message = tokio::time::timeout_at(liveness_deadline, stream.message()) => {
                            match message {
                                Ok(message) => message,
                                Err(_) => break,
                            }
                        },
                    };
                    match message {
                        Ok(Some(wire)) => {
                            let encoded_bytes = prost::Message::encoded_len(&wire);
                            let event = match decode_covered_status_event(
                                &wire,
                                FieldPath::root("covered_status_stream_event"),
                            ) {
                                Ok(event) => event,
                                Err(error) => {
                                    tracing::warn!(%participation, detail = %error, "covered observation frame is malformed");
                                    set_state(&state, SubscriptionState::Rejected);
                                    return;
                                }
                            };
                            if let Err(error) = validate_covered_event_participation(
                                &applied,
                                &requested_versions,
                                &event,
                            ) {
                                tracing::warn!(%participation, detail = %error, "covered observation frame conflicts with its stream");
                                set_state(&state, error.subscription_state());
                                return;
                            }
                            let new_liveness = match &event.fact {
                                CoveredStatusStreamFact::CatchUpComplete(_) if !catch_up_seen => {
                                    catch_up_seen = true;
                                    true
                                }
                                CoveredStatusStreamFact::Bookmark(marker)
                                    if marker.sequence > bookmark_sequence =>
                                {
                                    bookmark_sequence = marker.sequence;
                                    true
                                }
                                _ => false,
                            };
                            let convergence = match &event.fact {
                                CoveredStatusStreamFact::ParticipationConvergence(receipt) => {
                                    Some(*receipt)
                                }
                                _ => None,
                            };
                            let terminal = match &event.fact {
                                CoveredStatusStreamFact::Status(status) if status.is_terminal() => {
                                    Some((status.identity(), status.state()))
                                }
                                _ => None,
                            };
                            if let Err(error) = permit.publish(
                                ObservationFrame::Covered {
                                    participation,
                                    generation: applied.generation.get(),
                                    event,
                                },
                                encoded_bytes,
                            ) {
                                tracing::warn!(
                                    %participation,
                                    charged_bytes = error.charged_bytes,
                                    max_frame_bytes = error.max_frame_bytes,
                                    "covered observation frame exceeds its reserved budget"
                                );
                                set_state(&state, SubscriptionState::Rejected);
                                return;
                            }
                            if let Some(receipt) = convergence {
                                tracing::debug!(
                                    %participation,
                                    execution_id = ?participation.query_execution_id(),
                                    version = %receipt.version(),
                                    state = ?receipt.state(),
                                    "frontend retained worker stop and participation fence"
                                );
                            }
                            if let Some((identity, state)) = terminal {
                                tracing::debug!(
                                    execution_id = ?identity.query_execution_id(),
                                    stage = identity.stage_id().get(),
                                    task = identity.task_id().get(),
                                    backend = %identity.backend_process_id(),
                                    ?state,
                                    "frontend retained task terminal"
                                );
                            }
                            if new_liveness {
                                connection.observe_liveness();
                                observed_liveness = true;
                                liveness_deadline =
                                    tokio::time::Instant::now() + COVERED_STREAM_LIVENESS_WINDOW;
                            }
                        }
                        Ok(None) => break,
                        Err(status) if subscription_rejection_is_fatal(&status) => {
                            tracing::warn!(%participation, code = ?status.code(), detail = status.message(), "covered observation stream rejected");
                            set_state(&state, SubscriptionState::Rejected);
                            return;
                        }
                        Err(status) => {
                            connection.observe_loss(
                                crate::native::transport::is_observed_connection_loss(&status),
                            );
                            break;
                        }
                    }
                }
                if observed_liveness {
                    failures = 0;
                }
            }
            Err(error) if error.fatal_status().is_some() => {
                let status = error.fatal_status().expect("fatal status was checked");
                tracing::warn!(%participation, code = ?status.code(), detail = status.message(), "covered observation subscription rejected");
                set_state(&state, SubscriptionState::Rejected);
                return;
            }
            Err(error) => connection.observe_loss(error.observed_connection_loss()),
        }
        // A disconnection does not erase accepted facts or manufacture a
        // local observation gap. Reconnect from the owner's applied cursors.
        failures += 1;
        if failures >= error_budget {
            set_state(&state, connection.exhausted_state());
            return;
        }
        set_state(&state, SubscriptionState::Resubscribing);
        observe_resubscribe();
        let sample =
            std::hash::BuildHasher::hash_one(&jitter, (applied.participation, last_generation));
        let backoff = covered_reconnect_backoff(failures, sample);
        tokio::select! {
            changed = reconciliation.changed() => {
                if changed.is_err() { return; }
                reconciliation.borrow_and_update();
            }
            _ = tokio::time::sleep(backoff) => {}
        }
    }
}
