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

//! Legacy observation transport, retained until the attempt cutover.

use super::*;

/// One running subscription. Dropping it stops the stream.
pub(super) struct Subscription {
    pub(super) state: Arc<Mutex<SubscriptionState>>,
    pub(super) reconciliation: tokio::sync::watch::Sender<TaskCursorReconciliation>,
    pub(super) task: tokio::task::JoinHandle<()>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The `SubscribeTaskStatus` client of one attempt.
///
/// One logical subscription per participation, started lazily the first time
/// that participation has a task to observe. A new task joins the running stream on
/// its own, so nothing here freezes a task set.
pub(crate) struct TaskStatusSubscriber {
    pub(super) targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    pub(super) intake: StatusIntakeHandle,
    pub(super) participation_convergence: Option<ParticipationConvergenceIntakeHandle>,
    pub(super) error_budget: u32,
    pub(super) data_runtime: FrontendDataRuntime,
    pub(super) active: Mutex<BTreeMap<ParticipationRef, Subscription>>,
}

impl fmt::Debug for TaskStatusSubscriber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskStatusSubscriber")
            .field("backends", &self.targets.len())
            .field("error_budget", &self.error_budget)
            .finish()
    }
}

impl TaskStatusSubscriber {
    /// Builds the task-only transport retained until the production owner cut
    /// installs the convergence intake in the same atomic change.
    ///
    /// This is a migration-only constructor. New ownership must use
    /// [`Self::new_context_aware`].
    #[cfg(test)]
    pub(crate) fn new(
        backends: &[BackendProcessDescriptor],
        intake: StatusIntakeHandle,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if error_budget == 0 {
            return Err(NativeTaskTransportCreationError::ZeroStatusErrorBudget);
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            participation_convergence: None,
            error_budget,
            data_runtime,
            active: Mutex::new(BTreeMap::new()),
        })
    }

    /// Builds the single-stream task and participation observation transport.
    #[cfg(test)]
    pub(crate) fn new_participation_aware(
        backends: &[BackendProcessDescriptor],
        intake: StatusIntakeHandle,
        participation_convergence: ParticipationConvergenceIntakeHandle,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if error_budget == 0 {
            return Err(NativeTaskTransportCreationError::ZeroStatusErrorBudget);
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            participation_convergence: Some(participation_convergence),
            error_budget,
            data_runtime,
            active: Mutex::new(BTreeMap::new()),
        })
    }

    /// Starts this participation's one subscription if it is not already running.
    pub(crate) fn ensure(
        &self,
        participation: ParticipationRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| "task status subscription lock poisoned".to_owned())?;
        if active.contains_key(&participation) {
            return Ok(());
        }
        let subscription = self.start(participation, cursors)?;
        active.insert(participation, subscription);
        Ok(())
    }

    /// Reconciles this participation's one subscription to the runner's cursors.
    ///
    /// This is the answer to observation loss the runner saw on its own side,
    /// such as a full intake queue. Loss the stream itself sees is recovered
    /// by the subscription without anyone asking. The bounded watch channel
    /// coalesces repeated reconciliation requests and gives the same loop the
    /// later one-way lifecycle control seam; it never creates a second stream.
    pub(crate) fn resubscribe(
        &self,
        participation: ParticipationRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        let cursors = task_cursor_map(participation, cursors)?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| "task status subscription lock poisoned".to_owned())?;
        if let Some(subscription) = active.get(&participation) {
            subscription.reconciliation.send_modify(|current| {
                current.generation = current
                    .generation
                    .checked_add(1)
                    .expect("task cursor reconciliation generation exhausted");
                current.cursors.clone_from(&cursors);
            });
        } else {
            active.insert(
                participation,
                self.start_with_cursors(participation, cursors)?,
            );
        }
        observe_resubscribe();
        Ok(())
    }

    /// Stops this participation's subscription.
    pub(crate) fn stop(&self, participation: ParticipationRef) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&participation);
        }
    }

    /// What this participation's subscription is currently doing.
    pub(crate) fn state(&self, participation: ParticipationRef) -> Option<SubscriptionState> {
        let active = self.active.lock().ok()?;
        let subscription = active.get(&participation)?;
        subscription.state.lock().ok().map(|state| *state)
    }

    pub(super) fn start(
        &self,
        participation: ParticipationRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<Subscription, String> {
        self.start_with_cursors(participation, task_cursor_map(participation, cursors)?)
    }

    pub(super) fn start_with_cursors(
        &self,
        participation: ParticipationRef,
        cursors: BTreeMap<TaskIdentity, TaskStatusCursor>,
    ) -> Result<Subscription, String> {
        let target = self
            .targets
            .get(&participation.backend_process_id())
            .ok_or_else(|| {
                format!(
                    "participation {} addresses a backend process outside the frozen topology",
                    participation.backend_process_id()
                )
            })?;
        let state = Arc::new(Mutex::new(SubscriptionState::Opening));
        let (reconciliation, receiver) = tokio::sync::watch::channel(TaskCursorReconciliation {
            generation: 0,
            cursors,
        });
        let task = self.data_runtime.spawn(run_subscription(
            target.client.clone(),
            participation,
            receiver,
            self.intake.clone(),
            self.participation_convergence.clone(),
            self.error_budget,
            Arc::clone(&state),
        ));
        Ok(Subscription {
            state,
            reconciliation,
            task,
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct TaskCursorReconciliation {
    pub(super) generation: u64,
    pub(super) cursors: BTreeMap<TaskIdentity, TaskStatusCursor>,
}

pub(super) fn task_cursor_map(
    participation: ParticipationRef,
    cursors: Vec<TaskStatusCursor>,
) -> Result<BTreeMap<TaskIdentity, TaskStatusCursor>, String> {
    let count = cursors.len();
    let cursors = cursors
        .into_iter()
        .map(|cursor| {
            let identity = cursor.identity();
            if identity.query_execution_id() != participation.query_execution_id()
                || identity.backend_process_id() != participation.backend_process_id()
            {
                return Err(format!(
                    "task status cursor {identity} is outside participation {participation}"
                ));
            }
            Ok((identity, cursor))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    if cursors.len() != count {
        return Err("task status reconciliation contains duplicate task cursors".to_owned());
    }
    Ok(cursors)
}

pub(super) async fn run_subscription(
    client: Client,
    participation: ParticipationRef,
    mut reconciliation: tokio::sync::watch::Receiver<TaskCursorReconciliation>,
    intake: StatusIntakeHandle,
    participation_convergence: Option<ParticipationConvergenceIntakeHandle>,
    error_budget: u32,
    state: Arc<Mutex<SubscriptionState>>,
) {
    let initial = reconciliation.borrow_and_update().clone();
    let mut reconciliation_generation = initial.generation;
    let mut cursors = initial.cursors;
    let mut participation_cursor = participation_convergence
        .as_ref()
        .map(|_| ParticipationConvergenceCursor::unobserved(participation));
    let mut failures = 0_u32;
    'subscription: loop {
        let opened = tokio::select! {
            changed = reconciliation.changed() => {
                if changed.is_err() {
                    return;
                }
                apply_task_cursor_reconciliation(
                    &mut reconciliation,
                    &mut reconciliation_generation,
                    &mut cursors,
                );
                set_state(&state, SubscriptionState::Resubscribing);
                continue 'subscription;
            }
            opened = open_subscription(&client, participation, &cursors, participation_cursor) => opened,
        };
        match opened {
            Ok(mut stream) => {
                set_state(&state, SubscriptionState::Live);
                let mut delivered = false;
                loop {
                    let message = tokio::select! {
                        changed = reconciliation.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            apply_task_cursor_reconciliation(
                                &mut reconciliation,
                                &mut reconciliation_generation,
                                &mut cursors,
                            );
                            set_state(&state, SubscriptionState::Resubscribing);
                            continue 'subscription;
                        }
                        message = stream.message() => message,
                    };
                    match message {
                        Ok(Some(event)) => {
                            match observe_stream_event(
                                participation,
                                &event,
                                &mut cursors,
                                &intake,
                                participation_convergence.as_ref(),
                                &mut participation_cursor,
                            )
                            .await
                            {
                                Ok(StreamObservation::Delivered) => delivered = true,
                                Ok(StreamObservation::AwaitTaskReconciliation) => {
                                    set_state(&state, SubscriptionState::Resubscribing);
                                    if reconciliation.changed().await.is_err() {
                                        return;
                                    }
                                    apply_task_cursor_reconciliation(
                                        &mut reconciliation,
                                        &mut reconciliation_generation,
                                        &mut cursors,
                                    );
                                    continue 'subscription;
                                }
                                Err(next) => {
                                    set_state(&state, next);
                                    intake.note_observation_incomplete();
                                    return;
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(status) => {
                            if subscription_rejection_is_fatal(&status) {
                                tracing::warn!(
                                    code = ?status.code(),
                                    detail = status.message(),
                                    "SubscribeTaskStatus stream rejected"
                                );
                                set_state(&state, SubscriptionState::Rejected);
                                intake.note_observation_incomplete();
                                return;
                            }
                            break;
                        }
                    }
                }
                if delivered {
                    // A stream that actually observed something is not part of
                    // the previous failure run. A stream that opened and
                    // delivered nothing is, so a flapping backend cannot reset
                    // the budget forever.
                    failures = 0;
                }
            }
            Err(status) if subscription_rejection_is_fatal(&status) => {
                tracing::warn!(
                    code = ?status.code(),
                    detail = status.message(),
                    "SubscribeTaskStatus subscription rejected"
                );
                set_state(&state, SubscriptionState::Rejected);
                intake.note_observation_incomplete();
                return;
            }
            Err(_) => {}
        }
        // The observation channel is gone while the query is not. Telling the
        // runner is the whole recovery: the backend's tasks are untouched, and
        // no lease and no task terminal decision is involved.
        intake.note_observation_loss();
        failures += 1;
        if failures >= error_budget {
            set_state(&state, SubscriptionState::BudgetExhausted);
            return;
        }
        set_state(&state, SubscriptionState::Resubscribing);
        observe_resubscribe();
        tokio::time::sleep(RESUBSCRIBE_BACKOFF_STEP * failures).await;
    }
}

pub(super) fn apply_task_cursor_reconciliation(
    reconciliation: &mut tokio::sync::watch::Receiver<TaskCursorReconciliation>,
    generation: &mut u64,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
) {
    let latest = reconciliation.borrow_and_update();
    assert!(
        latest.generation > *generation,
        "task cursor reconciliation generations are strictly increasing"
    );
    *generation = latest.generation;
    cursors.clone_from(&latest.cursors);
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum StreamObservation {
    Delivered,
    AwaitTaskReconciliation,
}

pub(super) async fn observe_stream_event(
    participation: ParticipationRef,
    event: &proto::TaskStatusStreamEvent,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
    participation_convergence: Option<&ParticipationConvergenceIntakeHandle>,
    participation_cursor: &mut Option<ParticipationConvergenceCursor>,
) -> Result<StreamObservation, SubscriptionState> {
    let decoded =
        decode_participation_aware_status_event(event, FieldPath::root("task_status_stream_event"))
            .map_err(|error| {
                tracing::warn!(detail = %error, "SubscribeTaskStatus event is malformed");
                SubscriptionState::Rejected
            })?;
    match decoded {
        ParticipationAwareStatusStreamEvent::Status(status) => {
            let identity = status.identity();
            let version = status.version();
            let cursor = cursors
                .get(&identity)
                .copied()
                .unwrap_or_else(|| TaskStatusCursor::unobserved(identity));
            if let Some(current) = cursor.current_version() {
                if version < current {
                    tracing::warn!(
                        task = %identity,
                        current_version = current.get(),
                        event_version = version.get(),
                        "SubscribeTaskStatus event would move its task cursor backwards"
                    );
                    return Err(SubscriptionState::Rejected);
                }
                if version == current {
                    return Ok(StreamObservation::Delivered);
                }
            }
            observe_task_event(
                participation,
                identity,
                StatusEvent::Published(Box::new(status)),
                Some(cursor.advanced_to(version)),
                cursors,
                intake,
            )
        }
        ParticipationAwareStatusStreamEvent::Gone(identity) => observe_task_event(
            participation,
            identity,
            StatusEvent::Gone(identity),
            None,
            cursors,
            intake,
        ),
        ParticipationAwareStatusStreamEvent::ParticipationConvergence(receipt) => {
            observe_participation_convergence(
                participation,
                receipt,
                participation_convergence,
                participation_cursor,
            )
            .await
        }
    }
}

/// Enqueues one observed event and advances its task's cursor only after the
/// bounded intake retained it.
///
/// This is everything the receive path may do. It classifies nothing, opens no
/// edge, completes no stage, and settles no operation.
pub(super) fn observe_task_event(
    participation: ParticipationRef,
    identity: TaskIdentity,
    event: StatusEvent,
    next_cursor: Option<TaskStatusCursor>,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
) -> Result<StreamObservation, SubscriptionState> {
    if identity.query_execution_id() != participation.query_execution_id() {
        tracing::warn!(
            context_execution = ?participation.query_execution_id(),
            event_execution = ?identity.query_execution_id(),
            "SubscribeTaskStatus event addresses a different query execution"
        );
        return Err(SubscriptionState::QueryMismatch);
    }
    if identity.backend_process_id() != participation.backend_process_id() {
        tracing::warn!(
            context_backend = %participation.backend_process_id(),
            event_backend = %identity.backend_process_id(),
            "SubscribeTaskStatus event addresses a different backend process"
        );
        return Err(SubscriptionState::ProcessMismatch);
    }
    match intake.publish(event) {
        StatusIntakeAdmission::Enqueued => {
            if let Some(next_cursor) = next_cursor {
                cursors.insert(identity, next_cursor);
            }
            Ok(StreamObservation::Delivered)
        }
        // The intake has already recorded observation loss and woken the
        // TaskRound. Stop this stream immediately so no later version can
        // leap over the snapshot it could not retain. This is local
        // backpressure, not a network failure, so the subscription loop must
        // not spend its transport error budget or resubscribe from its private
        // cursor. TaskRound will replace it from the state machine's cursor.
        StatusIntakeAdmission::Overflowed => Ok(StreamObservation::AwaitTaskReconciliation),
    }
}

#[cfg(test)]
pub(super) fn observe_event(
    participation: ParticipationRef,
    event: &proto::TaskStatusStreamEvent,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
) -> Result<(), SubscriptionState> {
    let decoded =
        decode_participation_aware_status_event(event, FieldPath::root("task_status_stream_event"))
            .map_err(|_| SubscriptionState::Rejected)?;
    let observed = match decoded {
        ParticipationAwareStatusStreamEvent::Status(status) => {
            let identity = status.identity();
            let cursor = cursors
                .get(&identity)
                .copied()
                .unwrap_or_else(|| TaskStatusCursor::unobserved(identity));
            observe_task_event(
                participation,
                identity,
                StatusEvent::Published(Box::new(status.clone())),
                Some(cursor.advanced_to(status.version())),
                cursors,
                intake,
            )?
        }
        ParticipationAwareStatusStreamEvent::Gone(identity) => observe_task_event(
            participation,
            identity,
            StatusEvent::Gone(identity),
            None,
            cursors,
            intake,
        )?,
        ParticipationAwareStatusStreamEvent::ParticipationConvergence(_) => {
            return Err(SubscriptionState::Rejected);
        }
    };
    match observed {
        StreamObservation::Delivered => Ok(()),
        StreamObservation::AwaitTaskReconciliation => Err(SubscriptionState::Resubscribing),
    }
}

pub(super) async fn observe_participation_convergence(
    participation: ParticipationRef,
    receipt: ParticipationConvergenceReceipt,
    intake: Option<&ParticipationConvergenceIntakeHandle>,
    cursor: &mut Option<ParticipationConvergenceCursor>,
) -> Result<StreamObservation, SubscriptionState> {
    let (Some(intake), Some(current)) = (intake, cursor.as_mut()) else {
        tracing::warn!(
            %participation,
            "a task-only SubscribeTaskStatus stream received a participation convergence event"
        );
        return Err(SubscriptionState::Rejected);
    };
    if receipt.participation() != participation {
        tracing::warn!(
            %participation,
            receipt_context = %receipt.participation(),
            "SubscribeTaskStatus convergence event addresses a different participation"
        );
        return Err(SubscriptionState::Rejected);
    }
    if let Some(version) = current.current_version() {
        if receipt.version() < version {
            tracing::warn!(
                %participation,
                cursor_version = %version,
                receipt_version = %receipt.version(),
                "SubscribeTaskStatus convergence cursor would move backwards"
            );
            return Err(SubscriptionState::Rejected);
        }
        if receipt.version() == version {
            return Ok(StreamObservation::Delivered);
        }
    }

    loop {
        // Read the epoch before publishing. A release that races with the
        // overflow is then visible to `wait_for_capacity_change`; reading it
        // afterwards could wait on an epoch that already incorporated the only
        // capacity transition owed to this receipt.
        let capacity = intake.capacity_state();
        match intake.publish(participation, receipt) {
            Ok(admission) => {
                assert!(admission.authorizes_cursor_advance());
                *current = current.advanced_to(receipt.version());
                return Ok(StreamObservation::Delivered);
            }
            Err(ParticipationConvergencePublishError::Overflow) => {
                let changed = intake.wait_for_capacity_change(capacity.epoch()).await;
                if changed.is_closed() {
                    tracing::warn!(
                        %participation,
                        "participation convergence intake closed while waiting for capacity"
                    );
                    return Err(SubscriptionState::Rejected);
                }
            }
            Err(error) => {
                tracing::warn!(
                    %participation,
                    detail = %error,
                    "SubscribeTaskStatus convergence event was rejected"
                );
                return Err(SubscriptionState::Rejected);
            }
        }
    }
}

pub(super) async fn open_subscription(
    client: &Client,
    participation: ParticipationRef,
    cursors: &BTreeMap<TaskIdentity, TaskStatusCursor>,
    participation_cursor: Option<ParticipationConvergenceCursor>,
) -> Result<tonic::Streaming<proto::TaskStatusStreamEvent>, tonic::Status> {
    let cursors = cursors.values().copied().collect::<Vec<_>>();
    let request = encode_participation_aware_subscribe_task_status(
        participation,
        &cursors,
        participation_cursor,
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let mut grpc = client
        .grpc_with_channel_error(NativeRpcMethod::SubscribeTaskStatus)
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    grpc.subscribe_task_status(tonic::Request::new(request))
        .await
        .map(tonic::Response::into_inner)
}
