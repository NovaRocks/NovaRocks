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

//! Factory-shared ownership of exact outbound streams for all sink drivers.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use novarocks_execution_contract::{ExchangeDeliveryRequirement, ExchangeStreamIdentity};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};

use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use crate::runtime::observable::Observable;
use crate::runtime::profile::{OperatorProfiles, ProfileUnit, clamp_u128_to_i64};
use crate::runtime::runtime_state::RuntimeErrorState;

use super::ExchangeFrameTransmitter;
use super::exchange_edge::{
    EdgeSendGate, EdgeSendState, ExchangeDestinationKey, ExchangeEdgeGates,
};
use super::exchange_stream_sender::{
    ExchangeSenderStopState, ExchangeStreamSender, ExchangeStreamSenderConfig,
    ExchangeStreamSenderStatus, ExchangeUnitPublishOutcome, ExchangeUnitSendScheduler,
    ExchangeUnitSenderError,
};

#[derive(Debug)]
struct GroupContract(&'static str);
impl std::fmt::Display for GroupContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for GroupContract {}
impl FailureProjection for GroupContract {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::ExchangeStreamContract.reason(),
        )
    }
}
fn contract(detail: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(GroupContract(detail))
}

#[derive(Debug)]
struct DeliveryFailure(FailureFact);
impl std::fmt::Display for DeliveryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("exchange stream delivery failed")
    }
}
impl std::error::Error for DeliveryFailure {}
impl FailureProjection for DeliveryFailure {
    fn failure_fact(&self) -> FailureFact {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupPublication {
    Published,
    Backpressured,
    Withdrawn,
}

struct Entry {
    config: ExchangeStreamSenderConfig,
    sender: Arc<ExchangeStreamSender>,
    gate: Arc<EdgeSendGate>,
    // 0=unpublished, 1=one publisher in progress, 2=published. This is local
    // End publication ownership, never an independent delivery/Task verdict.
    end: AtomicU8,
    failure: Arc<OnceLock<ExecutionFailure>>,
}
struct Binding {
    control: Arc<RuntimeErrorState>,
    transmitter: Arc<dyn ExchangeFrameTransmitter>,
    scheduler: Arc<ExchangeUnitSendScheduler>,
    gates: Arc<ExchangeEdgeGates>,
    entries: BTreeMap<ExchangeStreamIdentity, Arc<Entry>>,
}
#[derive(Default)]
struct State {
    binding: Option<Binding>,
    observing: bool,
    observe_again: bool,
}

pub(crate) struct ExchangeSenderGroup {
    observable: Arc<Observable>,
    state: Mutex<State>,
    first_failure: Arc<OnceLock<ExecutionFailure>>,
    profiles: Arc<Mutex<Option<OperatorProfiles>>>,
}

impl ExchangeSenderGroup {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new_cyclic(|weak: &std::sync::Weak<Self>| {
            let observable = Arc::new(Observable::new());
            let owner = weak.clone();
            observable.add_observer(Arc::new(move || {
                if let Some(owner) = owner.upgrade() {
                    owner.observe();
                }
            }));
            Self {
                observable,
                state: Mutex::new(State::default()),
                first_failure: Arc::new(OnceLock::new()),
                profiles: Arc::new(Mutex::new(None)),
            }
        })
    }
    pub(crate) fn observable(&self) -> &Arc<Observable> {
        &self.observable
    }

    /// Observation only. Busy is unavailable; never wait for a sender or
    /// clone a sender/claim/payload merely to produce a diagnostic marker.
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn try_diagnostic_state(&self) -> Option<String> {
        let state = self.state.try_lock().ok()?;
        let Some(binding) = &state.binding else {
            return Some("unbound".into());
        };
        let mut samples = Vec::with_capacity(binding.entries.len());
        for (stream, entry) in &binding.entries {
            samples.push((
                *stream,
                entry.gate.state(),
                entry.end.load(Ordering::Acquire),
                entry.sender.try_diagnostic_snapshot()?,
            ));
        }
        Some(format!("{samples:?}"))
    }

    /// The stream group contributes its transport counters once to the
    /// factory's operator profile, regardless of how many drivers publish.
    pub(crate) fn install_profiles(&self, profiles: OperatorProfiles) {
        let mut current = self.profiles.lock().expect("exchange group profile lock");
        if current.is_none() {
            *current = Some(profiles);
        }
    }

    // All drivers bind this same original Task/runtime and frozen roster. No
    // sender performs I/O at construction; release the group lock before any
    // operation which may synchronously call transport, observers or control.
    pub(crate) fn bind(
        self: &Arc<Self>,
        control: Arc<RuntimeErrorState>,
        configs: Vec<ExchangeStreamSenderConfig>,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
        scheduler: Arc<ExchangeUnitSendScheduler>,
        gates: Arc<ExchangeEdgeGates>,
    ) -> ExecutionResult<()> {
        let task = control
            .task_identity()
            .ok_or_else(|| contract("exchange sender requires its original Task owner"))?;
        let mut state = self.state.lock().expect("exchange sender group lock");
        if let Some(bound) = &state.binding {
            if !Arc::ptr_eq(&bound.control, &control)
                || !Arc::ptr_eq(&bound.transmitter, &transmitter)
                || !Arc::ptr_eq(&bound.scheduler, &scheduler)
                || !Arc::ptr_eq(&bound.gates, &gates)
                || bound.entries.len() != configs.len()
                || configs.iter().any(|config| {
                    bound
                        .entries
                        .get(&config.stream)
                        .is_none_or(|entry| !same_config(&entry.config, config))
                })
            {
                return Err(contract(
                    "exchange sender factory cannot rebind its frozen Task/runtime or streams",
                ));
            }
            return Ok(());
        }
        let measured: Arc<dyn ExchangeFrameTransmitter> = Arc::new(MeasuredTransmitter {
            inner: transmitter.clone(),
            profiles: self.profiles.clone(),
        });
        let mut entries = BTreeMap::new();
        for config in configs {
            if config.stream.source().task() != task || entries.contains_key(&config.stream) {
                return Err(contract(
                    "exchange sender roster has a foreign or duplicate stream",
                ));
            }
            let key = ExchangeDestinationKey::new(
                config.stream.destination_fragment_instance_id(),
                config.stream.destination_node().get(),
            );
            let gate = gates
                .gate_for_destination(key)
                .cloned()
                .ok_or_else(|| contract("exchange stream has no exact frozen destination gate"))?;
            let sender_observable = Arc::new(Observable::new());
            let sender = ExchangeStreamSender::try_new(
                config.clone(),
                measured.clone(),
                scheduler.clone(),
                sender_observable.clone(),
            )
            .map_err(ExecutionFailure::from_domain)?;
            let failure = Arc::new(OnceLock::new());
            let original_control = control.clone();
            let sender_view = Arc::downgrade(&sender);
            let failure_view = failure.clone();
            let first_failure = self.first_failure.clone();
            let group_observable = Arc::downgrade(&self.observable);
            // Real callback ownership retains its original Task even after the
            // factory group exits. All edges back to sender/group are weak.
            sender_observable.add_observer(Arc::new(move || {
                if let Some(sender) = sender_view.upgrade()
                    && let Some(fact) = sender.failure()
                {
                    let error = ExecutionFailure::from_domain(DeliveryFailure(fact));
                    if failure_view.set(error.clone()).is_ok() {
                        let _ = first_failure.set(error.clone());
                        original_control.set_error(error);
                    }
                }
                if let Some(observable) = group_observable.upgrade() {
                    observable.defer_notify().arm();
                }
            }));
            entries.insert(
                config.stream,
                Arc::new(Entry {
                    config,
                    sender,
                    gate,
                    end: AtomicU8::new(0),
                    failure,
                }),
            );
        }
        let wake_gates = gates.clone();
        state.binding = Some(Binding {
            control,
            transmitter,
            scheduler,
            gates,
            entries,
        });
        drop(state);
        wake_gates.register_open_waiter(&self.observable);
        self.observe();
        Ok(())
    }
    fn entry(&self, stream: ExchangeStreamIdentity) -> ExecutionResult<Arc<Entry>> {
        self.state
            .lock()
            .expect("exchange sender group lock")
            .binding
            .as_ref()
            .and_then(|bound| bound.entries.get(&stream))
            .cloned()
            .ok_or_else(|| contract("exchange stream was not bound to its sink factory"))
    }
    fn entries(&self) -> Vec<Arc<Entry>> {
        self.state
            .lock()
            .expect("exchange sender group lock")
            .binding
            .as_ref()
            .map_or_else(Vec::new, |bound| bound.entries.values().cloned().collect())
    }
    pub(crate) fn failure(&self) -> Option<ExecutionFailure> {
        self.first_failure.get().cloned()
    }
    pub(crate) fn can_publish(&self, stream: ExchangeStreamIdentity) -> bool {
        self.entry(stream).is_ok_and(|entry| {
            let snapshot = entry.sender.snapshot();
            // Definitive terminal/AfterEnd outcomes must remain runnable so
            // the driver surfaces them, rather than parking without a wake.
            snapshot.status != ExchangeStreamSenderStatus::Open
                || snapshot.retention.end_sequence.is_some()
                || snapshot.pending_grant.is_none()
        })
    }
    pub(crate) fn has_end(&self, stream: ExchangeStreamIdentity) -> bool {
        self.entry(stream)
            .is_ok_and(|entry| entry.sender.snapshot().retention.end_sequence.is_some())
    }

    pub(crate) fn backend_number(&self, stream: ExchangeStreamIdentity) -> ExecutionResult<i32> {
        Ok(self.entry(stream)?.config.backend_number)
    }
    pub(crate) fn publish(
        self: &Arc<Self>,
        stream: ExchangeStreamIdentity,
        payload: &Bytes,
        end: bool,
    ) -> ExecutionResult<GroupPublication> {
        if let Some(failure) = self.failure() {
            return Err(failure);
        }
        let entry = self.entry(stream)?;
        if entry.sender.snapshot().status == ExchangeStreamSenderStatus::Withdrawn {
            return Ok(GroupPublication::Withdrawn);
        }
        if end
            && entry
                .end
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return if entry.end.load(Ordering::Acquire) == 1 {
                Ok(GroupPublication::Backpressured)
            } else {
                Err(contract(
                    "exchange data cannot republish an already published End",
                ))
            };
        }
        let result = entry.sender.publish(payload, end);
        if end {
            entry.end.store(
                if matches!(result, Ok(ExchangeUnitPublishOutcome::Published(_))) {
                    2
                } else {
                    0
                },
                Ordering::Release,
            );
        }
        match result {
            Ok(ExchangeUnitPublishOutcome::Published(_)) => Ok(GroupPublication::Published),
            Ok(ExchangeUnitPublishOutcome::Backpressured)
            | Err(ExchangeUnitSenderError::Kernel(
                novarocks_execution_contract::ordered_retained_stream::StreamError::WindowFull,
            )) => Ok(GroupPublication::Backpressured),
            Err(ExchangeUnitSenderError::Stopped) => self.stopped_publication(&entry),
            Err(error) => Err(ExecutionFailure::from_domain(error)),
        }
    }
    pub(crate) fn finish(
        self: &Arc<Self>,
        stream: ExchangeStreamIdentity,
    ) -> ExecutionResult<GroupPublication> {
        if let Some(failure) = self.failure() {
            return Err(failure);
        }
        let entry = self.entry(stream)?;
        if entry.sender.snapshot().status == ExchangeStreamSenderStatus::Withdrawn {
            return Ok(GroupPublication::Withdrawn);
        }
        match entry
            .end
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        {
            Err(2) => return Ok(GroupPublication::Published),
            Err(_) => return Ok(GroupPublication::Backpressured),
            Ok(_) => {}
        }
        let result = entry.sender.finish();
        entry
            .end
            .store(if result.is_ok() { 2 } else { 0 }, Ordering::Release);
        match result {
            Ok(_) => Ok(GroupPublication::Published),
            Err(ExchangeUnitSenderError::Stopped) => self.stopped_publication(&entry),
            Err(error) => Err(ExecutionFailure::from_domain(error)),
        }
    }
    pub(crate) fn is_idle(&self) -> bool {
        self.entries().iter().all(|entry| {
            let snapshot = entry.sender.snapshot();
            snapshot.active_calls == 0
                && snapshot.stop_obligation != ExchangeSenderStopState::Pending
                && (!matches!(snapshot.status, ExchangeStreamSenderStatus::Failed(_))
                    || (entry.failure.get().is_some() && self.first_failure.get().is_some()))
                && (snapshot.status != ExchangeStreamSenderStatus::Open
                    || snapshot.retention.produced_through == snapshot.retention.consumed_through)
        })
    }

    fn stopped_publication(&self, entry: &Entry) -> ExecutionResult<GroupPublication> {
        let snapshot = entry.sender.snapshot();
        if matches!(snapshot.status, ExchangeStreamSenderStatus::Failed(_)) {
            // Failure publication releases the sender lock before notifying
            // its original Task. Let that same occurrence become observable;
            // do not win the Task race with a newly invented Stopped cause.
            return match self.failure() {
                Some(failure) => Err(failure),
                None => Ok(GroupPublication::Backpressured),
            };
        }
        if snapshot.status == ExchangeStreamSenderStatus::Withdrawn
            || (snapshot.status == ExchangeStreamSenderStatus::Stopped
                && entry.config.delivery == ExchangeDeliveryRequirement::DemandDriven
                && entry.gate.is_normally_canceled())
        {
            // A gate may close after the driver's Open snapshot. This retires
            // only publication; any Pending Stop still prevents group exit.
            return Ok(GroupPublication::Withdrawn);
        }
        Err(ExecutionFailure::from_domain(
            ExchangeUnitSenderError::Stopped,
        ))
    }
    pub(crate) fn retained_payload_bytes(&self) -> u128 {
        self.entries()
            .iter()
            .map(|entry| entry.sender.snapshot().retention.payload_bytes as u128)
            .sum()
    }
    pub(crate) fn cancel(self: &Arc<Self>) {
        for entry in self.entries() {
            entry.sender.stop();
        }
    }
    fn observe(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().expect("exchange sender group lock");
            if state.observing {
                state.observe_again = true;
                return;
            }
            state.observing = true;
        }
        loop {
            let binding = {
                let state = self.state.lock().expect("exchange sender group lock");
                state.binding.as_ref().map(|bound| {
                    (
                        bound.control.clone(),
                        bound.gates.clone(),
                        bound.entries.values().cloned().collect::<Vec<_>>(),
                    )
                })
            };
            if let Some((control, gates, entries)) = binding {
                for entry in entries {
                    let snapshot = entry.sender.snapshot();
                    // Receiver-authored Withdrawn has already been validated
                    // against delivery by the sender. Close only its exact gate.
                    if snapshot.status == ExchangeStreamSenderStatus::Withdrawn
                        && !entry.gate.is_normally_canceled()
                    {
                        // Integration adds this operation to the gate owner,
                        // replacing the old process queue's gate mutation.
                        let key = ExchangeDestinationKey::new(
                            entry.config.stream.destination_fragment_instance_id(),
                            entry.config.stream.destination_node().get(),
                        );
                        if let Err(error) = gates.close_destination_normally(key) {
                            let error = ExecutionFailure::from_domain(error);
                            if self.first_failure.set(error.clone()).is_ok() {
                                control.set_error(error);
                            }
                        }
                    }
                    // Normal withdrawal is not route retirement. An unknown
                    // Stop remains Pending until precise receiver evidence or
                    // a separate authoritative lifecycle retirement arrives.
                    // MustDrain keeps observing already published delivery
                    // until the receiver's exact Consumed/Withdrawn facts.
                    if entry.gate.state() == EdgeSendState::NormallyCanceled
                        && entry.config.delivery == ExchangeDeliveryRequirement::DemandDriven
                        && snapshot.status == ExchangeStreamSenderStatus::Open
                    {
                        entry.sender.stop();
                    }
                }
            }
            let mut state = self.state.lock().expect("exchange sender group lock");
            if state.observe_again {
                state.observe_again = false;
            } else {
                state.observing = false;
                break;
            }
        }
    }
}

fn same_config(a: &ExchangeStreamSenderConfig, b: &ExchangeStreamSenderConfig) -> bool {
    a.stream == b.stream
        && a.destination == b.destination
        && a.sender_count == b.sender_count
        && a.sender_id == b.sender_id
        && a.backend_number == b.backend_number
        && a.delivery == b.delivery
        && a.retained_window == b.retained_window
        && a.max_unit_bytes == b.max_unit_bytes
        && a.retry_delay == b.retry_delay
}

/// Reuses the same actual Native connection owner. Only data RPC attempts
/// contribute the existing sink counters; control registration and subscription
/// remain independent of data activity and never reserve a data permit.
struct MeasuredTransmitter {
    inner: Arc<dyn ExchangeFrameTransmitter>,
    profiles: Arc<Mutex<Option<OperatorProfiles>>>,
}
impl ExchangeFrameTransmitter for MeasuredTransmitter {
    fn normal_close_accepted(&self, notice: super::exchange::ExchangeNormalCloseAccepted) {
        self.inner.normal_close_accepted(notice);
    }
    fn deliver_unit(
        &self,
        frame: super::exchange_unit::ExchangeUnitFrame,
        stop: tokio_util::sync::CancellationToken,
        activity: super::exchange_unit::ExchangeCallActivity,
        done: super::exchange_unit::ExchangeDeliveryCallback,
    ) {
        let bytes = frame.receiver.payload.len();
        let profiles = self.profiles.clone();
        let started = std::time::Instant::now();
        self.inner.deliver_unit(
            frame,
            stop,
            activity,
            Box::new(move |outcome| {
                let elapsed = started.elapsed().as_nanos();
                let profile = profiles
                    .lock()
                    .expect("exchange group profile lock")
                    .clone();
                if let Some(profile) = profile {
                    profile
                        .common
                        .counter_add("RequestSent", ProfileUnit::Unit, 1);
                    profile.common.counter_add(
                        "BytesSent",
                        ProfileUnit::Bytes,
                        clamp_u128_to_i64(bytes as u128),
                    );
                    profile.unique.counter_add(
                        "NetworkTime",
                        ProfileUnit::TimeNs,
                        clamp_u128_to_i64(elapsed),
                    );
                    profile.common.counter_add(
                        "OverallTime",
                        ProfileUnit::TimeNs,
                        clamp_u128_to_i64(elapsed),
                    );
                }
                if let super::exchange_unit::ExchangeCallOutcome::Observed(receipt) = &outcome
                    && receipt.facts.receiver_closed
                        != Some(novarocks_execution_contract::ExchangeCloseDisposition::Withdrawn)
                {
                    super::exchange_metrics::observe_exchange_shuffle_bytes(bytes);
                }
                done(outcome);
            }),
        );
    }
    fn register_unit_demands(
        &self,
        destination: crate::runtime::endpoint::RuntimeEndpoint,
        entries: Vec<super::exchange_unit::ExchangeDemandEntry>,
        stop: tokio_util::sync::CancellationToken,
        done: super::exchange_unit::ExchangeDemandCallback,
    ) {
        self.inner
            .register_unit_demands(destination, entries, stop, done);
    }
    fn watch_unit_facts(
        &self,
        destination: crate::runtime::endpoint::RuntimeEndpoint,
        stream: ExchangeStreamIdentity,
        wake: super::exchange_unit::ExchangeGrantWake,
    ) -> Result<(), FailureFact> {
        self.inner.watch_unit_facts(destination, stream, wake)
    }
    fn forget_unit_facts(&self, stream: ExchangeStreamIdentity) {
        self.inner.forget_unit_facts(stream);
    }
    fn schedule_unit_retry(
        &self,
        delay: std::time::Duration,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), FailureFact> {
        self.inner.schedule_unit_retry(delay, task)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(novarocks_dev_controls)]
    #[test]
    fn busy_diagnostic_sampling_does_not_wait_or_report_an_empty_group() {
        let group = super::ExchangeSenderGroup::new();
        let guard = group.state.lock().unwrap();
        assert!(group.try_diagnostic_state().is_none());
        drop(guard);
        assert_eq!(group.try_diagnostic_state().as_deref(), Some("unbound"));
    }
    use super::super::exchange::test_support::Fixture;
    use super::super::{ExchangeReceiverKey, ExchangeReceiverRegistration};
    use super::*;
    use crate::task_execution::domain::{EdgeOpenVersion, ExchangeEdgeId};
    use novarocks_types::UniqueId;

    #[test]
    fn gate_withdrawal_between_driver_snapshot_and_publication_is_not_a_failure() {
        let fixture = Fixture::new();
        let source = UniqueId::new(811, 1);
        let destination = UniqueId::new(812, 1);
        let key = ExchangeReceiverKey {
            fragment_instance_id: destination,
            node_id: 7,
        };
        let factory = fixture.creation(destination);
        let declaration = crate::exec::chunk::typed_fixture::layout(
            Arc::new(arrow::datatypes::Schema::empty()),
            &[],
            &[],
        );
        fixture
            .port
            .register({
                let expected_chunk_schema =
                    factory.create_compiled_chunk_schema(&declaration).unwrap();
                ExchangeReceiverRegistration {
                    key,
                    expected_senders: 1,
                    wire_chunk_schema: expected_chunk_schema.clone(),
                    expected_chunk_schema,
                    column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
                    creation_factory: factory,
                }
            })
            .unwrap();
        fixture.bind(key, &[source]);
        let stream = fixture.stream(source, destination, key.node_id, 0);
        let destination_key = ExchangeDestinationKey::new(destination, key.node_id);
        let edge = ExchangeEdgeId::new(1).unwrap();
        let gates = ExchangeEdgeGates::try_new([(edge, vec![destination_key])]).unwrap();
        gates
            .open(EdgeOpenVersion::new(1).unwrap(), &[edge])
            .unwrap();
        let group = ExchangeSenderGroup::new();
        group.bind(
            fixture.state(source).error_state(),
            vec![ExchangeStreamSenderConfig {
                stream,
                destination: crate::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 9090).unwrap(),
                sender_count: 1, sender_id: 0, backend_number: 1,
                delivery: ExchangeDeliveryRequirement::DemandDriven,
                retained_window: fixture.runtime.exchange_flow_limits().stream_window(),
                max_unit_bytes: novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
                retry_delay: std::time::Duration::from_millis(10),
            }],
            fixture.transport.clone(), fixture.runtime.exchange_unit_scheduler(), gates.clone(),
        ).unwrap();
        // The driver already observed Open. The real gate-owner operation can
        // run before either local publication boundary is entered.
        assert_eq!(
            gates.gate_for_destination(destination_key).unwrap().state(),
            EdgeSendState::Open
        );
        gates.close_destination_normally(destination_key).unwrap();
        assert_eq!(
            group.entry(stream).unwrap().sender.snapshot().status,
            ExchangeStreamSenderStatus::Stopped
        );
        assert_eq!(
            group
                .publish(stream, &Bytes::from_static(b"never sent"), false)
                .unwrap(),
            GroupPublication::Withdrawn
        );
        assert_eq!(group.finish(stream).unwrap(), GroupPublication::Withdrawn);
        let snapshot = group.entry(stream).unwrap().sender.snapshot();
        assert_eq!(snapshot.retention.produced_through, 0);
        assert_eq!(
            snapshot.stop_obligation,
            ExchangeSenderStopState::NotRequired
        );
        assert!(group.failure().is_none());
        assert!(fixture.state(source).error_state().error().is_none());
        assert!(group.is_idle());
        fixture.port.cancel(key);
        fixture.port.remove(key);
    }
}
