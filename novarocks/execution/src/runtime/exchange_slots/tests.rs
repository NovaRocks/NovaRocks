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

use super::*;
use novarocks_execution_contract::ExchangeSource;
use novarocks_types::identity::{AttemptId, QueryId, StageId, TaskId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, LazyLock};
use std::time::Duration;

fn limits(floor: usize, cap: usize, floating: usize) -> ExchangeFlowLimits {
    ExchangeFlowLimits::try_new(floor, cap, floating, 2, Duration::from_secs(30)).unwrap()
}

fn registration(query: i64, ordinal: u32, senders: u32) -> ExchangeSlotRegistration {
    let execution =
        QueryExecutionId::new(QueryId::new(1, query), AttemptId::new(1).unwrap()).unwrap();
    static BACKEND: LazyLock<BackendProcessId> = LazyLock::new(BackendProcessId::new_v7);
    let backend = *BACKEND;
    let destination = TaskIdentity::new(
        execution,
        StageId::new(2).unwrap(),
        TaskId::new(ordinal).unwrap(),
        backend,
    );
    let kernel = UniqueId::new(query, i64::from(ordinal));
    let node = FragmentNodeId::new(7);
    let source_backend = BackendProcessId::new_v7();
    let streams = (0..senders)
        .map(|sender| {
            ExchangeStreamIdentity::try_new(
                ExchangeSource::new(
                    TaskIdentity::new(
                        execution,
                        StageId::new(1).unwrap(),
                        TaskId::new(sender + 1).unwrap(),
                        source_backend,
                    ),
                    UniqueId::new(query + 100, i64::from(sender) + 1),
                    sender,
                ),
                destination,
                kernel,
                node,
            )
            .unwrap()
        })
        .collect();
    ExchangeSlotRegistration {
        task: destination,
        fragment_instance_id: kernel,
        node,
        streams,
        grantable_unit_bytes: 64,
    }
}

fn sequence(n: u64) -> ExchangeUnitSequence {
    ExchangeUnitSequence::try_new(n).unwrap()
}
fn demand(n: u64) -> ExchangeUnitDemand {
    ExchangeUnitDemand {
        sequence: sequence(n),
        wire_bytes: 7,
    }
}
fn unit(n: u64, end_after_unit: bool) -> ExchangeUnitItem {
    ExchangeUnitItem::Unit {
        demand: demand(n),
        end_after_unit,
    }
}
fn claim(
    instance: &ExchangeSlotInstance,
    stream: ExchangeStreamIdentity,
    n: u64,
    end: bool,
) -> ExchangeSlotDecodeLease {
    match instance.begin_admission(stream, unit(n, end)).unwrap() {
        ExchangeSlotAdmission::Decode(lease) => lease,
        other => panic!("expected one decode lease, got {other:?}"),
    }
}
fn commit(lease: ExchangeSlotDecodeLease) -> ExchangeSlotDecodeResult {
    lease.finish_with(|result| result).unwrap()
}
fn transfer(instance: &ExchangeSlotInstance, stream: ExchangeStreamIdentity, n: u64) {
    instance
        .consume_with(|| Some((stream, sequence(n))), || ())
        .unwrap();
}
fn retire(instance: &ExchangeSlotInstance, streams: &[ExchangeStreamIdentity]) {
    let closes: Vec<_> = streams
        .iter()
        .map(|stream| (*stream, ExchangeUnitCloseDisposition::Withdrawn))
        .collect();
    instance.retire_with(&closes, || ()).unwrap();
}

#[test]
fn flow_observation_keeps_retired_decoding_positions_until_actual_lease_exit() {
    let registry = ExchangeSlotRegistry::new(limits(1, 2, 1));
    let config = registration(900, 1, 1);
    let stream = config.streams[0];
    let instance = registry.register(config.clone()).unwrap();
    instance.register_demand(stream, demand(1)).unwrap();
    instance.register_demand(stream, demand(2)).unwrap();
    let before = registry.try_flow_snapshot().unwrap();
    assert_eq!(
        (
            before.in_use,
            before.max_instance_in_use,
            before.floating_in_use
        ),
        (2, 2, 1)
    );
    let lease = claim(&instance, stream, 1, false);
    assert!(matches!(
        instance.begin_admission(stream, unit(1, false)).unwrap(),
        ExchangeSlotAdmission::Observed(ExchangeUnitReceipt {
            verdict: ExchangeAdmissionVerdict::Idempotent,
            ..
        })
    ));
    retire(&instance, &config.streams);
    drop(instance);
    assert_eq!(registry.snapshot().registered_instances, 0);
    let retained = registry.try_flow_snapshot().unwrap();
    assert_eq!(
        (
            retained.live_instances,
            retained.closed_instances,
            retained.in_use,
            retained.decoding
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(retained.replayed_items, 1);
    drop(lease);
    let exited = registry.try_flow_snapshot().unwrap();
    assert_eq!(
        (
            exited.live_instances,
            exited.in_use,
            exited.decoding,
            exited.floating_in_use,
            exited.pending_demands
        ),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(exited.replayed_items, 1);
}

#[test]
fn a_busy_original_ledger_is_unavailable_without_waiting_or_zeroing_usage() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(901, 1, 1);
    let instance = registry.register(config.clone()).unwrap();
    instance
        .register_demand(config.streams[0], demand(1))
        .unwrap();
    let state = instance.instance.state.lock().unwrap();
    assert!(registry.try_flow_snapshot().is_none());
    drop(state);
    assert_eq!(registry.try_flow_snapshot().unwrap().in_use, 1);
    retire(&instance, &config.streams);
    drop(instance);
    assert_eq!(registry.try_flow_snapshot().unwrap().live_instances, 0);
}

#[test]
fn the_last_observation_pin_does_not_wait_for_a_busy_index() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(902, 1, 1);
    let instance = registry.register(config.clone()).unwrap();
    let pin = Arc::clone(&instance.instance);
    retire(&instance, &config.streams);
    drop(instance);
    let observed = registry.core.observed_instances.lock().unwrap();
    drop(pin);
    assert!(
        observed
            .values()
            .all(|instance| instance.upgrade().is_none())
    );
    drop(observed);
    assert_eq!(registry.try_flow_snapshot().unwrap().live_instances, 0);
    assert!(registry.core.observed_instances.lock().unwrap().is_empty());

    // Registrations also reclaim missed removals when no metrics are scraped.
    let instance = registry.register(config.clone()).unwrap();
    retire(&instance, &config.streams);
    let observed = registry.core.observed_instances.lock().unwrap();
    drop(instance);
    drop(observed);
    let next = registry.register(registration(903, 1, 1)).unwrap();
    assert_eq!(registry.core.observed_instances.lock().unwrap().len(), 1);
    assert_eq!(registry.try_flow_snapshot().unwrap().live_instances, 1);
    drop(next);
}

#[test]
fn registration_is_atomic_and_each_instance_progresses_without_floating_slots() {
    let registry = ExchangeSlotRegistry::new(limits(2, 3, 0));
    let first = registration(1, 1, 1);
    let second = registration(2, 2, 1);
    let mut invalid = second.clone();
    invalid.streams.push(invalid.streams[0]);
    assert!(matches!(
        registry.register_batch(vec![first.clone(), invalid]),
        Err(ExchangeSlotError::InvalidRegistration(_))
    ));
    assert_eq!(registry.snapshot().registered_instances, 0);
    assert!(matches!(
        registry.register_batch(vec![first.clone(), first.clone()]),
        Err(ExchangeSlotError::DuplicateInstance(_))
    ));
    assert_eq!(registry.snapshot().registered_instances, 0);
    let configs = [first, second];
    let instances = registry.register_batch(configs.to_vec()).unwrap();
    assert_eq!(registry.snapshot().registered_instances, 2);
    assert!(matches!(
        registry.register(configs[0].clone()),
        Err(ExchangeSlotError::DuplicateInstance(_))
    ));
    assert_eq!(registry.snapshot().registered_instances, 2);
    for (instance, config) in instances.iter().zip(&configs) {
        let stream = config.streams[0];
        instance.register_demand(stream, demand(1)).unwrap();
        instance.register_demand(stream, demand(2)).unwrap();
        assert_eq!(instance.snapshot().in_use, 2);
        assert_eq!(instance.facts(stream).unwrap().granted_through, 2);
    }
    assert_eq!(registry.snapshot().floating_in_use, 0);
    for (instance, config) in instances.iter().zip(&configs) {
        retire(instance, &config.streams);
    }
    assert_eq!(registry.snapshot().registered_instances, 0);
}

#[test]
fn registration_requires_exact_unique_frozen_streams() {
    let registry = ExchangeSlotRegistry::new(limits(1, 2, 0));
    let mut wrong = registration(1, 1, 1);
    wrong.streams.push(wrong.streams[0]);
    assert!(matches!(
        registry.register(wrong),
        Err(ExchangeSlotError::InvalidRegistration(_))
    ));
    let mut wrong = registration(1, 1, 1);
    wrong.task = registration(2, 1, 1).task;
    assert!(matches!(
        registry.register(wrong),
        Err(ExchangeSlotError::InvalidRegistration(_))
    ));
    assert_eq!(registry.snapshot().registered_instances, 0);
}

#[test]
fn a_slow_instance_cannot_borrow_another_instances_floor() {
    let registry = ExchangeSlotRegistry::new(limits(1, 2, 1));
    let first = registration(1, 1, 1);
    let second = registration(2, 2, 1);
    let a = registry.register(first.clone()).unwrap();
    let b = registry.register(second.clone()).unwrap();
    let sa = first.streams[0];
    let sb = second.streams[0];
    a.register_demand(sa, demand(1)).unwrap();
    a.register_demand(sa, demand(2)).unwrap();
    assert_eq!(a.snapshot().in_use, 2);
    assert_eq!(registry.snapshot().floating_in_use, 1);
    assert_eq!(b.register_demand(sb, demand(1)).unwrap().granted_through, 1);
    b.register_demand(sb, demand(2)).unwrap();
    a.register_demand(sa, demand(3)).unwrap();
    commit(claim(&a, sa, 1, false));
    transfer(&a, sa, 1);
    assert_eq!(b.facts(sb).unwrap().granted_through, 2);
    assert_eq!(a.facts(sa).unwrap().granted_through, 2);
    assert_eq!(registry.snapshot().floating_in_use, 1);
    retire(&a, &first.streams);
    retire(&b, &second.streams);
    assert_eq!(registry.snapshot().floating_in_use, 0);
    assert_eq!(registry.snapshot().registered_instances, 0);
}

#[test]
fn floating_slots_rotate_queries_before_another_instance_of_the_same_query() {
    let registry = ExchangeSlotRegistry::new(limits(1, 2, 1));
    let configs = [
        registration(9, 1, 1),
        registration(1, 2, 1),
        registration(1, 3, 1),
        registration(2, 4, 1),
    ];
    let instances = registry.register_batch(configs.to_vec()).unwrap();
    for (instance, config) in instances.iter().zip(&configs) {
        instance
            .register_demand(config.streams[0], demand(1))
            .unwrap();
        instance
            .register_demand(config.streams[0], demand(2))
            .unwrap();
    }
    // Query 9 occupies the floating slot; the wait order is Q1(A,B), Q2(C).
    let release_one = |index: usize| {
        let stream = configs[index].streams[0];
        commit(claim(&instances[index], stream, 1, false));
        transfer(&instances[index], stream, 1);
    };
    release_one(0);
    assert_eq!(
        instances[1]
            .facts(configs[1].streams[0])
            .unwrap()
            .granted_through,
        2
    );
    assert_eq!(
        instances[2]
            .facts(configs[2].streams[0])
            .unwrap()
            .granted_through,
        1
    );
    release_one(1);
    assert_eq!(
        instances[3]
            .facts(configs[3].streams[0])
            .unwrap()
            .granted_through,
        2
    );
    assert_eq!(
        instances[2]
            .facts(configs[2].streams[0])
            .unwrap()
            .granted_through,
        1
    );
    release_one(3);
    assert_eq!(
        instances[2]
            .facts(configs[2].streams[0])
            .unwrap()
            .granted_through,
        2
    );
    for (instance, config) in instances.iter().zip(&configs) {
        retire(instance, &config.streams);
    }
    assert_eq!(registry.snapshot().waiting_instances, 0);
}

#[test]
fn demand_order_is_fifo_within_one_instance() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(1, 1, 3);
    let instance = registry.register(config.clone()).unwrap();
    for stream in &config.streams {
        instance.register_demand(*stream, demand(1)).unwrap();
    }
    let [a, b, c]: [ExchangeStreamIdentity; 3] = config.streams.clone().try_into().unwrap();
    assert_eq!(instance.facts(a).unwrap().granted_through, 1);
    assert_eq!(instance.facts(b).unwrap().granted_through, 0);
    commit(claim(&instance, a, 1, false));
    transfer(&instance, a, 1);
    assert_eq!(instance.facts(b).unwrap().granted_through, 1);
    assert_eq!(instance.facts(c).unwrap().granted_through, 0);
    instance.stop_sender(b).unwrap();
    assert_eq!(instance.facts(c).unwrap().granted_through, 1);
    retire(&instance, &config.streams);
}

#[test]
fn in_progress_replay_has_one_decode_and_stop_fences_late_work() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(1, 1, 1);
    let stream = config.streams[0];
    let instance = registry.register(config.clone()).unwrap();
    instance.register_demand(stream, demand(1)).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let exit = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let work = {
        let instance = instance.clone();
        let entered = Arc::clone(&entered);
        let exit = Arc::clone(&exit);
        let calls = Arc::clone(&calls);
        std::thread::spawn(move || {
            let lease = claim(&instance, stream, 1, false);
            calls.fetch_add(1, Ordering::SeqCst);
            entered.wait();
            exit.wait();
            commit(lease)
        })
    };
    entered.wait();
    assert!(matches!(
        instance.begin_admission(stream, unit(1, false)).unwrap(),
        ExchangeSlotAdmission::Observed(_)
    ));
    assert_eq!(instance.snapshot().decoding, 1);
    instance.stop_sender(stream).unwrap();
    assert!(
        instance
            .register_demand(stream, demand(2))
            .unwrap()
            .sender_stopped
    );
    assert!(matches!(
        instance.begin_admission(stream, unit(1, false)).unwrap(),
        ExchangeSlotAdmission::Observed(_)
    ));
    assert!(matches!(
        instance
            .begin_admission(stream, ExchangeUnitItem::End(sequence(2)))
            .unwrap(),
        ExchangeSlotAdmission::Observed(_)
    ));
    exit.wait();
    assert_eq!(
        work.join().unwrap().disposition,
        ExchangeDecodeCommit::Committed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    transfer(&instance, stream, 1);
    assert_eq!(instance.snapshot().in_use, 0);
    assert_eq!(
        instance.complete_input_consumption(stream),
        Err(ExchangeSlotError::Receipt(
            ExchangeReceiptError::InputNotConsumed
        ))
    );
    retire(&instance, &config.streams);
}

#[test]
fn retirement_keeps_the_decode_slot_until_actual_exit() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(1, 1, 1);
    let stream = config.streams[0];
    let instance = registry.register(config.clone()).unwrap();
    instance.register_demand(stream, demand(1)).unwrap();
    let lease = claim(&instance, stream, 1, true);
    retire(&instance, &config.streams);
    assert!(registry.get(instance.key()).is_none());
    assert_eq!(registry.snapshot().registered_instances, 0);
    assert_eq!(instance.snapshot().in_use, 1);
    let retained = Arc::downgrade(&instance.instance);
    drop(instance);
    assert!(retained.upgrade().is_some());
    let result = commit(lease);
    assert!(retained.upgrade().is_none());
    assert_eq!(result.disposition, ExchangeDecodeCommit::Discarded);
    assert_eq!(result.receipt.facts.custody_through, 0);
    assert_eq!(registry.snapshot().registered_instances, 0);
}

#[test]
fn cleanup_before_consume_refuses_transfer_without_forging_consumption() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(1, 1, 1);
    let stream = config.streams[0];
    let instance = registry.register(config.clone()).unwrap();
    instance.register_demand(stream, demand(1)).unwrap();
    commit(claim(&instance, stream, 1, true));
    let queue = Mutex::new(VecDeque::from([(stream, sequence(1))]));
    let closes = [(stream, ExchangeUnitCloseDisposition::Withdrawn)];
    instance
        .retire_with(&closes, || queue.lock().unwrap().clear())
        .unwrap();
    let transfers = AtomicUsize::new(0);
    let result = instance.consume_with(
        || queue.lock().unwrap().front().copied(),
        || {
            transfers.fetch_add(1, Ordering::SeqCst);
            queue.lock().unwrap().pop_front()
        },
    );
    assert_eq!(
        result,
        Err(ExchangeSlotError::Receipt(ExchangeReceiptError::HandoffGap))
    );
    assert_eq!(transfers.load(Ordering::SeqCst), 0);
    assert_eq!(instance.facts(stream).unwrap().custody_through, 2);
    assert_eq!(
        instance.facts(stream).unwrap().receiver_closed,
        Some(ExchangeUnitCloseDisposition::Withdrawn)
    );
    assert_eq!(registry.snapshot().registered_instances, 0);
}

#[test]
fn early_end_and_last_unit_only_advance_custody_after_the_missing_prefix() {
    for combined in [false, true] {
        let registry = ExchangeSlotRegistry::new(limits(1, 2, 1));
        let config = registration(1, 1, 1);
        let stream = config.streams[0];
        let instance = registry.register(config.clone()).unwrap();
        instance.register_demand(stream, demand(1)).unwrap();
        instance.register_demand(stream, demand(2)).unwrap();
        if !combined {
            instance
                .begin_admission(stream, ExchangeUnitItem::End(sequence(3)))
                .unwrap();
        }
        assert_eq!(
            commit(claim(&instance, stream, 2, combined))
                .receipt
                .facts
                .custody_through,
            0
        );
        assert!(
            instance
                .consume_with(|| Some((stream, sequence(2))), || ())
                .is_err()
        );
        assert_eq!(
            commit(claim(&instance, stream, 1, false))
                .receipt
                .facts
                .custody_through,
            3
        );
        transfer(&instance, stream, 1);
        transfer(&instance, stream, 2);
        instance.complete_input_consumption(stream).unwrap();
        instance
            .retire_with(
                &[(
                    stream,
                    ExchangeUnitCloseDisposition::Consumed {
                        final_sequence: sequence(3),
                    },
                )],
                || (),
            )
            .unwrap();
        assert_eq!(registry.snapshot().registered_instances, 0);
        assert_eq!(registry.snapshot().floating_in_use, 0);
    }
}

#[test]
fn oversized_units_are_refused_without_consuming_a_slot() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(1, 1, 1);
    let stream = config.streams[0];
    let instance = registry.register(config.clone()).unwrap();
    let mut too_large = demand(1);
    too_large.wire_bytes = 65;
    assert!(matches!(
        instance.register_demand(stream, too_large),
        Err(ExchangeSlotError::Receipt(
            ExchangeReceiptError::UnitTooLarge { .. }
        ))
    ));
    assert_eq!(instance.snapshot().in_use, 0);
    assert_eq!(instance.snapshot().pending_demands, 0);
    retire(&instance, &config.streams);
}

#[test]
fn listener_callbacks_reenter_reads_and_retain_the_final_close_facts() {
    struct Listener {
        core: Weak<RegistryCore>,
        seen: Mutex<Vec<ExchangeStreamObservation>>,
    }
    impl ExchangeUnitStateListener for Listener {
        fn changed(&self, observation: ExchangeStreamObservation) {
            let registry = ExchangeSlotRegistry {
                core: self.core.upgrade().unwrap(),
            };
            let _ = registry.snapshot();
            let instance = registry.get(ExchangeReceiverKey {
                fragment_instance_id: observation.stream.destination_fragment_instance_id(),
                node_id: observation.stream.destination_node().get(),
            });
            if observation.facts.receiver_closed.is_some() {
                assert!(
                    instance.is_some(),
                    "the final close is handed off before directory removal"
                );
            }
            if let Some(instance) = instance {
                let _ = instance.facts(observation.stream).unwrap();
            }
            self.seen.lock().unwrap().push(observation);
        }
    }
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let listener = Arc::new(Listener {
        core: Arc::downgrade(&registry.core),
        seen: Mutex::new(Vec::new()),
    });
    registry.set_listener(listener.clone()).unwrap();
    let config = registration(1, 1, 1);
    let stream = config.streams[0];
    let instance = registry.register(config.clone()).unwrap();
    instance.register_demand(stream, demand(1)).unwrap();
    let lease = claim(&instance, stream, 1, false);
    retire(&instance, &config.streams);
    lease.abandon();
    let seen = listener.seen.lock().unwrap();
    assert!(seen.iter().any(|state| state.facts.granted_through == 1));
    assert!(
        seen.iter()
            .any(|state| state.facts.receiver_closed
                == Some(ExchangeUnitCloseDisposition::Withdrawn))
    );
    assert!(
        registry
            .sender_states(stream.source().task().backend_process_id(), None, 1)
            .is_empty()
    );
    assert_eq!(registry.snapshot().registered_instances, 0);
}

#[test]
fn sender_state_pages_are_bounded_and_use_exact_sender_processes() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(1, 1, 3);
    let sender = config.streams[0].source().task().backend_process_id();
    let instance = registry.register(config.clone()).unwrap();
    let first = registry.sender_states(sender, None, 2);
    assert_eq!(first.len(), 2);
    let second = registry.sender_states(sender, Some(first.last().unwrap().stream), 2);
    assert_eq!(second.len(), 1);
    assert!(
        first
            .iter()
            .all(|a| second.iter().all(|b| a.stream < b.stream))
    );
    assert!(
        registry
            .sender_states(BackendProcessId::new_v7(), None, 2)
            .is_empty()
    );
    retire(&instance, &config.streams);
}

#[test]
fn an_invalid_close_set_cannot_close_a_prefix_of_streams() {
    let registry = ExchangeSlotRegistry::new(limits(1, 2, 1));
    let config = registration(1, 1, 2);
    let instance = registry.register(config.clone()).unwrap();
    for stream in &config.streams {
        instance.register_demand(*stream, demand(1)).unwrap();
        commit(claim(&instance, *stream, 1, true));
    }
    transfer(&instance, config.streams[0], 1);
    instance
        .complete_input_consumption(config.streams[0])
        .unwrap();
    let closes: Vec<_> = config
        .streams
        .iter()
        .map(|stream| {
            (
                *stream,
                ExchangeUnitCloseDisposition::Consumed {
                    final_sequence: sequence(2),
                },
            )
        })
        .collect();
    assert!(matches!(
        instance.retire_with(&closes, || ()),
        Err(ExchangeSlotError::Receipt(
            ExchangeReceiptError::InputNotConsumed
        ))
    ));
    for stream in &config.streams {
        assert_eq!(instance.facts(*stream).unwrap().receiver_closed, None);
    }
    retire(&instance, &config.streams);
}

#[derive(Default)]
struct RecordingListener(Mutex<Vec<ExchangeStreamObservation>>);

impl ExchangeUnitStateListener for RecordingListener {
    fn changed(&self, observation: ExchangeStreamObservation) {
        self.0.lock().unwrap().push(observation);
    }
}

#[test]
fn listener_omits_idempotent_register_data_stop_and_close_observations() {
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let listener = Arc::new(RecordingListener::default());
    registry.set_listener(listener.clone()).unwrap();
    let config = registration(1, 1, 1);
    let stream = config.streams[0];
    let instance = registry.register(config.clone()).unwrap();
    let observed = || listener.0.lock().unwrap().len();

    instance.register_demand(stream, demand(1)).unwrap();
    assert_eq!(observed(), 1);
    for _ in 0..3 {
        instance.register_demand(stream, demand(1)).unwrap();
    }
    let first = claim(&instance, stream, 1, false);
    for _ in 0..3 {
        instance.begin_admission(stream, unit(1, false)).unwrap();
    }
    assert_eq!(
        observed(),
        1,
        "a Decoding claim and its replays expose no new fact"
    );
    commit(first);
    assert_eq!(observed(), 2, "custody advancement is observable");
    for _ in 0..3 {
        instance.begin_admission(stream, unit(1, false)).unwrap();
    }
    transfer(&instance, stream, 1);
    assert_eq!(observed(), 2);

    instance.register_demand(stream, demand(2)).unwrap();
    assert_eq!(observed(), 3, "the next grant is observable");
    for _ in 0..3 {
        instance.register_demand(stream, demand(2)).unwrap();
    }
    let second = claim(&instance, stream, 2, true);
    assert_eq!(
        observed(),
        3,
        "seeing the End boundary does not advance custody"
    );
    commit(second);
    assert_eq!(observed(), 4);
    instance.stop_sender(stream).unwrap();
    assert_eq!(observed(), 5, "the first SenderStopped is observable");
    for _ in 0..3 {
        instance.stop_sender(stream).unwrap();
        instance.register_demand(stream, demand(2)).unwrap();
        instance.begin_admission(stream, unit(2, true)).unwrap();
    }
    assert_eq!(observed(), 5);
    retire(&instance, &config.streams);
    assert_eq!(observed(), 6, "the final receiver close is observable");
    for _ in 0..3 {
        retire(&instance, &config.streams);
    }
    assert_eq!(observed(), 6);

    let facts: Vec<_> = listener
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|observation| observation.facts)
        .collect();
    assert!(facts.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(facts[0].granted_through, 1);
    assert_eq!(facts[1].custody_through, 1);
    assert_eq!(facts[2].granted_through, 2);
    assert_eq!(facts[3].custody_through, 3);
    assert!(facts[4].sender_stopped);
    assert_eq!(
        facts[5].receiver_closed,
        Some(ExchangeUnitCloseDisposition::Withdrawn)
    );
}

#[test]
fn listener_omits_out_of_order_decode_and_actual_exit_without_public_progress() {
    let registry = ExchangeSlotRegistry::new(limits(1, 2, 1));
    let listener = Arc::new(RecordingListener::default());
    registry.set_listener(listener.clone()).unwrap();
    let config = registration(1, 1, 1);
    let stream = config.streams[0];
    let instance = registry.register(config.clone()).unwrap();
    instance.register_demand(stream, demand(1)).unwrap();
    instance.register_demand(stream, demand(2)).unwrap();
    let observed = || listener.0.lock().unwrap().len();
    assert_eq!(observed(), 2);
    let second = claim(&instance, stream, 2, true);
    commit(second);
    assert_eq!(
        observed(),
        2,
        "out-of-order decode only changes internal unit state"
    );
    let first = claim(&instance, stream, 1, false);
    retire(&instance, &config.streams);
    assert_eq!(observed(), 3);
    first.abandon();
    assert_eq!(
        observed(),
        3,
        "actual exit returns capacity without repeating close facts"
    );
    assert_eq!(registry.snapshot().registered_instances, 0);
    assert_eq!(registry.snapshot().floating_in_use, 0);
}

#[test]
fn repeated_retire_cannot_remove_the_directory_before_final_fact_handoff() {
    struct BlockingListener {
        entered: Arc<Barrier>,
        exit: Arc<Barrier>,
    }
    impl ExchangeUnitStateListener for BlockingListener {
        fn changed(&self, observation: ExchangeStreamObservation) {
            if observation.facts.receiver_closed.is_some() {
                self.entered.wait();
                self.exit.wait();
            }
        }
    }
    let registry = ExchangeSlotRegistry::new(limits(1, 1, 0));
    let config = registration(1, 1, 1);
    let instance = registry.register(config.clone()).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let exit = Arc::new(Barrier::new(2));
    registry
        .set_listener(Arc::new(BlockingListener {
            entered: Arc::clone(&entered),
            exit: Arc::clone(&exit),
        }))
        .unwrap();
    let work = {
        let instance = instance.clone();
        let streams = config.streams.clone();
        std::thread::spawn(move || retire(&instance, &streams))
    };
    entered.wait();
    retire(&instance, &config.streams);
    assert!(registry.get(instance.key()).is_some());
    assert!(instance.snapshot().closed);
    exit.wait();
    work.join().unwrap();
    assert!(registry.get(instance.key()).is_none());
    assert_eq!(registry.snapshot().registered_instances, 0);
}
