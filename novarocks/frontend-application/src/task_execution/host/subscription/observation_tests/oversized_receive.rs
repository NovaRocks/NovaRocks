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

use super::*;
use crate::task_execution::host::subscriptions::{
    SubscriptionFailureCustody, SubscriptionFailureRecord,
};
use novarocks_execution_contract::{ParticipationState, QuiesceParticipationReceipt, TaskIdentity};
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, encode_covered_status_event,
};
use std::error::Error;

#[test]
fn oversized_received_frame_keeps_its_original_cut_until_failure_retires_and_credit_returns() {
    // Unexpected assertion/borrow cancellation retains the original fixture.
    // Success uses its existing actual role/header/actor/Scope cleanup.
    let original = std::mem::ManuallyDrop::new(Fixture::new(1, 1));
    let f = &*original;
    let max_frame_bytes = f.bank.budget.max_frame_bytes;
    let tasks = (1..=64)
        .map(|id| {
            TaskIdentity::new(
                f.key.participation.query_execution_id(),
                StageId::new(1).unwrap(),
                TaskId::new(id).unwrap(),
                f.key.participation.backend_process_id(),
            )
        })
        .collect();
    let quiesce = QuiesceParticipationReceipt::new(
        f.key.participation,
        1,
        tasks,
        ParticipationState::Quiescing,
    );
    let retained_task_bytes = std::mem::size_of_val(quiesce.accepted_tasks());
    let wire = encode_covered_status_event(&CoveredStatusStreamEvent {
        fact: CoveredStatusStreamFact::Quiesce(quiesce.clone()),
        source_revision: Some(1),
    })
    .unwrap();
    let encoded = prost::Message::encoded_len(&wire);
    drop(wire);
    let event = ObservationEvent::Fact {
        source_revision: Some(1),
        fact: ObservationFact::Quiesce(Arc::new(quiesce)),
    };
    let mut credit = Some(f.bank.credit(false, Some(f.key)).unwrap());
    let mut network = ready(());
    let receipt = match f.input.poll_receive(
        f.key,
        &mut credit,
        Pin::new(&mut network),
        &mut Context::from_waker(Waker::noop()),
    ) {
        Poll::Ready(Ok((receipt, ()))) => receipt,
        _ => panic!("the same original network Ready must own its pending receipt"),
    };
    f.input.root_control(f.bank.seal()).unwrap();
    let error = match f.input.complete_receive(&receipt, event, encoded) {
        Err(error) => error,
        Ok(()) => {
            std::mem::ManuallyDrop::into_inner(original).finish();
            panic!("the real retained event must exceed the frozen frame budget");
        }
    };
    let oversized = matches!(error, ObservationError::Oversized {charged,reserved}
        if reserved == max_frame_bytes && charged > reserved && charged >= retained_task_bytes);
    let pending_original = {
        let state = f.input.0.state.lock().unwrap();
        state.in_flight.is_none()
            && state.queue.iter().any(|entry| {
                matches!(&entry.value,
            EntryValue::Pending {receipt: same,..} if Arc::ptr_eq(same, &receipt.identity))
            })
    };
    let blocked_before_failure = f
        .bank
        .poll_read(f.key, &mut Context::from_waker(Waker::noop()))
        .is_pending();
    f.bank.cancel_read_wait(f.key);
    let failures = SubscriptionFailureRecord::new(
        novarocks_coordination_contract::AttemptActionId::new(NonZeroU64::MIN),
        f.key,
    );
    let failure = failures.ingress_rejected(f.key, error);
    let reference = failure.cause.original;
    let typed = failure.kind
        == novarocks_attempt_protocol::input::ObservationFailureKind::ProtocolViolation;
    f.input
        .complete_receive(&receipt, ObservationEvent::Failed(failure), 0)
        .unwrap();
    let input = f
        .input
        .0
        .state
        .lock()
        .unwrap()
        .in_flight
        .as_ref()
        .unwrap()
        .input;
    f.runtime.poll_one(Tick(1)).unwrap();
    let consumed_keeps_credit = f
        .bank
        .poll_read(f.key, &mut Context::from_waker(Waker::noop()))
        .is_pending();
    f.bank.cancel_read_wait(f.key);
    let before_retirement = {
        let seen = f.seen.lock().unwrap();
        seen.len() == 1 && matches!(&seen[0].1, Seen::Observations(values) if values == &[u64::MAX])
    };
    f.runtime.retire_input(actor(), input).unwrap();
    f.input.input_retired(input).unwrap();
    let returned = match f
        .bank
        .poll_read(f.key, &mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(credit)) => Some(credit),
        _ => None,
    };
    let loanable = returned.is_some();
    drop(returned);
    f.bank.cancel_read_wait(f.key);
    f.consume(true);
    let failure_before_control = {
        let seen = f.seen.lock().unwrap();
        seen.len() == 2
            && matches!(&seen[0].1, Seen::Observations(values) if values == &[u64::MAX])
            && seen[1].1 == Seen::Control
    };
    let retained = failures.take(reference).unwrap();
    let original_error = retained
        .source()
        .and_then(|e| e.downcast_ref::<ObservationError>())
        == Some(&error);
    let once = failures.take(reference).is_none();
    drop((retained, failures));
    std::mem::ManuallyDrop::into_inner(original).finish();

    assert!(retained_task_bytes > max_frame_bytes && encoded != 0);
    assert!(oversized && pending_original && typed && original_error && once);
    assert!(blocked_before_failure && consumed_keeps_credit);
    assert!(before_retirement && failure_before_control && loanable);
}
