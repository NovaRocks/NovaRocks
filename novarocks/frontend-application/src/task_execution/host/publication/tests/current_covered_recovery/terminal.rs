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

//! Same Core closing ACK versus actual H2 participation receipt. No LP Ready,
//! Native Create, actual Worker implementation or M5 assertion is made here.
use super::*;
use novarocks_attempt_protocol::port_output::AbortIssueSettlement;
use novarocks_coordination_contract::{
    AttemptStopReason, ExecutionFailure, ExecutionFailureSite, QueryFailureReason,
};
use novarocks_execution_contract::{
    OperationOutcome, ParticipationConvergenceReceipt, ParticipationConvergenceState,
    ParticipationConvergenceVersion,
};

fn close_pending(rig: &Rig, generation: NonZeroU64) -> bool {
    rig.trace.lock().unwrap().effects.iter().any(|effect| {
        matches!(&effect.body,
        EffectBody::CloseSubscription {participation,generation:original}
            if *participation==rig.participation&&*original==generation)
    })
}
fn apply_original_close(
    rig: &mut Rig,
    source: &Arc<SubscriptionSource<FrontendActor>>,
    generation: NonZeroU64,
) {
    let effect = {
        let mut trace = rig.trace.lock().unwrap();
        let index = trace
            .effects
            .iter()
            .position(|effect| {
                matches!(&effect.body,
            EffectBody::CloseSubscription {participation,generation:original}
                if *participation==rig.participation&&*original==generation)
            })
            .expect("the same original Core actually requested this Close");
        trace.effects.remove(index).unwrap()
    };
    let EffectBody::CloseSubscription {
        participation,
        generation: original,
    } = effect.body
    else {
        unreachable!()
    };
    assert_eq!((participation, original), (rig.participation, generation));
    assert!(source.matches(participation, rig.roots.as_ref().unwrap().binding()));
    source.close_generation(generation).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_terminal_cleanup_keeps_original_stream_until_actual_participation_receipt() {
    let failure =
        ExecutionFailure::from_input(novarocks_attempt_protocol::port_projection::failure_input(
            QueryFailureReason::PreparationProtocol,
            "original residual subscription terminal",
            ExecutionFailureSite::Coordinator,
        ));
    let mut retained = Retained(Some(Box::new(Run {
        rig: Rig::with_mode(SeedMode::Terminal(failure.clone())),
        peer: None,
        native: None,
        control: None,
        root: None,
        driver: None,
        capacity: None,
        window: None,
        cohort: None,
        source: None,
        jobs: [None, None],
    })));
    let c = retained.0.as_mut().unwrap();
    let rig = &mut c.rig;
    assert_eq!(
        rig.trace
            .lock()
            .unwrap()
            .effects
            .iter()
            .filter(|effect| matches!(&effect.body,
        EffectBody::Port(port) if matches!(&port.body,AttemptToLogicalBody::AbortSettlement {
            settlement:AbortIssueSettlement::Worker(OperationOutcome::Accepted),..})))
            .count(),
        1
    );
    assert!(
        !close_pending(rig, n(1)),
        "the exact closing ACK supplied no actual Worker participation proof"
    );
    let (effect, request) = rig
        .take_open(n(1))
        .expect("same Core's original residual Open1");
    assert!(request.participation_cursor.is_none());
    c.peer = Some(Loopback::start().await);
    let peer = c.peer.as_ref().unwrap();
    peer.expect_covered_frames_then_hold(vec![frame(
        CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
            generation: 1,
            initial_cut: 0,
        }),
        None,
    )]);
    peer.expect_covered_frames_then_hold(Vec::new());
    let (control, driver, capacity) = wc::fixture();
    c.control = Some(control);
    c.driver = Some(driver);
    c.capacity = Some(capacity);
    c.root = Some(
        c.control
            .as_ref()
            .unwrap()
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap(),
    );
    let scope = c.root.as_ref().unwrap().owner.scope();
    wc::bind(&scope, c.driver.as_mut().unwrap());
    c.window = Some(
        c.capacity
            .as_ref()
            .unwrap()
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap(),
    );
    c.cohort = Some(wc::cohort(&scope, 1, c.driver.as_mut().unwrap()));
    c.source = Some(
        SubscriptionSource::from_cohort(
            rig.participation,
            &scope,
            c.cohort.as_ref().unwrap(),
            rig.ingress.as_ref().unwrap().clone(),
        )
        .unwrap(),
    );
    let source = c.source.as_ref().unwrap();
    let source_scope = source.scope().unwrap();
    c.native = Some(FrontendDataRuntime::new(Handle::current()));
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(rig.participation.backend_process_id())]),
        0,
        c.native.as_ref().unwrap().clone(),
    );
    let [first_slot, second_slot] = &mut c.jobs;
    *first_slot = Some(rig.start(source, effect, request, client.clone()));
    let first = first_slot.as_mut().unwrap();
    drive_until(rig, source, &[&first.phase], |rig| {
        rig.trace.lock().unwrap().observations.iter().any(|row| {
            row.generation == n(1)
                && matches!(
                    &row.event,
                    ObservationEvent::Fact {
                        fact: ObservationFact::CatchUpComplete { generation: 1, .. },
                        ..
                    }
                )
        })
    })
    .await;
    // The same original failure is delivered through the real current LP
    // ticket too. Repeated Stop cannot turn closing ACK into Worker exit.
    rig.command(LogicalToAttemptBody::Stop {
        reason: AttemptStopReason::LogicalExecutionFailed,
        failure: failure.clone(),
    });
    rig.drain(&[&first.phase]);
    rig.sync_source(source);
    assert!(
        !close_pending(rig, n(1)),
        "the Core did not request early Close behind an unprocessed trace"
    );
    assert!(
        rig.trace
            .lock()
            .unwrap()
            .observations
            .iter()
            .all(|row| !matches!(
                &row.event,
                ObservationEvent::Fact {
                    fact: ObservationFact::ParticipationConvergence(_),
                    ..
                }
            ))
    );
    assert_eq!(peer.subscription_requests().len(), 1);
    assert_eq!(peer.open_covered_subscriptions(), 1);
    assert_eq!(*first.result.lock().unwrap(), None);
    assert!(source.reap_generation(n(1)).is_err());
    peer.close_held_subscriptions();
    drive_until(rig, source, &[&first.phase], |rig| close_pending(rig, n(1))).await;
    apply_original_close(rig, source, n(1));
    settle_original(rig, source, first).await;
    assert_eq!(
        *first.result.lock().unwrap(),
        Some(SubscriptionEnd::RemoteClosed)
    );
    for _ in 0..4 {
        if rig.trace.lock().unwrap().last.contains_key(&n(2)) {
            break;
        }
        rig.advance_original_deadline();
    }
    let (effect, request) = rig
        .take_open(n(2))
        .expect("same stopped Core reopens only after original actual exit");
    assert!(
        request.participation_cursor.is_none(),
        "no positive proof is fabricated after EOF/closing ACK"
    );
    assert_eq!(request.required_identities.as_ref(), rig.tasks.as_slice());
    *second_slot = Some(rig.start(source, effect, request, client));
    let second = second_slot.as_mut().unwrap();
    drive_until(rig, source, &[&second.phase], |_| {
        peer.subscription_requests().len() == 2 && peer.open_covered_subscriptions() == 1
    })
    .await;
    let physical = decode_covered_subscribe_task_status(
        &peer.subscription_requests()[1],
        FieldPath::root("same_stopped_core_original_reconnect"),
    )
    .unwrap();
    assert_eq!(physical.participation, rig.participation);
    assert_eq!(physical.generation, n(2));
    assert!(physical.participation_cursor.is_none());
    assert!(!close_pending(rig, n(2)));
    assert_eq!(*second.result.lock().unwrap(), None);
    let original = ParticipationConvergenceReceipt::new(
        rig.participation,
        ParticipationConvergenceVersion::FIRST,
        ParticipationConvergenceState::WorkerStoppedAndParticipationFenced,
    );
    peer.publish_held_covered_event(frame(
        CoveredStatusStreamFact::ParticipationConvergence(original),
        None,
    ))
    .await;
    drive_until(rig, source, &[&second.phase], |rig| {
        close_pending(rig, n(2))
    })
    .await;
    assert_eq!(
        rig.trace
            .lock()
            .unwrap()
            .observations
            .iter()
            .filter(|row| row.participation == rig.participation
                && row.generation == n(2)
                && matches!(&row.event,
            ObservationEvent::Fact {fact:ObservationFact::ParticipationConvergence(got),..}
                if *got==original))
            .count(),
        1,
        "the same Core consumed exactly the actual late H2 proof"
    );
    assert_eq!(
        *second.result.lock().unwrap(),
        None,
        "actual Worker proof/Core Close intent is not this original local future's exit"
    );
    assert!(source.reap_generation(n(2)).is_err());
    // Act on THIS Core's actual Close, not a fixture timer/manual business cut.
    apply_original_close(rig, source, n(2));
    settle_original(rig, source, second).await;
    assert_eq!(
        *second.result.lock().unwrap(),
        Some(SubscriptionEnd::CoreClosed)
    );
    assert_eq!(
        peer.subscription_requests().len(),
        2,
        "a positive actual proof permits no third RPC"
    );
    assert_eq!(
        rig.seal_count(),
        0,
        "terminal cleanup invents no Root success promise"
    );
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    wc::drive(c.driver.as_mut().unwrap());
    tokio::time::timeout(Duration::from_secs(2), source_scope.wait_released())
        .await
        .unwrap();
    drop((first_slot.take(), second_slot.take(), source_scope, scope));
    drop((
        c.source.take(),
        c.cohort.take(),
        c.window.take(),
        c.capacity.take(),
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    c.native
        .as_mut()
        .unwrap()
        .drain_original_apply_until(deadline)
        .await
        .unwrap();
    c.native
        .as_mut()
        .unwrap()
        .drain_native_outgoing_until(deadline)
        .await
        .unwrap();
    assert!(c.peer.as_mut().unwrap().settle().await);
    drop((c.native.take(), c.peer.take()));
    let root = c.root.take().unwrap();
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    c.control.as_ref().unwrap().close_admission();
    while let Some(original) = c.control.as_ref().unwrap().next_control() {
        original.acknowledge();
    }
    wc::drive(c.driver.as_mut().unwrap());
    if let Err(refused) = c.control.take().unwrap().shutdown() {
        let (error, original) = refused.into_parts();
        c.control = Some(original);
        panic!("same original terminal Source WC remains unproven: {error:?}");
    }
    drop(c.driver.take());
    rig.finish();
    drop(retained.0.take());
}
