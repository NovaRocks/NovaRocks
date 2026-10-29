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

//! Independent route and original-R service under the smallest owner budget.

use super::*;
use crate::control_runtime::{
    establish_executors::EstablishExecutors, process_control::RootHostCell,
};
use novarocks_control_runtime::driver::{ActorHostTurn, ActorRegistration};
use std::num::NonZeroUsize;

struct PoolJoin(Option<EstablishExecutors>);
impl PoolJoin {
    fn join(&mut self) {
        if let Some(owner) = self.0.take()
            && let Err((reason, _original)) = owner.shutdown()
        {
            eprintln!("fairness fixture retains a source executor owner: {reason:?}");
            std::process::abort();
        }
    }
}
impl Drop for PoolJoin {
    fn drop(&mut self) {
        self.join();
    }
}

fn root_turn(
    runtime: &ActorRuntime<WorkerActor>,
    registration: &ActorRegistration,
) -> ActorHostTurn<WorkerActor> {
    runtime.notify_host_actor(registration).unwrap();
    let mut other_claims = Vec::new();
    let turn = loop {
        let turn = runtime
            .claim_host_actor()
            .expect("the original Root was notified");
        if turn.matches_registration(registration) {
            break turn;
        }
        other_claims.push(turn);
    };
    for other in other_claims {
        other.release(true).unwrap();
    }
    turn
}

fn probe_in_handoff_order(f: &mut RouteFixture, input: InputId) -> EstablishReplayResult {
    let runtime = f.f.runtime.clone();
    for claimed in 0..=2 {
        let host_address = f.f.root.service().context(f.f.context).unwrap();
        let mut host = host_address.lock().unwrap();
        host.dispatch.index_committed(&runtime, 64).unwrap();
        if let Some(probe) = host.dispatch.take_ready(&runtime, 64).unwrap() {
            let result = match &probe.effect {
                WorkerAction::Context(ContextEffect::EstablishProbed {
                    request, result, ..
                }) if request.token == input => result.clone(),
                _ => panic!("the actual Context probe follows route handoff order"),
            };
            host.handoff_establish_probe(probe).unwrap();
            return result;
        }
        if claimed < 2 {
            runtime.poll_one(Tick(3)).unwrap();
        }
    }
    panic!("the two registered actors must expose the original queued probe");
}

fn finish_conflict_return(
    f: &mut RouteFixture,
    input: InputId,
    mut handle: ControlRequestHandle,
    getters: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
) {
    f.f.runtime.poll_one(Tick(4)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(matches!(&reply.effect, WorkerAction::ControlReply(reply)
        if reply.request.token == input
            && reply.result == RequestResult::Outcome(OperationOutcome::ContextConflict)));
    let disposal =
        f.f.root
            .begin_rejected_ingress_disposal(&reply)
            .unwrap()
            .unwrap();
    f.f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    drop(delivery);
    drop(handle);
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert!(thread::spawn(move || disposal.run()).join().unwrap());
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn ready_routes_cannot_consume_the_original_request_retirement_allowance() {
    let mut f = RouteFixture::with_geometry(4, StorageNeed::default(), true);
    let (first, first_handle, first_getters, first_drops) = f.begin_route(op(12), 100, None);
    let (second, second_handle, second_getters, second_drops) = f.begin_route(op(13), 100, None);
    // These are genuine deferred handoffs. Their Root inputs can retire while
    // the original RPCs still own their sources and wait for the actual actor.
    assert!(f.f.runtime.snapshot(context_actor()).is_err());
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert!(f.f.runtime.input_custody(actor(), first).unwrap().is_none());
    assert!(
        f.f.runtime
            .input_custody(actor(), second)
            .unwrap()
            .is_none()
    );

    let owner = match f
        .f
        .root
        .start_establish_executors(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap())
    {
        Ok(owner) => owner,
        Err(refusal) => {
            let mut partial = PoolJoin(refusal.owner.map(|owner| *owner));
            partial.join();
            panic!(
                "fairness source executor startup refused: {:?}",
                refusal.error
            );
        }
    };
    let handles = owner.handles().unwrap();
    let mut pools = PoolJoin(Some(owner));
    // A real never-submitted R becomes retireable only through its explicit
    // unused withdrawal after the actual external handle has been dropped.
    let (withdrawn, external) =
        f.f.root
            .admit_request(ActorId::new(2).unwrap(), op(14), at(100))
            .unwrap();
    drop(external);
    assert!(
        f.f.root
            .begin_unsubmitted_withdrawal(withdrawn)
            .unwrap()
            .is_none()
    );
    f.adopt_pending_context();
    let replies_before = f.f.ledger.used(Dimension::Replies.id());
    let registration = f.f.root.registration().clone();
    let runtime = f.f.runtime.clone();
    let cell = Arc::new(RootHostCell::new(f.f.root));
    let turn = root_turn(&runtime, &registration);
    let first_turn = cell.advance_ready(&turn, &handles, 1).unwrap();
    let second_turn = cell.advance_ready(&turn, &handles, 1).unwrap();
    turn.release(second_turn.runnable).unwrap();
    f.f.root = Arc::try_unwrap(cell)
        .ok()
        .expect("the same Root owner returns")
        .into_owner();
    let served_both_classes = first_turn.routed == 1
        && second_turn.routed == 1
        && first_turn.requests.visited == 1
        && second_turn.requests.visited == 1
        && first_turn.requests.retired + second_turn.requests.retired == 1;
    let retired_original = !f.f.root.resources().requests.contains_key(&withdrawn)
        && f.f.ledger.used(Dimension::Replies.id()) + 1 == replies_before;
    {
        let original_address = f.f.root.service().context(f.f.context).unwrap();
        let mut original_context = original_address.lock().unwrap();
        let routes = &original_context.resources_for_test().routes;
        assert_eq!(routes.len(), 2);
        assert!(routes.contains_key(&first) && routes.contains_key(&second));
    }
    // Preserve the same physical pools until their real joins. This test does
    // not fabricate a getter, a Context decision or a Source exit from counts.
    drop(handles);
    pools.join();
    assert!(
        matches!(probe_in_handoff_order(&mut f, first), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ContextConflict)
    );
    assert!(
        f.f.root
            .service()
            .context(f.f.context)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(first)
            .unwrap()
    );
    finish_conflict_return(&mut f, first, first_handle, first_getters, first_drops);
    assert!(
        matches!(probe_in_handoff_order(&mut f, second), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ContextConflict)
    );
    assert!(
        f.f.root
            .service()
            .context(f.f.context)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(second)
            .unwrap()
    );
    finish_conflict_return(&mut f, second, second_handle, second_getters, second_drops);
    f.finish_model_refusal();
    assert!(served_both_classes && retired_original);
}

#[test]
fn a_real_context_lock_wait_releases_root_inventory_but_keeps_the_original_host_turn() {
    use std::{sync::mpsc, time::Instant};
    let mut f = RouteFixture::with_geometry(2, StorageNeed::default(), true);
    let (input, handle, getters, drops) = f.begin_route(op(12), 100, None);
    f.adopt_pending_context();
    let runtime = f.f.runtime.clone();
    let registration = f.f.root.registration().clone();
    let address = f.f.root.service().context(f.f.context).unwrap();
    let mut context_guard = address.lock().unwrap();
    let context_scope = context_guard
        .resources_for_test()
        .scope
        .as_ref()
        .unwrap()
        .clone();
    let before = context_scope.snapshot().unwrap();
    let owner = match f
        .f
        .root
        .start_establish_executors(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap())
    {
        Ok(owner) => owner,
        Err(refusal) => {
            let mut partial = PoolJoin(refusal.owner.map(|owner| *owner));
            partial.join();
            panic!("actual source executor startup: {:?}", refusal.error);
        }
    };
    let handles = owner.handles().unwrap();
    let mut pools = PoolJoin(Some(owner));
    let cell = Arc::new(RootHostCell::new(f.f.root));
    let turn = root_turn(&runtime, &registration);
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let worker_cell = cell.clone();
    let worker_handles = handles.clone();
    let worker = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let progress = worker_cell.advance_ready(&turn, &worker_handles, 1);
        // Root owns this same exclusive Common turn throughout preparation,
        // the target-lock wait and the independent original-R progress phase.
        let runnable = progress.as_ref().map_or(true, |progress| progress.runnable);
        turn.release(runnable).unwrap();
        done_tx.send(()).unwrap();
        progress
    });
    let started = started_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut root_was_free_after_prepare = false;
    while Instant::now() < deadline {
        if let Some(root) = cell.try_lock() {
            let original = root.resources().requests.get(&input).unwrap();
            // The actual Root return role exists only after original-R
            // preparation, before any target Context Work can be admitted.
            if original.route_return_role().is_some() && original.pending_route().is_none() {
                root_was_free_after_prepare = true;
                break;
            }
        }
        thread::yield_now();
    }
    let still_waiting_for_context = matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    let context_unchanged_while_blocked = context_scope.snapshot().unwrap() == before;
    let mut other_turns = Vec::new();
    let mut no_second_root_turn = true;
    while let Some(other) = runtime.claim_host_actor() {
        no_second_root_turn &= !other.matches_registration(&registration);
        other_turns.push(other);
    }
    for other in other_turns {
        other.release(true).unwrap();
    }
    // Always release the real mutex before asserting/receiving/joining. A
    // regression holding Root across bind can thus unwind with full cleanup.
    drop(context_guard);
    let returned = done_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    let progress = worker.join().unwrap().unwrap();
    f.f.root = Arc::try_unwrap(cell)
        .ok()
        .expect("worker returned its Root alias")
        .into_owner();
    drop(handles);
    pools.join();
    assert!(
        matches!(probe_in_handoff_order(&mut f, input), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ContextConflict)
    );
    assert!(
        address
            .lock()
            .unwrap()
            .advance_establish_route(input)
            .unwrap()
    );
    finish_conflict_return(&mut f, input, handle, getters, drops);
    f.finish_model_refusal();
    assert!(started && returned);
    assert!(root_was_free_after_prepare && still_waiting_for_context);
    assert!(context_unchanged_while_blocked);
    assert!(no_second_root_turn);
    assert_eq!(progress.routed, 1);
}
