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
use crate::control_runtime::establish_executors::EstablishExecutors;
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
                }) if request.token == input => *result,
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
    let first_turn = f.f.root.advance_ready_requests(&handles, 1).unwrap();
    let second_turn = f.f.root.advance_ready_requests(&handles, 1).unwrap();
    let served_both_classes = first_turn.routed == 1
        && second_turn.routed == 1
        && first_turn.visited == 1
        && second_turn.visited == 1
        && first_turn.retired + second_turn.retired == 1;
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
    assert_eq!(
        probe_in_handoff_order(&mut f, first),
        EstablishReplayResult::Decided(OperationOutcome::ContextConflict)
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
    assert_eq!(
        probe_in_handoff_order(&mut f, second),
        EstablishReplayResult::Decided(OperationOutcome::ContextConflict)
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
