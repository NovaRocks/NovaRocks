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

//! The same paid Task request preserves the original proposal prefix before
//! its high-priority event. The fixture never starts Native execution.

use super::*;
use novarocks_control_runtime::driver::{ActorRuntime, Clock, InputPhase};
use novarocks_execution_contract::{AbortCause, TaskConclusionProposal};
use std::sync::atomic::{AtomicBool, Ordering};

struct BeforeRequestStep {
    runtime: ActorRuntime<WorkerActor>,
    prefix: InputId,
    event: InputId,
    observed: AtomicBool,
}
impl Clock for BeforeRequestStep {
    fn now(&self) -> Tick {
        // Clock sampling occurs outside runtime locks, after the actual input
        // enters Consuming. Inspect those real receipts, not input ID order.
        if self
            .runtime
            .input_custody(context_actor(), self.event)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Consuming)
        {
            assert!(
                self.runtime
                    .input_custody(context_actor(), self.prefix)
                    .unwrap()
                    .is_some_and(|input| input.phase == InputPhase::Committed),
                "the original prefix must commit before this actual event step"
            );
            self.observed.store(true, Ordering::SeqCst);
        }
        Tick(8)
    }
}

#[test]
fn task_rpc_consumes_its_actual_proposal_prefix_before_its_control_event() {
    let (mut f, task, preparation) = accepted_task_fixture();
    let ready = ready(&f, 1);
    let ingress = f
        .service
        .context(f.identity)
        .unwrap()
        .task(task)
        .unwrap()
        .port()
        .ingress()
        .clone();
    let handoff = ingress.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
    assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
    let operation = operation(104);
    let deadline = MonotonicInstant::from_origin(Duration::from_secs(5));
    let (work, mut handle) = f
        .service
        .context(f.identity)
        .unwrap()
        .admit_task_request(&ready, task, InputClass::Control, operation, deadline)
        .unwrap();
    f.service
        .context(f.identity)
        .unwrap()
        .submit_request(work, ContextCommand::GetFinalInfo { task })
        .unwrap();
    let actual = f
        .service
        .context(f.identity)
        .unwrap()
        .deliver_task_event(task)
        .unwrap()
        .unwrap();
    let prefix = actual
        .prefix
        .expect("the original nonempty proposal prefix");
    assert_ne!(prefix, actual.event);
    let clock = BeforeRequestStep {
        runtime: f.runtime.clone(),
        prefix,
        event: actual.event,
        observed: AtomicBool::new(false),
    };
    // A finite actor budget may consume both inputs in one turn or leave the
    // second for the next. The oracle samples the actual before-event step.
    for _ in 0..2 {
        f.runtime.poll_one_with_clock(&clock).unwrap();
        if clock.observed.load(Ordering::SeqCst) {
            break;
        }
    }
    assert!(clock.observed.load(Ordering::SeqCst));
    assert!(
        f.runtime
            .input_custody(context_actor(), actual.event)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Committed)
    );
    drop(clock);

    // These model effects carry neutral protocol facts. Their completion is
    // not a Loader, Native preparation, runtime-stop or opaque-source exit.
    complete_model_effects(&mut f, prefix);
    let ticket = reply(&f, actual.event);
    let WorkerAction::Reply(settlement) = &ticket.effect else {
        panic!("the original Task request's complete reply");
    };
    assert_eq!(settlement.request.token, actual.event);
    assert_eq!(settlement.request.operation, operation);
    assert_eq!(settlement.request.deadline, deadline);
    assert_eq!(settlement.task, Some(task));
    f.service
        .context(f.identity)
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let delivered = handle.try_take().expect("the original request's reply");
    assert_eq!(delivered.request().operation, operation);
    assert_eq!(delivered.request().deadline, deadline);
    // The fixture supplies no final Native statistics or physical completion;
    // its ordering proof does not guess a final Task verdict from the proposal.
    drop((handle, delivered));

    let mut retired = false;
    for _ in 0..8 {
        let (scope, actual_work) = ready
            .next_context_ready()
            .expect("the same original Task request remains ready to retire");
        assert_eq!(scope, f.service.context(f.identity).unwrap().scope().id());
        assert_eq!(actual_work, work);
        if f.service
            .context(f.identity)
            .unwrap()
            .advance_request(actual_work)
            .unwrap()
        {
            retired = true;
            break;
        }
    }
    assert!(retired);
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(ready.registered(), 0);
    drop(ingress);
    finish_accepted_task_fixture(f, task, preparation);
}
