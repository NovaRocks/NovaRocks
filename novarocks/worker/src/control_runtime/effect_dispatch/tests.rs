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
use crate::control_runtime::actors::tests::{Fixture, actor, initial_establishment, operation};
use novarocks_control_runtime_core::{identity::Tick, mailbox::InputClass};
use novarocks_worker_protocol::{
    context::{ContextInput, EstablishReplayRequest, EstablishReplayResult},
    creation::RequestContext,
    lease::MonotonicInstant,
};
use std::time::Duration;

#[test]
fn context_control_handoffs_keep_core_order_across_batches_and_reverse_request_tokens() {
    let f = Fixture::new();
    let initial = initial_establishment();
    // These are correlation values from the requesting actor, not Context
    // inputs or Context Work authority. Their numeric order is irrelevant.
    let requests = [900, 100].map(|token| EstablishReplayRequest {
        request: RequestContext {
            token: InputId::new(token).unwrap(),
            operation: operation(2),
            deadline: MonotonicInstant::from_origin(Duration::from_secs(10)),
        },
        ticket: initial.ticket,
        payload: initial.payload,
        semantics: initial.semantics,
    });
    let inputs = requests.map(|request| {
        f.send(
            ContextInput::ProbeEstablish {
                context: f.identity,
                request,
                comparison: None,
            },
            None,
            InputClass::Lifecycle,
            2,
        )
    });
    f.runtime.poll_one(Tick(2)).unwrap();
    let mut dispatch =
        EffectDispatch::context(f.runtime.clone(), actor(), ContextCore::MAX_STEP_WORK * 2);
    let indexed = dispatch.index_committed(&f.runtime, 32).unwrap();
    let first = dispatch.take_ready(&f.runtime, 8).unwrap().unwrap();
    let first_ref = EffectRef {
        input: first.input,
        action: first.descriptor.id,
    };
    let first_request = match &first.effect {
        WorkerAction::Context(ContextEffect::EstablishProbed {
            request,
            result: EstablishReplayResult::Decided(_),
            ..
        }) => *request,
        _ => panic!("actual first Context owner verdict"),
    };
    let premature = dispatch.completion_observed(first_ref);
    let overtaken = dispatch.take_ready(&f.runtime, 8).unwrap();
    let blocked = overtaken.is_none();
    f.runtime.effect_completed(first).unwrap();
    let first_completed = dispatch.completion_observed(first_ref);
    let second = overtaken
        .or_else(|| dispatch.take_ready(&f.runtime, 8).unwrap())
        .expect("second original Context handoff");
    let stale = dispatch.completion_observed(first_ref);
    let second_ref = EffectRef {
        input: second.input,
        action: second.descriptor.id,
    };
    let second_request = match &second.effect {
        WorkerAction::Context(ContextEffect::EstablishProbed { request, .. }) => *request,
        _ => panic!("actual second Context owner verdict"),
    };
    f.runtime.effect_completed(second).unwrap();
    let second_completed = dispatch.completion_observed(second_ref);
    let empty = dispatch.is_empty();
    for input in inputs {
        f.runtime.retire_input(actor(), input).unwrap();
    }
    f.finish();

    // Verify after the actual actor/input/Work cleanup, including on an
    // ordering regression. A drop assertion must not hide the first failure.
    assert_eq!(indexed.inputs, inputs);
    assert_eq!(first_request, requests[0].request);
    assert_eq!(second_request, requests[1].request);
    assert!(
        blocked,
        "later Context metadata cannot overtake its handoff"
    );
    assert!(premature.is_err());
    assert!(first_completed.is_ok());
    assert!(stale.is_err());
    assert!(second_completed.is_ok());
    assert!(empty);
}
