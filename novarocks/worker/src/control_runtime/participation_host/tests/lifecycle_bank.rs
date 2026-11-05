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

//! The actual process-funded callback is admitted once before host capture.
//! Its unused withdrawal retires the original input position, never a host body.

use super::*;

#[test]
fn original_process_callback_is_unique_and_withdraws_without_inventing_execution_exit() {
    let mut fixture = Harness::new(PARTICIPATION_BOOTSTRAP_STORAGE);
    let materialize = fixture.materialize();
    let binding = fixture.binding(participation_actor(), &materialize);
    fixture
        .service
        .adopt_participation(&materialize, binding)
        .unwrap();
    fixture.complete_materialize_handoff(materialize);
    let address = fixture.service.participation(fixture.identity).unwrap();
    let mut host = address.lock().unwrap();
    let child = host.scope().clone();
    let baseline = child.snapshot().unwrap();
    let before = fixture.runtime.snapshot(participation_actor()).unwrap();
    let root_before = fixture.runtime.snapshot(control_actor()).unwrap();
    let quota = actual_admission_usage(&fixture);
    host.admit_lifecycle().unwrap();
    let admitted = fixture.runtime.snapshot(participation_actor()).unwrap();
    assert_eq!(admitted.standing_inputs, before.standing_inputs + 1);
    assert_eq!(admitted.inputs, before.inputs);
    assert_eq!(
        fixture.runtime.snapshot(control_actor()).unwrap(),
        root_before
    );
    assert_eq!(child.snapshot().unwrap(), baseline);
    assert!(matches!(
        host.admit_lifecycle(),
        Err(ParticipationHostError::Io(
            ParticipationIoError::AlreadyAdmitted
        ))
    ));
    assert_eq!(
        fixture.runtime.snapshot(participation_actor()).unwrap(),
        admitted
    );
    let callback = host.resources.lifecycle.as_mut().unwrap();
    callback.withdraw_unused().unwrap();
    callback.withdraw_unused().unwrap();
    let withdrawn = fixture.runtime.snapshot(participation_actor()).unwrap();
    assert_eq!(withdrawn.standing_inputs, before.standing_inputs);
    assert_eq!(withdrawn.inputs, before.inputs);
    assert_eq!(child.snapshot().unwrap(), baseline);
    assert!(
        child.gates().is_ok(),
        "withdrawing an unused callback is not Scope removal"
    );
    assert_eq!(actual_admission_usage(&fixture), quota);
    assert!(host.resources.lifecycle.is_some());
    drop(host);
    let (host, retirement) = fixture.actual_model_refusal_retirement();
    fixture.finish_retired_participation(host, retirement);
}
