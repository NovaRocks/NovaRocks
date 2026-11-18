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

//! The original close predicate and closed-state admission fence.

use super::*;

#[test]
fn original_feedback_close_releases_initial_wait_and_refuses_reconfiguration() {
    let execution = execution_id();
    let process = BackendProcessId::new_v7();
    let state = RuntimeFilterFeedbackState::new(execution, declaration(process)).unwrap();
    assert!(state.is_initial_wait_blocked(17, [7]));
    let feedback =
        TaskRuntimeFilterFeedback::parse(&task_envelope(execution, 7, [9; 32], exact(41))).unwrap();
    state.close();
    assert!(!state.is_initial_wait_blocked(17, [7]));
    state.close();
    assert_eq!(
        state.admit_task_feedback(&feedback, process).unwrap(),
        RuntimeFilterFeedbackAdmission::Applied
    );
    assert!(!state.is_initial_wait_blocked(17, [7]));
    assert!(state.state.lock().unwrap().channels[&7].winner.is_none());
    assert!(state.configure(declaration(process)).is_err());
    assert!(state.state.lock().unwrap().closed);
}
