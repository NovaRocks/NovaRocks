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
use crate::api::logical_runtime::LogicalExit;
use novarocks_types::{QueryId, identity::AttemptId};

#[test]
fn runtime_failure_retains_original_typed_cause_without_forging_join_error() {
    let source = OriginalLogicalExit::new();
    let association = OriginalAssociation {
        registry: 1,
        initial: QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        generation: 1,
    };
    source.reserve_silent(association);
    source.installed_silent(association);
    assert!(!source.pending_actor());
    // This fixture reports the install-only role's actual exit separately.
    // Pending actor custody is a legacy role condition, not the Installed phase.
    source.role_ended_silent(false);
    assert!(source.pending_actor());
    assert!(!source.has_failure());
    let error = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "original runtime cause");
    source.runtime_joined_silent(association, LogicalExit::OwnerFailed(error.clone()));
    let view = LogicalExecutionExitView {
        state: source.lock(),
    };
    assert_eq!(view.actor_phase(), LogicalActorExitPhase::Joined);
    assert!(view.role_ended());
    assert!(view.actor_result().is_none());
    assert_eq!(
        view.runtime_actor_result(),
        Some(&LogicalExit::OwnerFailed(error))
    );
    drop(view);
    assert!(!source.pending_actor());
    assert!(source.has_failure());
    assert!(source.has_unhandled_failure());
}

#[test]
fn runtime_completion_is_distinct_from_role_and_native_actual_exit() {
    let source = OriginalLogicalExit::new();
    let association = OriginalAssociation {
        registry: 1,
        initial: QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        generation: 1,
    };
    source.reserve_silent(association);
    source.installed_silent(association);
    // Model the original pre-actor body still retained by its source.
    source.lock().native_body_exited = false;
    source.runtime_joined_silent(association, LogicalExit::Completed);
    let state = source.lock();
    assert!(!state.native_body_exited);
    assert!(!state.role_ended);
    assert!(state.role.is_none());
    drop(state);
    assert!(!source.has_failure());
}
