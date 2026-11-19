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
use crate::test_support::*;
use novarocks_coordination_contract::{QueryFailureReason, SelectedTaskTermination};
use novarocks_execution_contract::{
    AbortCause, SafeDetail, TaskFailure, TaskOutputFacts, TaskStatus, TaskStatusVersion,
    TerminationDetail, failure::FailureProjection,
};
use std::num::NonZeroU64;
fn token(n: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
}
fn finished() -> AttemptRootProjection {
    AttemptRootProjection {
        status: TaskStatus::try_new(
            root(1),
            TaskStatusVersion::new(6).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap(),
        failure: AttemptRootFailure::None,
    }
}
fn derived() -> AttemptRootProjection {
    AttemptRootProjection {
        status: TaskStatus::try_new(
            root(1),
            TaskStatusVersion::new(4).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::new(false),
        )
        .unwrap(),
        failure: AttemptRootFailure::None,
    }
}
fn selected() -> SelectedTaskTermination {
    SelectedTaskTermination::new(
        root(1),
        TerminationDetail::Failed(TaskFailure::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::truncating("original root failure"),
        )),
    )
}
#[test]
fn terminal_demand_precedes_seal_and_the_lane_stays_singleflight() {
    let mut control = RootControl::new(root(1));
    control.request_seal();
    control.request_terminal();
    let demand = control.next_request(token(1)).unwrap();
    assert_eq!(demand.kind, RootControlKind::TerminalDemand);
    assert_eq!(control.next_request(token(2)), None);
    assert_eq!(
        control.settle(RootControlKind::Seal, token(1), true),
        Err(RootControlRefusal::UnexpectedReply)
    );
    control
        .settle(RootControlKind::TerminalDemand, token(1), true)
        .unwrap();
    assert_eq!(
        control.next_request(token(2)).unwrap().kind,
        RootControlKind::Seal
    );
}
#[test]
fn only_exact_finished_root_accepts_the_original_seal_reply() {
    let mut control = RootControl::new(root(1));
    control.request_seal();
    control.next_request(token(1)).unwrap();
    assert_eq!(
        control.settle(RootControlKind::Seal, token(1), true),
        Err(RootControlRefusal::InvalidSuccessSeal)
    );
    assert!(control.in_flight().is_some());
    control.observe(finished()).unwrap();
    control
        .settle(RootControlKind::Seal, token(1), true)
        .unwrap();
    assert!(control.sealed());
    control.request_seal();
    assert_eq!(control.next_request(token(2)), None);
}
#[test]
fn root_withdrawal_waits_for_authoritative_terminal_instead_of_inventing_failure() {
    let mut control = RootControl::new(root(1));
    control.request_terminal();
    assert!(control.accepted().is_none());
    assert!(!control.finished());
    let mut projection = derived();
    assert_eq!(
        control.observe(projection.clone()),
        Ok(RootObservation::PendingFailure)
    );
    projection.failure = AttemptRootFailure::DerivedPending;
    assert_eq!(
        control.observe(projection.clone()),
        Ok(RootObservation::PendingFailure)
    );
    projection.failure = AttemptRootFailure::Authoritative(selected());
    let RootObservation::Authoritative(failure) = control.observe(projection.clone()).unwrap()
    else {
        panic!("original authoritative cause required")
    };
    assert_eq!(failure.input().detail().as_str(), "original root failure");
    assert_eq!(control.observe(projection), Ok(RootObservation::Ignored));
}
#[test]
fn same_status_allows_only_original_derived_refinement_once() {
    let mut control = RootControl::new(root(1));
    let mut projection = derived();
    control.observe(projection.clone()).unwrap();
    projection.failure = AttemptRootFailure::Authoritative(selected());
    control.observe(projection.clone()).unwrap();
    projection.failure = AttemptRootFailure::DerivedPending;
    assert_eq!(
        control.observe(projection),
        Err(RootControlRefusal::FailureRefinement)
    );
}
#[test]
fn old_status_is_ignored_but_same_version_conflict_and_terminal_overwrite_fail() {
    let mut control = RootControl::new(root(1));
    control.observe(finished()).unwrap();
    assert_eq!(
        control.observe(AttemptRootProjection {
            status: TaskStatus::created(root(1)),
            failure: AttemptRootFailure::None
        }),
        Ok(RootObservation::Ignored)
    );
    let conflicting = AttemptRootProjection {
        status: TaskStatus::try_new(
            root(1),
            TaskStatusVersion::new(6).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap(),
        failure: AttemptRootFailure::None,
    };
    assert_eq!(
        control.observe(conflicting),
        Err(RootControlRefusal::StatusConflict)
    );
    let later = AttemptRootProjection {
        status: TaskStatus::try_new(
            root(1),
            TaskStatusVersion::new(7).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap(),
        failure: AttemptRootFailure::None,
    };
    assert_eq!(
        control.observe(later),
        Err(RootControlRefusal::StatusConflict)
    );
}
#[test]
fn foreign_root_observation_does_not_change_the_accepted_snapshot() {
    let mut control = RootControl::new(root(1));
    let foreign = AttemptRootProjection {
        status: TaskStatus::created(root(2)),
        failure: AttemptRootFailure::None,
    };
    assert_eq!(
        control.observe(foreign),
        Err(RootControlRefusal::IdentityMismatch)
    );
    assert!(control.accepted().is_none());
}
#[test]
fn failed_control_reply_does_not_grant_success_or_create_an_unrequested_retry() {
    let mut control = RootControl::new(root(1));
    control.request_seal();
    control.next_request(token(1)).unwrap();
    control
        .settle(RootControlKind::Seal, token(1), false)
        .unwrap();
    assert!(!control.sealed());
    assert_eq!(control.next_request(token(2)), None);
}

#[test]
fn finished_root_retains_original_non_root_producer_failure_from_same_attempt() {
    let producer = TaskIdentity::new(
        activation(1).execution(),
        novarocks_types::StageId::new(2).unwrap(),
        novarocks_types::TaskId::new(3).unwrap(),
        root(1).backend_process_id(),
    );
    let selected = SelectedTaskTermination::new(producer, selected().detail().clone());
    let mut projection = finished();
    projection.failure = AttemptRootFailure::Authoritative(selected.clone());
    let mut control = RootControl::new(root(1));
    let RootObservation::Authoritative(failure) = control.observe(projection).unwrap() else {
        panic!("original producer failure required")
    };
    assert_eq!(
        failure.input(),
        &ExecutionFailureInput::from_selected_task(&selected).unwrap()
    );
    assert!(!control.finished());
}

#[test]
fn selected_failure_from_another_attempt_is_rejected() {
    let selected = SelectedTaskTermination::new(root(2), selected().detail().clone());
    let mut projection = finished();
    projection.failure = AttemptRootFailure::Authoritative(selected);
    let mut control = RootControl::new(root(1));
    assert_eq!(
        control.observe(projection),
        Err(RootControlRefusal::IdentityMismatch)
    );
}
