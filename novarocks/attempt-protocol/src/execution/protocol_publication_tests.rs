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

//! Exercise the cursor against the original accepted QExec author, including
//! the real sticky writer witness and original failure latch.

use super::*;
use crate::capacity::{QueueStage, StepContext};
use crate::config::DispatchLane;
use crate::failure::AttemptFailure;
use crate::publication::protocol_cursor::{
    ProtocolPublicationCursor, PublicationApplyDisposition, PublicationProgress,
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{
    AbortCause, CancelReason, TaskOutputFacts, TaskStatusVersion, TaskWriterFacts,
};
use std::num::NonZeroU64;

struct Units {
    left: usize,
    charged: usize,
}
impl Units {
    fn new(left: usize) -> Self {
        Self { left, charged: 0 }
    }
}
impl StepContext for Units {
    fn charge(&mut self, units: usize) -> bool {
        if units > self.left {
            return false;
        }
        self.left -= units;
        self.charged += units;
        true
    }
    fn reserve_queue_stage(
        &mut self,
        _: DispatchLane,
        _: bool,
        _: BackendProcessId,
        _: usize,
        _: usize,
    ) -> QueueStage {
        panic!("protocol publication must not reserve Native transport")
    }
}
fn fixture() -> (QueryTaskExecution, ProtocolPublicationCursor) {
    let execution = crate::test_support::execution_with_strict_input();
    let cursor = ProtocolPublicationCursor::new(execution.graph());
    (execution, cursor)
}
fn effect(n: u64) -> AttemptActionId {
    AttemptActionId::new(NonZeroU64::new(n).unwrap())
}
fn status(execution: &QueryTaskExecution, id: u32, version: u64) -> TaskStatus {
    TaskStatus::try_new_with_installed(
        execution
            .graph()
            .task(TaskId::new(id).unwrap())
            .unwrap()
            .identity(),
        TaskStatusVersion::new(version).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::new(false),
        true,
    )
    .unwrap()
}
fn accept(execution: &mut QueryTaskExecution, status: &TaskStatus) {
    execution
        .apply_published(status, &mut StatusReport::default())
        .unwrap();
}
fn finish(cursor: &mut ProtocolPublicationCursor, execution: &QueryTaskExecution, id: u64) {
    assert_eq!(
        cursor.advance(execution, &mut Units::new(100)).unwrap(),
        PublicationProgress::Ready
    );
    assert!(cursor.take_ready(effect(id)).is_some());
    assert_eq!(
        cursor.applied(effect(id), Ok(true)),
        PublicationApplyDisposition::Applied { published: true }
    );
}
#[test]
fn initial_empty_binding_publishes_once_and_seal_only_change_still_publishes() {
    let (mut execution, mut cursor) = fixture();
    assert!(cursor.select(&mut execution, false).unwrap());
    assert!(!cursor.permits_protocol_turn());
    let mut units = Units::new(0);
    assert_eq!(
        cursor.advance(&execution, &mut units).unwrap(),
        PublicationProgress::Ready
    );
    assert_eq!(units.charged, 0);
    let batch = cursor.take_ready(effect(1)).unwrap();
    assert!(batch.changes.is_empty());
    assert_eq!(batch.owner_revision, 0);
    assert!(!cursor.permits_protocol_turn());
    assert_eq!(
        cursor.applied(effect(1), Ok(false)),
        PublicationApplyDisposition::Applied { published: false }
    );
    assert!(cursor.permits_protocol_turn());
    assert!(!cursor.select(&mut execution, false).unwrap());
    assert!(cursor.select(&mut execution, true).unwrap());
    assert!(cursor.ready().unwrap().root_success_sealed);
    finish(&mut cursor, &execution, 2);
    assert!(!cursor.select(&mut execution, true).unwrap());
}
#[test]
fn dirty_root_is_transferred_and_each_original_task_is_charged_in_task_id_order() {
    let (mut execution, mut cursor) = fixture();
    for id in [3, 1, 2] {
        let observed = status(&execution, id, 2);
        accept(&mut execution, &observed);
    }
    let revision = execution.accepted_protocol_revision();
    assert!(cursor.select(&mut execution, false).unwrap());
    assert!(execution.accepted_protocol_task_changes().is_empty());
    assert_eq!(
        cursor.advance(&execution, &mut Units::new(0)).unwrap(),
        PublicationProgress::Yield
    );
    assert!(cursor.needs_continuation());
    for index in 0..3 {
        let mut units = Units::new(1);
        assert_eq!(
            cursor.advance(&execution, &mut units).unwrap(),
            if index == 2 {
                PublicationProgress::Ready
            } else {
                PublicationProgress::Yield
            }
        );
        assert_eq!(units.charged, 1);
        assert!(!cursor.permits_protocol_turn());
    }
    let batch = cursor.ready().unwrap();
    assert!(!cursor.needs_continuation());
    assert_eq!(batch.owner_revision, revision);
    assert_eq!(
        batch
            .changes
            .iter()
            .map(|row| row.identity.task_id().get())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let pointer = batch.changes.as_ptr();
    let capacity = batch.changes.capacity();
    // Repeated Source-credit Full is represented by leaving the value owned.
    // It never clears the cut, emits a copy or schedules another primary turn.
    for _ in 0..100 {
        assert_eq!(
            cursor.advance(&execution, &mut Units::new(0)).unwrap(),
            PublicationProgress::Ready
        );
        assert_eq!(cursor.ready().unwrap().changes.as_ptr(), pointer);
        assert_eq!(cursor.ready().unwrap().changes.capacity(), capacity);
    }
    let batch = cursor.take_ready(effect(10)).unwrap();
    assert!(!cursor.needs_continuation());
    assert_eq!(batch.changes.as_ptr(), pointer);
    assert_eq!(batch.changes.capacity(), capacity);
    assert!(cursor.take_ready(effect(11)).is_none());
    assert_eq!(
        cursor.advance(&execution, &mut Units::new(100)).unwrap(),
        PublicationProgress::AwaitingApply
    );
    assert_eq!(
        cursor.applied(effect(11), Ok(true)),
        PublicationApplyDisposition::Stale
    );
    assert!(!cursor.permits_protocol_turn());
    assert_eq!(
        cursor.applied(effect(10), Ok(true)),
        PublicationApplyDisposition::Applied { published: true }
    );
    assert!(cursor.permits_protocol_turn());
}
#[test]
fn later_primary_change_is_rejected_without_destroying_or_replacing_the_old_cut() {
    let (mut execution, mut cursor) = fixture();
    let first = status(&execution, 1, 2);
    accept(&mut execution, &first);
    let second = status(&execution, 2, 2);
    accept(&mut execution, &second);
    cursor.select(&mut execution, false).unwrap();
    assert_eq!(
        cursor.advance(&execution, &mut Units::new(1)).unwrap(),
        PublicationProgress::Yield
    );
    // This intentionally violates the future Core's primary selection gate.
    // The component detects the mixed revision rather than publishing it.
    let later = status(&execution, 2, 3);
    accept(&mut execution, &later);
    assert!(cursor.advance(&execution, &mut Units::new(1)).is_err());
    assert!(cursor.ready().is_none());
    assert!(!cursor.permits_protocol_turn());
    assert_eq!(execution.accepted_protocol_task_changes().len(), 1);
    assert!(cursor.select(&mut execution, false).is_err());
}
#[test]
fn accepted_first_writer_survives_later_status_without_writer_in_the_same_batch() {
    let (mut execution, mut cursor) = fixture();
    let first = status(&execution, 2, 2).with_writer(TaskWriterFacts::empty().with_written(7, 19));
    accept(&mut execution, &first);
    let current = status(&execution, 2, 3);
    accept(&mut execution, &current);
    cursor.select(&mut execution, false).unwrap();
    cursor.advance(&execution, &mut Units::new(1)).unwrap();
    let row = &cursor.ready().unwrap().changes[0];
    assert_eq!(row.status.as_ref(), Some(&current));
    assert_eq!(row.first_writer_witness.as_ref(), Some(&first));
}
#[test]
fn failure_only_refinement_and_seal_are_selected_before_later_cleanup_changes() {
    let (mut execution, mut cursor) = fixture();
    finish_initial(&mut execution, &mut cursor);
    let identity = execution.graph().root_identity();
    execution
        .latch_accepted_task_failure(
            identity,
            TerminationDetail::Aborted(AbortCause::PeerTaskFailed),
        )
        .unwrap();
    assert!(execution.accepted_protocol_task_changes().is_empty());
    assert!(cursor.select(&mut execution, true).unwrap());
    let selected = cursor.ready().unwrap().selected_failure.clone().unwrap();
    assert_eq!(selected.identity(), identity);
    assert!(cursor.ready().unwrap().root_success_sealed);
    let batch = cursor.take_ready(effect(2)).unwrap();
    assert_eq!(
        cursor.applied(effect(2), Ok(true)),
        PublicationApplyDisposition::Applied { published: true }
    );
    // After actual apply resumes the original turn, a later cleanup cause
    // cannot rewrite the earlier immutable payload or its accepted seal.
    execution
        .latch_accepted_task_failure(
            identity,
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
        )
        .unwrap();
    assert_eq!(batch.selected_failure.as_ref(), Some(&selected));
    assert!(batch.root_success_sealed);
    assert!(cursor.select(&mut execution, true).unwrap());
    assert!(cursor.ready().unwrap().changes.is_empty());
    assert_ne!(
        cursor.ready().unwrap().selected_failure.as_ref(),
        Some(&selected)
    );
}
fn finish_initial(execution: &mut QueryTaskExecution, cursor: &mut ProtocolPublicationCursor) {
    cursor.select(execution, false).unwrap();
    finish(cursor, execution, 1);
}
#[test]
fn actual_apply_failure_blocks_the_old_round_tail_and_cannot_be_overwritten_by_late_success() {
    let (mut execution, mut cursor) = fixture();
    cursor.select(&mut execution, false).unwrap();
    cursor.take_ready(effect(1)).unwrap();
    let failure = AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::UnknownOperation));
    assert_eq!(
        cursor.applied(effect(1), Err(failure.clone())),
        PublicationApplyDisposition::Failed(failure)
    );
    assert!(!cursor.permits_protocol_turn());
    assert_eq!(
        cursor.advance(&execution, &mut Units::new(1)).unwrap(),
        PublicationProgress::Failed
    );
    assert_eq!(
        cursor.applied(effect(1), Ok(true)),
        PublicationApplyDisposition::Stale
    );
    assert!(!cursor.permits_protocol_turn());
    assert!(cursor.select(&mut execution, false).is_err());
}
