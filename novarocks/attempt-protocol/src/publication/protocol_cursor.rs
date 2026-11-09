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

//! A charged continuation at the original protocol-publication point, before
//! the same round's covered reconciliation, ordinary pumps and queue expiry.
//! The original QExec remains the sole accepted-status owner. While the dirty
//! root is borrowed, the Core must select only independent control/custody and
//! this publication's continuation; it must not mutate primary protocol facts.
//! Actual host apply, not effect handoff or actual job exit, releases the turn.
//! Failed/removed Core state (including an assembled batch) stays retained for
//! credentialed host removal off the poller; it must not be reset or cloned.

use super::{ProtocolPublication, ProtocolTaskChange};
use crate::{
    capacity::StepContext, error::TaskExecutionError, execution::QueryTaskExecution,
    failure::AttemptFailure, graph::TaskGraph,
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::TaskIdentity;
use novarocks_types::TaskId;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationProgress {
    Idle,
    Yield,
    Ready,
    AwaitingApply,
    Failed,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicationApplyDisposition {
    Stale,
    Applied { published: bool },
    Failed(AttemptFailure),
}
#[derive(Debug)]
struct Cut {
    remaining: BTreeSet<TaskId>,
    publication: ProtocolPublication,
}
#[derive(Clone, Copy, Debug)]
struct Flight {
    effect: AttemptActionId,
    revision: u64,
    sealed: bool,
}
#[derive(Debug)]
pub(crate) struct ProtocolPublicationCursor {
    root: TaskIdentity,
    last_applied: Option<(u64, bool)>,
    cut: Option<Cut>,
    flight: Option<Flight>,
    failed: bool,
}
impl ProtocolPublicationCursor {
    pub(crate) fn new(graph: &TaskGraph) -> Self {
        Self {
            root: graph.root_identity(),
            last_applied: None,
            cut: None,
            flight: None,
            failed: false,
        }
    }
    pub(crate) fn permits_protocol_turn(&self) -> bool {
        !self.has_pending_apply() && !self.failed
    }
    pub(crate) fn has_pending_apply(&self) -> bool {
        self.cut.is_some() || self.flight.is_some()
    }
    /// Only unfinished charged traversal can request an immediate continuation.
    /// A ready batch waits for real Source credit; an in-flight batch waits for
    /// its actual apply result. Neither is a reason to poll an idle actor.
    pub(crate) fn needs_continuation(&self) -> bool {
        !self.failed
            && self
                .cut
                .as_ref()
                .is_some_and(|cut| !cut.remaining.is_empty())
    }
    /// Call only at a real old publication point, including initial drive
    /// binding. A dormant/unbound drive must not call this method. No automatic
    /// finally/after-every-input publication is introduced here.
    pub(crate) fn select(
        &mut self,
        execution: &mut QueryTaskExecution,
        sealed: bool,
    ) -> Result<bool, TaskExecutionError> {
        self.verify_execution(execution)?;
        if !self.permits_protocol_turn() {
            return Err(error("protocol publication is already outstanding"));
        }
        let revision = execution.accepted_protocol_revision();
        if self.last_applied == Some((revision, sealed)) {
            return Ok(false);
        }
        // Reserve the exact dirty count once, before any charged push. Vec
        // allocation does not initialize rows or copy an accumulated prefix.
        // The original actor metadata declaration must price this one batch;
        // this passive component grants neither bytes nor an extra wallet.
        let changes = Vec::with_capacity(execution.accepted_protocol_task_changes().len());
        let selected_failure = execution.selected_task_failure().cloned();
        self.cut = Some(Cut {
            remaining: execution.take_accepted_protocol_task_changes(),
            publication: ProtocolPublication {
                owner_revision: revision,
                changes,
                selected_failure,
                root_success_sealed: sealed,
            },
        });
        Ok(true)
    }
    pub(crate) fn advance(
        &mut self,
        execution: &QueryTaskExecution,
        context: &mut dyn StepContext,
    ) -> Result<PublicationProgress, TaskExecutionError> {
        self.verify_execution(execution)?;
        if self.failed {
            return Ok(PublicationProgress::Failed);
        }
        if self.flight.is_some() {
            return Ok(PublicationProgress::AwaitingApply);
        }
        let Some(cut) = self.cut.as_mut() else {
            return Ok(PublicationProgress::Idle);
        };
        if execution.accepted_protocol_revision() != cut.publication.owner_revision
            || !execution.accepted_protocol_task_changes().is_empty()
        {
            return Err(error(
                "primary protocol facts changed during publication assembly",
            ));
        }
        while let Some(&task_id) = cut.remaining.first() {
            if !context.charge(1) {
                return Ok(PublicationProgress::Yield);
            }
            let task = execution
                .task(task_id)
                .ok_or_else(|| error("changed accepted Task is absent from its frozen graph"))?;
            // TaskStatus has no rich writer/provider payload. Its sole dynamic
            // value is one bounded SafeDetail (at most 512 content bytes).
            // Both copies are bounded by that existing contract per paid row.
            let row = ProtocolTaskChange {
                identity: task.identity(),
                status: task.status().cloned(),
                first_writer_witness: execution
                    .accepted_protocol_writer_witness(task_id)
                    .map(|witness| witness.as_ref().clone()),
            };
            cut.publication.changes.push(row);
            cut.remaining.pop_first();
        }
        Ok(PublicationProgress::Ready)
    }
    /// Full leaves this same immutable allocation and cut in place. The host
    /// must prepay its original result Source before taking the ready value.
    pub(crate) fn ready(&self) -> Option<&ProtocolPublication> {
        self.cut
            .as_ref()
            .filter(|cut| cut.remaining.is_empty())
            .map(|cut| &cut.publication)
    }
    pub(crate) fn take_ready(&mut self, effect: AttemptActionId) -> Option<ProtocolPublication> {
        let batch = self.ready()?;
        self.flight = Some(Flight {
            effect,
            revision: batch.owner_revision,
            sealed: batch.root_success_sealed,
        });
        Some(
            self.cut
                .take()
                .expect("the ready cut remains owned")
                .publication,
        )
    }
    pub(crate) fn applied(
        &mut self,
        effect: AttemptActionId,
        result: Result<bool, AttemptFailure>,
    ) -> PublicationApplyDisposition {
        let Some(flight) = self.flight.filter(|flight| flight.effect == effect) else {
            return PublicationApplyDisposition::Stale;
        };
        self.flight = None;
        match result {
            Ok(published) => {
                self.last_applied = Some((flight.revision, flight.sealed));
                PublicationApplyDisposition::Applied { published }
            }
            Err(failure) => {
                // The Core must enter the original error/cleanup policy. It
                // cannot resume the old round's later sends after failed apply.
                self.failed = true;
                PublicationApplyDisposition::Failed(failure)
            }
        }
    }
    fn verify_execution(&self, execution: &QueryTaskExecution) -> Result<(), TaskExecutionError> {
        if execution.graph().root_identity() != self.root {
            return Err(error(
                "protocol publication received another original graph",
            ));
        }
        Ok(())
    }
}
fn error(message: &str) -> TaskExecutionError {
    TaskExecutionError::Schedule(message.into())
}
