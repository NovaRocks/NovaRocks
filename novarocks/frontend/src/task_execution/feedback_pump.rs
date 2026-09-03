// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The frontend end of the dynamic filter feedback loop.
//!
//! A backend that reduced a runtime filter to a terminal logical domain
//! advertises a version in its task status and retains the payload for a
//! separate read. This is the reader: on every turn it compares each task's
//! advertised version against the version this frontend has already ingested,
//! fetches what is newer, and feeds it to the attempt's
//! [`RuntimeFilterFeedbackState`] -- which is what the connector split sources
//! are already blocked on.
//!
//! # What is not here
//!
//! No policy about waiting. The split-assignment round owns the initial wait
//! cap and degrades to unpruned enumeration when it elapses, so this reader is
//! purely an optimization path: a fetch that does not complete costs pruning
//! and never rows.
//!
//! # The cursor is per task
//!
//! One task's version must never suppress another's fetch. Versions are minted
//! per task by its own status owner, so two tasks can sit at version one with
//! entirely different payloads; a shared cursor would silently drop the second.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_execution::task_execution::domain::DomainVersion;
use novarocks_execution::task_execution::identity::TaskIdentity;
use novarocks_execution::task_execution::status::{DynamicFilterAdvertisement, TaskStatus};
use novarocks_types::identity::{BackendProcessId, TaskId};

use crate::native::fragment_transport::{
    DynamicFilterRead, DynamicFilterReadError, NativeTaskResultTransport, TaskResultTransport,
};
use crate::runtime_filter::feedback::{
    RuntimeFilterFeedbackAdmission, RuntimeFilterFeedbackState, TaskRuntimeFilterFeedback,
};

use super::error::TaskExecutionError;
use super::execution::QueryTaskExecution;
use super::round::TurnPump;

/// The one read this pump asks of the task transport.
///
/// Narrower than the transport it is implemented by, for the same reason the
/// runner depends on `StatusSubscriptions` rather than the whole subscriber: a
/// reader that could also poll a result or read a final info would grow a
/// second reason to hold this handle.
pub(crate) trait TaskDynamicFilterReads: Send + Sync {
    fn dynamic_filters(
        &self,
        identity: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError>;
}

impl TaskDynamicFilterReads for NativeTaskResultTransport {
    fn dynamic_filters(
        &self,
        identity: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        TaskResultTransport::dynamic_filters(self, identity, acknowledged)
    }
}

/// The frontend reader of one attempt's dynamic filter feedback.
pub(crate) struct DynamicFilterFeedbackPump {
    feedback: Arc<RuntimeFilterFeedbackState>,
    reads: Arc<dyn TaskDynamicFilterReads>,
    /// The highest version this frontend has settled per task.
    cursors: BTreeMap<TaskId, DomainVersion>,
    ingested: usize,
}

impl DynamicFilterFeedbackPump {
    /// The label this owner is counted under when it is installed.
    pub(crate) const PUMP_NAME: &'static str = "dynamic_filter_feedback";

    /// Builds the reader, or reports that this attempt has nothing to read.
    ///
    /// `declared_channels` is the count the sealed filter deployment declared.
    /// With none, no backend can ever publish feedback for this attempt and no
    /// split source can wait on one, so there is nothing to poll for and the
    /// runner is given no pump at all rather than one that scans every task
    /// every turn to find nothing.
    pub(crate) fn new(
        feedback: Arc<RuntimeFilterFeedbackState>,
        reads: Arc<dyn TaskDynamicFilterReads>,
        declared_channels: usize,
    ) -> Option<Self> {
        (declared_channels > 0).then_some(Self {
            feedback,
            reads,
            cursors: BTreeMap::new(),
            ingested: 0,
        })
    }

    /// How many advertised versions this reader has ingested.
    ///
    /// The loop's observable: a reader that is built but never driven, or
    /// driven but never supplied with an advertisement, reports zero.
    #[cfg(test)]
    pub(crate) const fn ingested(&self) -> usize {
        self.ingested
    }

    /// Every task whose latest status advertises a version above this reader's
    /// own cursor.
    fn outstanding(
        &self,
        execution: &QueryTaskExecution,
    ) -> Vec<(TaskId, TaskIdentity, DynamicFilterAdvertisement)> {
        let mut outstanding = Vec::new();
        for stage in execution.graph().stages() {
            let Some(execution_stage) = execution.stage(stage.stage_id()) else {
                continue;
            };
            for (&task_id, task) in execution_stage.tasks() {
                let Some(advertisement) = task.status().and_then(TaskStatus::dynamic_filters)
                else {
                    continue;
                };
                if self
                    .cursors
                    .get(&task_id)
                    .is_some_and(|settled| advertisement.version() <= *settled)
                {
                    continue;
                }
                outstanding.push((task_id, task.identity(), advertisement));
            }
        }
        outstanding
    }

    /// Reads one task and admits whatever it answered.
    fn ingest(
        &mut self,
        task_id: TaskId,
        identity: TaskIdentity,
        advertised: DomainVersion,
    ) -> Result<usize, TaskExecutionError> {
        let acknowledged = self.cursors.get(&task_id).copied();
        let read = match self.reads.dynamic_filters(identity, acknowledged) {
            Ok(read) => read,
            // The read did not complete. The cursor is left alone so the next
            // turn asks again, and the split source keeps waiting inside its
            // own cap.
            Err(DynamicFilterReadError::Unavailable(detail)) => {
                tracing::debug!(
                    task = %identity,
                    detail,
                    "dynamic filter read did not complete; it is retried next turn"
                );
                return Ok(0);
            }
            // The backend answered that this request is not legal against the
            // task it names. That is a disagreement about what this frontend
            // is looking at, not a lost optimization, so it is not swallowed.
            Err(DynamicFilterReadError::Refused(detail)) => {
                return Err(TaskExecutionError::Schedule(format!(
                    "dynamic filter read for task {identity} was refused: {detail}"
                )));
            }
        };
        let Some(version) = read.version() else {
            // Settled with nothing: the task went terminal and no longer
            // retains what it advertised. Nothing further will arrive for this
            // version, so the cursor moves past it -- leaving it behind would
            // re-ask every turn for a payload that is gone. A later, higher
            // version would clear this cursor again.
            self.cursors.insert(task_id, advertised);
            tracing::debug!(
                task = %identity,
                advertised = advertised.get(),
                "advertised dynamic filter version is no longer retained"
            );
            return Ok(0);
        };
        if version < advertised {
            // The response contract is "never below the version that triggered
            // the fetch". A lower one means the answer does not correspond to
            // the advertisement this read was made against.
            return Err(TaskExecutionError::Schedule(format!(
                "dynamic filter read for task {identity} answered version {} below the advertised \
                 version {}",
                version.get(),
                advertised.get()
            )));
        }
        let publisher = identity.backend_process_id();
        for feedback in read.feedback() {
            self.admit(identity, feedback, publisher)?;
        }
        self.cursors.insert(task_id, version);
        self.ingested += 1;
        emit_ingested_marker(identity, version, read.feedback().len());
        Ok(1)
    }

    fn admit(
        &self,
        identity: TaskIdentity,
        feedback: &TaskRuntimeFilterFeedback,
        publisher: BackendProcessId,
    ) -> Result<(), TaskExecutionError> {
        match self.feedback.admit_task_feedback(feedback, publisher) {
            Ok(RuntimeFilterFeedbackAdmission::Applied) => Ok(()),
            // Both fenced dispositions leave query-local state untouched, and
            // neither can be caused by this frontend asking: they mean the
            // answer described another attempt or another publisher.
            Ok(disposition) => Err(TaskExecutionError::Schedule(format!(
                "dynamic filter feedback from task {identity} channel {} was fenced as \
                 {disposition:?}",
                feedback.channel_id()
            ))),
            Err(detail) => Err(TaskExecutionError::Schedule(format!(
                "dynamic filter feedback from task {identity} channel {} is not admissible: \
                 {detail}",
                feedback.channel_id()
            ))),
        }
    }
}

impl TurnPump for DynamicFilterFeedbackPump {
    fn name(&self) -> &'static str {
        Self::PUMP_NAME
    }

    fn drive(&mut self, execution: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError> {
        let mut moved = 0;
        for (task_id, identity, advertisement) in self.outstanding(execution) {
            moved += self.ingest(task_id, identity, advertisement.version())?;
        }
        Ok(moved)
    }
}

/// Stable evidence that a fetched version reached the attempt's feedback state.
///
/// It fires where the loop does its work -- after admission, not where the
/// reader is constructed -- because a marker emitted at construction would say
/// only that the reader exists, which is exactly the defect this loop had.
fn emit_ingested_marker(identity: TaskIdentity, version: DomainVersion, domains: usize) {
    if !(cfg!(debug_assertions)
        && std::env::var_os("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER").is_some())
    {
        return;
    }
    let execution = identity.query_execution_id();
    eprintln!(
        "NOVAROCKS_TASK_DYNAMIC_FILTER_INGESTED execution_id={}:{}:{} stage={} task={} version={} domains={}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        version.get(),
        domains,
    );
}
