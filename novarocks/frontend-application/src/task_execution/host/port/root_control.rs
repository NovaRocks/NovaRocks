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

//! Exact root-control correlation for commands already committed by the LP.
use super::control::PortControlError;
use crate::control_runtime::actor::FrontendActor;
use crate::task_execution::host::observations::{
    ObservationError, OrderedObservationInput, WeakOrderedObservationInput,
};
pub(crate) use crate::task_execution::host::observations::{RootControlBinding, RootControlRef};
use novarocks_coordination_contract::{
    AttemptRequestToken,
    logical_port::{
        AttemptToLogical, AttemptToLogicalBody, LogicalToAttempt, LogicalToAttemptBody,
    },
};
use novarocks_execution_contract::TaskIdentity;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootControlKind {
    Seal,
    Terminal,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RootControlCommand {
    pub(crate) token: AttemptRequestToken,
    pub(crate) kind: RootControlKind,
    pub(crate) root: TaskIdentity,
}
#[derive(Clone, Copy)]
struct Row {
    command: RootControlCommand,
    queued: bool,
    submitting: bool,
    replied: bool,
}
#[derive(Default)]
struct Rows {
    seal: Option<Row>,
    terminal: Option<Row>,
    closed: bool,
    fenced: bool,
    writer_error: Option<ObservationError>,
}
impl Rows {
    fn row(&self, kind: RootControlKind) -> &Option<Row> {
        match kind {
            RootControlKind::Seal => &self.seal,
            RootControlKind::Terminal => &self.terminal,
        }
    }
    fn row_mut(&mut self, kind: RootControlKind) -> &mut Option<Row> {
        match kind {
            RootControlKind::Seal => &mut self.seal,
            RootControlKind::Terminal => &mut self.terminal,
        }
    }
}
/// One original Seal and one pending terminal demand. No QA request, success
/// publisher, callback, Scope control or second token source is stored here.
pub(crate) struct RootControlRequests {
    binding: RootControlBinding,
    rows: Mutex<Rows>,
    ingress: Mutex<Option<WeakOrderedObservationInput<FrontendActor>>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootMaterialization {
    Queued,
    AlreadyQueued,
    Blocked,
}
impl RootControlRequests {
    pub(crate) fn new(binding: RootControlBinding) -> Arc<Self> {
        Arc::new(Self {
            binding,
            rows: Mutex::new(Rows::default()),
            ingress: Mutex::new(None),
        })
    }
    pub(crate) fn backing_payload_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<Self>()
    }
    pub(crate) fn bind_ingress(
        &self,
        original: &OrderedObservationInput<FrontendActor>,
    ) -> Result<(), PortControlError> {
        if !self.binding.same_original(original.root_binding())
            || self.binding.reply_author().is_none()
        {
            return Err(PortControlError::ForeignPair);
        }
        let rows = self.rows.lock().expect("original root control rows");
        if rows.closed || rows.fenced {
            return Err(PortControlError::Closed);
        }
        let mut ingress = self
            .ingress
            .lock()
            .expect("original root observation writer");
        if ingress.is_some() {
            return Err(PortControlError::ForeignPair);
        }
        *ingress = Some(original.weak());
        Ok(())
    }
    pub(super) fn enqueue_original(
        &self,
        reference: RootControlRef,
    ) -> Result<(), ObservationError> {
        let ingress = self
            .ingress
            .lock()
            .expect("original root observation writer")
            .as_ref()
            .and_then(|source| source.upgrade())
            .ok_or(ObservationError::Closed)?;
        ingress.queue_root_control(reference)?;
        // Acceptance has already transferred the exact original reference.
        // A later drive error belongs to failure custody, not a false refusal.
        if let Err(original) = ingress.drive() {
            self.rows
                .lock()
                .expect("original root control rows")
                .writer_error = Some(original);
        }
        Ok(())
    }
    pub(crate) fn take_writer_error(&self) -> Option<ObservationError> {
        self.rows
            .lock()
            .expect("original root control rows")
            .writer_error
            .take()
    }
    pub(crate) fn binding(&self) -> &RootControlBinding {
        &self.binding
    }
    pub(crate) fn validate_reference(
        &self,
        original: &RootControlRef,
    ) -> Result<(), ObservationError> {
        if !original.belongs_to(&self.binding) {
            return Err(ObservationError::InvalidRootControl);
        }
        let rows = self.rows.lock().expect("original root control rows");
        if rows.closed
            || ![&rows.seal, &rows.terminal]
                .into_iter()
                .flatten()
                .any(|r| r.command.token == original.token() && !r.replied)
        {
            return Err(ObservationError::InvalidRootControl);
        }
        Ok(())
    }
    pub(crate) fn project(
        &self,
        original: &RootControlRef,
    ) -> Result<LogicalToAttempt, ObservationError> {
        self.validate_reference(original)?;
        let rows = self.rows.lock().expect("original root control rows");
        let row = [&rows.seal, &rows.terminal]
            .into_iter()
            .flatten()
            .find(|r| r.command.token == original.token() && !r.replied)
            .ok_or(ObservationError::InvalidRootControl)?;
        Ok(LogicalToAttempt {
            activation: self.binding.activation(),
            body: match row.command.kind {
                RootControlKind::Seal => LogicalToAttemptBody::SealRequest {
                    token: original.token(),
                },
                RootControlKind::Terminal => LogicalToAttemptBody::TerminalControlDemand {
                    token: original.token(),
                    root: self.binding.root(),
                },
            },
        })
    }

    /// Only the reader of the actual LP-published cell calls this. The queue
    /// runs after both the snapshot and row lock have been released, preserving
    /// the original observation writer's cross-class FIFO and real credit.
    pub(super) fn materialize(
        &self,
        command: RootControlCommand,
        mut enqueue: impl FnMut(RootControlRef) -> Result<(), ObservationError>,
    ) -> Result<RootMaterialization, PortControlError> {
        if command.root != self.binding.root() {
            return Err(PortControlError::ForeignRoot);
        }
        {
            let mut rows = self.rows.lock().expect("original root control rows");
            if rows.closed {
                return Err(PortControlError::Closed);
            }
            if rows.fenced && rows.row(command.kind).is_none() {
                return Err(PortControlError::Closed);
            }
            if [&rows.seal, &rows.terminal]
                .into_iter()
                .flatten()
                .any(|r| r.command.token == command.token && r.command.kind != command.kind)
            {
                return Err(PortControlError::ConflictingRootToken);
            }
            let row = rows.row_mut(command.kind);
            if let Some(prior) = row {
                if prior.command != command {
                    return Err(PortControlError::ConflictingRootToken);
                }
                if prior.queued || prior.replied {
                    return Ok(RootMaterialization::AlreadyQueued);
                }
                if prior.submitting {
                    return Ok(RootMaterialization::Blocked);
                }
                prior.submitting = true;
            } else {
                *row = Some(Row {
                    command,
                    queued: false,
                    submitting: true,
                    replied: false,
                });
            }
        }
        let result = enqueue(self.binding.reference(command.token));
        let mut rows = self.rows.lock().expect("original root control rows");
        let row = rows
            .row_mut(command.kind)
            .as_mut()
            .expect("same installed root control row");
        row.submitting = false;
        match result {
            Ok(()) => {
                row.queued = true;
                Ok(RootMaterialization::Queued)
            }
            Err(ObservationError::Busy) => Ok(RootMaterialization::Blocked),
            Err(original) => Err(PortControlError::Observation(original)),
        }
    }
    /// Called by the original fact outlet only after the exact AP reply has
    /// actually entered the LP inlet. This is not input/producer exit evidence.
    pub(crate) fn reply_delivered(
        &self,
        original: &AttemptToLogical,
    ) -> Result<bool, PortControlError> {
        if original.activation != self.binding.activation() {
            return Err(PortControlError::ForeignActivation);
        }
        let (token, kind) = match &original.body {
            AttemptToLogicalBody::SealResult { token, .. } => (*token, RootControlKind::Seal),
            AttemptToLogicalBody::TerminalControlResult { token, .. } => {
                (*token, RootControlKind::Terminal)
            }
            _ => return Err(PortControlError::WrongMessage),
        };
        let mut rows = self.rows.lock().expect("original root control rows");
        let row = rows
            .row_mut(kind)
            .as_mut()
            .filter(|r| r.command.token == token && (r.queued || r.submitting))
            .ok_or(PortControlError::ConflictingRootToken)?;
        Ok(!std::mem::replace(&mut row.replied, true))
    }
    pub(crate) fn close_after_exit(&self) -> Result<(), PortControlError> {
        let mut rows = self.rows.lock().expect("original root control rows");
        if rows.closed {
            return Ok(());
        }
        if [&rows.seal, &rows.terminal]
            .into_iter()
            .flatten()
            .any(|r| r.submitting || !r.replied)
        {
            return Err(PortControlError::UnsettledRoot);
        }
        rows.closed = true;
        drop(rows);
        let original = self
            .ingress
            .lock()
            .expect("original root observation writer")
            .take();
        drop(original);
        Ok(())
    }
    pub(crate) fn fence_for_close(&self) -> bool {
        let mut rows = self.rows.lock().expect("original root control rows");
        rows.fenced = true;
        ![&rows.seal, &rows.terminal]
            .into_iter()
            .flatten()
            .any(|r| r.submitting)
    }
    pub(super) fn close_after_failure(&self) {
        self.rows.lock().expect("original root control rows").closed = true;
        let original = self
            .ingress
            .lock()
            .expect("original root observation writer")
            .take();
        drop(original);
    }
}

impl crate::task_execution::host::observations::source::RootControlProjection
    for RootControlRequests
{
    fn matches(&self, binding: &RootControlBinding) -> bool {
        self.binding.same_original(binding)
    }
    fn close_after_exit(&self) -> Result<(), ObservationError> {
        RootControlRequests::close_after_exit(self).map_err(|_| ObservationError::Busy)
    }
    fn fence_for_close(&self) -> bool {
        RootControlRequests::fence_for_close(self)
    }
    fn project(
        &self,
        reference: &RootControlRef,
    ) -> Result<
        novarocks_coordination_contract::LogicalToAttempt<
            novarocks_attempt_protocol::failure::AttemptFailure,
        >,
        ObservationError,
    > {
        RootControlRequests::project(self, reference).map(|value| {
            novarocks_attempt_protocol::port_projection::project_logical_message(&value)
        })
    }
    fn validate_reference(&self, reference: &RootControlRef) -> Result<(), ObservationError> {
        RootControlRequests::validate_reference(self, reference)
    }
}

#[cfg(test)]
mod tests;
