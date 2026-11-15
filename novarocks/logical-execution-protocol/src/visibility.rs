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

//! The four logical decisions on the result path. Segments remain in the relay.

use crate::start::{LogicalOutputMode, RootCarrier};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_execution_contract::{TaskIdentity, root_result::RootResultEnd};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchemaState {
    NotStarted,
    InFlight,
    Done,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenDecision {
    WriteSchema,
    Opened,
    DeliveryGranted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultPathRefusal {
    Concluded,
    Frozen,
    ForeignActivation,
    CarrierMismatch,
    NotOpened,
    SchemaPending,
    DuplicateOpen,
    ConflictingRoot,
    InvalidEnd,
    EndAlreadyConsumed,
    EndSlotNotRequested,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureDisposition {
    /// The original failure is retained while recovery is decided.
    Freeze,
    /// A visible prefix cannot be replayed by a successor.
    Conclude,
    /// A committed success or Ready cannot be rewritten by transport cleanup.
    Residual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EndSummary {
    pub end: RootResultEnd,
    pub accepted_packets: u64,
    pub delivered_through: Option<u64>,
}

impl EndSummary {
    pub(crate) fn validate(self) -> Result<(), ResultPathRefusal> {
        let packets = self.end.sequence.get() - 1;
        if packets != self.accepted_packets || self.delivered_through != packets.checked_sub(1) {
            return Err(ResultPathRefusal::InvalidEnd);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct OpenDelivery {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    frozen: bool,
    end: Option<EndSummary>,
    end_slot_requested: bool,
}

/// Schema is logical-execution-wide; only the current delivery changes on retry.
#[derive(Debug)]
pub(crate) struct VisibilityState {
    output: LogicalOutputMode,
    carrier: Option<RootCarrier>,
    schema: SchemaState,
    visible_to: Option<AttemptActivationIdentity>,
    frozen: Option<AttemptActivationIdentity>,
    open: Option<OpenDelivery>,
    committed: bool,
}

impl VisibilityState {
    pub(crate) fn frozen(&self, activation: AttemptActivationIdentity) -> bool {
        self.frozen == Some(activation)
    }
    pub(crate) fn new(output: LogicalOutputMode, carrier: Option<RootCarrier>) -> Self {
        Self {
            output,
            carrier,
            schema: SchemaState::NotStarted,
            visible_to: None,
            frozen: None,
            open: None,
            committed: false,
        }
    }

    pub(crate) const fn schema(&self) -> SchemaState {
        self.schema
    }
    pub(crate) const fn visible_to(&self) -> Option<AttemptActivationIdentity> {
        self.visible_to
    }
    pub(crate) const fn committed(&self) -> bool {
        self.committed
    }
    pub(crate) fn root(&self, activation: AttemptActivationIdentity) -> Option<TaskIdentity> {
        self.open
            .filter(|o| o.activation == activation)
            .map(|o| o.root)
    }

    /// `current` is the state owner's Running verdict, not a caller's guessed id.
    pub(crate) fn open_delivery(
        &mut self,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        carrier: RootCarrier,
        current: bool,
        concluded: bool,
    ) -> Result<OpenDecision, ResultPathRefusal> {
        self.check(activation, current, concluded)?;
        if self.carrier != Some(carrier) || carrier.validate().is_err() {
            return Err(ResultPathRefusal::CarrierMismatch);
        }
        if self.output == LogicalOutputMode::CompletionOnly {
            return Err(ResultPathRefusal::CarrierMismatch);
        }
        if root.query_execution_id() != activation.execution() {
            return Err(ResultPathRefusal::ForeignActivation);
        }
        if let Some(open) = self.open
            && open.activation == activation
        {
            return Err(if open.root == root {
                ResultPathRefusal::DuplicateOpen
            } else {
                ResultPathRefusal::ConflictingRoot
            });
        }
        self.open = Some(OpenDelivery {
            activation,
            root,
            frozen: false,
            end: None,
            end_slot_requested: false,
        });
        match self.output {
            LogicalOutputMode::InternalResultStream => Ok(OpenDecision::DeliveryGranted),
            LogicalOutputMode::ResultStream => {
                if self.schema == SchemaState::NotStarted {
                    self.schema = SchemaState::InFlight;
                    Ok(OpenDecision::WriteSchema)
                } else {
                    Ok(OpenDecision::Opened)
                }
            }
            LogicalOutputMode::CompletionOnly => Err(ResultPathRefusal::CarrierMismatch),
        }
    }

    /// The one original schema writer may finish after its originating attempt.
    pub(crate) fn schema_written(&mut self) -> Result<(), ResultPathRefusal> {
        match self.schema {
            SchemaState::InFlight => {
                self.schema = SchemaState::Done;
                Ok(())
            }
            SchemaState::Done => Ok(()),
            SchemaState::NotStarted => Err(ResultPathRefusal::SchemaPending),
        }
    }

    /// The relay has already reserved its original consumer queue position.
    pub(crate) fn grant_visibility(
        &mut self,
        activation: AttemptActivationIdentity,
        current: bool,
        concluded: bool,
    ) -> Result<(), ResultPathRefusal> {
        self.check(activation, current, concluded)?;
        let open = self.open.as_ref().ok_or(ResultPathRefusal::NotOpened)?;
        if open.activation != activation {
            return Err(ResultPathRefusal::ForeignActivation);
        }
        if self.output != LogicalOutputMode::ResultStream {
            return Err(ResultPathRefusal::CarrierMismatch);
        }
        if self.schema != SchemaState::Done {
            return Err(ResultPathRefusal::SchemaPending);
        }
        self.visible_to = Some(activation);
        Ok(())
    }

    pub(crate) fn failure(
        &mut self,
        activation: AttemptActivationIdentity,
        current: bool,
        concluded: bool,
    ) -> Result<FailureDisposition, ResultPathRefusal> {
        if self.committed || concluded {
            return Ok(FailureDisposition::Residual);
        }
        if !current {
            return Err(ResultPathRefusal::ForeignActivation);
        }
        self.frozen = Some(activation);
        if let Some(open) = self.open.as_mut()
            && open.activation == activation
        {
            open.frozen = true;
        }
        Ok(if self.visible_to == Some(activation) {
            FailureDisposition::Conclude
        } else {
            FailureDisposition::Freeze
        })
    }

    pub(crate) fn end_consumed(
        &mut self,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        summary: EndSummary,
        current: bool,
        concluded: bool,
    ) -> Result<(), ResultPathRefusal> {
        self.check(activation, current, concluded)?;
        summary.validate()?;
        let open = self.open.as_mut().ok_or(ResultPathRefusal::NotOpened)?;
        if open.activation != activation || open.root != root {
            return Err(ResultPathRefusal::ConflictingRoot);
        }
        match open.end {
            Some(held) if held == summary => Ok(()),
            Some(_) => Err(ResultPathRefusal::EndAlreadyConsumed),
            None => {
                open.end = Some(summary);
                Ok(())
            }
        }
    }

    pub(crate) fn end(&self, activation: AttemptActivationIdentity) -> Option<EndSummary> {
        self.open
            .filter(|o| o.activation == activation)
            .and_then(|o| o.end)
    }

    /// Four independent gates: Finished, exact consumed End, delivered frontier,
    /// and the Establish ledger. A success seal is additionally required.
    pub(crate) fn request_end_slot(
        &mut self,
        activation: AttemptActivationIdentity,
        root_finished: bool,
        success_sealed: bool,
        establish_ready: bool,
    ) -> Result<bool, ResultPathRefusal> {
        let open = self.open.as_mut().ok_or(ResultPathRefusal::NotOpened)?;
        if open.activation != activation {
            return Err(ResultPathRefusal::ForeignActivation);
        }
        if open.frozen || self.committed {
            return Ok(false);
        }
        if self.output != LogicalOutputMode::ResultStream
            || !root_finished
            || !success_sealed
            || !establish_ready
            || self.schema != SchemaState::Done
        {
            return Ok(false);
        }
        let Some(summary) = open.end else {
            return Ok(false);
        };
        summary.validate()?;
        if open.end_slot_requested {
            return Ok(false);
        }
        open.end_slot_requested = true;
        Ok(true)
    }

    /// Commit before emitting EnqueueEnd. Later End disposition is transport only.
    pub(crate) fn commit_end_slot(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), ResultPathRefusal> {
        let open = self.open.as_ref().ok_or(ResultPathRefusal::NotOpened)?;
        if open.activation != activation {
            return Err(ResultPathRefusal::ForeignActivation);
        }
        if open.frozen || !open.end_slot_requested {
            return Err(ResultPathRefusal::EndSlotNotRequested);
        }
        if self.committed {
            return Err(ResultPathRefusal::Concluded);
        }
        self.committed = true;
        Ok(())
    }

    pub(crate) fn commit_ready(&mut self) {
        self.committed = true;
    }

    fn check(
        &self,
        activation: AttemptActivationIdentity,
        current: bool,
        concluded: bool,
    ) -> Result<(), ResultPathRefusal> {
        if concluded || self.committed {
            return Err(ResultPathRefusal::Concluded);
        }
        if !current {
            return Err(ResultPathRefusal::ForeignActivation);
        }
        if self.frozen == Some(activation) {
            return Err(ResultPathRefusal::Frozen);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
