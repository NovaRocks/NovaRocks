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

//! One actual async position, without an independent local execution queue.
//! The original stable source retains this grant until its own phase exits.
use super::*;
use std::num::NonZeroU64;

#[must_use = "Install the admission in the same original async source"]
pub struct AsyncActionSource {
    original: control_action::ControlActionSource,
}
#[must_use = "Drive the original finite admission before polling physical I/O"]
pub struct AsyncActionAdmission {
    original: control_action::ControlActionAdmission,
}
/// The original joint AsyncIo positions themselves are the execution slots.
/// There is no second pool, unit physical receipt, or finite CPU privilege.
#[must_use = "Retain through the same actual I/O phase and its real Drop"]
pub struct AsyncActionGrant {
    pub(super) claims: [JointClaim; 1],
}
impl WorkOwner {
    pub fn async_action_source(&self, lane: JointLane) -> Result<AsyncActionSource, WorkError> {
        Ok(AsyncActionSource {
            original: self.control_action_source(lane, control_action::ControlActionKind::Async)?,
        })
    }
}
impl RetainedPayloadGrant {
    /// Continue only this original positive Root's physical I/O Source after
    /// its WorkOwner moved into a supervised execution/protocol receiver.
    /// Retention aliases and WorkScope alone cannot create this author.
    pub fn async_continuation_source(
        &self,
        root: &WorkScope,
        lane: JointLane,
    ) -> Result<AsyncActionSource, WorkError> {
        Ok(AsyncActionSource {
            original: control_action::capture_payload_continuation(self, root, lane)?,
        })
    }
}
impl AsyncActionSource {
    pub fn try_begin(
        &self,
        metadata_bytes: NonZeroU64,
        positions: NonZeroU64,
    ) -> Result<JointTryBegin<AsyncActionAdmission>, JointAdmissionFailure> {
        match self.original.try_begin(metadata_bytes, positions)? {
            JointTryBegin::Started(original) => {
                Ok(JointTryBegin::Started(AsyncActionAdmission { original }))
            }
            JointTryBegin::Full(wait) => Ok(JointTryBegin::Full(wait)),
        }
    }
    pub fn begin(
        self,
        metadata_bytes: NonZeroU64,
        positions: NonZeroU64,
    ) -> Result<AsyncActionAdmission, JointAdmissionFailure> {
        Ok(AsyncActionAdmission {
            original: self.original.begin(metadata_bytes, positions)?,
        })
    }
}
impl AsyncActionAdmission {
    /// Both outcomes preserve the destination; successful publication is only
    /// an internal move. A caller must not hold a foreign mutex during poll.
    pub fn poll_install_in(
        &mut self,
        cx: &mut Context<'_>,
        destination: &mut Option<AsyncActionGrant>,
    ) -> Poll<Result<(), JointAdmissionFailure>> {
        if destination.is_some() {
            return Poll::Ready(Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            }));
        }
        let mut batch = match Pin::new(&mut self.original.admission).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(batch)) => batch,
        };
        // The request author admits exactly one all-New action. The ordinary
        // reply performs the exact source recheck before handing out guards.
        if batch.adjustment.is_some() || batch.new_claims.len() != 1 {
            return Poll::Ready(Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            }));
        }
        let claim = batch
            .new_claims
            .pop()
            .expect("checked single original async claim");
        *destination = Some(AsyncActionGrant { claims: [claim] });
        // The stable original physical grant now owns its claim. Only the
        // empty reply container/body exits; no live AsyncIo amount is retired.
        drop(batch);
        Poll::Ready(Ok(()))
    }
}

/// Initial actual Root backing, before the first Product F poll. Its exact D
/// is the already-bound original control D. Subsequent growth uses the closed
/// RootPayload batch; no raw claim/view escapes this constructor.
#[must_use]
pub struct RootPayloadInitialAdmission {
    original: control_action::ControlActionAdmission,
    batch: Option<JointGrantedBatch>,
    raw: Option<JointClaim>,
}
#[must_use]
pub struct RootPayloadInitialSource {
    original: control_action::ControlActionSource,
}
impl RootPayloadInitialSource {
    pub fn try_begin(
        &self,
        metadata_bytes: NonZeroU64,
        payload_bytes: NonZeroU64,
    ) -> Result<JointTryBegin<RootPayloadInitialAdmission>, JointAdmissionFailure> {
        match self.original.try_begin(metadata_bytes, payload_bytes)? {
            JointTryBegin::Started(original) => {
                Ok(JointTryBegin::Started(RootPayloadInitialAdmission {
                    original,
                    batch: None,
                    raw: None,
                }))
            }
            JointTryBegin::Full(wait) => Ok(JointTryBegin::Full(wait)),
        }
    }
}
impl WorkOwner {
    pub fn root_payload_initial_source(&self) -> Result<RootPayloadInitialSource, WorkError> {
        Ok(RootPayloadInitialSource {
            original: self.control_action_source(
                JointLane::Foreground,
                control_action::ControlActionKind::Payload,
            )?,
        })
    }
    pub fn begin_root_payload(
        &self,
        metadata_bytes: NonZeroU64,
        payload_bytes: NonZeroU64,
    ) -> Result<RootPayloadInitialAdmission, JointAdmissionFailure> {
        let source = self
            .control_action_source(
                JointLane::Foreground,
                control_action::ControlActionKind::Payload,
            )
            .map_err(|error| JointAdmissionFailure { error })?;
        Ok(RootPayloadInitialAdmission {
            original: source.begin(metadata_bytes, payload_bytes)?,
            batch: None,
            raw: None,
        })
    }
}
impl RootPayloadInitialAdmission {
    pub fn poll_install_in(
        &mut self,
        cx: &mut Context<'_>,
        root: &WorkScope,
        target: &mut Option<RetainedPayloadGrant>,
    ) -> Poll<Result<crate::WorkloadProgressNotification, JointAdmissionFailure>> {
        if target.is_some() {
            return Poll::Ready(Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            }));
        }
        if self.batch.is_none() {
            let batch = match Pin::new(&mut self.original.admission).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(batch)) => batch,
            };
            self.batch = Some(batch);
        }
        let batch = self.batch.as_mut().unwrap();
        if self.raw.is_none() {
            if batch.adjustment.is_some() || batch.new_claims.len() != 1 {
                return Poll::Ready(Err(JointAdmissionFailure {
                    error: WorkError::Conflict,
                }));
            }
            self.raw = batch.new_claims.pop();
        }
        Poll::Ready(
            RetainedPayloadGrant::adopt_in(root, &mut self.raw, target)
                .map_err(|error| JointAdmissionFailure { error }),
        )
    }
}

#[cfg(test)]
#[path = "async_action/tests.rs"]
mod tests;
