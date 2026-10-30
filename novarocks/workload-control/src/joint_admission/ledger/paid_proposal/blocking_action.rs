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

//! Blocking action API backed by the original shared NodeControl author.
use super::*;
use std::num::NonZeroU64;

pub struct BlockingActionSource {
    original: control_action::ControlActionSource,
}
pub struct BlockingActionAdmission {
    admission: JointAdmission,
}
impl WorkOwner {
    pub fn blocking_action_source(
        &self,
        lane: JointLane,
    ) -> Result<BlockingActionSource, WorkError> {
        Ok(BlockingActionSource {
            original: self
                .control_action_source(lane, control_action::ControlActionKind::Blocking)?,
        })
    }
}
impl BlockingActionSource {
    pub fn begin(
        self,
        metadata_bytes: NonZeroU64,
        physical_queue_bytes: NonZeroU64,
    ) -> Result<BlockingActionAdmission, JointAdmissionFailure> {
        Ok(BlockingActionAdmission {
            admission: self
                .original
                .begin(metadata_bytes, physical_queue_bytes)?
                .admission,
        })
    }
}
impl BlockingActionAdmission {
    pub fn poll_local_offer(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<JointLocalOffer<'_>, JointAdmissionFailure>> {
        self.admission.poll_local_offer(cx)
    }
}
#[cfg(test)]
mod tests;
