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

//! Linear input custody. An edge never owns receiver materialization state.

use crate::runtime::mem_tracker::MemTracker;
use crate::{
    exec::chunk::Chunk,
    runtime::task::{ExecutionFailure, ExecutionResult, io::RootInputPermit},
};
use std::{fmt, sync::Arc};

/// Field order is intentional: input storage exits before its original permit.
pub struct RawInput {
    chunk: Chunk,
    permit: Option<RootInputPermit>,
}
impl RawInput {
    pub(super) fn new(chunk: Chunk, permit: Option<RootInputPermit>) -> Self {
        Self { chunk, permit }
    }
    pub fn rows(&self) -> usize {
        self.chunk.len()
    }
    pub fn result_metadata(&self) -> crate::runtime::task::io::ResultInputMetadata {
        (&self.chunk).into()
    }
    #[cfg(test)]
    pub(crate) fn for_test(chunk: Chunk) -> Self {
        Self::new(chunk, None)
    }
    #[cfg(test)]
    pub(crate) fn covered_for_test(chunk: Chunk, permit: RootInputPermit) -> Self {
        Self::new(chunk, Some(permit))
    }
    #[cfg(test)]
    pub(crate) fn original_for_test(&self) -> &Chunk {
        &self.chunk
    }
    #[cfg(test)]
    pub(super) fn original(&self) -> &Chunk {
        &self.chunk
    }
    pub(super) fn permit(&self) -> Option<&RootInputPermit> {
        self.permit.as_ref()
    }
    pub(super) fn into_edge_parts(self) -> (Chunk, Option<RootInputPermit>) {
        (self.chunk, self.permit)
    }
    /// Only the receiver's acceptance path can transfer this responsibility.
    pub(super) fn accept(mut self, tracker: Option<&Arc<MemTracker>>) -> AcceptedInput {
        if self.permit.is_none()
            && let Some(tracker) = tracker
        {
            self.chunk.transfer_to(tracker);
        }
        AcceptedInput {
            raw: self,
            tracker: tracker.cloned(),
        }
    }
}
impl fmt::Debug for RawInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawInput")
            .field("rows", &self.rows())
            .field("precovered", &self.permit.is_some())
            .finish()
    }
}

pub struct AcceptedInput {
    raw: RawInput,
    tracker: Option<Arc<MemTracker>>,
}
impl AcceptedInput {
    /// Preparation runs under the receiver's binding. On failure, the accepted
    /// input is still returned to that receiver's cleanup, never to the edge.
    pub(super) fn prepare(
        self,
        prepare: impl FnOnce(&Chunk) -> ExecutionResult<Chunk>,
    ) -> Result<PreparedInput, InputPreparationFailure> {
        match prepare(&self.raw.chunk) {
            Ok(mut chunk) => {
                if let Some(tracker) = self.tracker.as_ref() {
                    chunk.transfer_to(tracker);
                }
                let RawInput {
                    chunk: original,
                    permit,
                } = self.raw;
                drop(original);
                Ok(PreparedInput { chunk, permit })
            }
            Err(failure) => Err(InputPreparationFailure {
                input: self,
                failure,
            }),
        }
    }
    /// Cleanup takes original custody without claiming preparation completed.
    /// The receiving owner must retain both parts before invoking destructors.
    pub(super) fn into_exit_parts(self) -> (Chunk, Option<RootInputPermit>) {
        self.raw.into_edge_parts()
    }
    /// An original-input consumer keeps its own finite carrier validation and
    /// preparation policy. This does not add a driver hydration pass.
    pub(super) fn prepare_original(self) -> PreparedInput {
        let RawInput { chunk, permit } = self.raw;
        PreparedInput { chunk, permit }
    }
}

pub struct InputPreparationFailure {
    pub input: AcceptedInput,
    pub failure: ExecutionFailure,
}

pub struct PreparedInput {
    chunk: Chunk,
    permit: Option<RootInputPermit>,
}
impl PreparedInput {
    pub fn into_uncovered_chunk(self) -> ExecutionResult<Chunk> {
        if self.permit.is_some() {
            return Err(novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "ordinary kernel received a precovered root input",
                ),
            )
            .into());
        }
        Ok(self.chunk)
    }
    #[cfg(test)]
    pub(crate) fn for_test(chunk: Chunk) -> Self {
        Self {
            chunk,
            permit: None,
        }
    }
    pub fn chunk(&self) -> &Chunk {
        &self.chunk
    }
    pub fn into_parts(self) -> (Chunk, Option<RootInputPermit>) {
        (self.chunk, self.permit)
    }
}

/// A parent operation has accepted this derived branch output. Each child
/// still performs its own admission and installed preparation policy.
pub(crate) fn derived_raw_input(chunk: Chunk) -> RawInput {
    RawInput::new(chunk, None)
}
