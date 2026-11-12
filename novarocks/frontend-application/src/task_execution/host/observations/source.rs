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

//! The registered observation Source shares one physical window and writer.

use super::{
    MetadataRetirementRefusal, ObservationBudget, ObservationCredit, ObservationError,
    ObservationMetadataLayout, ObservationMetadataObligation, ObservationSourcePool,
    RootControlBinding, RootControlRef, StreamKey, backing, credits::ObservationCredits,
    read_progress::ReadProgressInput,
};
use crate::task_execution::bridge::root_control::RootControlRequests;
use novarocks_attempt_protocol::{failure::AttemptFailure, input::ReadProgressSnapshot};
use novarocks_coordination_contract::LogicalToAttempt;
use novarocks_execution_contract::ParticipationRef;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// The original assembler admits the actual structural backing before
/// constructing the Source. Its one retained claim covers the writer's
/// layouts and the Source's credit, progress, and frozen roster backing.
pub(crate) trait ObservationMetadataAuthor: Send + Sync + 'static {
    fn retain(
        &self,
        required: ObservationMetadataLayout,
        participants: usize,
    ) -> Result<Box<dyn ObservationMetadataObligation>, ObservationError>;
}

pub(crate) struct BoundObservationSource {
    binding: RootControlBinding,
    requests: Arc<RootControlRequests>,
    original: Arc<SourcePositions>,
    metadata: Arc<dyn ObservationMetadataAuthor>,
}

struct SourcePositions {
    credits: Arc<ObservationCredits>,
    progress: Mutex<BTreeMap<ParticipationRef, ReadProgressInput>>,
}

impl SourcePositions {
    fn retire_after_exit(&self) -> Result<(), ObservationError> {
        // Credits include all exact stream receipts. A refused publisher or
        // a still-live coalesced occurrence must keep its original role.
        self.credits.retire_after_exit()?;
        let mut roles = std::mem::take(
            &mut *self
                .progress
                .lock()
                .expect("original observation progress roster"),
        );
        while let Some((participant, role)) = roles.pop_first() {
            if let Err(error) = role.close_after_exit() {
                roles.insert(participant, role);
                *self
                    .progress
                    .lock()
                    .expect("original observation progress roster") = roles;
                return Err(ObservationError::Runtime(error));
            }
        }
        Ok(())
    }
}

impl BoundObservationSource {
    pub(super) fn backing_bytes(participants: usize) -> Result<usize, ObservationError> {
        backing::sum([
            backing::arc::<Self>()?.size(),
            backing::arc::<SourcePositions>()?.size(),
            std::mem::size_of::<SourceMetadata>(),
            backing::tree::<ParticipationRef, ReadProgressInput>(participants)?,
            ReadProgressInput::backing_bytes(participants)?,
            backing::array::<ReadProgressInput>(participants)?,
        ])
    }

    pub(crate) fn new(
        binding: RootControlBinding,
        requests: Arc<RootControlRequests>,
        credits: Arc<ObservationCredits>,
        progress: BTreeMap<ParticipationRef, ReadProgressInput>,
        metadata: Arc<dyn ObservationMetadataAuthor>,
    ) -> Arc<Self> {
        assert!(requests.matches(&binding));
        assert!(credits.matches_roster(progress.keys().copied()));
        Arc::new(Self {
            binding,
            requests,
            original: Arc::new(SourcePositions {
                credits,
                progress: Mutex::new(progress),
            }),
            metadata,
        })
    }

    /// Roll back original paid positions when the writer was never published.
    /// An admitted writer must go through its retained metadata obligation.
    pub(crate) fn retire_unpublished(&self) -> Result<(), ObservationError> {
        self.original.retire_after_exit()
    }
}

struct SourceMetadata {
    source: Arc<SourcePositions>,
    original: Option<Box<dyn ObservationMetadataObligation>>,
}

impl ObservationMetadataObligation for SourceMetadata {
    fn backing_pin(&self) -> Option<backing::ObservationBackingPin> {
        self.original.as_ref()?.backing_pin()
    }

    fn retire_after_exit(mut self: Box<Self>) -> Result<(), MetadataRetirementRefusal> {
        if let Err(error) = self.source.retire_after_exit() {
            return Err(MetadataRetirementRefusal {
                error,
                original: self,
            });
        }
        match self
            .original
            .take()
            .expect("original metadata backing")
            .retire_after_exit()
        {
            Ok(()) => Ok(()),
            Err(refused) => {
                self.original = Some(refused.original);
                Err(MetadataRetirementRefusal {
                    error: refused.error,
                    original: self,
                })
            }
        }
    }
}

impl ObservationSourcePool for BoundObservationSource {
    fn budget(&self) -> ObservationBudget {
        self.original.credits.budget()
    }

    fn fixed_root_control_binding(&self) -> RootControlBinding {
        self.binding.clone()
    }

    fn close_root_controls(&self) -> Result<(), ObservationError> {
        self.requests.close_after_exit()
    }

    fn fence_root_controls_for_close(&self) -> bool {
        self.requests.fence_for_close()
    }

    fn retain_metadata(
        &self,
        required: ObservationMetadataLayout,
    ) -> Result<Box<dyn ObservationMetadataObligation>, ObservationError> {
        let participants = self
            .original
            .progress
            .lock()
            .expect("original observation progress roster")
            .len();
        let original = self.metadata.retain(required, participants)?;
        Ok(Box::new(SourceMetadata {
            source: self.original.clone(),
            original: Some(original),
        }))
    }

    fn register_stream(&self, key: StreamKey) -> Result<(), ObservationError> {
        self.original.credits.register_stream(key)
    }

    fn retire_stream(&self, key: StreamKey) -> Result<(), ObservationError> {
        self.original.credits.retire_stream(key)
    }

    fn project_root_control(
        &self,
        reference: &RootControlRef,
    ) -> Result<LogicalToAttempt<AttemptFailure>, ObservationError> {
        self.requests.project(reference)
    }

    fn poll_read(
        &self,
        stream: StreamKey,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ObservationCredit, ObservationError>> {
        self.original.credits.poll_read(stream, cx)
    }

    fn cancel_read_wait(&self, stream: StreamKey) {
        self.original.credits.cancel_read_wait(stream);
    }

    fn read_resumed(&self, progress: ReadProgressSnapshot) -> Result<(), ObservationError> {
        let role = self
            .original
            .progress
            .lock()
            .expect("original observation progress roster")
            .get(&progress.participation)
            .cloned()
            .ok_or(ObservationError::UnknownStream)?;
        self.original.credits.read_resumed(&role, progress)
    }

    fn try_control(
        &self,
        reference: &RootControlRef,
    ) -> Result<ObservationCredit, ObservationError> {
        self.requests.validate_reference(reference)?;
        self.original.credits.try_control()
    }

    fn stream_claims_exited(&self, stream: StreamKey) -> bool {
        self.original.credits.stream_claims_exited(stream)
    }
}
