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

//! Correlated process reads against the original frozen manifest.

use crate::control_runtime::membership::FrozenMember;
use novarocks_attempt_protocol::input::{ExecutorFact, MembershipObservation};
use novarocks_control_runtime::actor_turn::Clock;
use novarocks_coordination_contract::{AttemptRequestToken, MonotonicInstant};
use novarocks_execution_contract::ParticipationRef;
use novarocks_query_application::api::{
    BackendProcessObservation, BackendProcessObservationService, BackendTopologyError,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) struct AttemptProcessObserver {
    source: BackendProcessObservationService,
    members: Arc<[FrozenMember]>,
    clock: Arc<dyn Clock>,
    error: Mutex<Option<(AttemptRequestToken, ParticipationRef, BackendTopologyError)>>,
}

#[derive(Debug)]
pub(super) struct ForeignProcessObservation;

impl AttemptProcessObserver {
    pub(crate) fn new(
        source: BackendProcessObservationService,
        members: Arc<[FrozenMember]>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        assert!(
            members
                .windows(2)
                .all(|pair| pair[0].participation < pair[1].participation)
        );
        Self {
            source,
            members,
            clock,
            error: Mutex::new(None),
        }
    }

    pub(super) fn read(
        &self,
        request: AttemptRequestToken,
        participation: ParticipationRef,
    ) -> Result<ExecutorFact, ForeignProcessObservation> {
        let member = self
            .members
            .binary_search_by_key(&participation, |member| member.participation)
            .map_err(|_| ForeignProcessObservation)?;
        let original = participation.backend_process_id();
        let result = self
            .source
            .observe_process_at_endpoint(original, &self.members[member].endpoint);
        let sampled_at = MonotonicInstant::from_origin(Duration::from_nanos(self.clock.now().0));
        let observation = match result {
            Ok(BackendProcessObservation::Current) => MembershipObservation::ExactOriginal,
            Ok(BackendProcessObservation::Replaced { current_process })
                if current_process != original =>
            {
                MembershipObservation::ExactReplacement(current_process)
            }
            Ok(_) => MembershipObservation::Unobserved,
            Err(error) => {
                *self.error.lock().unwrap() = Some((request, participation, error));
                MembershipObservation::Unobserved
            }
        };
        Ok(ExecutorFact::ProcessSnapshot {
            request,
            participation,
            sampled_at,
            observation,
        })
    }
}
