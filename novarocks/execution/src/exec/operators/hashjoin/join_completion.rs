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

//! Join's participant declaration and failure brand for shared stages.

use super::build_state::build_contract;
use crate::{
    exec::{
        operators::local_exchanger::LocalExchanger,
        pipeline::operator::InputConstructionOwner,
        shared_stage::stage_completion::{StageCompletion, StageParticipants},
    },
    runtime::{observable::Observable, task::ExecutionResult},
};
use novarocks_functions::KernelEvaluationControl;
use std::{num::NonZeroUsize, sync::Arc};

#[cfg(test)]
pub(super) use crate::exec::shared_stage::stage_completion::tests;
pub(super) use crate::exec::shared_stage::{
    arithmetic, funded_shared, funded_vec,
    stage_completion::{PublishResult, RemainingDemand},
};

pub(crate) type JoinCompletion<A> = StageCompletion<A>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JoinParticipants {
    pub(crate) inputs: usize,
    pub(crate) finalizers: NonZeroUsize,
    pub(crate) probes: usize,
    pub(crate) tails: usize,
}
impl JoinParticipants {
    pub(crate) fn handoff_producers(self) -> ExecutionResult<Option<usize>> {
        if self.tails == 0 {
            return Ok(None);
        }
        self.probes
            .checked_add(self.tails)
            .map(Some)
            .ok_or_else(arithmetic)
    }
}

pub(super) fn new_join_completion<A>(
    participants: JoinParticipants,
    handoff: Option<Arc<LocalExchanger>>,
    changed: Arc<Observable>,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<Arc<JoinCompletion<A>>> {
    StageCompletion::new(
        StageParticipants {
            inputs: participants.inputs,
            finalizers: participants.finalizers,
            probes: participants.probes,
            tails: participants.tails,
        },
        participants.handoff_producers()?,
        handoff,
        changed,
        owner,
        control,
        build_contract,
    )
}
