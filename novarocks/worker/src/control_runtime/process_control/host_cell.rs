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

//! Original Root inventory cell for the shared host poller. Each Context
//! transfer is prepared under Root, then bound with that Root guard released.

use super::{ProcessControlOwner, RootRequestAdvanceError};
use crate::control_runtime::{
    actors::WorkerActor,
    control_request::{ControlRequestError, RequestIoProgress},
    establish_executors::EstablishExecutorHandles,
    establish_route::EstablishRouteError,
};
use novarocks_control_runtime::driver::{ActorHostTurn, ActorRegistration};
use std::sync::{Mutex, MutexGuard};

pub(crate) struct RootHostCell {
    owner: Mutex<ProcessControlOwner>,
    registration: ActorRegistration,
}
#[derive(Debug, Default)]
pub(crate) struct RootHostProgress {
    pub routed: usize,
    pub stale_targets: usize,
    pub requests: RequestIoProgress,
    /// Only this turn's route/source classes, not all Node effects or Tasks.
    pub runnable: bool,
}
impl RootHostCell {
    pub(crate) fn new(owner: ProcessControlOwner) -> Self {
        let registration = owner.registration().clone();
        Self {
            owner: Mutex::new(owner),
            registration,
        }
    }
    pub(crate) fn lock(&self) -> MutexGuard<'_, ProcessControlOwner> {
        self.owner.lock().expect("original Root host inventory")
    }
    #[cfg(test)]
    pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, ProcessControlOwner>> {
        self.owner.try_lock().ok()
    }
    pub(crate) fn into_owner(self) -> ProcessControlOwner {
        self.owner
            .into_inner()
            .expect("original Root host inventory")
    }
    /// The exclusive Common host turn remains claimed across all phases, but
    /// the Root mutex does not. No foreign getter, callback or Drop runs here.
    pub(crate) fn advance_ready(
        &self,
        turn: &ActorHostTurn<WorkerActor>,
        handles: &EstablishExecutorHandles,
        budget: usize,
    ) -> Result<RootHostProgress, RootRequestAdvanceError> {
        if !turn.matches_registration(&self.registration)
            || !handles.belongs_to(self.lock().request_ready())
        {
            return Err(RootRequestAdvanceError::WrongOwner);
        }
        let mut progress = RootHostProgress::default();
        for _ in 0..budget {
            let original = match self.lock().take_ready_route_binding() {
                Ok(original) => original,
                Err(ControlRequestError::Route(EstablishRouteError::TargetRemoved)) => {
                    // The same actor incarnation cannot recover. Preserve the
                    // original pending R/escrow for its Core deadline/cancel;
                    // do not busy-requeue or turn a late target into fail-stop.
                    progress.stale_targets += 1;
                    continue;
                }
                Err(error) => return Err(RootRequestAdvanceError::Control(error)),
            };
            let Some(original) = original else {
                break;
            };
            // The temporary Root guard has ended before a target lock is taken.
            match original.bind() {
                Ok(()) => progress.routed += 1,
                Err(ControlRequestError::Route(EstablishRouteError::TargetRemoved)) => {
                    progress.stale_targets += 1;
                }
                Err(error) => return Err(RootRequestAdvanceError::Control(error)),
            }
        }
        let mut owner = self.lock();
        progress.requests = owner.advance_ready_requests(handles, budget)?;
        progress.runnable =
            !owner.resources.ready_routes.is_empty() || owner.request_ready().has_control_ready();
        Ok(progress)
    }
}
