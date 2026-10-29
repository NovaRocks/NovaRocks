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

//! The single production consumer of original R readiness. Source returns,
//! source disposition and RPC retirement share the same bounded owner turn.

use super::*;
use crate::control_runtime::{
    control_request::{RequestIoError, RequestIoProgress},
    establish_executors::EstablishExecutorHandles,
    establish_ingress::{EstablishDisposalJob, EstablishIngressJob},
};

#[derive(Debug)]
pub(crate) enum RootRequestAdvanceError {
    WrongOwner,
    Control(ControlRequestError),
    Request {
        input: InputId,
        error: RequestIoError,
    },
}

impl ProcessControlOwner {
    pub(crate) fn stage_establish_getter(
        &mut self,
        input: InputId,
        original: EstablishIngressJob,
    ) -> Result<(), (ControlRequestError, EstablishIngressJob)> {
        let Some(custody) = self.resources.requests.get_mut(&input) else {
            return Err((ControlRequestError::WrongRequest, original));
        };
        custody.stage_getter(original)
    }

    pub(crate) fn stage_establish_disposal(
        &mut self,
        input: InputId,
        original: EstablishDisposalJob,
    ) -> Result<(), (ControlRequestError, EstablishDisposalJob)> {
        let Some(custody) = self.resources.requests.get_mut(&input) else {
            return Err((ControlRequestError::WrongRequest, original));
        };
        custody.stage_disposal(original)
    }

    pub(crate) fn advance_ready_requests(
        &mut self,
        handles: &EstablishExecutorHandles,
        budget: usize,
    ) -> Result<RequestIoProgress, RootRequestAdvanceError> {
        // Check the actual Root namespace before consuming any ready address.
        if !handles.belongs_to(&self.resources.request_ready) {
            return Err(RootRequestAdvanceError::WrongOwner);
        }
        let generation = self
            .resources
            .scope
            .gates()
            .map_err(|error| RootRequestAdvanceError::Control(ControlRequestError::Runtime(error)))?
            .generation;
        let failure_work = self.resources.roles[FAILURE].work.id();
        // Context transfers are driven outside this Root inventory lock.
        // Source returns retain their own independent finite allowance.
        let mut progress = RequestIoProgress::default();
        for _ in 0..budget {
            let Some(input) = self.resources.request_ready.next_ready() else {
                break;
            };
            let custody = self
                .resources
                .requests
                .get_mut(&input)
                .expect("ready address retains its original R");
            let advanced = custody
                .advance_source_io(handles, generation, failure_work)
                .map_err(|error| RootRequestAdvanceError::Request { input, error })?;
            progress.visited += advanced.visited;
            progress.dispatched += advanced.dispatched;
            progress.submitted += advanced.submitted;
            if custody
                .try_retire()
                .map_err(|error| RootRequestAdvanceError::Request {
                    input,
                    error: RequestIoError::Control(error),
                })?
            {
                if let Some(role) = custody.route_return_role() {
                    assert_eq!(self.resources.return_routes.remove(&role), Some(input));
                }
                self.resources.requests.remove(&input);
                progress.retired += 1;
            }
        }
        Ok(progress)
    }
}
