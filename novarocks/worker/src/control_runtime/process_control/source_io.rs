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
    establish_ingress::EstablishIngressJob,
};

#[cfg(test)]
use crate::control_runtime::establish_ingress::EstablishDisposalJob;

#[derive(Debug)]
pub(crate) enum ControlRequestAdvanceError {
    WrongOwner,
    ControlRequestFailed(ControlRequestError),
    Rejected(super::rejected_ingress::RejectedIngressAdvanceError),
    Request {
        input: InputId,
        error: RequestIoError,
    },
}
impl std::fmt::Display for ControlRequestAdvanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => {
                f.write_str("Root request executor does not belong to the original owner")
            }
            Self::ControlRequestFailed(error) => {
                write!(f, "Root request control operation failed: {error}")
            }
            Self::Rejected(error) => write!(f, "Root rejected ingress cleanup failed: {error}"),
            Self::Request { input, error } => {
                write!(f, "Root request input {input:?} failed: {error:?}")
            }
        }
    }
}
impl std::error::Error for ControlRequestAdvanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ControlRequestFailed(error) => Some(error),
            Self::Rejected(error) => Some(error),
            Self::WrongOwner | Self::Request { .. } => None,
        }
    }
}

impl ProcessControlOwner {
    pub(super) fn advance_rejected_ingress(
        &mut self,
        handles: &EstablishExecutorHandles,
        budget: usize,
    ) -> Result<super::rejected_ingress::RejectedIngressProgress, ControlRequestAdvanceError> {
        match self.resources.rejected_ingress.as_mut() {
            Some(original) => original
                .advance(handles, budget)
                .map_err(ControlRequestAdvanceError::Rejected),
            None => Ok(Default::default()),
        }
    }
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

    #[cfg(test)]
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
        ports: Option<&crate::TaskExecutionPorts>,
        budget: usize,
    ) -> Result<RequestIoProgress, ControlRequestAdvanceError> {
        // Check the actual Root namespace before consuming any ready address.
        if !handles.belongs_to(&self.resources.request_ready) {
            return Err(ControlRequestAdvanceError::WrongOwner);
        }
        self.resources
            .request_ready
            .retry_capacity(
                self.registration(),
                self.resources.request_ready.source_capacity_revision(),
                budget,
            )
            .map_err(ControlRequestAdvanceError::ControlRequestFailed)?;
        let generation = self
            .resources
            .scope
            .gates()
            .map_err(|error| {
                ControlRequestAdvanceError::ControlRequestFailed(ControlRequestError::Runtime(
                    error,
                ))
            })?
            .generation;
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
            progress.dispatched += custody
                .advance_protocol_observation(handles, ports)
                .map_err(|error| ControlRequestAdvanceError::Request { input, error })?;
            let advanced = custody
                .advance_source_io(handles, generation)
                .map_err(|error| ControlRequestAdvanceError::Request { input, error })?;
            progress.visited += advanced.visited;
            progress.dispatched += advanced.dispatched;
            progress.submitted += advanced.submitted;
            if custody
                .try_retire()
                .map_err(|error| ControlRequestAdvanceError::Request {
                    input,
                    error: RequestIoError::Control(error),
                })?
            {
                if let Some(return_input_id) = custody.route_return_input_id() {
                    assert_eq!(
                        self.resources.return_routes.remove(&return_input_id),
                        Some(input)
                    );
                }
                self.remove_retired_request(input);
                progress.retired += 1;
            }
        }
        Ok(progress)
    }
}
