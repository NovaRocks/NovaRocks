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

//! Native wire adapter for one Backend process heartbeat.
//!
//! Worker owns drain and admission-epoch facts. This adapter validates the
//! native request and projects those facts into the generated heartbeat DTO;
//! it does not decide membership or task admission.

use std::sync::Arc;

use novarocks_execution_contract::BackendProcessDescriptor;
use novarocks_proto_codec::membership::BackendProcessId;
use novarocks_proto_models::novarocks as proto;
use novarocks_types::BackendProcessId as DomainBackendProcessId;
use novarocks_worker::{WorkerAdmissionEpochAuthority, WorkerDrainState};

/// The Native response adapter for one immutable Backend process identity.
#[derive(Clone)]
pub struct BackendHeartbeatResponder {
    process_id: DomainBackendProcessId,
    descriptor: BackendProcessDescriptor,
    drain: Arc<WorkerDrainState>,
    admission_epoch: Arc<dyn WorkerAdmissionEpochAuthority>,
}

impl BackendHeartbeatResponder {
    pub fn new(
        descriptor: BackendProcessDescriptor,
        drain: Arc<WorkerDrainState>,
        admission_epoch: Arc<dyn WorkerAdmissionEpochAuthority>,
    ) -> Self {
        let process_id = descriptor.process_id();
        Self {
            process_id,
            descriptor,
            drain,
            admission_epoch,
        }
    }

    pub fn respond(
        &self,
        request: proto::HeartbeatRequest,
    ) -> Result<proto::HeartbeatResponse, tonic::Status> {
        let expected_process_id = request.expected_process_id.ok_or_else(|| {
            tonic::Status::invalid_argument("heartbeat expected process id is required")
        })?;
        let expected_process_id = BackendProcessId::parse(expected_process_id)
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?
            .domain()
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        if expected_process_id != self.process_id {
            return Err(tonic::Status::failed_precondition(
                "heartbeat expected backend process id does not match this backend",
            ));
        }
        let num_cores = std::thread::available_parallelism()
            .map(|count| count.get() as u32)
            .unwrap_or(1);
        Ok(proto::HeartbeatResponse {
            num_cores,
            descriptor: Some(
                novarocks_proto_codec::membership::BackendProcessDescriptor::from_contract(
                    self.descriptor.clone(),
                )
                .as_proto()
                .clone(),
            ),
            reported_state: if self.drain.is_draining() {
                proto::BackendReportedState::Draining as i32
            } else {
                proto::BackendReportedState::Running as i32
            },
            admission_epoch_capability: Some(proto::AdmissionEpochCapability {
                value: self
                    .admission_epoch
                    .admission_epoch_capability()
                    .to_bytes()
                    .to_vec(),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::BackendHeartbeatResponder;
    use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};
    use novarocks_proto_models::novarocks as proto;
    use novarocks_types::{BackendProcessId, NativeCompatibilityId};
    use novarocks_worker::{
        AdmissionEpochCapability, WorkerAdmissionEpochAuthority, WorkerDrainState,
    };

    struct Epoch(AdmissionEpochCapability);

    impl WorkerAdmissionEpochAuthority for Epoch {
        fn admission_epoch_capability(&self) -> AdmissionEpochCapability {
            self.0
        }
    }

    fn responder(drain: Arc<WorkerDrainState>) -> (BackendHeartbeatResponder, BackendProcessId) {
        let process_id = BackendProcessId::new_v7();
        let descriptor = BackendProcessDescriptor::try_new(
            process_id,
            RuntimeEndpoint::new("be-0.internal", 9090).expect("endpoint"),
            "warehouse-a",
            "build-identity",
            NativeCompatibilityId::new([7; 32]),
        )
        .expect("descriptor");
        let epoch = AdmissionEpochCapability::try_from_bytes([9; 16]).expect("epoch");
        (
            BackendHeartbeatResponder::new(descriptor, drain, Arc::new(Epoch(epoch))),
            process_id,
        )
    }

    fn request(process_id: BackendProcessId) -> proto::HeartbeatRequest {
        proto::HeartbeatRequest {
            expected_process_id: Some(proto::BackendProcessId {
                value: process_id.to_bytes().to_vec(),
            }),
        }
    }

    #[test]
    fn projects_worker_facts_into_a_running_heartbeat() {
        let (responder, process_id) = responder(Arc::new(WorkerDrainState::new()));

        let response = responder.respond(request(process_id)).expect("heartbeat");

        assert!(response.num_cores >= 1);
        assert!(response.descriptor.is_some());
        assert_eq!(
            response.reported_state,
            proto::BackendReportedState::Running as i32
        );
        assert_eq!(
            response
                .admission_epoch_capability
                .expect("admission epoch")
                .value,
            vec![9; 16]
        );
    }

    #[test]
    fn projects_the_same_worker_drain_fact_into_the_heartbeat() {
        let drain = Arc::new(WorkerDrainState::new());
        let (responder, process_id) = responder(Arc::clone(&drain));
        drain.begin_drain();

        let response = responder.respond(request(process_id)).expect("heartbeat");

        assert_eq!(
            response.reported_state,
            proto::BackendReportedState::Draining as i32
        );
    }

    #[test]
    fn rejects_missing_invalid_and_foreign_process_id() {
        let (responder, _process_id) = responder(Arc::new(WorkerDrainState::new()));

        assert_eq!(
            responder
                .respond(proto::HeartbeatRequest {
                    expected_process_id: None,
                })
                .expect_err("missing process id must fail")
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            responder
                .respond(proto::HeartbeatRequest {
                    expected_process_id: Some(proto::BackendProcessId { value: vec![1; 15] }),
                })
                .expect_err("invalid process id must fail")
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            responder
                .respond(request(BackendProcessId::new_v7()))
                .expect_err("foreign process id must fail")
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
