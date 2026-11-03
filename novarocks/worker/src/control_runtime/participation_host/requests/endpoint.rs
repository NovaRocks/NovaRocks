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

//! Borrows the original direct or Task-causal endpoint without changing its
//! publication, external delivery or request identity ownership.

use crate::control_runtime::{
    actors::RequestMetadata,
    request_endpoint::RequestEndpoint,
    request_ready::{ControlRequestReady, RequestExternalLeg},
};
use novarocks_control_runtime::publication::PublishedReply;
use novarocks_worker_protocol::participation::ParticipationReplyBody;
use std::sync::Arc;

pub(super) enum ParticipationRequestEndpoint {
    Direct(Arc<RequestEndpoint<ParticipationReplyBody>>),
    Task(Arc<RequestEndpoint<ParticipationReplyBody, RequestMetadata>>),
}
impl ParticipationRequestEndpoint {
    pub(super) fn reply(&self) -> &PublishedReply<ParticipationReplyBody> {
        match self {
            Self::Direct(endpoint) => &endpoint.reply,
            Self::Task(endpoint) => &endpoint.reply,
        }
    }
    pub(super) fn external(&self) -> &RequestExternalLeg {
        match self {
            Self::Direct(endpoint) => &endpoint.external,
            Self::Task(endpoint) => &endpoint.external,
        }
    }
    pub(super) fn ready(&self) -> &ControlRequestReady {
        match self {
            Self::Direct(endpoint) => &endpoint.ready,
            Self::Task(endpoint) => &endpoint.ready,
        }
    }
    pub(super) fn external_ended(&self) -> bool {
        self.external().is_ended()
    }
    pub(super) fn metadata(&self) -> RequestMetadata {
        match self {
            Self::Direct(endpoint) => RequestMetadata {
                operation: endpoint.request.operation,
                deadline: endpoint.request.deadline,
            },
            Self::Task(endpoint) => endpoint.request,
        }
    }
}
