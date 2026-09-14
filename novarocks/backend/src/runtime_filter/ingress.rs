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

//! Backend bridge from query-context ownership to Native ingress composition.

use std::sync::Arc;

use novarocks_native_adapter::runtime_filter_ingress::{
    BackendRuntimeFilterParticipantAuthority, native_runtime_filter_envelope_ingress,
};
use novarocks_native_adapter::runtime_filter_participant::RuntimeFilterParticipant;
use novarocks_native_adapter::runtime_filter_rpc::BackendRuntimeFilterEnvelopeIngress;
use novarocks_worker::runtime_filter::domain::BackendParticipantIdentity;

use crate::task_execution::NativeQueryContextHost;

/// The task protocol context host is the only Backend participant owner.
struct QueryContextParticipantAuthority(Arc<NativeQueryContextHost>);

impl BackendRuntimeFilterParticipantAuthority for QueryContextParticipantAuthority {
    fn authority_name(&self) -> &'static str {
        "the task query-context host"
    }

    fn claim_participant(
        &self,
        participant: BackendParticipantIdentity,
    ) -> Option<Arc<RuntimeFilterParticipant>> {
        self.0.claim_runtime_filter_participant(participant)
    }
}

pub(crate) fn native_runtime_filter_envelope_ingress_for_context_host(
    query_contexts: Arc<NativeQueryContextHost>,
) -> Arc<dyn BackendRuntimeFilterEnvelopeIngress> {
    native_runtime_filter_envelope_ingress(Arc::new(QueryContextParticipantAuthority(
        query_contexts,
    )))
}
