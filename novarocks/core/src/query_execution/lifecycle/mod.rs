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

pub mod contract;
pub mod terminal;

pub use contract::{QueryLifecycleError, QueryLifecycleErrorCode, QueryTerminalIngress};
pub use novarocks_protocol::lifecycle::{
    AttemptId, ExchangeRouteManifest, FragmentLiveObservation, ParticipantBackendIdentity,
    ParticipantManifest, ParticipantManifestDigest, ParticipantRole, QueryAbortRequest,
    QueryControlAttach, QueryControlCommand, QueryControlEndpoint, QueryControlEvent,
    QueryExecutionId, QueryInitAck, QueryInitOutcome, QueryInitRequest, QueryOptions,
    QueryTerminationAck, QueryTerminationReason, RuntimeFilterContribution,
};
pub use novarocks_protocol::lifecycle::{
    QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartOutcome,
    QueryStartRequest, StageDigest, StageDigestVersion, StageFragment,
};
pub use novarocks_protocol::lifecycle::{
    QueryTerminalAck, QueryTerminalReportAck, QueryTerminalReportOutcome,
};
pub use terminal::{
    FragmentTerminalOutcome, FragmentTerminalSnapshot, ImmutableQueryTerminalRecord,
    NegativeAttestation, NegativeAttestationReason, PARTICIPANT_TERMINAL_OUTCOME_VERSION_V1,
    ParticipantTerminalOutcome, QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
    QUERY_TERMINAL_SNAPSHOT_VERSION_V1, QueryTerminalProfileContributionV1,
    QueryTerminalRuntimeFilterChannelInstallStateV1, QueryTerminalRuntimeFilterChannelKeyV1,
    QueryTerminalRuntimeFilterChannelTerminalStateV1, QueryTerminalRuntimeFilterChannelV1,
    QueryTerminalRuntimeFilterConsumerKeyV1, QueryTerminalRuntimeFilterConsumerV1,
    QueryTerminalRuntimeFilterProducerStreamKeyV1, QueryTerminalRuntimeFilterProducerStreamV1,
    QueryTerminalRuntimeFilterScanNotEvaluatedV1, QueryTerminalRuntimeFilterSubscriptionTerminalV1,
    QueryTerminalRuntimeFilterTransportRouteKeyV1, QueryTerminalRuntimeFilterTransportRouteV1,
    QueryTerminalSnapshot, QueryTerminalSnapshotDigest, TerminalTelemetry,
    TerminalTelemetryUnavailable, TerminalizationProof, TerminalizationProofFragment,
    p0_max_encoded_len,
};
