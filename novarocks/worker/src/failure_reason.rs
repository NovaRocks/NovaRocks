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

//! Stable Worker-owned reasons. Classification stays with each producer.
use novarocks_execution_contract::failure::{FailureReason, domain};
use std::num::NonZeroU16;
#[derive(Clone, Copy)]
#[repr(u16)]
pub(crate) enum WorkerFailureReason {
    ConnectorTrackerRebound = 1,
    ConnectorTrackerInstallRaced = 2,
    ConnectorBeforeAdmission = 3,
    ConnectorWrongTracker = 4,
    ConnectorWrongExecution = 5,
    ScanWrongRangeKind = 6,
    ScanOutputSlots = 7,
    ScanStreamReclaimed = 8,
    SinkCommitAlreadyRegistered = 9,
    RootOutputRowCount = 10,
    RootProducerExitedWithoutEnd = 11,
    RuntimeFilterInvalidContract = 12,
    RuntimeFilterParticipantMismatch = 13,
    ScanMissingExecutionSource = 14,
    ResultProcessReservation = 20,
    ResultStreamReservation = 21,
    ResultWriterFinished = 22,
    ResultWriterAborted = 23,
    RootCapacity = 30,
    RootBackingReservation = 38,
    RootMetadataTooSmall = 39,
    RootProducerContract = 41,
    RootPayloadContract = 42,
    RootReadIdentity = 43,
    RootForeignBuilder = 44,
    RootSendGeometry = 45,
    RootChannelClosed = 46,
    UnownedStopReport = 47,
    PreparedRootSink = 48,
    PreparedRootTask = 49,
    DomainTokenContentMismatch = 60,
    DomainTokenGap = 61,
    DomainAfterSeal = 62,
    DomainUnknownMember = 63,
    DomainNonMonotonic = 64,
    CredentialLeaseRepeated = 65,
    InboundRecordSize = 70,
    InboundReservationConflict = 71,
    InboundRecordCount = 72,
    InboundRecordBytes = 73,
    InboundContextClosed = 74,
    InboundKeyClosed = 75,
    InboundKeyClaimed = 76,
    InboundForeignClosedKey = 77,
    InboundCloseContract = 78,
    CatalogConfiguration = 80,
    CatalogPropertiesConflict = 81,
    CatalogNotReadyAfterInstall = 82,
    QueryContextNativeKey = 83,
    QueryFragmentNativeKey = 84,
    QueryContextMissing = 85,
    ResultPolicyZeroRoot = 86,
    ResultPolicyZeroProcess = 87,
    ResultPolicyRootExceedsProcess = 88,
    FilterTransportZeroRouteIdentity = 100,
    FilterTransportIdentityKindMismatch = 101,
    FilterTransportPayloadRequired = 102,
    FilterTransportPayloadForbidden = 103,
    FilterTransportProducerOpenRequired = 104,
    FilterTransportProducerOpenForbidden = 105,
    FilterTransportAcceptStatusRequired = 106,
    FilterTransportAcceptStatusForbidden = 107,
    FilterTransportZeroLocalPartitionCount = 108,
    FilterTransportEmptyRejectionReason = 109,
    FilterTransportZeroRpcDeadline = 110,
    FilterTransportIdentityConflict = 111,
    FilterTransportRetiredIdentity = 112,
    CompletionShuttingDown = 120,
    CompletionUnavailable = 121,
    CompletionDuplicate = 122,
    CompletionSlotCapacity = 123,
    CompletionStatusIdentity = 124,
    CatalogExecutionLeaseUnavailable = 140,
    CatalogExecutionReadUnavailable = 141,
    CatalogExecutionWriteUnavailable = 142,
    ReadRegistrationPoisoned = 143,
    ReadRegistrationDuplicate = 144,
    PreparationOwnerUnavailable = 145,
    PreparationWorkerStart = 146,
    PreparationPanicked = 147,
    TestUpdateConsumptionUnsupportedHost = 149,
    TestUpdateConsumptionLiveTaskRequired = 150,
    PreparationChildRegistration = 151,
    PreparationChildRollback = 152,
    PreparationInboundCleanup = 153,
    PreparationReceiverCleanup = 154,
    PreparationChildPoll = 155,
    PreparationChildAdopt = 156,
    PreparationChildDrop = 157,
    PreparationExitPublication = 158,
    PreparationChildUnresolved = 159,
    PreparationRuntimeStandDown = 160,
    PreparationPanicPayloadDrop = 161,
    SinkCommitAfterSettled = 162,
    RootMetadataNotification = 163,
    RootMetadataNotificationInvariant = 164,
    RootReadUnknown = 165,
    SettlementCall = 166,
}
impl WorkerFailureReason {
    pub(crate) const fn reason(self) -> FailureReason {
        FailureReason::new(domain::WORKER, NonZeroU16::new(self as u16).unwrap())
    }
}
