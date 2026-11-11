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

//! Original failure custody for one admitted covered subscription generation.

use super::{
    NativeSubscriptionFailure, NativeSubscriptionFailureCustody, ObservationError, StreamKey,
    SubscriptionFailureCustody,
};
use crate::{
    native::task_transport::wire::CoveredEventValidationError,
    task_execution::error::{ParticipantObservationFailure, TaskExecutionError},
};
use novarocks_attempt_protocol::{
    failure::{AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure},
    input::{ObservationFailure, ObservationFailureKind},
};
use novarocks_coordination_contract::{AttemptActionId, AttemptFailureClass};
use novarocks_proto_codec::ProtocolError;
use novarocks_query_application::execution_error::ExecutionFailureInput;
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use std::{
    alloc::Layout,
    fmt,
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
enum SubscriptionError {
    Native(NativeSubscriptionFailure),
    Decode(ProtocolError),
    Validate(CoveredEventValidationError),
    Ingress(ObservationError),
}

impl SubscriptionError {
    fn kind(&self) -> ObservationFailureKind {
        use ObservationFailureKind as Kind;
        match self {
            Self::Native(original) => original.kind(),
            Self::Validate(CoveredEventValidationError::ProcessMismatch) => Kind::ProcessMismatch,
            Self::Validate(CoveredEventValidationError::QueryMismatch) => Kind::QueryMismatch,
            Self::Decode(_)
            | Self::Validate(CoveredEventValidationError::Protocol(_))
            | Self::Ingress(_) => Kind::ProtocolViolation,
        }
    }

    fn observation_state(&self, kind: ObservationFailureKind) -> ParticipantObservationFailure {
        use ParticipantObservationFailure::{IdentityViolation, Transport};
        match kind {
            ObservationFailureKind::ProcessMismatch => IdentityViolation("process_mismatch"),
            ObservationFailureKind::QueryMismatch => IdentityViolation("query_mismatch"),
            ObservationFailureKind::ProtocolViolation
                if matches!(
                    self,
                    Self::Native(NativeSubscriptionFailure::WrongClient { .. })
                ) =>
            {
                IdentityViolation("process_mismatch")
            }
            ObservationFailureKind::ProtocolViolation | ObservationFailureKind::RpcRejected => {
                Transport("rejected")
            }
            // Core owns the reconnect streak and selects any eventual backend
            // loss. One transport error supplies only its observation evidence.
            ObservationFailureKind::RecoverableTransport { .. } => Transport("resubscribing"),
        }
    }
}

/// Carries the original error, including Native opening/reading and codec
/// context, from its generation to the terminal or disposal custodian.
#[derive(Debug)]
pub(crate) struct OriginalSubscriptionFailure {
    key: StreamKey,
    original: SubscriptionError,
    state: ParticipantObservationFailure,
}

impl OriginalSubscriptionFailure {
    pub(crate) fn preparation_input(&self) -> ExecutionFailureInput {
        TaskExecutionError::ParticipantUnobservable {
            backend: self.key.participation.backend_process_id(),
            state: self.state,
        }
        .preparation_input()
    }
}

impl fmt::Display for OriginalSubscriptionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Native covered subscription generation {} failed",
            self.key.generation
        )
    }
}

impl std::error::Error for OriginalSubscriptionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.original {
            SubscriptionError::Native(
                NativeSubscriptionFailure::Open(status) | NativeSubscriptionFailure::Read(status),
            ) => Some(status),
            SubscriptionError::Native(NativeSubscriptionFailure::Encode(error))
            | SubscriptionError::Decode(error) => Some(error),
            SubscriptionError::Native(NativeSubscriptionFailure::Channel(error)) => Some(error),
            SubscriptionError::Ingress(error) => Some(error),
            SubscriptionError::Native(NativeSubscriptionFailure::WrongClient { .. })
            | SubscriptionError::Validate(_) => None,
        }
    }
}

enum Slot {
    Unreported,
    Retained(OriginalSubscriptionFailure),
    Transferred,
}

/// The original Open effect prepays one fixed slot. The job writes its one
/// terminal error before publishing the compact observation, and the Source
/// keeps the other reference until selection or retirement.
pub(crate) struct SubscriptionFailureRecord {
    key: StreamKey,
    reference: HostFailureRef,
    slot: Mutex<Slot>,
}

impl SubscriptionFailureRecord {
    pub(crate) fn new(open: AttemptActionId, key: StreamKey) -> Arc<Self> {
        Arc::new(Self {
            key,
            reference: HostFailureRef(NonZeroU64::new(open.get()).expect("nonzero Open effect")),
            slot: Mutex::new(Slot::Unreported),
        })
    }

    pub(crate) fn layout() -> Result<Layout, LayoutResourceError> {
        arc_layout(Layout::new::<Self>())
    }

    pub(crate) const fn reference(&self) -> HostFailureRef {
        self.reference
    }

    pub(crate) fn project(&self, reference: HostFailureRef) -> Option<ExecutionFailureInput> {
        if reference != self.reference {
            return None;
        }
        match &*self.slot.lock().unwrap() {
            Slot::Retained(original) => Some(original.preparation_input()),
            _ => None,
        }
    }

    pub(crate) fn is_terminal_candidate(&self) -> bool {
        matches!(&*self.slot.lock().unwrap(), Slot::Retained(original)
            if !matches!(original.original.kind(), ObservationFailureKind::RecoverableTransport { .. }))
    }

    pub(crate) fn take(&self, reference: HostFailureRef) -> Option<OriginalSubscriptionFailure> {
        if reference != self.reference {
            return None;
        }
        let mut slot = self.slot.lock().unwrap();
        if !matches!(*slot, Slot::Retained(_)) {
            return None;
        }
        let Slot::Retained(original) = std::mem::replace(&mut *slot, Slot::Transferred) else {
            unreachable!("the original slot is retained");
        };
        Some(original)
    }

    fn retain(&self, key: StreamKey, original: SubscriptionError) -> ObservationFailure {
        assert_eq!(key, self.key, "the Open effect owns exactly this stream");
        let kind = original.kind();
        let state = original.observation_state(kind);
        // Match the existing ParticipantUnobservable projection without
        // allocating its safe detail on the I/O job.
        let class = match state {
            ParticipantObservationFailure::IdentityViolation(_) => {
                AttemptFailureClass::ContractViolation
            }
            ParticipantObservationFailure::Transport(_) => AttemptFailureClass::ExecutionFailure,
            ParticipantObservationFailure::ConnectionLost => unreachable!("Core owns exhaustion"),
        };
        let mut slot = self.slot.lock().unwrap();
        assert!(
            matches!(*slot, Slot::Unreported),
            "one terminal error per Open effect"
        );
        *slot = Slot::Retained(OriginalSubscriptionFailure {
            key,
            original,
            state,
        });
        ObservationFailure {
            kind,
            cause: HostProjectedFailure {
                original: self.reference,
                kind: HostFailureKind::NativeTransport,
                class,
                site: AttemptFailureSite::Participation(key.participation),
            },
        }
    }
}

impl SubscriptionFailureCustody for SubscriptionFailureRecord {
    fn malformed(&self, key: StreamKey, original: ProtocolError) -> ObservationFailure {
        self.retain(key, SubscriptionError::Decode(original))
    }

    fn ingress_rejected(&self, key: StreamKey, original: ObservationError) -> ObservationFailure {
        self.retain(key, SubscriptionError::Ingress(original))
    }

    fn conflicting(
        &self,
        key: StreamKey,
        original: CoveredEventValidationError,
    ) -> ObservationFailure {
        self.retain(key, SubscriptionError::Validate(original))
    }
}

impl NativeSubscriptionFailureCustody for SubscriptionFailureRecord {
    fn retain_native_failure(
        &self,
        key: StreamKey,
        original: NativeSubscriptionFailure,
    ) -> HostProjectedFailure {
        self.retain(key, SubscriptionError::Native(original)).cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::{ParticipationRef, failure::FailureProjection};
    use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
    use novarocks_query_application::execution_error::{ExecutionFailureSite, QueryFailureReason};
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    fn id(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn key() -> StreamKey {
        StreamKey {
            participation: ParticipationRef::new(
                QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap(),
                FrontendProcessId::new_v7(),
                BackendProcessId::new_v7(),
            ),
            generation: id(1),
        }
    }

    #[test]
    fn original_native_failure_moves_once_only_for_its_open_effect() {
        let key = key();
        let first = SubscriptionFailureRecord::new(AttemptActionId::new(id(10)), key);
        let successor = SubscriptionFailureRecord::new(
            AttemptActionId::new(id(12)),
            StreamKey {
                generation: id(2),
                ..key
            },
        );
        let details = bytes::Bytes::from_static(b"original-status-details");
        let cause = first.retain_native_failure(
            key,
            NativeSubscriptionFailure::Read(tonic::Status::with_details(
                tonic::Code::FailedPrecondition,
                "raw remote message",
                details.clone(),
            )),
        );
        assert_eq!(cause.original, first.reference());
        assert_eq!(
            cause.site,
            AttemptFailureSite::Participation(key.participation)
        );
        assert!(successor.take(cause.original).is_none());
        assert!(first.take(successor.reference()).is_none());

        let original = first.take(cause.original).unwrap();
        let SubscriptionError::Native(NativeSubscriptionFailure::Read(status)) = &original.original
        else {
            panic!("the original reading status must be retained");
        };
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "raw remote message");
        assert_eq!(status.details(), details.as_ref());
        let input = original.preparation_input();
        assert_eq!(
            input.fact(),
            QueryFailureReason::ObservationLost.failure_fact()
        );
        assert_eq!(
            input.site(),
            ExecutionFailureSite::Backend {
                process: key.participation.backend_process_id()
            }
        );
        assert_eq!(cause.class, input.attempt_class());
        assert!(!input.detail().as_str().contains("raw remote message"));
        assert!(first.take(cause.original).is_none());
    }

    #[test]
    fn original_validation_codec_and_ingress_failures_keep_their_typed_causes() {
        let key = key();
        let validation = SubscriptionFailureRecord::new(AttemptActionId::new(id(17)), key);
        let fact = validation.conflicting(key, CoveredEventValidationError::QueryMismatch);
        assert_eq!(fact.kind, ObservationFailureKind::QueryMismatch);
        let original = validation.take(fact.cause.original).unwrap();
        assert!(matches!(
            original.original,
            SubscriptionError::Validate(CoveredEventValidationError::QueryMismatch)
        ));
        let input = original.preparation_input();
        assert_eq!(
            input.fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(fact.cause.class, input.attempt_class());

        let decode = SubscriptionFailureRecord::new(AttemptActionId::new(id(20)), key);
        let error = ProtocolError::new(
            FieldPath::root("frame").field("identity"),
            ProtocolErrorKind::MissingField,
            "missing original field",
        );
        let fact = decode.malformed(key, error.clone());
        assert_eq!(fact.kind, ObservationFailureKind::ProtocolViolation);
        let original = decode.take(fact.cause.original).unwrap();
        assert!(matches!(original.original, SubscriptionError::Decode(raw) if raw == error));

        let ingress = SubscriptionFailureRecord::new(AttemptActionId::new(id(22)), key);
        let error = ObservationError::Oversized {
            charged: 3,
            reserved: 2,
        };
        let fact = ingress.ingress_rejected(key, error);
        assert_eq!(fact.kind, ObservationFailureKind::ProtocolViolation);
        let original = ingress.take(fact.cause.original).unwrap();
        assert!(matches!(original.original, SubscriptionError::Ingress(raw) if raw == error));
    }
}
