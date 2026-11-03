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

//! Participation convergence wire codec.

use novarocks_execution_contract::task_execution::participation_convergence::{
    ParticipationConvergenceCursor, ParticipationConvergenceReceipt, ParticipationConvergenceState,
    ParticipationConvergenceVersion,
};
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::novarocks;

use crate::identity::{decode_participation_ref, encode_participation_ref};
use crate::{invalid, invalid_enum, missing};

pub fn decode_participation_convergence_state(
    value: i32,
    path: FieldPath,
) -> Result<ParticipationConvergenceState, ProtocolError> {
    match novarocks::ParticipationConvergenceState::try_from(value) {
        Ok(novarocks::ParticipationConvergenceState::WorkerStoppedAndParticipationFenced) => {
            Ok(ParticipationConvergenceState::WorkerStoppedAndParticipationFenced)
        }
        Ok(novarocks::ParticipationConvergenceState::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "participation convergence state must be a known non-default value",
        )),
    }
}

pub fn encode_participation_convergence_state(value: ParticipationConvergenceState) -> i32 {
    match value {
        ParticipationConvergenceState::WorkerStoppedAndParticipationFenced => {
            novarocks::ParticipationConvergenceState::WorkerStoppedAndParticipationFenced as i32
        }
    }
}

pub fn decode_participation_convergence_receipt(
    src: &novarocks::ParticipationConvergenceReceipt,
    path: FieldPath,
) -> Result<ParticipationConvergenceReceipt, ProtocolError> {
    let participation = src.participation.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("participation"),
            "participation convergence receipt requires an exact participation",
        )
    })?;
    let participation =
        decode_participation_ref(participation, path.clone().field("participation"))?;
    let version = ParticipationConvergenceVersion::new(src.version)
        .map_err(|error| invalid(path.clone().field("version"), error.to_string()))?;
    let state = decode_participation_convergence_state(src.state, path.field("state"))?;
    Ok(ParticipationConvergenceReceipt::new(
        participation,
        version,
        state,
    ))
}

pub fn encode_participation_convergence_receipt(
    value: ParticipationConvergenceReceipt,
) -> novarocks::ParticipationConvergenceReceipt {
    novarocks::ParticipationConvergenceReceipt {
        participation: Some(encode_participation_ref(value.participation())),
        version: value.version().get(),
        state: encode_participation_convergence_state(value.state()),
    }
}

pub fn decode_participation_convergence_cursor(
    src: &novarocks::ParticipationConvergenceCursor,
    path: FieldPath,
) -> Result<ParticipationConvergenceCursor, ProtocolError> {
    let participation = src.participation.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("participation"),
            "participation convergence cursor requires an exact participation",
        )
    })?;
    let participation = decode_participation_ref(participation, path.field("participation"))?;
    Ok(
        match ParticipationConvergenceVersion::new(src.current_version) {
            Ok(version) => ParticipationConvergenceCursor::at(participation, version),
            Err(_) => ParticipationConvergenceCursor::unobserved(participation),
        },
    )
}

pub fn encode_participation_convergence_cursor(
    value: ParticipationConvergenceCursor,
) -> novarocks::ParticipationConvergenceCursor {
    novarocks::ParticipationConvergenceCursor {
        participation: Some(encode_participation_ref(value.participation())),
        current_version: value
            .current_version()
            .map_or(0, ParticipationConvergenceVersion::get),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{
        ParticipationAwareStatusStreamEvent, decode_participation_aware_status_event,
        decode_participation_aware_subscribe_task_status, decode_status_event,
        encode_participation_aware_subscribe_task_status, encode_participation_convergence_event,
    };
    use novarocks_execution_contract::task_execution::identity::ParticipationRef;
    use novarocks_proto_codec::ProtocolErrorKind;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    fn participation() -> ParticipationRef {
        ParticipationRef::new(
            QueryExecutionId::new(
                QueryId::new(41, 42),
                AttemptId::new(3).expect("nonzero attempt"),
            )
            .expect("nonzero query"),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    fn receipt() -> ParticipationConvergenceReceipt {
        ParticipationConvergenceReceipt::new(
            participation(),
            ParticipationConvergenceVersion::FIRST,
            ParticipationConvergenceState::WorkerStoppedAndParticipationFenced,
        )
    }

    #[test]
    fn receipt_round_trips_exact_identity_version_and_state() {
        let receipt = receipt();
        let encoded = encode_participation_convergence_receipt(receipt);
        let decoded = decode_participation_convergence_receipt(
            &encoded,
            FieldPath::root("participation_convergence"),
        )
        .expect("valid receipt");
        assert_eq!(decoded, receipt);
    }

    #[test]
    fn receipt_rejects_missing_identity_zero_version_and_unknown_state() {
        let encoded = encode_participation_convergence_receipt(receipt());

        let mut missing_identity = encoded.clone();
        missing_identity.participation = None;
        let error = decode_participation_convergence_receipt(
            &missing_identity,
            FieldPath::root("participation_convergence"),
        )
        .expect_err("identity is mandatory");
        assert_eq!(error.kind(), &ProtocolErrorKind::MissingField);

        let mut zero_version = encoded.clone();
        zero_version.version = 0;
        let error = decode_participation_convergence_receipt(
            &zero_version,
            FieldPath::root("participation_convergence"),
        )
        .expect_err("receipt version must be nonzero");
        assert_eq!(error.kind(), &ProtocolErrorKind::InvalidValue);

        let mut unknown_state = encoded;
        unknown_state.state = i32::MAX;
        let error = decode_participation_convergence_receipt(
            &unknown_state,
            FieldPath::root("participation_convergence"),
        )
        .expect_err("state set is closed");
        assert_eq!(error.kind(), &ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn cursor_zero_means_unobserved_and_still_requires_exact_identity() {
        let cursor = ParticipationConvergenceCursor::unobserved(participation());
        let encoded = encode_participation_convergence_cursor(cursor);
        assert_eq!(encoded.current_version, 0);
        assert_eq!(
            decode_participation_convergence_cursor(
                &encoded,
                FieldPath::root("participation_convergence_cursor"),
            )
            .expect("valid cursor"),
            cursor
        );

        let mut missing_identity = encoded;
        missing_identity.participation = None;
        let error = decode_participation_convergence_cursor(
            &missing_identity,
            FieldPath::root("participation_convergence_cursor"),
        )
        .expect_err("cursor identity is mandatory");
        assert_eq!(error.kind(), &ProtocolErrorKind::MissingField);
    }

    #[test]
    fn subscription_preserves_outer_and_cursor_participations_for_exact_validation() {
        let outer_participation = participation();
        let cursor_participation = participation();
        assert_ne!(outer_participation, cursor_participation);
        let cursor = ParticipationConvergenceCursor::unobserved(cursor_participation);
        let encoded = encode_participation_aware_subscribe_task_status(
            outer_participation,
            &[],
            Some(cursor),
        )
        .expect("bounded subscription");

        let (decoded_outer, task_cursors, decoded_cursor) =
            decode_participation_aware_subscribe_task_status(
                &encoded,
                FieldPath::root("subscribe_task_status"),
            )
            .expect("valid subscription");
        assert_eq!(decoded_outer, outer_participation);
        assert!(task_cursors.is_empty());
        assert_eq!(decoded_cursor, Some(cursor));

        let error = crate::operation::decode_subscribe_task_status(
            &encoded,
            FieldPath::root("subscribe_task_status"),
        )
        .expect_err("legacy decoder must not ignore a convergence cursor");
        assert_eq!(error.kind(), &ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn convergence_event_uses_only_the_participation_aware_decoder() {
        let receipt = receipt();
        let encoded = encode_participation_convergence_event(receipt);
        match decode_participation_aware_status_event(
            &encoded,
            FieldPath::root("task_status_event"),
        )
        .expect("valid convergence event")
        {
            ParticipationAwareStatusStreamEvent::ParticipationConvergence(decoded) => {
                assert_eq!(decoded, receipt)
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let error = decode_status_event(&encoded, FieldPath::root("task_status_event"))
            .expect_err("legacy decoder must not ignore a convergence event");
        assert_eq!(error.kind(), &ProtocolErrorKind::InvalidValue);
    }
}
