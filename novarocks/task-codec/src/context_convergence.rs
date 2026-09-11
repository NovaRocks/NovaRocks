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

//! Query-context convergence wire codec.

use novarocks_execution_contract::task_execution::context_convergence::{
    QueryContextConvergenceCursor, QueryContextConvergenceReceipt, QueryContextConvergenceState,
    QueryContextConvergenceVersion,
};
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::novarocks;

use crate::identity::{decode_query_context_ref, encode_query_context_ref};
use crate::{invalid, invalid_enum, missing};

pub fn decode_query_context_convergence_state(
    value: i32,
    path: FieldPath,
) -> Result<QueryContextConvergenceState, ProtocolError> {
    match novarocks::QueryContextConvergenceState::try_from(value) {
        Ok(novarocks::QueryContextConvergenceState::WorkerStoppedAndContextFenced) => {
            Ok(QueryContextConvergenceState::WorkerStoppedAndContextFenced)
        }
        Ok(novarocks::QueryContextConvergenceState::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "query context convergence state must be a known non-default value",
        )),
    }
}

pub fn encode_query_context_convergence_state(value: QueryContextConvergenceState) -> i32 {
    match value {
        QueryContextConvergenceState::WorkerStoppedAndContextFenced => {
            novarocks::QueryContextConvergenceState::WorkerStoppedAndContextFenced as i32
        }
    }
}

pub fn decode_query_context_convergence_receipt(
    src: &novarocks::QueryContextConvergenceReceipt,
    path: FieldPath,
) -> Result<QueryContextConvergenceReceipt, ProtocolError> {
    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "query context convergence receipt requires an exact context",
        )
    })?;
    let context = decode_query_context_ref(context, path.clone().field("query_context"))?;
    let version = QueryContextConvergenceVersion::new(src.version)
        .map_err(|error| invalid(path.clone().field("version"), error.to_string()))?;
    let state = decode_query_context_convergence_state(src.state, path.field("state"))?;
    Ok(QueryContextConvergenceReceipt::new(context, version, state))
}

pub fn encode_query_context_convergence_receipt(
    value: QueryContextConvergenceReceipt,
) -> novarocks::QueryContextConvergenceReceipt {
    novarocks::QueryContextConvergenceReceipt {
        query_context: Some(encode_query_context_ref(value.context())),
        version: value.version().get(),
        state: encode_query_context_convergence_state(value.state()),
    }
}

pub fn decode_query_context_convergence_cursor(
    src: &novarocks::QueryContextConvergenceCursor,
    path: FieldPath,
) -> Result<QueryContextConvergenceCursor, ProtocolError> {
    let context = src.query_context.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_context"),
            "query context convergence cursor requires an exact context",
        )
    })?;
    let context = decode_query_context_ref(context, path.field("query_context"))?;
    Ok(
        match QueryContextConvergenceVersion::new(src.current_version) {
            Ok(version) => QueryContextConvergenceCursor::at(context, version),
            Err(_) => QueryContextConvergenceCursor::unobserved(context),
        },
    )
}

pub fn encode_query_context_convergence_cursor(
    value: QueryContextConvergenceCursor,
) -> novarocks::QueryContextConvergenceCursor {
    novarocks::QueryContextConvergenceCursor {
        query_context: Some(encode_query_context_ref(value.context())),
        current_version: value
            .current_version()
            .map_or(0, QueryContextConvergenceVersion::get),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{
        ContextAwareStatusStreamEvent, decode_context_aware_status_event,
        decode_context_aware_subscribe_task_status, decode_status_event,
        encode_context_aware_subscribe_task_status, encode_context_convergence_event,
    };
    use novarocks_execution_contract::task_execution::identity::QueryContextRef;
    use novarocks_proto_codec::ProtocolErrorKind;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    fn context() -> QueryContextRef {
        QueryContextRef::new(
            QueryExecutionId::new(
                QueryId::new(41, 42),
                AttemptId::new(3).expect("nonzero attempt"),
            )
            .expect("nonzero query"),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    fn receipt() -> QueryContextConvergenceReceipt {
        QueryContextConvergenceReceipt::new(
            context(),
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        )
    }

    #[test]
    fn receipt_round_trips_exact_identity_version_and_state() {
        let receipt = receipt();
        let encoded = encode_query_context_convergence_receipt(receipt);
        let decoded = decode_query_context_convergence_receipt(
            &encoded,
            FieldPath::root("context_convergence"),
        )
        .expect("valid receipt");
        assert_eq!(decoded, receipt);
    }

    #[test]
    fn receipt_rejects_missing_identity_zero_version_and_unknown_state() {
        let encoded = encode_query_context_convergence_receipt(receipt());

        let mut missing_identity = encoded.clone();
        missing_identity.query_context = None;
        let error = decode_query_context_convergence_receipt(
            &missing_identity,
            FieldPath::root("context_convergence"),
        )
        .expect_err("identity is mandatory");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);

        let mut zero_version = encoded.clone();
        zero_version.version = 0;
        let error = decode_query_context_convergence_receipt(
            &zero_version,
            FieldPath::root("context_convergence"),
        )
        .expect_err("receipt version must be nonzero");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);

        let mut unknown_state = encoded;
        unknown_state.state = i32::MAX;
        let error = decode_query_context_convergence_receipt(
            &unknown_state,
            FieldPath::root("context_convergence"),
        )
        .expect_err("state set is closed");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn cursor_zero_means_unobserved_and_still_requires_exact_identity() {
        let cursor = QueryContextConvergenceCursor::unobserved(context());
        let encoded = encode_query_context_convergence_cursor(cursor);
        assert_eq!(encoded.current_version, 0);
        assert_eq!(
            decode_query_context_convergence_cursor(
                &encoded,
                FieldPath::root("context_convergence_cursor"),
            )
            .expect("valid cursor"),
            cursor
        );

        let mut missing_identity = encoded;
        missing_identity.query_context = None;
        let error = decode_query_context_convergence_cursor(
            &missing_identity,
            FieldPath::root("context_convergence_cursor"),
        )
        .expect_err("cursor identity is mandatory");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn subscription_preserves_outer_and_cursor_contexts_for_exact_validation() {
        let outer_context = context();
        let cursor_context = context();
        assert_ne!(outer_context, cursor_context);
        let cursor = QueryContextConvergenceCursor::unobserved(cursor_context);
        let encoded = encode_context_aware_subscribe_task_status(outer_context, &[], Some(cursor))
            .expect("bounded subscription");

        let (decoded_outer, task_cursors, decoded_cursor) =
            decode_context_aware_subscribe_task_status(
                &encoded,
                FieldPath::root("subscribe_task_status"),
            )
            .expect("valid subscription");
        assert_eq!(decoded_outer, outer_context);
        assert!(task_cursors.is_empty());
        assert_eq!(decoded_cursor, Some(cursor));

        let error = crate::operation::decode_subscribe_task_status(
            &encoded,
            FieldPath::root("subscribe_task_status"),
        )
        .expect_err("legacy decoder must not ignore a convergence cursor");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn convergence_event_uses_only_the_context_aware_decoder() {
        let receipt = receipt();
        let encoded = encode_context_convergence_event(receipt);
        match decode_context_aware_status_event(&encoded, FieldPath::root("task_status_event"))
            .expect("valid convergence event")
        {
            ContextAwareStatusStreamEvent::ContextConvergence(decoded) => {
                assert_eq!(decoded, receipt)
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let error = decode_status_event(&encoded, FieldPath::root("task_status_event"))
            .expect_err("legacy decoder must not ignore a convergence event");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
    }
}
