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

//! Context root request/reply projection. Body allocations are byte carriers;
//! Native transport must preflight envelope and hold physical leases separately.
use crate::identity::{decode_task_identity, encode_task_identity};
use crate::status::{
    decode_failure_fact, decode_resource_facts, decode_safe_detail, encode_failure_fact,
    encode_resource_facts,
};
use crate::{invalid, missing};
use novarocks_execution_contract::root_result::{
    RootReadDeferral, RootReadDeferred, RootReadOutcome, RootReadRefusal, RootReadResponse,
    RootResultData, RootResultEnd, RootResultRead, RootResultReply,
};
use novarocks_proto_codec::root_result::{decode_kind, decode_profile, encode_kind};
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::{novarocks as wire, result as result_wire};
use std::num::NonZeroU64;
use std::time::Duration;

pub fn encode_read(value: &RootResultRead) -> wire::FetchRootResultRequest {
    wire::FetchRootResultRequest {
        root_task: Some(encode_task_identity(value.root_task())),
        profile_id: value.profile().get(),
        output_kind: Some(encode_kind(value.kind())),
        wanted_sequence: value.wanted().map(NonZeroU64::get),
        consumed_sequence: value.consumed(),
        max_wait_millis: value.max_wait().as_millis() as u64,
    }
}
pub fn decode_read(
    value: &wire::FetchRootResultRequest,
    path: FieldPath,
) -> Result<RootResultRead, ProtocolError> {
    let task = decode_task_identity(
        value
            .root_task
            .as_ref()
            .ok_or_else(|| missing(path.clone().field("root_task"), "root identity is required"))?,
        path.clone().field("root_task"),
    )?;
    let profile = decode_profile(value.profile_id, path.clone().field("profile_id"))?;
    let kind = decode_kind(
        value.output_kind.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("output_kind"),
                "root purpose is required",
            )
        })?,
        path.clone().field("output_kind"),
    )?;
    let wanted = value
        .wanted_sequence
        .map(|n| {
            NonZeroU64::new(n).ok_or_else(|| {
                invalid(
                    path.clone().field("wanted_sequence"),
                    "wanted sequence must be positive",
                )
            })
        })
        .transpose()?;
    RootResultRead::try_new(
        task,
        profile,
        kind,
        wanted,
        value.consumed_sequence,
        Duration::from_millis(value.max_wait_millis),
    )
    .map_err(|error| invalid(path, error.to_string()))
}
fn encode_end(value: RootResultEnd) -> result_wire::RootEnd {
    result_wire::RootEnd {
        sequence: value.sequence.get(),
        output_rows: value.output_rows,
    }
}
fn decode_end(
    value: &result_wire::RootEnd,
    path: FieldPath,
) -> Result<RootResultEnd, ProtocolError> {
    Ok(RootResultEnd {
        sequence: NonZeroU64::new(value.sequence)
            .ok_or_else(|| invalid(path, "End sequence must be positive"))?,
        output_rows: value.output_rows,
    })
}
pub fn encode_reply(
    value: &RootResultReply,
) -> Result<wire::FetchRootResultResponse, ProtocolError> {
    value
        .validate()
        .map_err(|error| invalid(FieldPath::root("root_reply"), error.to_string()))?;
    use wire::root_read_receipt::Outcome;
    let outcome = match &value.outcome {
        RootReadOutcome::AckOnly => Outcome::AckOnly(true),
        RootReadOutcome::NotReady => Outcome::NotReady(true),
        RootReadOutcome::Retired => Outcome::Retired(true),
        RootReadOutcome::AwaitTerminalControl => Outcome::AwaitTerminalControl(true),
        RootReadOutcome::End(end) => Outcome::End(encode_end(*end)),
        RootReadOutcome::Data(data) => Outcome::Data(result_wire::RootData {
            sequence: data.sequence().get(),
            body: data.body().clone(),
            end_after_data: data.end_after_data().map(encode_end),
        }),
    };
    Ok(wire::FetchRootResultResponse {
        root_task: Some(encode_task_identity(value.root_task)),
        profile_id: value.profile.get(),
        output_kind: Some(encode_kind(value.kind)),
        result: Some(wire::fetch_root_result_response::Result::Reply(
            wire::RootReadReceipt {
                accepted_consumed_sequence: value.accepted_consumed,
                outcome: Some(outcome),
            },
        )),
    })
}
pub fn decode_response(
    value: wire::FetchRootResultResponse,
    expected: &RootResultRead,
    maximum_delivered_consumed: u64,
    path: FieldPath,
) -> Result<RootReadResponse, ProtocolError> {
    use wire::root_read_receipt::Outcome;
    let task = decode_task_identity(
        value.root_task.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("root_task"),
                "root reply identity is required",
            )
        })?,
        path.clone().field("root_task"),
    )?;
    let profile = decode_profile(value.profile_id, path.clone().field("profile_id"))?;
    let kind = decode_kind(
        value.output_kind.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("output_kind"),
                "root reply purpose is required",
            )
        })?,
        path.clone().field("output_kind"),
    )?;
    if task != expected.root_task() || profile != expected.profile() || kind != expected.kind() {
        return Err(invalid(
            path,
            "root reply does not match its frozen request identity/profile/purpose",
        ));
    }
    use wire::fetch_root_result_response::Result as WireResult;
    let value = match value.result {
        Some(WireResult::Reply(receipt)) => receipt,
        Some(WireResult::Deferred(deferred)) => {
            use wire::root_read_deferred::Reason;
            let p = path.field("deferred");
            let reason = match deferred.reason {
                Some(Reason::Preparing(true)) => RootReadDeferral::Preparing,
                Some(Reason::ReadHoldersBusy(r)) => RootReadDeferral::ReadHoldersBusy(
                    decode_resource_facts(&r, p.field("read_holders_busy"))?,
                ),
                Some(Reason::SendMetadataBusy(r)) => RootReadDeferral::SendMetadataBusy(
                    decode_resource_facts(&r, p.field("send_metadata_busy"))?,
                ),
                Some(Reason::SendBackingBusy(r)) => RootReadDeferral::SendBackingBusy(
                    decode_resource_facts(&r, p.field("send_backing_busy"))?,
                ),
                _ => return Err(invalid(p, "deferred root read requires a closed reason")),
            };
            return Ok(RootReadResponse::Deferred(RootReadDeferred::for_request(
                expected, reason,
            )));
        }
        Some(WireResult::Refused(refused)) => {
            let p = path.field("refused");
            let fact = decode_failure_fact(
                refused.fact.as_ref().ok_or_else(|| {
                    missing(
                        p.clone().field("fact"),
                        "root refusal requires producer facts",
                    )
                })?,
                p.clone().field("fact"),
            )?;
            let detail = decode_safe_detail(&refused.safe_detail, p.field("safe_detail"))?;
            return Ok(RootReadResponse::Refused(Box::new(
                RootReadRefusal::for_request(expected, fact, detail),
            )));
        }
        None => return Err(missing(path, "root read requires one response result")),
    };
    // One read/ACK RPC is in flight. Older legal cumulative requests may
    // return a newer applied watermark, bounded by the caller's actual socket
    // delivery proofs rather than by prefetch or validation.
    // A seal can win before this request applies its ACK. The closed marker
    // reports the actual frozen watermark, not a fabricated acknowledgement.
    let sealed = matches!(value.outcome, Some(Outcome::AwaitTerminalControl(true)));
    if (!sealed && value.accepted_consumed_sequence < expected.consumed())
        || value.accepted_consumed_sequence > maximum_delivered_consumed
    {
        return Err(invalid(
            path,
            "root reply accepted-consumed exceeds proven delivery or precedes its request",
        ));
    }
    let outcome = match value.outcome {
        Some(Outcome::AckOnly(true)) if expected.wanted().is_none() => RootReadOutcome::AckOnly,
        Some(Outcome::NotReady(true))
            if expected
                .wanted()
                .is_some_and(|wanted| wanted.get() > value.accepted_consumed_sequence) =>
        {
            RootReadOutcome::NotReady
        }
        Some(Outcome::Retired(true))
            if expected
                .wanted()
                .is_some_and(|wanted| wanted.get() <= value.accepted_consumed_sequence) =>
        {
            RootReadOutcome::Retired
        }
        Some(Outcome::AwaitTerminalControl(true)) => RootReadOutcome::AwaitTerminalControl,
        Some(Outcome::End(end)) => {
            let end = decode_end(&end, path.clone().field("end"))?;
            if Some(end.sequence) != expected.wanted() {
                return Err(invalid(path, "root reply End sequence differs from wanted"));
            }
            RootReadOutcome::End(end)
        }
        Some(Outcome::Data(data)) => {
            let sequence = NonZeroU64::new(data.sequence)
                .ok_or_else(|| invalid(path.clone(), "Data sequence must be positive"))?;
            if Some(sequence) != expected.wanted() {
                return Err(invalid(
                    path,
                    "root reply Data sequence differs from wanted",
                ));
            }
            let end = data
                .end_after_data
                .as_ref()
                .map(|end| decode_end(end, path.clone().field("end_after_data")))
                .transpose()?;
            RootReadOutcome::Data(
                RootResultData::try_new(kind, sequence, data.body, end)
                    .map_err(|error| invalid(path.clone(), error.to_string()))?,
            )
        }
        _ => {
            return Err(invalid(
                path,
                "root reply requires a closed outcome consistent with its read",
            ));
        }
    };
    let reply = RootResultReply {
        root_task: task,
        profile,
        kind,
        accepted_consumed: value.accepted_consumed_sequence,
        outcome,
    };
    reply
        .validate()
        .map_err(|error| invalid(path, error.to_string()))?;
    Ok(RootReadResponse::Reply(reply))
}

/// Non-receipt results carry exact binding and producer facts, never an ACK.
pub fn encode_response(
    value: &RootReadResponse,
) -> Result<wire::FetchRootResultResponse, ProtocolError> {
    use wire::fetch_root_result_response::Result as WireResult;
    let result = match value {
        RootReadResponse::Reply(reply) => return encode_reply(reply),
        RootReadResponse::Deferred(deferred) => {
            use wire::root_read_deferred::Reason;
            let reason = match deferred.reason() {
                RootReadDeferral::Preparing => Reason::Preparing(true),
                RootReadDeferral::ReadHoldersBusy(r) => {
                    Reason::ReadHoldersBusy(encode_resource_facts(r))
                }
                RootReadDeferral::SendMetadataBusy(r) => {
                    Reason::SendMetadataBusy(encode_resource_facts(r))
                }
                RootReadDeferral::SendBackingBusy(r) => {
                    Reason::SendBackingBusy(encode_resource_facts(r))
                }
            };
            WireResult::Deferred(wire::RootReadDeferred {
                reason: Some(reason),
            })
        }
        RootReadResponse::Refused(refused) => WireResult::Refused(wire::RootReadRefusal {
            fact: Some(encode_failure_fact(refused.fact())),
            safe_detail: refused.detail().as_str().to_owned(),
        }),
    };
    Ok(wire::FetchRootResultResponse {
        root_task: Some(encode_task_identity(value.root_task())),
        profile_id: value.profile().get(),
        output_kind: Some(encode_kind(value.kind())),
        result: Some(result),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    // These existing receipt tests intentionally reject non-receipt branches.
    fn decode_reply(
        value: wire::FetchRootResultResponse,
        expected: &RootResultRead,
        maximum: u64,
        path: FieldPath,
    ) -> Result<RootResultReply, ProtocolError> {
        match decode_response(value, expected, maximum, path.clone())? {
            RootReadResponse::Reply(reply) => Ok(reply),
            _ => Err(invalid(path, "test expected an actual receipt")),
        }
    }
    fn receipt(value: &mut wire::FetchRootResultResponse) -> &mut wire::RootReadReceipt {
        match value.result.as_mut().expect("encoded result") {
            wire::fetch_root_result_response::Result::Reply(reply) => reply,
            _ => panic!("test expected an actual receipt"),
        }
    }
    use bytes::Bytes;
    use novarocks_execution_contract::identity::TaskIdentity;
    use novarocks_result_contract::{RootOutputKind, RootProfileId};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    fn read(wanted: Option<u64>, consumed: u64) -> RootResultRead {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        RootResultRead::try_new(
            identity,
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            wanted.map(|n| NonZeroU64::new(n).unwrap()),
            consumed,
            Duration::from_millis(100),
        )
        .unwrap()
    }
    #[test]
    fn wanted_and_consumed_are_independent_and_ack_only_is_explicit() {
        for request in [read(Some(2), 0), read(None, 2)] {
            assert_eq!(
                decode_read(&encode_read(&request), FieldPath::root("read")).unwrap(),
                request
            );
        }
        let request = read(Some(1), 0);
        let mut wire = encode_read(&request);
        wire.wanted_sequence = Some(0);
        assert!(decode_read(&wire, FieldPath::root("read")).is_err());
        wire = encode_read(&request);
        wire.max_wait_millis = 1001;
        assert!(decode_read(&wire, FieldPath::root("read")).is_err());
        wire = encode_read(&request);
        wire.profile_id = 2;
        assert!(decode_read(&wire, FieldPath::root("read")).is_err());
    }
    #[test]
    fn replies_preserve_adjacent_end_and_exact_identity() {
        let request = read(Some(1), 0);
        let reply = RootResultReply {
            root_task: request.root_task(),
            profile: request.profile(),
            kind: request.kind(),
            accepted_consumed: 0,
            outcome: RootReadOutcome::Data(
                RootResultData::try_new(
                    request.kind(),
                    NonZeroU64::new(1).unwrap(),
                    Bytes::from_static(&[1, 0, 0, 0, b'x']),
                    Some(RootResultEnd {
                        sequence: NonZeroU64::new(2).unwrap(),
                        output_rows: 1,
                    }),
                )
                .unwrap(),
            ),
        };
        assert_eq!(
            decode_reply(
                encode_reply(&reply).unwrap(),
                &request,
                0,
                FieldPath::root("reply")
            )
            .unwrap(),
            reply
        );
        let mut wire = encode_reply(&reply).unwrap();
        wire.root_task = Some(encode_task_identity(read(Some(1), 0).root_task()));
        assert!(decode_reply(wire, &request, 0, FieldPath::root("reply")).is_err());
        let mut wire = encode_reply(&reply).unwrap();
        wire.profile_id = 2;
        assert!(decode_reply(wire, &request, 0, FieldPath::root("reply")).is_err());
        let mut wire = encode_reply(&reply).unwrap();
        wire.output_kind = Some(encode_kind(RootOutputKind::CountOnly));
        assert!(decode_reply(wire, &request, 0, FieldPath::root("reply")).is_err());
    }
    #[test]
    fn outcomes_must_agree_with_the_applied_retirement_frontier() {
        let request = read(Some(3), 1);
        let reply = RootResultReply {
            root_task: request.root_task(),
            profile: request.profile(),
            kind: request.kind(),
            accepted_consumed: 2,
            outcome: RootReadOutcome::Retired,
        };
        assert!(
            decode_reply(
                encode_reply(&reply).unwrap(),
                &request,
                2,
                FieldPath::root("reply")
            )
            .is_err()
        );
        let request = read(Some(1), 1);
        let reply = RootResultReply {
            root_task: request.root_task(),
            profile: request.profile(),
            kind: request.kind(),
            accepted_consumed: 1,
            outcome: RootReadOutcome::NotReady,
        };
        assert!(
            decode_reply(
                encode_reply(&reply).unwrap(),
                &request,
                1,
                FieldPath::root("reply")
            )
            .is_err()
        );
        for outcome in [
            RootReadOutcome::End(RootResultEnd {
                sequence: NonZeroU64::new(1).unwrap(),
                output_rows: 0,
            }),
            RootReadOutcome::Data(
                RootResultData::try_new(
                    request.kind(),
                    NonZeroU64::new(1).unwrap(),
                    Bytes::from_static(b"x"),
                    None,
                )
                .unwrap(),
            ),
        ] {
            let reply = RootResultReply {
                outcome,
                ..reply.clone()
            };
            assert!(encode_reply(&reply).is_err());
        }
    }
    #[test]
    fn not_ready_and_retired_still_return_applied_watermark() {
        for outcome in [RootReadOutcome::NotReady, RootReadOutcome::Retired] {
            let wanted = if matches!(outcome, RootReadOutcome::Retired) {
                2
            } else {
                3
            };
            let request = read(Some(wanted), 1);
            let reply = RootResultReply {
                root_task: request.root_task(),
                profile: request.profile(),
                kind: request.kind(),
                accepted_consumed: 2,
                outcome,
            };
            assert_eq!(
                decode_reply(
                    encode_reply(&reply).unwrap(),
                    &request,
                    2,
                    FieldPath::root("reply")
                )
                .unwrap(),
                reply
            );
            assert!(
                decode_reply(
                    encode_reply(&reply).unwrap(),
                    &request,
                    1,
                    FieldPath::root("reply")
                )
                .is_err()
            );
        }
    }

    #[test]
    fn seal_before_ack_preserves_actual_watermark_without_fabricating_consumption() {
        for wanted in [None, Some(3)] {
            let request = read(wanted, 2);
            let reply = RootResultReply {
                root_task: request.root_task(),
                profile: request.profile(),
                kind: request.kind(),
                accepted_consumed: 1,
                outcome: RootReadOutcome::AwaitTerminalControl,
            };
            assert_eq!(
                decode_reply(
                    encode_reply(&reply).unwrap(),
                    &request,
                    2,
                    FieldPath::root("reply"),
                )
                .unwrap(),
                reply,
            );
            assert!(
                decode_reply(
                    encode_reply(&reply).unwrap(),
                    &request,
                    0,
                    FieldPath::root("reply"),
                )
                .is_err(),
                "closed replies cannot exceed actual socket delivery proofs",
            );
            let mut wire = encode_reply(&reply).unwrap();
            receipt(&mut wire).outcome = Some(wire::root_read_receipt::Outcome::AckOnly(true));
            assert!(decode_reply(wire, &request, 2, FieldPath::root("reply")).is_err());
            let mut wire = encode_reply(&reply).unwrap();
            receipt(&mut wire).outcome = Some(wire::root_read_receipt::Outcome::NotReady(true));
            assert!(decode_reply(wire, &request, 2, FieldPath::root("reply")).is_err());
            let mut wire = encode_reply(&reply).unwrap();
            receipt(&mut wire).outcome = Some(wire::root_read_receipt::Outcome::Retired(true));
            assert!(decode_reply(wire, &request, 2, FieldPath::root("reply")).is_err());
        }
    }
    #[test]
    fn non_receipt_root_results_preserve_request_and_producer_facts_without_ack() {
        use novarocks_execution_contract::{SafeDetail, failure::*};
        use prost::Message;
        use std::num::NonZeroU16;
        let request = read(Some(3), 2);
        let facts = ResourceFacts {
            dimension: ResourceDimension::Bytes,
            request_scope: Some(ResourceScope::Request),
            constraint_scope: None,
            requested: Some(u64::MAX),
            required_growth: Some(0),
            limit: None,
            live: Some(0),
            committed: Some(u64::MAX),
        };
        let reason = FailureReason::new(
            NonZeroU16::new(65535).unwrap(),
            NonZeroU16::new(65534).unwrap(),
        );
        let failure = FailureFact::new(FailureClass::Resource, reason).with_resource(facts);
        let mut results = vec![RootReadResponse::Refused(Box::new(
            RootReadRefusal::for_request(
                &request,
                failure,
                SafeDetail::new("x".repeat(512)).unwrap(),
            ),
        ))];
        for code in 1..=4 {
            results.push(RootReadResponse::Refused(Box::new(
                RootReadRefusal::for_request(
                    &request,
                    FailureFact::new(
                        FailureClass::Dependency,
                        FailureReason::new(
                            domain::STORAGE_AUTHORITY,
                            NonZeroU16::new(code).unwrap(),
                        ),
                    ),
                    SafeDetail::new(format!("Dependency failure (reason 23:{code})")).unwrap(),
                ),
            )));
        }
        for reason in [
            RootReadDeferral::Preparing,
            RootReadDeferral::ReadHoldersBusy(facts),
            RootReadDeferral::SendMetadataBusy(facts),
            RootReadDeferral::SendBackingBusy(facts),
        ] {
            results.push(RootReadResponse::Deferred(RootReadDeferred::for_request(
                &request, reason,
            )));
        }
        for result in results {
            let wire = encode_response(&result).unwrap();
            assert!(!matches!(
                wire.result,
                Some(wire::fetch_root_result_response::Result::Reply(_))
            ));
            let bytes = wire.encode_to_vec();
            crate::resource_preflight::check_root_result_reply(&bytes).unwrap();
            assert!(bytes.len() <= novarocks_result_contract::RootProfileV1::ENVELOPE_BYTES);
            let decoded = decode_response(
                wire::FetchRootResultResponse::decode(bytes.as_slice()).unwrap(),
                &request,
                2,
                FieldPath::root("reply"),
            )
            .unwrap();
            assert_eq!(decoded, result);
        }
    }
    #[test]
    fn non_receipt_root_results_require_actual_facts_and_closed_deferral() {
        let request = read(Some(1), 0);
        let binding = || wire::FetchRootResultResponse {
            root_task: Some(encode_task_identity(request.root_task())),
            profile_id: request.profile().get(),
            output_kind: Some(encode_kind(request.kind())),
            result: None,
        };
        let mut values = vec![binding()];
        let mut value = binding();
        value.result = Some(wire::fetch_root_result_response::Result::Refused(
            wire::RootReadRefusal {
                fact: None,
                safe_detail: "original domain refusal".into(),
            },
        ));
        values.push(value);
        for reason in [
            None,
            Some(wire::root_read_deferred::Reason::Preparing(false)),
        ] {
            let mut value = binding();
            value.result = Some(wire::fetch_root_result_response::Result::Deferred(
                wire::RootReadDeferred { reason },
            ));
            values.push(value);
        }
        for value in values {
            assert!(decode_response(value, &request, 0, FieldPath::root("reply")).is_err());
        }
    }
}
