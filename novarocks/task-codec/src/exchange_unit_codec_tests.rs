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

use super::*;
use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
use novarocks_execution_contract::{ExchangeSource, FragmentNodeId, TaskIdentity};
use novarocks_proto_codec::ProtocolErrorKind;
use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId, StageId, TaskId};
use prost::Message;
use std::num::NonZeroU16;

fn path() -> FieldPath {
    FieldPath::root("exchange_unit")
}

fn pair() -> ExchangeProcessPair {
    ExchangeProcessPair {
        sender: BackendProcessId::new_v7(),
        receiver: BackendProcessId::new_v7(),
    }
}

fn task(process: BackendProcessId, ordinal: u32) -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(ordinal).unwrap(),
        TaskId::new(ordinal).unwrap(),
        process,
    )
}

fn stream(pair: ExchangeProcessPair, ordinal: u32) -> ExchangeStreamIdentity {
    ExchangeStreamIdentity::try_new(
        ExchangeSource::new(
            task(pair.sender, 1),
            UniqueId::new(11, i64::from(ordinal) + 12),
            ordinal,
        ),
        task(pair.receiver, 2),
        UniqueId::new(21, 22),
        FragmentNodeId::new(7),
    )
    .unwrap()
}

fn sequence(value: u64) -> ExchangeUnitSequence {
    ExchangeUnitSequence::try_new(value).unwrap()
}

fn demand(value: u64) -> ExchangeUnitDemand {
    ExchangeUnitDemand {
        sequence: sequence(value),
        wire_bytes: 3,
    }
}

fn frame() -> DecodedExchangeUnitFrame {
    DecodedExchangeUnitFrame {
        stream: stream(pair(), 1),
        sender_count: 2,
        sender_id: 4,
        backend_number: 5,
        item: ExchangeUnitItem::Unit {
            demand: demand(1),
            end_after_unit: false,
        },
        payload: Bytes::from_static(b"ipc"),
        next_demand: Some(demand(2)),
    }
}

fn facts() -> ExchangeStreamFacts {
    ExchangeStreamFacts {
        custody_through: 1,
        granted_through: 2,
        ..Default::default()
    }
}

fn refusal() -> FailureFact {
    FailureFact::new(
        FailureClass::Resource,
        FailureReason::new(domain::EXCHANGE, NonZeroU16::new(7).unwrap()),
    )
}

#[test]
fn unit_sequences_preserve_the_unsigned_domain_and_refuse_zero() {
    for value in [1, i64::MAX as u64 + 1, u64::MAX] {
        assert_eq!(decode_unit_sequence(value, path()).unwrap().get(), value);
    }
    assert_eq!(
        decode_unit_sequence(0, path()).unwrap_err().kind(),
        &ProtocolErrorKind::InvalidValue
    );
    let mut value = encode_delivery_request(frame());
    let Some(wire::deliver_exchange_unit_request::Item::Unit(unit)) = value.item.as_mut() else {
        panic!("unit expected");
    };
    unit.sequence = u64::MAX;
    unit.end_after_unit = true;
    value.next_demand = None;
    assert!(decode_delivery_request(value, path()).is_err());
}

#[test]
fn delivery_round_trip_keeps_the_same_payload_allocation_and_full_envelope() {
    let expected = frame();
    let pointer = expected.payload.as_ptr();
    let encoded = encode_delivery_request(expected.clone());
    let decoded = decode_delivery_request(encoded, path()).unwrap();
    assert_eq!(decoded, expected);
    assert_eq!(decoded.payload.as_ptr(), pointer);

    let encoded = encode_delivery_request(expected.clone());
    let transported =
        wire::DeliverExchangeUnitRequest::decode(encoded.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        decode_delivery_request(transported, path()).unwrap(),
        expected
    );
}

#[test]
fn envelope_length_and_required_fields_are_checked_before_delivery() {
    let valid = encode_delivery_request(frame());
    let mutations: [fn(&mut wire::DeliverExchangeUnitRequest); 8] = [
        |value| value.finst_id_hi += 1,
        |value| value.source_finst_id_lo += 1,
        |value| value.node_id += 1,
        |value| value.sender_ordinal = 0,
        |value| value.sender_count = 1,
        |value| value.stream = None,
        |value| value.item = None,
        |value| {
            let Some(wire::deliver_exchange_unit_request::Item::Unit(unit)) = value.item.as_mut()
            else {
                panic!("unit expected");
            };
            unit.wire_bytes += 1;
        },
    ];
    for mutate in mutations {
        let mut value = valid.clone();
        mutate(&mut value);
        assert!(decode_delivery_request(value, path()).is_err());
    }
    let mut bad = valid;
    let Some(wire::deliver_exchange_unit_request::Item::Unit(unit)) = bad.item.as_mut() else {
        panic!("unit expected");
    };
    unit.wire_bytes = 99;
    assert_eq!(
        decode_delivery_request(bad, path()).unwrap_err().path(),
        &path().field("unit").field("wire_bytes")
    );
}

#[test]
fn end_is_distinct_and_old_replays_may_piggyback_a_newer_demand() {
    let mut value = frame();
    value.next_demand = Some(demand(4));
    assert_eq!(
        decode_delivery_request(encode_delivery_request(value.clone()), path()).unwrap(),
        value
    );
    value.item = ExchangeUnitItem::Unit {
        demand: demand(1),
        end_after_unit: true,
    };
    assert!(decode_delivery_request(encode_delivery_request(value.clone()), path()).is_err());
    value.next_demand = None;
    assert_eq!(
        decode_delivery_request(encode_delivery_request(value.clone()), path()).unwrap(),
        value
    );
    value.item = ExchangeUnitItem::End(sequence(2));
    value.payload = Bytes::new();
    assert_eq!(
        decode_delivery_request(encode_delivery_request(value.clone()), path()).unwrap(),
        value
    );
}

#[test]
fn close_and_stream_facts_preserve_unsigned_end_and_independent_sender_stop() {
    let stream = stream(pair(), 1);
    for closed in [
        ExchangeUnitCloseDisposition::Withdrawn,
        ExchangeUnitCloseDisposition::Consumed {
            final_sequence: sequence(u64::MAX),
        },
    ] {
        let encoded = encode_receiver_close(closed);
        assert_eq!(decode_receiver_close(&encoded, path()).unwrap(), closed);
    }
    let expected = DecodedExchangeStreamState {
        stream,
        facts: ExchangeStreamFacts {
            custody_through: u64::MAX,
            granted_through: u64::MAX - 1,
            sender_stopped: true,
            receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
                final_sequence: sequence(u64::MAX),
            }),
        },
    };
    assert_eq!(
        decode_stream_state(&encode_stream_state(expected), path()).unwrap(),
        expected
    );
    assert!(
        decode_receiver_close(
            &wire::ExchangeUnitReceiverClosed { disposition: None },
            path()
        )
        .is_err()
    );
    let mut invalid = encode_stream_state(expected);
    invalid.custody_through -= 1;
    assert!(decode_stream_state(&invalid, path()).is_err());
    let mut invalid = encode_stream_state(expected);
    // End is not a data grant, even at the top of the unsigned domain.
    invalid.granted_through = u64::MAX;
    assert!(decode_stream_state(&invalid, path()).is_err());
    let mut invalid = encode_stream_state(expected);
    invalid.granted_through -= 1;
    assert!(decode_stream_state(&invalid, path()).is_err());
}

#[test]
fn delivery_results_match_request_and_preserve_typed_refusals() {
    let expected = frame();
    for verdict in [
        ExchangeAdmissionVerdict::Applied,
        ExchangeAdmissionVerdict::Idempotent,
    ] {
        let outcome = DecodedExchangeDeliveryOutcome::Observed {
            verdict,
            facts: facts(),
        };
        let value = encode_delivery_response(expected.stream, sequence(1), outcome);
        assert_eq!(
            decode_delivery_response(&value, expected.stream, sequence(1), path()).unwrap(),
            outcome
        );
        assert!(decode_delivery_response(&value, expected.stream, sequence(2), path()).is_err());
        assert!(decode_delivery_response(&value, stream(pair(), 1), sequence(1), path()).is_err());
    }
    let outcome = DecodedExchangeDeliveryOutcome::Refused(refusal());
    let value = encode_delivery_response(expected.stream, sequence(1), outcome);
    assert_eq!(
        decode_delivery_response(&value, expected.stream, sequence(1), path()).unwrap(),
        outcome
    );
    let mut unknown = encode_delivery_response(
        expected.stream,
        sequence(1),
        DecodedExchangeDeliveryOutcome::Observed {
            verdict: ExchangeAdmissionVerdict::Applied,
            facts: facts(),
        },
    );
    let Some(wire::deliver_exchange_unit_response::Outcome::Observed(observation)) =
        unknown.outcome.as_mut()
    else {
        panic!("observation expected");
    };
    observation.verdict = 99;
    assert_eq!(
        decode_delivery_response(&unknown, expected.stream, sequence(1), path())
            .unwrap_err()
            .kind(),
        &ProtocolErrorKind::InvalidEnum
    );
    unknown.outcome = None;
    assert!(decode_delivery_response(&unknown, expected.stream, sequence(1), path()).is_err());
}

#[test]
fn demand_batches_accept_more_than_a_publication_page_and_preserve_identity_checks() {
    let pair = pair();
    let entries = (0..257)
        .map(|index| DecodedExchangeDemandEntry {
            stream: stream(pair, u32::try_from(index).unwrap()),
            operation: if index == 0 {
                ExchangeDemandOperation::StopSender
            } else {
                ExchangeDemandOperation::Demand(demand(1))
            },
        })
        .collect::<Vec<_>>();
    let encoded = encode_demand_request(&entries, path()).unwrap();
    let decoded = decode_demand_request(&encoded, path()).unwrap();
    assert_eq!(decoded.pair, pair);
    assert_eq!(decoded.entries, entries);
    let responses = entries
        .iter()
        .map(|entry| DecodedExchangeDemandResult {
            stream: entry.stream,
            outcome: Ok(facts()),
        })
        .collect::<Vec<_>>();
    let streams = entries.iter().map(|entry| entry.stream).collect::<Vec<_>>();
    assert_eq!(
        decode_demand_response(&encode_demand_response(&responses), &streams, path()).unwrap(),
        responses
    );
    let mut duplicate = encoded.clone();
    duplicate.entries[1] = duplicate.entries[0].clone();
    assert_eq!(
        decode_demand_request(&duplicate, path())
            .unwrap_err()
            .kind(),
        &ProtocolErrorKind::DuplicateField
    );
    let mut crossed = encoded.clone();
    crossed.entries[1].stream = Some(encode_stream(stream(
        ExchangeProcessPair {
            sender: BackendProcessId::new_v7(),
            ..pair
        },
        1,
    )));
    assert!(decode_demand_request(&crossed, path()).is_err());
    assert!(
        decode_demand_request(
            &wire::RegisterExchangeDemandRequest { entries: vec![] },
            path()
        )
        .is_err()
    );
}

#[test]
fn demand_responses_cover_exact_requested_streams_and_each_typed_outcome() {
    let pair = pair();
    let streams = [stream(pair, 0), stream(pair, 1)];
    let values = [
        DecodedExchangeDemandResult {
            stream: streams[0],
            outcome: Ok(facts()),
        },
        DecodedExchangeDemandResult {
            stream: streams[1],
            outcome: Err(refusal()),
        },
    ];
    let encoded = encode_demand_response(&values);
    assert_eq!(
        decode_demand_response(&encoded, &streams, path()).unwrap(),
        values
    );
    let mut duplicate = encoded.clone();
    duplicate.results[1] = duplicate.results[0].clone();
    assert!(decode_demand_response(&duplicate, &streams, path()).is_err());
    assert!(decode_demand_response(&encoded, &streams[..1], path()).is_err());
    let mut contradictory = encoded;
    let Some(wire::exchange_demand_result::Outcome::State(state)) =
        contradictory.results[0].outcome.as_mut()
    else {
        panic!("state expected");
    };
    state.stream = Some(encode_stream(streams[1]));
    assert!(decode_demand_response(&contradictory, &streams, path()).is_err());
}

#[test]
fn subscription_requires_exact_process_identities_and_nonzero_generation() {
    let expected = DecodedExchangeGrantSubscription {
        pair: pair(),
        generation: NonZeroU64::new(7).unwrap(),
    };
    let mut encoded = encode_grant_subscription(expected);
    assert_eq!(
        decode_grant_subscription(&encoded, path()).unwrap(),
        expected
    );
    encoded.generation = 0;
    assert!(decode_grant_subscription(&encoded, path()).is_err());
    encoded.generation = 7;
    encoded.sender.as_mut().unwrap().value = vec![0; 16];
    assert!(decode_grant_subscription(&encoded, path()).is_err());
    encoded.sender = None;
    assert!(decode_grant_subscription(&encoded, path()).is_err());
}

#[test]
fn grant_batches_accept_more_than_a_publication_page_and_preserve_snapshot_fences() {
    let expected = DecodedExchangeGrantSubscription {
        pair: pair(),
        generation: NonZeroU64::new(7).unwrap(),
    };
    let value = DecodedExchangeGrantBatch {
        generation: expected.generation,
        snapshot: true,
        snapshot_complete: true,
        states: (0..257)
            .map(|ordinal| DecodedExchangeStreamState {
                stream: stream(expected.pair, ordinal),
                facts: facts(),
            })
            .collect(),
    };
    let encoded = encode_grant_batch(&value, expected.pair, path()).unwrap();
    assert_eq!(
        decode_grant_batch(&encoded, expected, path()).unwrap(),
        value
    );
    let mut empty = value.clone();
    empty.states.clear();
    assert_eq!(
        decode_grant_batch(
            &encode_grant_batch(&empty, expected.pair, path()).unwrap(),
            expected,
            path()
        )
        .unwrap(),
        empty
    );
    let mut invalid = encoded.clone();
    invalid.generation += 1;
    assert!(decode_grant_batch(&invalid, expected, path()).is_err());
    invalid = encoded.clone();
    invalid.states.push(invalid.states[0].clone());
    assert!(decode_grant_batch(&invalid, expected, path()).is_err());
    invalid = encoded.clone();
    invalid.states[0].stream = Some(encode_stream(stream(pair(), 1)));
    assert!(decode_grant_batch(&invalid, expected, path()).is_err());
    invalid = encoded;
    invalid.snapshot = false;
    assert!(decode_grant_batch(&invalid, expected, path()).is_err());
}
