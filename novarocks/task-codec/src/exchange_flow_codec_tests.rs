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
use novarocks_execution_contract::TaskIdentity;
use novarocks_proto_codec::ProtocolErrorKind;
use novarocks_types::identity::{
    AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use prost::Message;

fn task(attempt: u64, ordinal: u32) -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(attempt).unwrap()).unwrap(),
        StageId::new(ordinal).unwrap(),
        TaskId::new(ordinal).unwrap(),
        BackendProcessId::new_v7(),
    )
}

fn stream() -> ExchangeStreamIdentity {
    ExchangeStreamIdentity::try_new(
        ExchangeSource::new(task(1, 1), UniqueId::new(11, 12), 1),
        task(1, 2),
        UniqueId::new(21, 22),
        FragmentNodeId::new(7),
    )
    .unwrap()
}

fn path() -> FieldPath {
    FieldPath::root("exchange")
}

#[test]
fn stream_round_trip_preserves_full_source_and_destination_identity() {
    let stream = stream();
    let encoded = encode_stream(stream);
    let wire = wire::ExchangeStreamIdentity::decode(encoded.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decode_stream(&wire, path()).unwrap(), stream);
    assert_eq!(
        wire.source_task,
        Some(encode_task_identity(stream.source().task()))
    );
    assert_eq!(
        wire.destination_task,
        Some(encode_task_identity(stream.destination()))
    );
}

#[test]
fn missing_task_negative_node_cross_attempt_and_invalid_process_are_refused() {
    let valid = encode_stream(stream());
    let mutations: [fn(&mut wire::ExchangeStreamIdentity); 6] = [
        |r| r.source_task = None,
        |r| r.destination_task = None,
        |r| r.destination_node_id = -1,
        |r| r.destination_task = Some(encode_task_identity(task(2, 2))),
        |r| {
            r.source_task
                .as_mut()
                .unwrap()
                .backend_process_id
                .as_mut()
                .unwrap()
                .value = vec![0; 16]
        },
        |r| {
            r.destination_task
                .as_mut()
                .unwrap()
                .backend_process_id
                .as_mut()
                .unwrap()
                .value = vec![7; 15]
        },
    ];
    for mutate in mutations {
        let mut value = valid.clone();
        mutate(&mut value);
        assert!(decode_stream(&value, path()).is_err());
    }
}

#[test]
fn sequence_projection_preserves_nonzero_and_maximum_and_rejects_zero() {
    for sequence in [1, 2, u64::MAX] {
        assert_eq!(decode_sequence(sequence, path()).unwrap().get(), sequence);
    }
    assert_eq!(
        decode_sequence(0, path()).unwrap_err().kind(),
        &ProtocolErrorKind::InvalidValue
    );
}

#[test]
fn close_round_trip_preserves_cumulative_final_sequence_and_withdrawal() {
    let stream = stream();
    for disposition in [
        ExchangeCloseDisposition::Consumed {
            final_sequence: ExchangeFrameSequence::FIRST,
        },
        ExchangeCloseDisposition::Consumed {
            final_sequence: ExchangeFrameSequence::try_new(19).unwrap(),
        },
        ExchangeCloseDisposition::Consumed {
            final_sequence: ExchangeFrameSequence::try_new(u64::MAX).unwrap(),
        },
        ExchangeCloseDisposition::Withdrawn,
    ] {
        let close = ExchangeStreamClose {
            stream,
            disposition,
        };
        let encoded = encode_normal_close(close, 2);
        let wire = wire::ExchangeNormalClosed::decode(encoded.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            decode_normal_close(&wire, stream, 2, path()).unwrap(),
            close
        );
    }
}

#[test]
fn close_requires_explicit_disposition_and_exact_frozen_stream_fields() {
    let stream = stream();
    let close = ExchangeStreamClose {
        stream,
        disposition: ExchangeCloseDisposition::Consumed {
            final_sequence: ExchangeFrameSequence::FIRST,
        },
    };
    let valid = encode_normal_close(close, 2);
    let mutations: [fn(&mut wire::ExchangeNormalClosed); 12] = [
        |r| r.disposition = None,
        |r| r.destination_task = None,
        |r| r.destination_task = Some(encode_task_identity(task(2, 2))),
        |r| {
            r.destination_task.as_mut().unwrap().backend_process_id =
                Some(encode_task_identity(task(1, 2)).backend_process_id.unwrap())
        },
        |r| r.destination_node_id += 1,
        |r| r.source_finst_id_hi += 1,
        |r| r.source_finst_id_lo += 1,
        |r| r.destination_finst_id_hi += 1,
        |r| r.destination_finst_id_lo += 1,
        |r| r.sender_count = 0,
        |r| r.sender_count = 3,
        |r| r.sender_ordinal = 0,
    ];
    for mutate in mutations {
        let mut value = valid.clone();
        mutate(&mut value);
        assert!(decode_normal_close(&value, stream, 2, path()).is_err());
    }
    assert!(decode_normal_close(&valid, stream, 0, path()).is_err());
    assert!(decode_normal_close(&valid, stream, 1, path()).is_err());
    let mut zero = valid;
    zero.disposition = Some(wire::exchange_normal_closed::Disposition::Consumed(
        wire::ExchangeConsumed { final_sequence: 0 },
    ));
    assert_eq!(
        decode_normal_close(&zero, stream, 2, path())
            .unwrap_err()
            .kind(),
        &ProtocolErrorKind::InvalidValue
    );
}
