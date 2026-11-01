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

//! Pure exchange identity, sequence and normal-close projection.

use novarocks_execution_contract::exchange_flow::{
    ExchangeCloseDisposition, ExchangeFrameSequence, ExchangeStreamClose, ExchangeStreamIdentity,
};
use novarocks_execution_contract::{ExchangeSource, FragmentNodeId};
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::novarocks as wire;
use novarocks_types::UniqueId;

use crate::identity::{decode_task_identity, encode_task_identity};
use crate::{inconsistent, invalid, missing};

pub fn decode_stream(
    value: &wire::ExchangeStreamIdentity,
    path: FieldPath,
) -> Result<ExchangeStreamIdentity, ProtocolError> {
    let source = decode_task_identity(
        value.source_task.as_ref().ok_or_else(|| {
            missing(
                path.field("source_task"),
                "exchange source Task is required",
            )
        })?,
        path.field("source_task"),
    )?;
    let destination = decode_task_identity(
        value.destination_task.as_ref().ok_or_else(|| {
            missing(
                path.field("destination_task"),
                "exchange destination Task is required",
            )
        })?,
        path.field("destination_task"),
    )?;
    ExchangeStreamIdentity::try_new(
        ExchangeSource::new(
            source,
            UniqueId::new(value.source_finst_id_hi, value.source_finst_id_lo),
            value.sender_ordinal,
        ),
        destination,
        UniqueId::new(value.destination_finst_id_hi, value.destination_finst_id_lo),
        FragmentNodeId::new(value.destination_node_id),
    )
    .map_err(|error| invalid(path, error.to_string()))
}

pub fn encode_stream(value: ExchangeStreamIdentity) -> wire::ExchangeStreamIdentity {
    wire::ExchangeStreamIdentity {
        source_task: Some(encode_task_identity(value.source().task())),
        source_finst_id_hi: value.source().fragment_instance_id().high(),
        source_finst_id_lo: value.source().fragment_instance_id().low(),
        sender_ordinal: value.source().sender_ordinal(),
        destination_task: Some(encode_task_identity(value.destination())),
        destination_finst_id_hi: value.destination_fragment_instance_id().high(),
        destination_finst_id_lo: value.destination_fragment_instance_id().low(),
        destination_node_id: value.destination_node().get(),
    }
}

pub fn decode_sequence(
    value: u64,
    path: FieldPath,
) -> Result<ExchangeFrameSequence, ProtocolError> {
    ExchangeFrameSequence::try_new(value).map_err(|error| invalid(path, error.to_string()))
}

/// The old close carrier omits source Task, so the frozen caller stream supplies
/// that identity; every address carried by the reply must match it exactly.
pub fn decode_normal_close(
    value: &wire::ExchangeNormalClosed,
    expected: ExchangeStreamIdentity,
    expected_sender_count: u32,
    path: FieldPath,
) -> Result<ExchangeStreamClose, ProtocolError> {
    let destination = decode_task_identity(
        value.destination_task.as_ref().ok_or_else(|| {
            missing(
                path.field("destination_task"),
                "normal close requires the exact destination Task",
            )
        })?,
        path.field("destination_task"),
    )?;
    if destination != expected.destination()
        || value.destination_finst_id_hi != expected.destination_fragment_instance_id().high()
        || value.destination_finst_id_lo != expected.destination_fragment_instance_id().low()
        || value.destination_node_id != expected.destination_node().get()
        || value.source_finst_id_hi != expected.source().fragment_instance_id().high()
        || value.source_finst_id_lo != expected.source().fragment_instance_id().low()
        || value.sender_ordinal != expected.source().sender_ordinal()
        || expected_sender_count == 0
        || value.sender_count != expected_sender_count
        || value.sender_ordinal >= expected_sender_count
    {
        return Err(inconsistent(
            path.clone(),
            "normal close does not match its frozen exchange stream",
        ));
    }
    let disposition = match value.disposition.as_ref().ok_or_else(|| {
        missing(
            path.field("disposition"),
            "normal close requires an explicit disposition",
        )
    })? {
        wire::exchange_normal_closed::Disposition::Consumed(consumed) => {
            ExchangeCloseDisposition::Consumed {
                final_sequence: decode_sequence(
                    consumed.final_sequence,
                    path.field("consumed").field("final_sequence"),
                )?,
            }
        }
        wire::exchange_normal_closed::Disposition::Withdrawn(_) => {
            ExchangeCloseDisposition::Withdrawn
        }
    };
    Ok(ExchangeStreamClose {
        stream: expected,
        disposition,
    })
}

pub fn encode_normal_close(
    value: ExchangeStreamClose,
    sender_count: u32,
) -> wire::ExchangeNormalClosed {
    let stream = value.stream;
    wire::ExchangeNormalClosed {
        destination_task: Some(encode_task_identity(stream.destination())),
        destination_finst_id_hi: stream.destination_fragment_instance_id().high(),
        destination_finst_id_lo: stream.destination_fragment_instance_id().low(),
        destination_node_id: stream.destination_node().get(),
        source_finst_id_hi: stream.source().fragment_instance_id().high(),
        source_finst_id_lo: stream.source().fragment_instance_id().low(),
        sender_ordinal: stream.source().sender_ordinal(),
        sender_count,
        disposition: Some(match value.disposition {
            ExchangeCloseDisposition::Consumed { final_sequence } => {
                wire::exchange_normal_closed::Disposition::Consumed(wire::ExchangeConsumed {
                    final_sequence: final_sequence.get(),
                })
            }
            ExchangeCloseDisposition::Withdrawn => {
                wire::exchange_normal_closed::Disposition::Withdrawn(wire::ExchangeWithdrawn {})
            }
        }),
    }
}

#[cfg(test)]
#[path = "exchange_flow_codec_tests.rs"]
mod tests;
