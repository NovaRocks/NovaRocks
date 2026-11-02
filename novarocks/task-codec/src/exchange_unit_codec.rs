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

//! Pure projection of ordered Exchange units and their cumulative control facts.
//!
//! These checks establish wire structure and exact identities. They do not grant
//! receiver capacity, authorize a caller, infer consumption or classify a failure.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use bytes::Bytes;
use novarocks_execution_contract::exchange_flow::ExchangeStreamIdentity;
use novarocks_execution_contract::failure::FailureFact;
use novarocks_execution_contract::ordered_retained_stream::exchange::{
    ExchangeAdmissionVerdict, ExchangeStreamFacts, ExchangeUnitCloseDisposition,
    ExchangeUnitDemand, ExchangeUnitItem, ExchangeUnitSequence,
};
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::novarocks as wire;
use novarocks_types::UniqueId;
use novarocks_types::identity::BackendProcessId;

use crate::exchange_flow_codec::{decode_stream, encode_stream};
use crate::status::{decode_failure_fact, encode_failure_fact};
use crate::{duplicate, inconsistent, invalid, invalid_enum, missing};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeProcessPair {
    pub sender: BackendProcessId,
    pub receiver: BackendProcessId,
}

impl ExchangeProcessPair {
    pub const fn for_stream(stream: ExchangeStreamIdentity) -> Self {
        Self {
            sender: stream.source().task().backend_process_id(),
            receiver: stream.destination().backend_process_id(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedExchangeUnitFrame {
    pub stream: ExchangeStreamIdentity,
    pub sender_count: u32,
    pub sender_id: i32,
    pub backend_number: i32,
    pub item: ExchangeUnitItem,
    pub payload: Bytes,
    pub next_demand: Option<ExchangeUnitDemand>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedExchangeStreamState {
    pub stream: ExchangeStreamIdentity,
    pub facts: ExchangeStreamFacts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodedExchangeDeliveryOutcome {
    Observed {
        verdict: ExchangeAdmissionVerdict,
        facts: ExchangeStreamFacts,
    },
    Refused(FailureFact),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeDemandOperation {
    Demand(ExchangeUnitDemand),
    StopSender,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedExchangeDemandEntry {
    pub stream: ExchangeStreamIdentity,
    pub operation: ExchangeDemandOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedExchangeDemandBatch {
    pub pair: ExchangeProcessPair,
    pub entries: Vec<DecodedExchangeDemandEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedExchangeDemandResult {
    pub stream: ExchangeStreamIdentity,
    pub outcome: Result<ExchangeStreamFacts, FailureFact>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedExchangeGrantSubscription {
    pub pair: ExchangeProcessPair,
    pub generation: NonZeroU64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedExchangeGrantBatch {
    pub generation: NonZeroU64,
    pub snapshot: bool,
    pub snapshot_complete: bool,
    pub states: Vec<DecodedExchangeStreamState>,
}

pub fn decode_unit_sequence(
    value: u64,
    path: FieldPath,
) -> Result<ExchangeUnitSequence, ProtocolError> {
    ExchangeUnitSequence::try_new(value).map_err(|error| invalid(path, error.to_string()))
}

pub fn decode_unit_demand(
    value: &wire::ExchangeUnitDemand,
    path: FieldPath,
) -> Result<ExchangeUnitDemand, ProtocolError> {
    if value.wire_bytes == 0 {
        return Err(invalid(
            path.field("wire_bytes"),
            "exchange data unit must contain wire bytes",
        ));
    }
    Ok(ExchangeUnitDemand {
        sequence: decode_unit_sequence(value.sequence, path.field("sequence"))?,
        wire_bytes: value.wire_bytes,
    })
}

pub fn encode_unit_demand(value: ExchangeUnitDemand) -> wire::ExchangeUnitDemand {
    wire::ExchangeUnitDemand {
        sequence: value.sequence.get(),
        wire_bytes: value.wire_bytes,
    }
}

fn checked_stream(
    value: Option<&wire::ExchangeStreamIdentity>,
    path: FieldPath,
) -> Result<ExchangeStreamIdentity, ProtocolError> {
    let stream = decode_stream(
        value.ok_or_else(|| missing(path.clone(), "exchange stream identity is required"))?,
        path.clone(),
    )?;
    if stream.source().fragment_instance_id() == UniqueId::new(0, 0)
        || stream.destination_fragment_instance_id() == UniqueId::new(0, 0)
    {
        return Err(invalid(
            path,
            "exchange source and destination fragment instance IDs must be nonzero",
        ));
    }
    Ok(stream)
}

fn require_pair(
    stream: ExchangeStreamIdentity,
    pair: ExchangeProcessPair,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if ExchangeProcessPair::for_stream(stream) != pair {
        return Err(inconsistent(
            path,
            "exchange entry crosses its exact sender/receiver process pair",
        ));
    }
    Ok(())
}

fn require_nonempty_batch(length: usize, path: FieldPath) -> Result<(), ProtocolError> {
    if length == 0 {
        return Err(invalid(path, "exchange control batch cannot be empty"));
    }
    Ok(())
}

pub fn decode_delivery_request(
    value: wire::DeliverExchangeUnitRequest,
    path: FieldPath,
) -> Result<DecodedExchangeUnitFrame, ProtocolError> {
    let stream = checked_stream(value.stream.as_ref(), path.field("stream"))?;
    if stream.destination_fragment_instance_id()
        != UniqueId::new(value.finst_id_hi, value.finst_id_lo)
        || stream.destination_node().get() != value.node_id
        || stream.source().fragment_instance_id()
            != UniqueId::new(value.source_finst_id_hi, value.source_finst_id_lo)
        || stream.source().sender_ordinal() != value.sender_ordinal
    {
        return Err(inconsistent(
            path.clone(),
            "exchange unit envelope differs from its exact stream identity",
        ));
    }
    if value.sender_count == 0 || value.sender_ordinal >= value.sender_count {
        return Err(invalid(
            path.field("sender_count"),
            "exchange sender ordinal must be below a nonzero sender count",
        ));
    }
    let (item, payload) = match value
        .item
        .ok_or_else(|| missing(path.field("item"), "exchange unit item is required"))?
    {
        wire::deliver_exchange_unit_request::Item::Unit(unit) => {
            let demand = decode_unit_demand(
                &wire::ExchangeUnitDemand {
                    sequence: unit.sequence,
                    wire_bytes: unit.wire_bytes,
                },
                path.field("unit"),
            )?;
            if u64::try_from(unit.payload.len()).ok() != Some(demand.wire_bytes) {
                return Err(inconsistent(
                    path.field("unit").field("wire_bytes"),
                    "exchange unit wire length differs from its payload",
                ));
            }
            if unit.end_after_unit {
                demand.sequence.checked_next().map_err(|error| {
                    invalid(
                        path.field("unit").field("end_after_unit"),
                        error.to_string(),
                    )
                })?;
            }
            (
                ExchangeUnitItem::Unit {
                    demand,
                    end_after_unit: unit.end_after_unit,
                },
                unit.payload,
            )
        }
        wire::deliver_exchange_unit_request::Item::End(end) => (
            ExchangeUnitItem::End(decode_unit_sequence(
                end.sequence,
                path.field("end").field("sequence"),
            )?),
            Bytes::new(),
        ),
    };
    let next_demand = value
        .next_demand
        .as_ref()
        .map(|demand| decode_unit_demand(demand, path.field("next_demand")))
        .transpose()?;
    if let Some(next) = next_demand {
        match item {
            ExchangeUnitItem::Unit {
                demand,
                end_after_unit: false,
            } if next.sequence > demand.sequence => {}
            _ => {
                return Err(inconsistent(
                    path.field("next_demand"),
                    "piggybacked demand must name the next unit before End",
                ));
            }
        }
    }
    Ok(DecodedExchangeUnitFrame {
        stream,
        sender_count: value.sender_count,
        sender_id: value.sender_id,
        backend_number: value.be_number,
        item,
        payload,
        next_demand,
    })
}

pub fn encode_delivery_request(
    value: DecodedExchangeUnitFrame,
) -> wire::DeliverExchangeUnitRequest {
    let stream = value.stream;
    wire::DeliverExchangeUnitRequest {
        finst_id_hi: stream.destination_fragment_instance_id().high(),
        finst_id_lo: stream.destination_fragment_instance_id().low(),
        node_id: stream.destination_node().get(),
        sender_id: value.sender_id,
        be_number: value.backend_number,
        source_finst_id_hi: stream.source().fragment_instance_id().high(),
        source_finst_id_lo: stream.source().fragment_instance_id().low(),
        sender_ordinal: stream.source().sender_ordinal(),
        sender_count: value.sender_count,
        stream: Some(encode_stream(stream)),
        item: Some(match value.item {
            ExchangeUnitItem::Unit {
                demand,
                end_after_unit,
            } => wire::deliver_exchange_unit_request::Item::Unit(wire::ExchangeUnitData {
                sequence: demand.sequence.get(),
                wire_bytes: demand.wire_bytes,
                payload: value.payload,
                end_after_unit,
            }),
            ExchangeUnitItem::End(sequence) => {
                wire::deliver_exchange_unit_request::Item::End(wire::ExchangeUnitEnd {
                    sequence: sequence.get(),
                })
            }
        }),
        next_demand: value.next_demand.map(encode_unit_demand),
    }
}

pub fn decode_receiver_close(
    value: &wire::ExchangeUnitReceiverClosed,
    path: FieldPath,
) -> Result<ExchangeUnitCloseDisposition, ProtocolError> {
    match value.disposition.as_ref().ok_or_else(|| {
        missing(
            path.field("disposition"),
            "exchange receiver close requires a disposition",
        )
    })? {
        wire::exchange_unit_receiver_closed::Disposition::Consumed(consumed) => {
            Ok(ExchangeUnitCloseDisposition::Consumed {
                final_sequence: decode_unit_sequence(
                    consumed.final_sequence,
                    path.field("consumed").field("final_sequence"),
                )?,
            })
        }
        wire::exchange_unit_receiver_closed::Disposition::Withdrawn(_) => {
            Ok(ExchangeUnitCloseDisposition::Withdrawn)
        }
    }
}

pub fn encode_receiver_close(
    value: ExchangeUnitCloseDisposition,
) -> wire::ExchangeUnitReceiverClosed {
    wire::ExchangeUnitReceiverClosed {
        disposition: Some(match value {
            ExchangeUnitCloseDisposition::Consumed { final_sequence } => {
                wire::exchange_unit_receiver_closed::Disposition::Consumed(
                    wire::ExchangeUnitConsumed {
                        final_sequence: final_sequence.get(),
                    },
                )
            }
            ExchangeUnitCloseDisposition::Withdrawn => {
                wire::exchange_unit_receiver_closed::Disposition::Withdrawn(
                    wire::ExchangeWithdrawn {},
                )
            }
        }),
    }
}

pub fn decode_stream_state(
    value: &wire::ExchangeStreamState,
    path: FieldPath,
) -> Result<DecodedExchangeStreamState, ProtocolError> {
    let stream = checked_stream(value.stream.as_ref(), path.field("stream"))?;
    let receiver_closed = value
        .receiver_closed
        .as_ref()
        .map(|closed| decode_receiver_close(closed, path.field("receiver_closed")))
        .transpose()?;
    if value.custody_through > value.granted_through.saturating_add(1) {
        return Err(inconsistent(
            path.field("custody_through"),
            "exchange custody exceeds its granted data prefix and optional End",
        ));
    }
    if let Some(ExchangeUnitCloseDisposition::Consumed { final_sequence }) = receiver_closed
        && (final_sequence.get() != value.custody_through
            || value.granted_through.checked_add(1) != Some(final_sequence.get()))
    {
        return Err(inconsistent(
            path.field("receiver_closed"),
            "exchange consumed End differs from the custody or granted data prefix",
        ));
    }
    Ok(DecodedExchangeStreamState {
        stream,
        facts: ExchangeStreamFacts {
            custody_through: value.custody_through,
            granted_through: value.granted_through,
            sender_stopped: value.sender_stopped,
            receiver_closed,
        },
    })
}

pub fn encode_stream_state(value: DecodedExchangeStreamState) -> wire::ExchangeStreamState {
    wire::ExchangeStreamState {
        stream: Some(encode_stream(value.stream)),
        custody_through: value.facts.custody_through,
        granted_through: value.facts.granted_through,
        sender_stopped: value.facts.sender_stopped,
        receiver_closed: value.facts.receiver_closed.map(encode_receiver_close),
    }
}

pub fn decode_delivery_response(
    value: &wire::DeliverExchangeUnitResponse,
    expected_stream: ExchangeStreamIdentity,
    expected_sequence: ExchangeUnitSequence,
    path: FieldPath,
) -> Result<DecodedExchangeDeliveryOutcome, ProtocolError> {
    if decode_unit_sequence(value.item_sequence, path.field("item_sequence"))? != expected_sequence
    {
        return Err(inconsistent(
            path.field("item_sequence"),
            "exchange response differs from the requested item sequence",
        ));
    }
    match value.outcome.as_ref().ok_or_else(|| {
        missing(
            path.field("outcome"),
            "exchange delivery outcome is required",
        )
    })? {
        wire::deliver_exchange_unit_response::Outcome::Observed(observation) => {
            let verdict = match wire::ExchangeUnitVerdict::try_from(observation.verdict) {
                Ok(wire::ExchangeUnitVerdict::Applied) => ExchangeAdmissionVerdict::Applied,
                Ok(wire::ExchangeUnitVerdict::Idempotent) => ExchangeAdmissionVerdict::Idempotent,
                Ok(wire::ExchangeUnitVerdict::Unspecified) | Err(_) => {
                    return Err(invalid_enum(
                        path.field("observed").field("verdict"),
                        "exchange unit verdict must be specified and known",
                    ));
                }
            };
            let state = decode_stream_state(
                observation.state.as_ref().ok_or_else(|| {
                    missing(
                        path.field("observed").field("state"),
                        "exchange observation requires cumulative stream facts",
                    )
                })?,
                path.field("observed").field("state"),
            )?;
            if state.stream != expected_stream {
                return Err(inconsistent(
                    path.field("observed").field("state").field("stream"),
                    "exchange response differs from its requested exact stream",
                ));
            }
            Ok(DecodedExchangeDeliveryOutcome::Observed {
                verdict,
                facts: state.facts,
            })
        }
        wire::deliver_exchange_unit_response::Outcome::Refused(failure) => {
            Ok(DecodedExchangeDeliveryOutcome::Refused(
                decode_failure_fact(failure, path.field("refused"))?,
            ))
        }
    }
}

pub fn encode_delivery_response(
    stream: ExchangeStreamIdentity,
    sequence: ExchangeUnitSequence,
    outcome: DecodedExchangeDeliveryOutcome,
) -> wire::DeliverExchangeUnitResponse {
    wire::DeliverExchangeUnitResponse {
        item_sequence: sequence.get(),
        outcome: Some(match outcome {
            DecodedExchangeDeliveryOutcome::Observed { verdict, facts } => {
                wire::deliver_exchange_unit_response::Outcome::Observed(
                    wire::ExchangeUnitObservation {
                        verdict: match verdict {
                            ExchangeAdmissionVerdict::Applied => {
                                wire::ExchangeUnitVerdict::Applied as i32
                            }
                            ExchangeAdmissionVerdict::Idempotent => {
                                wire::ExchangeUnitVerdict::Idempotent as i32
                            }
                        },
                        state: Some(encode_stream_state(DecodedExchangeStreamState {
                            stream,
                            facts,
                        })),
                    },
                )
            }
            DecodedExchangeDeliveryOutcome::Refused(fact) => {
                wire::deliver_exchange_unit_response::Outcome::Refused(encode_failure_fact(fact))
            }
        }),
    }
}

pub fn decode_demand_request(
    value: &wire::RegisterExchangeDemandRequest,
    path: FieldPath,
) -> Result<DecodedExchangeDemandBatch, ProtocolError> {
    require_nonempty_batch(value.entries.len(), path.field("entries"))?;
    let mut pair = None;
    let mut seen = BTreeSet::new();
    let mut entries = Vec::with_capacity(value.entries.len());
    for (index, entry) in value.entries.iter().enumerate() {
        let entry_path = path.field("entries").index(index);
        let stream = checked_stream(entry.stream.as_ref(), entry_path.field("stream"))?;
        require_pair(
            stream,
            *pair.get_or_insert_with(|| ExchangeProcessPair::for_stream(stream)),
            entry_path.field("stream"),
        )?;
        if !seen.insert(stream) {
            return Err(duplicate(
                entry_path.field("stream"),
                "exchange stream repeats in demand batch",
            ));
        }
        let operation = match entry.operation.as_ref().ok_or_else(|| {
            missing(
                entry_path.field("operation"),
                "exchange demand operation is required",
            )
        })? {
            wire::exchange_demand_entry::Operation::Demand(demand) => {
                ExchangeDemandOperation::Demand(decode_unit_demand(
                    demand,
                    entry_path.field("demand"),
                )?)
            }
            wire::exchange_demand_entry::Operation::StopSender(_) => {
                ExchangeDemandOperation::StopSender
            }
        };
        entries.push(DecodedExchangeDemandEntry { stream, operation });
    }
    Ok(DecodedExchangeDemandBatch {
        pair: pair.ok_or_else(|| invalid(path, "exchange demand batch cannot be empty"))?,
        entries,
    })
}

pub fn encode_demand_request(
    entries: &[DecodedExchangeDemandEntry],
    path: FieldPath,
) -> Result<wire::RegisterExchangeDemandRequest, ProtocolError> {
    let value = wire::RegisterExchangeDemandRequest {
        entries: entries
            .iter()
            .map(|entry| wire::ExchangeDemandEntry {
                stream: Some(encode_stream(entry.stream)),
                operation: Some(match entry.operation {
                    ExchangeDemandOperation::Demand(demand) => {
                        wire::exchange_demand_entry::Operation::Demand(encode_unit_demand(demand))
                    }
                    ExchangeDemandOperation::StopSender => {
                        wire::exchange_demand_entry::Operation::StopSender(
                            wire::StopExchangeSender {},
                        )
                    }
                }),
            })
            .collect(),
    };
    decode_demand_request(&value, path)?;
    Ok(value)
}

pub fn decode_demand_response(
    value: &wire::RegisterExchangeDemandResponse,
    expected_streams: &[ExchangeStreamIdentity],
    path: FieldPath,
) -> Result<Vec<DecodedExchangeDemandResult>, ProtocolError> {
    require_nonempty_batch(value.results.len(), path.field("results"))?;
    if value.results.len() != expected_streams.len() {
        return Err(inconsistent(
            path.field("results"),
            "exchange demand response does not cover its request",
        ));
    }
    let expected = expected_streams.iter().copied().collect::<BTreeSet<_>>();
    if expected.len() != expected_streams.len() {
        return Err(duplicate(
            path.clone(),
            "expected demand streams must be unique",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut decoded = Vec::with_capacity(value.results.len());
    for (index, result) in value.results.iter().enumerate() {
        let result_path = path.field("results").index(index);
        let stream = checked_stream(result.stream.as_ref(), result_path.field("stream"))?;
        if !expected.contains(&stream) {
            return Err(inconsistent(
                result_path.field("stream"),
                "exchange demand response names an unrequested stream",
            ));
        }
        if !seen.insert(stream) {
            return Err(duplicate(
                result_path.field("stream"),
                "exchange stream repeats in demand response",
            ));
        }
        let outcome = match result.outcome.as_ref().ok_or_else(|| {
            missing(
                result_path.field("outcome"),
                "exchange demand result is required",
            )
        })? {
            wire::exchange_demand_result::Outcome::State(state) => {
                let state = decode_stream_state(state, result_path.field("state"))?;
                if state.stream != stream {
                    return Err(inconsistent(
                        result_path.field("state").field("stream"),
                        "exchange demand state differs from its result identity",
                    ));
                }
                Ok(state.facts)
            }
            wire::exchange_demand_result::Outcome::Refused(failure) => {
                Err(decode_failure_fact(failure, result_path.field("refused"))?)
            }
        };
        decoded.push(DecodedExchangeDemandResult { stream, outcome });
    }
    Ok(decoded)
}

pub fn encode_demand_response(
    values: &[DecodedExchangeDemandResult],
) -> wire::RegisterExchangeDemandResponse {
    wire::RegisterExchangeDemandResponse {
        results: values
            .iter()
            .map(|value| wire::ExchangeDemandResult {
                stream: Some(encode_stream(value.stream)),
                outcome: Some(match value.outcome {
                    Ok(facts) => wire::exchange_demand_result::Outcome::State(encode_stream_state(
                        DecodedExchangeStreamState {
                            stream: value.stream,
                            facts,
                        },
                    )),
                    Err(failure) => {
                        wire::exchange_demand_result::Outcome::Refused(encode_failure_fact(failure))
                    }
                }),
            })
            .collect(),
    }
}

fn decode_backend(
    value: Option<&wire::BackendProcessId>,
    path: FieldPath,
) -> Result<BackendProcessId, ProtocolError> {
    let value =
        value.ok_or_else(|| missing(path.clone(), "exchange process identity is required"))?;
    let bytes = value.value.as_slice().try_into().map_err(|_| {
        invalid(
            path.field("value"),
            "process identity must contain exactly 16 bytes",
        )
    })?;
    BackendProcessId::try_from_bytes(bytes)
        .map_err(|error| invalid(path.field("value"), error.to_string()))
}

pub fn decode_grant_subscription(
    value: &wire::SubscribeExchangeGrantsRequest,
    path: FieldPath,
) -> Result<DecodedExchangeGrantSubscription, ProtocolError> {
    Ok(DecodedExchangeGrantSubscription {
        pair: ExchangeProcessPair {
            sender: decode_backend(value.sender.as_ref(), path.field("sender"))?,
            receiver: decode_backend(value.receiver.as_ref(), path.field("receiver"))?,
        },
        generation: NonZeroU64::new(value.generation).ok_or_else(|| {
            invalid(
                path.field("generation"),
                "exchange subscription generation must be nonzero",
            )
        })?,
    })
}

pub fn encode_grant_subscription(
    value: DecodedExchangeGrantSubscription,
) -> wire::SubscribeExchangeGrantsRequest {
    wire::SubscribeExchangeGrantsRequest {
        sender: Some(wire::BackendProcessId {
            value: value.pair.sender.to_bytes().to_vec(),
        }),
        receiver: Some(wire::BackendProcessId {
            value: value.pair.receiver.to_bytes().to_vec(),
        }),
        generation: value.generation.get(),
    }
}

pub fn decode_grant_batch(
    value: &wire::ExchangeGrantBatch,
    expected: DecodedExchangeGrantSubscription,
    path: FieldPath,
) -> Result<DecodedExchangeGrantBatch, ProtocolError> {
    if value.generation != expected.generation.get() {
        return Err(inconsistent(
            path.field("generation"),
            "exchange grant batch differs from its subscription generation",
        ));
    }
    if value.snapshot_complete && !value.snapshot {
        return Err(inconsistent(
            path.field("snapshot_complete"),
            "snapshot completion requires a snapshot batch",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut states = Vec::with_capacity(value.states.len());
    for (index, state) in value.states.iter().enumerate() {
        let state_path = path.field("states").index(index);
        let state = decode_stream_state(state, state_path.clone())?;
        require_pair(state.stream, expected.pair, state_path.field("stream"))?;
        if !seen.insert(state.stream) {
            return Err(duplicate(
                state_path.field("stream"),
                "exchange stream repeats in grant batch",
            ));
        }
        states.push(state);
    }
    Ok(DecodedExchangeGrantBatch {
        generation: expected.generation,
        snapshot: value.snapshot,
        snapshot_complete: value.snapshot_complete,
        states,
    })
}

pub fn encode_grant_batch(
    value: &DecodedExchangeGrantBatch,
    pair: ExchangeProcessPair,
    path: FieldPath,
) -> Result<wire::ExchangeGrantBatch, ProtocolError> {
    let encoded = wire::ExchangeGrantBatch {
        generation: value.generation.get(),
        snapshot: value.snapshot,
        snapshot_complete: value.snapshot_complete,
        states: value
            .states
            .iter()
            .copied()
            .map(encode_stream_state)
            .collect(),
    };
    decode_grant_batch(
        &encoded,
        DecodedExchangeGrantSubscription {
            pair,
            generation: value.generation,
        },
        path,
    )?;
    Ok(encoded)
}

#[cfg(test)]
#[path = "exchange_unit_codec_tests.rs"]
mod tests;
