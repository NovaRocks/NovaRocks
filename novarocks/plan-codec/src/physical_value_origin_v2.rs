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

//! Complete ValueOrigin projection through the sealed payload namespace.
//! References are preserved, not resolved as Fragment semantics. Provider
//! purpose/binding/install checks remain mandatory at that public owner.

use crate::physical_connector_payload_v2::{
    ConnectorPayloadCodecError, DecodedConnectorPayloads, EncodedConnectorPayloads,
    bytes_shared_upper,
};
use novarocks_physical_plan as physical;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, CompilePhase};
use std::{fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct ValueOriginProjectionLimits {
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueOriginProjectionFacts {
    pub reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum ValueOriginCodecError {
    Control(CompileControlError),
    Payload(ConnectorPayloadCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for ValueOriginCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ConnectorPayloadCodecError> for ValueOriginCodecError {
    fn from(error: ConnectorPayloadCodecError) -> Self {
        match error {
            ConnectorPayloadCodecError::Control(cause) => Self::Control(cause),
            error => Self::Payload(error),
        }
    }
}
impl fmt::Display for ValueOriginCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ValueOriginCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Payload(source) => Some(source),
            _ => None,
        }
    }
}
type Error = ValueOriginCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("value origin resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("value origin resource product overflow"))
}
fn cap(value: usize, max: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let admitted = value <= max;
    work.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid("value origin projection envelope exceeded"))
    }
}
fn floor(source: usize, known: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let admitted = source >= known;
    work.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid(
            "value origin source invoice omits original namespace backing",
        ))
    }
}
fn finish<T>(result: Result<T, Error>, work: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn work_bound(sources: usize, potential_clone: bool) -> Result<usize, Error> {
    // A retained-floor scan observes O(1) header/capacity/Layout facts per
    // source. Encoding also scans every original pointer to prove UNIQUE
    // association; receiving uses the same index's binary lookup. 128*N
    // covers both plus fixed conversion/gates and ID inspection. No provider
    // payload bytes are scanned. The sole Bytes model covers the optional
    // Shared allocation during clone; Arc header clones only change counters.
    add(
        512,
        add(
            mul(sources, 128)?,
            if potential_clone {
                mul(bytes_shared_upper()?, 4)?
            } else {
                0
            },
        )?,
    )
}
type OriginAdmit<'a> =
    dyn FnMut(&ValueOriginProjectionFacts) -> Result<(), CompileControlError> + 'a;

// Plain keeps the historical standalone trace; Admitted borrows the parent's
// scope and exposes the same numerical author before its completed callbacks.
struct Admission<'a, 'callback> {
    parent: Option<&'a mut OriginAdmit<'callback>>,
}
impl Admission<'_, '_> {
    fn numeric<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        match result {
            Err(Error::InvalidShape(_)) if self.parent.is_some() => {
                Err(CompileControlError::ResourceExhausted.into())
            }
            result => result,
        }
    }
    fn gate(
        &mut self,
        references: usize,
        source: usize,
        clone_bytes: usize,
        bound: usize,
        limits: ValueOriginProjectionLimits,
    ) -> Result<(), Error> {
        if self.parent.is_none() {
            return Ok(());
        }
        let facts = self.numeric(snapshot(references, source, clone_bytes, bound))?;
        if facts.allocation_requests_upper_bound > limits.max_allocation_requests
            || facts.allocation_request_bytes_upper_bound > limits.max_allocation_request_bytes
            || facts.coexisting_source_and_request_bytes_upper_bound
                > limits.max_coexisting_source_and_request_bytes
            || facts.cumulative_work_upper_bound > limits.max_work
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        if let Some(parent) = &mut self.parent {
            parent(&facts)?;
        }
        Ok(())
    }
    fn floor(
        &self,
        source: usize,
        known: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        if self.parent.is_some() && source < known {
            return Err(invalid(
                "value origin source invoice omits original namespace backing",
            ));
        }
        floor(source, known, work)
    }
}
fn snapshot(
    references: usize,
    source: usize,
    clone_bytes: usize,
    bound: usize,
) -> Result<ValueOriginProjectionFacts, Error> {
    Ok(ValueOriginProjectionFacts {
        reference_count: references,
        allocation_requests_upper_bound: usize::from(clone_bytes != 0),
        allocation_request_bytes_upper_bound: clone_bytes,
        coexisting_source_and_request_bytes_upper_bound: add(source, clone_bytes)?,
        cumulative_work_upper_bound: bound,
    })
}
fn facts(
    references: usize,
    source: usize,
    clone_bytes: usize,
    work_bound: usize,
    limits: ValueOriginProjectionLimits,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueOriginProjectionFacts, Error> {
    let result = admission.numeric(snapshot(references, source, clone_bytes, work_bound))?;
    if admission.parent.is_some() {
        admission.gate(references, source, clone_bytes, work_bound, limits)?;
    } else {
        cap(
            result.allocation_requests_upper_bound,
            limits.max_allocation_requests,
            work,
        )?;
        cap(clone_bytes, limits.max_allocation_request_bytes, work)?;
        cap(
            result.coexisting_source_and_request_bytes_upper_bound,
            limits.max_coexisting_source_and_request_bytes,
            work,
        )?;
    }
    Ok(result)
}
fn same_control(
    expected: &dyn novarocks_type_contract::PureCompileControl,
    work: &CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if std::ptr::addr_eq(expected, work.control()) {
        Ok(())
    } else {
        Err(invalid(
            "value origin caller work uses a different namespace control",
        ))
    }
}
fn required(id: Option<u32>) -> Result<u32, Error> {
    id.ok_or_else(|| invalid("value origin required reference is absent"))
}
pub(crate) fn encode_phase(phase: physical::AggregatePhase) -> wire::AggregatePhase {
    let kind = match phase {
        physical::AggregatePhase::Single => wire::aggregate_phase::Kind::Single(Empty {}),
        physical::AggregatePhase::Partial { sequence } => {
            wire::aggregate_phase::Kind::PartialSequenceId(sequence.get())
        }
        physical::AggregatePhase::Intermediate { sequence } => {
            wire::aggregate_phase::Kind::IntermediateSequenceId(sequence.get())
        }
        physical::AggregatePhase::Final { sequence } => {
            wire::aggregate_phase::Kind::FinalSequenceId(sequence.get())
        }
    };
    wire::AggregatePhase { kind: Some(kind) }
}
pub(crate) fn decode_phase(
    phase: &wire::AggregatePhase,
) -> Result<physical::AggregatePhase, Error> {
    decode_phase_with(phase, invalid)
}
/// Share the exact closed phase grammar without coupling callers' error graphs.
pub(crate) fn decode_phase_with<E>(
    phase: &wire::AggregatePhase,
    missing: impl FnOnce(&'static str) -> E,
) -> Result<physical::AggregatePhase, E> {
    Ok(
        match phase
            .kind
            .as_ref()
            .ok_or_else(|| missing("value origin aggregate phase kind is absent"))?
        {
            wire::aggregate_phase::Kind::Single(_) => physical::AggregatePhase::Single,
            wire::aggregate_phase::Kind::PartialSequenceId(sequence) => {
                physical::AggregatePhase::Partial {
                    sequence: physical::AggregateSequenceId::new(*sequence),
                }
            }
            wire::aggregate_phase::Kind::IntermediateSequenceId(sequence) => {
                physical::AggregatePhase::Intermediate {
                    sequence: physical::AggregateSequenceId::new(*sequence),
                }
            }
            wire::aggregate_phase::Kind::FinalSequenceId(sequence) => {
                physical::AggregatePhase::Final {
                    sequence: physical::AggregateSequenceId::new(*sequence),
                }
            }
        },
    )
}
fn encode_writer(kind: physical::WriterDerivedKind) -> i32 {
    (match kind {
        physical::WriterDerivedKind::RelationKind => wire::WriterDerivedKind::RelationKind,
        physical::WriterDerivedKind::AffectedRows => wire::WriterDerivedKind::AffectedRows,
        physical::WriterDerivedKind::CommitFragment => wire::WriterDerivedKind::CommitFragment,
        physical::WriterDerivedKind::ChangeEvent => wire::WriterDerivedKind::ChangeEvent,
        physical::WriterDerivedKind::RelationAuxiliary => {
            wire::WriterDerivedKind::RelationAuxiliary
        }
        physical::WriterDerivedKind::WriteTargetOrdinal => {
            wire::WriterDerivedKind::WriteTargetOrdinal
        }
        physical::WriterDerivedKind::GroupingKey => wire::WriterDerivedKind::GroupingKey,
    }) as i32
}
fn decode_writer(kind: i32) -> Result<physical::WriterDerivedKind, Error> {
    Ok(match wire::WriterDerivedKind::try_from(kind) {
        Ok(wire::WriterDerivedKind::RelationKind) => physical::WriterDerivedKind::RelationKind,
        Ok(wire::WriterDerivedKind::AffectedRows) => physical::WriterDerivedKind::AffectedRows,
        Ok(wire::WriterDerivedKind::CommitFragment) => physical::WriterDerivedKind::CommitFragment,
        Ok(wire::WriterDerivedKind::ChangeEvent) => physical::WriterDerivedKind::ChangeEvent,
        Ok(wire::WriterDerivedKind::RelationAuxiliary) => {
            physical::WriterDerivedKind::RelationAuxiliary
        }
        Ok(wire::WriterDerivedKind::WriteTargetOrdinal) => {
            physical::WriterDerivedKind::WriteTargetOrdinal
        }
        Ok(wire::WriterDerivedKind::GroupingKey) => physical::WriterDerivedKind::GroupingKey,
        Ok(wire::WriterDerivedKind::Unspecified) | Err(_) => {
            return Err(invalid(
                "value origin writer kind is unknown or unspecified",
            ));
        }
    })
}
fn phase_refs(phase: physical::AggregatePhase) -> usize {
    match phase {
        physical::AggregatePhase::Single => 1,
        physical::AggregatePhase::Partial { .. }
        | physical::AggregatePhase::Intermediate { .. }
        | physical::AggregatePhase::Final { .. } => 2,
    }
}

/// Preserve all nine variants from the actual typed source. ProviderField
/// requires its exact original payload owner in this sealed namespace. Aliased
/// owners emitted under multiple IDs are explicitly ambiguous until a future
/// full-package sealed-reference author binds them; no first lookup is used.
pub fn encode_value_origin(
    source: &physical::ValueOrigin,
    payloads: &EncodedConnectorPayloads<'_, '_>,
    source_retained_bytes: usize,
    limits: ValueOriginProjectionLimits,
) -> Result<(wire::ValueOrigin, ValueOriginProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(payloads.original_control(), CompilePhase::Encode)?;
    let result = encode_observed(source, payloads, source_retained_bytes, limits, &mut work);
    finish(result, work)
}
pub(crate) fn encode_observed(
    source: &physical::ValueOrigin,
    payloads: &EncodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ValueOrigin, ValueOriginProjectionFacts), Error> {
    encode_core(
        source,
        payloads,
        source_bytes,
        limits,
        &mut Admission { parent: None },
        work,
    )
}

pub(crate) fn encode_admitted(
    source: &physical::ValueOrigin,
    payloads: &EncodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    admit: &mut OriginAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ValueOrigin, ValueOriginProjectionFacts), Error> {
    same_control(payloads.original_control(), work)?;
    encode_core(
        source,
        payloads,
        source_bytes,
        limits,
        &mut Admission {
            parent: Some(admit),
        },
        work,
    )
}
fn encode_core(
    source: &physical::ValueOrigin,
    payloads: &EncodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ValueOrigin, ValueOriginProjectionFacts), Error> {
    let bound = admission.numeric(work_bound(payloads.source_count(), false))?;
    admission.gate(0, source_bytes, 0, bound, limits)?;
    if admission.parent.is_some() {
        let known = payloads.retained_floor_header_admitted()?;
        if source_bytes < known {
            return Err(invalid(
                "value origin source invoice omits original namespace backing",
            ));
        }
    }
    if admission.parent.is_none() {
        cap(bound, limits.max_work, work)?;
    }
    admission.floor(source_bytes, size_of::<physical::ValueOrigin>(), work)?;
    let retained = payloads.retained_floor_observed(work)?;
    // The original source can itself own the payload lent to this namespace:
    // summing the root and prior source invoice would double-count that owner.
    // This is a lower floor; the caller's complete coexistence invoice remains
    // mandatory and includes both source roots when independently retained.
    admission.floor(
        source_bytes,
        retained.max(size_of::<physical::ValueOrigin>()),
        work,
    )?;
    let (kind, references) = match source {
        physical::ValueOrigin::JoinMark { node } => {
            (wire::value_origin::Kind::JoinMarkNodeId(node.get()), 1)
        }
        physical::ValueOrigin::Summary { field } => (
            wire::value_origin::Kind::Summary(crate::physical_summary_v2::encode_field(*field)),
            0,
        ),
        physical::ValueOrigin::ProviderField { scan_node, field } => {
            let id = payloads.source_id_observed(&field.column_payload, work)?;
            (
                wire::value_origin::Kind::ProviderField(wire::ProviderFieldOrigin {
                    scan_node_id: Some(scan_node.get()),
                    column_payload_id: Some(id),
                }),
                2,
            )
        }
        physical::ValueOrigin::Expr { node, expr } => (
            wire::value_origin::Kind::Expression(wire::ExpressionOrigin {
                node_id: Some(node.get()),
                expr_id: Some(expr.get()),
            }),
            2,
        ),
        physical::ValueOrigin::NullExtended { node, of } => (
            wire::value_origin::Kind::NullExtended(wire::NullExtendedOrigin {
                node_id: Some(node.get()),
                original_value_id: Some(of.get()),
            }),
            2,
        ),
        physical::ValueOrigin::AggregateState { call, phase } => (
            wire::value_origin::Kind::AggregateState(wire::AggregateStateOrigin {
                call_id: Some(call.get()),
                phase: Some(encode_phase(*phase)),
            }),
            phase_refs(*phase),
        ),
        physical::ValueOrigin::AggregateResult { call } => (
            wire::value_origin::Kind::AggregateResultCallId(call.get()),
            1,
        ),
        physical::ValueOrigin::NodeOutput {
            node,
            output_ordinal,
        } => (
            wire::value_origin::Kind::NodeOutput(wire::NodeOutputOrigin {
                node_id: Some(node.get()),
                output_ordinal: *output_ordinal,
            }),
            1,
        ),
        physical::ValueOrigin::ExchangeImport { edge, source_value } => (
            wire::value_origin::Kind::ExchangeImport(wire::ExchangeImportOrigin {
                edge_id: Some(edge.get()),
                source_value_id: Some(source_value.get()),
            }),
            2,
        ),
        physical::ValueOrigin::CteImport {
            edge,
            producer_fragment,
            producer_value,
        } => (
            wire::value_origin::Kind::CteImport(wire::CteImportOrigin {
                edge_id: Some(edge.get()),
                producer_fragment_id: Some(producer_fragment.get()),
                producer_value_id: Some(producer_value.get()),
            }),
            3,
        ),
        physical::ValueOrigin::WriterDerived { writer_node, kind } => (
            wire::value_origin::Kind::WriterDerived(wire::WriterDerivedOrigin {
                writer_node_id: Some(writer_node.get()),
                kind: encode_writer(*kind),
            }),
            1,
        ),
    };
    admission.gate(references, source_bytes, 0, bound, limits)?;
    work.step()?;
    let facts = facts(references, source_bytes, 0, bound, limits, admission, work)?;
    Ok((wire::ValueOrigin { kind: Some(kind) }, facts))
}

/// The sole encoder constructs only inline DTO variants; this borrowed pass
/// validates all source associations and returns the same numerical author
/// without requesting storage. A composing namespace counts both passes.
pub(crate) fn preflight_encode_observed(
    source: &physical::ValueOrigin,
    payloads: &EncodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueOriginProjectionFacts, Error> {
    encode_observed(source, payloads, source_bytes, limits, work).map(|(_, facts)| facts)
}

/// Validate with the original grammar and growing caller admission, without
/// creating a control scope or requesting provider backing.
pub(crate) fn preflight_encode_admitted(
    source: &physical::ValueOrigin,
    payloads: &EncodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    admit: &mut OriginAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueOriginProjectionFacts, Error> {
    encode_admitted(source, payloads, source_bytes, limits, admit, work).map(|(_, facts)| facts)
}

/// Materialize the complete typed origin with the original namespace control.
/// ProviderField clones only the actual neutral payload selected by its ID;
/// later Fragment validation still checks purpose, binding and references.
pub fn decode_value_origin(
    source: &wire::ValueOrigin,
    payloads: &DecodedConnectorPayloads<'_, '_>,
    source_retained_bytes: usize,
    limits: ValueOriginProjectionLimits,
) -> Result<(physical::ValueOrigin, ValueOriginProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(payloads.original_control(), CompilePhase::Decode)?;
    let result = decode_observed(source, payloads, source_retained_bytes, limits, &mut work);
    finish(result, work)
}
enum DecodedOriginDecision<'a> {
    Inline(physical::ValueOrigin),
    Provider {
        scan_node: physical::NodeId,
        payload: &'a novarocks_connector_contract::ConnectorEncodedPayload,
    },
}

// All presence, vocabulary and reference validation lives in this one
// borrowed decision author. Only its Provider arm requires a later clone.
fn decode_decision<'a>(
    source: &wire::ValueOrigin,
    payloads: &'a DecodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(DecodedOriginDecision<'a>, ValueOriginProjectionFacts), Error> {
    let kind = source.kind.as_ref();
    let early_bound = if admission.parent.is_some() {
        let provider = matches!(kind, Some(wire::value_origin::Kind::ProviderField(_)));
        let bound = admission.numeric(work_bound(payloads.source_count(), provider))?;
        admission.gate(0, source_bytes, 0, bound, limits)?;
        let known = payloads.retained_floor_header_admitted()?;
        if source_bytes < known {
            return Err(invalid(
                "value origin source invoice omits original namespace backing",
            ));
        }
        Some(bound)
    } else {
        None
    };
    work.step()?;
    let kind = kind.ok_or_else(|| invalid("value origin kind is absent"))?;
    let provider = matches!(kind, wire::value_origin::Kind::ProviderField(_));
    let bound = match early_bound {
        Some(bound) => bound,
        None => work_bound(payloads.source_count(), provider)?,
    };
    if admission.parent.is_none() {
        cap(bound, limits.max_work, work)?;
    }
    admission.floor(source_bytes, size_of::<wire::ValueOrigin>(), work)?;
    let retained = payloads.retained_floor_observed(work)?;
    admission.floor(
        source_bytes,
        retained.max(size_of::<wire::ValueOrigin>()),
        work,
    )?;
    if let wire::value_origin::Kind::ProviderField(field) = kind {
        let scan_node = physical::NodeId::new(required(field.scan_node_id)?);
        let id = required(field.column_payload_id)?;
        work.step()?;
        let payload = payloads.payload_observed(id, work)?;
        let payload =
            payload.ok_or_else(|| invalid("value origin provider column payload ID is unknown"))?;
        let clone_bytes = if payload.payload().is_empty() {
            0
        } else {
            bytes_shared_upper()?
        };
        admission.gate(2, source_bytes, clone_bytes, bound, limits)?;
        work.step()?;
        let facts = facts(2, source_bytes, clone_bytes, bound, limits, admission, work)?;
        return Ok((
            DecodedOriginDecision::Provider { scan_node, payload },
            facts,
        ));
    }
    let (origin, references) = match kind {
        wire::value_origin::Kind::JoinMarkNodeId(node) => (
            physical::ValueOrigin::JoinMark {
                node: physical::NodeId::new(*node),
            },
            1,
        ),
        wire::value_origin::Kind::ProviderField(_) => {
            return Err(invalid("value origin provider branch is inconsistent"));
        }
        wire::value_origin::Kind::Expression(value) => (
            physical::ValueOrigin::Expr {
                node: physical::NodeId::new(required(value.node_id)?),
                expr: physical::ExprId::new(required(value.expr_id)?),
            },
            2,
        ),
        wire::value_origin::Kind::NullExtended(value) => (
            physical::ValueOrigin::NullExtended {
                node: physical::NodeId::new(required(value.node_id)?),
                of: physical::ValueId::new(required(value.original_value_id)?),
            },
            2,
        ),
        wire::value_origin::Kind::AggregateState(value) => {
            let call = physical::AggregateCallId::new(required(value.call_id)?);
            let phase = decode_phase(
                value
                    .phase
                    .as_ref()
                    .ok_or_else(|| invalid("value origin aggregate phase is absent"))?,
            )?;
            (
                physical::ValueOrigin::AggregateState { call, phase },
                phase_refs(phase),
            )
        }
        wire::value_origin::Kind::Summary(field) => (
            physical::ValueOrigin::Summary {
                field: crate::physical_summary_v2::decode_field(field).map_err(invalid)?,
            },
            0,
        ),
        wire::value_origin::Kind::AggregateResultCallId(call) => (
            physical::ValueOrigin::AggregateResult {
                call: physical::AggregateCallId::new(*call),
            },
            1,
        ),
        wire::value_origin::Kind::NodeOutput(value) => (
            physical::ValueOrigin::NodeOutput {
                node: physical::NodeId::new(required(value.node_id)?),
                output_ordinal: value.output_ordinal,
            },
            1,
        ),
        wire::value_origin::Kind::ExchangeImport(value) => (
            physical::ValueOrigin::ExchangeImport {
                edge: physical::EdgeId::new(required(value.edge_id)?),
                source_value: physical::ValueId::new(required(value.source_value_id)?),
            },
            2,
        ),
        wire::value_origin::Kind::CteImport(value) => (
            physical::ValueOrigin::CteImport {
                edge: physical::EdgeId::new(required(value.edge_id)?),
                producer_fragment: physical::FragmentId::new(required(value.producer_fragment_id)?),
                producer_value: physical::ValueId::new(required(value.producer_value_id)?),
            },
            3,
        ),
        wire::value_origin::Kind::WriterDerived(value) => (
            physical::ValueOrigin::WriterDerived {
                writer_node: physical::NodeId::new(required(value.writer_node_id)?),
                kind: decode_writer(value.kind)?,
            },
            1,
        ),
    };
    admission.gate(references, source_bytes, 0, bound, limits)?;
    work.step()?;
    let facts = facts(references, source_bytes, 0, bound, limits, admission, work)?;
    Ok((DecodedOriginDecision::Inline(origin), facts))
}

/// Validate and price the receiving origin without cloning a provider payload.
/// The composing namespace admits all requests before invoking emission.
pub(crate) fn preflight_decode_observed(
    source: &wire::ValueOrigin,
    payloads: &DecodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueOriginProjectionFacts, Error> {
    decode_decision(
        source,
        payloads,
        source_bytes,
        limits,
        &mut Admission { parent: None },
        work,
    )
    .map(|(_, facts)| facts)
}

pub(crate) fn preflight_decode_admitted(
    source: &wire::ValueOrigin,
    payloads: &DecodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    admit: &mut OriginAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValueOriginProjectionFacts, Error> {
    same_control(payloads.original_control(), work)?;
    decode_decision(
        source,
        payloads,
        source_bytes,
        limits,
        &mut Admission {
            parent: Some(admit),
        },
        work,
    )
    .map(|(_, facts)| facts)
}

pub(crate) fn decode_observed(
    source: &wire::ValueOrigin,
    payloads: &DecodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(physical::ValueOrigin, ValueOriginProjectionFacts), Error> {
    decode_core(
        source,
        payloads,
        source_bytes,
        limits,
        &mut Admission { parent: None },
        work,
    )
}
pub(crate) fn decode_admitted(
    source: &wire::ValueOrigin,
    payloads: &DecodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    admit: &mut OriginAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(physical::ValueOrigin, ValueOriginProjectionFacts), Error> {
    same_control(payloads.original_control(), work)?;
    decode_core(
        source,
        payloads,
        source_bytes,
        limits,
        &mut Admission {
            parent: Some(admit),
        },
        work,
    )
}
fn decode_core(
    source: &wire::ValueOrigin,
    payloads: &DecodedConnectorPayloads<'_, '_>,
    source_bytes: usize,
    limits: ValueOriginProjectionLimits,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(physical::ValueOrigin, ValueOriginProjectionFacts), Error> {
    let (decision, facts) =
        decode_decision(source, payloads, source_bytes, limits, admission, work)?;
    let origin = match decision {
        DecodedOriginDecision::Inline(origin) => origin,
        DecodedOriginDecision::Provider { scan_node, payload } => {
            // Locked Bytes.clone may allocate one Shared for a promotable
            // buffer. Both this single-origin gate and the namespace's total
            // gate precede it. Header Arc clones only change reference counts.
            work.flush()?;
            let column_payload = payload.clone();
            work.flush()?;
            physical::ValueOrigin::ProviderField {
                scan_node,
                field: physical::ProviderColumnReference { column_payload },
            }
        }
    };
    Ok((origin, facts))
}

#[cfg(test)]
mod tests;
