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

//! Full neutral read-reference namespace through two sealed original sources.
//! Purpose, provider-private interpretation and Fragment closure remain at their
//! actual checked owners. The caller invoice covers ALL coexisting sources;
//! namespace floors below are only alias-safe necessary lower bounds.

use crate::{
    allocation_exit_v2::reserve_exit,
    binding_index_v2::BindingIndex,
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::{
        ConnectorPayloadCodecError, DecodedConnectorPayloads, EncodedConnectorPayloads,
        arc_u8_slice_bytes, arc_u8_slice_bytes_for_mode, bytes_shared_upper_for_mode,
    },
    physical_provider_binding_v2::{
        DecodedProviderBindings, EncodedProviderBindings, ProviderBindingCodecError,
    },
};
use novarocks_connector_contract::{
    ConnectorError, ConnectorReadInputVersion, ConnectorReadRelationKind,
    ConnectorReadRelationPayload,
};
use novarocks_physical_plan::ProviderReadReference;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub struct ProviderReadProjectionLimits {
    pub max_definitions: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderReadProjectionFacts {
    pub definition_count: usize,
    /// Cumulative version bytes retained in resource and work accounting.
    pub input_version_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum ProviderReadCodecError {
    Control(CompileControlError),
    Binding(ProviderBindingCodecError),
    Payload(ConnectorPayloadCodecError),
    Index(BindingCodecError),
    Contract(ConnectorError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for ProviderReadCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ProviderBindingCodecError> for ProviderReadCodecError {
    fn from(e: ProviderBindingCodecError) -> Self {
        match e {
            ProviderBindingCodecError::Control(c) => Self::Control(c),
            e => Self::Binding(e),
        }
    }
}
impl From<ConnectorPayloadCodecError> for ProviderReadCodecError {
    fn from(e: ConnectorPayloadCodecError) -> Self {
        match e {
            ConnectorPayloadCodecError::Control(c) => Self::Control(c),
            e => Self::Payload(e),
        }
    }
}
impl From<BindingCodecError> for ProviderReadCodecError {
    fn from(e: BindingCodecError) -> Self {
        match e {
            BindingCodecError::Control(c) => Self::Control(c),
            e => Self::Index(e),
        }
    }
}
impl fmt::Display for ProviderReadCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::Contract(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ProviderReadCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::Payload(source) => Some(source),
            Self::Index(source) => Some(source),
            Self::Contract(source) => Some(source),
            _ => None,
        }
    }
}
type Error = ProviderReadCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("provider read resource sum overflow"))
}
#[cfg(test)]
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("provider read resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|v| v.size())
        .map_err(|_| invalid("provider read allocation layout is unrepresentable"))
}
type Admit<'a> = dyn FnMut(&ProviderReadProjectionFacts) -> Result<(), CompileControlError> + 'a;
fn numerical<T>(value: Option<T>, observed: bool, message: &'static str) -> Result<T, Error> {
    value.ok_or_else(|| {
        if observed {
            Error::Control(CompileControlError::ResourceExhausted)
        } else {
            invalid(message)
        }
    })
}
fn numerical_bytes<T>(n: usize, observed: bool) -> Result<usize, Error> {
    numerical(
        Layout::array::<T>(n).ok().map(|layout| layout.size()),
        observed,
        "provider read allocation layout is unrepresentable",
    )
}
fn check_control(
    original: &dyn PureCompileControl,
    work: &CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if std::ptr::addr_eq(original, work.control()) {
        Ok(())
    } else {
        Err(invalid("provider read control loan differs"))
    }
}
fn cap(n: usize, max: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let allowed = n <= max;
    work.step()?;
    if allowed {
        Ok(())
    } else {
        Err(invalid("provider read projection envelope exceeded"))
    }
}
fn floor(source: usize, known: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let allowed = source >= known;
    work.step()?;
    if allowed {
        Ok(())
    } else {
        Err(invalid(
            "provider read source invoice omits original backing",
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
fn reserve<T>(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    work.flush()?;
    let mut out = Vec::new();
    let outcome = out.try_reserve_exact(n);
    reserve_exit::<Error>(outcome, work)?;
    Ok(out)
}
fn copy(input: &[u8], work: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    let mut out = reserve(input.len(), work)?;
    for chunk in input.chunks(1024) {
        out.extend_from_slice(chunk);
        work.step()?;
    }
    Ok(out)
}
fn same_control(
    left: &dyn PureCompileControl,
    right: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let same = std::ptr::addr_eq(left, right);
    work.step()?;
    if same {
        Ok(())
    } else {
        Err(invalid(
            "provider read namespaces have different original controls",
        ))
    }
}
pub(crate) const fn encode_relation_kind(
    kind: ConnectorReadRelationKind,
) -> wire::ConnectorRelationKind {
    match kind {
        ConnectorReadRelationKind::Table => wire::ConnectorRelationKind::Table,
        ConnectorReadRelationKind::TableFunction => wire::ConnectorRelationKind::TableFunction,
        ConnectorReadRelationKind::ChangeWindow => wire::ConnectorRelationKind::ChangeWindow,
        ConnectorReadRelationKind::SystemTable => wire::ConnectorRelationKind::SystemTable,
        ConnectorReadRelationKind::TableExecute => wire::ConnectorRelationKind::TableExecute,
        ConnectorReadRelationKind::MergeTable => wire::ConnectorRelationKind::MergeTable,
    }
}
pub(crate) fn decode_relation_kind(kind: i32) -> Result<ConnectorReadRelationKind, Error> {
    Ok(
        match wire::ConnectorRelationKind::try_from(kind)
            .map_err(|_| invalid("unknown connector relation kind"))?
        {
            wire::ConnectorRelationKind::Table => ConnectorReadRelationKind::Table,
            wire::ConnectorRelationKind::TableFunction => ConnectorReadRelationKind::TableFunction,
            wire::ConnectorRelationKind::ChangeWindow => ConnectorReadRelationKind::ChangeWindow,
            wire::ConnectorRelationKind::SystemTable => ConnectorReadRelationKind::SystemTable,
            wire::ConnectorRelationKind::TableExecute => ConnectorReadRelationKind::TableExecute,
            wire::ConnectorRelationKind::MergeTable => ConnectorReadRelationKind::MergeTable,
            wire::ConnectorRelationKind::Unspecified => {
                return Err(invalid("unspecified connector relation kind"));
            }
        },
    )
}
fn required(value: Option<u32>, work: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    work.step()?;
    value.ok_or_else(|| invalid("provider read reference ID is absent"))
}
#[derive(Default)]
struct Requests {
    observed: bool,
    count: usize,
    bytes: usize,
}
impl Requests {
    fn record(&mut self, n: usize) -> Result<(), Error> {
        if n != 0 {
            self.count = numerical(
                self.count.checked_add(1),
                self.observed,
                "provider read resource sum overflow",
            )?;
            self.bytes = numerical(
                self.bytes.checked_add(n),
                self.observed,
                "provider read resource sum overflow",
            )?;
        }
        Ok(())
    }
}
fn bound(
    n: usize,
    bindings: usize,
    payloads: usize,
    input: usize,
    requests: &Requests,
) -> Result<usize, Error> {
    let add = |a: usize, b: usize| {
        numerical(
            a.checked_add(b),
            requests.observed,
            "provider read resource sum overflow",
        )
    };
    let mul = |a: usize, b: usize| {
        numerical(
            a.checked_mul(b),
            requests.observed,
            "provider read resource product overflow",
        )
    };
    let height = (usize::BITS - n.leading_zeros()) as usize;
    // Index sort is <=4 operations/heap level. Encoding visits every source
    // once in preflight and again while emitting to preserve pointer identity;
    // decoding does two binary lookups per payload and binding. The linear
    // source-count bound covers both, plus each prior floor walk ONCE. Native
    // Arc/Bytes/constructor work is included numerically and bracketed, not
    // claimed to cooperate inside their opaque library calls.
    let namespaces = add(bindings, mul(payloads, 2)?)?;
    add(
        512,
        add(
            mul(
                n,
                add(128, add(mul(height + 1, 16)?, mul(namespaces, 4)?)?)?,
            )?,
            add(
                mul(add(bindings, payloads)?, 16)?,
                add(
                    mul(input, 4)?,
                    add(mul(requests.bytes, 4)?, requests.count)?,
                )?,
            )?,
        )?,
    )
}
fn numerical_facts(
    n: usize,
    input: usize,
    requests: &Requests,
    source: usize,
    namespace_counts: (usize, usize),
) -> Result<ProviderReadProjectionFacts, Error> {
    let (bindings, payloads) = namespace_counts;
    let bound = bound(n, bindings, payloads, input, requests)?;
    let facts = ProviderReadProjectionFacts {
        definition_count: n,
        input_version_bytes: input,
        allocation_requests_upper_bound: requests.count,
        allocation_request_bytes_upper_bound: requests.bytes,
        coexisting_source_and_request_bytes_upper_bound: numerical(
            source.checked_add(requests.bytes),
            requests.observed,
            "provider read resource sum overflow",
        )?,
        cumulative_work_upper_bound: bound,
    };
    Ok(facts)
}
fn prefix(
    n: usize,
    input: usize,
    requests: &Requests,
    envelope: (usize, (usize, usize), ProviderReadProjectionLimits),
    admit: &mut Option<&mut Admit<'_>>,
) -> Result<ProviderReadProjectionFacts, Error> {
    let result = numerical_facts(n, input, requests, envelope.0, envelope.1)?;
    if let Some(callback) = admit.as_mut() {
        let limits = envelope.2;
        if result.definition_count > limits.max_definitions
            || result.allocation_requests_upper_bound > limits.max_allocation_requests
            || result.allocation_request_bytes_upper_bound > limits.max_allocation_request_bytes
            || result.coexisting_source_and_request_bytes_upper_bound
                > limits.max_coexisting_source_and_request_bytes
            || result.cumulative_work_upper_bound > limits.max_work
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        callback(&result)?;
    }
    Ok(result)
}
fn facts(
    n: usize,
    input: usize,
    requests: Requests,
    envelope: (usize, (usize, usize), ProviderReadProjectionLimits),
    admit: &mut Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProviderReadProjectionFacts, Error> {
    let result = prefix(n, input, &requests, envelope, admit)?;
    let limits = envelope.2;
    cap(
        result.allocation_requests_upper_bound,
        limits.max_allocation_requests,
        work,
    )?;
    cap(
        result.allocation_request_bytes_upper_bound,
        limits.max_allocation_request_bytes,
        work,
    )?;
    cap(
        result.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        work,
    )?;
    cap(result.cumulative_work_upper_bound, limits.max_work, work)?;
    Ok(result)
}
fn prior_work(
    n: usize,
    bindings: usize,
    payloads: usize,
    limits: ProviderReadProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    // Admit floor/source-ID/lookup scans before any potentially long walk. The
    // complete request/copy bound is gated separately before first reserve.
    cap(
        bound(n, bindings, payloads, 0, &Requests::default())?,
        limits.max_work,
        work,
    )
}

pub struct EncodedProviderReads<'source, 'control> {
    inputs: &'source [(u32, &'source ProviderReadReference)],
    bindings: &'source EncodedProviderBindings<'source, 'control>,
    payloads: &'source EncodedConnectorPayloads<'source, 'control>,
    wire: Vec<wire::ProviderReadReferenceDefinition>,
    indices: BindingIndex,
    facts: ProviderReadProjectionFacts,
    original_source_bytes: usize,
}
impl<'source, 'control> EncodedProviderReads<'source, 'control> {
    pub fn as_wire(&self) -> &[wire::ProviderReadReferenceDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ProviderReadReferenceDefinition> {
        self.wire
    }
    pub fn facts(&self) -> &ProviderReadProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.inputs.len()
    }
    pub fn bindings(&self) -> &EncodedProviderBindings<'source, 'control> {
        self.bindings
    }
    pub fn payloads(&self) -> &EncodedConnectorPayloads<'source, 'control> {
        self.payloads
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.bindings.original_control()
    }
    pub fn read(&self, id: u32) -> Result<Option<&'source ProviderReadReference>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.read_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn read_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ProviderReadReference>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.inputs[at].0, work)?
            .map(|at| self.inputs[at].1))
    }
    pub fn source_id(&self, source: &ProviderReadReference) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.source_id_observed(source, &mut work);
        finish(result, work)
    }
    pub(crate) fn source_id_observed(
        &self,
        source: &ProviderReadReference,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let mut found = None;
        for (id, read) in self.inputs {
            let same = std::ptr::eq(*read, source);
            work.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("provider read source association is ambiguous"));
                }
                found = Some(*id);
            }
        }
        found.ok_or_else(|| invalid("provider read source owner is not in this namespace"))
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(result, work)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut n = add(self.original_source_bytes, size_of::<Self>())?;
        n = add(n, self.indices.backing_bytes()?)?;
        n = add(
            n,
            bytes::<wire::ProviderReadReferenceDefinition>(self.wire.capacity())?,
        )?;
        work.step()?;
        for def in &self.wire {
            n = add(n, def.input_version.capacity())?;
            work.step()?;
        }
        Ok(n)
    }
    /// Borrow the same original namespace and caller scope; no entry/footer.
    pub fn read_in(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ProviderReadReference>, Error> {
        check_control(self.original_control(), work)?;
        self.read_observed(id, work)
    }
    pub fn retained_invoice_floor_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        check_control(self.original_control(), work)?;
        self.retained_floor_observed(work)
    }
    pub fn source_id_in(
        &self,
        source: &ProviderReadReference,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        check_control(self.original_control(), work)?;
        self.source_id_observed(source, work)
    }
}
pub struct DecodedProviderReads<'wire, 'control> {
    wire: &'wire [wire::ProviderReadReferenceDefinition],
    bindings: &'wire DecodedProviderBindings<'wire, 'control>,
    payloads: &'wire DecodedConnectorPayloads<'wire, 'control>,
    reads: Vec<ProviderReadReference>,
    indices: BindingIndex,
    facts: ProviderReadProjectionFacts,
    original_source_bytes: usize,
}
impl<'wire, 'control> DecodedProviderReads<'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::ProviderReadReferenceDefinition] {
        self.wire
    }
    pub fn facts(&self) -> &ProviderReadProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.wire.len()
    }
    pub fn bindings(&self) -> &DecodedProviderBindings<'wire, 'control> {
        self.bindings
    }
    pub fn payloads(&self) -> &DecodedConnectorPayloads<'wire, 'control> {
        self.payloads
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.bindings.original_control()
    }
    pub fn read(&self, id: u32) -> Result<Option<&ProviderReadReference>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.read_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn read_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ProviderReadReference>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.wire[at].id, work)?
            .map(|at| &self.reads[at]))
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(result, work)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut n = add(self.original_source_bytes, size_of::<Self>())?;
        n = add(n, self.indices.backing_bytes()?)?;
        n = add(n, bytes::<ProviderReadReference>(self.reads.capacity())?)?;
        work.step()?;
        for read in &self.reads {
            n = add(n, arc_u8_slice_bytes(read.input_version.as_bytes().len())?)?;
            work.step()?;
        }
        // Binding/payload backing aliases the prior namespaces already included
        // in the caller invoice. Potential new Bytes Shared blocks are request
        // upper bounds, not observable retained lower floors.
        Ok(n)
    }
    /// Borrow the same original namespace and caller scope; no entry/footer.
    pub fn read_in(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ProviderReadReference>, Error> {
        check_control(self.original_control(), work)?;
        self.read_observed(id, work)
    }
    pub fn retained_invoice_floor_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        check_control(self.original_control(), work)?;
        self.retained_floor_observed(work)
    }
}

pub fn encode_provider_reads<'source, 'control>(
    inputs: &'source [(u32, &'source ProviderReadReference)],
    bindings: &'source EncodedProviderBindings<'source, 'control>,
    payloads: &'source EncodedConnectorPayloads<'source, 'control>,
    source_retained_bytes: usize,
    limits: ProviderReadProjectionLimits,
) -> Result<EncodedProviderReads<'source, 'control>, Error> {
    let control = bindings.original_control();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_core(
        inputs,
        bindings,
        payloads,
        (source_retained_bytes, limits),
        None,
        &mut work,
    );
    finish(result, work)
}
fn encode_core<'source, 'control>(
    inputs: &'source [(u32, &'source ProviderReadReference)],
    bindings: &'source EncodedProviderBindings<'source, 'control>,
    payloads: &'source EncodedConnectorPayloads<'source, 'control>,
    envelope: (usize, ProviderReadProjectionLimits),
    mut admit: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedProviderReads<'source, 'control>, Error> {
    let (source, limits) = envelope;
    let observed = admit.is_some();
    let add = |a: usize, b: usize| {
        numerical(
            a.checked_add(b),
            observed,
            "provider read resource sum overflow",
        )
    };
    let envelope = (
        source,
        (bindings.source_count(), payloads.source_count()),
        limits,
    );
    let mut req = Requests {
        observed,
        ..Requests::default()
    };
    if observed {
        req.record(numerical_bytes::<usize>(inputs.len(), true)?)?;
        req.record(numerical_bytes::<wire::ProviderReadReferenceDefinition>(
            inputs.len(),
            true,
        )?)?;
        prefix(inputs.len(), 0, &req, envelope, &mut admit)?;
    }

    same_control(
        bindings.original_control(),
        payloads.original_control(),
        work,
    )?;
    cap(inputs.len(), limits.max_definitions, work)?;
    prior_work(
        inputs.len(),
        bindings.source_count(),
        payloads.source_count(),
        limits,
        work,
    )?;
    let known = bindings
        .retained_floor_observed(work)?
        .max(payloads.retained_floor_observed(work)?);
    floor(source, known, work)?;
    let roots = bytes::<(u32, &ProviderReadReference)>(inputs.len())?;
    floor(source, roots, work)?;

    if !observed {
        req.record(bytes::<usize>(inputs.len())?)?;
        req.record(bytes::<wire::ProviderReadReferenceDefinition>(
            inputs.len(),
        )?)?;
    }
    let mut input = 0;
    let mut individual = 0;
    for (_, read) in inputs {
        let n = read.input_version.as_bytes().len();
        input = add(input, n)?;
        individual = individual.max(add(
            size_of::<ProviderReadReference>(),
            arc_u8_slice_bytes_for_mode(n, observed)?,
        )?);
        req.record(numerical_bytes::<u8>(n, observed)?)?;
        if observed {
            prefix(inputs.len(), input, &req, envelope, &mut admit)?;
        }
        work.step()?;
        bindings.source_id_observed(&read.binding, work)?;
        payloads.source_id_observed(read.relation.table(), work)?;
        payloads.source_id_observed(read.relation.view(), work)?;
    }
    floor(source, add(roots, individual)?, work)?;
    let facts = facts(inputs.len(), input, req, envelope, &mut admit, work)?;
    let indices = BindingIndex::prepare(inputs.len(), |at| inputs[at].0, work)?;
    let mut output = reserve(inputs.len(), work)?;
    for (id, read) in inputs {
        output.push(wire::ProviderReadReferenceDefinition {
            id: *id,
            provider_binding_id: Some(bindings.source_id_observed(&read.binding, work)?),
            input_version: copy(read.input_version.as_bytes(), work)?,
            kind: encode_relation_kind(read.relation.kind()) as i32,
            table_payload_id: Some(payloads.source_id_observed(read.relation.table(), work)?),
            view_payload_id: Some(payloads.source_id_observed(read.relation.view(), work)?),
        });
        work.step()?;
    }
    Ok(EncodedProviderReads {
        inputs,
        bindings,
        payloads,
        wire: output,
        indices,
        facts,
        original_source_bytes: source,
    })
}
type PayloadCapture<'a> = dyn FnMut(
        &novarocks_connector_contract::ConnectorEncodedPayload,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error>
    + 'a;
fn decode_refs<'a>(
    def: &wire::ProviderReadReferenceDefinition,
    bindings: &'a DecodedProviderBindings<'_, '_>,
    payloads: &'a DecodedConnectorPayloads<'_, '_>,
    mut capture: Option<&mut PayloadCapture<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        &'a novarocks_connector_contract::ConnectorReadBinding,
        &'a novarocks_connector_contract::ConnectorEncodedPayload,
        &'a novarocks_connector_contract::ConnectorEncodedPayload,
    ),
    Error,
> {
    let id = required(def.provider_binding_id, work)?;
    let binding = bindings
        .binding_observed(id, work)?
        .ok_or_else(|| invalid("provider read binding ID is unknown"))?;
    let id = required(def.table_payload_id, work)?;
    let table = if let Some(callback) = capture.as_mut() {
        payloads.payload_captured_in(id, &mut **callback, work)?
    } else {
        payloads.payload_observed(id, work)?
    }
    .ok_or_else(|| invalid("provider read table payload ID is unknown"))?;
    let id = required(def.view_payload_id, work)?;
    let view = if let Some(callback) = capture.as_mut() {
        payloads.payload_captured_in(id, &mut **callback, work)?
    } else {
        payloads.payload_observed(id, work)?
    }
    .ok_or_else(|| invalid("provider read view payload ID is unknown"))?;
    Ok((binding, table, view))
}
pub fn decode_provider_reads<'wire, 'control>(
    definitions: &'wire [wire::ProviderReadReferenceDefinition],
    bindings: &'wire DecodedProviderBindings<'wire, 'control>,
    payloads: &'wire DecodedConnectorPayloads<'wire, 'control>,
    source_retained_bytes: usize,
    limits: ProviderReadProjectionLimits,
) -> Result<DecodedProviderReads<'wire, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(bindings.original_control(), CompilePhase::Decode)?;
    let result = decode_core(
        definitions,
        bindings,
        payloads,
        (source_retained_bytes, limits),
        None,
        &mut work,
    );
    finish(result, work)
}
fn decode_core<'wire, 'control>(
    definitions: &'wire [wire::ProviderReadReferenceDefinition],
    bindings: &'wire DecodedProviderBindings<'wire, 'control>,
    payloads: &'wire DecodedConnectorPayloads<'wire, 'control>,
    envelope: (usize, ProviderReadProjectionLimits),
    mut admit: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedProviderReads<'wire, 'control>, Error> {
    let (source, limits) = envelope;
    let observed = admit.is_some();
    let add = |a: usize, b: usize| {
        numerical(
            a.checked_add(b),
            observed,
            "provider read resource sum overflow",
        )
    };
    let envelope = (
        source,
        (bindings.source_count(), payloads.source_count()),
        limits,
    );
    let mut req = Requests {
        observed,
        ..Requests::default()
    };
    if observed {
        req.record(numerical_bytes::<usize>(definitions.len(), true)?)?;
        req.record(numerical_bytes::<ProviderReadReference>(
            definitions.len(),
            true,
        )?)?;
        if !definitions.is_empty() {
            req.record(numerical_bytes::<u8>(
                ConnectorReadInputVersion::invalid_length_diagnostic().len(),
                true,
            )?)?;
        }
        prefix(definitions.len(), 0, &req, envelope, &mut admit)?;
    }

    same_control(
        bindings.original_control(),
        payloads.original_control(),
        work,
    )?;
    cap(definitions.len(), limits.max_definitions, work)?;
    prior_work(
        definitions.len(),
        bindings.source_count(),
        payloads.source_count(),
        limits,
        work,
    )?;
    let known = bindings
        .retained_floor_observed(work)?
        .max(payloads.retained_floor_observed(work)?);
    floor(source, known, work)?;
    let mut known = bytes::<wire::ProviderReadReferenceDefinition>(definitions.len())?;
    floor(source, known, work)?;

    if !observed {
        req.record(bytes::<usize>(definitions.len())?)?;
        req.record(bytes::<ProviderReadReference>(definitions.len())?)?;
    }
    let mut input = 0;
    for def in definitions {
        if observed {
            input = add(input, def.input_version.len())?;
            known = add(known, def.input_version.capacity())?;
            req.record(arc_u8_slice_bytes_for_mode(
                def.input_version.len(),
                observed,
            )?)?;
            prefix(definitions.len(), input, &req, envelope, &mut admit)?;
        }
        let kind = decode_relation_kind(def.kind);
        work.step()?;
        kind?;
        if observed {
            let mut capture = |payload: &novarocks_connector_contract::ConnectorEncodedPayload,
                               _: &mut CompileCheckpoints<'_>| {
                if !payload.payload().is_empty() {
                    req.record(bytes_shared_upper_for_mode(observed)?)?;
                }
                prefix(definitions.len(), input, &req, envelope, &mut admit)?;
                Ok(())
            };
            decode_refs(def, bindings, payloads, Some(&mut capture), work)?;
            // Original table/view completed observations remain after the
            // successful lookup captures, without charging its requests again.
            work.step()?;
            work.step()?;
        } else {
            let (_, table, view) = decode_refs(def, bindings, payloads, None, work)?;
            input = add(input, def.input_version.len())?;
            known = add(known, def.input_version.capacity())?;
            req.record(arc_u8_slice_bytes_for_mode(
                def.input_version.len(),
                observed,
            )?)?;
            for payload in [table, view] {
                if !payload.payload().is_empty() {
                    req.record(bytes_shared_upper_for_mode(observed)?)?;
                }
                work.step()?;
            }
        }
        work.step()?;
    }
    // Sole InputVersion::try_new converts to Arc THEN validates. Its one
    // possible error is ConnectorError::new with this exact owned String;
    // cleanup_context is None and no other fields allocate (read_facts.rs).
    // Reserve ONE possible terminal diagnostic, not one per successful input.
    if !observed && !definitions.is_empty() {
        req.record(bytes::<u8>(
            ConnectorReadInputVersion::invalid_length_diagnostic().len(),
        )?)?;
    }
    floor(source, known, work)?;
    let facts = facts(definitions.len(), input, req, envelope, &mut admit, work)?;
    let indices = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    let mut output = reserve(definitions.len(), work)?;
    for def in definitions {
        let kind = decode_relation_kind(def.kind)?;
        let (binding, table, view) = decode_refs(def, bindings, payloads, None, work)?;
        work.flush()?;
        let version =
            ConnectorReadInputVersion::try_new(Arc::<[u8]>::from(def.input_version.as_slice()));
        work.flush()?;
        let version = version.map_err(Error::Contract)?;
        work.flush()?;
        let read = ProviderReadReference {
            binding: binding.clone(),
            input_version: version,
            relation: ConnectorReadRelationPayload::new(kind, table.clone(), view.clone()),
        };
        work.flush()?;
        output.push(read);
        work.step()?;
    }
    Ok(DecodedProviderReads {
        wire: definitions,
        bindings,
        payloads,
        reads: output,
        indices,
        facts,
        original_source_bytes: source,
    })
}

/// Same original reference mapper on one borrowed scope, with synchronous
/// cumulative admission. The exact read/payload/type owners remain borrowed.
pub fn encode_provider_reads_in<'source, 'control>(
    inputs: &'source [(u32, &'source ProviderReadReference)],
    bindings: &'source EncodedProviderBindings<'source, 'control>,
    payloads: &'source EncodedConnectorPayloads<'source, 'control>,
    source: usize,
    limits: ProviderReadProjectionLimits,
    admit: &mut impl FnMut(&ProviderReadProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<EncodedProviderReads<'source, 'control>, Error> {
    check_control(bindings.original_control(), work)?;
    encode_core(
        inputs,
        bindings,
        payloads,
        (source, limits),
        Some(admit),
        work,
    )
}
pub fn decode_provider_reads_in<'wire, 'control>(
    definitions: &'wire [wire::ProviderReadReferenceDefinition],
    bindings: &'wire DecodedProviderBindings<'wire, 'control>,
    payloads: &'wire DecodedConnectorPayloads<'wire, 'control>,
    source: usize,
    limits: ProviderReadProjectionLimits,
    admit: &mut impl FnMut(&ProviderReadProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<DecodedProviderReads<'wire, 'control>, Error> {
    check_control(bindings.original_control(), work)?;
    decode_core(
        definitions,
        bindings,
        payloads,
        (source, limits),
        Some(admit),
        work,
    )
}

#[cfg(test)]
mod tests;
