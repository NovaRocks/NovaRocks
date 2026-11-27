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

//! Shared complete node envelope resource and observed namespace author.
//! Kind semantics and Fragment/Package closure remain with their original owners.

use crate::{
    allocation_exit_v2::reserve_exit,
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::ConnectorPayloadCodecError,
    physical_expression_v2::ExpressionCodecError,
    physical_properties_v2::{
        self as properties, PhysicalPropertyCodecError, PhysicalPropertyProjectionFacts,
        PhysicalPropertyProjectionLimits,
    },
    physical_relation_v2::RelationCodecError,
    physical_type_v2::TypeCodecError,
    physical_value_v2::{DecodedValues, EncodedValues, ValueCodecError},
};
use novarocks_connector_contract::ConnectorIdentityError;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeProjectionLimits {
    pub max_input_nodes: usize,
    pub max_value_references: usize,
    pub max_list_items: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
    pub properties: PhysicalPropertyProjectionLimits,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeProjectionFacts {
    pub input_node_count: usize,
    pub value_reference_count: usize,
    pub list_item_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum NodeCodecError {
    Control(CompileControlError),
    Properties(PhysicalPropertyCodecError),
    Type(TypeCodecError),
    Value(ValueCodecError),
    Expression(ExpressionCodecError),
    Binding(BindingCodecError),
    Relation(RelationCodecError),
    Payload(ConnectorPayloadCodecError),
    Constant(p::ConstantReferenceError),
    Identity(ConnectorIdentityError),
    Root(novarocks_proto_codec::ProtocolError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for NodeCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<PhysicalPropertyCodecError> for NodeCodecError {
    fn from(error: PhysicalPropertyCodecError) -> Self {
        match error {
            PhysicalPropertyCodecError::Control(cause) => Self::Control(cause),
            error => Self::Properties(error),
        }
    }
}
impl From<TypeCodecError> for NodeCodecError {
    fn from(error: TypeCodecError) -> Self {
        match error {
            TypeCodecError::Control(cause) => Self::Control(cause),
            error => Self::Type(error),
        }
    }
}
impl From<ValueCodecError> for NodeCodecError {
    fn from(error: ValueCodecError) -> Self {
        match error {
            ValueCodecError::Control(cause) => Self::Control(cause),
            error => Self::Value(error),
        }
    }
}
impl From<ExpressionCodecError> for NodeCodecError {
    fn from(error: ExpressionCodecError) -> Self {
        match error {
            ExpressionCodecError::Control(cause) => Self::Control(cause),
            error => Self::Expression(error),
        }
    }
}
impl From<BindingCodecError> for NodeCodecError {
    fn from(error: BindingCodecError) -> Self {
        match error {
            BindingCodecError::Control(cause) => Self::Control(cause),
            error => Self::Binding(error),
        }
    }
}
impl From<RelationCodecError> for NodeCodecError {
    fn from(error: RelationCodecError) -> Self {
        match error {
            RelationCodecError::Control(cause) => Self::Control(cause),
            error => Self::Relation(error),
        }
    }
}
impl From<ConnectorPayloadCodecError> for NodeCodecError {
    fn from(error: ConnectorPayloadCodecError) -> Self {
        match error {
            ConnectorPayloadCodecError::Control(cause) => Self::Control(cause),
            error => Self::Payload(error),
        }
    }
}
impl From<p::ConstantReferenceError> for NodeCodecError {
    fn from(error: p::ConstantReferenceError) -> Self {
        use novarocks_constant_contract::ConstantError;
        match error {
            p::ConstantReferenceError::Control(cause)
            | p::ConstantReferenceError::Constant(ConstantError::Control(cause)) => {
                Self::Control(cause)
            }
            p::ConstantReferenceError::Constant(ConstantError::Limit(_)) => {
                Self::Control(CompileControlError::ResourceExhausted)
            }
            error => Self::Constant(error),
        }
    }
}
impl fmt::Display for NodeCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Properties(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Value(e) => e.fmt(f),
            Self::Expression(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Relation(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Constant(e) => e.fmt(f),
            Self::Identity(e) => e.fmt(f),
            Self::Root(error) => error.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for NodeCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Properties(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Value(source) => Some(source),
            Self::Expression(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::Relation(source) => Some(source),
            Self::Payload(source) => Some(source),
            Self::Constant(source) => Some(source),
            Self::Identity(source) => Some(source),
            Self::Root(source) => Some(source),
            _ => None,
        }
    }
}
type Error = NodeCodecError;
pub(crate) fn invalid(text: &'static str) -> Error {
    Error::InvalidShape(text)
}
pub(crate) fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}
pub(crate) fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|layout| layout.size())
        .map_err(|_| CompileControlError::ResourceExhausted.into())
}
pub(crate) fn cap(
    n: usize,
    maximum: impl Into<Option<usize>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let Some(maximum) = maximum.into() else {
        return Ok(());
    };
    // A known numerical refusal is already the originating control cause;
    // no later checkpoint or ordinary footer may replace it.
    check_cap(n, maximum)?;
    w.step()?;
    Ok(())
}
pub(crate) fn check_cap(n: usize, maximum: usize) -> Result<(), Error> {
    if n > maximum {
        Err(CompileControlError::ResourceExhausted.into())
    } else {
        Ok(())
    }
}
pub(crate) fn floor(
    invoice: usize,
    known: usize,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let accepted = invoice >= known;
    w.step()?;
    if accepted {
        Ok(())
    } else {
        Err(invalid("Repeat source invoice omits original backing"))
    }
}
pub(crate) fn finish<T>(w: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
pub(crate) fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut output = Vec::new();
    let result = output.try_reserve_exact(n);
    reserve_exit::<Error>(result, w)?;
    Ok(output)
}
pub(crate) fn boxed<T>(input: Vec<T>, w: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, Error> {
    w.flush()?;
    let output = input.into_boxed_slice();
    w.flush()?;
    Ok(output)
}

pub(crate) trait Values {
    fn count(&self) -> usize;
    fn retained_floor_header(&self) -> Result<usize, Error>;
    fn retained_floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error>;
    fn contains(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<bool, Error>;
    fn value_captured<'a>(
        &'a self,
        id: u32,
        capture: &mut impl FnMut(&'a p::ValueDef, &mut CompileCheckpoints<'_>) -> Result<(), Error>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a p::ValueDef>, Error>;
}
impl Values for EncodedValues<'_, '_, '_> {
    fn count(&self) -> usize {
        self.source_count()
    }
    fn retained_floor_header(&self) -> Result<usize, Error> {
        Ok(self.retained_floor_header_admitted()?)
    }
    fn retained_floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error> {
        Ok(self.retained_floor_observed(w)?)
    }
    fn contains(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<bool, Error> {
        Ok(self.value_observed(id, w)?.is_some())
    }
    fn value_captured<'a>(
        &'a self,
        id: u32,
        capture: &mut impl FnMut(&'a p::ValueDef, &mut CompileCheckpoints<'_>) -> Result<(), Error>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a p::ValueDef>, Error> {
        EncodedValues::value_captured(self, id, &mut |value, work| capture(value, work), w)
    }
}
impl Values for DecodedValues<'_, '_, '_> {
    fn count(&self) -> usize {
        self.source_count()
    }
    fn retained_floor_header(&self) -> Result<usize, Error> {
        Ok(self.retained_floor_header_admitted()?)
    }
    fn retained_floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error> {
        Ok(self.retained_floor_observed(w)?)
    }
    fn contains(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<bool, Error> {
        Ok(self.value_observed(id, w)?.is_some())
    }
    fn value_captured<'a>(
        &'a self,
        id: u32,
        capture: &mut impl FnMut(&'a p::ValueDef, &mut CompileCheckpoints<'_>) -> Result<(), Error>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a p::ValueDef>, Error> {
        DecodedValues::value_captured(self, id, capture, w)
    }
}
pub(crate) fn reference(
    id: u32,
    values: &impl Values,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = values.contains(id, w)?;
    w.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "Repeat value reference is not in the original namespace",
        ))
    }
}
pub(crate) fn physical_property_refs(
    input: &p::PhysicalProperties,
    values: &impl Values,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    for id in input.distribution.value_references() {
        reference(id.get(), values, w)?;
    }
    for key in &input.ordering {
        reference(key.value.get(), values, w)?;
    }
    Ok(())
}
pub(crate) fn wire_property_refs(
    input: &wire::PhysicalProperties,
    values: &impl Values,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if let Some(kind) = input.distribution.as_ref().and_then(|v| v.kind.as_ref()) {
        let keys = match kind {
            wire::distribution::Kind::Hash(v) => &v.key_value_ids[..],
            wire::distribution::Kind::ConditionalKey(v) => {
                reference(
                    v.applies_value_id
                        .ok_or_else(|| invalid("conditional partition applicability is absent"))?,
                    values,
                    w,
                )?;
                &v.key_value_ids[..]
            }
            wire::distribution::Kind::BucketShuffle(v) => &v.key_value_ids[..],
            _ => &[],
        };
        for id in keys {
            reference(*id, values, w)?;
        }
    }
    for key in &input.ordering {
        let id = key
            .value_id
            .ok_or_else(|| invalid("Repeat property ordering value is absent"))?;
        reference(id, values, w)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Model {
    pub(crate) inputs: usize,
    pub(crate) refs: usize,
    pub(crate) items: usize,
    pub(crate) requests: usize,
    pub(crate) requested: usize,
    pub(crate) delegated_work: usize,
}
/// Admit the containing package's current node contribution synchronously.
/// This hook borrows its original author; it creates no checkpoint scope.
pub(crate) type NodeAdmit<'a> =
    dyn FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError> + 'a;
impl Model {
    /// An actual nonzero request Layout supplied by its sole source author.
    /// Arc slice headers remain requests even when their payload is empty.
    pub(crate) fn layout_request(&mut self, layout: Layout, copies: usize) -> Result<(), Error> {
        self.requested = add(self.requested, mul(layout.size(), copies)?)?;
        if layout.size() != 0 {
            self.requests = add(self.requests, copies)?;
        }
        Ok(())
    }
    pub(crate) fn request<T>(&mut self, n: usize, copies: usize) -> Result<(), Error> {
        self.requested = add(self.requested, mul(bytes::<T>(n)?, copies)?)?;
        if n != 0 {
            self.requests = add(self.requests, copies)?;
        }
        Ok(())
    }
    pub(crate) fn property(&mut self, facts: PhysicalPropertyProjectionFacts) -> Result<(), Error> {
        self.refs = add(self.refs, facts.value_reference_count)?;
        self.requests = add(self.requests, facts.allocation_requests_upper_bound)?;
        self.requested = add(self.requested, facts.allocation_request_bytes_upper_bound)?;
        // The sole property author performs preflight here and again in emit.
        // Admit both passes; never duplicate its numerical/layout algorithm.
        self.delegated_work = add(
            self.delegated_work,
            mul(facts.cumulative_work_upper_bound, 2)?,
        )?;
        Ok(())
    }
    pub(crate) fn facts(
        &self,
        source: usize,
        values: usize,
        l: impl Into<Option<NodeProjectionLimits>>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<NodeProjectionFacts, Error> {
        self.facts_in(source, values, l, &mut |_| Ok(()), w)
    }
    pub(crate) fn facts_in(
        &self,
        source: usize,
        values: usize,
        l: impl Into<Option<NodeProjectionLimits>>,
        admit: &mut NodeAdmit<'_>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<NodeProjectionFacts, Error> {
        let (facts, axes) = self.checked_facts_core(source, values, l.into())?;
        admit(&facts)?;
        for _ in 0..axes {
            w.step()?;
        }
        Ok(facts)
    }
    /// Admit known original facts before completed work or a request boundary.
    /// Growing snapshots replace this node's prior contribution in the caller.
    pub(crate) fn admit_in(
        &self,
        source: usize,
        values: usize,
        l: impl Into<Option<NodeProjectionLimits>>,
        admit: &mut NodeAdmit<'_>,
    ) -> Result<NodeProjectionFacts, Error> {
        let facts = self.numerical_facts(source, values, l)?;
        admit(&facts)?;
        Ok(facts)
    }
    /// The same numerical author for a nested model's synchronous admission.
    /// This adds neither an observation nor another budget/source invoice.
    pub(crate) fn numerical_facts(
        &self,
        source: usize,
        values: usize,
        l: impl Into<Option<NodeProjectionLimits>>,
    ) -> Result<NodeProjectionFacts, Error> {
        self.checked_facts_core(source, values, l.into())
            .map(|(facts, _)| facts)
    }
    pub(crate) fn numerical_input_derived_facts(
        &self,
        source: usize,
        values: usize,
    ) -> Result<NodeProjectionFacts, Error> {
        self.checked_facts_core(source, values, None)
            .map(|(facts, _)| facts)
    }
    fn checked_facts_core(
        &self,
        source: usize,
        values: usize,
        limits: Option<NodeProjectionLimits>,
    ) -> Result<(NodeProjectionFacts, usize), Error> {
        let search_height = (usize::BITS - values.leading_zeros()) as usize + 1;
        // Each original list/reference has a bounded count/shape pass, exact
        // namespace lookup and emission pass. The multiplier covers their
        // fixed-size arithmetic/header gates and the final footer. Requested
        // bytes cover writes and possible Vec-to-Box moves; library internals
        // remain opaque boundaries rather than synthetic work callbacks.
        let own_work = add(
            256,
            add(
                mul(add(self.items, self.inputs)?, 32)?,
                add(
                    mul(self.refs, add(search_height, 32)?)?,
                    mul(self.requested, 4)?,
                )?,
            )?,
        )?;
        let facts = NodeProjectionFacts {
            input_node_count: self.inputs,
            value_reference_count: self.refs,
            list_item_count: self.items,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.requested,
            coexisting_source_and_request_bytes_upper_bound: add(source, self.requested)?,
            cumulative_work_upper_bound: add(own_work, self.delegated_work)?,
        };
        // Every numerical fact is now known. Admit all axes before observing
        // completed gate work, so a later axis cannot lose to the next quantum.
        let Some(l) = limits else {
            return Ok((facts, 0));
        };
        let axes = [
            (facts.input_node_count, l.max_input_nodes),
            (facts.value_reference_count, l.max_value_references),
            (facts.list_item_count, l.max_list_items),
            (
                facts.allocation_requests_upper_bound,
                l.max_allocation_requests,
            ),
            (
                facts.allocation_request_bytes_upper_bound,
                l.max_allocation_request_bytes,
            ),
            (
                facts.coexisting_source_and_request_bytes_upper_bound,
                l.max_coexisting_source_and_request_bytes,
            ),
            (facts.cumulative_work_upper_bound, l.max_work),
        ];
        for (actual, maximum) in axes {
            check_cap(actual, maximum)?;
        }
        Ok((facts, axes.len()))
    }
}
pub(crate) fn count_prefix(
    inputs: usize,
    items: usize,
    source: usize,
    known: usize,
    l: impl Into<Option<NodeProjectionLimits>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    // This lower bound precedes all variable-length numerical counting.
    let work = add(256, mul(add(items, inputs)?, 32)?)?;
    if let Some(limits) = l.into() {
        check_cap(inputs, limits.max_input_nodes)?;
        check_cap(items, limits.max_list_items)?;
        check_cap(work, limits.max_work)?;
        for _ in 0..3 {
            w.step()?;
        }
    }
    floor(source, known, w)
}

/// Header contributions only: callers keep their original stage ordering.
pub(crate) fn physical_header_floor(input: &p::PhysicalNode) -> Result<usize, Error> {
    add(
        size_of::<p::PhysicalNode>(),
        add(
            bytes::<p::NodeId>(input.inputs.len())?,
            add(
                bytes::<p::PhysicalProperties>(input.required_inputs.len())?,
                bytes::<p::ValueId>(input.output.columns.len())?,
            )?,
        )?,
    )
}
pub(crate) fn wire_header_floor(
    input: &wire::PhysicalNode,
    port: &wire::OutputPort,
) -> Result<usize, Error> {
    add(
        size_of::<wire::PhysicalNode>(),
        add(
            bytes::<u32>(input.input_node_ids.capacity())?,
            add(
                bytes::<wire::PhysicalProperties>(input.required_inputs.capacity())?,
                bytes::<u32>(port.value_ids.capacity())?,
            )?,
        )?,
    )
}
pub(crate) fn encode_header_requests(
    input: &p::PhysicalNode,
    model: &mut Model,
) -> Result<(), Error> {
    model.request::<u32>(input.inputs.len(), 1)?;
    model.request::<wire::PhysicalProperties>(input.required_inputs.len(), 1)?;
    model.request::<u32>(input.output.columns.len(), 1)
}
pub(crate) fn decode_header_requests(
    input: &wire::PhysicalNode,
    port: &wire::OutputPort,
    model: &mut Model,
) -> Result<(), Error> {
    model.request::<p::NodeId>(input.input_node_ids.len(), 2)?;
    model.request::<p::PhysicalProperties>(input.required_inputs.len(), 2)?;
    model.request::<p::ValueId>(port.value_ids.len(), 2)
}
pub(crate) fn encode_ids(
    input: &[p::ValueId],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u32>, Error> {
    let mut output = reserve(input.len(), w)?;
    for id in input {
        output.push(id.get());
        w.step()?;
    }
    Ok(output)
}
pub(crate) fn decode_ids(
    input: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[p::ValueId]>, Error> {
    let mut output = reserve(input.len(), w)?;
    for id in input {
        output.push(p::ValueId::new(*id));
        w.step()?;
    }
    boxed(output, w)
}
pub(crate) type EncodedHeader = (
    Vec<u32>,
    Vec<wire::PhysicalProperties>,
    wire::PhysicalProperties,
    wire::OutputPort,
);
pub(crate) type DecodedHeader = (
    Box<[p::NodeId]>,
    Box<[p::PhysicalProperties]>,
    p::PhysicalProperties,
    p::OutputPort,
);
pub(crate) fn encode_header(
    input: &p::PhysicalNode,
    source: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<EncodedHeader, Error> {
    let properties = limits.into().map(|limits| limits.properties);
    let mut inputs = reserve(input.inputs.len(), w)?;
    for id in &input.inputs {
        inputs.push(id.get());
        w.step()?;
    }
    let mut required = reserve(input.required_inputs.len(), w)?;
    for property in &input.required_inputs {
        required.push(properties::encode_observed(property, source, properties, w)?.0);
        w.step()?;
    }
    let output_properties =
        properties::encode_observed(&input.output_properties, source, properties, w)?.0;
    let output = wire::OutputPort {
        node_id: Some(input.output.node.get()),
        value_ids: encode_ids(&input.output.columns, w)?,
    };
    Ok((inputs, required, output_properties, output))
}
pub(crate) fn decode_header(
    input: &wire::PhysicalNode,
    source: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<DecodedHeader, Error> {
    let properties = limits.into().map(|limits| limits.properties);
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("prepared Repeat output port is absent"))?;
    let mut inputs = reserve(input.input_node_ids.len(), w)?;
    for id in &input.input_node_ids {
        inputs.push(p::NodeId::new(*id));
        w.step()?;
    }
    let inputs = boxed(inputs, w)?;
    let mut required = reserve(input.required_inputs.len(), w)?;
    for property in &input.required_inputs {
        required.push(properties::decode_observed(property, source, properties, w)?.0);
        w.step()?;
    }
    let required = boxed(required, w)?;
    let output_properties = properties::decode_observed(
        input
            .output_properties
            .as_ref()
            .ok_or_else(|| invalid("prepared Repeat output properties are absent"))?,
        source,
        properties,
        w,
    )?
    .0;
    let output = p::OutputPort {
        node: p::NodeId::new(
            port.node_id
                .ok_or_else(|| invalid("prepared Repeat output node ID is absent"))?,
        ),
        columns: decode_ids(&port.value_ids, w)?,
    };
    Ok((inputs, required, output_properties, output))
}

#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod owner_tests;

impl From<novarocks_constant_contract::ConstantTableError> for NodeCodecError {
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(p::ConstantReferenceError::from(error))
    }
}
