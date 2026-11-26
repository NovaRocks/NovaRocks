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

//! Complete ValueDef namespace through the original type/payload authors.
//! The payload token supplies the original control; TypeTable supplies type
//! authority and currently does not retain a control. Fragment reference,
//! provenance, provider purpose, and installed-owner checks remain mandatory.
//! Source invoices are caller-owned full backing facts; floors below are only
//! necessary lower bounds, never a host allocation grant or RSS measurement.

use crate::{
    allocation_exit_v2::reserve_exit,
    binding_index_v2::BindingIndex,
    borrowed_type_resources::{verify_type_binding, verify_type_binding_admitted},
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::{
        ConnectorPayloadCodecError, DecodedConnectorPayloads, EncodedConnectorPayloads,
        arc_str_bytes,
    },
    physical_type_v2::{self, DecodedTypeTable, EncodedTypeTable, TypeCodecError},
    physical_value_origin_v2::{
        self as origins, ValueOriginCodecError, ValueOriginProjectionFacts,
        ValueOriginProjectionLimits,
    },
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy)]
pub struct ValueSource<'source> {
    pub source: &'source p::ValueDef,
    pub value_type_id: u32,
}
#[derive(Clone, Copy, Debug)]
pub struct ValueProjectionLimits {
    pub max_definitions: usize,
    pub max_origin_references: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
    pub origins: ValueOriginProjectionLimits,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueProjectionFacts {
    pub definition_count: usize,
    pub origin_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum ValueCodecError {
    Control(CompileControlError),
    Payload(ConnectorPayloadCodecError),
    Type(TypeCodecError),
    Origin(ValueOriginCodecError),
    Index(BindingCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for ValueCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
macro_rules! lift {
    ($source:ty, $variant:ident, $control:path) => {
        impl From<$source> for ValueCodecError {
            fn from(e: $source) -> Self {
                match e {
                    $control(cause) => Self::Control(cause),
                    e => Self::$variant(e),
                }
            }
        }
    };
}
lift!(
    ConnectorPayloadCodecError,
    Payload,
    ConnectorPayloadCodecError::Control
);
lift!(TypeCodecError, Type, TypeCodecError::Control);
lift!(
    ValueOriginCodecError,
    Origin,
    ValueOriginCodecError::Control
);
lift!(BindingCodecError, Index, BindingCodecError::Control);
impl fmt::Display for ValueCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Origin(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ValueCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Payload(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Origin(source) => Some(source),
            Self::Index(source) => Some(source),
            _ => None,
        }
    }
}
type Error = ValueCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("value resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("value resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|layout| layout.size())
        .map_err(|_| invalid("value allocation layout is unrepresentable"))
}
fn cap(
    n: usize,
    max: impl Into<Option<usize>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let Some(max) = max.into() else {
        return Ok(());
    };
    let admitted = n <= max;
    w.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid("value projection envelope exceeded"))
    }
}
fn floor(source: usize, known: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let admitted = source >= known;
    w.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid("value source invoice omits original backing"))
    }
}
fn finish<T>(result: Result<T, Error>, w: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut out = Vec::new();
    let result = out.try_reserve_exact(n);
    reserve_exit::<Error>(result, w)?;
    Ok(out)
}
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    let value = id;
    w.step()?;
    value.ok_or_else(|| invalid("value type reference is absent"))
}
fn origin<'a>(
    def: &'a wire::ValueDefinition,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a wire::ValueOrigin, Error> {
    let origin = def.origin.as_ref();
    w.step()?;
    origin.ok_or_else(|| invalid("value origin is absent"))
}
fn typed<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a p::ValueType, Error> {
    // The actual sparse BTreeMap lookup remains opaque. Its conservative
    // source-count work bound is admitted before these original boundaries.
    w.flush()?;
    let value = types.value_type(id);
    w.flush()?;
    value.ok_or_else(|| invalid("value type ID is unknown"))
}

#[derive(Default)]
struct Model {
    references: usize,
    requests: usize,
    bytes: usize,
    dictionary_bytes: usize,
    work: usize,
}
impl Model {
    fn request(&mut self, n: usize) -> Result<(), Error> {
        if n != 0 {
            self.requests = add(self.requests, 1)?;
            self.bytes = add(self.bytes, n)?;
        }
        Ok(())
    }
    fn origin(&mut self, facts: ValueOriginProjectionFacts) -> Result<(), Error> {
        self.references = add(self.references, facts.reference_count)?;
        self.requests = add(self.requests, facts.allocation_requests_upper_bound)?;
        self.bytes = add(self.bytes, facts.allocation_request_bytes_upper_bound)?;
        // Numerical preflight and the sole observed projection each run once.
        // Only projection allocates, so requests are counted ONCE.
        self.work = add(self.work, mul(facts.cumulative_work_upper_bound, 2)?)?;
        Ok(())
    }
    fn clone_requests(&mut self, requests: usize, bytes: usize) -> Result<(), Error> {
        self.requests = add(self.requests, requests)?;
        self.bytes = add(self.bytes, bytes)?;
        self.dictionary_bytes = add(self.dictionary_bytes, bytes)?;
        Ok(())
    }
    fn clone_type(&mut self, facts: physical_type_v2::ValueTypeCloneFacts) -> Result<(), Error> {
        self.clone_requests(
            facts.allocation_requests_upper_bound(),
            facts.allocation_request_bytes_upper_bound(),
        )?;
        self.work = add(self.work, mul(facts.work_upper_bound(), 2)?)?;
        Ok(())
    }
    fn numerical_facts(
        &self,
        n: usize,
        source: usize,
        l: Option<ValueProjectionLimits>,
    ) -> Result<ValueProjectionFacts, Error> {
        let coexist = add(source, self.bytes)?;
        let work = add(self.work, add(mul(self.bytes, 4)?, self.requests)?)?;
        if l.is_some_and(|l| {
            n > l.max_definitions
                || self.references > l.max_origin_references
                || self.requests > l.max_allocation_requests
                || self.bytes > l.max_allocation_request_bytes
                || coexist > l.max_coexisting_source_and_request_bytes
                || work > l.max_work
        }) {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        Ok(ValueProjectionFacts {
            definition_count: n,
            origin_reference_count: self.references,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.bytes,
            coexisting_source_and_request_bytes_upper_bound: coexist,
            cumulative_work_upper_bound: work,
        })
    }
    fn gate(
        &self,
        n: usize,
        source: usize,
        l: Option<ValueProjectionLimits>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<ValueProjectionFacts, Error> {
        cap(self.references, l.map(|l| l.max_origin_references), w)?;
        cap(self.requests, l.map(|l| l.max_allocation_requests), w)?;
        cap(self.bytes, l.map(|l| l.max_allocation_request_bytes), w)?;
        let coexist = add(source, self.bytes)?;
        cap(
            coexist,
            l.map(|l| l.max_coexisting_source_and_request_bytes),
            w,
        )?;
        let work = add(self.work, add(mul(self.bytes, 4)?, self.requests)?)?;
        cap(work, l.map(|l| l.max_work), w)?;
        Ok(ValueProjectionFacts {
            definition_count: n,
            origin_reference_count: self.references,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.bytes,
            coexisting_source_and_request_bytes_upper_bound: coexist,
            cumulative_work_upper_bound: work,
        })
    }
}
type ParentAdmission<'a> = dyn FnMut(&ValueProjectionFacts) -> Result<(), CompileControlError> + 'a;
struct Admission<'borrow, 'callback> {
    parent: Option<&'borrow mut ParentAdmission<'callback>>,
}
impl Admission<'_, '_> {
    fn observed(&self) -> bool {
        self.parent.is_some()
    }
    // Only arithmetic/Layout/model operations use this adapter. Ordinary
    // source/type/origin errors retain their original author and category.
    fn numeric<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        if self.observed() {
            result.map_err(|_| CompileControlError::ResourceExhausted.into())
        } else {
            result
        }
    }
    fn gate(
        &mut self,
        model: &Model,
        n: usize,
        source: usize,
        limits: Option<ValueProjectionLimits>,
    ) -> Result<(), Error> {
        if self.observed() {
            let facts = self.numeric(model.numerical_facts(n, source, limits))?;
            if let Some(parent) = &mut self.parent {
                parent(&facts)?;
            }
        }
        Ok(())
    }
    fn origin_prefix(
        &mut self,
        model: &mut Model,
        next: &ValueOriginProjectionFacts,
        previous: &mut Option<ValueOriginProjectionFacts>,
        n: usize,
        source: usize,
        limits: Option<ValueProjectionLimits>,
    ) -> Result<(), CompileControlError> {
        let delta = |next: usize, previous: usize| {
            next.checked_sub(previous)
                .ok_or(CompileControlError::ResourceExhausted)
        };
        let prev = *previous;
        let result = (|| -> Result<(), Error> {
            model.references = self.numeric(add(
                model.references,
                delta(next.reference_count, prev.map_or(0, |f| f.reference_count))?,
            ))?;
            model.requests = self.numeric(add(
                model.requests,
                delta(
                    next.allocation_requests_upper_bound,
                    prev.map_or(0, |f| f.allocation_requests_upper_bound),
                )?,
            ))?;
            model.bytes = self.numeric(add(
                model.bytes,
                delta(
                    next.allocation_request_bytes_upper_bound,
                    prev.map_or(0, |f| f.allocation_request_bytes_upper_bound),
                )?,
            ))?;
            // One original numerical/decision pass and one later emission;
            // only emission allocates, so its requests are accumulated once.
            model.work = self.numeric(add(
                model.work,
                self.numeric(mul(
                    delta(
                        next.cumulative_work_upper_bound,
                        prev.map_or(0, |f| f.cumulative_work_upper_bound),
                    )?,
                    2,
                ))?,
            ))?;
            self.gate(model, n, source, limits)
        })();
        match result {
            Ok(()) => {
                *previous = Some(*next);
                Ok(())
            }
            Err(Error::Control(cause)) => Err(cause),
            Err(_) => unreachable!("only numerical origin contribution enters admission"),
        }
    }
    fn gate_control(
        &mut self,
        model: &Model,
        n: usize,
        source: usize,
        limits: Option<ValueProjectionLimits>,
    ) -> Result<(), CompileControlError> {
        match self.gate(model, n, source, limits) {
            Ok(()) => Ok(()),
            Err(Error::Control(cause)) => Err(cause),
            Err(_) => unreachable!("pure admission preserves the typed numerical cause"),
        }
    }
    fn complete(
        &mut self,
        model: &Model,
        n: usize,
        source: usize,
        limits: Option<ValueProjectionLimits>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ValueProjectionFacts, Error> {
        self.gate(model, n, source, limits)?;
        model.gate(n, source, limits, work)
    }
}
fn same_control(
    expected: &dyn PureCompileControl,
    work: &CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if !std::ptr::addr_eq(expected, work.control()) {
        return Err(invalid("value namespace belongs to another control"));
    }
    Ok(())
}

fn own_work(n: usize, payloads: usize, types: usize) -> Result<usize, Error> {
    let height = (usize::BITS - n.leading_zeros()) as usize;
    // Original source/count loops, two value-root lookup passes, and the sole
    // index heapsort/dedup. Origin source scans and type comparison/clone work
    // are separately added from their real author facts, not inferred here.
    let per_value = add(128, add(mul(height + 1, 16)?, mul(types, 4)?)?)?;
    add(1024, add(mul(payloads, 16)?, mul(n, per_value)?)?)
}
fn individual_floor(
    value: &p::ValueDef,
    dictionary_bytes: usize,
    w: &mut CompileCheckpoints<'_>,
) -> Result<usize, Error> {
    let mut known = add(size_of::<p::ValueDef>(), dictionary_bytes)?;
    if let p::ValueOrigin::ProviderField { field, .. } = &value.origin {
        let header = field.column_payload.header();
        // Original buffer length is only a known floor. Its opaque capacity,
        // Field/metadata backing and all aliases remain in the host invoice.
        known = add(
            known,
            add(
                field.column_payload.payload().len(),
                add(
                    arc_str_bytes(header.provider_id().as_str().len())?,
                    arc_str_bytes(header.catalog().catalog_name().as_str().len())?,
                )?,
            )?,
        )?;
    }
    w.step()?;
    Ok(known)
}

pub struct EncodedValues<'loan, 'source, 'control> {
    inputs: &'loan [ValueSource<'source>],
    payloads: &'loan EncodedConnectorPayloads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    wire: Vec<wire::ValueDefinition>,
    indices: BindingIndex,
    facts: ValueProjectionFacts,
    original_source_bytes: usize,
}
impl<'loan, 'source, 'control> EncodedValues<'loan, 'source, 'control> {
    pub fn as_wire(&self) -> &[wire::ValueDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ValueDefinition> {
        self.wire
    }
    pub fn facts(&self) -> &ValueProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.inputs.len()
    }
    pub fn payloads(&self) -> &EncodedConnectorPayloads<'source, 'control> {
        self.payloads
    }
    pub fn types(&self) -> &EncodedTypeTable<'source> {
        self.types
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.payloads.original_control()
    }
    pub fn value(&self, id: u32) -> Result<Option<&'source p::ValueDef>, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.value_observed(id, &mut w);
        finish(result, w)
    }
    pub(crate) fn value_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source p::ValueDef>, Error> {
        self.value_captured(id, &mut |_, _| Ok(()), w)
    }
    /// Capture the original value loan before its matched comparison callback.
    /// The consumer admits its own comparison/copy contribution here.
    pub(crate) fn value_captured<E>(
        &self,
        id: u32,
        capture: &mut impl FnMut(&'source p::ValueDef, &mut CompileCheckpoints<'_>) -> Result<(), E>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source p::ValueDef>, E>
    where
        E: From<Error> + From<BindingCodecError> + From<CompileControlError>,
    {
        same_control(self.original_control(), w).map_err(E::from)?;
        self.indices
            .find_captured(
                id,
                |at| self.inputs[at].source.id.get(),
                &mut |at, work| capture(self.inputs[at].source, work),
                w,
            )
            .map(|found| found.map(|at| self.inputs[at].source))
    }
    pub fn source_id(&self, value: &p::ValueDef) -> Result<u32, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.source_id_observed(value, &mut w);
        finish(result, w)
    }
    pub(crate) fn source_id_observed(
        &self,
        value: &p::ValueDef,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        same_control(self.original_control(), w)?;
        let mut found = None;
        for input in self.inputs {
            let same = std::ptr::eq(input.source, value);
            w.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("value source association is ambiguous"));
                }
                found = Some(value.id.get());
            }
        }
        found.ok_or_else(|| invalid("value source owner is not in this namespace"))
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.retained_floor_observed(&mut w);
        finish(result, w)
    }
    fn retained_floor_header(&self) -> Result<usize, Error> {
        add(
            self.original_source_bytes,
            add(
                size_of::<Self>(),
                add(
                    self.indices.backing_bytes()?,
                    bytes::<wire::ValueDefinition>(self.wire.capacity())?,
                )?,
            )?,
        )
    }
    pub(crate) fn retained_floor_header_admitted(&self) -> Result<usize, CompileControlError> {
        self.retained_floor_header()
            .map_err(|_| CompileControlError::ResourceExhausted)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        same_control(self.original_control(), w)?;
        let known = self.retained_floor_header()?;
        w.step()?;
        // All origin DTO variants are inline; they own no string/Vec payload.
        Ok(known)
    }
}
pub struct DecodedValues<'loan, 'wire, 'control> {
    wire: &'wire [wire::ValueDefinition],
    payloads: &'loan DecodedConnectorPayloads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    values: Vec<p::ValueDef>,
    indices: BindingIndex,
    facts: ValueProjectionFacts,
    original_source_bytes: usize,
    owned_dictionary_bytes: usize,
}
impl<'loan, 'wire, 'control> DecodedValues<'loan, 'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::ValueDefinition] {
        self.wire
    }
    pub fn facts(&self) -> &ValueProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.wire.len()
    }
    pub fn payloads(&self) -> &DecodedConnectorPayloads<'wire, 'control> {
        self.payloads
    }
    pub fn types(&self) -> &DecodedTypeTable {
        self.types
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.payloads.original_control()
    }
    pub fn value(&self, id: u32) -> Result<Option<&p::ValueDef>, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.value_observed(id, &mut w);
        finish(result, w)
    }
    pub(crate) fn value_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&p::ValueDef>, Error> {
        self.value_captured(id, &mut |_, _| Ok(()), w)
    }
    /// Capture the actual owned value before observing the matched lookup.
    pub(crate) fn value_captured<'a, E>(
        &'a self,
        id: u32,
        capture: &mut impl FnMut(&'a p::ValueDef, &mut CompileCheckpoints<'_>) -> Result<(), E>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a p::ValueDef>, E>
    where
        E: From<Error> + From<BindingCodecError> + From<CompileControlError>,
    {
        same_control(self.original_control(), w).map_err(E::from)?;
        self.indices
            .find_captured(
                id,
                |at| self.wire[at].id,
                &mut |at, work| capture(&self.values[at], work),
                w,
            )
            .map(|found| found.map(|at| &self.values[at]))
    }
    pub fn into_values(self) -> Vec<p::ValueDef> {
        self.values
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.retained_floor_observed(&mut w);
        finish(result, w)
    }
    // Pure original header arithmetic: no control callback, source walk or
    // allocation. The returned necessary floor is not another source invoice.
    fn retained_floor_header(&self) -> Result<usize, Error> {
        let known = add(
            self.original_source_bytes,
            add(
                size_of::<Self>(),
                add(
                    self.indices.backing_bytes()?,
                    bytes::<p::ValueDef>(self.values.capacity())?,
                )?,
            )?,
        )?;
        add(known, self.owned_dictionary_bytes)
    }
    pub(crate) fn retained_floor_header_admitted(&self) -> Result<usize, CompileControlError> {
        self.retained_floor_header()
            .map_err(|_| CompileControlError::ResourceExhausted)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        same_control(self.original_control(), w)?;
        let known = self.retained_floor_header()?;
        w.step()?;
        // Shared payload/Field/metadata backing stays in the original invoice;
        // only independently owned output Dictionary boxes are added here.
        Ok(known)
    }
}

pub fn encode_values<'loan, 'source, 'control>(
    inputs: &'loan [ValueSource<'source>],
    payloads: &'loan EncodedConnectorPayloads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    source: usize,
    limits: ValueProjectionLimits,
) -> Result<EncodedValues<'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(payloads.original_control(), CompilePhase::Encode)?;
    let result = encode_core(
        inputs,
        payloads,
        types,
        source,
        Some(limits),
        &mut Admission { parent: None },
        &mut w,
    );
    finish(result, w)
}
/// Project the original Value sources on the caller's exact payload meter.
/// The caller owns entry/footer and the truthful union source invoice. Prefix
/// facts replace this child's prior contribution. Passing None derives
/// requests and work from the exact value, type and payload sources.
pub fn encode_values_observed_in<'loan, 'source, 'control>(
    inputs: &'loan [ValueSource<'source>],
    payloads: &'loan EncodedConnectorPayloads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    source: usize,
    limits: impl Into<Option<ValueProjectionLimits>>,
    admit: &mut ParentAdmission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedValues<'loan, 'source, 'control>, Error> {
    encode_core(
        inputs,
        payloads,
        types,
        source,
        limits.into(),
        &mut Admission {
            parent: Some(admit),
        },
        work,
    )
}

fn encode_core<'loan, 'source, 'control>(
    inputs: &'loan [ValueSource<'source>],
    payloads: &'loan EncodedConnectorPayloads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    source: usize,
    l: Option<ValueProjectionLimits>,
    admission: &mut Admission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<EncodedValues<'loan, 'source, 'control>, Error> {
    same_control(payloads.original_control(), w)?;
    if admission.observed() && l.is_some_and(|l| inputs.len() > l.max_definitions) {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    if !admission.observed() {
        cap(inputs.len(), l.map(|l| l.max_definitions), w)?;
    }
    let root = admission.numeric(bytes::<ValueSource<'_>>(inputs.len()))?;
    let mut model = Model::default();
    admission.numeric(model.request(admission.numeric(bytes::<usize>(inputs.len()))?))?;
    admission
        .numeric(model.request(admission.numeric(bytes::<wire::ValueDefinition>(inputs.len()))?))?;
    model.work = admission.numeric(own_work(
        inputs.len(),
        payloads.source_count(),
        types.source_counts().0,
    ))?;
    if admission.observed() {
        model.work = admission.numeric(add(
            model
                .work
                .max(crate::binding_index_v2::prepare_work_upper_bound(
                    inputs.len(),
                )?),
            admission.numeric(mul(
                inputs.len(),
                physical_type_v2::value_type_clone_preflight_work_upper_bound(),
            ))?,
        ))?;
        admission.gate(&model, inputs.len(), source, l)?;
        let payload_header = payloads.retained_floor_header_admitted()?;
        if source < payload_header {
            return Err(invalid("value source invoice omits original backing"));
        }
        cap(inputs.len(), l.map(|l| l.max_definitions), w)?;
    }
    cap(model.work, l.map(|l| l.max_work), w)?;
    floor(source, root, w)?;
    floor(source, payloads.retained_floor_observed(w)?, w)?;
    let mut individual = 0usize;
    for input in inputs {
        let original = input.source;
        let source_clone = physical_type_v2::preflight_value_type_clone(&original.ty, w)?;
        if admission.observed()
            && source_clone.work_upper_bound()
                > physical_type_v2::value_type_clone_preflight_work_upper_bound()
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        individual = individual.max(individual_floor(
            original,
            source_clone.allocation_request_bytes_upper_bound(),
            w,
        )?);
        if !admission.observed() {
            model.work = add(model.work, source_clone.work_upper_bound())?;
        }
        floor(source, add(root, individual)?, w)?;
        let bound = types
            .value_type_observed(input.value_type_id, w)?
            .ok_or_else(|| invalid("value type ID is unknown"))?;
        let remaining = l
            .map(|l| {
                l.max_work.checked_sub(model.work).ok_or_else(|| {
                    if admission.observed() {
                        CompileControlError::ResourceExhausted.into()
                    } else {
                        invalid("value work envelope exhausted")
                    }
                })
            })
            .transpose()?;
        let checked = if admission.observed() {
            let base = model.work;
            verify_type_binding_admitted::<Error>(
                &original.ty,
                bound,
                source,
                remaining,
                &mut |facts| {
                    model.work = admission.numeric(add(base, facts.work_upper_bound()))?;
                    admission.gate(&model, inputs.len(), source, l)
                },
                w,
            )?
        } else {
            let checked = verify_type_binding(&original.ty, bound, source, remaining, w)?;
            model.work = add(model.work, checked.work_upper_bound())?;
            checked
        };
        if !checked.matches() {
            return Err(invalid("value full source type differs"));
        }
        if admission.observed() {
            let mut previous = None;
            origins::preflight_encode_admitted(
                &original.origin,
                payloads,
                source,
                l.map(|l| l.origins),
                &mut |facts| {
                    admission.origin_prefix(
                        &mut model,
                        facts,
                        &mut previous,
                        inputs.len(),
                        source,
                        l,
                    )
                },
                w,
            )?;
        } else {
            model.origin(origins::preflight_encode_observed(
                &original.origin,
                payloads,
                source,
                l.map(|l| l.origins),
                w,
            )?)?;
        }
        admission.gate(&model, inputs.len(), source, l)?;
        cap(model.work, l.map(|l| l.max_work), w)?;
        w.step()?;
    }
    let facts = admission.complete(&model, inputs.len(), source, l, w)?;
    let indices = BindingIndex::prepare(inputs.len(), |at| inputs[at].source.id.get(), w)?;
    let mut output = reserve(inputs.len(), w)?;
    for input in inputs {
        let (origin, _) = if admission.observed() {
            origins::encode_admitted(
                &input.source.origin,
                payloads,
                source,
                l.map(|l| l.origins),
                &mut |_| admission.gate_control(&model, inputs.len(), source, l),
                w,
            )?
        } else {
            origins::encode_observed(
                &input.source.origin,
                payloads,
                source,
                l.map(|l| l.origins),
                w,
            )?
        };
        output.push(wire::ValueDefinition {
            id: input.source.id.get(),
            value_type_id: Some(input.value_type_id),
            origin: Some(origin),
        });
        w.step()?;
    }
    Ok(EncodedValues {
        inputs,
        payloads,
        types,
        wire: output,
        indices,
        facts,
        original_source_bytes: source,
    })
}
pub fn decode_values<'loan, 'wire, 'control>(
    defs: &'wire [wire::ValueDefinition],
    payloads: &'loan DecodedConnectorPayloads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    source: usize,
    limits: ValueProjectionLimits,
) -> Result<DecodedValues<'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(payloads.original_control(), CompilePhase::Decode)?;
    let result = decode_core(
        defs,
        payloads,
        types,
        source,
        Some(limits),
        &mut Admission { parent: None },
        &mut w,
    );
    finish(result, w)
}
/// Materialize through the original type/origin owners without a private scope.
/// Passing None derives requests and work from the exact received value,
/// type and payload namespaces. Full Fragment validation remains separate.
pub fn decode_values_observed_in<'loan, 'wire, 'control>(
    defs: &'wire [wire::ValueDefinition],
    payloads: &'loan DecodedConnectorPayloads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    source: usize,
    limits: impl Into<Option<ValueProjectionLimits>>,
    admit: &mut ParentAdmission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedValues<'loan, 'wire, 'control>, Error> {
    decode_core(
        defs,
        payloads,
        types,
        source,
        limits.into(),
        &mut Admission {
            parent: Some(admit),
        },
        work,
    )
}

fn decode_core<'loan, 'wire, 'control>(
    defs: &'wire [wire::ValueDefinition],
    payloads: &'loan DecodedConnectorPayloads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    source: usize,
    l: Option<ValueProjectionLimits>,
    admission: &mut Admission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<DecodedValues<'loan, 'wire, 'control>, Error> {
    same_control(payloads.original_control(), w)?;
    if admission.observed() && l.is_some_and(|l| defs.len() > l.max_definitions) {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    if !admission.observed() {
        cap(defs.len(), l.map(|l| l.max_definitions), w)?;
    }
    let mut model = Model::default();
    admission.numeric(model.request(admission.numeric(bytes::<usize>(defs.len()))?))?;
    admission.numeric(model.request(admission.numeric(bytes::<p::ValueDef>(defs.len()))?))?;
    model.work = admission.numeric(own_work(
        defs.len(),
        payloads.source_count(),
        types.value_types().len(),
    ))?;
    if admission.observed() {
        let clone_work = admission.numeric(mul(
            defs.len(),
            admission.numeric(mul(
                physical_type_v2::value_type_clone_preflight_work_upper_bound(),
                2,
            ))?,
        ))?;
        model.work = admission.numeric(add(
            model
                .work
                .max(crate::binding_index_v2::prepare_work_upper_bound(
                    defs.len(),
                )?),
            clone_work,
        ))?;
        admission.gate(&model, defs.len(), source, l)?;
        let payload_header = payloads.retained_floor_header_admitted()?;
        if source < payload_header {
            return Err(invalid("value source invoice omits original backing"));
        }
        cap(defs.len(), l.map(|l| l.max_definitions), w)?;
    }
    cap(model.work, l.map(|l| l.max_work), w)?;
    floor(source, bytes::<wire::ValueDefinition>(defs.len())?, w)?;
    floor(source, payloads.retained_floor_observed(w)?, w)?;
    for def in defs {
        let value = typed(types, required(def.value_type_id, w)?, w)?;
        if admission.observed() {
            let mut previous_requests = 0;
            let mut previous_bytes = 0;
            physical_type_v2::preflight_value_type_clone_admitted::<Error>(
                value,
                &mut |facts, _work| {
                    if facts.work_upper_bound()
                        > physical_type_v2::value_type_clone_preflight_work_upper_bound()
                    {
                        return Err(CompileControlError::ResourceExhausted.into());
                    }
                    let requests = facts
                        .allocation_requests_upper_bound()
                        .checked_sub(previous_requests)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                    let bytes = facts
                        .allocation_request_bytes_upper_bound()
                        .checked_sub(previous_bytes)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                    admission.numeric(model.clone_requests(requests, bytes))?;
                    previous_requests = facts.allocation_requests_upper_bound();
                    previous_bytes = facts.allocation_request_bytes_upper_bound();
                    // Dictionary output requests are known now. Admission
                    // precedes the sole clone walk's completed operation.
                    admission.gate(&model, defs.len(), source, l)
                },
                w,
            )?;
        } else {
            let clone = physical_type_v2::preflight_value_type_clone(value, w)?;
            model.clone_type(clone)?;
        }
        if admission.observed() {
            let original = origin(def, w)?;
            let mut previous = None;
            origins::preflight_decode_admitted(
                original,
                payloads,
                source,
                l.map(|l| l.origins),
                &mut |facts| {
                    admission.origin_prefix(&mut model, facts, &mut previous, defs.len(), source, l)
                },
                w,
            )?;
        } else {
            model.origin(origins::preflight_decode_observed(
                origin(def, w)?,
                payloads,
                source,
                l.map(|l| l.origins),
                w,
            )?)?;
        }
        admission.gate(&model, defs.len(), source, l)?;
        cap(model.work, l.map(|l| l.max_work), w)?;
        w.step()?;
    }
    let facts = admission.complete(&model, defs.len(), source, l, w)?;
    let indices = BindingIndex::prepare(defs.len(), |at| defs[at].id, w)?;
    let mut output = reserve(defs.len(), w)?;
    for def in defs {
        let ty = physical_type_v2::clone_value_type_observed(
            typed(types, required(def.value_type_id, w)?, w)?,
            w,
        )?;
        let (origin, _) = if admission.observed() {
            origins::decode_admitted(
                origin(def, w)?,
                payloads,
                source,
                l.map(|l| l.origins),
                &mut |_| admission.gate_control(&model, defs.len(), source, l),
                w,
            )?
        } else {
            origins::decode_observed(origin(def, w)?, payloads, source, l.map(|l| l.origins), w)?
        };
        output.push(p::ValueDef {
            id: p::ValueId::new(def.id),
            ty,
            origin,
        });
        w.step()?;
    }
    Ok(DecodedValues {
        wire: defs,
        payloads,
        types,
        values: output,
        indices,
        facts,
        original_source_bytes: source,
        owned_dictionary_bytes: model.dictionary_bytes,
    })
}
#[cfg(test)]
mod tests;
