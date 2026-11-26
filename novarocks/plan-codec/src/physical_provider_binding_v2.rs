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

//! Complete neutral provider-binding namespace. Original sources and control
//! are sealed together; no provider capability or read/install proof is minted.
//! Future multi-namespace composition must retain the SAME original control
//! loan, not substitute a new callback owner.

use crate::{
    allocation_exit_v2::reserve_exit,
    binding_index_v2::BindingIndex,
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::{arc_str_bytes, arc_str_bytes_for_mode},
};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorIdentityError, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorWriteBinding,
};
use novarocks_proto_models::{catalog, physical_package_v2 as wire};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct ProviderBindingProjectionLimits {
    pub max_definitions: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderBindingProjectionFacts {
    pub definition_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum ProviderBindingCodecError {
    Control(CompileControlError),
    Identity(ConnectorIdentityError),
    Index(BindingCodecError),
    ArcLayout(crate::physical_connector_payload_v2::ConnectorPayloadCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for ProviderBindingCodecError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<BindingCodecError> for ProviderBindingCodecError {
    fn from(value: BindingCodecError) -> Self {
        match value {
            BindingCodecError::Control(cause) => Self::Control(cause),
            value => Self::Index(value),
        }
    }
}
impl From<crate::physical_connector_payload_v2::ConnectorPayloadCodecError>
    for ProviderBindingCodecError
{
    fn from(value: crate::physical_connector_payload_v2::ConnectorPayloadCodecError) -> Self {
        match value {
            crate::physical_connector_payload_v2::ConnectorPayloadCodecError::Control(cause) => {
                Self::Control(cause)
            }
            value => Self::ArcLayout(value),
        }
    }
}
impl fmt::Display for ProviderBindingCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Identity(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::ArcLayout(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ProviderBindingCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Identity(source) => Some(source),
            Self::Index(source) => Some(source),
            Self::ArcLayout(source) => Some(source),
            _ => None,
        }
    }
}
type Error = ProviderBindingCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("provider binding resource sum overflow"))
}
#[cfg(test)]
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("provider binding resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|v| v.size())
        .map_err(|_| invalid("provider binding allocation layout is unrepresentable"))
}
type Admit<'a> = dyn FnMut(&ProviderBindingProjectionFacts) -> Result<(), CompileControlError> + 'a;
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
        "provider binding allocation layout is unrepresentable",
    )
}
fn check_control(
    original: &dyn PureCompileControl,
    work: &CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if std::ptr::addr_eq(original, work.control()) {
        Ok(())
    } else {
        Err(invalid("provider binding control loan differs"))
    }
}
fn cap(n: usize, max: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    if n > max {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    work.step()?;
    Ok(())
}
fn floor(source: usize, known: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let valid = source >= known;
    work.step()?;
    if valid {
        Ok(())
    } else {
        Err(invalid(
            "provider binding source invoice omits original backing",
        ))
    }
}
fn finish<T>(outcome: Result<T, Error>, work: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&outcome, Err(Error::Control(_))) {
        return outcome;
    }
    work.finish()?;
    outcome
}
fn reserve<T>(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    work.flush()?;
    let mut output = Vec::new();
    let result = output.try_reserve_exact(n);
    reserve_exit::<Error>(result, work)?;
    Ok(output)
}
fn copy(input: &[u8], work: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    let mut output = reserve(input.len(), work)?;
    for chunk in input.chunks(1024) {
        output.extend_from_slice(chunk);
        work.step()?;
    }
    Ok(output)
}
fn string(input: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, Error> {
    let bytes = copy(input.as_bytes(), work)?;
    work.flush()?;
    let result = String::from_utf8(bytes);
    work.flush()?;
    result.map_err(|_| invalid("provider binding authored identity is not UTF-8"))
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
                "provider binding resource sum overflow",
            )?;
            self.bytes = numerical(
                self.bytes.checked_add(n),
                self.observed,
                "provider binding resource sum overflow",
            )?;
        }
        Ok(())
    }
}
fn numerical_facts(
    n: usize,
    input_bytes: usize,
    requests: &Requests,
    source: usize,
) -> Result<ProviderBindingProjectionFacts, Error> {
    let add = |a: usize, b: usize| {
        numerical(
            a.checked_add(b),
            requests.observed,
            "provider binding resource sum overflow",
        )
    };
    let mul = |a: usize, b: usize| {
        numerical(
            a.checked_mul(b),
            requests.observed,
            "provider binding resource product overflow",
        )
    };
    let height = (usize::BITS - n.leading_zeros()) as usize;
    // Sole BindingIndex: <=4 completed operations per heap level, plus build,
    // extraction and adjacent dedup. Raw native-identity ASCII validation,
    // Arc construction and String verification are bracketed opaque work;
    // their raw byte lengths and requests are included, not claimed to yield
    // cooperatively inside those libraries. No formal host MEM grant.
    let bound = add(
        256,
        add(
            mul(n, add(128, mul(height + 1, 16)?)?)?,
            add(
                mul(input_bytes, 4)?,
                add(mul(requests.bytes, 4)?, requests.count)?,
            )?,
        )?,
    )?;
    let facts = ProviderBindingProjectionFacts {
        definition_count: n,
        allocation_requests_upper_bound: requests.count,
        allocation_request_bytes_upper_bound: requests.bytes,
        coexisting_source_and_request_bytes_upper_bound: add(source, requests.bytes)?,
        cumulative_work_upper_bound: bound,
    };
    Ok(facts)
}
// Refuse all facts already known at this point before a completed-operation
// checkpoint can expose a later cause. This is the same numerical author used
// by both legacy read sources and the joint namespace.
fn admit_known(
    facts: &ProviderBindingProjectionFacts,
    limits: Option<ProviderBindingProjectionLimits>,
) -> Result<(), Error> {
    if let Some(limits) = limits
        && (facts.definition_count > limits.max_definitions
            || facts.allocation_requests_upper_bound > limits.max_allocation_requests
            || facts.allocation_request_bytes_upper_bound > limits.max_allocation_request_bytes
            || facts.coexisting_source_and_request_bytes_upper_bound
                > limits.max_coexisting_source_and_request_bytes
            || facts.cumulative_work_upper_bound > limits.max_work)
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    Ok(())
}
fn prefix(
    n: usize,
    input: usize,
    requests: &Requests,
    envelope: (usize, Option<ProviderBindingProjectionLimits>),
    admit: &mut Option<&mut Admit<'_>>,
) -> Result<(), Error> {
    let result = numerical_facts(n, input, requests, envelope.0)?;
    admit_known(&result, envelope.1)?;
    if let Some(callback) = admit.as_mut() {
        callback(&result)?;
    }
    Ok(())
}
fn facts(
    n: usize,
    input_bytes: usize,
    requests: Requests,
    source: usize,
    limits: Option<ProviderBindingProjectionLimits>,
    admit: &mut Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProviderBindingProjectionFacts, Error> {
    let facts = numerical_facts(n, input_bytes, &requests, source)?;
    prefix(n, input_bytes, &requests, (source, limits), admit)?;
    // Preserve the original successful observation sequence.
    for _ in 0..4 {
        work.step()?;
    }
    Ok(facts)
}

fn catalog<'a>(
    definition: &'a wire::ProviderBindingDefinition,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a catalog::CatalogHandle, Error> {
    let result = definition.catalog.as_ref();
    work.step()?;
    result.ok_or_else(|| invalid("provider binding catalog is absent"))
}
fn provider_arc(n: usize) -> Result<usize, Error> {
    Ok(arc_str_bytes(n)?)
}

/// Actual immutable owners in the single package-wide ID namespace. A write
/// source is never reconstructed from a read source with equal metadata.
#[derive(Clone, Copy)]
pub enum ProviderBindingSource<'source> {
    Read(&'source ConnectorReadBinding),
    Write(&'source ConnectorWriteBinding),
}
impl ProviderBindingSource<'_> {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        match self {
            Self::Read(v) => v.descriptor(),
            Self::Write(v) => v.descriptor(),
        }
    }
    fn catalog_handle(&self) -> &CatalogHandle {
        match self {
            Self::Read(v) => v.catalog_handle(),
            Self::Write(v) => v.catalog_handle(),
        }
    }
    fn owner_bytes(&self) -> usize {
        match self {
            Self::Read(_) => size_of::<ConnectorReadBinding>(),
            Self::Write(_) => size_of::<ConnectorWriteBinding>(),
        }
    }
}
#[derive(Clone, Copy)]
enum BindingSources<'source> {
    Read(&'source [(u32, &'source ConnectorReadBinding)]),
    Joint(&'source [(u32, ProviderBindingSource<'source>)]),
}
impl<'source> BindingSources<'source> {
    fn len(self) -> usize {
        match self {
            Self::Read(v) => v.len(),
            Self::Joint(v) => v.len(),
        }
    }
    fn at(self, at: usize) -> (u32, ProviderBindingSource<'source>) {
        match self {
            Self::Read(v) => (v[at].0, ProviderBindingSource::Read(v[at].1)),
            Self::Joint(v) => v[at],
        }
    }
    fn backing_bytes(self) -> Result<usize, Error> {
        match self {
            Self::Read(v) => bytes::<(u32, &ConnectorReadBinding)>(v.len()),
            Self::Joint(v) => bytes::<(u32, ProviderBindingSource<'_>)>(v.len()),
        }
    }
}

pub struct EncodedProviderBindings<'source, 'control> {
    inputs: BindingSources<'source>,
    wire: Vec<wire::ProviderBindingDefinition>,
    indices: BindingIndex,
    facts: ProviderBindingProjectionFacts,
    control: &'control dyn PureCompileControl,
    original_source_bytes: usize,
}
impl<'source, 'control> EncodedProviderBindings<'source, 'control> {
    pub fn as_wire(&self) -> &[wire::ProviderBindingDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ProviderBindingDefinition> {
        self.wire
    }
    pub fn facts(&self) -> &ProviderBindingProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.inputs.len()
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.control
    }
    pub fn binding(&self, id: u32) -> Result<Option<&'source ConnectorReadBinding>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.binding_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ConnectorReadBinding>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.inputs.at(at).0, work)?
            .and_then(|at| match self.inputs.at(at).1 {
                ProviderBindingSource::Read(v) => Some(v),
                ProviderBindingSource::Write(_) => None,
            }))
    }
    /// Resolve an exact source owner, not an equivalent reconstructed binding.
    pub fn source_id(&self, source: &ConnectorReadBinding) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.source_id_observed(source, &mut work);
        finish(result, work)
    }
    pub(crate) fn source_id_observed(
        &self,
        source: &ConnectorReadBinding,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let mut found = None;
        for at in 0..self.inputs.len() {
            let (id, binding) = self.inputs.at(at);
            let same = matches!(binding, ProviderBindingSource::Read(v) if std::ptr::eq(v, source));
            work.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("provider binding source association is ambiguous"));
                }
                found = Some(id);
            }
        }
        found.ok_or_else(|| invalid("provider binding source owner is not in this namespace"))
    }
    pub fn write_binding(&self, id: u32) -> Result<Option<&'source ConnectorWriteBinding>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.write_binding_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn write_binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ConnectorWriteBinding>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.inputs.at(at).0, work)?
            .and_then(|at| match self.inputs.at(at).1 {
                ProviderBindingSource::Write(v) => Some(v),
                ProviderBindingSource::Read(_) => None,
            }))
    }
    pub fn write_source_id(&self, source: &ConnectorWriteBinding) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.write_source_id_observed(source, &mut work);
        finish(result, work)
    }
    pub(crate) fn write_source_id_observed(
        &self,
        source: &ConnectorWriteBinding,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let mut found = None;
        for at in 0..self.inputs.len() {
            let (id, binding) = self.inputs.at(at);
            let same =
                matches!(binding, ProviderBindingSource::Write(v) if std::ptr::eq(v, source));
            work.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("provider binding source association is ambiguous"));
                }
                found = Some(id);
            }
        }
        found.ok_or_else(|| invalid("provider binding source owner is not in this namespace"))
    }
    /// Known retained lower floor, not an allocator/live-byte measurement.
    /// Repeated floor/lookup work must be admitted by the consuming scope.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(result, work)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut result = add(self.original_source_bytes, size_of::<Self>())?;
        result = add(result, self.indices.backing_bytes()?)?;
        result = add(
            result,
            bytes::<wire::ProviderBindingDefinition>(self.wire.capacity())?,
        )?;
        work.step()?;
        for definition in &self.wire {
            let catalog = catalog(definition, work)?;
            result = add(
                result,
                add(
                    definition.provider_id.capacity(),
                    add(
                        definition.instance_id.capacity(),
                        add(catalog.catalog_name.capacity(), catalog.version.capacity())?,
                    )?,
                )?,
            )?;
            work.step()?;
        }
        Ok(result)
    }
    /// Borrow the same original namespace and caller scope; no entry/footer.
    pub fn binding_in(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ConnectorReadBinding>, Error> {
        check_control(self.original_control(), work)?;
        self.binding_observed(id, work)
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
        source: &ConnectorReadBinding,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        check_control(self.original_control(), work)?;
        self.source_id_observed(source, work)
    }
    pub fn write_binding_in(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ConnectorWriteBinding>, Error> {
        check_control(self.original_control(), work)?;
        self.write_binding_observed(id, work)
    }
    pub fn write_source_id_in(
        &self,
        source: &ConnectorWriteBinding,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        check_control(self.original_control(), work)?;
        self.write_source_id_observed(source, work)
    }
}
pub struct DecodedProviderBindings<'wire, 'control> {
    wire: &'wire [wire::ProviderBindingDefinition],
    bindings: Vec<ConnectorReadBinding>,
    write_bindings: Vec<ConnectorWriteBinding>,
    indices: BindingIndex,
    facts: ProviderBindingProjectionFacts,
    control: &'control dyn PureCompileControl,
    original_source_bytes: usize,
}
impl<'wire, 'control> DecodedProviderBindings<'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::ProviderBindingDefinition] {
        self.wire
    }
    pub fn facts(&self) -> &ProviderBindingProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.wire.len()
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.control
    }
    pub fn binding(&self, id: u32) -> Result<Option<&ConnectorReadBinding>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.binding_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ConnectorReadBinding>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.wire[at].id, work)?
            .map(|at| &self.bindings[at]))
    }
    /// This is a neutral metadata view, not an installed write capability.
    pub fn write_binding(&self, id: u32) -> Result<Option<&ConnectorWriteBinding>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.write_binding_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn write_binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ConnectorWriteBinding>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.wire[at].id, work)?
            .and_then(|at| self.write_bindings.get(at)))
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
        let mut result = add(self.original_source_bytes, size_of::<Self>())?;
        result = add(result, self.indices.backing_bytes()?)?;
        result = add(
            result,
            bytes::<ConnectorReadBinding>(self.bindings.capacity())?,
        )?;
        // Joint metadata views share the three immutable identity Arcs. Their
        // separate vector backing remains live and is charged independently.
        result = add(
            result,
            bytes::<ConnectorWriteBinding>(self.write_bindings.capacity())?,
        )?;
        work.step()?;
        for binding in &self.bindings {
            result = add(
                result,
                provider_arc(binding.descriptor().provider_id.as_str().len())?,
            )?;
            // Each receiving constructor creates three independent Arcs. The
            // encoder's potentially aliased instance/catalog source differs.
            result = add(
                result,
                provider_arc(binding.descriptor().instance_id.as_str().len())?,
            )?;
            result = add(
                result,
                provider_arc(binding.catalog_handle().catalog_name().as_str().len())?,
            )?;
            work.step()?;
        }
        Ok(result)
    }
    /// Borrow the same original namespace and caller scope; no entry/footer.
    pub fn binding_in(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ConnectorReadBinding>, Error> {
        check_control(self.original_control(), work)?;
        self.binding_observed(id, work)
    }
    pub fn retained_invoice_floor_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        check_control(self.original_control(), work)?;
        self.retained_floor_observed(work)
    }
    pub fn write_binding_in(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ConnectorWriteBinding>, Error> {
        check_control(self.original_control(), work)?;
        self.write_binding_observed(id, work)
    }
}

pub fn encode_provider_bindings<'source, 'control>(
    inputs: &'source [(u32, &'source ConnectorReadBinding)],
    source_retained_bytes: usize,
    limits: impl Into<Option<ProviderBindingProjectionLimits>>,
    control: &'control dyn PureCompileControl,
) -> Result<EncodedProviderBindings<'source, 'control>, Error> {
    let limits = limits.into();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_core(
        BindingSources::Read(inputs),
        source_retained_bytes,
        limits,
        control,
        None,
        &mut work,
    );
    finish(result, work)
}
pub fn encode_joint_provider_bindings<'source, 'control>(
    inputs: &'source [(u32, ProviderBindingSource<'source>)],
    source_retained_bytes: usize,
    limits: impl Into<Option<ProviderBindingProjectionLimits>>,
    control: &'control dyn PureCompileControl,
) -> Result<EncodedProviderBindings<'source, 'control>, Error> {
    let limits = limits.into();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_core(
        BindingSources::Joint(inputs),
        source_retained_bytes,
        limits,
        control,
        None,
        &mut work,
    );
    finish(result, work)
}
fn encode_core<'source, 'control>(
    inputs: BindingSources<'source>,
    source: usize,
    limits: Option<ProviderBindingProjectionLimits>,
    control: &'control dyn PureCompileControl,
    mut admit: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedProviderBindings<'source, 'control>, Error> {
    let observed = admit.is_some();
    let add = |a: usize, b: usize| {
        numerical(
            a.checked_add(b),
            observed,
            "provider binding resource sum overflow",
        )
    };
    if limits.is_some_and(|limits| inputs.len() > limits.max_definitions) {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let roots = inputs.backing_bytes()?;
    let mut requests = Requests {
        observed: admit.is_some(),
        ..Requests::default()
    };
    requests.record(numerical_bytes::<usize>(inputs.len(), admit.is_some())?)?;
    requests.record(numerical_bytes::<wire::ProviderBindingDefinition>(
        inputs.len(),
        admit.is_some(),
    )?)?;
    admit_known(
        &numerical_facts(inputs.len(), 0, &requests, source)?,
        limits,
    )?;
    if admit.is_some() {
        prefix(inputs.len(), 0, &requests, (source, limits), &mut admit)?;
    }
    if let Some(limits) = limits {
        cap(inputs.len(), limits.max_definitions, work)?;
    }
    floor(
        if limits.is_none() {
            source.max(roots)
        } else {
            source
        },
        roots,
        work,
    )?;
    let mut input_bytes = 0;
    let mut individual = 0;
    for at in 0..inputs.len() {
        let (_, binding) = inputs.at(at);
        let provider = binding.descriptor().provider_id.as_str().len();
        let instance = binding.descriptor().instance_id.as_str().len();
        let catalog = binding.catalog_handle().catalog_name().as_str().len();
        input_bytes = add(input_bytes, add(provider, add(instance, catalog)?)?)?;
        // Instance and catalog identities can clone the SAME Arc. Counting
        // their sum would reject an exactly invoiced lawful source. Max is a
        // safe floor whether those immutable owners alias or are independent.
        individual = individual.max(add(
            binding.owner_bytes(),
            add(
                arc_str_bytes_for_mode(provider, admit.is_some())?,
                arc_str_bytes_for_mode(instance, admit.is_some())?
                    .max(arc_str_bytes_for_mode(catalog, admit.is_some())?),
            )?,
        )?);
        for n in [provider, instance, catalog, 32] {
            requests.record(numerical_bytes::<u8>(n, admit.is_some())?)?;
        }
        prefix(
            inputs.len(),
            input_bytes,
            &requests,
            (source, limits),
            &mut admit,
        )?;
        work.step()?;
    }
    floor(
        if limits.is_none() {
            source.max(add(roots, individual)?)
        } else {
            source
        },
        add(roots, individual)?,
        work,
    )?;
    let facts = facts(
        inputs.len(),
        input_bytes,
        requests,
        source,
        limits,
        &mut admit,
        work,
    )?;
    let indices = BindingIndex::prepare(inputs.len(), |at| inputs.at(at).0, work)?;
    let mut output = reserve(inputs.len(), work)?;
    for at in 0..inputs.len() {
        let (id, binding) = inputs.at(at);
        output.push(wire::ProviderBindingDefinition {
            id,
            provider_id: string(binding.descriptor().provider_id.as_str(), work)?,
            instance_id: string(binding.descriptor().instance_id.as_str(), work)?,
            catalog: Some(catalog::CatalogHandle {
                catalog_name: string(binding.catalog_handle().catalog_name().as_str(), work)?,
                version: copy(binding.catalog_handle().version().as_bytes(), work)?,
            }),
        });
        work.step()?;
    }
    Ok(EncodedProviderBindings {
        inputs,
        wire: output,
        indices,
        facts,
        control,
        original_source_bytes: source,
    })
}
pub fn decode_provider_bindings<'wire, 'control>(
    definitions: &'wire [wire::ProviderBindingDefinition],
    source_retained_bytes: usize,
    limits: impl Into<Option<ProviderBindingProjectionLimits>>,
    control: &'control dyn PureCompileControl,
) -> Result<DecodedProviderBindings<'wire, 'control>, Error> {
    let limits = limits.into();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_core(
        definitions,
        (source_retained_bytes, limits),
        control,
        false,
        None,
        &mut work,
    );
    finish(result, work)
}
pub fn decode_joint_provider_bindings<'wire, 'control>(
    definitions: &'wire [wire::ProviderBindingDefinition],
    source_retained_bytes: usize,
    limits: impl Into<Option<ProviderBindingProjectionLimits>>,
    control: &'control dyn PureCompileControl,
) -> Result<DecodedProviderBindings<'wire, 'control>, Error> {
    let limits = limits.into();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_core(
        definitions,
        (source_retained_bytes, limits),
        control,
        true,
        None,
        &mut work,
    );
    finish(result, work)
}
fn decode_core<'wire, 'control>(
    definitions: &'wire [wire::ProviderBindingDefinition],
    envelope: (usize, Option<ProviderBindingProjectionLimits>),
    control: &'control dyn PureCompileControl,
    joint: bool,
    mut admit: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedProviderBindings<'wire, 'control>, Error> {
    let (source, limits) = envelope;
    let observed = admit.is_some();
    let add = |a: usize, b: usize| {
        numerical(
            a.checked_add(b),
            observed,
            "provider binding resource sum overflow",
        )
    };
    if limits.is_some_and(|limits| definitions.len() > limits.max_definitions) {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let roots = bytes::<wire::ProviderBindingDefinition>(definitions.len())?;
    let mut known = roots;
    let mut requests = Requests {
        observed: admit.is_some(),
        ..Requests::default()
    };
    requests.record(numerical_bytes::<usize>(
        definitions.len(),
        admit.is_some(),
    )?)?;
    requests.record(numerical_bytes::<ConnectorReadBinding>(
        definitions.len(),
        admit.is_some(),
    )?)?;
    if joint {
        requests.record(numerical_bytes::<ConnectorWriteBinding>(
            definitions.len(),
            admit.is_some(),
        )?)?;
    }
    prefix(
        definitions.len(),
        0,
        &requests,
        (source, limits),
        &mut admit,
    )?;
    if let Some(limits) = limits {
        cap(definitions.len(), limits.max_definitions, work)?;
    }
    floor(source, roots, work)?;
    let mut input_bytes = 0;
    for definition in definitions {
        let catalog = definition
            .catalog
            .as_ref()
            .ok_or_else(|| invalid("provider binding catalog is absent"));
        let catalog = match catalog {
            Ok(v) => v,
            Err(e) => {
                work.step()?;
                return Err(e);
            }
        };
        input_bytes = add(
            input_bytes,
            add(
                definition.provider_id.len(),
                add(definition.instance_id.len(), catalog.catalog_name.len())?,
            )?,
        )?;
        known = add(
            known,
            add(
                definition.provider_id.capacity(),
                add(
                    definition.instance_id.capacity(),
                    add(catalog.catalog_name.capacity(), catalog.version.capacity())?,
                )?,
            )?,
        )?;
        for n in [
            definition.provider_id.len(),
            definition.instance_id.len(),
            catalog.catalog_name.len(),
        ] {
            requests.record(arc_str_bytes_for_mode(n, admit.is_some())?)?;
        }
        prefix(
            definitions.len(),
            input_bytes,
            &requests,
            (source, limits),
            &mut admit,
        )?;
        work.step()?; // Original catalog-presence observation.
        let version = catalog.version.len() == 32;
        work.step()?;
        if !version {
            return Err(invalid(
                "provider binding catalog version is not exactly 32 bytes",
            ));
        }
        work.step()?;
    }
    floor(source, known, work)?;
    let facts = facts(
        definitions.len(),
        input_bytes,
        requests,
        source,
        limits,
        &mut admit,
        work,
    )?;
    let indices = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    let mut output = reserve(definitions.len(), work)?;
    let mut write_output = if joint {
        reserve(definitions.len(), work)?
    } else {
        Vec::new()
    };
    for definition in definitions {
        let catalog = catalog(definition, work)?;
        work.flush()?;
        let provider_id = ConnectorProviderId::parse(&definition.provider_id);
        work.flush()?;
        let provider_id = provider_id.map_err(Error::Identity)?;
        work.flush()?;
        let instance_id = ConnectorInstanceId::try_from_canonical(&definition.instance_id);
        work.flush()?;
        let instance_id = instance_id.map_err(Error::Identity)?;
        work.flush()?;
        let catalog_name = ConnectorInstanceId::try_from_canonical(&catalog.catalog_name);
        work.flush()?;
        let catalog_name = catalog_name.map_err(Error::Identity)?;
        let version =
            CatalogVersion::from_bytes(catalog.version.as_slice().try_into().map_err(|_| {
                invalid("provider binding catalog version is not exactly 32 bytes")
            })?);
        let descriptor = ConnectorInstanceDescriptor {
            provider_id,
            instance_id,
        };
        let handle = CatalogHandle::new(catalog_name, version);
        if joint {
            // Cloning these metadata carriers increments the same three Arcs;
            // it does not parse, allocate identity backing or grant capability.
            write_output.push(ConnectorWriteBinding::new(
                descriptor.clone(),
                handle.clone(),
            ));
            work.step()?;
        }
        output.push(ConnectorReadBinding::new(descriptor, handle));
        work.step()?;
    }
    Ok(DecodedProviderBindings {
        wire: definitions,
        bindings: output,
        write_bindings: write_output,
        indices,
        facts,
        control,
        original_source_bytes: source,
    })
}

/// Same original metadata author on the caller's existing scope. Facts replace
/// this component's prior snapshot; the complete original source B occurs once.
pub fn encode_provider_bindings_in<'source, 'control>(
    inputs: &'source [(u32, &'source ConnectorReadBinding)],
    source: usize,
    limits: impl Into<Option<ProviderBindingProjectionLimits>>,
    admit: &mut impl FnMut(&ProviderBindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<EncodedProviderBindings<'source, 'control>, Error> {
    let limits = limits.into();
    encode_core(
        BindingSources::Read(inputs),
        source,
        limits,
        work.control(),
        Some(admit),
        work,
    )
}
pub fn encode_joint_provider_bindings_in<'source, 'control>(
    inputs: &'source [(u32, ProviderBindingSource<'source>)],
    source: usize,
    limits: impl Into<Option<ProviderBindingProjectionLimits>>,
    admit: &mut impl FnMut(&ProviderBindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<EncodedProviderBindings<'source, 'control>, Error> {
    let limits = limits.into();
    encode_core(
        BindingSources::Joint(inputs),
        source,
        limits,
        work.control(),
        Some(admit),
        work,
    )
}
pub fn decode_provider_bindings_in<'wire, 'control>(
    definitions: &'wire [wire::ProviderBindingDefinition],
    source: usize,
    limits: impl Into<Option<ProviderBindingProjectionLimits>>,
    admit: &mut impl FnMut(&ProviderBindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<DecodedProviderBindings<'wire, 'control>, Error> {
    let limits = limits.into();
    decode_core(
        definitions,
        (source, limits),
        work.control(),
        false,
        Some(admit),
        work,
    )
}
pub fn decode_joint_provider_bindings_in<'wire, 'control>(
    definitions: &'wire [wire::ProviderBindingDefinition],
    source: usize,
    limits: impl Into<Option<ProviderBindingProjectionLimits>>,
    admit: &mut impl FnMut(&ProviderBindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<DecodedProviderBindings<'wire, 'control>, Error> {
    let limits = limits.into();
    decode_core(
        definitions,
        (source, limits),
        work.control(),
        true,
        Some(admit),
        work,
    )
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod joint_tests;
