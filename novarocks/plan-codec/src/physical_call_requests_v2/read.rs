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

use super::{
    CallRequestAdmit, CallRequestCodecError as E, CallRequestProjectionFacts as Facts,
    CallRequestProjectionLimits as Limits, finish,
};
use crate::{
    allocation_exit_v2::reserve_exit,
    borrowed_type_resources::{type_binding_prefix_work_upper_bound_in, verify_type_binding},
    physical_type_v2::{
        DecodedTypeTable, TypeCodecError, ValueTypeCloneFacts, clone_value_type_observed,
        preflight_value_type_clone, preflight_value_type_clone_admitted,
        value_type_clone_root_facts,
    },
};
use novarocks_expression_contract::PhysicalCallRequest;
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, PhysicalCallSite, StaticFunctionArgument,
};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, MAX_VALUE_TYPE_DEPTH,
    MAX_VALUE_TYPE_NODES, PureCompileControl,
};
use std::{alloc::Layout, mem::size_of};

/// The package borrows its own complete type owner.
#[derive(Clone, Copy)]
pub enum CallRequestTypeOwner<'a> {
    Package(&'a DecodedTypeTable),
}
impl CallRequestTypeOwner<'_> {
    fn len(self) -> usize {
        match self {
            Self::Package(types) => types.value_types().len(),
        }
    }
    fn inline_bytes(self) -> usize {
        match self {
            Self::Package(types) => std::mem::size_of_val(types),
        }
    }
}
impl<'a> CallRequestTypeOwner<'a> {
    fn lookup(
        self,
        id: u32,
        _w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a FunctionValueType>, E> {
        match self {
            Self::Package(types) => Ok(types.value_type(id)),
        }
    }
}

trait ReadFailure:
    From<E>
    + From<TypeCodecError>
    + From<CompileControlError>
    + From<novarocks_physical_plan::ConstantReferenceError>
    + From<novarocks_constant_contract::ConstantTableError>
{
    fn is_primary_refusal(&self) -> bool;
}
impl ReadFailure for E {
    fn is_primary_refusal(&self) -> bool {
        matches!(self, Self::Control(_))
    }
}
impl<H> ReadFailure for crate::host_projection_v2::ProjectionFailure<E, H> {
    fn is_primary_refusal(&self) -> bool {
        matches!(self, Self::Host(_) | Self::Codec(E::Control(_)))
    }
}
type Parent<'a, F> = dyn FnMut(&Facts) -> Result<(), F> + 'a;

/// This token loans the original DTO, checked type roots and admitted pools.
/// Its raw output still requires the original Fragment coverage constructor.
pub struct PreparedCallRequestsDecode<'loan, W: RequestWire = wire::OriginalCallRequest> {
    source: &'loan [W],
    types: CallRequestTypeOwner<'loan>,
    _pools: &'loan ConstantPools,
    control: &'loan dyn PureCompileControl,
    facts: Facts,
    _keys: Vec<W::Key>,
    creation_storage: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
}
impl<W: RequestWire> PreparedCallRequestsDecode<'_, W> {
    pub fn facts(&self) -> &Facts {
        &self.facts
    }
}
fn invalid(message: &'static str) -> E {
    E::InvalidShape(message)
}
fn resource() -> E {
    E::Control(CompileControlError::ResourceExhausted)
}
fn add(a: usize, b: usize) -> Result<usize, E> {
    a.checked_add(b).ok_or_else(resource)
}
fn mul(a: usize, b: usize) -> Result<usize, E> {
    a.checked_mul(b).ok_or_else(resource)
}
fn bytes<T>(n: usize) -> Result<usize, E> {
    Layout::array::<T>(n)
        .map(|l| l.size())
        .map_err(|_| resource())
}
fn gate(value: usize, maximum: usize) -> Result<(), E> {
    if value > maximum {
        Err(resource())
    } else {
        Ok(())
    }
}
// A numerical refusal is already a primary resource cause; never observe a
// later callback which could replace it. Ordinary source-shape outcomes still
// record their completed calculation before the encompassing footer.
fn observed<T, F: ReadFailure>(
    result: Result<T, F>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<T, F> {
    if result
        .as_ref()
        .is_err_and(|error| error.is_primary_refusal())
    {
        return result;
    }
    w.step()?;
    result
}
fn source_gate(source: usize, known: usize) -> Result<(), E> {
    if source < known {
        Err(invalid(
            "call request source invoice omits original backing",
        ))
    } else {
        Ok(())
    }
}
fn required<T: Copy>(value: Option<T>, message: &'static str) -> Result<T, E> {
    value.ok_or_else(|| invalid(message))
}
fn relation_definition(input: &wire::OriginalCallRequest) -> Result<PhysicalCallSite, E> {
    use wire::call_request_definition::Kind;
    match input.definition.as_ref().and_then(|d| d.kind.as_ref()) {
        Some(Kind::Relational(site)) => Ok(crate::physical_semantics_v2::decode_call_site(site)?),
        None => Err(invalid("call request definition is absent")),
    }
}

fn reference(input: &wire::ConstantReference) -> Result<ConstantReference, E> {
    Ok(ConstantReference {
        pool: ConstantPoolId::new(required(
            input.pool_id,
            "request constant pool ID is absent",
        )?),
        ordinal: input.row_ordinal,
    })
}
fn value<'a>(
    types: CallRequestTypeOwner<'a>,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, E> {
    w.flush()?;
    let found = types.lookup(id, w)?;
    w.step()?;
    w.flush()?;
    found.ok_or_else(|| invalid("request value type ID is absent from the original table"))
}
fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, E> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut output = Vec::new();
    if n != 0 {
        let result = output.try_reserve_exact(n);
        reserve_exit::<E>(result, w)?;
    }
    Ok(output)
}
fn boxed<T>(output: Vec<T>, w: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, E> {
    // Conversion can replace an over-capacity Vec allocation. Both requests
    // and their overlap are admitted, independently of allocator usable size.
    w.flush()?;
    let output = output.into_boxed_slice();
    w.step()?;
    w.flush()?;
    Ok(output)
}

#[derive(Clone, Copy)]
enum Profile {
    Derived,
    Receiving(Limits),
}
impl Profile {
    fn from_limits(value: impl Into<Option<Limits>>) -> Self {
        match value.into() {
            Some(value) => Self::Receiving(value),
            None => Self::Derived,
        }
    }
}
impl Profile {
    fn remaining_work(self, used: usize) -> Result<Option<usize>, E> {
        match self {
            Self::Derived => Ok(None),
            Self::Receiving(limits) => limits
                .max_work
                .checked_sub(used)
                .map(Some)
                .ok_or_else(resource),
        }
    }
}

struct Model {
    facts: Facts,
    arguments: usize,
    parameters: usize,
    comparisons: usize,
    known: usize,
    maximum_type_backing: usize,
    source: usize,
    types: usize,
    pools: usize,
    raw_work: usize,
    peak_work: usize,
    output_map_work: usize,
}
impl Model {
    fn request<T>(&mut self, n: usize, copies: usize) -> Result<(), E> {
        let b = bytes::<T>(n)?;
        self.facts.request_bytes_upper_bound =
            add(self.facts.request_bytes_upper_bound, mul(b, copies)?)?;
        if b != 0 {
            self.facts.allocation_requests_upper_bound =
                add(self.facts.allocation_requests_upper_bound, copies)?;
        }
        Ok(())
    }
    fn admit<F: ReadFailure>(
        &mut self,
        limits: Profile,
        parent: &mut Option<&mut Parent<'_, F>>,
    ) -> Result<(), F> {
        // Original checked clone grammar bounds its fixed borrowed scratch and
        // all possible visited owned Dictionary nodes BEFORE that helper runs.
        // This deliberately conservative per-reference envelope is not an
        // invented type cap or a claim about allocator/library cooperation.
        let scratch =
            bytes::<Option<(&arrow::datatypes::DataType, usize)>>(MAX_VALUE_TYPE_DEPTH + 1)?;
        let clone = add(scratch, add(mul(MAX_VALUE_TYPE_NODES, 16)?, 64)?)?;
        let height = (usize::BITS - self.facts.definition_count.leading_zeros()) as usize;
        let index = mul(
            self.facts.definition_count,
            add(64, mul(add(height, 1)?, 32)?)?,
        )?;
        let lookups = add(add(self.types, self.pools)?, 16)?;
        let references = mul(
            self.facts.type_reference_count,
            add(clone, mul(lookups, 4)?)?,
        )?;
        let source_visits = mul(add(self.types, self.pools)?, 32)?;
        let arguments = mul(self.arguments, 64)?;
        let parameters = mul(self.parameters, 16)?;
        let movement = mul(self.facts.request_bytes_upper_bound, 4)?;
        let total = add(index, source_visits)?;
        let total = add(total, add(arguments, parameters)?)?;
        let total = add(total, add(references, self.comparisons)?)?;
        self.raw_work = add(self.output_map_work, add(128, add(total, movement)?)?)?;
        self.facts.cumulative_work_upper_bound = self.raw_work;
        if parent.is_some() {
            self.peak_work = self.peak_work.max(self.raw_work);
            self.facts.cumulative_work_upper_bound = self.peak_work;
        }
        self.facts.coexisting_source_and_request_bytes_upper_bound = add(
            add(self.source, size_of::<PreparedCallRequestsDecode<'_>>())?,
            self.facts.request_bytes_upper_bound,
        )?;
        if let Profile::Receiving(limits) = limits {
            gate(self.facts.definition_count, limits.max_definitions)?;
            gate(self.facts.type_reference_count, limits.max_type_references)?;
            gate(
                self.facts.request_bytes_upper_bound,
                limits.max_request_bytes,
            )?;
            gate(
                self.facts.allocation_requests_upper_bound,
                limits.max_allocation_requests,
            )?;
            gate(
                self.facts.coexisting_source_and_request_bytes_upper_bound,
                limits.max_coexisting_source_and_request_bytes,
            )?;
            gate(self.facts.cumulative_work_upper_bound, limits.max_work)?;
        }
        source_gate(self.source, add(self.known, self.maximum_type_backing)?)?;
        if let Some(parent) = parent.as_deref_mut() {
            parent(&self.facts)?;
        }
        Ok(())
    }
    fn type_reference<F: ReadFailure>(
        &mut self,
        ty: &FunctionValueType,
        limits: Profile,
        parent: &mut Option<&mut Parent<'_, F>>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), F> {
        if parent.is_some() {
            let requests = self.facts.allocation_requests_upper_bound;
            let bytes = self.facts.request_bytes_upper_bound;
            let mut capture = |facts: ValueTypeCloneFacts, _: &mut CompileCheckpoints<'_>| {
                self.facts.allocation_requests_upper_bound =
                    add(requests, facts.allocation_requests_upper_bound())?;
                self.facts.request_bytes_upper_bound =
                    add(bytes, facts.allocation_request_bytes_upper_bound())?;
                self.maximum_type_backing = self
                    .maximum_type_backing
                    .max(facts.allocation_request_bytes_upper_bound());
                self.admit(limits, parent)
            };
            capture(value_type_clone_root_facts(ty)?, w)?;
            preflight_value_type_clone_admitted(ty, &mut capture, w)?;
            return observed(self.admit(limits, parent), w);
        }
        let facts = preflight_value_type_clone(ty, w)?;
        self.facts.allocation_requests_upper_bound = add(
            self.facts.allocation_requests_upper_bound,
            facts.allocation_requests_upper_bound(),
        )?;
        self.facts.request_bytes_upper_bound = add(
            self.facts.request_bytes_upper_bound,
            facts.allocation_request_bytes_upper_bound(),
        )?;
        self.maximum_type_backing = self
            .maximum_type_backing
            .max(facts.allocation_request_bytes_upper_bound());
        // The numerical helper also describes the ensuing sole clone. The
        // admitted grammar envelope above covers its complete own walk.
        observed(self.admit(limits, parent), w)
    }
    fn value<'a, F: ReadFailure>(
        &mut self,
        types: CallRequestTypeOwner<'a>,
        id: u32,
        limits: Profile,
        parent: &mut Option<&mut Parent<'_, F>>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a FunctionValueType, F> {
        if parent.is_none() {
            let ty = value(types, id, w)?;
            self.type_reference(ty, limits, parent, w)?;
            return Ok(ty);
        }
        // The model has admitted the opaque lookup work. Capture its actual
        // root and admit the sole clone before any pending observation; Plain
        // retains its original pre-lookup flush in value() above.
        let ty = types.lookup(id, w)?;
        if let Some(ty) = ty {
            self.type_reference(ty, limits, parent, w)?;
        }
        w.step()?;
        w.flush()?;
        ty.ok_or_else(|| {
            F::from(invalid(
                "request value type ID is absent from the original table",
            ))
        })
    }
    fn compare<F: ReadFailure>(
        &mut self,
        left: &FunctionValueType,
        right: &FunctionValueType,
        limits: Profile,
        parent: &mut Option<&mut Parent<'_, F>>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, F> {
        let remaining = limits.remaining_work(self.raw_work)?;
        let baseline = self.comparisons;
        let prefix = type_binding_prefix_work_upper_bound_in(left, right, self.source)?;
        self.comparisons = add(baseline, prefix.work_upper_bound())?;
        self.admit(limits, parent)?;
        let compared = crate::borrowed_type_resources::verify_type_binding_optional_work_in(
            left,
            right,
            self.source,
            remaining,
            &mut |facts| {
                self.comparisons = add(baseline, facts.work_upper_bound())?;
                self.admit(limits, parent)
            },
            w,
        )?;
        self.comparisons = add(baseline, compared.work_upper_bound())?;
        self.admit(limits, parent)?;
        Ok(compared.matches())
    }
}
fn capture_arguments<F: ReadFailure, W: RequestWire>(
    entry: &W,
    model: &mut Model,
    limits: Profile,
    parent: &mut Option<&mut Parent<'_, F>>,
) -> Result<(), F> {
    model.arguments = add(model.arguments, entry.arguments().len())?;
    model.known = add(
        model.known,
        bytes::<wire::OriginalFunctionArgument>(entry.arguments().capacity())?,
    )?;
    model.request::<StaticFunctionArgument>(entry.arguments().len(), 2)?;
    if parent.is_some() {
        model.admit(limits, parent)?;
    }
    Ok(())
}
fn preflight<F: ReadFailure, W: RequestWire>(
    source: &[W],
    source_capacity: usize,
    types: CallRequestTypeOwner<'_>,
    pools: &ConstantPools,
    invoice: usize,
    limits: Profile,
    parent: &mut Option<&mut Parent<'_, F>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Facts, F> {
    let n = source.len();
    let type_count = types.len();
    let pool_count = pools.entries().len();
    let raw = add(size_of::<Vec<W>>(), bytes::<W>(source_capacity)?)?;
    let type_roots = mul(
        type_count,
        add(size_of::<u32>(), size_of::<FunctionValueType>())?,
    )?;
    let type_roots = add(types.inline_bytes(), type_roots)?;
    let pool_roots = mul(
        pool_count,
        add(
            size_of::<ConstantPoolId>(),
            size_of::<novarocks_physical_plan::ConstantPool>(),
        )?,
    )?;
    let pool_roots = add(size_of::<ConstantPools>(), pool_roots)?;
    let known = add(raw, add(type_roots, pool_roots)?)?;
    let mut model = Model {
        facts: Facts {
            definition_count: n,
            type_reference_count: 0,
            allocation_requests_upper_bound: 0,
            request_bytes_upper_bound: 0,
            coexisting_source_and_request_bytes_upper_bound: 0,
            cumulative_work_upper_bound: 0,
        },
        arguments: 0,
        parameters: 0,
        comparisons: 0,
        known,
        maximum_type_backing: 0,
        source: invoice,
        types: type_count,
        pools: pool_count,
        raw_work: 0,
        peak_work: 0,
        output_map_work: 0,
    };
    model.request::<W::Key>(n, 1)?;
    model.request::<(W::Key, PhysicalCallRequest)>(n, 1)?;
    if W::MAP_OUTPUT {
        let tree = crate::btree_resources_v2::insertion_only::<W::Key, PhysicalCallRequest>(n)
            .map_err(|error| match error {
                crate::btree_resources_v2::BTreeResourceError::Arithmetic(_) => resource(),
                crate::btree_resources_v2::BTreeResourceError::SourceModel(message) => {
                    invalid(message)
                }
            })?;
        model.facts.allocation_requests_upper_bound = add(
            model.facts.allocation_requests_upper_bound,
            tree.allocation_requests_upper_bound,
        )?;
        model.facts.request_bytes_upper_bound = add(
            model.facts.request_bytes_upper_bound,
            tree.request_bytes_upper_bound,
        )?;
        model.output_map_work = tree.cumulative_work_upper_bound;
    }
    observed(model.admit(limits, parent), w)?;
    // Only the explicit bounded fixture accepts a retained-source invoice.
    // The production request links to the original, already owned pools.
    if limits.is_some() {
        let mut maximum_pool_backing = 0;
        for pool in pools.entries().values() {
            let retained = usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
                .map_err(|_| resource())?;
            maximum_pool_backing = maximum_pool_backing.max(add(
                novarocks_physical_plan::ConstantPool::backing_allocation_layout().size(),
                retained,
            )?);
            observed(
                source_gate(invoice, add(model.known, maximum_pool_backing)?),
                w,
            )?;
        }
        model.known = add(model.known, maximum_pool_backing)?;
    }
    for entry in source {
        if parent.is_some() {
            capture_arguments(entry, &mut model, limits, parent)?;
        }
        let key = entry.definition();
        w.step()?;
        key?;
        let logical = required(
            entry.logical_argument_count(),
            "request logical argument count is absent",
        );
        w.step()?;
        let logical = usize::try_from(logical?).map_err(|_| resource())?;
        let valid = logical <= entry.arguments().len();
        w.step()?;
        if !valid {
            return Err(invalid("request logical argument count exceeds actual channels").into());
        }
        if parent.is_none() {
            capture_arguments(entry, &mut model, limits, parent)?;
        }
        observed(model.admit(limits, parent), w)?;
        for argument in entry.arguments() {
            match argument.kind.as_ref() {
                Some(wire::original_function_argument::Kind::Value(input)) => {
                    model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
                    observed(model.admit(limits, parent), w)?;
                    let id = required(
                        input.value_type_id,
                        "request argument value type ID is absent",
                    );
                    w.step()?;
                    let ty = model.value(types, id?, limits, parent, w)?;
                    if let Some(input) = &input.constant {
                        let address = observed(reference(input), w)?;
                        let matches = if parent.is_some() {
                            w.flush()?;
                            let mut matches = false;
                            let found = pools.resolve_source_captured_observed(
                                address,
                                &mut |found, w| {
                                    matches =
                                        model.compare(ty, found.value_type(), limits, parent, w)?;
                                    Ok::<_, F>(())
                                },
                                w,
                            );
                            let found = observed(found, w)?;
                            w.flush()?;
                            // Selected handles share the admitted original pool.
                            // No literal factory or replacement backing is used.
                            let _ = found;
                            matches
                        } else {
                            w.flush()?;
                            let found = pools.resolve_source_observed(address, w).map_err(F::from);
                            let found = observed(found, w);
                            if found
                                .as_ref()
                                .is_err_and(|error| error.is_primary_refusal())
                            {
                                return found.map(|_| model.facts);
                            }
                            w.flush()?;
                            let found = found?;
                            let remaining =
                                limits.remaining_work(model.facts.cumulative_work_upper_bound)?;
                            let compared = match remaining {
                                Some(maximum) => verify_type_binding(ty,found.value_type(),invoice,maximum,w)?,
                                None => crate::borrowed_type_resources::verify_type_binding_optional_work_in::<TypeCodecError>(
                                    ty,found.value_type(),invoice,None,&mut |_| Ok(()),w)?,
                            };
                            model.comparisons =
                                add(model.comparisons, compared.work_upper_bound())?;
                            observed(model.admit(limits, parent), w)?;
                            compared.matches()
                        };
                        w.step()?;
                        if !matches {
                            return Err(novarocks_physical_plan::ConstantReferenceError::Table(
                                novarocks_constant_contract::ConstantTableError::SourceTypeMismatch(
                                    address,
                                ),
                            )
                            .into());
                        }
                    }
                }
                Some(wire::original_function_argument::Kind::Lambda(input)) => {
                    let count = add(input.parameter_value_type_ids.len(), 1)?;
                    model.facts.type_reference_count =
                        add(model.facts.type_reference_count, count)?;
                    model.parameters = add(model.parameters, input.parameter_value_type_ids.len())?;
                    model.known = add(
                        model.known,
                        bytes::<u32>(input.parameter_value_type_ids.capacity())?,
                    )?;
                    model.request::<FunctionValueType>(input.parameter_value_type_ids.len(), 2)?;
                    if !input.parameter_value_type_ids.is_empty() {
                        let layout=novarocks_type_contract::FunctionParameterTypes::storage_allocation_layout().map_err(|_|resource())?;
                        model.facts.allocation_requests_upper_bound =
                            add(model.facts.allocation_requests_upper_bound, 1)?;
                        model.facts.request_bytes_upper_bound =
                            add(model.facts.request_bytes_upper_bound, layout.size())?;
                    }
                    observed(model.admit(limits, parent), w)?;
                    for &id in &input.parameter_value_type_ids {
                        model.value(types, id, limits, parent, w)?;
                        w.step()?;
                    }
                    let id = required(
                        input.result_value_type_id,
                        "request lambda result type ID is absent",
                    );
                    w.step()?;
                    model.value(types, id?, limits, parent, w)?;
                }
                None => {
                    w.step()?;
                    return Err(invalid("request argument kind is absent").into());
                }
            }
            w.step()?;
        }
        if let Some(id) = entry.expected_result_value_type_id() {
            model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
            observed(model.admit(limits, parent), w)?;
            model.value(types, id, limits, parent, w)?;
        }
        w.step()?;
    }
    Ok(model.facts)
}

// Same bounded heap-index geometry as the original BindingIndex, on complete
// definition keys rather than dense/max-ID storage. Each comparison/move is
// observed; there is no uninterruptible std sort or hidden key dictionary.
fn sift<K: Copy + Ord>(
    keys: &mut [K],
    mut root: usize,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    loop {
        let left = add(mul(root, 2)?, 1)?;
        w.step()?;
        if left >= keys.len() {
            return Ok(());
        }
        let right = add(left, 1)?;
        let mut child = left;
        if right < keys.len() {
            let greater = keys[right] > keys[left];
            w.step()?;
            if greater {
                child = right;
            }
        }
        let greater = keys[child] > keys[root];
        w.step()?;
        if !greater {
            return Ok(());
        }
        keys.swap(root, child);
        w.step()?;
        root = child;
    }
}
fn index<W: RequestWire>(source: &[W], w: &mut CompileCheckpoints<'_>) -> Result<Vec<W::Key>, E> {
    let mut keys = reserve(source.len(), w)?;
    for entry in source {
        let key = entry.definition();
        w.step()?;
        keys.push(key?);
    }
    for root in (0..keys.len() / 2).rev() {
        sift(&mut keys, root, w)?;
    }
    for end in (1..keys.len()).rev() {
        keys.swap(0, end);
        w.step()?;
        sift(&mut keys[..end], 0, w)?;
    }
    for pair in keys.windows(2) {
        let duplicate = pair[0] == pair[1];
        w.step()?;
        if duplicate {
            return Err(invalid("call request definition is duplicated"));
        }
    }
    Ok(keys)
}
fn prepare_entries_core<'loan, 'control: 'loan, F: ReadFailure, W: RequestWire>(
    source: &'loan [W],
    source_capacity: usize,
    types: CallRequestTypeOwner<'loan>,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: Profile,
    parent: &mut Option<&mut Parent<'_, F>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedCallRequestsDecode<'loan, W>, F> {
    let facts = preflight(
        source,
        source_capacity,
        types,
        pools,
        source_retained_bytes,
        limits,
        parent,
        work,
    )?;
    let keys = index(source, work)?;
    work.step()?;
    Ok(PreparedCallRequestsDecode {
        source,
        types,
        _pools: pools,
        control: work.control(),
        facts,
        _keys: keys,
        creation_storage: None,
    })
}
fn prepare_core<'loan, 'control: 'loan, F: ReadFailure>(
    source: Option<&'loan wire::FragmentCallRequests>,
    types: CallRequestTypeOwner<'loan>,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: Profile,
    parent: &mut Option<&mut Parent<'_, F>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedCallRequestsDecode<'loan>, F> {
    let source = source.ok_or_else(|| invalid("fragment call request table is absent"))?;
    prepare_entries_core(
        &source.entries,
        source.entries.capacity(),
        types,
        pools,
        source_retained_bytes,
        limits,
        parent,
        work,
    )
}

pub fn prepare_call_requests_decode<'loan>(
    source: Option<&'loan wire::FragmentCallRequests>,
    types: &'loan DecodedTypeTable,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: impl Into<Option<Limits>>,
    control: &'loan dyn PureCompileControl,
) -> Result<PreparedCallRequestsDecode<'loan>, E> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = prepare_core(
        source,
        CallRequestTypeOwner::Package(types),
        pools,
        source_retained_bytes,
        Profile::from_limits(limits),
        &mut None::<&mut Parent<'_, E>>,
        &mut work,
    );
    finish(work, result)
}

/// Same receiver author on the caller's original meter. The parent replaces
/// the complete child contribution; its source invoice is included once.
pub(crate) fn prepare_call_requests_decode_in<'loan, 'control: 'loan>(
    source: Option<&'loan wire::FragmentCallRequests>,
    types: &'loan DecodedTypeTable,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: impl Into<Option<Limits>>,
    admit: &mut CallRequestAdmit<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedCallRequestsDecode<'loan>, E> {
    prepare_core(
        source,
        CallRequestTypeOwner::Package(types),
        pools,
        source_retained_bytes,
        Profile::from_limits(limits),
        &mut Some(&mut |facts: &Facts| admit(facts).map_err(E::from)),
        work,
    )
}

pub fn prepare_call_requests_decode_with_creator_in<'loan, 'control: 'loan, H>(
    source: Option<&'loan wire::FragmentCallRequests>,
    types: CallRequestTypeOwner<'loan>,
    pools: &'loan ConstantPools,
    original_source_bytes: usize,
    limits: impl Into<Option<Limits>>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedCallRequestsDecode<'loan>, crate::host_projection_v2::ProjectionFailure<E, H>> {
    prepare_with_creator_core(
        source,
        types,
        pools,
        original_source_bytes,
        Profile::from_limits(limits),
        creation,
        work,
    )
}

fn prepare_with_creator_core<'loan, 'control: 'loan, H>(
    source: Option<&'loan wire::FragmentCallRequests>,
    types: CallRequestTypeOwner<'loan>,
    pools: &'loan ConstantPools,
    original_source_bytes: usize,
    limits: Profile,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedCallRequestsDecode<'loan>, crate::host_projection_v2::ProjectionFailure<E, H>> {
    let mut previous = 0usize;
    let mut token = prepare_core::<crate::host_projection_v2::ProjectionFailure<E, H>>(
        source,
        types,
        pools,
        original_source_bytes,
        limits,
        &mut Some(&mut |facts: &Facts| {
            let additional = facts
                .request_bytes_upper_bound
                .checked_sub(previous)
                .ok_or_else(|| {
                    crate::host_projection_v2::ProjectionFailure::<E, H>::from(resource())
                })?;
            if additional != 0 {
                creation
                    .observe_opaque_upper_bound(additional)
                    .map_err(crate::host_projection_v2::ProjectionFailure::Host)?;
            }
            previous = facts.request_bytes_upper_bound;
            Ok(())
        }),
        work,
    )?;
    token.creation_storage = Some(creation.storage_lifetime());
    Ok(token)
}

fn decode_core<W: RequestWire>(
    token: PreparedCallRequestsDecode<'_, W>,
    work: &mut CompileCheckpoints<'_>,
    storage: Option<&novarocks_type_contract::ValueTypeStorageLifetime>,
) -> Result<Vec<(W::Key, PhysicalCallRequest)>, E> {
    let mut output = reserve(token.source.len(), work)?;
    for entry in token.source {
        let key = entry.definition();
        work.step()?;
        let mut arguments = reserve(entry.arguments().len(), work)?;
        for argument in entry.arguments() {
            let argument = match argument.kind.as_ref() {
                Some(wire::original_function_argument::Kind::Value(input)) => {
                    let id = required(
                        input.value_type_id,
                        "request argument value type ID is absent",
                    );
                    work.step()?;
                    let ty = clone_value_type_observed(value(token.types, id?, work)?, work)?;
                    let constant = match &input.constant {
                        Some(input) => {
                            let address = reference(input);
                            work.step()?;
                            Some(address?)
                        }
                        None => None,
                    };
                    StaticFunctionArgument::Value {
                        value_type: ty,
                        constant,
                    }
                }
                Some(wire::original_function_argument::Kind::Lambda(input)) => {
                    let mut parameters = reserve(input.parameter_value_type_ids.len(), work)?;
                    for &id in &input.parameter_value_type_ids {
                        parameters.push(clone_value_type_observed(
                            value(token.types, id, work)?,
                            work,
                        )?);
                        work.step()?;
                    }
                    let id = required(
                        input.result_value_type_id,
                        "request lambda result type ID is absent",
                    );
                    work.step()?;
                    let result_type =
                        clone_value_type_observed(value(token.types, id?, work)?, work)?;
                    StaticFunctionArgument::Lambda {
                        parameter_types: {
                            let boxed = boxed(parameters, work)?;
                            match storage {
                                Some(storage) => {
                                    novarocks_type_contract::FunctionParameterTypes::from_boxed_in(
                                        boxed,
                                        storage.clone(),
                                    )
                                }
                                None => {
                                    novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                        boxed,
                                    )
                                }
                            }
                        },
                        result_type,
                    }
                }
                None => {
                    work.step()?;
                    return Err(invalid("request argument kind is absent"));
                }
            };
            arguments.push(argument);
            work.step()?;
        }
        let expected_result_type = match entry.expected_result_value_type_id() {
            Some(id) => Some(clone_value_type_observed(
                value(token.types, id, work)?,
                work,
            )?),
            None => None,
        };
        let logical = required(
            entry.logical_argument_count(),
            "request logical argument count is absent",
        );
        work.step()?;
        let logical_argument_count = usize::try_from(logical?).map_err(|_| resource())?;
        output.push((
            key?,
            PhysicalCallRequest {
                arguments: boxed(arguments, work)?,
                logical_argument_count,
                expected_result_type,
            },
        ));
        work.step()?;
    }
    work.step()?;
    Ok(output)
}
pub fn decode_call_requests(
    token: PreparedCallRequestsDecode<'_>,
) -> Result<Vec<(PhysicalCallSite, PhysicalCallRequest)>, E> {
    let mut work = CompileCheckpoints::try_new(token.control, CompilePhase::Decode)?;
    let result = decode_core(token, &mut work, None);
    finish(work, result)
}

pub(crate) fn decode_call_requests_in(
    token: PreparedCallRequestsDecode<'_>,
    admit: &mut CallRequestAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<(PhysicalCallSite, PhysicalCallRequest)>, E> {
    if !std::ptr::addr_eq(token.control, work.control()) {
        return Err(invalid(
            "call request prepared controller differs from caller",
        ));
    }
    // Immutable preparation bills every ensuing copy. The complete parent
    // gate precedes the first real output reserve or opaque clone boundary.
    admit(&token.facts)?;
    decode_core(token, work, None)
}
pub struct DecodedCallRequests {
    entries: Vec<(PhysicalCallSite, PhysicalCallRequest)>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl DecodedCallRequests {
    pub fn entries(&self) -> &[(PhysicalCallSite, PhysicalCallRequest)] {
        &self.entries
    }
    pub fn into_parts(
        self,
    ) -> (
        Vec<(PhysicalCallSite, PhysicalCallRequest)>,
        novarocks_type_contract::ValueTypeStorageLifetime,
    ) {
        (self.entries, self.storage)
    }
}
pub fn decode_call_requests_with_creator_in<H>(
    mut token: PreparedCallRequestsDecode<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedCallRequests, crate::host_projection_v2::ProjectionFailure<E, H>> {
    if !std::ptr::addr_eq(token.control, work.control()) {
        return Err(invalid("call request prepared controller differs from caller").into());
    }
    let storage = token.creation_storage.take().ok_or_else(|| {
        crate::host_projection_v2::ProjectionFailure::<E, H>::from(invalid(
            "request token has no original construction owner",
        ))
    })?;
    // Preparation admitted all ensuing output requests before any actual copy.
    let entries = decode_core(token, work, Some(&storage))?;
    Ok(DecodedCallRequests { entries, storage })
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

/// Borrowed fields of the two disjoint request-address vocabularies. The
/// original grammar, type/constant checks and Creator projection stay shared.
pub trait RequestWire {
    type Key: Copy + Ord;
    const MAP_OUTPUT: bool;
    fn definition(&self) -> Result<Self::Key, E>;
    fn arguments(&self) -> &Vec<wire::OriginalFunctionArgument>;
    fn logical_argument_count(&self) -> Option<u32>;
    fn expected_result_value_type_id(&self) -> Option<u32>;
}
impl RequestWire for wire::OriginalCallRequest {
    type Key = PhysicalCallSite;
    const MAP_OUTPUT: bool = false;
    fn definition(&self) -> Result<Self::Key, E> {
        relation_definition(self)
    }
    fn arguments(&self) -> &Vec<wire::OriginalFunctionArgument> {
        &self.arguments
    }
    fn logical_argument_count(&self) -> Option<u32> {
        self.logical_argument_count
    }
    fn expected_result_value_type_id(&self) -> Option<u32> {
        self.expected_result_value_type_id
    }
}
impl RequestWire for wire::UnitCallRequest {
    type Key = novarocks_expression_contract::ExprId;
    const MAP_OUTPUT: bool = true;
    fn definition(&self) -> Result<Self::Key, E> {
        Ok(Self::Key::new(required(
            self.definition_id,
            "unit request definition is absent",
        )?))
    }
    fn arguments(&self) -> &Vec<wire::OriginalFunctionArgument> {
        &self.arguments
    }
    fn logical_argument_count(&self) -> Option<u32> {
        self.logical_argument_count
    }
    fn expected_result_value_type_id(&self) -> Option<u32> {
        self.expected_result_value_type_id
    }
}
type DecodedUnitRequests = (
    std::collections::BTreeMap<novarocks_expression_contract::ExprId, PhysicalCallRequest>,
    novarocks_type_contract::ValueTypeStorageLifetime,
    Facts,
);
pub(crate) fn decode_unit_requests_in<H>(
    source: &Vec<wire::UnitCallRequest>,
    types: &DecodedTypeTable,
    pools: &ConstantPools,
    invoice: usize,
    limits: impl Into<Option<Limits>>,
    observe: &mut CallRequestAdmit<'_>,
    mut creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedUnitRequests, crate::host_projection_v2::ProjectionFailure<E, H>> {
    use crate::host_projection_v2::ProjectionFailure as Failure;
    let storage = creation.as_ref().map_or_else(
        novarocks_type_contract::ValueTypeStorageLifetime::pure,
        |creation| creation.storage_lifetime(),
    );
    let mut previous = 0usize;
    let mut admit = |facts: &Facts| -> Result<(), Failure<E, H>> {
        observe(facts).map_err(E::from)?;
        let additional = facts
            .request_bytes_upper_bound
            .checked_sub(previous)
            .ok_or_else(|| Failure::from(resource()))?;
        if additional != 0
            && let Some(creation) = creation.as_deref_mut()
        {
            creation
                .observe_opaque_upper_bound(additional)
                .map_err(Failure::Host)?;
        }
        previous = facts.request_bytes_upper_bound;
        Ok(())
    };
    let token = prepare_entries_core(
        source,
        source.capacity(),
        CallRequestTypeOwner::Package(types),
        pools,
        invoice,
        Profile::from_limits(limits),
        &mut Some(&mut admit),
        work,
    )?;
    let facts = token.facts;
    let entries = decode_core(token, work, Some(&storage))?;
    let mut output = std::collections::BTreeMap::new();
    for (id, request) in entries {
        work.flush()?;
        output.insert(id, request);
        work.step()?;
        work.flush()?;
    }
    Ok((output, storage, facts))
}
