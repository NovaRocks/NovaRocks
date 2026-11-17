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
use crate::{
    allocation_exit_v2::reserve_exit,
    borrowed_type_resources::{
        type_binding_prefix_work_upper_bound_in, verify_type_binding,
        verify_type_binding_admitted_in,
    },
    physical_expression_v2::expression_tree_lookup_work,
    physical_node_v2 as resources,
    physical_semantics_v2::encode_call_site,
    physical_type_v2::EncodedTypeTable,
};
use novarocks_expression_contract::PhysicalCallRequest;
use novarocks_physical_plan::{
    ConstantPools, FragmentCallRequests, PhysicalCallSite, StaticFunctionArgument as Argument,
};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompilePhase, FunctionValueType, PureCompileControl};
use std::{collections::BTreeMap, mem::size_of};

type Error = CallRequestCodecError;
use crate::host_projection_v2::{AdmissionRefusal, ProjectionFailure};
type HostError<H> = ProjectionFailure<Error, H>;
type HostAdmit<'a, H> = dyn FnMut(&CallRequestProjectionFacts) -> Result<(), HostError<H>> + 'a;
fn project_admission<H>(error: AdmissionRefusal<H>) -> HostError<H> {
    match error {
        AdmissionRefusal::Control(cause) => cause.into(),
        AdmissionRefusal::Host(error) => ProjectionFailure::Host(error),
    }
}
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn numeric(error: resources::NodeCodecError) -> Error {
    match error {
        resources::NodeCodecError::Control(cause) => Error::Control(cause),
        _ => shape("request projection arithmetic or layout is unrepresentable"),
    }
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    resources::add(a, b).map_err(numeric)
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    resources::mul(a, b).map_err(numeric)
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    resources::bytes::<T>(n).map_err(numeric)
}
fn resource() -> Error {
    Error::Control(CompileControlError::ResourceExhausted)
}
fn admitted(value: usize, maximum: usize) -> Result<(), Error> {
    if value > maximum {
        Err(resource())
    } else {
        Ok(())
    }
}
fn checked_shape(
    ok: bool,
    message: &'static str,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    w.step()?;
    if ok { Ok(()) } else { Err(shape(message)) }
}

// This uses the shared checked numerical/layout algebra. The source floor is
// necessary storage only, not a full backing invoice or allocator grant.
struct Model<'parent, 'callback, H> {
    parent: Option<&'parent mut HostAdmit<'callback, H>>,
    work_peak: usize,
    facts: CallRequestProjectionFacts,
    items: usize,
    constants: usize,
    type_roots: usize,
    pool_lookup_work: usize,
    delegated: usize,
    source: usize,
    known: usize,
}
impl<'parent, 'callback, H> Model<'parent, 'callback, H> {
    fn new(
        source: usize,
        types: &EncodedTypeTable<'_>,
        pools: &ConstantPools,
        parent: Option<&'parent mut HostAdmit<'callback, H>>,
    ) -> Result<Self, Error> {
        let (roots, fields) = types.source_counts();
        let type_floor = add(
            size_of::<EncodedTypeTable<'_>>(),
            add(
                bytes::<(u32, FunctionValueType)>(roots)?,
                bytes::<(u32, std::sync::Arc<arrow::datatypes::Field>)>(fields)?,
            )?,
        )?;
        Ok(Self {
            parent,
            work_peak: 0,
            facts: CallRequestProjectionFacts {
                definition_count: 0,
                type_reference_count: 0,
                allocation_requests_upper_bound: 0,
                request_bytes_upper_bound: 0,
                coexisting_source_and_request_bytes_upper_bound: 0,
                cumulative_work_upper_bound: 0,
            },
            items: 0,
            constants: 0,
            type_roots: roots,
            pool_lookup_work: expression_tree_lookup_work(pools.entries().len())
                .map_err(|_| shape("request pool lookup work is unrepresentable"))?,
            delegated: 0,
            source,
            known: type_floor.max(size_of::<ConstantPools>()),
        })
    }
    fn request<T>(&mut self, n: usize) -> Result<(), Error> {
        let size = bytes::<T>(n)?;
        self.facts.request_bytes_upper_bound = add(self.facts.request_bytes_upper_bound, size)?;
        if size != 0 {
            self.facts.allocation_requests_upper_bound =
                add(self.facts.allocation_requests_upper_bound, 1)?;
        }
        Ok(())
    }
    fn check(&mut self, limits: CallRequestProjectionLimits) -> Result<(), HostError<H>> {
        admitted(self.facts.definition_count, limits.max_definitions)?;
        admitted(self.facts.type_reference_count, limits.max_type_references)?;
        admitted(
            self.facts.request_bytes_upper_bound,
            limits.max_request_bytes,
        )?;
        admitted(
            self.facts.allocation_requests_upper_bound,
            limits.max_allocation_requests,
        )?;
        self.facts.coexisting_source_and_request_bytes_upper_bound =
            add(self.source, self.facts.request_bytes_upper_bound)?;
        admitted(
            self.facts.coexisting_source_and_request_bytes_upper_bound,
            limits.max_coexisting_source_and_request_bytes,
        )?;
        let owned = add(128, mul(32, add(self.items, self.facts.definition_count)?)?)?;
        let lookup = add(
            mul(self.facts.type_reference_count, self.type_roots)?,
            mul(self.constants, self.pool_lookup_work)?,
        )?;
        let copies = add(
            mul(self.facts.request_bytes_upper_bound, 4)?,
            self.facts.allocation_requests_upper_bound,
        )?;
        self.facts.cumulative_work_upper_bound =
            add(add(owned, lookup)?, add(copies, self.delegated)?)?;
        admitted(self.facts.cumulative_work_upper_bound, limits.max_work)?;
        if self.source < self.known {
            return Err((shape("request source invoice omits original backing")).into());
        }
        if let Some(parent) = self.parent.as_deref_mut() {
            self.work_peak = self.work_peak.max(self.facts.cumulative_work_upper_bound);
            admitted(self.work_peak, limits.max_work)?;
            let mut prefix = self.facts;
            prefix.cumulative_work_upper_bound = self.work_peak;
            parent(&prefix)?;
        }
        Ok(())
    }
    fn comparison_prefix(
        &mut self,
        left: &FunctionValueType,
        right: &FunctionValueType,
        limits: CallRequestProjectionLimits,
    ) -> Result<(), HostError<H>> {
        if self.parent.is_some() {
            let prefix = type_binding_prefix_work_upper_bound_in(left, right, self.source)?;
            let base = self.delegated;
            self.delegated = add(base, prefix.work_upper_bound())?;
            let result = self.check(limits);
            self.delegated = base;
            result?;
        }
        Ok(())
    }
    fn compare(
        &mut self,
        left: &FunctionValueType,
        right: &FunctionValueType,
        limits: CallRequestProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, HostError<H>> {
        self.comparison_prefix(left, right, limits)?;
        self.check(limits)?;
        // Subtract actual accumulated work, not the replacement peak for this
        // same comparison. Retain that peak in the final prepared facts.
        let remaining = limits
            .max_work
            .checked_sub(self.facts.cumulative_work_upper_bound)
            .ok_or_else(resource)?;
        let compared = if self.parent.is_some() {
            let base = self.delegated;
            verify_type_binding_admitted_in::<HostError<H>>(
                left,
                right,
                self.source,
                remaining,
                &mut |prefix| {
                    self.delegated = add(base, prefix.work_upper_bound())?;
                    self.check(limits)
                },
                w,
            )?
        } else {
            let compared = verify_type_binding(left, right, self.source, remaining, w)?;
            self.delegated = add(self.delegated, compared.work_upper_bound())?;
            compared
        };
        self.check(limits)?;
        w.step()?;
        Ok(compared.matches())
    }
}

/// Retains all original immutable loans until the single consuming emission.
/// Source invoices and request facts do not authorize host/library allocation.
pub struct PreparedCallRequestsEncode<'loan, 'source, K = PhysicalCallSite> {
    source: &'loan BTreeMap<K, PhysicalCallRequest>,
    types: &'loan EncodedTypeTable<'source>,
    type_ids: &'loan [CallRequestTypeIds<'loan, K>],
    pools: &'loan ConstantPools,
    control: &'loan dyn PureCompileControl,
    facts: CallRequestProjectionFacts,
}
impl<K> PreparedCallRequestsEncode<'_, '_, K> {
    pub const fn facts(&self) -> &CallRequestProjectionFacts {
        &self.facts
    }
}
fn verify_id<H>(
    types: &EncodedTypeTable<'_>,
    id: u32,
    expected: &FunctionValueType,
    model: &mut Model<'_, '_, H>,
    limits: CallRequestProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    // Linear source lookup is admitted by Model before delegating. The
    // captured branch admits the actual selected root before the old flush.
    if model.parent.is_none() {
        w.flush()?;
    }
    let actual = if model.parent.is_some() {
        types.value_type_captured::<HostError<H>>(
            id,
            &mut |actual, work| {
                model.comparison_prefix(expected, actual, limits)?;
                work.flush()?;
                Ok(())
            },
            w,
        )?
    } else {
        types.value_type_observed(id, w)?
    };
    w.step()?;
    w.flush()?;
    let actual = actual.ok_or_else(|| shape("request supplied value type ID is absent"))?;
    if !model.compare(expected, actual, limits, w)? {
        return Err((shape("request complete type differs from its supplied type root")).into());
    }
    Ok(())
}
fn validate_constant<H>(
    reference: novarocks_physical_plan::ConstantReference,
    expected: &FunctionValueType,
    pools: &ConstantPools,
    model: &mut Model<'_, '_, H>,
    limits: CallRequestProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    // Address-only selection keeps original Field/backing/ordinal and avoids
    // the bucketed metadata comparator's unaccounted heap scratch.
    model.check(limits)?;
    if model.parent.is_none() {
        w.flush()?;
    }
    let selected = if model.parent.is_some() {
        pools.resolve_source_captured_observed::<HostError<H>>(
            reference,
            &mut |selected, work| {
                capture_constant(selected, expected, model, limits)?;
                work.flush()?;
                Ok(())
            },
            w,
        )
    } else {
        pools
            .resolve_source_observed(reference, w)
            .map_err(HostError::<H>::from)
    };
    let selected = match selected {
        Err(ProjectionFailure::Codec(Error::Control(cause))) => {
            return Err((Error::Control(cause)).into());
        }
        Err(ProjectionFailure::Host(error)) => return Err(ProjectionFailure::Host(error)),
        outcome => {
            w.step()?;
            w.flush()?;
            outcome?
        }
    };
    capture_constant(&selected, expected, model, limits)?;
    if !model.compare(expected, selected.value_type(), limits, w)? {
        return Err(
            (Error::Constant(novarocks_physical_plan::ConstantReferenceError::Table(
                novarocks_constant_contract::ConstantTableError::SourceTypeMismatch(reference),
            )))
            .into(),
        );
    }
    w.step()?;
    Ok(())
}
fn capture_constant<H>(
    selected: &novarocks_physical_plan::ConstantValue,
    expected: &FunctionValueType,
    model: &mut Model<'_, '_, H>,
    limits: CallRequestProjectionLimits,
) -> Result<(), HostError<H>> {
    let backing = usize::try_from(
        selected
            .pool()
            .resource_facts()
            .retained_buffer_capacity_bytes,
    )
    .map_err(|_| shape("request constant backing floor is unrepresentable"))?;
    model.known = model.known.max(add(size_of::<ConstantPools>(), backing)?);
    model.check(limits)?;
    model.comparison_prefix(expected, selected.value_type(), limits)
}
fn count_request_header<H, K>(
    request: &PhysicalCallRequest,
    ids: &CallRequestTypeIds<'_, K>,
    model: &mut Model<'_, '_, H>,
    owned_source: &mut usize,
    limits: CallRequestProjectionLimits,
) -> Result<(), HostError<H>> {
    model.items = add(model.items, request.arguments.len())?;
    *owned_source = add(
        *owned_source,
        bytes::<Argument<novarocks_physical_plan::ConstantReference>>(request.arguments.len())?,
    )?;
    model.known = model.known.max(*owned_source).max(add(
        size_of::<CallRequestTypeIds<'_, K>>(),
        bytes::<ArgumentTypeIds<'_>>(ids.arguments.len())?,
    )?);
    model.request::<wire::OriginalFunctionArgument>(request.arguments.len())?;
    model.check(limits)
}
fn count_lambda_header<H>(
    parameter_types: &[FunctionValueType],
    parameters: &[u32],
    model: &mut Model<'_, '_, H>,
    owned_source: &mut usize,
    limits: CallRequestProjectionLimits,
) -> Result<(), HostError<H>> {
    model.facts.type_reference_count = add(
        model.facts.type_reference_count,
        add(parameter_types.len(), 1)?,
    )?;
    model.items = add(model.items, parameter_types.len())?;
    *owned_source = add(
        *owned_source,
        bytes::<FunctionValueType>(parameter_types.len())?,
    )?;
    model.known = model.known.max(*owned_source).max(add(
        size_of::<ArgumentTypeIds<'_>>(),
        bytes::<u32>(parameters.len())?,
    )?);
    model.request::<u32>(parameter_types.len())?;
    model.check(limits)
}
fn preflight<H, K: WireRequestKey>(
    source: &BTreeMap<K, PhysicalCallRequest>,
    types: &EncodedTypeTable<'_>,
    ids: &[CallRequestTypeIds<'_, K>],
    pools: &ConstantPools,
    invoice: usize,
    limits: CallRequestProjectionLimits,
    parent: Option<&mut HostAdmit<'_, H>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<CallRequestProjectionFacts, HostError<H>> {
    let mut model = Model::new(invoice, types, pools, parent)?;
    model.facts.definition_count = source.len();
    model.known = model.known.max(add(
        size_of::<BTreeMap<K, PhysicalCallRequest>>(),
        add(
            bytes::<(K, PhysicalCallRequest)>(source.len())?,
            bytes::<CallRequestTypeIds<'_, K>>(ids.len())?,
        )?,
    )?);
    model.request::<K::Wire>(source.len())?;
    model.check(limits)?;
    checked_shape(
        ids.len() == source.len(),
        "request type bindings do not cover the original table",
        w,
    )?;
    let mut owned_source = model.known;
    for ((definition, request), ids) in source.iter().zip(ids) {
        if model.parent.is_some() {
            count_request_header(request, ids, &mut model, &mut owned_source, limits)?;
        }
        checked_shape(
            *definition == ids.definition,
            "request type bindings differ from original ordered definitions",
            w,
        )?;
        checked_shape(
            request.arguments.len() == ids.arguments.len(),
            "request argument ID shape differs from original source",
            w,
        )?;
        checked_shape(
            request.logical_argument_count <= request.arguments.len(),
            "request logical count exceeds original argument extent",
            w,
        )?;
        let count = u32::try_from(request.logical_argument_count);
        w.step()?;
        count.map_err(|_| shape("request logical argument count is unrepresentable"))?;
        if model.parent.is_none() {
            count_request_header(request, ids, &mut model, &mut owned_source, limits)?;
        }
        for (argument, ids) in request.arguments.iter().zip(ids.arguments) {
            match (argument, ids) {
                (Argument::Value { .. }, ArgumentTypeIds::Value(_)) => {
                    model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
                    if matches!(
                        argument,
                        Argument::Value {
                            constant: Some(_),
                            ..
                        }
                    ) {
                        model.constants = add(model.constants, 1)?;
                    }
                }
                (
                    Argument::Lambda {
                        parameter_types, ..
                    },
                    ArgumentTypeIds::Lambda { parameters, .. },
                ) => {
                    if model.parent.is_some() {
                        count_lambda_header(
                            parameter_types,
                            parameters,
                            &mut model,
                            &mut owned_source,
                            limits,
                        )?;
                    }
                    checked_shape(
                        parameter_types.len() == parameters.len(),
                        "request Lambda parameter ID shape differs from source",
                        w,
                    )?;
                    if model.parent.is_none() {
                        count_lambda_header(
                            parameter_types,
                            parameters,
                            &mut model,
                            &mut owned_source,
                            limits,
                        )?;
                    }
                }
                _ => {
                    w.step()?;
                    return Err((shape("request argument ID variant differs from source")).into());
                }
            }
            model.check(limits)?;
            w.step()?;
        }
        if model.parent.is_some() && request.expected_result_type.is_some() {
            model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
            model.check(limits)?;
        }
        checked_shape(
            request.expected_result_type.is_some() == ids.expected_result_type.is_some(),
            "request expected constraint presence differs from source",
            w,
        )?;
        if model.parent.is_none() && request.expected_result_type.is_some() {
            model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
        }
        model.check(limits)?;
    }
    // All cumulative requests, source floors and own-loop bounds precede type
    // walks, constant selection and the first output allocation.
    for ((_, request), ids) in source.iter().zip(ids) {
        for (argument, ids) in request.arguments.iter().zip(ids.arguments) {
            match (argument, ids) {
                (
                    Argument::Value {
                        value_type,
                        constant,
                    },
                    ArgumentTypeIds::Value(id),
                ) => {
                    verify_id(types, *id, value_type, &mut model, limits, w)?;
                    if let Some(reference) = constant {
                        validate_constant(*reference, value_type, pools, &mut model, limits, w)?;
                    }
                }
                (
                    Argument::Lambda {
                        parameter_types,
                        result_type,
                    },
                    ArgumentTypeIds::Lambda { parameters, result },
                ) => {
                    for (ty, id) in parameter_types.iter().zip(*parameters) {
                        verify_id(types, *id, ty, &mut model, limits, w)?;
                        w.step()?;
                    }
                    verify_id(types, *result, result_type, &mut model, limits, w)?;
                }
                _ => return Err((shape("request admitted argument shape changed")).into()),
            }
            w.step()?;
        }
        if let (Some(ty), Some(id)) = (&request.expected_result_type, ids.expected_result_type) {
            verify_id(types, id, ty, &mut model, limits, w)?;
        }
        w.step()?;
    }
    if model.parent.is_some() {
        model.facts.cumulative_work_upper_bound = model.work_peak;
    }
    Ok(model.facts)
}

pub fn prepare_call_requests_encode<'loan, 'source>(
    source: &'loan FragmentCallRequests,
    types: &'loan EncodedTypeTable<'source>,
    type_ids: &'loan [CallRequestTypeIds<'loan>],
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: CallRequestProjectionLimits,
    control: &'loan dyn PureCompileControl,
) -> Result<PreparedCallRequestsEncode<'loan, 'source>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = preflight::<std::convert::Infallible, PhysicalCallSite>(
        source.entries(),
        types,
        type_ids,
        pools,
        source_retained_bytes,
        limits,
        None,
        &mut w,
    )
    .map(|facts| PreparedCallRequestsEncode {
        source: source.entries(),
        types,
        type_ids,
        pools,
        control,
        facts,
    });
    finish(w, result.map_err(ProjectionFailure::without_host))
}
fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut output = Vec::new();
    reserve_exit::<Error>(output.try_reserve_exact(n), w)?;
    w.step()?;
    w.flush()?;
    Ok(output)
}
pub(crate) fn encode_source_policy(
    value: novarocks_physical_plan::ConstantPolicy,
) -> wire::SourceConstantPolicy {
    wire::SourceConstantPolicy {
        max_rows: Some(value.max_rows),
        max_array_nodes: Some(value.max_array_nodes),
        max_logical_elements: Some(value.max_logical_elements),
        max_retained_buffer_bytes: Some(value.max_retained_buffer_bytes),
        max_type_depth: Some(value.max_type_depth),
        max_type_nodes: Some(value.max_type_nodes),
        max_dictionary_depth: Some(value.max_dictionary_depth),
        max_metadata_bytes: Some(value.max_metadata_bytes),
        max_library_validation_work: Some(value.max_library_validation_work),
        max_library_validation_bytes: Some(value.max_library_validation_bytes),
    }
}
pub fn encode_call_requests(
    token: PreparedCallRequestsEncode<'_, '_>,
) -> Result<wire::FragmentCallRequests, Error> {
    let mut w = CompileCheckpoints::try_new(token.control, CompilePhase::Encode)?;
    let result = emit_core(token, &mut w).map(|entries| wire::FragmentCallRequests { entries });
    finish(w, result)
}
fn emit_core<K: WireRequestKey>(
    token: PreparedCallRequestsEncode<'_, '_, K>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<K::Wire>, Error> {
    (|| {
        // These loans remain live through emission; nothing is re-resolved or
        // re-admitted after the immutable prepared correspondence was checked.
        let _loans = (token.types, token.pools);
        let mut entries = reserve::<K::Wire>(token.source.len(), w)?;
        for ((definition, request), ids) in token.source.iter().zip(token.type_ids) {
            w.step()?;
            let mut arguments =
                reserve::<wire::OriginalFunctionArgument>(request.arguments.len(), w)?;
            for (argument, ids) in request.arguments.iter().zip(ids.arguments) {
                let kind = match (argument, ids) {
                    (Argument::Value { constant, .. }, ArgumentTypeIds::Value(id)) => {
                        wire::original_function_argument::Kind::Value(wire::OriginalValueArgument {
                            value_type_id: Some(*id),
                            constant: constant.map(|r| wire::ConstantReference {
                                pool_id: Some(r.pool.get()),
                                row_ordinal: r.ordinal,
                            }),
                        })
                    }
                    (Argument::Lambda { .. }, ArgumentTypeIds::Lambda { parameters, result }) => {
                        let mut parameter_value_type_ids = reserve::<u32>(parameters.len(), w)?;
                        for id in *parameters {
                            parameter_value_type_ids.push(*id);
                            w.step()?;
                        }
                        wire::original_function_argument::Kind::Lambda(wire::LambdaArgumentType {
                            parameter_value_type_ids,
                            result_value_type_id: Some(*result),
                        })
                    }
                    _ => return Err(shape("request prepared argument variant changed")),
                };
                arguments.push(wire::OriginalFunctionArgument { kind: Some(kind) });
                w.step()?;
            }
            entries.push(K::project(
                *definition,
                RequestFields {
                    arguments,
                    logical_argument_count: Some(
                        u32::try_from(request.logical_argument_count)
                            .map_err(|_| shape("request prepared logical count changed"))?,
                    ),
                    expected_result_value_type_id: ids.expected_result_type,
                    constant_policy: Some(encode_source_policy(request.constant_policy)),
                },
            ));
            w.step()?;
        }
        Ok(entries)
    })()
}

/// Prepare on the caller's original meter, replacing this child contribution.
/// No scope, entry, finish, type reconstruction or constant admission is added.
pub fn prepare_call_requests_encode_in<'loan, 'source, 'control: 'loan>(
    source: &'loan FragmentCallRequests,
    types: &'loan EncodedTypeTable<'source>,
    type_ids: &'loan [CallRequestTypeIds<'loan>],
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: CallRequestProjectionLimits,
    admit: &mut CallRequestAdmit<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedCallRequestsEncode<'loan, 'source>, Error> {
    prepare_call_requests_encode_with_host_in(
        source,
        types,
        type_ids,
        pools,
        source_retained_bytes,
        limits,
        &mut |facts| admit(facts).map_err(AdmissionRefusal::<std::convert::Infallible>::Control),
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

/// Same immutable source/type/pool loans, with a nominal capture-only refusal.
pub fn prepare_call_requests_encode_with_host_in<'loan, 'source, 'control: 'loan, H>(
    source: &'loan FragmentCallRequests,
    types: &'loan EncodedTypeTable<'source>,
    type_ids: &'loan [CallRequestTypeIds<'loan>],
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: CallRequestProjectionLimits,
    admit: &mut dyn FnMut(&CallRequestProjectionFacts) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedCallRequestsEncode<'loan, 'source>, HostError<H>> {
    prepare_call_requests_encode_host_core(
        source,
        types,
        type_ids,
        pools,
        source_retained_bytes,
        limits,
        &mut |facts| admit(facts).map_err(project_admission),
        work,
    )
}

pub fn prepare_call_requests_encode_host_core<'loan, 'source, 'control: 'loan, H>(
    source: &'loan FragmentCallRequests,
    types: &'loan EncodedTypeTable<'source>,
    type_ids: &'loan [CallRequestTypeIds<'loan>],
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: CallRequestProjectionLimits,
    admit: &mut HostAdmit<'_, H>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedCallRequestsEncode<'loan, 'source>, HostError<H>> {
    let facts = preflight(
        source.entries(),
        types,
        type_ids,
        pools,
        source_retained_bytes,
        limits,
        Some(admit),
        work,
    )?;
    Ok(PreparedCallRequestsEncode {
        source: source.entries(),
        types,
        type_ids,
        pools,
        control: work.control(),
        facts,
    })
}
pub(crate) fn encode_call_requests_in(
    token: PreparedCallRequestsEncode<'_, '_>,
    admit: &mut CallRequestAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FragmentCallRequests, Error> {
    encode_call_requests_with_host_in(
        token,
        &mut |facts| admit(facts).map_err(AdmissionRefusal::<std::convert::Infallible>::Control),
        work,
    )
    .map_err(ProjectionFailure::without_host)
}
fn encode_call_requests_with_host_in<H>(
    token: PreparedCallRequestsEncode<'_, '_>,
    admit: &mut dyn FnMut(&CallRequestProjectionFacts) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FragmentCallRequests, HostError<H>> {
    if !std::ptr::addr_eq(token.control, work.control()) {
        return Err(shape("request encoder belongs to another controller").into());
    }
    admit(&token.facts).map_err(project_admission)?;
    emit_core(token, work)
        .map(|entries| wire::FragmentCallRequests { entries })
        .map_err(ProjectionFailure::Codec)
}
impl PreparedCallRequestsEncode<'_, '_> {
    pub fn emit_in(
        self,
        admit: &mut CallRequestAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<wire::FragmentCallRequests, Error> {
        encode_call_requests_in(self, admit, work)
    }
    pub fn emit_with_host_in<H>(
        self,
        admit: &mut dyn FnMut(&CallRequestProjectionFacts) -> Result<(), AdmissionRefusal<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<wire::FragmentCallRequests, HostError<H>> {
        encode_call_requests_with_host_in(self, admit, work)
    }
}

#[cfg(test)]
#[path = "encode_tests.rs"]
mod tests;

struct RequestFields {
    arguments: Vec<wire::OriginalFunctionArgument>,
    logical_argument_count: Option<u32>,
    expected_result_value_type_id: Option<u32>,
    constant_policy: Option<wire::SourceConstantPolicy>,
}
trait WireRequestKey: Copy + Ord {
    type Wire;
    fn project(self, fields: RequestFields) -> Self::Wire;
}
impl WireRequestKey for PhysicalCallSite {
    type Wire = wire::OriginalCallRequest;
    fn project(self, f: RequestFields) -> Self::Wire {
        wire::OriginalCallRequest {
            definition: Some(wire::CallRequestDefinition {
                kind: Some(wire::call_request_definition::Kind::Relational(
                    encode_call_site(self),
                )),
            }),
            arguments: f.arguments,
            logical_argument_count: f.logical_argument_count,
            expected_result_value_type_id: f.expected_result_value_type_id,
            constant_policy: f.constant_policy,
        }
    }
}
impl WireRequestKey for novarocks_expression_contract::ExprId {
    type Wire = wire::UnitCallRequest;
    fn project(self, f: RequestFields) -> Self::Wire {
        wire::UnitCallRequest {
            definition_id: Some(self.get()),
            arguments: f.arguments,
            logical_argument_count: f.logical_argument_count,
            expected_result_value_type_id: f.expected_result_value_type_id,
            constant_policy: f.constant_policy,
        }
    }
}
pub(crate) fn encode_unit_requests_in(
    unit: &novarocks_expression_contract::BoundExprSet,
    types: &EncodedTypeTable<'_>,
    ids: &[super::UnitRequestTypeIds<'_>],
    pools: &ConstantPools,
    invoice: usize,
    limits: CallRequestProjectionLimits,
    admit: &mut CallRequestAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<wire::UnitCallRequest>, CallRequestProjectionFacts), Error> {
    let facts = preflight::<std::convert::Infallible, _>(
        unit.requests(),
        types,
        ids,
        pools,
        invoice,
        limits,
        Some(&mut |facts| {
            admit(facts).map_err(|cause| ProjectionFailure::Codec(Error::Control(cause)))
        }),
        work,
    )
    .map_err(ProjectionFailure::without_host)?;
    admit(&facts)?;
    let output = emit_core(
        PreparedCallRequestsEncode {
            source: unit.requests(),
            types,
            type_ids: ids,
            pools,
            control: work.control(),
            facts,
        },
        work,
    )?;
    Ok((output, facts))
}
