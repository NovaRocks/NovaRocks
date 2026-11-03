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

use super::owner_admission::{Admission, Admit, Arithmetic, lookup_facts, same_control};
use super::{ExpressionCodecError as Error, PreparedExpressionIds, kind::encode_kind};
use crate::{
    allocation_exit_v2::reserve_exit,
    borrowed_type_resources::{
        VerifiedTypeBinding, verify_type_binding, verify_type_binding_admitted,
    },
    physical_aggregate_binding_v2::{
        EncodedAggregateBindings, VerifiedAggregateSignature, verify_aggregate_signature,
        verify_aggregate_signature_admitted,
    },
    physical_binding_v2::{
        EncodedFunctionBindings, VerifiedSignature, verify_scalar_signature,
        verify_scalar_signature_admitted,
    },
    physical_semantics_v2::SemanticsCodecError,
    physical_type_v2::EncodedTypeTable,
};
use novarocks_physical_plan::{
    BinaryOperator, ConstantPools, ExprArena, ExprId, ExprKind, ExprNode,
};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, SemanticParameterKey, SemanticParameters,
    arrow_data_types_exact_borrowed_with_profile_observed,
};
use std::alloc::Layout;

/// One ID binding per original definition, in ExprArena's canonical order.
/// This authoring order does not constrain a future receiving wire order.
#[derive(Clone, Copy)]
pub struct ExpressionTypeIds<'a> {
    pub expr: ExprId,
    pub value_type_id: u32,
    pub lambda_parameter_type_ids: &'a [u32],
    pub function_binding_id: Option<u32>,
    pub aggregate_binding_id: Option<u32>,
}
#[derive(Clone, Copy, Debug)]
pub struct ExpressionProjectionLimits {
    pub max_definitions: usize,
    pub max_type_references: usize,
    pub max_expression_references: usize,
    pub max_new_allocation_requests: usize,
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct ExpressionNamespaceWriteFacts {
    pub definition_count: usize,
    pub type_reference_count: usize,
    pub expression_reference_count: usize,
    pub new_allocation_requests_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
/// Original source loans prevent changing any definition, scoped parameter or
/// namespace between preflight and emission. Preparing this owner allocates no
/// heap storage and clones no source type or selected function signature.
pub struct PreparedExpressionNamespaceWrite<'loan, 'source, 'control> {
    arena: &'loan ExprArena,
    inputs: &'loan [ExpressionTypeIds<'source>],
    types: &'loan EncodedTypeTable<'source>,
    _functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    _aggregates: &'loan EncodedAggregateBindings<'loan, 'source>,
    _parameters: &'loan SemanticParameters,
    _pools: &'loan ConstantPools,
    facts: ExpressionNamespaceWriteFacts,
    control: &'control dyn PureCompileControl,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
}
/// The private DTO and the original immutable source loans form one emission.
/// Borrowing this owner never clones a type, signature or constant backing.
/// Consuming its DTO ends this source-correspondence capability.
pub struct EncodedExpressions<'loan, 'source, 'control> {
    source: PreparedExpressionNamespaceWrite<'loan, 'source, 'control>,
    wire: Vec<wire::ExpressionDefinition>,
}
impl<'loan, 'source, 'control> EncodedExpressions<'loan, 'source, 'control> {
    pub fn as_wire(&self) -> &[wire::ExpressionDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ExpressionDefinition> {
        self.wire
    }
    pub const fn facts(&self) -> &ExpressionNamespaceWriteFacts {
        &self.source.facts
    }
    pub fn source_count(&self) -> usize {
        self.source.arena.len()
    }
    pub fn arena(&self) -> &'loan ExprArena {
        self.source.arena
    }
    pub fn types(&self) -> &'loan EncodedTypeTable<'source> {
        self.source.types
    }
    pub fn functions(&self) -> &'loan EncodedFunctionBindings<'loan, 'source> {
        self.source._functions
    }
    pub fn aggregates(&self) -> &'loan EncodedAggregateBindings<'loan, 'source> {
        self.source._aggregates
    }
    pub fn parameters(&self) -> &'loan SemanticParameters {
        self.source._parameters
    }
    pub fn pools(&self) -> &'loan ConstantPools {
        self.source._pools
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.source.control
    }
    /// Numeric cost of one original sparse lookup for an encompassing caller
    /// to accumulate before delegation. This is not a separate resource grant.
    pub fn lookup_work_upper_bound(&self) -> Result<usize, Error> {
        tree_lookup_work(self.source_count())
    }
    pub fn expression(&self, id: u32) -> Result<Option<&'loan ExprNode>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.expression_observed(id, &mut work);
        finish(work, result)
    }
    pub(crate) fn expression_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan ExprNode>, Error> {
        // The actual BTree lookup remains opaque, rather than claiming its
        // internal comparisons as completed checkpoint units.
        work.flush()?;
        let node = self.source.arena.get(ExprId::new(id));
        work.flush()?;
        work.step()?;
        Ok(node)
    }
    pub fn expression_in(
        &self,
        id: u32,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan ExprNode>, Error> {
        same_control(self.original_control(), work)?;
        admit(&lookup_facts(self.lookup_work_upper_bound()?))?;
        self.expression_observed(id, work)
    }
    pub fn source_id_in(
        &self,
        source: &ExprNode,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        same_control(self.original_control(), work)?;
        admit(&lookup_facts(add(self.lookup_work_upper_bound()?, 1)?))?;
        self.source_id_observed(source, work)
    }
    pub fn retained_floor_in(
        &self,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        same_control(self.original_control(), work)?;
        let floor = self.retained_floor_header_in()?;
        admit(&lookup_facts(1))?;
        work.step()?;
        Ok(floor)
    }
    pub(crate) fn retained_floor_header_in(
        &self,
    ) -> Result<usize, novarocks_type_contract::CompileControlError> {
        self.source
            .source_retained_bytes
            .checked_add(std::mem::size_of::<Self>())
            .and_then(|n| {
                n.checked_add(bytes::<wire::ExpressionDefinition>(self.wire.capacity()).ok()?)
            })
            .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)
    }
    pub fn source_id(&self, source: &ExprNode) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.source_id_observed(source, &mut work);
        finish(work, result)
    }
    pub(crate) fn source_id_observed(
        &self,
        source: &ExprNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let actual = self.expression_observed(source.id.get(), work)?;
        let same = actual.is_some_and(|actual| std::ptr::eq(actual, source));
        work.step()?;
        if !same {
            return Err(shape("expression source owner is not in this namespace"));
        }
        Ok(source.id.get())
    }
    /// Necessary live-storage floor: original whole-source invoice, inline
    /// token and actual root DTO Vec capacity. Nested DTO backing is covered
    /// by the emitted owner's request model and the caller's whole invoice;
    /// this floor is not a complete retained-size measurement or host grant.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(work, result)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let floor = add(
            self.source.source_retained_bytes,
            add(
                std::mem::size_of::<Self>(),
                bytes::<wire::ExpressionDefinition>(self.wire.capacity())?,
            )?,
        );
        work.step()?;
        floor
    }
}
impl<'loan, 'source, 'control> PreparedExpressionNamespaceWrite<'loan, 'source, 'control> {
    pub const fn facts(&self) -> &ExpressionNamespaceWriteFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<EncodedExpressions<'loan, 'source, 'control>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = self.emit_core(None, &mut work);
        finish(work, result)
    }
    pub fn emit_in(
        self,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<EncodedExpressions<'loan, 'source, 'control>, Error> {
        same_control(self.control, work)?;
        self.emit_core(Some(admit), work)
    }
    fn emit_core(
        self,
        parent: Option<&mut Admit<'_>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<EncodedExpressions<'loan, 'source, 'control>, Error> {
        let mut admission = Admission {
            parent,
            source: self.source_retained_bytes,
            limits: self.limits,
        };
        admission.fixed(&self.facts)?;
        (|| {
            work.flush()?;
            let mut output = Vec::new();
            let reserved = output.try_reserve_exact(self.inputs.len());
            reserve_exit::<Error>(reserved, work)?;
            for ((_, node), input) in self.arena.iter().zip(self.inputs) {
                // Only CAST consumes the actual carrier ID. All other header
                // type IDs were proved against this same immutable table.
                let carrier = if matches!(node.kind, ExprKind::Cast { .. }) {
                    Some(
                        self.types
                            .root_value_binding_observed(input.value_type_id, work)?
                            .ok_or_else(|| shape("prepared expression root type is absent"))?
                            .0,
                    )
                } else {
                    None
                };
                let ids = PreparedExpressionIds {
                    root_carrier_type_id: carrier,
                    lambda_parameter_type_ids: input.lambda_parameter_type_ids,
                    function_binding_id: input.function_binding_id,
                    aggregate_binding_id: input.aggregate_binding_id,
                };
                let kind = encode_kind(node, &ids, work)?;
                output.push(wire::ExpressionDefinition {
                    id: node.id.get(),
                    owner_node_id: Some(node.owner.get()),
                    lambda_scope_expr_id: node.lambda_scope.map(ExprId::get),
                    value_type_id: Some(input.value_type_id),
                    kind: Some(kind),
                });
                work.step()?;
            }
            Ok(EncodedExpressions {
                source: self,
                wire: output,
            })
        })()
    }
}

pub(super) fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
pub(super) fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| shape("expression writer resource sum overflow"))
}
pub(super) fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| shape("expression writer resource product overflow"))
}
pub(super) fn bytes<T>(count: usize) -> Result<usize, Error> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| shape("expression writer layout is unrepresentable"))
}
pub(super) fn cap(
    actual: usize,
    limit: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let allowed = actual <= limit;
    work.step()?;
    if !allowed {
        return Err(shape("expression writer envelope exceeded"));
    }
    Ok(())
}
pub(super) fn check(
    facts: &ExpressionNamespaceWriteFacts,
    limits: ExpressionProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    cap(facts.definition_count, limits.max_definitions, work)?;
    cap(facts.type_reference_count, limits.max_type_references, work)?;
    cap(
        facts.expression_reference_count,
        limits.max_expression_references,
        work,
    )?;
    cap(
        facts.new_allocation_requests_upper_bound,
        limits.max_new_allocation_requests,
        work,
    )?;
    cap(
        facts.new_allocation_request_bytes_upper_bound,
        limits.max_new_allocation_request_bytes,
        work,
    )?;
    cap(
        facts.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        work,
    )?;
    cap(
        facts.cumulative_work_upper_bound,
        limits.max_cumulative_work,
        work,
    )
}
pub(super) fn vector<T>(
    facts: &mut ExpressionNamespaceWriteFacts,
    count: usize,
) -> Result<(), Error> {
    facts.new_allocation_request_bytes_upper_bound = add(
        facts.new_allocation_request_bytes_upper_bound,
        bytes::<T>(count)?,
    )?;
    facts.new_allocation_requests_upper_bound = add(
        facts.new_allocation_requests_upper_bound,
        usize::from(count != 0),
    )?;
    Ok(())
}
pub(super) fn charge(
    facts: &mut ExpressionNamespaceWriteFacts,
    value: usize,
    limits: ExpressionProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    facts.cumulative_work_upper_bound = add(facts.cumulative_work_upper_bound, value)?;
    cap(
        facts.cumulative_work_upper_bound,
        limits.max_cumulative_work,
        work,
    )
}
pub(crate) fn tree_lookup_work(entries: usize) -> Result<usize, Error> {
    // Rust 1.92 BTree nodes have at most eleven keys. Even binary fanout
    // gives at most bit-length+1 visited levels; sixteen units per level
    // include key, edge and node-header work without another lookup index.
    crate::btree_resources_v2::lookup_work(entries).map_err(shape)
}
pub(super) fn compare_types(
    left: &novarocks_type_contract::FunctionValueType,
    right: &novarocks_type_contract::FunctionValueType,
    facts: &mut ExpressionNamespaceWriteFacts,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedTypeBinding, Error> {
    let remaining = admission.remaining(facts)?;
    if admission.observed() {
        let base = facts.cumulative_work_upper_bound;
        let verified = verify_type_binding_admitted::<Error>(
            left,
            right,
            admission.source,
            remaining,
            &mut |prefix| {
                facts.cumulative_work_upper_bound =
                    admission.numeric(add(base, prefix.work_upper_bound()))?;
                admission.gate(facts)
            },
            work,
        )?;
        cap(
            facts.cumulative_work_upper_bound,
            admission.limits.max_cumulative_work,
            work,
        )?;
        Ok(verified)
    } else {
        let verified = verify_type_binding(left, right, admission.source, remaining, work)?;
        admission.charge(facts, verified.work_upper_bound(), work)?;
        Ok(verified)
    }
}
pub(super) fn preflight_types(
    left: &novarocks_type_contract::FunctionValueType,
    right: &novarocks_type_contract::FunctionValueType,
    facts: &mut ExpressionNamespaceWriteFacts,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<crate::borrowed_type_resources::BoundTypeComparisonFacts, Error> {
    let remaining = admission.remaining(facts)?;
    if admission.observed() {
        let base = facts.cumulative_work_upper_bound;
        let verified = crate::borrowed_type_resources::preflight_type_binding_admitted::<Error>(
            left,
            right,
            admission.source,
            remaining,
            &mut |prefix| {
                facts.cumulative_work_upper_bound =
                    admission.numeric(add(base, prefix.work_upper_bound()))?;
                admission.gate(facts)
            },
            work,
        )?;
        cap(
            facts.cumulative_work_upper_bound,
            admission.limits.max_cumulative_work,
            work,
        )?;
        Ok(verified)
    } else {
        let verified = crate::borrowed_type_resources::preflight_type_binding(
            left,
            right,
            admission.source,
            remaining,
            work,
        )?;
        admission.charge(facts, verified.work_upper_bound(), work)?;
        Ok(verified)
    }
}
fn binding_error(error: Error) -> crate::physical_binding_v2::BindingCodecError {
    match error {
        Error::Binding(error) => error,
        Error::Control(cause) => cause.into(),
        _ => unreachable!("signature comparison delegates only the original binding error author"),
    }
}
fn compare_scalar(
    left: &novarocks_physical_plan::BoundFunction,
    right: &novarocks_physical_plan::BoundFunction,
    facts: &mut ExpressionNamespaceWriteFacts,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedSignature, Error> {
    let remaining = admission.remaining(facts)?;
    if admission.observed() {
        let base = facts.cumulative_work_upper_bound;
        let verified = verify_scalar_signature_admitted(
            left,
            right,
            admission.source,
            remaining,
            &mut |prefix| {
                facts.cumulative_work_upper_bound = base
                    .checked_add(prefix)
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                admission.gate(facts).map_err(|error| match error {
                    Error::Control(cause) => {
                        crate::physical_binding_v2::BindingCodecError::Control(cause)
                    }
                    _ => unreachable!("a numerical parent gate has only typed control errors"),
                })
            },
            work,
        )?;
        cap(
            facts.cumulative_work_upper_bound,
            admission.limits.max_cumulative_work,
            work,
        )?;
        Ok(verified)
    } else {
        let verified = verify_scalar_signature(left, right, admission.source, remaining, work)?;
        admission.charge(facts, verified.work_upper_bound(), work)?;
        Ok(verified)
    }
}
fn compare_aggregate(
    left: &novarocks_physical_plan::AggregateBinding,
    right: &novarocks_physical_plan::AggregateBinding,
    facts: &mut ExpressionNamespaceWriteFacts,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedAggregateSignature, Error> {
    let remaining = admission.remaining(facts)?;
    if admission.observed() {
        let base = facts.cumulative_work_upper_bound;
        let verified = verify_aggregate_signature_admitted(
            left,
            right,
            admission.source,
            remaining,
            &mut |prefix| {
                facts.cumulative_work_upper_bound = base
                    .checked_add(prefix)
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                admission.gate(facts).map_err(|error| match error {
                    Error::Control(cause) => {
                        crate::physical_binding_v2::BindingCodecError::Control(cause)
                    }
                    _ => unreachable!("a numerical parent gate has only typed control errors"),
                })
            },
            work,
        )?;
        cap(
            facts.cumulative_work_upper_bound,
            admission.limits.max_cumulative_work,
            work,
        )?;
        Ok(verified)
    } else {
        let verified = verify_aggregate_signature(left, right, admission.source, remaining, work)?;
        admission.charge(facts, verified.work_upper_bound(), work)?;
        Ok(verified)
    }
}
fn signature_source_floor(
    function: &novarocks_physical_plan::BoundFunction,
    floor: &mut usize,
    source: usize,
    facts: &mut ExpressionNamespaceWriteFacts,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let numeric = Arithmetic(admission.observed());
    // These Boxes belong to this original owned expression occurrence; the
    // inline BoundFunction is already included in ExprNode/aggregate Layout.
    *floor = numeric.add(
        *floor,
        numeric.add(
            function.function_id.as_str().len(),
            function.overload.as_str().len(),
        )?,
    )?;
    *floor = numeric.add(
        *floor,
        numeric.bytes::<novarocks_physical_plan::FunctionArgumentType>(
            function.argument_types.len(),
        )?,
    )?;
    cap(*floor, source, work)?;
    admission.charge(
        facts,
        admission.numeric(mul(function.argument_types.len(), 8))?,
        work,
    )?;
    for argument in &function.argument_types {
        if let novarocks_physical_plan::FunctionArgumentType::Lambda {
            parameter_types, ..
        } = argument
        {
            *floor = numeric.add(
                *floor,
                numeric
                    .bytes::<novarocks_type_contract::FunctionValueType>(parameter_types.len())?,
            )?;
            cap(*floor, source, work)?;
        }
        work.step()?;
    }
    Ok(())
}
pub(super) fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, Error>,
) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// The caller supplies a truthful complete retained-source invoice. Known
/// inline/namespace floors do not prove nested strings, metadata bucket backing
/// or a formal host grant. Full graph, node, occurrence and kernel validity
/// remains the sole checked FragmentPackage/compiler chain's responsibility.
pub fn prepare_expression_definitions<'loan, 'source, 'control>(
    arena: &'loan ExprArena,
    inputs: &'loan [ExpressionTypeIds<'source>],
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    aggregates: &'loan EncodedAggregateBindings<'loan, 'source>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedExpressionNamespaceWrite<'loan, 'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare_core(
        arena,
        inputs,
        types,
        functions,
        aggregates,
        parameters,
        pools,
        source_retained_bytes,
        limits,
        control,
        None,
        &mut work,
    );
    finish(work, result)
}
#[expect(
    clippy::too_many_arguments,
    reason = "The namespace boundary keeps original inputs, frozen limits, admission, and caller checkpoints explicit."
)]
pub fn prepare_expression_definitions_in<'loan, 'source, 'control>(
    arena: &'loan ExprArena,
    inputs: &'loan [ExpressionTypeIds<'source>],
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    aggregates: &'loan EncodedAggregateBindings<'loan, 'source>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedExpressionNamespaceWrite<'loan, 'source, 'control>, Error> {
    prepare_core(
        arena,
        inputs,
        types,
        functions,
        aggregates,
        parameters,
        pools,
        source_retained_bytes,
        limits,
        work.control(),
        Some(admit),
        work,
    )
}
#[expect(
    clippy::too_many_arguments,
    reason = "The internal composer keeps expression sources and independent namespace authorities distinct."
)]
fn prepare_core<'loan, 'source, 'control>(
    arena: &'loan ExprArena,
    inputs: &'loan [ExpressionTypeIds<'source>],
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    aggregates: &'loan EncodedAggregateBindings<'loan, 'source>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
    control: &'control dyn PureCompileControl,
    parent: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedExpressionNamespaceWrite<'loan, 'source, 'control>, Error> {
    let mut admission = Admission {
        parent,
        source: source_retained_bytes,
        limits,
    };
    (|| {
        let count = arena.len();
        let numeric = Arithmetic(admission.observed());
        let initial_floor = if admission.observed() {
            Some(numeric.result(initial_source_floor(count, types, functions, aggregates))?)
        } else {
            None
        };
        let mut facts = ExpressionNamespaceWriteFacts::default();
        if admission.observed() {
            facts = admission.numeric(initial_facts(count))?;
            admission.gate(&mut facts)?;
        }
        cap(count, limits.max_definitions, work)?;
        let same_count = count == inputs.len();
        work.step()?;
        if !same_count {
            return Err(shape(
                "expression author requires one binding per original definition",
            ));
        }
        let same_table = std::ptr::eq(types, functions.type_sources())
            && std::ptr::eq(types, aggregates.type_sources())
            && std::ptr::eq(functions, aggregates.function_sources());
        work.step()?;
        if !same_table {
            return Err(shape(
                "expression author requires one original type emission",
            ));
        }
        let mut source_floor = match initial_floor {
            Some(value) => value,
            None => initial_source_floor(count, types, functions, aggregates)?,
        };
        cap(source_floor, source_retained_bytes, work)?;
        if !admission.observed() {
            facts = initial_facts(count)?;
        }
        cap(
            facts.cumulative_work_upper_bound,
            limits.max_cumulative_work,
            work,
        )?;
        let mut counts = KindCounts::default();
        for ((id, node), input) in arena.iter().zip(inputs) {
            let id_matches = *id == input.expr && node.id == input.expr;
            let counted = if admission.observed() {
                let result = count_kind(node, input, &mut facts, &mut counts, true);
                if let Err(Error::Control(cause)) = &result {
                    return Err(Error::Control(cause.clone()));
                }
                let mut prefix = facts;
                prefix.cumulative_work_upper_bound = admission.numeric(count_work(
                    &facts, &counts, types, functions, aggregates, parameters, pools,
                ))?;
                admission.gate(&mut prefix)?;
                Some(result)
            } else {
                None
            };
            work.step()?;
            if !id_matches {
                return Err(shape(
                    "expression binding order differs from original definitions",
                ));
            }
            let owned = match &node.kind {
                ExprKind::Conjunction { args }
                | ExprKind::Disjunction { args }
                | ExprKind::FunctionCall { args, .. } => numeric.bytes::<ExprId>(args.len())?,
                ExprKind::Lambda {
                    parameter_types, ..
                } => numeric
                    .bytes::<novarocks_type_contract::FunctionValueType>(parameter_types.len())?,
                ExprKind::InList { list, .. } => numeric.bytes::<ExprId>(list.len())?,
                ExprKind::Case { when_then, .. } => {
                    numeric.bytes::<(ExprId, ExprId)>(when_then.len())?
                }
                ExprKind::WindowCall {
                    args,
                    function_order_by,
                    aggregate_binding,
                    ..
                } => {
                    let mut owned = numeric.add(
                        numeric.bytes::<ExprId>(args.len())?,
                        numeric
                            .bytes::<novarocks_physical_plan::SortExpr>(function_order_by.len())?,
                    )?;
                    if aggregate_binding.is_some() {
                        owned = numeric.add(
                            owned,
                            std::mem::size_of::<novarocks_physical_plan::AggregateBinding>(),
                        )?;
                    }
                    owned
                }
                _ => 0,
            };
            source_floor = numeric.add(source_floor, owned)?;
            cap(source_floor, source_retained_bytes, work)?;
            match &node.kind {
                ExprKind::FunctionCall { function, .. } => signature_source_floor(
                    function,
                    &mut source_floor,
                    source_retained_bytes,
                    &mut facts,
                    &mut admission,
                    work,
                )?,
                ExprKind::WindowCall {
                    function,
                    aggregate_binding,
                    ..
                } => {
                    signature_source_floor(
                        function,
                        &mut source_floor,
                        source_retained_bytes,
                        &mut facts,
                        &mut admission,
                        work,
                    )?;
                    if let Some(aggregate) = aggregate_binding {
                        source_floor =
                            numeric.add(source_floor, aggregate.state_format.as_str().len())?;
                        if let Some(receipt) = &aggregate.state_interpretation {
                            source_floor = numeric.add(
                                source_floor,
                                numeric.bytes::<novarocks_type_contract::AggregateStateOrderKey>(
                                    receipt.order_keys.len(),
                                )?,
                            )?;
                        }
                        signature_source_floor(
                            &aggregate.function,
                            &mut source_floor,
                            source_retained_bytes,
                            &mut facts,
                            &mut admission,
                            work,
                        )?;
                    }
                }
                _ => {}
            }
            if let Some(counted) = counted {
                counted?;
            } else {
                count_kind(node, input, &mut facts, &mut counts, false)?;
            }
            work.step()?;
        }
        cap(
            numeric.add(source_floor, counts.max_lambda_id_bytes)?,
            source_retained_bytes,
            work,
        )?;
        facts.cumulative_work_upper_bound = admission.numeric(count_work(
            &facts, &counts, types, functions, aggregates, parameters, pools,
        ))?;
        facts.coexisting_source_and_request_bytes_upper_bound = numeric.add(
            source_retained_bytes,
            facts.new_allocation_request_bytes_upper_bound,
        )?;
        admission.complete(&mut facts, work)?;
        // Every original binding is proved before the first output allocation.
        for ((_, node), input) in arena.iter().zip(inputs) {
            let verified = if admission.observed() {
                let mut verified = None;
                let source = types.value_type_captured::<Error>(
                    input.value_type_id,
                    &mut |source, work| {
                        verified = Some(compare_types(
                            &node.ty,
                            source,
                            &mut facts,
                            &mut admission,
                            work,
                        )?);
                        Ok(())
                    },
                    work,
                )?;
                source
                    .ok_or_else(|| shape("expression refers to an unknown original value type"))?;
                verified.ok_or_else(|| shape("expression type capture is absent"))?
            } else {
                let (_, source) = types
                    .root_value_binding_observed(input.value_type_id, work)?
                    .ok_or_else(|| shape("expression refers to an unknown original value type"))?;
                compare_types(&node.ty, source, &mut facts, &mut admission, work)?
            };
            if !verified.matches() {
                return Err(shape(
                    "expression value type differs from its original root",
                ));
            }
            match &node.kind {
                ExprKind::Lambda {
                    parameter_types, ..
                } => {
                    let same = parameter_types.len() == input.lambda_parameter_type_ids.len();
                    work.step()?;
                    if !same {
                        return Err(shape(
                            "lambda type ID count differs from original parameters",
                        ));
                    }
                    for (parameter, id) in
                        parameter_types.iter().zip(input.lambda_parameter_type_ids)
                    {
                        let verified = if admission.observed() {
                            let mut verified = None;
                            let source = types.value_type_captured::<Error>(
                                *id,
                                &mut |source, work| {
                                    verified = Some(compare_types(
                                        parameter,
                                        source,
                                        &mut facts,
                                        &mut admission,
                                        work,
                                    )?);
                                    Ok(())
                                },
                                work,
                            )?;
                            source.ok_or_else(|| {
                                shape("lambda refers to an unknown original value type")
                            })?;
                            verified.ok_or_else(|| shape("lambda type capture is absent"))?
                        } else {
                            let source =
                                types.value_type_observed(*id, work)?.ok_or_else(|| {
                                    shape("lambda refers to an unknown original value type")
                                })?;
                            compare_types(parameter, source, &mut facts, &mut admission, work)?
                        };
                        if !verified.matches() {
                            return Err(shape("lambda parameter type differs from original root"));
                        }
                    }
                }
                _ => {
                    let empty = input.lambda_parameter_type_ids.is_empty();
                    work.step()?;
                    if !empty {
                        return Err(shape("non-lambda definition has lambda parameter type IDs"));
                    }
                }
            }
            let selected_function = match &node.kind {
                ExprKind::FunctionCall { function, .. } | ExprKind::WindowCall { function, .. } => {
                    Some(function)
                }
                _ => None,
            };
            let same_presence = selected_function.is_some() == input.function_binding_id.is_some();
            work.step()?;
            if !same_presence {
                return Err(shape(
                    "expression function ID presence differs from original kind",
                ));
            }
            if let Some(function) = selected_function {
                let id = input
                    .function_binding_id
                    .ok_or_else(|| shape("missing expression function ID"))?;
                let verified = if admission.observed() {
                    let mut verified = None;
                    let source = functions.scalar_binding_captured(
                        id,
                        &mut |source, work| {
                            verified = Some(
                                compare_scalar(function, source, &mut facts, &mut admission, work)
                                    .map_err(binding_error)?,
                            );
                            Ok(())
                        },
                        work,
                    )?;
                    source
                        .ok_or_else(|| shape("expression refers to an unknown scalar signature"))?;
                    verified.ok_or_else(|| shape("expression signature capture is absent"))?
                } else {
                    let source = functions
                        .scalar_binding_observed(id, work)?
                        .ok_or_else(|| shape("expression refers to an unknown scalar signature"))?;
                    compare_scalar(function, source, &mut facts, &mut admission, work)?
                };
                if !verified.matches() {
                    return Err(shape("expression function differs from original signature"));
                }
            }
            let aggregate = match &node.kind {
                ExprKind::WindowCall {
                    aggregate_binding, ..
                } => aggregate_binding.as_deref(),
                _ => None,
            };
            let same_presence = aggregate.is_some() == input.aggregate_binding_id.is_some();
            work.step()?;
            if !same_presence {
                return Err(shape(
                    "expression aggregate ID presence differs from original kind",
                ));
            }
            if let Some(aggregate) = aggregate {
                let id = input
                    .aggregate_binding_id
                    .ok_or_else(|| shape("missing expression aggregate ID"))?;
                let verified = if admission.observed() {
                    let mut verified = None;
                    let source = aggregates.binding_captured(
                        id,
                        &mut |source, work| {
                            verified = Some(
                                compare_aggregate(
                                    aggregate,
                                    source,
                                    &mut facts,
                                    &mut admission,
                                    work,
                                )
                                .map_err(binding_error)?,
                            );
                            Ok(())
                        },
                        work,
                    )?;
                    source.ok_or_else(|| {
                        shape("expression refers to an unknown aggregate signature")
                    })?;
                    verified.ok_or_else(|| shape("expression aggregate capture is absent"))?
                } else {
                    let source = aggregates.binding_observed(id, work)?.ok_or_else(|| {
                        shape("expression refers to an unknown aggregate signature")
                    })?;
                    compare_aggregate(aggregate, source, &mut facts, &mut admission, work)?
                };
                if !verified.matches() {
                    return Err(shape(
                        "expression aggregate differs from original signature",
                    ));
                }
            }
            if let ExprKind::Cast { target, .. } = &node.kind {
                // Reuse the same left topology's already measured full bound.
                // A raw target has no independent nullable/logical root flags.
                admission.charge(&mut facts, verified.work_upper_bound(), work)?;
                work.flush()?;
                let matched = arrow_data_types_exact_borrowed_with_profile_observed(
                    node.ty.data_type(),
                    target,
                    novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
                    || {
                        work.step()
                            .map_err(crate::physical_type_v2::TypeCodecError::from)
                    },
                );
                let matched = match matched {
                    Err(crate::physical_type_v2::TypeCodecError::Control(cause)) => {
                        return Err(Error::Control(cause));
                    }
                    result => result,
                };
                work.flush()?;
                if !matched? {
                    return Err(shape(
                        "cast target differs from its original result carrier",
                    ));
                }
            }
            if let ExprKind::Binary {
                op,
                allow_throw_exception,
                ..
            } = &node.kind
            {
                let arithmetic = matches!(
                    op,
                    BinaryOperator::Add
                        | BinaryOperator::Subtract
                        | BinaryOperator::Multiply
                        | BinaryOperator::Divide
                        | BinaryOperator::Modulo
                );
                let same_presence = arithmetic == allow_throw_exception.is_some();
                work.step()?;
                if !same_presence {
                    return Err(shape(
                        "binary intrinsic parameter presence differs from arithmetic kind",
                    ));
                }
            }
            for reference in node.kind.intrinsic_parameter_references() {
                let key_matches =
                    reference.expected_key == SemanticParameterKey::AllowThrowException;
                work.step()?;
                if !key_matches {
                    return Err(shape("intrinsic parameter has a wrong expected key"));
                }
                work.flush()?;
                let value = parameters.require(*reference);
                work.step()?;
                work.flush()?;
                value.map_err(SemanticsCodecError::from)?;
            }
            if let ExprKind::Constant(reference) = &node.kind {
                work.flush()?;
                let pool = pools.entries().get(&reference.pool);
                let captured = if admission.observed() {
                    let result = pool
                        .map(|pool| {
                            compare_types(
                                &node.ty,
                                pool.value_type(),
                                &mut facts,
                                &mut admission,
                                work,
                            )
                        })
                        .transpose();
                    if let Err(Error::Control(cause)) = &result {
                        return Err(Error::Control(cause.clone()));
                    }
                    Some(result)
                } else {
                    None
                };
                work.step()?;
                work.flush()?;
                let pool = pool.ok_or_else(|| {
                    shape("expression refers to an unknown original constant pool")
                })?;
                let retained =
                    usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes).map_err(
                        |_| shape("constant expression source retained extent is unrepresentable"),
                    );
                work.step()?;
                cap(retained?, source_retained_bytes, work)?;
                let ordinal_valid = u64::from(reference.ordinal) < pool.resource_facts().rows;
                work.step()?;
                if !ordinal_valid {
                    return Err(shape("constant ordinal differs from original pool extent"));
                }
                let verified = if let Some(captured) = captured {
                    captured?.ok_or_else(|| {
                        shape("expression refers to an unknown original constant pool")
                    })?
                } else {
                    compare_types(
                        &node.ty,
                        pool.value_type(),
                        &mut facts,
                        &mut admission,
                        work,
                    )?
                };
                if !verified.matches() {
                    return Err(shape("constant expression type differs from original pool"));
                }
            }
        }
        Ok(PreparedExpressionNamespaceWrite {
            arena,
            inputs,
            types,
            _functions: functions,
            _aggregates: aggregates,
            _parameters: parameters,
            _pools: pools,
            facts,
            control,
            source_retained_bytes,
            limits,
        })
    })()
}

pub fn encode_expression_definitions<'loan, 'source, 'control>(
    arena: &'loan ExprArena,
    inputs: &'loan [ExpressionTypeIds<'source>],
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    aggregates: &'loan EncodedAggregateBindings<'loan, 'source>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<EncodedExpressions<'loan, 'source, 'control>, Error> {
    prepare_expression_definitions(
        arena,
        inputs,
        types,
        functions,
        aggregates,
        parameters,
        pools,
        source_retained_bytes,
        limits,
        control,
    )?
    .emit()
}

#[derive(Default)]
struct KindCounts {
    function_lookups: usize,
    aggregate_lookups: usize,
    parameter_lookups: usize,
    constant_lookups: usize,
    max_lambda_id_bytes: usize,
}
fn count_kind(
    node: &ExprNode,
    input: &ExpressionTypeIds<'_>,
    facts: &mut ExpressionNamespaceWriteFacts,
    counts: &mut KindCounts,
    observed: bool,
) -> Result<(), Error> {
    let numeric = |r: Result<(), Error>| {
        if observed {
            r.map_err(|_| novarocks_type_contract::CompileControlError::ResourceExhausted.into())
        } else {
            r
        }
    };
    let numeric_add = |a, b| {
        if observed {
            add(a, b)
                .map_err(|_| novarocks_type_contract::CompileControlError::ResourceExhausted.into())
        } else {
            add(a, b)
        }
    };
    let numeric_mul = |a, b| {
        if observed {
            mul(a, b)
                .map_err(|_| novarocks_type_contract::CompileControlError::ResourceExhausted.into())
        } else {
            mul(a, b)
        }
    };
    let mut refs = 0usize;
    match &node.kind {
        ExprKind::Value(_) => {}
        ExprKind::LambdaParameter { .. } => refs = 1,
        ExprKind::Constant(_) => counts.constant_lookups = numeric_add(counts.constant_lookups, 1)?,
        ExprKind::Literal(_) => {
            return Err(shape(
                "legacy literal cannot be encoded as a constant reference",
            ));
        }
        ExprKind::Unary { .. } | ExprKind::IsNull { .. } | ExprKind::IsTruthValue { .. } => {
            refs = 1
        }
        ExprKind::Binary { .. } => refs = 2,
        ExprKind::Cast { .. } => {
            refs = 1;
            facts.type_reference_count = numeric_add(facts.type_reference_count, 1)?;
        }
        ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => {
            refs = args.len();
            numeric(vector::<u32>(facts, args.len()))?;
        }
        ExprKind::FunctionCall { args, .. } => {
            refs = args.len();
            numeric(vector::<u32>(facts, args.len()))?;
            counts.function_lookups = numeric_add(counts.function_lookups, 1)?;
        }
        ExprKind::Lambda {
            parameter_types, ..
        } => {
            refs = 1;
            numeric(vector::<u32>(facts, parameter_types.len()))?;
            facts.type_reference_count =
                numeric_add(facts.type_reference_count, parameter_types.len())?;
            counts.max_lambda_id_bytes = counts
                .max_lambda_id_bytes
                .max(bytes::<u32>(input.lambda_parameter_type_ids.len())?);
        }
        ExprKind::InList { list, .. } => {
            refs = numeric_add(1, list.len())?;
            numeric(vector::<u32>(facts, list.len()))?;
        }
        ExprKind::Between { .. } => refs = 3,
        ExprKind::Like { .. } => refs = 2,
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            refs = numeric_add(
                numeric_add(
                    usize::from(operand.is_some()),
                    numeric_mul(2, when_then.len())?,
                )?,
                usize::from(else_expr.is_some()),
            )?;
            numeric(vector::<wire::WhenThen>(facts, when_then.len()))?;
        }
        ExprKind::WindowCall {
            args,
            function_order_by,
            frame,
            aggregate_binding,
            ..
        } => {
            refs = numeric_add(args.len(), function_order_by.len())?;
            if let Some(frame) = frame {
                for bound in [&frame.start, &frame.end] {
                    if matches!(
                        bound,
                        novarocks_physical_plan::WindowBound::Preceding(_)
                            | novarocks_physical_plan::WindowBound::Following(_)
                    ) {
                        refs = numeric_add(refs, 1)?;
                    }
                }
            }
            numeric(vector::<u32>(facts, args.len()))?;
            numeric(vector::<wire::SortExpression>(
                facts,
                function_order_by.len(),
            ))?;
            counts.function_lookups = numeric_add(counts.function_lookups, 1)?;
            counts.aggregate_lookups = numeric_add(
                counts.aggregate_lookups,
                usize::from(aggregate_binding.is_some()),
            )?;
        }
    }
    facts.expression_reference_count = numeric_add(
        facts.expression_reference_count,
        numeric_add(refs, usize::from(node.lambda_scope.is_some()))?,
    )?;
    counts.parameter_lookups = numeric_add(
        counts.parameter_lookups,
        node.kind.intrinsic_parameter_references().count(),
    )?;
    Ok(())
}
fn initial_source_floor(
    count: usize,
    types: &EncodedTypeTable<'_>,
    functions: &EncodedFunctionBindings<'_, '_>,
    aggregates: &EncodedAggregateBindings<'_, '_>,
) -> Result<usize, Error> {
    let mut source_floor = add(
        bytes::<ExprNode>(count)?,
        bytes::<ExpressionTypeIds<'_>>(count)?,
    )?;
    source_floor = add(
        source_floor,
        bytes::<wire::FunctionBindingDefinition>(functions.as_wire().len())?,
    )?;
    source_floor = add(
        source_floor,
        bytes::<wire::AggregateBindingDefinition>(aggregates.as_wire().len())?,
    )?;
    let table = types.as_wire();
    source_floor = add(
        source_floor,
        bytes::<novarocks_proto_models::physical_type_v2::CarrierTypeDefinition>(
            table.carriers.capacity(),
        )?,
    )?;
    source_floor = add(
        source_floor,
        bytes::<novarocks_proto_models::physical_type_v2::ValueTypeDefinition>(
            table.value_types.capacity(),
        )?,
    )?;
    source_floor = add(
        source_floor,
        bytes::<novarocks_proto_models::physical_type_v2::FieldDefinition>(
            table.fields.capacity(),
        )?,
    )?;
    source_floor = add(
        source_floor,
        bytes::<novarocks_proto_models::value_type::LogicalTypeDefinition>(
            table.logical_types.capacity(),
        )?,
    )?;
    Ok(source_floor)
}
fn initial_facts(count: usize) -> Result<ExpressionNamespaceWriteFacts, Error> {
    let mut facts = ExpressionNamespaceWriteFacts {
        definition_count: count,
        type_reference_count: count,
        cumulative_work_upper_bound: add(128, mul(count, 96)?)?,
        ..Default::default()
    };
    vector::<wire::ExpressionDefinition>(&mut facts, count)?;
    Ok(facts)
}
fn count_work(
    facts: &ExpressionNamespaceWriteFacts,
    counts: &KindCounts,
    types: &EncodedTypeTable<'_>,
    functions: &EncodedFunctionBindings<'_, '_>,
    aggregates: &EncodedAggregateBindings<'_, '_>,
    parameters: &SemanticParameters,
    pools: &ConstantPools,
) -> Result<usize, Error> {
    let (value_roots, _) = types.source_counts();
    let lookups = add(
        mul(facts.type_reference_count, value_roots)?,
        add(
            mul(counts.function_lookups, functions.source_counts())?,
            mul(counts.aggregate_lookups, aggregates.source_counts())?,
        )?,
    )?;
    let tree_work = add(
        mul(
            counts.parameter_lookups,
            tree_lookup_work(parameters.entries().len())?,
        )?,
        mul(
            counts.constant_lookups,
            tree_lookup_work(pools.entries().len())?,
        )?,
    )?;
    add(
        facts.cumulative_work_upper_bound,
        add(
            facts.new_allocation_request_bytes_upper_bound,
            add(
                tree_work,
                add(lookups, mul(facts.expression_reference_count, 16)?)?,
            )?,
        )?,
    )
}

#[cfg(test)]
#[path = "namespace_tests.rs"]
mod tests;
