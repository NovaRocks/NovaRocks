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

//! Owned projection from the original checked expression namespace.
//! Original Fragment validation still owns graph, lexical and call semantics.

use super::owner_admission::{Admission, Admit, lookup_facts, same_control};
use super::{
    DecodedExpressions, ExpressionCodecError as Error, ExpressionNamespaceWriteFacts as Facts,
    ExpressionProjectionLimits as Limits,
};
use super::{namespace::finish, receiving_grammar};
use crate::{
    btree_resources_v2::{self, BTreeResourceError},
    physical_aggregate_binding_v2::MaterializedAggregateBindings,
    physical_binding_v2::{
        BindingProjectionLimits, MaterializationModel, MaterializedFunctionBinding,
        MaterializedFunctionBindings, add, boxed, cap, copy_scalar_signature_observed, mul,
        preflight_scalar_signature_copy, preflight_scalar_signature_copy_in, reserve,
    },
    physical_semantics_v2::{decode_decimal_policy, decode_reference},
    physical_type_v2::{DecodedTypeTable, clone_value_type_observed},
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::mem::size_of;

fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn tree_error(error: BTreeResourceError) -> Error {
    match error {
        BTreeResourceError::SourceModel(message) => shape(message),
        BTreeResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
    }
}
fn completed<T>(result: Result<T, Error>, w: &mut CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.step()?;
    result
}
fn required(
    value: Option<u32>,
    message: &'static str,
    w: &mut CompileCheckpoints<'_>,
) -> Result<u32, Error> {
    completed(value.ok_or_else(|| shape(message)), w)
}
fn binding_limits(l: Limits) -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: l.max_definitions,
        max_type_references: l.max_type_references,
        max_allocation_requests: l.max_new_allocation_requests,
        max_request_bytes: l.max_new_allocation_request_bytes,
        max_coexisting_source_and_request_bytes: l.max_coexisting_source_and_request_bytes,
        max_work: l.max_cumulative_work,
    }
}
fn facts(model: &MaterializationModel, refs: usize) -> Facts {
    binding_facts(&model.facts, refs)
}
fn binding_facts(f: &crate::physical_binding_v2::BindingProjectionFacts, refs: usize) -> Facts {
    Facts {
        definition_count: f.definition_count,
        type_reference_count: f.type_reference_count,
        expression_reference_count: refs,
        new_allocation_requests_upper_bound: f.allocation_requests_upper_bound,
        new_allocation_request_bytes_upper_bound: f.request_bytes_upper_bound,
        coexisting_source_and_request_bytes_upper_bound: f
            .coexisting_source_and_request_bytes_upper_bound,
        cumulative_work_upper_bound: f.cumulative_work_upper_bound,
    }
}

/// A real owned arena, retaining the original receiving namespace loans.
/// Into-arena ends those loans without granting graph or installed capability.
pub struct MaterializedExpressions<'loan, 'headers, 'wire, 'control> {
    definitions: Box<[novarocks_expression_contract::ExprNode]>,
    expressions: &'loan DecodedExpressions<'headers, 'wire, 'control>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'wire>,
    aggregates: &'loan MaterializedAggregateBindings<'loan, 'headers, 'wire>,
    facts: Facts,
    source_invoice: usize,
    retained_bytes: usize,
}
impl<'loan, 'headers, 'wire, 'control> MaterializedExpressions<'loan, 'headers, 'wire, 'control> {
    pub fn definitions(&self) -> &[novarocks_expression_contract::ExprNode] {
        let _namespace_loans = (&self.functions, &self.aggregates);
        &self.definitions
    }
    pub fn facts(&self) -> &Facts {
        &self.facts
    }
    pub fn into_definitions(self) -> Box<[novarocks_expression_contract::ExprNode]> {
        self.definitions
    }
    #[cfg(test)]
    pub(crate) fn expressions(&self) -> &'loan DecodedExpressions<'headers, 'wire, 'control> {
        self.expressions
    }
    #[cfg(test)]
    pub(crate) fn functions(&self) -> &'loan MaterializedFunctionBindings<'headers, 'wire> {
        self.functions
    }
    #[cfg(test)]
    pub(crate) fn aggregates(
        &self,
    ) -> &'loan MaterializedAggregateBindings<'loan, 'headers, 'wire> {
        self.aggregates
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.expressions.original_control()
    }
    pub fn lookup_work_upper_bound(&self) -> Result<usize, Error> {
        btree_resources_v2::lookup_work_typed(self.definitions.len()).map_err(tree_error)
    }
    /// Caller admits this opaque lookup's work and owns entry/ordinary footer.
    pub fn definition_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&novarocks_expression_contract::ExprNode>, Error> {
        let same = std::ptr::addr_eq(w.control(), self.original_control());
        w.step()?;
        if !same {
            return Err(shape(
                "owned expression lookup has a different original control",
            ));
        }
        w.flush()?;
        let found = self
            .definitions
            .get(id as usize)
            .filter(|node| node.id.get() == id);
        w.step()?;
        w.flush()?;
        Ok(found)
    }
    pub fn definition_in(
        &self,
        id: u32,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&novarocks_expression_contract::ExprNode>, Error> {
        same_control(self.original_control(), work)?;
        admit(&lookup_facts(
            self.lookup_work_upper_bound()?
                .checked_add(1)
                .ok_or(CompileControlError::ResourceExhausted)?,
        ))?;
        self.definition_observed(id, work)
    }
    /// Necessary owned output floor; no private BTree capacity is guessed.
    pub(crate) fn retained_output_floor(&self) -> Result<usize, Error> {
        Ok(add(size_of::<Self>(), self.retained_bytes)?)
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        Ok(add(self.source_invoice, self.retained_output_floor()?)?)
    }
}

pub struct PreparedExpressionMaterialization<'loan, 'headers, 'wire, 'control> {
    expressions: &'loan DecodedExpressions<'headers, 'wire, 'control>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'wire>,
    aggregates: &'loan MaterializedAggregateBindings<'loan, 'headers, 'wire>,
    facts: Facts,
    limits: Limits,
    source_invoice: usize,
    retained_bytes: usize,
}
impl PreparedExpressionMaterialization<'_, '_, '_, '_> {
    pub fn facts(&self) -> &Facts {
        &self.facts
    }
}

fn type_root<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    w.flush()?;
    let found = types.value_type(id);
    w.step()?;
    w.flush()?;
    found.ok_or_else(|| shape("owned expression value type is absent"))
}
fn function<'a>(
    functions: &'a MaterializedFunctionBindings<'_, '_>,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a novarocks_expression_contract::BoundFunction, Error> {
    match functions.definition_observed(id, w)? {
        Some(MaterializedFunctionBinding::Scalar(source)) => Ok(source),
        _ => Err(shape("owned expression scalar-result binding is absent")),
    }
}
fn list<T>(
    model: &mut MaterializationModel,
    n: usize,
    l: BindingProjectionLimits,
) -> Result<(), Error> {
    model.items = add(model.items, n)?;
    model.request::<T>(n, 2)?;
    model.check(l)?;
    Ok(())
}

fn check_model(
    model: &mut MaterializationModel,
    refs: usize,
    limits: BindingProjectionLimits,
    admission: &mut Admission<'_, '_>,
) -> Result<(), Error> {
    model.check(limits)?;
    admission.fixed(&facts(model, refs))
}
fn parent_gate(
    admission: &mut Admission<'_, '_>,
    prefix: &crate::physical_binding_v2::BindingProjectionFacts,
    refs: usize,
) -> Result<(), CompileControlError> {
    match admission.fixed(&binding_facts(prefix, refs)) {
        Ok(()) => Ok(()),
        Err(Error::Control(cause)) => Err(cause),
        _ => unreachable!("the synchronous numerical gate has only typed control errors"),
    }
}
fn clone_preflight(
    source: &FunctionValueType,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    refs: usize,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::physical_binding_v2::BindingCodecError> {
    if admission.observed() {
        model.count_owned_type_clone_in(
            source,
            limits,
            &mut |prefix| parent_gate(admission, prefix, refs),
            work,
        )
    } else {
        model.count_owned_type_clone(source, limits, work)
    }
}
fn type_root_preflight(
    types: &DecodedTypeTable,
    id: u32,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    refs: usize,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if !admission.observed() {
        return Ok(model.count_owned_type_clone(type_root(types, id, work)?, limits, work)?);
    }
    work.flush()?;
    let found = types.value_type(id);
    if let Some(source) = found {
        clone_preflight(source, model, limits, refs, admission, work)?;
    }
    work.step()?;
    work.flush()?;
    found.ok_or_else(|| shape("owned expression value type is absent"))?;
    Ok(())
}
fn preflight(
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    plan_limits: &p::PlanLimits,
    admission: &mut Admission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(Facts, usize), Error> {
    let source = admission.source;
    let limits = admission.limits;
    let same = std::ptr::eq(functions.headers(), expressions.functions())
        && std::ptr::eq(aggregates.headers(), expressions.aggregates())
        && std::ptr::eq(aggregates.functions(), functions)
        && std::ptr::addr_eq(w.control(), expressions.original_control())
        && std::ptr::addr_eq(w.control(), functions.headers().original_control())
        && std::ptr::addr_eq(w.control(), aggregates.original_control());
    if !admission.observed() {
        w.step()?;
        if !same {
            return Err(shape(
                "expression materialization has different original namespaces or control",
            ));
        }
    }
    let n = expressions.source_count();
    // Count and the original Fragment limit are known before any output walk.
    cap(n, plan_limits.fragment_expressions)?;
    let refs = expressions.facts().expression_reference_count;
    cap(refs, limits.max_expression_references)?;
    // These three floor authors contain only checked numerical operations.
    // The caller-owned path must lift their known overflow before any later
    // namespace observation; Plain keeps its original error and trace.
    let known = admission.numeric(expressions.owned_binding_floor(functions, Some(aggregates)))?;
    if !admission.observed() {
        expressions.retained_floor_observed(w)?;
    }
    let lookup = mul(
        2,
        btree_resources_v2::lookup_work_typed(expressions.types().value_types().len())
            .map_err(tree_error)?,
    )?;
    let mut model = MaterializationModel::for_composition(n, lookup, source, known);
    model.facts.type_reference_count = n;
    model.items = refs;
    // The local dense declaration table is one owned Box slice. Reserve and
    // possible Vec-to-Box shrink share this original request upper bound.
    model.request::<novarocks_expression_contract::ExprNode>(n, 2)?;
    // At most one function plus one aggregate namespace lookup per definition
    // in each pass; full type lookup work is charged per actual clone root.
    model.add_work(mul(
        mul(n, 2)?,
        add(
            functions.definitions().len(),
            aggregates.definitions().len(),
        )?,
    )?)?;
    let bl = binding_limits(limits);
    check_model(&mut model, refs, bl, admission)?;
    if admission.observed() {
        w.step()?;
        if !same {
            return Err(shape(
                "expression materialization has different original namespaces or control",
            ));
        }
        // The old receiving-floor observation still accounts its completed lookup.
        w.step()?;
    }
    // The count-only pass gates all source-owned list requests and type roots
    // before either the first nested clone walk or the first output allocation.
    for raw in expressions.as_wire() {
        use wire::expression_definition::Kind as K;
        let captured = raw
            .kind
            .as_ref()
            .ok_or_else(|| shape("owned expression kind is absent"));
        let kind = if admission.observed() {
            match captured {
                Ok(kind) => kind,
                Err(error) => return completed(Err(error), w),
            }
        } else {
            completed(captured, w)?
        };
        match kind {
            K::Conjunction(v) | K::Disjunction(v) => {
                list::<p::ExprId>(&mut model, v.expr_ids.len(), bl)?
            }
            K::InList(v) => list::<p::ExprId>(&mut model, v.list_expr_ids.len(), bl)?,
            K::CaseExpression(v) => list::<(p::ExprId, p::ExprId)>(&mut model, v.arms.len(), bl)?,
            K::Lambda(v) => {
                model.facts.type_reference_count = add(
                    model.facts.type_reference_count,
                    v.parameter_value_type_ids.len(),
                )?;
                list::<FunctionValueType>(&mut model, v.parameter_value_type_ids.len(), bl)?;
            }
            K::Cast(_) => {
                model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
            }
            K::FunctionCall(v) => list::<p::ExprId>(&mut model, v.argument_expr_ids.len(), bl)?,

            K::ValueId(_)
            | K::Constant(_)
            | K::LambdaParameter(_)
            | K::Unary(_)
            | K::Binary(_)
            | K::IsNull(_)
            | K::Between(_)
            | K::Like(_)
            | K::IsTruthValue(_) => {}
        }
        check_model(&mut model, refs, bl, admission)?;
        if admission.observed() {
            w.step()?;
        }
        w.step()?;
    }
    for raw in expressions.as_wire() {
        let id = required(
            raw.value_type_id,
            "owned expression value type is absent",
            w,
        )?;
        type_root_preflight(expressions.types(), id, &mut model, bl, refs, admission, w)?;
        use wire::expression_definition::Kind as K;
        match completed(
            raw.kind
                .as_ref()
                .ok_or_else(|| shape("owned expression kind is absent")),
            w,
        )? {
            K::Lambda(v) => {
                for id in &v.parameter_value_type_ids {
                    type_root_preflight(
                        expressions.types(),
                        *id,
                        &mut model,
                        bl,
                        refs,
                        admission,
                        w,
                    )?;
                    w.step()?;
                }
            }
            K::Cast(_) => {
                // The explicit value binding owns a normalized layout: all
                // descendants are shared Field/Fields/timezone references,
                // without Dictionary/REE Box allocations. The resulting
                // ExprNode keeps that same binding as its creation lifetime.
                model.add_work(2)?;
                check_model(&mut model, refs, bl, admission)?;
            }
            K::FunctionCall(v) => {
                let id = required(v.function_binding_id, "owned call binding is absent", w)?;
                if admission.observed() {
                    let found = functions.definition_captured(
                        id,
                        &mut |binding, work| {
                            if let MaterializedFunctionBinding::Scalar(source) = binding {
                                preflight_scalar_signature_copy_in(
                                    source,
                                    &mut model,
                                    bl,
                                    &mut |prefix| parent_gate(admission, prefix, refs),
                                    work,
                                )?;
                            }
                            Ok(())
                        },
                        w,
                    )?;
                    if !matches!(found, Some(MaterializedFunctionBinding::Scalar(_))) {
                        return Err(shape("owned expression scalar-result binding is absent"));
                    }
                } else {
                    preflight_scalar_signature_copy(
                        function(functions, id, w)?,
                        &mut model,
                        bl,
                        w,
                    )?;
                }
            }

            K::ValueId(_)
            | K::Constant(_)
            | K::LambdaParameter(_)
            | K::Unary(_)
            | K::Binary(_)
            | K::Conjunction(_)
            | K::Disjunction(_)
            | K::IsNull(_)
            | K::InList(_)
            | K::Between(_)
            | K::Like(_)
            | K::CaseExpression(_)
            | K::IsTruthValue(_) => {}
        }
        check_model(&mut model, refs, bl, admission)?;
        w.step()?;
    }
    let retained = model.retained;
    Ok((facts(&model, refs), retained))
}

pub fn prepare_expression_materialization<'loan, 'headers, 'wire, 'control>(
    expressions: &'loan DecodedExpressions<'headers, 'wire, 'control>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'wire>,
    aggregates: &'loan MaterializedAggregateBindings<'loan, 'headers, 'wire>,
    plan_limits: &'loan p::PlanLimits,
    source_retained_bytes: usize,
    limits: Limits,
) -> Result<PreparedExpressionMaterialization<'loan, 'headers, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let mut admission = Admission {
        parent: None,
        source: source_retained_bytes,
        limits,
    };
    let result = prepare_core(
        expressions,
        functions,
        aggregates,
        plan_limits,
        &mut admission,
        &mut w,
    );
    finish(w, result)
}
pub fn prepare_expression_materialization_in<'loan, 'headers, 'wire, 'control>(
    expressions: &'loan DecodedExpressions<'headers, 'wire, 'control>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'wire>,
    aggregates: &'loan MaterializedAggregateBindings<'loan, 'headers, 'wire>,
    plan_limits: &'loan p::PlanLimits,
    source_retained_bytes: usize,
    limits: Limits,
    admit: &mut Admit<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<PreparedExpressionMaterialization<'loan, 'headers, 'wire, 'control>, Error> {
    same_control(expressions.original_control(), w)?;
    let mut admission = Admission {
        parent: Some(admit),
        source: source_retained_bytes,
        limits,
    };
    prepare_core(
        expressions,
        functions,
        aggregates,
        plan_limits,
        &mut admission,
        w,
    )
}
fn prepare_core<'loan, 'headers, 'wire, 'control>(
    expressions: &'loan DecodedExpressions<'headers, 'wire, 'control>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'wire>,
    aggregates: &'loan MaterializedAggregateBindings<'loan, 'headers, 'wire>,
    plan_limits: &'loan p::PlanLimits,
    admission: &mut Admission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<PreparedExpressionMaterialization<'loan, 'headers, 'wire, 'control>, Error> {
    let source_retained_bytes = admission.source;
    let limits = admission.limits;
    preflight(
        expressions,
        functions,
        aggregates,
        plan_limits,
        admission,
        w,
    )
    .map(
        |(facts, retained_bytes)| PreparedExpressionMaterialization {
            expressions,
            functions,
            aggregates,
            facts,
            limits,
            source_invoice: source_retained_bytes,
            retained_bytes,
        },
    )
}

fn clone_type(
    source: &FunctionValueType,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, Error> {
    w.flush()?;
    let copied = clone_value_type_observed(source, w)?;
    w.step()?;
    w.flush()?;
    Ok(copied)
}
fn copy_value_projection(
    source: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<arrow::datatypes::DataType, Error> {
    work.flush()?;
    work.step()?;
    let projection = source.data_type().clone();
    work.step()?;
    work.flush()?;
    Ok(projection)
}
fn ids(input: &[u32], w: &mut CompileCheckpoints<'_>) -> Result<Box<[p::ExprId]>, Error> {
    let mut out = reserve(input.len(), w)?;
    for id in input {
        out.push(p::ExprId::new(*id));
        w.step()?;
    }
    Ok(boxed(out, w)?)
}
fn expr(
    value: Option<u32>,
    message: &'static str,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::ExprId, Error> {
    Ok(p::ExprId::new(required(value, message, w)?))
}
fn owned_kind(
    raw: &wire::ExpressionDefinition,
    ty: &FunctionValueType,
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    _aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_expression_contract::ExprKind, Error> {
    use wire::expression_definition::Kind as K;
    Ok(
        match completed(
            raw.kind
                .as_ref()
                .ok_or_else(|| shape("owned expression kind is absent")),
            w,
        )? {
            K::ValueId(id) => novarocks_expression_contract::ExprKind::Value(p::ValueId::new(*id)),
            K::Constant(v) => {
                novarocks_expression_contract::ExprKind::Constant(p::ConstantReference {
                    pool: p::ConstantPoolId::new(required(
                        v.pool_id,
                        "constant pool ID is absent",
                        w,
                    )?),
                    ordinal: v.row_ordinal,
                })
            }
            K::LambdaParameter(v) => novarocks_expression_contract::ExprKind::LambdaParameter {
                lambda: expr(v.lambda_expr_id, "lambda parameter owner is absent", w)?,
                ordinal: v.ordinal,
            },
            K::Unary(v) => novarocks_expression_contract::ExprKind::Unary {
                op: completed(receiving_grammar::decode_unary(v.op), w)?,
                expr: expr(v.expr_id, "unary operand is absent", w)?,
            },
            K::Binary(v) => novarocks_expression_contract::ExprKind::Binary {
                left: expr(v.left_expr_id, "binary left operand is absent", w)?,
                op: completed(receiving_grammar::decode_binary(v.op), w)?,
                right: expr(v.right_expr_id, "binary right operand is absent", w)?,
                decimal_overflow_policy: completed(
                    decode_decimal_policy(v.decimal_overflow_policy).map_err(Error::from),
                    w,
                )?,
                allow_throw_exception: v
                    .allow_throw_exception
                    .as_ref()
                    .map(|r| decode_reference(r, w).map_err(Error::from))
                    .transpose()?,
            },
            K::Conjunction(v) => novarocks_expression_contract::ExprKind::Conjunction {
                args: ids(&v.expr_ids, w)?,
            },
            K::Disjunction(v) => novarocks_expression_contract::ExprKind::Disjunction {
                args: ids(&v.expr_ids, w)?,
            },
            K::FunctionCall(v) => {
                let id = required(v.function_binding_id, "owned call binding is absent", w)?;
                novarocks_expression_contract::ExprKind::FunctionCall {
                    function: copy_scalar_signature_observed(function(functions, id, w)?, w)?,
                    args: ids(&v.argument_expr_ids, w)?,
                }
            }
            K::Lambda(v) => {
                let mut parameters = reserve(v.parameter_value_type_ids.len(), w)?;
                for id in &v.parameter_value_type_ids {
                    parameters.push(clone_type(type_root(expressions.types(), *id, w)?, w)?);
                    w.step()?;
                }
                novarocks_expression_contract::ExprKind::Lambda {
                    parameter_types: boxed(parameters, w)?,
                    body: expr(v.body_expr_id, "lambda body is absent", w)?,
                }
            }
            K::Cast(v) => novarocks_expression_contract::ExprKind::Cast {
                expr: expr(v.expr_id, "cast operand is absent", w)?,
                target: copy_value_projection(ty, w)?,
                decimal_overflow_policy: completed(
                    decode_decimal_policy(v.decimal_overflow_policy).map_err(Error::from),
                    w,
                )?,
                allow_throw_exception: decode_reference(
                    v.allow_throw_exception
                        .as_ref()
                        .ok_or_else(|| shape("cast ALLOW reference is absent"))?,
                    w,
                )?,
                array_failure_mode: completed(
                    super::decode_array_cast_failure_mode(v.array_failure_mode),
                    w,
                )?,
            },
            K::IsNull(v) => novarocks_expression_contract::ExprKind::IsNull {
                expr: expr(v.expr_id, "null-test operand is absent", w)?,
                negated: v.negated,
            },
            K::InList(v) => novarocks_expression_contract::ExprKind::InList {
                expr: expr(v.expr_id, "in-list operand is absent", w)?,
                list: ids(&v.list_expr_ids, w)?,
                negated: v.negated,
            },
            K::Between(v) => novarocks_expression_contract::ExprKind::Between {
                expr: expr(v.expr_id, "between operand is absent", w)?,
                low: expr(v.low_expr_id, "between low operand is absent", w)?,
                high: expr(v.high_expr_id, "between high operand is absent", w)?,
                negated: v.negated,
            },
            K::Like(v) => novarocks_expression_contract::ExprKind::Like {
                expr: expr(v.expr_id, "like operand is absent", w)?,
                pattern: expr(v.pattern_expr_id, "like pattern is absent", w)?,
                negated: v.negated,
            },
            K::CaseExpression(v) => {
                let mut arms = reserve(v.arms.len(), w)?;
                for arm in &v.arms {
                    arms.push((
                        expr(arm.when_expr_id, "case when operand is absent", w)?,
                        expr(arm.then_expr_id, "case then operand is absent", w)?,
                    ));
                    w.step()?;
                }
                novarocks_expression_contract::ExprKind::Case {
                    operand: v.operand_expr_id.map(p::ExprId::new),
                    when_then: boxed(arms, w)?,
                    else_expr: v.else_expr_id.map(p::ExprId::new),
                }
            }
            K::IsTruthValue(v) => novarocks_expression_contract::ExprKind::IsTruthValue {
                expr: expr(v.expr_id, "truth-test operand is absent", w)?,
                value: v.value,
                negated: v.negated,
            },
        },
    )
}

pub fn materialize_expressions<'loan, 'headers, 'wire, 'control>(
    token: PreparedExpressionMaterialization<'loan, 'headers, 'wire, 'control>,
) -> Result<MaterializedExpressions<'loan, 'headers, 'wire, 'control>, Error> {
    let mut w =
        CompileCheckpoints::try_new(token.expressions.original_control(), CompilePhase::Decode)?;
    let result = materialize_core(token, None, &mut w);
    finish(w, result)
}
pub fn materialize_expressions_in<'loan, 'headers, 'wire, 'control>(
    token: PreparedExpressionMaterialization<'loan, 'headers, 'wire, 'control>,
    admit: &mut Admit<'_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedExpressions<'loan, 'headers, 'wire, 'control>, Error> {
    same_control(token.expressions.original_control(), w)?;
    materialize_core(token, Some(admit), w)
}
fn materialize_core<'loan, 'headers, 'wire, 'control>(
    token: PreparedExpressionMaterialization<'loan, 'headers, 'wire, 'control>,
    parent: Option<&mut Admit<'_>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedExpressions<'loan, 'headers, 'wire, 'control>, Error> {
    let mut admission = Admission {
        parent,
        source: token.source_invoice,
        limits: token.limits,
    };
    admission.fixed(&token.facts)?;
    (|| {
        let mut definitions = reserve(token.expressions.source_count(), w)?;
        for raw in token.expressions.as_wire() {
            let id = required(
                raw.value_type_id,
                "owned expression value type is absent",
                w,
            )?;
            let ty = clone_type(type_root(token.expressions.types(), id, w)?, w)?;
            let kind = owned_kind(
                raw,
                &ty,
                token.expressions,
                token.functions,
                token.aggregates,
                w,
            )?;
            definitions.push(novarocks_expression_contract::ExprNode {
                id: p::ExprId::new(raw.id),
                lambda_scope: raw.lambda_scope_expr_id.map(p::ExprId::new),
                ty,
                kind,
            });
            w.step()?;
        }
        w.flush()?;
        let definitions = definitions.into_boxed_slice();
        w.step()?;
        w.flush()?;
        Ok(MaterializedExpressions {
            definitions,
            expressions: token.expressions,
            functions: token.functions,
            aggregates: token.aggregates,
            facts: token.facts,
            source_invoice: token.source_invoice,
            retained_bytes: token.retained_bytes,
        })
    })()
}

#[cfg(test)]
#[path = "materialize_tests.rs"]
mod tests;
