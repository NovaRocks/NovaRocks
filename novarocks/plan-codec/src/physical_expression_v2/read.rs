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

//! Complete borrowed expression namespace. Every wire kind is inspected, but
//! no legacy Physical binding, expression arena or executable package is
//! manufactured. Fragment graph, lexical scope, operator typing and occurrence
//! effects remain with their original checked owners.

use super::namespace::{
    ExpressionNamespaceWriteFacts, add, cap, compare_types, finish, mul, preflight_types, shape,
    tree_lookup_work, vector,
};
use super::owner_admission::{Admission, Admit, Arithmetic, lookup_facts, same_control};
use super::{ExpressionCodecError as Error, ExpressionProjectionLimits};
use crate::{
    binding_index_v2::BindingIndex,
    physical_aggregate_binding_v2::PreparedAggregateBindingHeaders,
    physical_binding_v2::PreparedFunctionBindingHeaders,
    physical_semantics_v2::{SemanticsCodecError, decode_decimal_policy, decode_reference},
    physical_type_v2::{DecodedTypeTable, TypeCodecError},
    physical_value_v2::DecodedValues,
};
use novarocks_physical_plan::{ConstantPoolId, ConstantPools, ConstantReference};
use novarocks_proto_models::{physical_package_v2 as wire, physical_semantics_v2 as semantics};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, SemanticParameterKey,
    SemanticParameters, arrow_data_types_exact_borrowed_with_profile_observed,
};
use std::mem::size_of;

/// Same seven numerical quantities as the emission author. The only new heap
/// request in this receiving component is the original sparse usize index.
pub type ExpressionNamespaceReadFacts = ExpressionNamespaceWriteFacts;

/// Immutable receiving correspondence, retaining every original dependency.
/// Parameters and pools are borrowed checked owners; this token does not
/// invent provenance for their construction or replace Fragment validation.
pub struct DecodedExpressions<'loan, 'wire, 'control> {
    definitions: &'wire [wire::ExpressionDefinition],
    values: &'loan DecodedValues<'loan, 'wire, 'control>,
    functions: &'loan PreparedFunctionBindingHeaders<'wire>,
    aggregates: &'loan PreparedAggregateBindingHeaders<'loan, 'wire>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    indices: BindingIndex,
    facts: ExpressionNamespaceReadFacts,
    source_retained_bytes: usize,
    control: &'control dyn PureCompileControl,
}
impl<'loan, 'wire, 'control> DecodedExpressions<'loan, 'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::ExpressionDefinition] {
        self.definitions
    }
    pub const fn facts(&self) -> &ExpressionNamespaceReadFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.definitions.len()
    }
    pub fn values(&self) -> &'loan DecodedValues<'loan, 'wire, 'control> {
        self.values
    }
    pub fn types(&self) -> &DecodedTypeTable {
        self.values.types()
    }
    pub fn functions(&self) -> &'loan PreparedFunctionBindingHeaders<'wire> {
        self.functions
    }
    pub fn aggregates(&self) -> &'loan PreparedAggregateBindingHeaders<'loan, 'wire> {
        self.aggregates
    }
    pub fn parameters(&self) -> &'loan SemanticParameters {
        self.parameters
    }
    pub fn pools(&self) -> &'loan ConstantPools {
        self.pools
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.control
    }
    pub fn lookup_work_upper_bound(&self) -> Result<usize, Error> {
        tree_lookup_work(self.definitions.len())
    }
    pub fn definition(&self, id: u32) -> Result<Option<&'wire wire::ExpressionDefinition>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.definition_observed(id, &mut work);
        finish(work, result)
    }
    pub(crate) fn definition_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'wire wire::ExpressionDefinition>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.definitions[at].id, work)?
            .map(|at| &self.definitions[at]))
    }
    pub fn definition_in(
        &self,
        id: u32,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'wire wire::ExpressionDefinition>, Error> {
        same_control(self.original_control(), work)?;
        admit(&lookup_facts(self.lookup_work_upper_bound()?))?;
        self.definition_observed(id, work)
    }
    pub fn source_id_in(
        &self,
        source: &wire::ExpressionDefinition,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        same_control(self.original_control(), work)?;
        admit(&lookup_facts(add(self.lookup_work_upper_bound()?, 1)?))?;
        let actual = self.definition_observed(source.id, work)?;
        let same = actual.is_some_and(|actual| std::ptr::eq(actual, source));
        work.step()?;
        if !same {
            return Err(shape("expression DTO is not from this receiving namespace"));
        }
        Ok(source.id)
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
        self.source_retained_bytes
            .checked_add(size_of::<Self>())
            .and_then(|n| n.checked_add(self.indices.backing_bytes().ok()?))
            .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)
    }
    pub fn source_id(&self, source: &wire::ExpressionDefinition) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = (|| {
            let actual = self.definition_observed(source.id, &mut work)?;
            let same = actual.is_some_and(|actual| std::ptr::eq(actual, source));
            work.step()?;
            if !same {
                return Err(shape("expression DTO is not from this receiving namespace"));
            }
            Ok(source.id)
        })();
        finish(work, result)
    }
    pub fn value_type(&self, id: u32) -> Result<Option<&FunctionValueType>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.value_type_observed(id, &mut work);
        finish(work, result)
    }
    pub(crate) fn value_type_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&FunctionValueType>, Error> {
        let Some(definition) = self.definition_observed(id, work)? else {
            return Ok(None);
        };
        let type_id = required(definition.value_type_id, "expression value type is absent")?;
        work.flush()?;
        let value = self.types().value_type(type_id);
        work.step()?;
        work.flush()?;
        Ok(value)
    }
    /// Necessary floor, not a whole retained-size measurement or a MEM grant.
    /// The original whole-source invoice includes all borrowed dependencies.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(work, result)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let floor = add(
            self.source_retained_bytes,
            add(size_of::<Self>(), self.indices.backing_bytes()?)?,
        );
        work.step()?;
        floor
    }
}

/// Borrow all seventeen closed wire kinds. Types and control come from the
/// original value namespace; function and aggregate tokens must retain that
/// same immutable correspondence. Source is a mandatory whole coexistence
/// invoice, not encoded length, a guessed capacity or allocation authority.
pub fn decode_expression_definitions<'loan, 'wire, 'control>(
    definitions: &'wire [wire::ExpressionDefinition],
    values: &'loan DecodedValues<'loan, 'wire, 'control>,
    functions: &'loan PreparedFunctionBindingHeaders<'wire>,
    aggregates: &'loan PreparedAggregateBindingHeaders<'loan, 'wire>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
) -> Result<DecodedExpressions<'loan, 'wire, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Decode)?;
    let result = decode_core(
        definitions,
        values,
        functions,
        aggregates,
        parameters,
        pools,
        source_retained_bytes,
        limits,
        None,
        &mut work,
    );
    finish(work, result)
}
pub fn decode_expression_definitions_in<'loan, 'wire, 'control>(
    definitions: &'wire [wire::ExpressionDefinition],
    values: &'loan DecodedValues<'loan, 'wire, 'control>,
    functions: &'loan PreparedFunctionBindingHeaders<'wire>,
    aggregates: &'loan PreparedAggregateBindingHeaders<'loan, 'wire>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedExpressions<'loan, 'wire, 'control>, Error> {
    same_control(values.original_control(), work)?;
    decode_core(
        definitions,
        values,
        functions,
        aggregates,
        parameters,
        pools,
        source_retained_bytes,
        limits,
        Some(admit),
        work,
    )
}
fn decode_core<'loan, 'wire, 'control>(
    definitions: &'wire [wire::ExpressionDefinition],
    values: &'loan DecodedValues<'loan, 'wire, 'control>,
    functions: &'loan PreparedFunctionBindingHeaders<'wire>,
    aggregates: &'loan PreparedAggregateBindingHeaders<'loan, 'wire>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
    parent: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedExpressions<'loan, 'wire, 'control>, Error> {
    let control = values.original_control();
    let mut admission = Admission {
        parent,
        source: source_retained_bytes,
        limits,
    };
    (|| {
        if admission.observed() {
            let mut initial = admission.numeric(initial_facts(definitions.len()))?;
            admission.gate(&mut initial)?;
            // Pure captured headers share the original numerical authors;
            // no source invoice is charged again and ordinary floor law stays below.
            values.retained_floor_header_admitted()?;
            admission.numeric(functions.retained_invoice_floor().map_err(Error::from))?;
            admission.numeric(aggregates.retained_invoice_floor().map_err(Error::from))?;
        }
        let same = std::ptr::eq(values.types(), functions.type_table())
            && std::ptr::eq(functions, aggregates.functions())
            && std::ptr::addr_eq(control, functions.original_control());
        work.step()?;
        if !same {
            return Err(shape(
                "receiving expression dependencies are not the same original namespaces",
            ));
        }
        let facts = preflight(
            definitions,
            values,
            functions,
            aggregates,
            source_retained_bytes,
            &mut admission,
            work,
        )?;
        let indices = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
        let mut output = DecodedExpressions {
            definitions,
            values,
            functions,
            aggregates,
            parameters,
            pools,
            indices,
            facts,
            source_retained_bytes,
            control,
        };
        output.validate(&mut admission, work)?;
        Ok(output)
    })()
}

fn required(id: Option<u32>, message: &'static str) -> Result<u32, Error> {
    id.ok_or_else(|| shape(message))
}

/// Visit actual ordered references without scratch, filtering or cloning.
/// Scope references are included independently of kind dependencies.
fn references(
    definition: &wire::ExpressionDefinition,
    mut visit: impl FnMut(u32) -> Result<(), Error>,
) -> Result<(), Error> {
    use wire::expression_definition::Kind as K;
    if let Some(scope) = definition.lambda_scope_expr_id {
        visit(scope)?;
    }
    match definition
        .kind
        .as_ref()
        .ok_or_else(|| shape("expression kind is absent"))?
    {
        K::ValueId(_) | K::Literal(_) => {}
        K::LambdaParameter(value) => visit(required(
            value.lambda_expr_id,
            "lambda parameter owner is absent",
        )?)?,
        K::Unary(value) => visit(required(value.expr_id, "unary operand is absent")?)?,
        K::Binary(value) => {
            visit(required(
                value.left_expr_id,
                "binary left operand is absent",
            )?)?;
            visit(required(
                value.right_expr_id,
                "binary right operand is absent",
            )?)?;
        }
        K::Conjunction(value) | K::Disjunction(value) => {
            for id in &value.expr_ids {
                visit(*id)?;
            }
        }
        K::FunctionCall(value) => {
            for id in &value.argument_expr_ids {
                visit(*id)?;
            }
        }
        K::Lambda(value) => visit(required(value.body_expr_id, "lambda body is absent")?)?,
        K::Cast(value) => visit(required(value.expr_id, "cast operand is absent")?)?,
        K::IsNull(value) => visit(required(value.expr_id, "null-test operand is absent")?)?,
        K::InList(value) => {
            visit(required(value.expr_id, "in-list operand is absent")?)?;
            for id in &value.list_expr_ids {
                visit(*id)?;
            }
        }
        K::Between(value) => {
            visit(required(value.expr_id, "between operand is absent")?)?;
            visit(required(
                value.low_expr_id,
                "between low operand is absent",
            )?)?;
            visit(required(
                value.high_expr_id,
                "between high operand is absent",
            )?)?;
        }
        K::Like(value) => {
            visit(required(value.expr_id, "like operand is absent")?)?;
            visit(required(value.pattern_expr_id, "like pattern is absent")?)?;
        }
        K::CaseExpression(value) => {
            if let Some(id) = value.operand_expr_id {
                visit(id)?;
            }
            for arm in &value.arms {
                visit(required(arm.when_expr_id, "case when operand is absent")?)?;
                visit(required(arm.then_expr_id, "case then operand is absent")?)?;
            }
            if let Some(id) = value.else_expr_id {
                visit(id)?;
            }
        }
        K::IsTruthValue(value) => visit(required(value.expr_id, "truth-test operand is absent")?)?,
        K::WindowCall(value) => {
            for id in &value.argument_expr_ids {
                visit(*id)?;
            }
            for key in &value.function_order_by {
                visit(required(key.expr_id, "window sort operand is absent")?)?;
            }
            if let Some(frame) = &value.frame {
                for bound in [&frame.start, &frame.end] {
                    let bound = bound
                        .as_ref()
                        .ok_or_else(|| shape("window frame bound is absent"))?;
                    match super::receiving_grammar::decode_window_bound(bound)? {
                        novarocks_physical_plan::WindowBound::Preceding(id)
                        | novarocks_physical_plan::WindowBound::Following(id) => visit(id.get())?,
                        novarocks_physical_plan::WindowBound::UnboundedPreceding
                        | novarocks_physical_plan::WindowBound::CurrentRow
                        | novarocks_physical_plan::WindowBound::UnboundedFollowing => {}
                    }
                }
            }
        }
    }
    Ok(())
}

fn initial_facts(count: usize) -> Result<ExpressionNamespaceReadFacts, Error> {
    let mut facts = ExpressionNamespaceReadFacts {
        definition_count: count,
        type_reference_count: count,
        cumulative_work_upper_bound: add(128, mul(count, add(96, tree_lookup_work(count)?)?)?)?,
        ..Default::default()
    };
    vector::<usize>(&mut facts, count)?;
    Ok(facts)
}
fn preflight(
    definitions: &[wire::ExpressionDefinition],
    values: &DecodedValues<'_, '_, '_>,
    functions: &PreparedFunctionBindingHeaders<'_>,
    aggregates: &PreparedAggregateBindingHeaders<'_, '_>,
    source: usize,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ExpressionNamespaceReadFacts, Error> {
    let numeric = Arithmetic(admission.observed());
    let count = definitions.len();
    let limits = admission.limits;
    if admission.observed() {
        let mut initial = admission.numeric(initial_facts(count))?;
        admission.gate(&mut initial)?;
    }
    cap(count, limits.max_definitions, work)?;
    // Admit the count walk and index build before any source traversal/reserve.
    let mut facts = admission.numeric(initial_facts(count))?;
    facts.coexisting_source_and_request_bytes_upper_bound =
        numeric.add(source, facts.new_allocation_request_bytes_upper_bound)?;
    admission.complete(&mut facts, work)?;
    let mut floor = numeric.bytes::<wire::ExpressionDefinition>(count)?;
    // These are lower floors of one declared whole-source invoice. The caller
    // must include independent backings too; max does not invent that proof.
    let dependency_floor = values
        .retained_floor_observed(work)?
        .max(functions.retained_invoice_floor()?)
        .max(aggregates.retained_invoice_floor()?);
    work.step()?;
    cap(floor.max(dependency_floor), source, work)?;
    for definition in definitions {
        use wire::expression_definition::Kind as K;
        let kind = definition
            .kind
            .as_ref()
            .ok_or_else(|| shape("expression kind is absent"))?;
        let owned = match kind {
            K::Conjunction(value) | K::Disjunction(value) => {
                numeric.bytes::<u32>(value.expr_ids.capacity())?
            }
            K::FunctionCall(value) => numeric.bytes::<u32>(value.argument_expr_ids.capacity())?,
            K::Lambda(value) => numeric.bytes::<u32>(value.parameter_value_type_ids.capacity())?,
            K::InList(value) => numeric.bytes::<u32>(value.list_expr_ids.capacity())?,
            K::CaseExpression(value) => numeric.bytes::<wire::WhenThen>(value.arms.capacity())?,
            K::WindowCall(value) => numeric.add(
                numeric.bytes::<u32>(value.argument_expr_ids.capacity())?,
                numeric.bytes::<wire::SortExpression>(value.function_order_by.capacity())?,
            )?,
            _ => 0,
        };
        floor = numeric.add(floor, owned)?;
        cap(floor, source, work)?;
        references(definition, |_| {
            facts.expression_reference_count = numeric.add(facts.expression_reference_count, 1)?;
            admission.charge(
                &mut facts,
                admission.numeric(numeric.add(16, tree_lookup_work(count)?))?,
                work,
            )?;
            cap(
                facts.expression_reference_count,
                limits.max_expression_references,
                work,
            )
        })?;
        match kind {
            K::Lambda(value) => {
                facts.type_reference_count = numeric.add(
                    facts.type_reference_count,
                    value.parameter_value_type_ids.len(),
                )?;
                admission.charge(
                    &mut facts,
                    admission.numeric(numeric.mul(value.parameter_value_type_ids.len(), 16))?,
                    work,
                )?;
            }
            K::Cast(_) => {
                facts.type_reference_count = numeric.add(facts.type_reference_count, 1)?
            }
            _ => {}
        }
        admission.gate(&mut facts)?;
        work.step()?;
        cap(facts.type_reference_count, limits.max_type_references, work)?;
    }
    // Header and lambda carrier lookups use the original BTree owner. The
    // reference count measures source IDs; repeated delegate costs accumulate
    // before each ensuing operation on this same numerical author.
    let type_lookup_work = numeric.mul(
        facts.type_reference_count,
        tree_lookup_work(values.types().value_types().len())?,
    )?;
    admission.charge(&mut facts, type_lookup_work, work)?;
    admission.complete(&mut facts, work)?;
    Ok(facts)
}

impl<'loan, 'wire, 'control> DecodedExpressions<'loan, 'wire, 'control> {
    fn charge(
        &mut self,
        amount: usize,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        admission.charge(&mut self.facts, amount, work)
    }
    fn type_root(
        &mut self,
        id: u32,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'loan FunctionValueType, Error> {
        let lookup_work = tree_lookup_work(self.types().value_types().len())?;
        self.charge(lookup_work, admission, work)?;
        work.flush()?;
        let values = self.values;
        let root = values.types().value_type(id);
        work.step()?;
        work.flush()?;
        root.ok_or_else(|| shape("expression value type is unknown"))
    }
    fn match_types(
        &mut self,
        left: &FunctionValueType,
        right: &FunctionValueType,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let verified = compare_types(left, right, &mut self.facts, admission, work)?;
        if !verified.matches() {
            return Err(shape("expression full value type differs from its source"));
        }
        Ok(())
    }
    // The actual borrowed pair is available before the lookup's completed
    // observation. Admit its sole comparer contribution at that boundary.
    // Ordinary failures still complete the original lookup observation.
    fn match_captured_types(
        &mut self,
        pair: Result<(&FunctionValueType, &FunctionValueType), Error>,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        if admission.observed() {
            let result =
                pair.and_then(|(left, right)| self.match_types(left, right, admission, work));
            if matches!(&result, Err(Error::Control(_))) {
                return result;
            }
            work.step()?;
            work.flush()?;
            result
        } else {
            work.step()?;
            work.flush()?;
            let (left, right) = pair?;
            self.match_types(left, right, admission, work)
        }
    }
    fn match_type_root(
        &mut self,
        left: &FunctionValueType,
        id: u32,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        if !admission.observed() {
            let right = self.type_root(id, admission, work)?;
            return self.match_types(left, right, admission, work);
        }
        let lookup_work = tree_lookup_work(self.types().value_types().len())?;
        self.charge(lookup_work, admission, work)?;
        work.flush()?;
        let types = self.values.types();
        let pair = types
            .value_type(id)
            .ok_or_else(|| shape("expression value type is unknown"))
            .map(|right| (left, right));
        self.match_captured_types(pair, admission, work)
    }
    fn required_expression(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'wire wire::ExpressionDefinition, Error> {
        self.definition_observed(id, work)?
            .ok_or_else(|| shape("expression reference is unknown"))
    }
    fn validate(
        &mut self,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let definitions = self.definitions;
        for definition in definitions {
            required(definition.owner_node_id, "expression owner node is absent")?;
            let type_id = required(definition.value_type_id, "expression value type is absent")?;
            self.type_root(type_id, admission, work)?;
            references(definition, |id| {
                self.required_expression(id, work)?;
                Ok(())
            })?;
            self.validate_kind(definition, admission, work)?;
            work.step()?;
        }
        admission.complete(&mut self.facts, work)
    }
    fn validate_kind(
        &mut self,
        definition: &wire::ExpressionDefinition,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let numeric = Arithmetic(admission.observed());
        use wire::expression_definition::Kind as K;
        let values = self.values;
        let types = values.types();
        let header = self.type_root(
            required(definition.value_type_id, "expression value type is absent")?,
            admission,
            work,
        )?;
        match definition
            .kind
            .as_ref()
            .ok_or_else(|| shape("expression kind is absent"))?
        {
            K::ValueId(id) => {
                self.charge(
                    tree_lookup_work(self.values.source_count())?,
                    admission,
                    work,
                )?;
                if admission.observed() {
                    values
                        .value_captured(
                            *id,
                            &mut |value, work| self.match_types(header, &value.ty, admission, work),
                            work,
                        )?
                        .ok_or_else(|| shape("expression value reference is unknown"))?;
                } else {
                    let value = values
                        .value_observed(*id, work)?
                        .ok_or_else(|| shape("expression value reference is unknown"))?;
                    self.match_types(header, &value.ty, admission, work)?;
                }
            }
            K::Literal(reference) => {
                let reference = ConstantReference {
                    pool: ConstantPoolId::new(required(
                        reference.pool_id,
                        "constant pool ID is absent",
                    )?),
                    ordinal: reference.row_ordinal,
                };
                self.charge(
                    numeric.mul(2, tree_lookup_work(self.pools.entries().len())?)?,
                    admission,
                    work,
                )?;
                work.flush()?;
                let pool = self.pools.entries().get(&reference.pool);
                let captured = if admission.observed() {
                    let result = pool
                        .map(|pool| {
                            compare_types(
                                header,
                                pool.value_type(),
                                &mut self.facts,
                                admission,
                                work,
                            )
                        })
                        .transpose();
                    if matches!(&result, Err(Error::Control(_))) {
                        return result.map(|_| ());
                    }
                    Some(result)
                } else {
                    None
                };
                work.step()?;
                work.flush()?;
                let pool = pool.ok_or_else(|| shape("expression constant pool is unknown"))?;
                let retained =
                    usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
                        .map_err(|_| shape("constant retained extent is unrepresentable"))?;
                cap(retained, self.source_retained_bytes, work)?;
                let verified = if let Some(captured) = captured {
                    captured?.ok_or_else(|| shape("expression constant pool is unknown"))?
                } else {
                    compare_types(header, pool.value_type(), &mut self.facts, admission, work)?
                };
                if !verified.matches() {
                    return Err(
                        novarocks_physical_plan::ConstantReferenceError::SourceTypeMismatch(
                            reference,
                        )
                        .into(),
                    );
                }
                work.flush()?;
                let selected = self
                    .pools
                    .resolve_source_observed(reference, work)
                    .map_err(Error::from);
                if matches!(&selected, Err(Error::Control(_))) {
                    return selected.map(|_| ());
                }
                work.flush()?;
                let selected = selected?;
                // Drop only the temporary shared selected handle, never its
                // original Field/type/backing. No payload is read or rebuilt.
                drop(selected);
            }
            K::LambdaParameter(value) => {
                let owner = self.required_expression(
                    required(value.lambda_expr_id, "lambda parameter owner is absent")?,
                    work,
                )?;
                let Some(K::Lambda(lambda)) = owner.kind.as_ref() else {
                    return Err(shape("lambda parameter owner is not a lambda"));
                };
                let ordinal = usize::try_from(value.ordinal)
                    .map_err(|_| shape("lambda ordinal is unrepresentable"))?;
                let parameter = *lambda
                    .parameter_value_type_ids
                    .get(ordinal)
                    .ok_or_else(|| shape("lambda parameter ordinal is out of bounds"))?;
                self.match_type_root(header, parameter, admission, work)?;
            }
            K::Unary(value) => {
                super::receiving_grammar::decode_unary(value.op)?;
            }
            K::Binary(value) => {
                let op = super::receiving_grammar::decode_binary(value.op)?;
                let arithmetic = op.arithmetic_operator().is_some();
                decode_decimal_policy(value.decimal_overflow_policy)?;
                let same_presence = arithmetic == value.allow_throw_exception.is_some();
                work.step()?;
                if !same_presence {
                    return Err(shape(
                        "binary ALLOW reference presence differs from its operator",
                    ));
                }
                if let Some(reference) = &value.allow_throw_exception {
                    self.allow_throw(reference, admission, work)?;
                }
            }
            K::Conjunction(_) | K::Disjunction(_) => {}
            K::FunctionCall(value) => {
                self.call(
                    definition,
                    required(value.function_binding_id, "function binding ID is absent")?,
                    &value.argument_expr_ids,
                    wire::FunctionKind::Scalar,
                    admission,
                    work,
                )?;
            }
            K::Lambda(value) => {
                for id in &value.parameter_value_type_ids {
                    self.type_root(*id, admission, work)?;
                    work.step()?;
                }
                let body = self.required_expression(
                    required(value.body_expr_id, "lambda body is absent")?,
                    work,
                )?;
                self.match_type_root(
                    header,
                    required(body.value_type_id, "lambda body type is absent")?,
                    admission,
                    work,
                )?;
            }
            K::Cast(value) => {
                decode_decimal_policy(value.decimal_overflow_policy)?;
                self.allow_throw(
                    value
                        .allow_throw_exception
                        .as_ref()
                        .ok_or_else(|| shape("cast ALLOW reference is absent"))?,
                    admission,
                    work,
                )?;
                let carrier_id =
                    required(value.target_carrier_type_id, "cast carrier ID is absent")?;
                self.charge(tree_lookup_work(types.carriers.len())?, admission, work)?;
                work.flush()?;
                let carrier = types.carrier(carrier_id);
                let captured = if admission.observed() && carrier.is_some() {
                    let result = preflight_types(header, header, &mut self.facts, admission, work);
                    if matches!(&result, Err(Error::Control(_))) {
                        return result.map(|_| ());
                    }
                    Some(result)
                } else {
                    None
                };
                work.step()?;
                work.flush()?;
                let carrier = carrier.ok_or_else(|| shape("cast carrier type is unknown"))?;
                // Raw carriers have no independent root flags. The same
                // complete-type numerical author bounds this datatype walk.
                let _proof = if let Some(captured) = captured {
                    captured?
                } else {
                    preflight_types(header, header, &mut self.facts, admission, work)?
                };
                work.flush()?;
                let same = arrow_data_types_exact_borrowed_with_profile_observed(
                    header.data_type(),
                    carrier,
                    novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
                    || work.step().map_err(TypeCodecError::from),
                )
                .map_err(Error::from)?;
                work.flush()?;
                if !same {
                    return Err(shape("cast carrier differs from its complete result type"));
                }
            }
            K::IsNull(_)
            | K::InList(_)
            | K::Between(_)
            | K::Like(_)
            | K::CaseExpression(_)
            | K::IsTruthValue(_) => {}
            K::WindowCall(value) => self.window(definition, value, admission, work)?,
        }
        work.step()?;
        Ok(())
    }
    fn allow_throw(
        &mut self,
        reference: &semantics::SemanticParameterRef,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.charge(
            tree_lookup_work(self.parameters.entries().len())?,
            admission,
            work,
        )?;
        let reference = decode_reference(reference, work)?;
        let valid = reference.expected_key == SemanticParameterKey::AllowThrowException;
        work.step()?;
        if !valid {
            return Err(shape(
                "intrinsic ALLOW reference has the wrong expected key",
            ));
        }
        work.flush()?;
        let resolved = self
            .parameters
            .require(reference)
            .map_err(SemanticsCodecError::from);
        work.step()?;
        work.flush()?;
        resolved?;
        Ok(())
    }
    fn function_header(
        &mut self,
        id: u32,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'wire wire::FunctionBindingDefinition, Error> {
        self.charge(
            tree_lookup_work(self.functions.as_wire().len())?,
            admission,
            work,
        )?;
        self.functions
            .definition_observed(id, work)?
            .ok_or_else(|| shape("expression function binding is unknown"))
    }
    fn call_result(
        &mut self,
        definition: &wire::ExpressionDefinition,
        function: &wire::FunctionBindingDefinition,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let numeric = Arithmetic(admission.observed());
        let Some(wire::function_binding_definition::Result::ScalarValueTypeId(result_id)) =
            function.result.as_ref()
        else {
            return Err(shape("expression function does not have a scalar result"));
        };
        let types = self.values.types();
        self.charge(
            numeric.mul(2, tree_lookup_work(types.value_types().len())?)?,
            admission,
            work,
        )?;
        work.flush()?;
        let result = types.value_type(*result_id);
        let header = types.value_type(required(
            definition.value_type_id,
            "call result type is absent",
        )?);
        self.match_captured_types(
            (|| {
                Ok((
                    header.ok_or_else(|| shape("call result header type is unknown"))?,
                    result.ok_or_else(|| shape("function result type is unknown"))?,
                ))
            })(),
            admission,
            work,
        )
    }
    fn arguments(
        &mut self,
        expected: &[wire::FunctionArgumentType],
        arguments: &[u32],
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let numeric = Arithmetic(admission.observed());
        let same_count = expected.len() == arguments.len();
        work.step()?;
        if !same_count {
            return Err(shape(
                "call arguments differ from its frozen signature arity",
            ));
        }
        self.charge(
            numeric.mul(
                expected.len(),
                numeric.add(16, tree_lookup_work(self.definitions.len())?)?,
            )?,
            admission,
            work,
        )?;
        let types = self.values.types();
        for (expected, id) in expected.iter().zip(arguments) {
            let actual = self.required_expression(*id, work)?;
            match expected
                .kind
                .as_ref()
                .ok_or_else(|| shape("function argument shape is absent"))?
            {
                wire::function_argument_type::Kind::ValueTypeId(_) => {
                    // Exact Value-domain compatibility includes lawful nested
                    // nullability widening. Its sole fits author requires the
                    // final Fragment; this component proves only the shape.
                    if matches!(
                        actual.kind,
                        Some(wire::expression_definition::Kind::Lambda(_))
                    ) {
                        return Err(shape("value argument references a lambda"));
                    }
                }
                wire::function_argument_type::Kind::Lambda(expected) => {
                    let Some(wire::expression_definition::Kind::Lambda(actual_lambda)) =
                        actual.kind.as_ref()
                    else {
                        return Err(shape("lambda argument references a value expression"));
                    };
                    let same_count = expected.parameter_value_type_ids.len()
                        == actual_lambda.parameter_value_type_ids.len();
                    work.step()?;
                    if !same_count {
                        return Err(shape(
                            "lambda argument parameter arity differs from its signature",
                        ));
                    }
                    self.charge(
                        numeric.mul(
                            numeric.add(expected.parameter_value_type_ids.len(), 1)?,
                            numeric.mul(2, tree_lookup_work(types.value_types().len())?)?,
                        )?,
                        admission,
                        work,
                    )?;
                    for (left, right) in actual_lambda
                        .parameter_value_type_ids
                        .iter()
                        .zip(&expected.parameter_value_type_ids)
                    {
                        work.flush()?;
                        let left = types.value_type(*left);
                        let right = types.value_type(*right);
                        self.match_captured_types(
                            (|| {
                                Ok((
                                    left.ok_or_else(|| {
                                        shape("lambda actual parameter type is unknown")
                                    })?,
                                    right.ok_or_else(|| {
                                        shape("lambda expected parameter type is unknown")
                                    })?,
                                ))
                            })(),
                            admission,
                            work,
                        )?;
                    }
                    work.flush()?;
                    let left = types.value_type(required(
                        actual.value_type_id,
                        "lambda argument result header is absent",
                    )?);
                    let right = types.value_type(required(
                        expected.result_value_type_id,
                        "lambda signature result is absent",
                    )?);
                    self.match_captured_types(
                        (|| {
                            Ok((
                                left.ok_or_else(|| shape("lambda actual result type is unknown"))?,
                                right.ok_or_else(|| {
                                    shape("lambda expected result type is unknown")
                                })?,
                            ))
                        })(),
                        admission,
                        work,
                    )?;
                }
            }
            work.step()?;
        }
        Ok(())
    }
    fn call(
        &mut self,
        definition: &wire::ExpressionDefinition,
        id: u32,
        arguments: &[u32],
        kind: wire::FunctionKind,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let function = self.function_header(id, admission, work)?;
        let valid_kind = function.kind == kind as i32;
        work.step()?;
        if !valid_kind {
            return Err(shape("expression function binding has the wrong kind"));
        }
        self.call_result(definition, function, admission, work)?;
        self.arguments(&function.arguments, arguments, admission, work)
    }
    fn signatures(
        &mut self,
        left: &wire::FunctionBindingDefinition,
        right: &wire::FunctionBindingDefinition,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let numeric = Arithmetic(admission.observed());
        // Header identities have already passed the sole <=1024-byte grammar.
        // Compare the actual strings at an honest opaque boundary; IDs in the
        // sparse namespace may alias one exact selected signature.
        self.charge(
            numeric.add(
                numeric.add(left.function_id.len(), right.function_id.len())?,
                numeric.add(left.overload_id.len(), right.overload_id.len())?,
            )?,
            admission,
            work,
        )?;
        work.flush()?;
        let same = left.function_id == right.function_id
            && left.overload_id == right.overload_id
            && left.kind == right.kind
            && left.arguments.len() == right.arguments.len();
        work.step()?;
        work.flush()?;
        if !same {
            return Err(shape("window aggregate and function signatures differ"));
        }
        let types = self.values.types();
        self.charge(
            numeric.mul(
                numeric.add(left.arguments.len(), 1)?,
                numeric.add(
                    16,
                    numeric.mul(2, tree_lookup_work(types.value_types().len())?)?,
                )?,
            )?,
            admission,
            work,
        )?;
        for (left, right) in left.arguments.iter().zip(&right.arguments) {
            match (left.kind.as_ref(), right.kind.as_ref()) {
                (
                    Some(wire::function_argument_type::Kind::ValueTypeId(left)),
                    Some(wire::function_argument_type::Kind::ValueTypeId(right)),
                ) => self.signature_types(*left, *right, admission, work)?,
                (
                    Some(wire::function_argument_type::Kind::Lambda(left)),
                    Some(wire::function_argument_type::Kind::Lambda(right)),
                ) => {
                    let same =
                        left.parameter_value_type_ids.len() == right.parameter_value_type_ids.len();
                    work.step()?;
                    if !same {
                        return Err(shape("aggregate lambda signature arity differs"));
                    }
                    self.charge(
                        numeric.mul(
                            left.parameter_value_type_ids.len(),
                            numeric.mul(2, tree_lookup_work(types.value_types().len())?)?,
                        )?,
                        admission,
                        work,
                    )?;
                    for (left, right) in left
                        .parameter_value_type_ids
                        .iter()
                        .zip(&right.parameter_value_type_ids)
                    {
                        self.signature_types(*left, *right, admission, work)?;
                    }
                    self.signature_types(
                        required(
                            left.result_value_type_id,
                            "aggregate lambda result is absent",
                        )?,
                        required(right.result_value_type_id, "window lambda result is absent")?,
                        admission,
                        work,
                    )?;
                }
                _ => return Err(shape("window aggregate argument shapes differ")),
            }
            work.step()?;
        }
        match (left.result.as_ref(), right.result.as_ref()) {
            (
                Some(wire::function_binding_definition::Result::ScalarValueTypeId(left)),
                Some(wire::function_binding_definition::Result::ScalarValueTypeId(right)),
            ) => self.signature_types(*left, *right, admission, work)?,
            _ => return Err(shape("window aggregate result is not scalar")),
        }
        Ok(())
    }
    fn signature_types(
        &mut self,
        left: u32,
        right: u32,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let types = self.values.types();
        work.flush()?;
        let left = types.value_type(left);
        let right = types.value_type(right);
        self.match_captured_types(
            (|| {
                Ok((
                    left.ok_or_else(|| shape("signature type is unknown"))?,
                    right.ok_or_else(|| shape("signature type is unknown"))?,
                ))
            })(),
            admission,
            work,
        )
    }
    fn window(
        &mut self,
        definition: &wire::ExpressionDefinition,
        value: &wire::WindowCall,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let numeric = Arithmetic(admission.observed());
        let id = required(
            value.function_binding_id,
            "window function binding ID is absent",
        )?;
        let function = self.function_header(id, admission, work)?;
        self.call_result(definition, function, admission, work)?;
        match wire::FunctionKind::try_from(function.kind) {
            Ok(wire::FunctionKind::Window) => {
                let ordinary = value.aggregate_binding_id.is_none()
                    && !value.distinct
                    && value.function_order_by.is_empty();
                work.step()?;
                if !ordinary {
                    return Err(shape("window function carries aggregate-only fields"));
                }
                self.arguments(
                    &function.arguments,
                    &value.argument_expr_ids,
                    admission,
                    work,
                )?;
            }
            Ok(wire::FunctionKind::Aggregate) => {
                self.charge(
                    tree_lookup_work(self.aggregates.as_wire().len())?,
                    admission,
                    work,
                )?;
                let aggregate = self
                    .aggregates
                    .definition_observed(
                        required(
                            value.aggregate_binding_id,
                            "aggregate window binding is absent",
                        )?,
                        work,
                    )?
                    .ok_or_else(|| shape("aggregate window binding is unknown"))?;
                let aggregate_function = self.function_header(
                    required(
                        aggregate.function_binding_id,
                        "aggregate function ID is absent",
                    )?,
                    admission,
                    work,
                )?;
                self.signatures(function, aggregate_function, admission, work)?;
                // Phase/channel and logical arguments plus ORDER BY inputs
                // are validated by the sole Fragment aggregate author. This
                // borrowed component preserves them without a scalar zip.
            }
            _ => return Err(shape("window binding is neither Window nor Aggregate")),
        }
        self.charge(
            numeric.mul(value.function_order_by.len(), 8)?,
            admission,
            work,
        )?;
        for sort in &value.function_order_by {
            let closed = crate::physical_properties_v2::decode_direction(sort.direction).is_ok()
                && crate::physical_properties_v2::decode_nulls(sort.null_ordering).is_ok();
            work.step()?;
            if !closed {
                return Err(shape(
                    "window sort direction or null order is unknown or unspecified",
                ));
            }
        }
        if let Some(frame) = &value.frame {
            let closed = super::receiving_grammar::decode_window_units(frame.units).is_ok()
                && super::receiving_grammar::decode_window_exclusion(frame.exclusion).is_ok();
            work.step()?;
            if !closed {
                return Err(shape(
                    "window frame unit or exclusion is unknown or unspecified",
                ));
            }
            // Ordered bounds and their actual expression references were
            // already visited. Absence of the optional whole frame is legal.
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
