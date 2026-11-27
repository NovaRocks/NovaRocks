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

//! Restricted construction edits. This does not author SQL call sources,
//! grants, occurrence effects, a dependency closure or a complete plan.
//! The SQL visitor must own its complete unpublished graph until its actual
//! source journal and dependency closure have both been checked.

use super::FragmentBuilder;
use crate::{
    AggregateBinding, BoundFunction, BoundTableFunction, NodeId, NodeKind, PhysicalCallSite,
    PhysicalNode, TopNReduction, ValueDef, ValueId, ValueType,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ValueTypeError,
    arrow_data_types_exact_borrowed_observed,
};

#[derive(Debug)]
pub enum NullabilityRebindError {
    Control(CompileControlError),
    Type(ValueTypeError),
    Association(&'static str),
}
impl From<CompileControlError> for NullabilityRebindError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ValueTypeError> for NullabilityRebindError {
    fn from(cause: ValueTypeError) -> Self {
        Self::Type(cause)
    }
}

/// A borrow of one construction owner. There is no raw mutable node, arena,
/// value-map accessor or completed-plan conversion on this editor.
pub struct UnpublishedNullabilityEditor<'a> {
    builder: &'a mut FragmentBuilder,
}
impl FragmentBuilder {
    pub fn nullability_editor(&mut self) -> UnpublishedNullabilityEditor<'_> {
        UnpublishedNullabilityEditor { builder: self }
    }
    pub fn construction_values(&self) -> impl ExactSizeIterator<Item = (&ValueId, &ValueDef)> {
        self.values.iter()
    }
    /// Read the original node-private definitions during unpublished source
    /// and nullability journaling. Absence never creates a replacement draft.
    pub fn construction_unit(&self, node: NodeId) -> Option<&crate::BoundExprSetBuilder> {
        self.unit_builders.get(&node)
    }
    pub fn construction_node(&self, id: NodeId) -> Option<&PhysicalNode> {
        self.nodes.get(&id)
    }
    pub fn construction_nodes(&self) -> impl ExactSizeIterator<Item = (&NodeId, &PhysicalNode)> {
        self.nodes.iter()
    }
    pub fn visit_construction_relational_calls_observed<'a, E: From<crate::FrozenCallError>>(
        &'a self,
        work: &mut CompileCheckpoints<'_>,
        visit: impl FnMut(
            crate::PhysicalCallSite,
            crate::PhysicalCallBinding<'a>,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), E>,
    ) -> Result<(), E> {
        crate::frozen_calls::visit_relational_node_calls_observed(self.nodes.values(), work, visit)
    }
}
impl UnpublishedNullabilityEditor<'_> {
    pub fn writer_relation_field_type(
        &mut self,
        node: NodeId,
        input_schema: bool,
        ordinal: usize,
        expected: &ValueType,
        revised: ValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), NullabilityRebindError> {
        let n = self
            .builder
            .nodes
            .get(&node)
            .ok_or(NullabilityRebindError::Association(
                "writer schema node is absent",
            ))?;
        let schema = match (&n.kind, input_schema) {
            (NodeKind::TableWriter { target }, false) => &target.output_schema,
            (NodeKind::TableFinish(spec), false) => &spec.output_schema,
            (NodeKind::TableFinish(spec), true) => &spec.input_schema,
            _ => {
                return Err(NullabilityRebindError::Association(
                    "writer schema edit differs from its actual lifecycle",
                ));
            }
        };
        let field = schema
            .fields
            .get(ordinal)
            .ok_or(NullabilityRebindError::Association(
                "writer schema ordinal is absent",
            ))?;
        exact_type(&field.ty, expected, work)?;
        root_admission(expected, &revised, work)?;
        let value =
            self.builder
                .values
                .get(&field.value)
                .ok_or(NullabilityRebindError::Association(
                    "writer schema value is absent",
                ))?;
        exact_type(&value.ty, &revised, work)?;
        let n = self.builder.nodes.get_mut(&node).expect("checked above");
        let schema = match (&mut n.kind, input_schema) {
            (NodeKind::TableWriter { target }, false) => &mut target.output_schema,
            (NodeKind::TableFinish(spec), false) => &mut spec.output_schema,
            (NodeKind::TableFinish(spec), true) => &mut spec.input_schema,
            _ => unreachable!("checked above"),
        };
        schema.fields[ordinal].ty = revised;
        Ok(())
    }
    pub fn value(&self, id: ValueId) -> Option<&ValueDef> {
        self.builder.values.get(&id)
    }
    pub fn node(&self, id: NodeId) -> Option<&PhysicalNode> {
        self.builder.nodes.get(&id)
    }

    /// Expected is the actual earlier type read by this construction owner,
    /// rather than a declaration inferred from selected rows or an AST.
    pub fn value_type(
        &mut self,
        id: ValueId,
        expected: &ValueType,
        revised: ValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), NullabilityRebindError> {
        let actual = self
            .builder
            .values
            .get(&id)
            .ok_or(NullabilityRebindError::Association(
                "value edit names an absent definition",
            ))?;
        exact_type(&actual.ty, expected, work)?;
        root_admission(expected, &revised, work)?;
        // All checks complete before replacing this field. Failure of a later
        // dependency causes the enclosing consumed SQL visitor to be dropped.
        self.builder.values.get_mut(&id).expect("checked above").ty = revised;
        Ok(())
    }

    /// Window ordering, frame, distinct/NULL treatment and all argument
    /// occurrences remain in the same node. Only the two actual signatures
    /// are moved after their earlier complete fields have been checked.
    pub fn window_signature(
        &mut self,
        site: PhysicalCallSite,
        expected: &BoundFunction,
        expected_aggregate: Option<&AggregateBinding>,
        revised: BoundFunction,
        revised_aggregate: Option<Box<AggregateBinding>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), NullabilityRebindError> {
        let PhysicalCallSite::Window { node, call } = site else {
            return Err(NullabilityRebindError::Association(
                "window edit names a different lifecycle",
            ));
        };
        let Some(PhysicalNode {
            kind: NodeKind::Window(spec),
            ..
        }) = self.builder.nodes.get(&node)
        else {
            return Err(NullabilityRebindError::Association("window node is absent"));
        };
        let actual = spec
            .calls
            .get(call as usize)
            .ok_or(NullabilityRebindError::Association("window call is absent"))?;
        let function = &actual.binding;
        let aggregate_binding = &actual.aggregate_binding;
        if !binding_equal(function, expected, work)? {
            return Err(NullabilityRebindError::Association(
                "window earlier binding differs from its actual definition",
            ));
        }
        signature_identity(expected, &revised, work)?;
        root_admission(&expected.result_type, &revised.result_type, work)?;
        match (
            aggregate_binding.as_deref(),
            expected_aggregate,
            revised_aggregate.as_deref(),
        ) {
            (None, None, None) => {}
            (Some(actual), Some(expected), Some(revised_state)) => {
                aggregate_equal(actual, expected, work)?;
                aggregate_admission(expected, revised_state, work)?;
                exact_type(
                    &revised_state.function.result_type,
                    &revised.result_type,
                    work,
                )?;
            }
            _ => {
                return Err(NullabilityRebindError::Association(
                    "window edit changes aggregate lifecycle presence",
                ));
            }
        }
        let NodeKind::Window(spec) = &mut self
            .builder
            .nodes
            .get_mut(&node)
            .expect("checked above")
            .kind
        else {
            unreachable!("checked above")
        };
        let actual = &mut spec.calls[call as usize];
        actual.binding = revised;
        actual.aggregate_binding = revised_aggregate;
        Ok(())
    }

    /// Table relation fields are not outer-input pass-through occurrences.
    /// Node outputs, argument definitions and left-outer policy cannot change.
    pub fn table_signature(
        &mut self,
        node: NodeId,
        expected: &BoundTableFunction,
        revised: BoundTableFunction,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), NullabilityRebindError> {
        let actual = self
            .builder
            .nodes
            .get(&node)
            .ok_or(NullabilityRebindError::Association(
                "table edit names an absent node",
            ))?;
        let NodeKind::TableFunction { function, .. } = &actual.kind else {
            return Err(NullabilityRebindError::Association(
                "table edit names a different lifecycle",
            ));
        };
        table_equal(function, expected, work)?;
        table_identity(expected, &revised, work)?;
        for (old, new) in expected
            .argument_types
            .iter()
            .zip(revised.argument_types.iter())
        {
            argument_type(old, new, false, work)?;
        }
        for (old, new) in expected
            .result_types
            .iter()
            .zip(revised.result_types.iter())
        {
            root_admission(old, new, work)?;
        }
        let NodeKind::TableFunction { function, .. } = &mut self
            .builder
            .nodes
            .get_mut(&node)
            .expect("checked above")
            .kind
        else {
            unreachable!("checked above")
        };
        *function = revised;
        Ok(())
    }

    /// ONE relational site vocabulary identifies ordinary aggregate,
    /// grouped TopN state and both Writer phases. The SQL state-source loan
    /// still owns producer identity and compatibility; this editor cannot
    /// create that loan or alter transport links, sequences or writer targets.
    pub fn aggregate_signature(
        &mut self,
        site: PhysicalCallSite,
        expected: &AggregateBinding,
        revised: AggregateBinding,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), NullabilityRebindError> {
        let (node, call) = aggregate_site(site)?;
        let actual = self
            .builder
            .nodes
            .get(&node)
            .ok_or(NullabilityRebindError::Association(
                "aggregate edit names an absent node",
            ))?;
        let binding = aggregate_binding(&actual.kind, site, call)?;
        aggregate_equal(binding, expected, work)?;
        aggregate_admission(expected, &revised, work)?;
        let actual = self.builder.nodes.get_mut(&node).expect("checked above");
        *aggregate_binding_mut(&mut actual.kind, site, call)? = revised;
        Ok(())
    }
}

fn signature_identity(
    original: &BoundFunction,
    revised: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), NullabilityRebindError> {
    if !identity_equal(original, revised, work)? {
        return Err(NullabilityRebindError::Association(
            "rebind changes call identity or original provenance",
        ));
    }
    for (original, revised) in original
        .argument_types
        .iter()
        .zip(revised.argument_types.iter())
    {
        argument_type(original, revised, false, work)?;
    }

    // This writer does not authorize argument shapes. The sole SQL canonical
    // author and final request/source validators must certify the actual new
    // argument contracts; signature equality alone never supplies that proof.
    Ok(())
}
fn identity_equal(
    a: &BoundFunction,
    b: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, NullabilityRebindError> {
    work.step()?;
    if a.kind != b.kind
        || a.argument_types.len() != b.argument_types.len()
        || !text_equal(a.function_id.as_str(), b.function_id.as_str(), work)?
        || !text_equal(a.overload.as_str(), b.overload.as_str(), work)?
    {
        return Ok(false);
    }
    match (&a.legacy_metadata, &b.legacy_metadata) {
        (None, None) => {}
        (Some(a), Some(b)) => {
            work.step()?;
            if a.volatility != b.volatility
                || a.argument_evaluation != b.argument_evaluation
                || a.failure_behavior != b.failure_behavior
                || a.intrinsic_row_error != b.intrinsic_row_error
                || a.semantic_parameters.len() != b.semantic_parameters.len()
            {
                return Ok(false);
            }
            for (a, b) in a
                .semantic_parameters
                .iter()
                .zip(b.semantic_parameters.iter())
            {
                work.step()?;
                if a != b {
                    return Ok(false);
                }
            }
        }
        _ => return Ok(false),
    }
    Ok(true)
}
fn binding_equal(
    a: &BoundFunction,
    b: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, NullabilityRebindError> {
    if !identity_equal(a, b, work)? {
        return Ok(false);
    }
    exact_type(&a.result_type, &b.result_type, work)?;
    for (a, b) in a.argument_types.iter().zip(b.argument_types.iter()) {
        argument_type(a, b, true, work)?;
    }
    Ok(true)
}
fn argument_type(
    a: &crate::FunctionArgumentType,
    b: &crate::FunctionArgumentType,
    exact: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), NullabilityRebindError> {
    use crate::FunctionArgumentType;
    work.step()?;
    let compare = if exact { exact_type } else { root_admission };
    match (a, b) {
        (FunctionArgumentType::Value(a), FunctionArgumentType::Value(b)) => compare(a, b, work),
        (
            FunctionArgumentType::Lambda {
                parameter_types: a,
                result_type: ar,
            },
            FunctionArgumentType::Lambda {
                parameter_types: b,
                result_type: br,
            },
        ) => {
            work.step()?;
            if a.len() != b.len() {
                return Err(NullabilityRebindError::Association(
                    "rebind changes lambda arity",
                ));
            }
            for (a, b) in a.iter().zip(b.iter()) {
                exact_type(a, b, work)?;
            }
            compare(ar, br, work)
        }
        _ => Err(NullabilityRebindError::Association(
            "rebind changes value/lambda channel role",
        )),
    }
}
fn text_equal(
    a: &str,
    b: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, NullabilityRebindError> {
    work.step()?;
    if a.len() != b.len() {
        return Ok(false);
    }
    for (a, b) in a.bytes().zip(b.bytes()) {
        work.step()?;
        if a != b {
            return Ok(false);
        }
    }
    Ok(true)
}
fn exact_type(
    a: &ValueType,
    b: &ValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), NullabilityRebindError> {
    work.step()?;
    if !a.exactly_equals_observed(b, || work.step().map_err(NullabilityRebindError::from))? {
        return Err(NullabilityRebindError::Association(
            "earlier full type differs from its actual construction definition",
        ));
    }
    Ok(())
}
fn root_admission(
    a: &ValueType,
    b: &ValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), NullabilityRebindError> {
    work.step()?;
    if (a.nullable() && !b.nullable())
        || !a.same_value_domain_observed(b, || work.step().map_err(NullabilityRebindError::from))?
        || !arrow_data_types_exact_borrowed_observed::<NullabilityRebindError>(
            a.data_type(),
            b.data_type(),
            || work.step().map_err(NullabilityRebindError::from),
        )?
    {
        return Err(NullabilityRebindError::Association(
            "rebind changes facts beyond root nullable admission",
        ));
    }
    Ok(())
}

fn aggregate_site(site: PhysicalCallSite) -> Result<(NodeId, usize), NullabilityRebindError> {
    match site {
        PhysicalCallSite::Aggregate { node, call }
        | PhysicalCallSite::TopNState { node, call }
        | PhysicalCallSite::WriterPartial { node, call }
        | PhysicalCallSite::WriterFinal { node, call } => Ok((
            node,
            usize::try_from(call).map_err(|_| {
                NullabilityRebindError::Association("aggregate ordinal is not representable")
            })?,
        )),
        PhysicalCallSite::Window { .. } | PhysicalCallSite::Table { .. } => Err(
            NullabilityRebindError::Association("aggregate edit names a non-aggregate site"),
        ),
    }
}
fn aggregate_binding(
    kind: &NodeKind,
    site: PhysicalCallSite,
    call: usize,
) -> Result<&AggregateBinding, NullabilityRebindError> {
    let binding = match (kind, site) {
        (NodeKind::Aggregate { calls, .. }, PhysicalCallSite::Aggregate { .. }) => {
            calls.get(call).map(|c| &c.binding)
        }
        (
            NodeKind::TopN {
                reduction: TopNReduction::GroupedStates { calls, .. },
                ..
            },
            PhysicalCallSite::TopNState { .. },
        ) => calls.get(call).map(|c| &c.binding),
        (NodeKind::TableWriter { target }, PhysicalCallSite::WriterPartial { .. }) => {
            target.partial_aggregates.get(call).map(|c| &c.binding)
        }
        (NodeKind::TableFinish(finish), PhysicalCallSite::WriterFinal { .. }) => {
            finish.final_aggregates.get(call).map(|c| &c.binding)
        }
        _ => None,
    };
    binding.ok_or(NullabilityRebindError::Association(
        "aggregate edit differs from its actual lifecycle or ordinal",
    ))
}
fn aggregate_binding_mut(
    kind: &mut NodeKind,
    site: PhysicalCallSite,
    call: usize,
) -> Result<&mut AggregateBinding, NullabilityRebindError> {
    let binding = match (kind, site) {
        (NodeKind::Aggregate { calls, .. }, PhysicalCallSite::Aggregate { .. }) => {
            calls.get_mut(call).map(|c| &mut c.binding)
        }
        (
            NodeKind::TopN {
                reduction: TopNReduction::GroupedStates { calls, .. },
                ..
            },
            PhysicalCallSite::TopNState { .. },
        ) => calls.get_mut(call).map(|c| &mut c.binding),
        (NodeKind::TableWriter { target }, PhysicalCallSite::WriterPartial { .. }) => target
            .partial_aggregates
            .get_mut(call)
            .map(|c| &mut c.binding),
        (NodeKind::TableFinish(finish), PhysicalCallSite::WriterFinal { .. }) => finish
            .final_aggregates
            .get_mut(call)
            .map(|c| &mut c.binding),
        _ => None,
    };
    binding.ok_or(NullabilityRebindError::Association(
        "aggregate edit differs from its actual lifecycle or ordinal",
    ))
}
fn aggregate_equal(
    a: &AggregateBinding,
    b: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), NullabilityRebindError> {
    // State-interpretation comparison is the original finite owner operation.
    // Opaque boundary observation is not a cooperative traversal or grant.
    work.flush()?;
    let same = a.phase == b.phase
        && a.logical_argument_count == b.logical_argument_count
        && match (&a.merge_state, &b.merge_state) {
            (Some(a), Some(b)) => {
                a.state_format == b.state_format
                    && a.state_argument_contract == b.state_argument_contract
                    && a.state_interpretation == b.state_interpretation
            }
            (None, None) => true,
            _ => false,
        };
    work.step()?;
    work.flush()?;
    if !same || !binding_equal(&a.function, &b.function, work)? {
        return Err(NullabilityRebindError::Association(
            "aggregate earlier binding differs from its actual definition",
        ));
    }
    if let (Some(a), Some(b)) = (a.intermediate_type(), b.intermediate_type()) {
        exact_type(a, b, work)?;
    }
    Ok(())
}
fn aggregate_admission(
    a: &AggregateBinding,
    b: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), NullabilityRebindError> {
    work.flush()?;
    let same = a.phase == b.phase
        && a.logical_argument_count == b.logical_argument_count
        && match (&a.merge_state, &b.merge_state) {
            (Some(a), Some(b)) => {
                a.state_format == b.state_format
                    && a.state_argument_contract == b.state_argument_contract
                    && a.state_interpretation == b.state_interpretation
            }
            (None, None) => true,
            _ => false,
        };
    work.step()?;
    work.flush()?;
    if !same {
        return Err(NullabilityRebindError::Association(
            "aggregate rebind changes phase, logical channels or original state interpretation",
        ));
    }
    signature_identity(&a.function, &b.function, work)?;
    root_admission(&a.function.result_type, &b.function.result_type, work)?;
    if let (Some(a), Some(b)) = (a.intermediate_type(), b.intermediate_type()) {
        root_admission(a, b, work)?;
    }
    Ok(())
}
fn table_identity(
    a: &BoundTableFunction,
    b: &BoundTableFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), NullabilityRebindError> {
    work.flush()?;
    let metadata = a.legacy_metadata == b.legacy_metadata;
    work.step()?;
    work.flush()?;
    if a.argument_types.len() != b.argument_types.len()
        || a.result_types.len() != b.result_types.len()
        || !text_equal(a.function_id.as_str(), b.function_id.as_str(), work)?
        || !text_equal(a.overload.as_str(), b.overload.as_str(), work)?
        || !metadata
    {
        return Err(NullabilityRebindError::Association(
            "table rebind changes identity, arity or original provenance",
        ));
    }
    Ok(())
}
fn table_equal(
    a: &BoundTableFunction,
    b: &BoundTableFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), NullabilityRebindError> {
    table_identity(a, b, work)?;
    for (left, right) in a.argument_types.iter().zip(b.argument_types.iter()) {
        argument_type(left, right, true, work)?;
    }
    for (left, right) in a.result_types.iter().zip(b.result_types.iter()) {
        exact_type(left, right, work)?;
    }
    Ok(())
}
