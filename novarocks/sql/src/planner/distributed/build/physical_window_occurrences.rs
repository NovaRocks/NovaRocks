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

//! Fresh installed Window owner facts with actual relation and parameter scopes.
use super::{
    contract_lowering::NodeExpressionEffects,
    expression_occurrences::{AuthoredPhysicalOccurrences, ExpressionOccurrenceError},
    physical_relational_effects::{
        PhysicalRelationalEffectsError, relational_context_observed,
        relational_root_effects_observed,
    },
    physical_window_requests::AuthoredPhysicalWindowRequest,
};
use crate::compiler::SqlFunctionCatalog;
use novarocks_functions::{
    CallEffectInput, PureCallSpecialization, PureKernelAbi, ScopedRelationalEffects,
};
use novarocks_physical_plan::{
    FrozenPhysicalCall, NodeKind, PhysicalCallSite, PhysicalNode, WindowCallSpec,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, DecimalOverflowPolicy,
    EffectContractError, ExpressionEffects, ExpressionUseId, FunctionKind, SemanticParameterRef,
    SemanticParameters,
};
use std::sync::Arc;
#[cfg(test)]
#[path = "physical_window_occurrences_tests.rs"]
mod tests;
#[derive(Debug)]
pub(crate) enum PhysicalWindowOccurrenceError {
    Control(CompileControlError),
    Function(ExpressionOccurrenceError),
    Effects(EffectContractError),
    MissingChildEffects(ExpressionUseId),
    InvalidSource(&'static str),
    UnsupportedAbi(PureKernelAbi),
}
impl From<CompileControlError> for PhysicalWindowOccurrenceError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<EffectContractError> for PhysicalWindowOccurrenceError {
    fn from(cause: EffectContractError) -> Self {
        Self::Effects(cause)
    }
}
impl From<ExpressionOccurrenceError> for PhysicalWindowOccurrenceError {
    fn from(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            error => Self::Function(error),
        }
    }
}
impl From<PhysicalRelationalEffectsError> for PhysicalWindowOccurrenceError {
    fn from(error: PhysicalRelationalEffectsError) -> Self {
        match error {
            PhysicalRelationalEffectsError::Control(cause) => Self::Control(cause),
            PhysicalRelationalEffectsError::Effects(cause) => Self::Effects(cause),
            PhysicalRelationalEffectsError::MissingChildEffects(id) => {
                Self::MissingChildEffects(id)
            }
            PhysicalRelationalEffectsError::InvalidSource(detail) => Self::InvalidSource(detail),
        }
    }
}
pub(crate) struct PhysicalWindowOccurrenceInput<'a> {
    pub node: &'a PhysicalNode,
    pub site: PhysicalCallSite,
    pub source: &'a WindowCallSpec,
    pub request: &'a AuthoredPhysicalWindowRequest<'a>,
    pub occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    pub child_effects: Option<&'a NodeExpressionEffects>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}
#[derive(Debug)]
pub(crate) struct FreshPhysicalWindowOccurrence {
    pub frozen: FrozenPhysicalCall,
    pub preparation: PureCallSpecialization,
}
/// Only ordered runtime parameter roots contribute scalar summaries. Partition
/// ordering has its own node roots; checked frame constants are static facts.
/// The caller owns original catalogue/source admission and the scope footer.
pub(crate) fn prepare_physical_window_occurrence_observed(
    input: PhysicalWindowOccurrenceInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalWindowOccurrence, PhysicalWindowOccurrenceError> {
    let node = input.occurrences.fragment().nodes().get(&input.node.id);
    work.step()?;
    let actual = match (&input.node.kind, input.site) {
        (NodeKind::Window(spec), PhysicalCallSite::Window { node: owner, call })
            if owner == input.node.id =>
        {
            spec.calls.get(call as usize)
        }
        _ => None,
    };
    let matching = node.is_some_and(|node| std::ptr::eq(node, input.node))
        && actual.is_some_and(|source| std::ptr::eq(source, input.source))
        && std::ptr::eq(input.source, input.request.source());
    work.step()?;
    if !matching {
        return Err(PhysicalWindowOccurrenceError::InvalidSource(
            "window occurrence has a different actual node or call slot",
        ));
    }
    let context = relational_context_observed(input.occurrences, input.site, work)?;
    let function = input.request.function();
    work.flush()?;
    let declaration = functions
        .pure_overload_declaration_observed(
            &function.function_id,
            function.kind,
            &input.request.selected().overload,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let abi = declaration.implementation().abi;
    let correct_abi = matches!(
        (function.kind, abi),
        (FunctionKind::Window, PureKernelAbi::WindowV1)
            | (FunctionKind::Aggregate, PureKernelAbi::AggregateWindowV1)
    );
    work.step()?;
    if !correct_abi {
        return Err(PhysicalWindowOccurrenceError::UnsupportedAbi(abi));
    }
    let count = input.request.request().arguments.len();
    work.flush()?;
    let mut argument_uses = Vec::new();
    argument_uses
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let mut children = ExpressionEffects::PURE_VALUE;
    for root in input
        .source
        .arguments
        .iter()
        .copied()
        .chain(input.source.order_by.iter().map(|key| key.expr))
    {
        let unit = input.node.expressions.as_deref().ok_or(
            PhysicalWindowOccurrenceError::InvalidSource(
                "Window parameter has no actual node unit",
            ),
        )?;
        let effects = input
            .child_effects
            .ok_or(PhysicalWindowOccurrenceError::InvalidSource(
                "Window parameter has no original unit summaries",
            ))?;
        let (use_id, effects) = relational_root_effects_observed(unit, root, effects, work)?;
        children = children.join(effects);
        argument_uses.push(Some(use_id));
        work.step()?;
    }
    let complete = argument_uses.len() == count;
    work.step()?;
    if !complete {
        return Err(PhysicalWindowOccurrenceError::InvalidSource(
            "Window parameter-use coverage differs from its request",
        ));
    }
    let call = CallEffectInput {
        context: context.into(),
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: &function.function_id,
        kind: function.kind,
        selected: input.request.selected().as_ref(),
        request: input.request.request(),
        environment: input.environment,
        parameters: input.parameters,
        decimal_overflow_policy: input.decimal_overflow_policy,
        proof_scope: input.proof_scope,
    };
    work.flush()?;
    let options = input
        .request
        .preparation(ScopedRelationalEffects::primitive(context, children));
    work.flush()?;
    let preparation = functions
        .prepare_fresh_selected(
            call,
            Arc::clone(input.request.selected()),
            options,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let frozen = FrozenPhysicalCall {
        site: input.site,
        context,
        materialized_writer_input: None,
        effects: preparation.call_contract().effects().clone(),
        decimal_overflow_policy: input.decimal_overflow_policy,
    };
    work.flush()?;
    Ok(FreshPhysicalWindowOccurrence {
        frozen,
        preparation,
    })
}
