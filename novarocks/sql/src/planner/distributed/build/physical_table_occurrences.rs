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

//! Fresh exact-owner table facts from independent actual argument roots.

use std::sync::Arc;

use novarocks_functions::{
    CallEffectInput, PureCallPreparation, PureCallSpecialization, PureKernelAbi,
    ScopedRelationalEffects,
};
use novarocks_physical_plan::{
    Fragment, FrozenPhysicalCall, NodeKind, PhysicalCallSite, PhysicalNode,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, CompileControlError,
    DecimalOverflowPolicy, EffectContractError, ExpressionEffects, ExpressionUseId, FunctionKind,
    SemanticParameterRef, SemanticParameters,
};

use super::{
    expression_occurrences::{AuthoredPhysicalOccurrences, ExpressionOccurrenceError},
    physical_relational_effects::{
        PhysicalRelationalEffectsError, relational_context_observed,
        relational_root_effects_observed,
    },
    physical_table_requests::AuthoredPhysicalTableRequest,
};
use crate::compiler::SqlFunctionCatalog;

#[cfg(test)]
#[path = "physical_table_occurrences_tests.rs"]
mod tests;

#[derive(Debug)]
pub(crate) enum PhysicalTableOccurrenceError {
    Control(CompileControlError),
    Function(ExpressionOccurrenceError),
    Effects(EffectContractError),
    MissingChildEffects(ExpressionUseId),
    InvalidSource(&'static str),
    UnsupportedAbi(PureKernelAbi),
}
impl From<CompileControlError> for PhysicalTableOccurrenceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExpressionOccurrenceError> for PhysicalTableOccurrenceError {
    fn from(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Function(other),
        }
    }
}
impl From<EffectContractError> for PhysicalTableOccurrenceError {
    fn from(error: EffectContractError) -> Self {
        Self::Effects(error)
    }
}

impl From<PhysicalRelationalEffectsError> for PhysicalTableOccurrenceError {
    fn from(error: PhysicalRelationalEffectsError) -> Self {
        match error {
            PhysicalRelationalEffectsError::Control(cause) => Self::Control(cause),
            PhysicalRelationalEffectsError::Effects(error) => Self::Effects(error),
            PhysicalRelationalEffectsError::MissingChildEffects(id) => {
                Self::MissingChildEffects(id)
            }
            PhysicalRelationalEffectsError::InvalidSource(reason) => Self::InvalidSource(reason),
        }
    }
}

/// The original immutable table node, static request and occurrence topology
/// are separate loans. Source policy/environment/proof are explicit; legacy
/// binding semantics and a package default do not supply them.
pub(crate) struct PhysicalTableOccurrenceInput<'a> {
    pub fragment: &'a Fragment,
    pub source: &'a PhysicalNode,
    pub request: &'a AuthoredPhysicalTableRequest<'a>,
    pub occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    pub child_effects: Option<&'a super::contract_lowering::NodeExpressionEffects>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

#[derive(Debug)]
pub(crate) struct FreshPhysicalTableOccurrence {
    pub frozen: FrozenPhysicalCall,
    pub preparation: PureCallSpecialization,
}

/// Prepare the installed TableV1 owner for this exact physical occurrence.
/// Each ordered argument is a separate root/domain, including repeated ExprId
/// definitions. Verify its original site, definition, demand and scoped summary
/// before joining neutral conservative effects into the table context. This
/// does not relocate a child use or authorize hoisting/caching across domains.
///
/// The caller admits all source, temporary argument-use storage, opaque owner
/// lookups/clones and retained preparation before entry. It also validates the
/// complete same immutable source/root-flow pairing and supplies child-first
/// summaries. This leaf neither reconstructs a flow nor creates a runtime
/// table instance, lateral/LEFT assembly, output layout or package authority.
/// The caller owns entry and success/ordinary footer; primary control returns
/// directly through the existing nested failure mapper.
pub(crate) fn prepare_physical_table_occurrence_observed(
    input: PhysicalTableOccurrenceInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalTableOccurrence, PhysicalTableOccurrenceError> {
    let actual = input.fragment.nodes().get(&input.source.id);
    work.step()?;
    let same_source = actual.is_some_and(|node| std::ptr::eq(node, input.source))
        && std::ptr::eq(input.source, input.request.source())
        && std::ptr::eq(input.occurrences.fragment(), input.fragment);
    work.step()?;
    if !same_source {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "table occurrence and request have different actual source loans",
        ));
    }
    let NodeKind::TableFunction {
        function,
        arguments,
        ..
    } = &input.source.kind
    else {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "table occurrence source is not an actual table function",
        ));
    };
    let same_function = std::ptr::eq(function, input.request.function())
        && input.request.request().arguments.len() == arguments.len();
    work.step()?;
    if !same_function {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "table occurrence has different binding or channel coverage",
        ));
    }
    let site = PhysicalCallSite::Table {
        node: input.source.id,
    };
    let context = relational_context_observed(input.occurrences, site, work)?;
    work.flush()?;
    let declaration = functions
        .pure_overload_declaration_observed(
            &function.function_id,
            FunctionKind::Table,
            &input.request.selected().overload,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let abi = declaration.implementation().abi;
    let correct = abi == PureKernelAbi::TableV1;
    work.step()?;
    if !correct {
        return Err(PhysicalTableOccurrenceError::UnsupportedAbi(abi));
    }
    let correct_control = declaration.effects().argument_control == ArgumentControl::Table;
    work.step()?;
    if !correct_control {
        return Err(PhysicalTableOccurrenceError::InvalidSource(
            "installed table owner has different argument control",
        ));
    }
    work.flush()?;
    let mut argument_uses = Vec::new();
    argument_uses
        .try_reserve_exact(arguments.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let unit = input.source.expressions.as_deref();
    let mut children = ExpressionEffects::PURE_VALUE;
    for &root in arguments {
        let unit = unit.ok_or(PhysicalTableOccurrenceError::InvalidSource(
            "table parameter has no actual node unit",
        ))?;
        let (use_id, actual_effects) = relational_root_effects_observed(
            unit,
            root,
            input
                .child_effects
                .ok_or(PhysicalTableOccurrenceError::InvalidSource(
                    "parameter has no original unit summaries",
                ))?,
            work,
        )?;
        children = children.join(actual_effects);
        argument_uses.push(Some(use_id));
        work.step()?;
    }
    let call = CallEffectInput {
        context: context.into(),
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: &function.function_id,
        kind: FunctionKind::Table,
        selected: input.request.selected().as_ref(),
        request: input.request.request(),
        environment: input.environment,
        parameters: input.parameters,
        decimal_overflow_policy: input.decimal_overflow_policy,
        proof_scope: input.proof_scope,
    };
    work.flush()?;
    let preparation = functions
        .prepare_fresh_selected(
            call,
            Arc::clone(input.request.selected()),
            PureCallPreparation::Table {
                arguments: ScopedRelationalEffects::primitive(context, children),
            },
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let frozen = FrozenPhysicalCall {
        materialized_writer_input: None,
        site,
        context,
        effects: preparation.call_contract().effects().clone(),
        decimal_overflow_policy: input.decimal_overflow_policy,
    };
    work.flush()?;
    Ok(FreshPhysicalTableOccurrence {
        frozen,
        preparation,
    })
}
