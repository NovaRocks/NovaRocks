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

//! Fresh Writer lifecycle facts from original materialized Value channels.

use std::sync::Arc;

use novarocks_functions::{
    CallArgumentUses, CallEffectInput, PureCallSpecialization, PureKernelAbi,
    ScopedRelationalEffects,
};
use novarocks_physical_plan::{
    Fragment, FrozenPhysicalCall, PhysicalCallSite, PhysicalNode, WriterAggregateCall,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, CompileControlError,
    DecimalOverflowPolicy, FunctionKind, SemanticParameterRef, SemanticParameters,
};

use super::{
    expression_occurrences::{
        AuthoredPhysicalOccurrences, ExpressionOccurrenceError, WriterOccurrenceContexts,
    },
    physical_relational_effects::{
        PhysicalRelationalEffectsError, relational_writer_context_observed,
        writer_value_context_effects_observed,
    },
    physical_writer_requests::AuthoredPhysicalWriterRequest,
};
use crate::compiler::SqlFunctionCatalog;

#[derive(Debug)]
pub(crate) enum PhysicalWriterOccurrenceError {
    Control(CompileControlError),
    Function(ExpressionOccurrenceError),
    Relational(PhysicalRelationalEffectsError),
    InvalidSource(&'static str),
    UnsupportedAbi(PureKernelAbi),
}
impl From<CompileControlError> for PhysicalWriterOccurrenceError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ExpressionOccurrenceError> for PhysicalWriterOccurrenceError {
    fn from(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Function(other),
        }
    }
}
impl From<PhysicalRelationalEffectsError> for PhysicalWriterOccurrenceError {
    fn from(error: PhysicalRelationalEffectsError) -> Self {
        match error {
            PhysicalRelationalEffectsError::Control(cause) => Self::Control(cause),
            other => Self::Relational(other),
        }
    }
}

/// The closed request loans its own original journal entry, captured request,
/// installed selection and actual materialized ValueDef. Final's complete
/// checked contributor graph and every producer's installed owner validation
/// are prerequisites of that request; no contributor substitutes its request
/// for the consumer. All occurrence scopes remain explicit caller inputs.
pub(crate) struct PhysicalWriterOccurrenceInput<'a, 'source> {
    pub fragment: &'source Fragment,
    pub node: &'source PhysicalNode,
    pub source: &'source WriterAggregateCall,
    pub request: &'a AuthoredPhysicalWriterRequest<'a, 'source>,
    pub occurrences: &'a AuthoredPhysicalOccurrences<'source>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

pub(crate) struct WriterOccurrenceCallInput<'a, 'source> {
    pub fragment: &'source Fragment,
    pub node: &'source PhysicalNode,
    pub source: &'source WriterAggregateCall,
    pub request: &'a AuthoredPhysicalWriterRequest<'a, 'source>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

#[derive(Debug)]
pub(crate) struct FreshPhysicalWriterOccurrence {
    pub frozen: FrozenPhysicalCall,
    pub preparation: PureCallSpecialization,
}

/// Prepare exactly one real Writer Partial or Final using its retained
/// catalogue and selected Arc. Materialized input reading uses the original
/// pure Value leaf; it does not re-evaluate an origin expression or transfer
/// upstream operator effects. A Writer Value use is a separate original
/// topology loan, never an invented ExprInvocation or borrowed Project root.
/// Final's state context and options borrow the same actual ValueDef type;
/// Functions remains the sole state-domain/refinement/preparation author.
///
/// The caller owns entry and ordinary/success footer, complete source/resource
/// admission and all opaque owner/metadata clones. This leaf adds no wallet,
/// publishes no partial call table and grants no runtime/MEM capability.
/// Originating nested control returns immediately on the original control.
pub(crate) fn prepare_physical_writer_occurrence_observed(
    input: PhysicalWriterOccurrenceInput<'_, '_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalWriterOccurrence, PhysicalWriterOccurrenceError> {
    let contexts = input.occurrences.writer_contexts();
    prepare_writer_occurrence_in(
        WriterOccurrenceCallInput {
            fragment: input.fragment,
            node: input.node,
            source: input.source,
            request: input.request,
            parameters: input.parameters,
            environment: input.environment,
            decimal_overflow_policy: input.decimal_overflow_policy,
            proof_scope: input.proof_scope,
        },
        &contexts,
        functions,
        work,
    )
}

fn prepare_writer_occurrence_in(
    input: WriterOccurrenceCallInput<'_, '_>,
    occurrences: &WriterOccurrenceContexts<'_, '_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalWriterOccurrence, PhysicalWriterOccurrenceError> {
    let actual = input.fragment.nodes().get(&input.node.id);
    work.step()?;
    let same = actual.is_some_and(|node| std::ptr::eq(node, input.node))
        && std::ptr::eq(input.fragment, input.request.fragment())
        && std::ptr::eq(input.node, input.request.node())
        && std::ptr::eq(input.source, input.request.source())
        && occurrences.root_fragment() == input.fragment.id();
    work.step()?;
    if !same {
        return Err(PhysicalWriterOccurrenceError::InvalidSource(
            "writer occurrence and request have different actual source loans",
        ));
    }
    let same_catalogue = std::ptr::eq(functions, input.request.entry().function_catalog().as_ref());
    work.step()?;
    if !same_catalogue {
        return Err(PhysicalWriterOccurrenceError::InvalidSource(
            "writer occurrence replaces its retained original function catalogue",
        ));
    }
    let same_policy = input.decimal_overflow_policy == input.request.decimal_overflow_policy();
    work.step()?;
    if !same_policy {
        return Err(PhysicalWriterOccurrenceError::InvalidSource(
            "writer occurrence changes the original logical source policy",
        ));
    }
    let site = input.request.site();
    let phase = input.request.phase();
    let correct_site = match site {
        PhysicalCallSite::WriterPartial { node, .. } => {
            node == input.node.id && phase == novarocks_functions::AggregateKernelPhase::Partial
        }
        PhysicalCallSite::WriterFinal { node, .. } => {
            node == input.node.id && phase == novarocks_functions::AggregateKernelPhase::Final
        }
        _ => false,
    };
    work.step()?;
    if !correct_site {
        return Err(PhysicalWriterOccurrenceError::InvalidSource(
            "writer occurrence has a different actual lifecycle site or phase",
        ));
    }
    let function = input.request.function();
    let same_binding = std::ptr::eq(&input.source.binding, input.request.binding())
        && std::ptr::eq(&input.source.binding.function, function)
        && function.kind == FunctionKind::Aggregate
        && input.source.input == input.request.input_id()
        && input.request.input().id == input.request.input_id();
    work.step()?;
    if !same_binding {
        return Err(PhysicalWriterOccurrenceError::InvalidSource(
            "writer occurrence changes its binding or actual materialized input",
        ));
    }
    let request = input.request.request();
    let complete = request.logical_argument_count == 1 && request.arguments.len() == 1;
    work.step()?;
    if !complete {
        return Err(PhysicalWriterOccurrenceError::InvalidSource(
            "writer occurrence differs from its original single logical channel",
        ));
    }
    let context = relational_writer_context_observed(occurrences, site, work)?;
    let (value_context, children) = writer_value_context_effects_observed(
        occurrences,
        input.fragment,
        site,
        input.request.input(),
        work,
    )?;
    work.flush()?;
    let declaration = functions
        .pure_overload_declaration_observed(
            &function.function_id,
            FunctionKind::Aggregate,
            &input.request.selected().overload,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let abi = declaration.implementation().abi;
    let supported = matches!(
        abi,
        PureKernelAbi::AggregateV1 | PureKernelAbi::AggregateWindowV1
    );
    work.step()?;
    if !supported {
        return Err(PhysicalWriterOccurrenceError::UnsupportedAbi(abi));
    }
    let correct_control = declaration.effects().argument_control == ArgumentControl::Aggregate;
    work.step()?;
    if !correct_control {
        return Err(PhysicalWriterOccurrenceError::InvalidSource(
            "installed writer owner has different aggregate argument control",
        ));
    }
    let call = CallEffectInput {
        context: context.into(),
        argument_uses: if phase.consumes_logical_arguments() {
            CallArgumentUses::MaterializedInput {
                context: value_context,
                input_type: &input.request.input().ty,
            }
        } else {
            CallArgumentUses::AggregateMerge {
                phase,
                state_context: value_context.into(),
                state_input_type: &input.request.input().ty,
            }
        },
        function_id: &function.function_id,
        kind: FunctionKind::Aggregate,
        selected: input.request.selected().as_ref(),
        request,
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
        site,
        context,
        materialized_writer_input: Some(value_context),
        effects: preparation.call_contract().effects().clone(),
        decimal_overflow_policy: input.decimal_overflow_policy,
    };
    work.flush()?;
    Ok(FreshPhysicalWriterOccurrence {
        frozen,
        preparation,
    })
}
