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

//! Exact higher-order call facts and one-shot element-domain body invocations.
//! Collection expansion and capture gathering remain in their exact owners.

use std::sync::Arc;

use arrow_array::new_empty_array;
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompilePhase, EvaluationDemand, ExpressionEffectContext,
    ExpressionUseId, FunctionKind, FunctionValueType, PureCompileControl,
};

use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::{
    EvaluationCheckpoints, logical_is_null, validate_argument_observed, validate_type_observed,
};
use crate::{
    CallEffectInput, EvaluatedArgument, FunctionArgumentType, FunctionBindingSelection,
    FunctionCallContract, FunctionResultType, KernelEvaluationControl, KernelFailure,
    LambdaElementRowMap, RefinedCallEffects, RowDataError, ScopedExpressionEffects, SelectedValues,
    Selection,
};

/// The body edge is captured from the same exact input/refinement receipt as
/// the call. It is not supplied later by name, result type or a sibling use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HigherOrderCallContract {
    call: FunctionCallContract,
    body_ordinal: usize,
    body_edge_use: ExpressionUseId,
    body_demand: EvaluationDemand,
}
impl HigherOrderCallContract {
    pub fn from_refined(
        input: CallEffectInput<'_>,
        receipt: &RefinedCallEffects<'_>,
        selected: Arc<FunctionBindingSelection>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(compile_failure)?;
        if input.kind != FunctionKind::Scalar || input.selected.aggregate.is_some() {
            return Err(invalid(
                "higher-order calls require an exact scalar binding",
            ));
        }
        let ArgumentControl::HigherOrder {
            body_ordinal,
            body_demand,
        } = receipt.facts().argument_control
        else {
            return Err(invalid("call has no exact higher-order control"));
        };
        let call = FunctionCallContract::from_refined(input, receipt, selected, control)?;
        let body_ordinal = body_ordinal as usize;
        let crate::CallArgumentUses::SelectedChannels(argument_uses) = input.argument_uses else {
            return Err(invalid(
                "higher-order calls require selected argument channels",
            ));
        };
        let body_edge_use = argument_uses
            .get(body_ordinal)
            .copied()
            .flatten()
            .ok_or_else(|| invalid("higher-order body has no exact argument use"))?;
        Ok(Self {
            call,
            body_ordinal,
            body_edge_use,
            body_demand,
        })
    }
    pub const fn call(&self) -> &FunctionCallContract {
        &self.call
    }
    pub const fn body_ordinal(&self) -> usize {
        self.body_ordinal
    }
    pub const fn body_edge_use(&self) -> ExpressionUseId {
        self.body_edge_use
    }
    pub const fn body_demand(&self) -> EvaluationDemand {
        self.body_demand
    }
    pub fn parameter_types(&self) -> &[FunctionValueType] {
        match &self.call.selected().argument_types[self.body_ordinal] {
            FunctionArgumentType::Lambda {
                parameter_types, ..
            } => parameter_types,
            FunctionArgumentType::Value(_) | FunctionArgumentType::CandidateGroup { .. } => {
                unreachable!("refinement checked the lambda channel")
            }
        }
    }
    pub fn body_result_type(&self) -> &FunctionValueType {
        match &self.call.selected().argument_types[self.body_ordinal] {
            FunctionArgumentType::Lambda { result_type, .. } => result_type,
            FunctionArgumentType::Value(_) | FunctionArgumentType::CandidateGroup { .. } => {
                unreachable!("refinement checked the lambda channel")
            }
        }
    }
    pub fn result_type(&self) -> &FunctionValueType {
        match &self.call.selected().result_type {
            FunctionResultType::Scalar(result) => result,
            FunctionResultType::Relation(_) => unreachable!("constructor accepts scalar bindings"),
        }
    }
}

/// Immutable body-use facts owned by the expression compiler. The complete
/// body effects include captures and nested calls in this element domain.
/// Its frozen environment belongs to its compiled child implementation; the
/// parent's own parameter projection is not the body's environment closure.
///
/// The compiler must prove the body edge/root relationship in its checked
/// control graph and bind ordered captures to their actual sources, including
/// enclosing lambda parameters. Equal types do not prove capture provenance.
/// This contract does not repeat the program's source IDs or lookup service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LambdaBodyContract {
    call: Arc<HigherOrderCallContract>,
    context: ExpressionEffectContext,
    effects: ScopedExpressionEffects,
    capture_types: Box<[FunctionValueType]>,
}
impl LambdaBodyContract {
    pub fn try_new(
        call: Arc<HigherOrderCallContract>,
        context: ExpressionEffectContext,
        effects: ScopedExpressionEffects,
        capture_types: Box<[FunctionValueType]>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let call_context = call
            .call()
            .context()
            .expression()
            .ok_or_else(|| invalid("higher-order contract has a non-scalar context"))?;
        // The lambda edge and inner body root can have distinct use IDs.
        // Row expansion must not reuse the outer batch's evaluation domain.
        if context.demand != call.body_demand()
            || context.region == call_context.region
            || effects.context() != context
        {
            return Err(invalid(
                "lambda body effects differ from its element context",
            ));
        }
        for capture in &capture_types {
            validate_type_observed(capture, &mut work)?;
            work.step().map_err(compile_failure)?;
        }
        work.finish().map_err(compile_failure)?;
        Ok(Self {
            call,
            context,
            effects,
            capture_types,
        })
    }
    pub fn call(&self) -> &Arc<HigherOrderCallContract> {
        &self.call
    }
    pub const fn context(&self) -> ExpressionEffectContext {
        self.context
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn parameter_types(&self) -> &[FunctionValueType] {
        self.call.parameter_types()
    }
    pub fn result_type(&self) -> &FunctionValueType {
        self.call.body_result_type()
    }
    pub fn capture_types(&self) -> &[FunctionValueType] {
        &self.capture_types
    }
}

/// Borrowed invocation-local parameter and capture carriers. The host owns
/// their storage and authorizes expansion bytes/work before gathering them;
/// outer-row count does not bound array length or comparator pair expansion.
/// Structural checking is neither a MEM grant nor proof of offsets, capture
/// sources, collection NULL rules or same-sized frame identity.
#[derive(Clone, Copy, Debug)]
pub struct LambdaBodyInput<'contract, 'input> {
    contract: &'contract LambdaBodyContract,
    row_map: LambdaElementRowMap<'input>,
    selection: Selection<'input>,
    parameters: &'input [EvaluatedArgument<'input>],
    captures: &'input [EvaluatedArgument<'input>],
}
impl<'contract, 'input> LambdaBodyInput<'contract, 'input> {
    pub fn try_new(
        contract: &'contract LambdaBodyContract,
        row_map: LambdaElementRowMap<'input>,
        selection: Selection<'input>,
        parameters: &'input [EvaluatedArgument<'input>],
        captures: &'input [EvaluatedArgument<'input>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        row_map.validate_selection(selection)?;
        if parameters.len() != contract.parameter_types().len()
            || captures.len() != contract.capture_types().len()
        {
            return Err(invalid(
                "lambda arguments differ from their exact body shape",
            ));
        }
        for (value, ty) in parameters
            .iter()
            .zip(contract.parameter_types())
            .chain(captures.iter().zip(contract.capture_types()))
        {
            validate_argument_observed(*value, selection, ty, control)?;
        }
        control.checkpoint(0)?;
        Ok(Self {
            contract,
            row_map,
            selection,
            parameters,
            captures,
        })
    }
    pub const fn contract(self) -> &'contract LambdaBodyContract {
        self.contract
    }
    pub const fn row_map(self) -> LambdaElementRowMap<'input> {
        self.row_map
    }
    pub const fn selection(self) -> Selection<'input> {
        self.selection
    }
    pub const fn parameters(self) -> &'input [EvaluatedArgument<'input>] {
        self.parameters
    }
    pub const fn captures(self) -> &'input [EvaluatedArgument<'input>] {
        self.captures
    }
}

/// One runtime-owned, exactly compiled body use. Mutable body state is private
/// to its evaluation instance. The interface has no source-row trial, fallback,
/// SQL-name dispatch or expression lookup operation. Input/capture errors must
/// be resolved by the host controller before this ordinary body invocation.
pub trait BoundLambdaBodyEvaluator: Send {
    fn contract(&self) -> &Arc<LambdaBodyContract>;
    fn evaluate<'input>(
        &mut self,
        input: LambdaBodyInput<'_, 'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'input>, KernelFailure>;
}

/// Compact child values/errors remain in their element domain. Only the exact
/// higher-order owner decides necessity and folds required errors to parents;
/// several failed elements are never converted to a successful parent NULL.
#[derive(Debug)]
pub struct LambdaBodyOutput<'input> {
    contract: Arc<LambdaBodyContract>,
    row_map: LambdaElementRowMap<'input>,
    values: SelectedValues<'input>,
}
impl<'input> LambdaBodyOutput<'input> {
    pub fn contract(&self) -> &Arc<LambdaBodyContract> {
        &self.contract
    }
    pub const fn row_map(&self) -> LambdaElementRowMap<'input> {
        self.row_map
    }
    pub const fn values(&self) -> &SelectedValues<'input> {
        &self.values
    }
    pub fn into_values(self) -> SelectedValues<'input> {
        self.values
    }
    /// The exact owner explicitly selects an actually returned child error as
    /// required. This O(1) bridge preserves its checked body identity and outer
    /// row domain; it never creates a child error from a successful SQL NULL.
    /// Duplicate parent errors/necessity remain the higher-order owner's job.
    pub fn required_parent_error(
        &self,
        error_index: usize,
    ) -> Result<Option<RequiredLambdaBodyError<'input>>, KernelFailure> {
        let Some(error) = self.values.errors().get(error_index) else {
            return Ok(None);
        };
        let element = self
            .values
            .selection()
            .row(error.selected_ordinal())
            .ok_or_else(|| internal("lambda error lost its selected element"))?;
        let parent = self
            .row_map
            .parent_ordinal(element)
            .ok_or_else(|| internal("lambda error lost its selected parent"))?;
        Ok(Some(RequiredLambdaBodyError {
            contract: Arc::clone(&self.contract),
            outer: self.row_map.outer_selection(),
            error: error.with_selected_ordinal(parent),
            origin: None,
        }))
    }
}

/// A required child error can only originate in a checked LambdaBodyOutput.
/// Its parent ordinal is compact in the retained outer Selection. A runtime
/// adapter validates the same body contract and outer rows before accepting
/// this channel; a parent own-row-error declaration cannot erase it.
#[derive(Clone, Debug)]
pub struct RequiredLambdaBodyError<'input> {
    contract: Arc<LambdaBodyContract>,
    outer: Selection<'input>,
    error: RowDataError,
    pub(crate) origin: Option<crate::aggregate_host_allocator::HostAggregateAllocator>,
}
impl<'input> RequiredLambdaBodyError<'input> {
    pub(crate) fn from_batch(
        contract: Arc<LambdaBodyContract>,
        outer: Selection<'input>,
        parent: usize,
        error: &RowDataError,
        origin: crate::aggregate_host_allocator::HostAggregateAllocator,
    ) -> Self {
        Self {
            contract,
            outer,
            error: error.with_selected_ordinal(parent),
            origin: Some(origin),
        }
    }

    pub fn contract(&self) -> &Arc<LambdaBodyContract> {
        &self.contract
    }
    pub const fn outer_selection(&self) -> Selection<'input> {
        self.outer
    }
    pub const fn error(&self) -> &RowDataError {
        &self.error
    }
    pub fn original_parent_row(&self) -> Option<usize> {
        self.outer.row(self.error.selected_ordinal())
    }
    /// Reborrow the same outer rows from the enclosing call rather than from
    /// a short-lived expansion frame. The evidence owns its body/diagnostic;
    /// no element parameters or expansion parent map survive this operation.
    /// Exact invocation provenance remains the enclosing host's obligation.
    pub fn into_outer_selection<'outer>(
        self,
        outer: Selection<'outer>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<RequiredLambdaBodyError<'outer>, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        if !self.outer.same_rows_observed(outer, || work.step())? {
            return Err(invalid("lambda error cannot move to unrelated outer rows"));
        }
        work.finish()?;
        Ok(RequiredLambdaBodyError {
            contract: self.contract,
            outer,
            error: self.error,
            origin: self.origin,
        })
    }
}

/// Exclusive one-shot batch frame over the host's body instance. Both success
/// and failure consume this frame before private code runs. The higher-order
/// invocation owner still proves domains do not overlap across separate frames
/// and latches outer failures for its entire operation; this is not a global
/// evaluated-element journal. Empty masks never call the private body.
pub struct LambdaBodyInvocation<'evaluator, 'contract, 'input> {
    evaluator: &'evaluator mut dyn BoundLambdaBodyEvaluator,
    contract: Arc<LambdaBodyContract>,
    input: LambdaBodyInput<'contract, 'input>,
    finished: bool,
}
impl<'evaluator, 'contract, 'input> LambdaBodyInvocation<'evaluator, 'contract, 'input> {
    pub fn try_bind(
        evaluator: &'evaluator mut dyn BoundLambdaBodyEvaluator,
        input: LambdaBodyInput<'contract, 'input>,
    ) -> Result<Self, KernelFailure> {
        let contract = Arc::clone(evaluator.contract());
        if !std::ptr::eq(contract.as_ref(), input.contract()) {
            return Err(invalid(
                "lambda evaluator belongs to a different compiled body",
            ));
        }
        Ok(Self {
            evaluator,
            contract,
            input,
            finished: false,
        })
    }
    pub fn run(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<LambdaBodyOutput<'input>, KernelFailure> {
        if self.finished {
            return Err(KernelFailure::InstanceFailed);
        }
        self.finished = true;
        control.checkpoint(0)?;
        self.validate_metadata()?;
        let input = self.input;
        let result = if input.selection().is_empty() {
            SelectedValues::try_new(
                input.selection(),
                input.contract().result_type().data_type(),
                new_empty_array(input.contract().result_type().data_type()),
                Box::default(),
            )
            .map_err(|_| internal("empty lambda output violates its contract"))
        } else {
            self.evaluator.evaluate(input, control)
        };
        let output = result?;
        self.validate_metadata()?;
        control.checkpoint(0)?;
        validate_body_output(input.contract(), input.selection(), &output, control)?;
        Ok(LambdaBodyOutput {
            contract: Arc::clone(&self.contract),
            row_map: input.row_map(),
            values: output,
        })
    }
    fn validate_metadata(&self) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(self.evaluator.contract(), &self.contract) {
            return Err(internal(
                "lambda evaluator changed its exact immutable contract",
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_body_output(
    body: &LambdaBodyContract,
    selection: Selection<'_>,
    output: &SelectedValues<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    if !output
        .selection()
        .same_rows_observed(selection, || work.step())?
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            output.values().data_type(),
            body.result_type().data_type(),
            || work.step(),
        )?
    {
        return Err(internal(
            "lambda body returned an unrelated selection or type",
        ));
    }
    if !output.errors().is_empty()
        && !body
            .effects()
            .for_use(body.context())
            .map_err(|_| internal("lambda body lost its exact effect scope"))?
            .may_raise_row_error
    {
        return Err(internal(
            "never-failing lambda body returned a row data error",
        ));
    }
    // SelectedValues already checks compact shape and ordered error NULLs.
    // Errors are child errors: the parent's own_row_error does not gate them.
    if !body.result_type().nullable() {
        let mut errors = output.errors().iter().peekable();
        for ordinal in 0..output.values().len() {
            if errors
                .peek()
                .is_some_and(|error| error.selected_ordinal() == ordinal)
            {
                errors.next();
                work.step()?;
            } else if logical_is_null(output.values().as_ref(), ordinal, 1, &mut work)? {
                return Err(internal(
                    "non-null lambda body returned a successful SQL NULL",
                ));
            }
        }
    }
    work.finish()
}

#[cfg(test)]
mod tests;
