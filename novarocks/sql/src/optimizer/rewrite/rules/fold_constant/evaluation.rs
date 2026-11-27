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

use std::{convert::Infallible, sync::Arc};

use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ExprCompileError, ExprEvaluationFailure, ScalarCallFailure, ScalarCallHost, ScalarCallPhase,
    ScalarCallRequestFacts, ScopeLayouts, compile_expr_set_pure,
};
use novarocks_expression_contract::{RootScope, UnitError};
use novarocks_failure_contract::{
    FailureClass, FailureProjection, RepresentationBoundary, SharedFailureCause,
};
use novarocks_functions::{
    AggregateStateAllocator, CompileKernelControl, ConstantValue, FunctionBindingError,
    KernelDiagnostic, KernelFailure, PureEngineFunctionCatalog, ScalarInvocationActivation,
    ScalarInvocationScope, ScalarResourceError, Selection,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ExpressionControlFlowError, PureCompileControl,
};

use super::admission::{admit_result, compiled_unit_can_fold};
use crate::binding::LogicalCallArgumentCaptureError;
use crate::compiler::SqlCompileError;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::scalar::{ScalarArena, ScalarId};
use crate::planner::distributed::build::{
    ContractLoweringError, StandaloneScalarEmission, lower_required_constant_unit,
};
use crate::planner::optimizer_bridge::scalar::materialize;

pub(super) enum FoldNodeOutcome {
    Folded(ConstantValue),
    Kept,
}

pub(super) struct EvaluatedFoldNode {
    pub(super) outcome: FoldNodeOutcome,
    /// Retained only to lend the original compiled unit to the fold observer.
    pub(super) unit: Option<StandaloneScalarEmission>,
}

impl EvaluatedFoldNode {
    fn declined_before_compilation() -> Self {
        Self {
            outcome: FoldNodeOutcome::Kept,
            unit: None,
        }
    }
}

/// Evaluate one already eligible arena node with constant children.
pub(super) fn fold_node(
    input: ScalarId,
    arena: &ScalarArena,
    ctx: &RewriteContext<'_>,
) -> Result<EvaluatedFoldNode, SqlCompileError> {
    let sealed_functions = ctx
        .sealed_functions()
        .ok_or_else(|| invalid("constant folding has no sealed function catalog"))?;
    let control = ctx.control_view();
    control.checkpoint(CompilePhase::Validate, 0)?;
    let expression = materialize(arena, input);
    let source = lower_required_constant_unit(
        &expression,
        Arc::clone(ctx.function_catalog_arc()),
        Arc::clone(sealed_functions),
        arena.constant_policy(),
        &control,
    );
    let Some(source) = lower_for_fold(source)? else {
        return Ok(EvaluatedFoldNode::declined_before_compilation());
    };
    let Some(compiled) = compile_for_fold(&source, sealed_functions, &control)? else {
        return Ok(EvaluatedFoldNode::declined_before_compilation());
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)?;
    let eligible = compiled_unit_can_fold(&compiled, &mut work)?;
    work.finish()?;
    let outcome = if eligible {
        evaluate_fold(&source, compiled, arena.value_type(input), &control)?
    } else {
        FoldNodeOutcome::Kept
    };
    Ok(EvaluatedFoldNode {
        outcome,
        unit: Some(source),
    })
}

fn lower_for_fold(
    source: Result<StandaloneScalarEmission, ContractLoweringError>,
) -> Result<Option<StandaloneScalarEmission>, SqlCompileError> {
    use ExpressionControlFlowError::{TooDeep, TooManyItems};
    use FunctionBindingError::UnavailableImplementation;
    match source {
        Ok(source) => Ok(Some(source)),
        Err(
            ContractLoweringError::UnsupportedExpression { .. }
            | ContractLoweringError::Unit(UnitError::Flow(TooDeep | TooManyItems))
            | ContractLoweringError::Unit(UnitError::Binding(UnavailableImplementation(_)))
            | ContractLoweringError::FunctionBinding(UnavailableImplementation(_))
            | ContractLoweringError::LogicalCallSource(LogicalCallArgumentCaptureError::Binding(
                UnavailableImplementation(_),
            )),
        ) => Ok(None),
        Err(ContractLoweringError::Semantics(cause))
            if cause.failure_fact().class() == FailureClass::Unsupported =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn compile_for_fold(
    source: &StandaloneScalarEmission,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<Option<Arc<CompiledExprSet>>, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let compiled = compile_expr_set_pure(
        source.unit(),
        EvaluationEnvironment {
            constants: source.constants(),
            parameters: source.parameters(),
        },
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        source.constant_policy(),
        functions,
        &mut work,
    );
    compiled_for_fold(work.finish_result(compiled, ExprCompileError::from))
}

fn compiled_for_fold(
    compiled: Result<Arc<CompiledExprSet>, ExprCompileError>,
) -> Result<Option<Arc<CompiledExprSet>>, SqlCompileError> {
    match compiled {
        Ok(compiled) => Ok(Some(compiled)),
        Err(ExprCompileError::Control(cause)) => Err(cause.into()),
        Err(error) if error.is_unsupported() => Ok(None),
        Err(error) => Err(SqlCompileError::CompilationSource(SharedFailureCause::new(
            error,
        ))),
    }
}

fn evaluate_fold(
    source: &StandaloneScalarEmission,
    compiled: Arc<CompiledExprSet>,
    arena_type: &novarocks_type_contract::FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<FoldNodeOutcome, SqlCompileError> {
    let layout = compiled
        .scope(RootScope::AllPorts)
        .ok_or_else(|| invalid("constant folding has no singleton input layout"))?;
    if !compiled.scopes().is_empty() || !layout.schema().fields().is_empty() {
        return Err(invalid("constant folding requires zero input ports"));
    }
    let input = RecordBatch::try_new_with_options(
        layout.schema().clone(),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|_| invalid("constant folding singleton input construction failed"))?;
    let kernel_control = CompileKernelControl::new(control, CompilePhase::FunctionSpecialization);
    let mut instance =
        CompiledExpressionInstance::try_new(compiled, source.root(), &kernel_control)
            .map_err(kernel_error)?;
    let selection = Selection::all(1);
    let values = match instance.evaluate(
        EvaluationInput::Batch {
            batch: &input,
            source: None,
        },
        ScalarInvocationScope::new(selection, selection),
        ScalarInvocationActivation::Activated,
        &kernel_control,
        &mut FoldCallHost { control },
    ) {
        Ok(values) => values,
        Err(ExprEvaluationFailure::Kernel(error))
            if error.is_local_output_allocation_failure() || is_fold_output_extent(&error) =>
        {
            drop(instance);
            drop(error);
            control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
            return Ok(FoldNodeOutcome::Kept);
        }
        Err(error) => return Err(evaluation_error(error)),
    };
    let (selected, array, row_errors) = values.into_parts();
    if !selected.is_all() || selected.batch_rows() != 1 {
        return Err(invalid(
            "constant folding returned a different selected domain",
        ));
    }
    if !row_errors.is_empty() {
        return Ok(FoldNodeOutcome::Kept);
    }
    Ok(
        match admit_result(array, arena_type, source.constant_policy(), control)? {
            Some(value) => FoldNodeOutcome::Folded(value),
            None => FoldNodeOutcome::Kept,
        },
    )
}

pub(super) fn is_fold_output_extent(error: &impl FailureProjection) -> bool {
    error.failure_fact().representation().is_some_and(|facts| {
        matches!(
            facts.boundary,
            RepresentationBoundary::MinimumUnit | RepresentationBoundary::SingleOutput
        )
    })
}

fn invalid(message: &'static str) -> SqlCompileError {
    kernel_error(KernelFailure::InvalidProgram(KernelDiagnostic::new(
        message,
    )))
}

fn kernel_error(error: KernelFailure) -> SqlCompileError {
    match error {
        KernelFailure::CompileControl(cause) => cause.into(),
        error => SqlCompileError::CompilationSource(SharedFailureCause::new(error)),
    }
}

fn evaluation_error(error: ExprEvaluationFailure<Infallible>) -> SqlCompileError {
    match error {
        ExprEvaluationFailure::Kernel(cause) => kernel_error(cause),
        ExprEvaluationFailure::Host(never) => match never {},
        error => SqlCompileError::CompilationSource(SharedFailureCause::new(error)),
    }
}

struct FoldCallHost<'a> {
    control: &'a dyn PureCompileControl,
}

impl ScalarCallHost for FoldCallHost<'_> {
    type Refusal = Infallible;

    fn begin_call(&mut self) {}

    fn run_admitted(
        &mut self,
        _phase: ScalarCallPhase,
        _facts: ScalarCallRequestFacts,
        body: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Self::Refusal>>,
    ) -> Result<(), ScalarCallFailure<Self::Refusal>> {
        self.control
            .checkpoint(CompilePhase::FunctionSpecialization, 1)
            .map_err(KernelFailure::CompileControl)?;
        body()
    }

    fn callback_request_facts(
        &mut self,
        _allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Self::Refusal>> {
        Err(ScalarCallFailure::Source(ScalarResourceError::Invariant(
            "constant folding cannot provide a scalar callback request source",
        )))
    }
}

#[cfg(test)]
#[path = "evaluation/tests.rs"]
mod tests;
