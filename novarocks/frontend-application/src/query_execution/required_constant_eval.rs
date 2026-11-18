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

//! Required SQL constants through the one compiled expression implementation.
//! The caller owns the original CPU job and must observe its real exit before
//! applying session effects. This synchronous bridge creates no task or Root.

use arrow::{
    array::{Array, UInt64Array},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use novarocks_execution::exec::expr::compiled_program::CompiledExpressionInstance;
use novarocks_execution::runtime::task::ExecutionFailure;
use novarocks_expression::RequiredExpressionRowError;
use novarocks_expression::{
    EvaluationEnvironment, ExprCompileFailure, ExprCreation, ScopeLayouts, compile_expr_set,
};
use novarocks_expression_contract::RootScope;
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use novarocks_functions::{
    AggregateStateAllocator, CompileKernelControl, ConstantCreationFailure, ConstantPolicy,
    ConstantPool, ConstantValue, KernelDiagnostic, KernelFailure, PureEngineFunctionCatalog,
    ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_sql::compiler::{
    PackageSemanticsError, PreparedRequiredConstant, SqlConstantEvaluationError,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionArgumentType,
    FunctionInstanceState, PureCompileControl, ValueSchemaCreationFactory,
    ValueSchemaCreationScope,
};
use novarocks_workload_control::{ResultWindowAlias, WorkScope};
use std::{fmt, sync::Arc};

mod resources;
use resources::RootResources;
pub(crate) use resources::with_root_value_creation;

/// The server supplies the same sealed function catalogue as the BE.
/// Each invocation borrows its original statement and Root resources.
#[derive(Clone)]
pub struct RequiredConstantInterpreter {
    functions: Arc<PureEngineFunctionCatalog>,
    policy: ConstantPolicy,
}
impl fmt::Debug for RequiredConstantInterpreter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequiredConstantInterpreter")
            .finish_non_exhaustive()
    }
}
impl RequiredConstantInterpreter {
    pub fn new(functions: Arc<PureEngineFunctionCatalog>, policy: ConstantPolicy) -> Self {
        Self { functions, policy }
    }

    pub fn evaluate(
        &self,
        prepared: PreparedRequiredConstant,
        root: &WorkScope,
        window: &ResultWindowAlias,
    ) -> Result<ConstantValue, SqlConstantEvaluationError> {
        let control = prepared.control();
        control.checkpoint(CompilePhase::Validate, 0)?;
        if prepared.constant_policy() != self.policy {
            return Err(invariant(
                "required constant policy differs from the installed interpreter",
            ));
        }
        if let Some(original) = prepared.original_value() {
            // SQL retained this already-admitted selected value and applied no
            // conversion. Borrowing it does not construct a new backing.
            if !window.is_for_scope(root) {
                return Err(SqlConstantEvaluationError::from_domain(
                    novarocks_workload_control::WorkError::ForeignAuthority,
                ));
            }
            root.check()
                .map_err(SqlConstantEvaluationError::from_domain)?;
            control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
            return Ok(original.clone());
        }
        let mut resources = RootResources::new(root, window, control)?;
        let result = self.evaluate_in(&prepared, &mut resources);
        result.map_err(|error| resources.retain_error(error))
    }

    fn evaluate_in(
        &self,
        prepared: &PreparedRequiredConstant,
        resources: &mut RootResources<'_>,
    ) -> Result<ConstantValue, SqlConstantEvaluationError> {
        let control = prepared.control();
        let source = prepared.source();
        let unit = source.unit();
        let root = source.root();
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        for claim in unit.claims().values() {
            work.step()?;
            if claim.effects.value_stability.is_volatile()
                || claim.effects.instance_state != FunctionInstanceState::None
                || !claim.effects.observable_effects.is_empty()
            {
                return Err(SqlConstantEvaluationError::from_domain(
                    RequiredConstantEligibilityFailure::new(),
                ));
            }
        }
        work.finish()?;

        let mut retained = resources.factory.begin_scope()?;
        let mut temporary = resources.factory.begin_scope()?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let compiled = compile_expr_set(
            unit,
            EvaluationEnvironment {
                constants: source.constants(),
                parameters: source.parameters(),
            },
            ScopeLayouts {
                ports: &[],
                all_ports: None,
            },
            source.constant_policy(),
            self.functions.as_ref(),
            ExprCreation {
                temporary: &mut temporary,
                retained: &mut retained,
                factory: &resources.factory,
            },
            &mut work,
        );
        // An original construction refusal keeps priority over the footer.
        // Ordinary compiler verdicts still flush their pending observation.
        let has_primary_refusal = compiled.as_ref().is_err_and(|cause| match cause {
            ExprCompileFailure::Owner(cause) => {
                cause.is_primary_construction()
                    || matches!(
                        cause.failure_fact().class(),
                        FailureClass::Resource | FailureClass::Control
                    )
            }
            ExprCompileFailure::Control(_)
            | ExprCompileFailure::Creator(_)
            | ExprCompileFailure::Resource(_) => true,
        });
        if !has_primary_refusal {
            work.finish()?;
        }
        let compiled = compiled.map_err(expression_compile_error)?;
        drop(temporary);
        drop(retained);

        let declaration = compiled
            .root(root)
            .ok_or_else(|| invariant("required constant has no compiled root"))?;
        let Some(FunctionArgumentType::Value(ty)) =
            compiled.types().get(declaration.definition.index())
        else {
            return Err(invariant(
                "required constant root has no complete value type",
            ));
        };
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        if !ty
            .exactly_equals_checked_observed(prepared.value_type(), || work.step())
            .map_err(SqlConstantEvaluationError::from)?
        {
            return Err(invariant(
                "required constant root type differs from its SQL declaration",
            ));
        }
        work.finish()?;
        let output_storage = ty.typed_binding().layout().storage_lifetime();
        let input_layout = compiled
            .scope(RootScope::AllPorts)
            .ok_or_else(|| invariant("required constant has no singleton input layout"))?;
        if !compiled.scopes().is_empty() || !input_layout.schema().fields().is_empty() {
            return Err(invariant("required constant must have zero input ports"));
        }
        let input = RecordBatch::try_new_with_options(
            input_layout.schema().clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|_| invariant("required singleton input construction failed"))?;
        let kernel_control =
            CompileKernelControl::new(control, CompilePhase::FunctionSpecialization);
        let allocator: Arc<dyn AggregateStateAllocator> = resources.allocator.clone();
        let mut instance = CompiledExpressionInstance::try_new_with_allocator(
            compiled,
            root,
            &kernel_control,
            Some(allocator),
        )
        .map_err(SqlConstantEvaluationError::outer_domain)?;
        let selection = Selection::all(1);
        let values = instance
            .evaluate_scoped_runtime(
                &input,
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &kernel_control,
                resources,
            )
            .map_err(SqlConstantEvaluationError::outer_domain)?;
        let (selected, array, errors) = values.into_parts();
        if !selected.is_all() || selected.batch_rows() != 1 || array.len() != 1 {
            return Err(invariant(
                "required computation returned a different selected domain",
            ));
        }
        if let Some(error) = errors.into_vec().into_iter().next() {
            let required = RequiredExpressionRowError::try_new(root, selected, error)
                .map_err(SqlConstantEvaluationError::outer_domain)?;
            // Only the actual selected row-error channel is ordinary Data.
            // Outer evaluator failures below keep their separate fatal port.
            return Err(SqlConstantEvaluationError::from_domain(
                ExecutionFailure::from(required),
            ));
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        if !novarocks_type_contract::arrow_data_types_exact_observed::<SqlConstantEvaluationError>(
            array.data_type(),
            prepared.value_type().data_type(),
            || {
                work.step()?;
                Ok(())
            },
        )? {
            return Err(invariant(
                "required result differs from its complete frozen carrier",
            ));
        }
        // Materialize a new flat result under the original copy host invoice.
        // The copy author proves no nonempty input alias before dropping the
        // old operation/source lease; output Buffer custody carries ONLY its
        // actual new backing grant. No stock-based second payload charge is made.
        let mut indices_scope = resources.factory.begin_scope()?;
        indices_scope.request_layout(
            std::alloc::Layout::array::<u64>(1)
                .map_err(|_| CompileControlError::ResourceExhausted)?,
        )?;
        indices_scope.request_layout(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<UInt64Array>(),
            )
            .map_err(|cause| {
                CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
                    cause,
                ))
            })?,
        )?;
        indices_scope
            .with_creation(|creation| {
                novarocks_functions::selected_copy::preflight_uint64_indices_construction(
                    creation,
                    &kernel_control,
                )
            })
            .map_err(copy_construction_error)?;
        let mut index_values = Vec::new();
        index_values
            .try_reserve_exact(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        index_values.push(0_u64);
        let indices = novarocks_functions::selected_copy::CopyIndices::UInt64(Arc::new(
            UInt64Array::new(index_values.into(), None),
        ));
        let array = novarocks_functions::selected_copy::take_flat_owned_copy_in(
            array,
            indices,
            (
                resources.operation_storage(),
                indices_scope.storage_lifetime(),
            ),
            resources.allocator.clone(),
            &kernel_control,
        )
        .map_err(copy_operation_error)?
        .into_values();
        drop(indices_scope);
        let mut storage = resources.factory.begin_scope()?;
        // The required-target contract admits five flat physical carriers.
        // Fund the new field header and its owned name under the result scope.
        const OUTPUT_NAME: &str = "value";
        storage.request_layout(
            std::alloc::Layout::array::<u8>(OUTPUT_NAME.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?,
        )?;
        storage.request_layout(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<arrow::datatypes::Field>(),
            )
            .map_err(|cause| {
                CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
                    cause,
                ))
            })?,
        )?;
        let mut name = String::new();
        name.try_reserve_exact(OUTPUT_NAME.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        name.push_str(OUTPUT_NAME);
        let field = Arc::new(arrow::datatypes::Field::new(
            name,
            prepared.value_type().data_type().clone(),
            prepared.value_type().nullable(),
        ));
        let lifetime = storage
            .storage_lifetime()
            .retaining_observed::<ConstantCreationFailure<CompileControlError>>(
                &output_storage,
                |layout| {
                    storage
                        .request_layout(layout)
                        .map_err(ConstantCreationFailure::Creator)
                },
            )
            .map_err(constant_creation_error)?;
        storage
            .with_creation(|creation| {
                novarocks_functions::selected_copy::preflight_flat_arrow_structure_construction(
                    array.data_type(),
                    array.nulls().is_some(),
                    creation,
                    &mut |_| control.checkpoint(CompilePhase::FunctionSpecialization, 0),
                )
            })
            .map_err(copy_construction_error)?;
        work.flush()?;
        let data = array.to_data();
        work.flush()?;
        let mut admitted = 0usize;
        let pool = ConstantPool::try_new_owned_observed_in(
            field,
            prepared.value_type().clone(),
            data,
            self.policy,
            lifetime,
            &mut |facts| {
                let delta = facts
                    .allocation_request_bytes_upper_bound
                    .checked_sub(admitted)
                    .ok_or(CompileControlError::ResourceExhausted)?;
                storage.observe_opaque_upper_bound(delta)?;
                admitted = facts.allocation_request_bytes_upper_bound;
                Ok(())
            },
            &mut work,
        )
        .map_err(constant_creation_error)?;
        work.finish()?;
        pool.value(0).map_err(Into::into)
    }
}

/// This exact required-computation eligibility decision is distinct from an
/// installed primitive's capability refusal. Only the original effects gate
/// creates this production error; it grants no new calculation capability.
#[derive(Debug)]
pub(crate) struct RequiredConstantEligibilityFailure(PackageSemanticsError);
impl RequiredConstantEligibilityFailure {
    pub(crate) fn new() -> Self {
        Self(PackageSemanticsError::UnsupportedCapability(
            "required constants do not execute volatile, stateful or effectful calls".into(),
        ))
    }
}
impl fmt::Display for RequiredConstantEligibilityFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RequiredConstantEligibilityFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for RequiredConstantEligibilityFailure {
    fn failure_fact(&self) -> FailureFact {
        self.0.failure_fact()
    }
}

fn expression_compile_error(
    error: ExprCompileFailure<CompileControlError>,
) -> SqlConstantEvaluationError {
    match error {
        ExprCompileFailure::Control(cause) | ExprCompileFailure::Creator(cause) => cause.into(),
        other => SqlConstantEvaluationError::outer_domain(other),
    }
}

fn copy_construction_error(
    error: novarocks_functions::selected_copy::CopyConstructionError<CompileControlError>,
) -> SqlConstantEvaluationError {
    use novarocks_functions::selected_copy::CopyConstructionError;
    match error {
        CopyConstructionError::Copy(cause) => {
            SqlConstantEvaluationError::outer_domain(cause.into_kernel())
        }
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => SqlConstantEvaluationError::outer_domain(cause),
    }
}
fn copy_operation_error(
    error: novarocks_functions::selected_copy::CopyOperationError,
) -> SqlConstantEvaluationError {
    use novarocks_functions::selected_copy::CopyOperationError;
    match error {
        CopyOperationError::Preflight(cause) => {
            SqlConstantEvaluationError::outer_domain(cause.into_kernel())
        }
        CopyOperationError::Control(cause) => SqlConstantEvaluationError::outer_domain(cause),
        CopyOperationError::OriginalData(cause) => {
            SqlConstantEvaluationError::outer_domain(RequiredCopyData(cause))
        }
    }
}
/// Preserve the original copy diagnostic and its host-owned charge. This is a
/// genuine Arrow Data return, not text classification or a formatter copy.
#[derive(Debug)]
struct RequiredCopyData(novarocks_functions::selected_copy::OriginalCopyData);
impl fmt::Display for RequiredCopyData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RequiredCopyData {}
impl FailureProjection for RequiredCopyData {
    fn failure_fact(&self) -> FailureFact {
        novarocks_functions::DataFailureReason::InvalidValue.failure_fact()
    }
}

fn constant_creation_error(
    error: ConstantCreationFailure<CompileControlError>,
) -> SqlConstantEvaluationError {
    match error {
        ConstantCreationFailure::Constant(cause) => cause.into(),
        ConstantCreationFailure::Creator(cause) => cause.into(),
        ConstantCreationFailure::Resource(cause) => SqlConstantEvaluationError::outer_domain(cause),
        ConstantCreationFailure::SourceMetadata(cause) => {
            SqlConstantEvaluationError::outer_domain(cause)
        }
    }
}
fn invariant(message: &str) -> SqlConstantEvaluationError {
    SqlConstantEvaluationError::outer_domain(KernelFailure::InvalidProgram(KernelDiagnostic::new(
        message,
    )))
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
pub(crate) use test_support::test_required_constant_interpreter;
#[cfg(test)]
mod tests;
