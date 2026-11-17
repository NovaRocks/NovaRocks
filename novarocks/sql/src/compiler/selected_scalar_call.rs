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

//! Shared scalar preparation and conversion authors for folding and lowering.
//! No physical definition, optimizer arena, Task, or runtime instance is owned here.

use super::{CarrierStep, SqlFunctionCatalog};
use crate::binding::SqlFunctionBinding;
use arrow::datatypes::DataType;
use novarocks_functions::{
    CallEffectInput, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingSelection, FunctionResultType, FunctionSpecializationFailure,
    PureCallPreparation, PureCallSpecialization, PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_type_contract::{
    ArgumentControl, ArithmeticOperator, ComparisonOperator, CompileControlError, ControlShape,
    DecimalOverflowPolicy, FunctionArgumentType, FunctionValueType, PureCompileControl,
};
use std::sync::Arc;

pub(crate) fn selected_scalar_control(
    functions: &dyn SqlFunctionCatalog,
    function: &novarocks_functions::FunctionId,
    kind: novarocks_functions::FunctionKind,
    selected: &FunctionBindingSelection,
    logical_argument_count: usize,
    control: &dyn PureCompileControl,
) -> Result<(PureKernelAbi, ArgumentControl), FunctionSpecializationFailure> {
    let declaration = functions.pure_overload_declaration_observed(
        function,
        kind,
        &selected.overload,
        control,
    )?;
    let abi = declaration.implementation().abi;
    let selected_control = if abi == PureKernelAbi::ScalarInvocationV1 {
        declaration
            .selected_argument_control_observed(function, selected, logical_argument_count, control)
            .map_err(FunctionSpecializationFailure::Binding)?
    } else {
        declaration.effects().argument_control
    };
    Ok((abi, selected_control))
}

/// Exact authored inputs before occurrence preparation. Scope-specific callers
/// supply evidence; this module alone projects it to the Functions call API.
pub(crate) struct SelectedScalarCallInput<'a> {
    pub context: novarocks_type_contract::ExpressionEffectContext,
    pub argument_uses: novarocks_functions::CallArgumentUses<'a>,
    pub function_id: &'a novarocks_functions::FunctionId,
    pub kind: novarocks_functions::FunctionKind,
    pub request: FunctionBindingRequest<'a>,
    pub environment: &'a [novarocks_type_contract::SemanticParameterRef],
    pub parameters: &'a novarocks_type_contract::SemanticParameters,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: novarocks_type_contract::CallProofScope,
}

pub(crate) fn prepare_selected_scalar_call_observed(
    functions: &dyn SqlFunctionCatalog,
    input: SelectedScalarCallInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    abi: PureKernelAbi,
    arguments: ScopedExpressionEffects,
    control: &dyn PureCompileControl,
) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
    let options = match abi {
        PureKernelAbi::ScalarV1 | PureKernelAbi::ScalarInvocationV1 => {
            PureCallPreparation::Scalar { arguments }
        }
        PureKernelAbi::ControlIntrinsicV1 => PureCallPreparation::ControlIntrinsic { arguments },
        _ => {
            return Err(FunctionSpecializationFailure::InvalidInput(
                "installed scalar has no supported scalar ABI",
            ));
        }
    };
    let call = CallEffectInput {
        context: input.context,
        argument_uses: input.argument_uses,
        function_id: input.function_id,
        kind: input.kind,
        selected: selected.as_ref(),
        request: input.request,
        environment: input.environment,
        parameters: input.parameters,
        decimal_overflow_policy: input.decimal_overflow_policy,
        proof_scope: input.proof_scope,
    };
    functions.prepare_fresh_selected(call, Arc::clone(&selected), options, control)
}

pub(crate) fn canonical_scalar_request_observed(
    functions: &dyn SqlFunctionCatalog,
    binding: &SqlFunctionBinding,
    logical_argument_count: usize,
    arguments: &[FunctionArgument],
    constraint: Option<&FunctionValueType>,
    control: &dyn PureCompileControl,
) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
    let original = binding.resolved();
    functions.select_exact_overload_observed(
        &original.function_id,
        original.kind,
        &original.selected.overload,
        FunctionBindingRequest {
            arguments,
            logical_argument_count,
            expected_result_type: constraint,
        },
        control,
    )
}

/// Bare NULL follows the original selected argument's complete declaration.
/// Both consumers create their own admitted constant through the same target.
pub(crate) fn canonical_null_target(
    actual: &FunctionValueType,
    expected: &FunctionArgumentType,
) -> Option<FunctionValueType> {
    if let FunctionArgumentType::Value(expected) = expected
        && *expected.data_type() != DataType::Null
        && *actual.data_type() == DataType::Null
    {
        Some(
            expected
                .clone()
                .with_nullable(true)
                .expect("validated root NULL contract"),
        )
    } else {
        None
    }
}

/// This plan owns conversion decisions before an actual expression occurrence exists.
/// Lowering emits it; folding prepares that same binding for its one constant use.
#[derive(Clone, Debug)]
pub(crate) enum PlannedConversion {
    Identity,
    Carrier(CarrierStep),
    ValueConversion {
        binding: SqlFunctionBinding,
        tail: Option<CarrierStep>,
    },
}
pub(crate) fn plan_conversion(
    functions: &dyn SqlFunctionCatalog,
    source: &FunctionValueType,
    target: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow_throw_exception: bool,
    control: &dyn PureCompileControl,
) -> Result<PlannedConversion, FunctionBindingError> {
    if source == target {
        return Ok(PlannedConversion::Identity);
    }
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let intermediate = if novarocks_type_contract::is_default_json_structure_cast(source, target) {
        None
    } else {
        novarocks_functions::builtin::value_conversion::conversion_intermediate_type(source, target)
            .map_err(|_| {
                FunctionBindingError::InvalidBinding("invalid value conversion domain".into())
            })?
    };
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let step = || CarrierStep {
        target: target.clone(),
        decimal_overflow_policy: policy,
        allow_throw_exception,
    };
    let Some(intermediate) = intermediate else {
        return Ok(PlannedConversion::Carrier(step()));
    };
    let argument = FunctionArgument::Value {
        value_type: source.clone(),
        constant: None,
    };
    let binding = functions.resolve_value_conversion_binding(&argument, &intermediate, control)?;
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        return Err(FunctionBindingError::InvalidBinding(
            "value conversion owner selected a relation result".into(),
        ));
    };
    if result != &intermediate
        || binding.selected.argument_types.as_ref() != [FunctionArgumentType::Value(source.clone())]
    {
        return Err(FunctionBindingError::InvalidBinding(
            "value conversion binding differs from its source and explicit target".into(),
        ));
    }
    let tail = (result != target).then(step);
    let binding = SqlFunctionBinding::new_with_conversion_constraint(
        binding,
        policy,
        intermediate,
        target.clone(),
    );
    Ok(PlannedConversion::ValueConversion { binding, tail })
}

pub(crate) fn comparison_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<FunctionValueType, novarocks_type_contract::ComparisonCoercionFailure> {
    novarocks_type_contract::wider_comparison_value_type(left, right)
}
pub(crate) fn comparison_target(
    common: &FunctionValueType,
    source: &FunctionValueType,
) -> FunctionValueType {
    common
        .clone()
        .with_nullable(source.nullable())
        .expect("validated root NULL contract")
}

pub(crate) const fn arithmetic_operator(op: crate::common::BinOp) -> Option<ArithmeticOperator> {
    use crate::common::BinOp;
    Some(match op {
        BinOp::Add => ArithmeticOperator::Add,
        BinOp::Sub => ArithmeticOperator::Subtract,
        BinOp::Mul => ArithmeticOperator::Multiply,
        BinOp::Div => ArithmeticOperator::Divide,
        BinOp::Mod => ArithmeticOperator::Modulo,
        _ => return None,
    })
}
pub(crate) const fn comparison_operator(op: crate::common::BinOp) -> Option<ComparisonOperator> {
    use crate::common::BinOp;
    Some(match op {
        BinOp::Eq => ComparisonOperator::Eq,
        BinOp::Ne => ComparisonOperator::Ne,
        BinOp::Lt => ComparisonOperator::Lt,
        BinOp::Le => ComparisonOperator::Le,
        BinOp::Gt => ComparisonOperator::Gt,
        BinOp::Ge => ComparisonOperator::Ge,
        _ => return None,
    })
}
pub(crate) const fn arithmetic_allows_throw(op: crate::common::BinOp) -> bool {
    arithmetic_operator(op).is_some()
}

pub(crate) fn scalar_shape(control: ArgumentControl, count: usize) -> Option<ControlShape> {
    Some(match control {
        ArgumentControl::Eager => ControlShape::Eager,
        ArgumentControl::TypeOnly => ControlShape::TypeOnly,
        ArgumentControl::NoArguments => ControlShape::NoArguments,
        ArgumentControl::If => ControlShape::If,
        ArgumentControl::Coalesce => ControlShape::Coalesce,
        ArgumentControl::SimpleCase | ArgumentControl::SearchedCase => {
            let simple = control == ArgumentControl::SimpleCase;
            let tail = count.checked_sub(usize::from(simple))?;
            ControlShape::Case {
                simple,
                arms: u32::try_from(tail / 2).ok()?,
                has_else: tail % 2 == 1,
            }
        }
        ArgumentControl::HigherOrder {
            body_ordinal,
            body_demand,
        } => ControlShape::HigherOrder {
            body_ordinal,
            body_demand,
        },
        ArgumentControl::TemporalSource(_)
        | ArgumentControl::Aggregate
        | ArgumentControl::Window
        | ArgumentControl::Table => return None,
    })
}

fn compile_specialization_error(error: FunctionSpecializationFailure) -> super::SqlCompileError {
    match error {
        FunctionSpecializationFailure::Control(c)
        | FunctionSpecializationFailure::Binding(FunctionBindingError::Control(c))
        | FunctionSpecializationFailure::Kernel(
            novarocks_functions::KernelFailure::CompileControl(c),
        ) => c.into(),
        other => super::SqlCompileError::CompilationSource(
            novarocks_failure_contract::SharedFailureCause::new(other),
        ),
    }
}
fn compile_binding_error(error: FunctionBindingError) -> super::SqlCompileError {
    error.into()
}
fn compile_flow_error(
    error: novarocks_type_contract::ExpressionControlFlowError,
) -> super::SqlCompileError {
    match error {
        novarocks_type_contract::ExpressionControlFlowError::Control(c) => c.into(),
        other => super::SqlCompileError::CompilationSource(
            novarocks_failure_contract::SharedFailureCause::new(other),
        ),
    }
}
struct SingleCallDefinitions(usize);
impl novarocks_type_contract::ExpressionDefinitionMembership<u32> for SingleCallDefinitions {
    fn definition_count(&self) -> usize {
        self.0
    }
    fn contains_definition(&self, id: u32) -> bool {
        (id as usize) < self.0
    }
}

/// Shape facts projected by the caller that owns the actual source definition.
/// The shared author below never examines values or infers source from equality.
pub(crate) struct EmittedScalarArgumentSource<'a> {
    pub value_type: &'a FunctionValueType,
    pub unchecked_literal: bool,
    pub checked_constant_reference: bool,
    pub immediate_function: Option<&'a novarocks_type_contract::FunctionId>,
}
pub(crate) fn source_argument_ordinal(
    function: &novarocks_type_contract::FunctionId,
    count: usize,
) -> Result<Option<usize>, &'static str> {
    match function.as_str() {
        "builtin.scalar/regexp_count/v1" if count == 2 => Ok(Some(1)),
        "builtin.scalar/regexp_count/v1" => Err("regexp_count source requires two arguments"),
        "builtin.scalar/to_base64/v1" if count == 1 => Ok(Some(0)),
        "builtin.scalar/to_base64/v1" => Err("to_base64 source requires one argument"),
        _ => Ok(None),
    }
}
pub(crate) fn scalar_argument_source(
    function: &novarocks_type_contract::FunctionId,
    count: usize,
    source: Option<EmittedScalarArgumentSource<'_>>,
) -> Result<super::FrozenFoldArgumentSource, &'static str> {
    use super::FrozenFoldArgumentSource as Source;
    let Some(_) = source_argument_ordinal(function, count)? else {
        return Ok(Source::Selected);
    };
    let source = source.ok_or("source-sensitive scalar argument is absent")?;
    if source.unchecked_literal {
        return Err(
            "source-sensitive scalar requires checked native-v1 source; unchecked Literal is not emitted",
        );
    }
    match function.as_str() {
        "builtin.scalar/regexp_count/v1" => Ok(Source::RegexpCountPattern(
            novarocks_type_contract::RegexpCountPatternSource::from_native_v1_emitted_shape(
                source.value_type,
                source.checked_constant_reference,
            )?,
        )),
        "builtin.scalar/to_base64/v1" => {
            if source.value_type.logical_type()
                != novarocks_type_contract::ValueLogicalType::Physical
                || *source.value_type.data_type() != DataType::Utf8
            {
                return Err("to_base64 emitted source is not exact Physical Utf8");
            }
            let name = source
                .immediate_function
                .map(novarocks_type_contract::native_v1_function_name)
                .transpose()
                .map_err(|_| "to_base64 immediate function has no native-v1 identity")?;
            Ok(Source::ToBase64Bytes(
                novarocks_type_contract::ToBase64ByteSource::from_immediate_native_function_name(
                    name,
                ),
            ))
        }
        _ => unreachable!("source ordinal identifies one installed scalar source contract"),
    }
}

/// Prepare one constant call with the same exact operational request and owner
/// as lowering. The request retains its original admitted inputs; the frozen
/// call separately retains canonical runtime inputs, with bare NULL re-admitted
/// through the same target rule as lowering. Ordinary CV backings stay shared.
pub(crate) fn freeze_fold_call(
    functions: &dyn SqlFunctionCatalog,
    binding: &SqlFunctionBinding,
    source_args: &[super::FoldArg],
    policy: novarocks_functions::ConstantPolicy,
    capture_constants: bool,
    control: &dyn PureCompileControl,
) -> Result<Option<super::FrozenFoldCall>, super::SqlCompileError> {
    use super::{FrozenFoldArgumentSource as Source, FrozenFoldCall};
    use novarocks_type_contract::{
        CallProofScope, CompilePhase, EvaluationDemand, EvaluationRegionId, ExpressionControlFlow,
        ExpressionEffectContext, ExpressionEvaluationRegion, ExpressionInvocation, ExpressionUseId,
        RegionGuard, SemanticParameters, control_argument_semantics,
    };
    let mut canonical_args = Vec::new();
    canonical_args
        .try_reserve_exact(source_args.len())
        .map_err(|source| {
            super::SqlCompileError::CompilationSource(
                novarocks_failure_contract::SharedFailureCause::new(
                    novarocks_functions::allocation_failure::AllocationRefusal::new(
                        "frozen fold canonical arguments",
                        source,
                    ),
                ),
            )
        })?;
    for argument in source_args {
        canonical_args.push(argument.clone());
        control.checkpoint(CompilePhase::FunctionSpecialization, 1)?;
    }
    let args = canonical_args.as_mut_slice();
    let (installed_abi, installed_control) = selected_scalar_control(
        functions,
        &binding.function_id,
        binding.kind,
        &binding.selected,
        binding.logical_argument_count,
        control,
    )
    .map_err(compile_specialization_error)?;
    if !matches!(
        installed_abi,
        PureKernelAbi::ScalarV1
            | PureKernelAbi::ScalarInvocationV1
            | PureKernelAbi::ControlIntrinsicV1
    ) || matches!(installed_control, ArgumentControl::TemporalSource(_))
    {
        return Ok(None);
    }
    if args.len() != binding.selected.argument_types.len() {
        return Err(super::SqlCompileError::InvalidRequest(
            "fold call differs from its exact selected arity".into(),
        ));
    }
    let mut request_args = Vec::new();
    request_args
        .try_reserve_exact(args.len())
        .map_err(|source| {
            super::SqlCompileError::CompilationSource(
                novarocks_failure_contract::SharedFailureCause::new(
                    novarocks_functions::allocation_failure::AllocationRefusal::new(
                        "frozen fold request arguments",
                        source,
                    ),
                ),
            )
        })?;
    for (arg, expected) in args.iter_mut().zip(binding.selected.argument_types.iter()) {
        if let Some(target) = canonical_null_target(&arg.value_type, expected) {
            arg.value = crate::constant::admit_syntax_constant(
                &crate::common::LiteralValue::Null,
                &target,
                policy,
                control,
            )?;
            arg.value_type = target;
        }
        request_args.push(FunctionArgument::Value {
            value_type: arg.value_type.clone(),
            constant: capture_constants.then(|| arg.value.clone()),
        });
        control.checkpoint(CompilePhase::FunctionSpecialization, 1)?;
    }
    let selected = canonical_scalar_request_observed(
        functions,
        binding,
        binding.logical_argument_count,
        &request_args,
        binding.result_constraint(),
        control,
    )
    .map_err(compile_binding_error)?;
    let (abi, selected_control) = selected_scalar_control(
        functions,
        &binding.function_id,
        binding.kind,
        &selected,
        binding.logical_argument_count,
        control,
    )
    .map_err(compile_specialization_error)?;
    if matches!(selected_control, ArgumentControl::TemporalSource(_)) {
        return Ok(None);
    }
    let shape = scalar_shape(selected_control, args.len()).ok_or_else(|| {
        super::SqlCompileError::InvalidRequest(
            "installed scalar owner has no scalar control shape".into(),
        )
    })?;
    let type_only = matches!(shape, ControlShape::TypeOnly | ControlShape::NoArguments);
    let root_use = ExpressionUseId::new(0);
    let root_region = EvaluationRegionId::new(0);
    let context = ExpressionEffectContext {
        use_id: root_use,
        region: root_region,
        demand: EvaluationDemand::Value,
    };
    let mut domains = vec![ExpressionEvaluationRegion {
        id: root_region,
        parent: None,
        guard: None,
    }];
    let mut uses = vec![ExpressionInvocation {
        context,
        definition: 0u32,
        control: shape,
        arguments: Box::default(),
    }];
    let mut argument_uses = Vec::new();
    let mut runtime_children = Vec::new();
    for ordinal in 0..args.len() {
        control.checkpoint(CompilePhase::FunctionSpecialization, 1)?;
        if type_only {
            argument_uses.push(None);
            continue;
        }
        let id = u32::try_from(ordinal + 1).map_err(|_| CompileControlError::ResourceExhausted)?;
        let use_id = ExpressionUseId::new(id);
        let (demand, guard) =
            control_argument_semantics(shape, args.len(), ordinal, EvaluationDemand::Value)
                .map_err(compile_flow_error)?;
        let domain = if let Some(kind) = guard {
            let domain = EvaluationRegionId::new(id);
            domains.push(ExpressionEvaluationRegion {
                id: domain,
                parent: Some(root_region),
                guard: Some(RegionGuard {
                    owner: root_use,
                    kind,
                }),
            });
            domain
        } else {
            root_region
        };
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                region: domain,
                demand,
            },
            definition: id,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        runtime_children.push(use_id);
        argument_uses.push(Some(use_id));
    }
    uses[0].arguments = runtime_children.into_boxed_slice();
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        &SingleCallDefinitions(args.len() + 1),
        CompilePhase::FunctionSpecialization,
        control,
    )
    .map_err(compile_flow_error)?;
    let mut children = ScopedExpressionEffects::pure_value(context);
    if !type_only {
        for (ordinal, child) in flow.uses()[&root_use].arguments.iter().enumerate() {
            children = children
                .join_control_argument(
                    ScopedExpressionEffects::pure_value(flow.uses()[child].context),
                    &flow,
                    ordinal,
                )
                .map_err(|e| super::SqlCompileError::InvalidRequest(e.to_string()))?;
            control.checkpoint(CompilePhase::FunctionSpecialization, 1)?;
        }
    }
    let source_ordinal = source_argument_ordinal(&binding.function_id, args.len())
        .map_err(|e| super::SqlCompileError::InvalidRequest(e.into()))?;
    let source = scalar_argument_source(
        &binding.function_id,
        args.len(),
        source_ordinal.map(|ordinal| EmittedScalarArgumentSource {
            value_type: &args[ordinal].value_type,
            unchecked_literal: false,
            checked_constant_reference: capture_constants,
            immediate_function: None,
        }),
    )
    .map_err(|e| super::SqlCompileError::InvalidRequest(e.into()))?;
    let argument_sources = match source {
        Source::Selected => novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        Source::RegexpCountPattern(source) => {
            novarocks_functions::CallArgumentUses::RegexpCountPattern {
                source,
                channels: &argument_uses,
            }
        }
        Source::ToBase64Bytes(source) => novarocks_functions::CallArgumentUses::ToBase64Bytes {
            source,
            channels: &argument_uses,
        },
    };
    let parameters = SemanticParameters::default();
    let input = SelectedScalarCallInput {
        context,
        argument_uses: argument_sources,
        function_id: &binding.function_id,
        kind: binding.kind,
        request: FunctionBindingRequest {
            arguments: &request_args,
            logical_argument_count: binding.logical_argument_count,
            expected_result_type: binding.result_constraint(),
        },
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: binding.decimal_overflow_policy(),
        proof_scope: CallProofScope::Region(root_region),
    };
    let prepared = prepare_selected_scalar_call_observed(
        functions,
        input,
        Arc::clone(&selected),
        abi,
        children,
        control,
    )
    .map_err(compile_specialization_error)?;
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(super::SqlCompileError::InvalidRequest(
            "fold scalar selected a relation result".into(),
        ));
    };
    Ok(Some(FrozenFoldCall {
        binding: binding.clone(),
        selected: Arc::clone(&selected),
        prepared,
        prepared_result_type: result.clone(),
        source,
        arguments: canonical_args.into_boxed_slice(),
    }))
}

pub(crate) fn freeze_conversion(
    functions: &dyn SqlFunctionCatalog,
    source: &super::FoldArg,
    target: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<super::ConversionPlan, super::SqlCompileError> {
    use super::ConversionPlan;
    match plan_conversion(
        functions,
        &source.value_type,
        target,
        policy,
        allow,
        control,
    )
    .map_err(compile_binding_error)?
    {
        PlannedConversion::Identity => Ok(ConversionPlan::Identity),
        PlannedConversion::Carrier(step) => Ok(ConversionPlan::Carrier(step)),
        PlannedConversion::ValueConversion { binding, tail } => {
            let args = [source.clone()];
            let call =
                freeze_fold_call(functions, &binding, &args, constant_policy, false, control)?
                    .ok_or_else(|| {
                        super::SqlCompileError::InvalidRequest(
                            "value conversion unexpectedly requires temporal sources".into(),
                        )
                    })?;
            Ok(ConversionPlan::ValueConversion { call, tail })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{FoldArg, FrozenFoldArgumentSource};
    use super::*;
    use crate::common::LiteralValue;

    fn control() -> &'static dyn PureCompileControl {
        crate::optimizer::test_optimizer_control()
    }
    fn arg(value: LiteralValue, ty: FunctionValueType) -> FoldArg {
        FoldArg {
            value: crate::constant::admit_syntax_constant(
                &value,
                &ty,
                crate::constant::test_constant_policy(),
                control(),
            )
            .unwrap(),
            value_type: ty,
        }
    }
    fn binding(name: &str, args: &[FoldArg]) -> SqlFunctionBinding {
        let arguments = args
            .iter()
            .map(|arg| FunctionArgument::Value {
                value_type: arg.value_type.clone(),
                constant: Some(arg.value.clone()),
            })
            .collect::<Vec<_>>();
        let resolved = crate::functions::builtin_sql_function_catalog()
            .resolve_scalar_binding(name, &arguments, control())
            .unwrap();
        SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::ReportError)
    }

    #[test]
    fn canonical_scalar_fold_reuses_exact_selection_and_retains_admitted_constant() {
        let args = [arg(
            LiteralValue::Int(-7),
            FunctionValueType::new(DataType::Int64, false),
        )];
        let original = args[0].value.clone();
        let binding = binding("abs", &args);
        let catalog = crate::functions::builtin_sql_function_catalog();
        let call = freeze_fold_call(
            catalog,
            &binding,
            &args,
            crate::constant::test_constant_policy(),
            true,
            control(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(call.binding().function_id, binding.function_id);
        assert_eq!(call.binding().selected.overload, binding.selected.overload);
        assert_eq!(
            args[0].value.pool().backing_identity(),
            original.pool().backing_identity()
        );
        assert_eq!(args[0].value.ordinal(), original.ordinal());
        let request = [FunctionArgument::Value {
            value_type: args[0].value_type.clone(),
            constant: Some(args[0].value.clone()),
        }];
        let selected = canonical_scalar_request_observed(
            catalog,
            &binding,
            1,
            &request,
            binding.result_constraint(),
            control(),
        )
        .unwrap();
        assert_eq!(call.selected().as_ref(), selected.as_ref());
        assert_eq!(
            call.prepared().call_contract().selected(),
            selected.as_ref()
        );
        assert_eq!(
            call.prepared().call_contract().decimal_overflow_policy(),
            DecimalOverflowPolicy::ReportError
        );
        assert_eq!(call.source(), FrozenFoldArgumentSource::Selected);
    }

    #[test]
    fn canonical_null_uses_lowering_target_for_both_preparation_and_runtime_argument() {
        let args = [arg(
            LiteralValue::Null,
            FunctionValueType::new(DataType::Null, true),
        )];
        let original = args[0].value.clone();
        let binding = binding("lower", &args);
        let target =
            canonical_null_target(&args[0].value_type, &binding.selected.argument_types[0])
                .expect("LOWER authors typed UTF8 NULL");
        let call = freeze_fold_call(
            crate::functions::builtin_sql_function_catalog(),
            &binding,
            &args,
            crate::constant::test_constant_policy(),
            true,
            control(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(args[0].value.value_type().data_type(), &DataType::Null);
        assert_eq!(
            args[0].value.pool().backing_identity(),
            original.pool().backing_identity()
        );
        let args = call.arguments();
        assert_eq!(args[0].value_type, target);
        assert_eq!(args[0].value.value_type(), &target);
        assert_eq!(target.data_type(), &DataType::Utf8);
        assert_ne!(
            original.pool().backing_identity(),
            args[0].value.pool().backing_identity()
        );
        assert_eq!(
            call.selected().argument_types.as_ref(),
            &[FunctionArgumentType::Value(target)]
        );
        assert!(
            args[0]
                .value
                .is_null_observed(novarocks_type_contract::CompilePhase::Validate, control())
                .unwrap()
        );
    }

    #[test]
    fn comparison_conversions_keep_each_side_nullability_and_late_output_null_policy() {
        use arrow::datatypes::TimeUnit;
        let catalog = crate::functions::builtin_sql_function_catalog();
        for (left, right) in [
            (
                FunctionValueType::new(DataType::Date32, false),
                FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), true),
            ),
            (
                FunctionValueType::new(DataType::Decimal128(5, 2), false),
                FunctionValueType::new(DataType::Decimal128(12, 4), true),
            ),
            (
                FunctionValueType::new(DataType::Int32, false),
                FunctionValueType::new(DataType::Decimal128(12, 4), true),
            ),
        ] {
            let common = comparison_type(&left, &right).unwrap();
            for source in [&left, &right] {
                let target = comparison_target(&common, source);
                assert_eq!(target.nullable(), source.nullable());
                match plan_conversion(
                    catalog,
                    source,
                    &target,
                    DecimalOverflowPolicy::OutputNull,
                    true,
                    control(),
                )
                .unwrap()
                {
                    PlannedConversion::Identity => assert_eq!(source, &target),
                    PlannedConversion::Carrier(step) => {
                        assert_eq!(step.target(), &target);
                        assert_eq!(
                            step.decimal_overflow_policy(),
                            DecimalOverflowPolicy::OutputNull
                        );
                        assert!(step.allow_throw_exception());
                    }
                    PlannedConversion::ValueConversion { .. } => {
                        panic!("physical comparison requires only carrier conversion")
                    }
                }
            }
        }
    }

    #[test]
    fn constant_control_calls_keep_the_installed_guard_shape() {
        let catalog = crate::functions::builtin_sql_function_catalog();
        for (name, args, expected) in [
            (
                "if",
                vec![
                    arg(
                        LiteralValue::Bool(true),
                        FunctionValueType::new(DataType::Boolean, false),
                    ),
                    arg(
                        LiteralValue::Int(4),
                        FunctionValueType::new(DataType::Int64, false),
                    ),
                    arg(
                        LiteralValue::Int(8),
                        FunctionValueType::new(DataType::Int64, false),
                    ),
                ],
                ArgumentControl::If,
            ),
            (
                "coalesce",
                vec![
                    arg(
                        LiteralValue::Null,
                        FunctionValueType::new(DataType::Int64, true),
                    ),
                    arg(
                        LiteralValue::Int(4),
                        FunctionValueType::new(DataType::Int64, false),
                    ),
                ],
                ArgumentControl::Coalesce,
            ),
        ] {
            let binding = binding(name, &args);
            let call = freeze_fold_call(
                catalog,
                &binding,
                &args,
                crate::constant::test_constant_policy(),
                true,
                control(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                call.prepared().call_contract().effects().argument_control,
                expected
            );
        }
    }
    #[test]
    fn emitted_source_author_keeps_regexp_literal_and_base64_immediate_identity() {
        let utf8 = FunctionValueType::new(DataType::Utf8, false);
        let regexp =
            novarocks_type_contract::FunctionId::try_new("builtin.scalar/regexp_count/v1").unwrap();
        let base64 =
            novarocks_type_contract::FunctionId::try_new("builtin.scalar/to_base64/v1").unwrap();
        let encryption =
            novarocks_type_contract::FunctionId::try_new("builtin.scalar/aes_encrypt/v1").unwrap();
        let fact = |constant, immediate| EmittedScalarArgumentSource {
            value_type: &utf8,
            unchecked_literal: false,
            checked_constant_reference: constant,
            immediate_function: immediate,
        };
        assert_eq!(
            scalar_argument_source(&regexp, 2, Some(fact(true, None))).unwrap(),
            FrozenFoldArgumentSource::RegexpCountPattern(
                novarocks_type_contract::RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent
            )
        );
        assert_eq!(
            scalar_argument_source(&regexp, 2, Some(fact(false, None))).unwrap(),
            FrozenFoldArgumentSource::RegexpCountPattern(
                novarocks_type_contract::RegexpCountPatternSource::Dynamic
            )
        );
        assert_eq!(
            scalar_argument_source(&base64, 1, Some(fact(true, None))).unwrap(),
            FrozenFoldArgumentSource::ToBase64Bytes(
                novarocks_type_contract::ToBase64ByteSource::Ordinary
            )
        );
        assert_eq!(
            scalar_argument_source(&base64, 1, Some(fact(false, Some(&encryption)))).unwrap(),
            FrozenFoldArgumentSource::ToBase64Bytes(
                novarocks_type_contract::ToBase64ByteSource::NativeV1EncryptionLatin1
            )
        );
    }

    #[test]
    fn folded_call_preparation_preserves_first_control_refusal_without_callbacks_afterward() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Refusal {
            at: usize,
            cause: CompileControlError,
            calls: AtomicUsize,
        }
        impl PureCompileControl for Refusal {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert!(units <= 256);
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                assert!(call <= self.at, "callback after the original refusal");
                if call == self.at {
                    Err(self.cause.clone())
                } else {
                    Ok(())
                }
            }
        }
        let args = [arg(
            LiteralValue::Int(-7),
            FunctionValueType::new(DataType::Int64, false),
        )];
        let binding = binding("abs", &args);
        let catalog = crate::functions::builtin_sql_function_catalog();
        // First count this same preparation using an unreachable refusal point.
        let count = Refusal {
            at: usize::MAX,
            cause: CompileControlError::Cancelled,
            calls: AtomicUsize::new(0),
        };
        freeze_fold_call(
            catalog,
            &binding,
            &args,
            crate::constant::test_constant_policy(),
            true,
            &count,
        )
        .unwrap()
        .unwrap();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..count.calls.load(Ordering::SeqCst) {
                let refusal = Refusal {
                    at,
                    cause: cause.clone(),
                    calls: AtomicUsize::new(0),
                };
                let error = freeze_fold_call(
                    catalog,
                    &binding,
                    &args,
                    crate::constant::test_constant_policy(),
                    true,
                    &refusal,
                )
                .unwrap_err();
                assert_eq!(error, super::super::SqlCompileError::from(cause.clone()));
                assert_eq!(refusal.calls.load(Ordering::SeqCst), at + 1);
            }
        }
    }
}
