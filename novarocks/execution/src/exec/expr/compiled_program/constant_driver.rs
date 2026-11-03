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

//! A depth-one driver over the same Frame and completion owner as program uses.
use super::super::constant_node::*;
use super::*;
use novarocks_functions::{CastOperation, ConstantValue, FunctionResultType, FunctionValueType};
use novarocks_local_program::{
    ProgramPrimitiveError, prepare_arithmetic_node, prepare_cast_node, prepare_comparison_node,
    prepare_null_safe_comparison_node,
};
use novarocks_type_contract::{
    ArgumentControl, CompilePhase, FunctionInstanceState, FunctionVolatility, PureCompileControl,
    ValueLogicalType,
};

#[derive(Default)]
struct Recipes<'a> {
    arithmetic: Option<novarocks_functions::PreparedArithmeticRecipe>,
    cast: Option<novarocks_functions::PreparedCastRecipe>,
    comparison: Option<novarocks_functions::PreparedComparisonRecipe>,
    null_safe: Option<novarocks_functions::PreparedNullSafeComparisonRecipe>,
    call: Option<&'a PreparedPureKernel>,
}
impl NodeRecipes for Recipes<'_> {
    fn comparison_recipe(
        &self,
        _: ComparisonPosition,
    ) -> Option<&novarocks_functions::PreparedComparisonRecipe> {
        self.comparison.as_ref()
    }
    fn null_safe_comparison_recipe(
        &self,
    ) -> Option<&novarocks_functions::PreparedNullSafeComparisonRecipe> {
        self.null_safe.as_ref()
    }
    fn native_like_recipe(&self) -> Option<&novarocks_functions::PreparedNativeLikeRecipe> {
        None
    }
    fn native_inlist_recipe(&self) -> Option<&novarocks_functions::PreparedNativeInListRecipe> {
        None
    }
    fn native_negate_recipe(&self) -> Option<&novarocks_functions::PreparedNativeNegateRecipe> {
        None
    }
    fn native_bitnot_recipe(&self) -> Option<&novarocks_functions::PreparedNativeBitNotRecipe> {
        None
    }
    fn arithmetic_recipe(&self) -> Option<&novarocks_functions::PreparedArithmeticRecipe> {
        self.arithmetic.as_ref()
    }
    fn cast_recipe(&self) -> Option<&novarocks_functions::PreparedCastRecipe> {
        self.cast.as_ref()
    }
    fn prepared_call(&self) -> Option<&PreparedPureKernel> {
        self.call
    }
}
enum Refusal {
    Declined(DeclineReason),
    Failure(ConstantNodeFailure),
}
impl From<KernelFailure> for Refusal {
    fn from(error: KernelFailure) -> Self {
        Self::Failure(error.into())
    }
}
impl From<ProgramPrimitiveError> for Refusal {
    fn from(error: ProgramPrimitiveError) -> Self {
        use novarocks_functions::{
            ArithmeticPrepareError as A, CastPrepareError as C, ComparisonPrepareError as B,
        };
        match error {
            ProgramPrimitiveError::Control(error) => {
                Self::Failure(ConstantNodeFailure::Control(error))
            }
            ProgramPrimitiveError::Arithmetic(A::Unsupported)
            | ProgramPrimitiveError::Cast(C::Unsupported)
            | ProgramPrimitiveError::Comparison(B::Unsupported) => {
                Self::Declined(DeclineReason::CapabilityMissing)
            }
            ProgramPrimitiveError::Arithmetic(A::Kernel(error))
            | ProgramPrimitiveError::Cast(C::Kernel(error))
            | ProgramPrimitiveError::Comparison(B::Kernel(error)) => error.into(),
            ProgramPrimitiveError::Arithmetic(A::Control(error))
            | ProgramPrimitiveError::Cast(C::Control(error))
            | ProgramPrimitiveError::Comparison(B::Control(error)) => {
                Self::Failure(ConstantNodeFailure::Control(error))
            }
            error => Self::Failure(ConstantNodeFailure::Primitive(error)),
        }
    }
}
fn invalid_node(message: &'static str) -> Refusal {
    Refusal::Failure(ConstantNodeFailure::InvalidFrozenNode(message))
}
struct TypedValue {
    value: OwnedValue,
    ty: FunctionValueType,
}

fn call_shape(call: &PreparedPureKernel) -> Result<ControlShape, Refusal> {
    match call {
        PreparedPureKernel::Scalar(kernel) if kernel.instance_inline_allocation_bytes() != 0 => {
            return Err(Refusal::Declined(DeclineReason::OpaqueHostRequired));
        }
        PreparedPureKernel::Scalar(_) | PreparedPureKernel::ControlIntrinsic(_) => {}
        _ => return Err(Refusal::Declined(DeclineReason::OpaqueHostRequired)),
    }
    let effects = call.call_contract().effects();
    if effects.value_stability == FunctionVolatility::Volatile
        || effects.instance_state != FunctionInstanceState::None
        || !effects.observable_effects.is_empty()
    {
        return Err(Refusal::Declined(DeclineReason::EffectfulCall));
    }
    Ok(match effects.argument_control {
        ArgumentControl::Eager => ControlShape::Eager,
        ArgumentControl::If => ControlShape::If,
        ArgumentControl::Coalesce => ControlShape::Coalesce,
        ArgumentControl::NoArguments => ControlShape::NoArguments,
        ArgumentControl::TemporalSource(_) => {
            return Err(Refusal::Declined(DeclineReason::TemporalSource));
        }
        _ => return Err(invalid_node("unsupported frozen constant call control")),
    })
}
fn exact_call(
    call: &PreparedPureKernel,
    children: &[TypedValue],
    result: &FunctionValueType,
    work: &mut Work<'_, '_>,
) -> Result<(), Refusal> {
    admit_call_protocol(call_shape(call)?, call, result.data_type())?;
    let selected = call.call_contract().selected();
    let FunctionResultType::Scalar(expected_result) = &selected.result_type else {
        return Err(invalid_node("constant call must have a scalar result"));
    };
    if !expected_result.exactly_equals_observed(result, || work.step())?
        || selected.argument_types.len() != children.len()
    {
        return Err(invalid_node(
            "constant call differs from frozen result or arity",
        ));
    }
    for (expected, child) in selected.argument_types.iter().zip(children) {
        let FunctionArgumentType::Value(expected) = expected else {
            return Err(invalid_node("constant call cannot contain a lambda"));
        };
        // The frozen selected signature owns permitted NULL widening. Validate
        // the actual value against it instead of inventing a new signature.
        let selected_rows = Selection::all(1);
        match &child.value {
            OwnedValue::Constant(value) => validate_evaluated_argument_observed(
                EvaluatedArgument::Constant(value),
                selected_rows,
                expected,
                work.control,
            )?,
            OwnedValue::ConstantView(value) => validate_evaluated_argument_observed(
                value.argument(),
                selected_rows,
                expected,
                work.control,
            )?,
            OwnedValue::Selected(array, errors) => {
                let selected = SelectedValues::try_new_observed(
                    selected_rows,
                    child.ty.data_type(),
                    array.clone(),
                    errors.clone(),
                    || work.step(),
                )?;
                validate_evaluated_argument_observed(
                    EvaluatedArgument::SelectedColumn(&selected),
                    selected_rows,
                    expected,
                    work.control,
                )?;
            }
            _ => {
                return Err(invalid_node(
                    "constant intermediate escaped selected value form",
                ));
            }
        }
    }
    Ok(())
}

struct Driver<'a> {
    domain: Arc<input_source::ActualInvocationDomain>,
    control: &'a dyn PureCompileControl,
    next_key: u32,
}
impl Driver<'_> {
    fn complete(
        &mut self,
        kind: CompletionKind,
        recipes: &dyn NodeRecipes,
        shape: ControlShape,
        children: Vec<TypedValue>,
        result: &FunctionValueType,
        work: &mut Work<'_, '_>,
    ) -> Result<TypedValue, Refusal> {
        let key = self.next_key;
        self.next_key = self
            .next_key
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let mut frame = Frame::new(
            key,
            shape,
            result.data_type(),
            vec![0],
            self.domain.clone(),
            false,
            novarocks_functions::ScalarInvocationActivation::Activated,
            vec![0],
            work,
        )?;
        let arity = if shape == ControlShape::NoArguments {
            0
        } else {
            children.len()
        };
        let mut children: Vec<_> = children.into_iter().map(Some).collect();
        while let Some(ordinals) = frame.next_ordinals(shape, arity, true, 1, work)? {
            let ordinal = frame.next;
            frame.next += 1;
            let child = children
                .get_mut(ordinal)
                .and_then(Option::take)
                .ok_or_else(|| invalid("constant child ordinal is absent"))?;
            let (_rows, _domain, _activation) =
                child_context(&frame, shape, &ordinals, false, work)?;
            let value = if ordinals.is_empty() {
                complete_undemanded_leaf(child.ty.data_type(), work)?
            } else {
                child.value
            };
            frame.attach(
                Child { ordinals, value },
                recipes,
                shape,
                EvaluationDemand::Value,
                true,
                1,
                work,
                None,
            )?;
        }
        let value = complete_node::<KernelFailure, _>(
            &kind,
            &mut frame,
            recipes,
            shape,
            EvaluationDemand::Value,
            result,
            Selection::all(1),
            false,
            &mut BTreeMap::new(),
            None,
            work,
        )?;
        Ok(TypedValue {
            value,
            ty: result.clone(),
        })
    }
    fn compare(
        &mut self,
        children: Vec<TypedValue>,
        left: ConstantConversion<'_>,
        right: ConstantConversion<'_>,
        operator: Option<novarocks_type_contract::ComparisonOperator>,
        result: &FunctionValueType,
        work: &mut Work<'_, '_>,
    ) -> Result<TypedValue, Refusal> {
        let mut children = children.into_iter();
        let left = self.convert(
            children
                .next()
                .ok_or_else(|| invalid("missing comparison left child"))?,
            left,
            work,
        )?;
        let right = self.convert(
            children
                .next()
                .ok_or_else(|| invalid("missing comparison right child"))?,
            right,
            work,
        )?;
        work.flush()?;
        let (kind, recipes) = if let Some(operator) = operator {
            let comparison = prepare_comparison_node(operator, &left.ty, &right.ty, self.control)?;
            if comparison.nullable_result() && !result.nullable() {
                return Err(invalid_node("comparison result loses SQL NULL"));
            }
            (
                CompletionKind::Comparison { null_safe: false },
                Recipes {
                    comparison: Some(comparison),
                    ..Default::default()
                },
            )
        } else {
            if result.nullable() {
                return Err(invalid_node(
                    "null-safe comparison result must be nonnullable",
                ));
            }
            (
                CompletionKind::Comparison { null_safe: true },
                Recipes {
                    null_safe: Some(prepare_null_safe_comparison_node(
                        &left.ty,
                        &right.ty,
                        self.control,
                    )?),
                    ..Default::default()
                },
            )
        };
        if result.data_type() != &DataType::Boolean
            || result.logical_type() != ValueLogicalType::Physical
        {
            return Err(invalid_node("comparison result must be Physical Boolean"));
        }
        self.complete(
            kind,
            &recipes,
            ControlShape::Eager,
            vec![left, right],
            result,
            work,
        )
    }
    fn carrier(
        &mut self,
        child: TypedValue,
        step: ConstantCarrierStep<'_>,
        work: &mut Work<'_, '_>,
    ) -> Result<TypedValue, Refusal> {
        work.flush()?;
        let recipe = prepare_cast_node(
            CastOperation::Carrier,
            &child.ty,
            step.target,
            step.decimal_overflow_policy,
            step.allow_throw_exception,
            self.control,
        )?;
        work.flush()?;
        self.complete(
            CompletionKind::Cast,
            &Recipes {
                cast: Some(recipe),
                ..Default::default()
            },
            ControlShape::Eager,
            vec![child],
            step.target,
            work,
        )
    }
    fn convert(
        &mut self,
        child: TypedValue,
        conversion: ConstantConversion<'_>,
        work: &mut Work<'_, '_>,
    ) -> Result<TypedValue, Refusal> {
        match conversion {
            ConstantConversion::Identity => Ok(child),
            ConstantConversion::Carrier(step) => self.carrier(child, step, work),
            ConstantConversion::ValueConversion {
                call,
                call_result,
                tail,
            } => {
                let shape = call_shape(call)?;
                let children = vec![child];
                exact_call(call, &children, call_result, work)?;
                let value = self.complete(
                    CompletionKind::Call,
                    &Recipes {
                        call: Some(call),
                        ..Default::default()
                    },
                    shape,
                    children,
                    call_result,
                    work,
                )?;
                match tail {
                    Some(step) => self.carrier(value, step, work),
                    None => Ok(value),
                }
            }
        }
    }
}

pub(in crate::exec::expr::compiled_program) fn evaluate(
    node: ConstantNode<'_>,
    children: &[ConstantValue],
    result: &FunctionValueType,
    control: &dyn PureCompileControl,
    phase: CompilePhase,
) -> Result<ConstantNodeOutcome, ConstantNodeFailure> {
    let kernel_control = novarocks_functions::CompileKernelControl::new(control, phase);
    evaluate_with_kernel_control(node, children, result, control, &kernel_control)
}

// Both the compile adapter and the private failure probes use this exact entry
// lifecycle; the public port still accepts only PureCompileControl.
fn evaluate_with_kernel_control(
    node: ConstantNode<'_>,
    children: &[ConstantValue],
    result: &FunctionValueType,
    control: &dyn PureCompileControl,
    kernel_control: &dyn KernelEvaluationControl,
) -> Result<ConstantNodeOutcome, ConstantNodeFailure> {
    let observed = ObservedControl {
        original: kernel_control,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    work.flush().map_err(ConstantNodeFailure::from)?;
    let outcome = evaluate_inner(node, children, result, control, &mut work);
    match outcome {
        Ok(value) => {
            work.finish(Ok(())).map_err(ConstantNodeFailure::from)?;
            Ok(value)
        }
        Err(Refusal::Declined(reason)) => {
            work.finish(Ok(())).map_err(ConstantNodeFailure::from)?;
            Ok(ConstantNodeOutcome::Declined(reason))
        }
        // An originating outer error never starts a fallible footer. Normal
        // declines and row-data carriers above still finish the shared Work.
        Err(Refusal::Failure(error)) => Err(error),
    }
}
fn evaluate_inner(
    node: ConstantNode<'_>,
    children: &[ConstantValue],
    result: &FunctionValueType,
    control: &dyn PureCompileControl,
    work: &mut Work<'_, '_>,
) -> Result<ConstantNodeOutcome, Refusal> {
    admit_node_result(result.data_type())?;
    let batch = RecordBatch::try_new_with_options(
        Arc::new(arrow::datatypes::Schema::empty()),
        Vec::new(),
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|_| invalid("constant empty port could not be constructed"))?;
    let domain = Arc::new(input_source::ActualInvocationDomain::root(
        CompiledInputLoan::RecordBatch(&batch),
        Selection::all(1),
        work.control,
    )?);
    let mut driver = Driver {
        domain,
        control,
        next_key: 0,
    };
    let mut values = Vec::with_capacity(children.len());
    for child in children {
        values.push(TypedValue {
            value: complete_constant_leaf(
                child,
                child.value_type(),
                Selection::all(1),
                None,
                work,
            )?,
            ty: child.value_type().clone(),
        });
        work.step()?;
    }
    let require = |actual, expected| {
        if actual == expected {
            Ok(())
        } else {
            Err(invalid_node("constant node arity mismatch"))
        }
    };
    let value = match node {
        ConstantNode::Conversion(conversion) => {
            require(values.len(), 1)?;
            driver.convert(
                values
                    .pop()
                    .ok_or_else(|| invalid("missing conversion child"))?,
                conversion,
                work,
            )?
        }
        ConstantNode::Comparison {
            operator,
            left,
            right,
        } => {
            require(values.len(), 2)?;
            driver.compare(values, left, right, Some(operator), result, work)?
        }
        ConstantNode::NullSafeEquality { left, right } => {
            require(values.len(), 2)?;
            driver.compare(values, left, right, None, result, work)?
        }
        ConstantNode::Arithmetic {
            operator,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            require(values.len(), 2)?;
            work.flush()?;
            let recipe = prepare_arithmetic_node(
                operator,
                &values[0].ty,
                &values[1].ty,
                result,
                decimal_overflow_policy,
                allow_throw_exception,
                control,
            )?;
            driver.complete(
                CompletionKind::Arithmetic,
                &Recipes {
                    arithmetic: Some(recipe),
                    ..Default::default()
                },
                ControlShape::Eager,
                values,
                result,
                work,
            )?
        }
        ConstantNode::Conjunction | ConstantNode::Disjunction => {
            require(values.len(), 2)?;
            let shape = if matches!(node, ConstantNode::Conjunction) {
                ControlShape::Conjunction
            } else {
                ControlShape::Disjunction
            };
            driver.complete(
                CompletionKind::Boolean,
                &Recipes::default(),
                shape,
                values,
                result,
                work,
            )?
        }
        ConstantNode::Not => {
            require(values.len(), 1)?;
            if result.data_type() != &DataType::Boolean
                || result.logical_type() != ValueLogicalType::Physical
                || (!result.nullable() && values[0].ty.nullable())
            {
                return Err(invalid_node(
                    "NOT result differs from its frozen Boolean NULL contract",
                ));
            }
            if !novarocks_type_contract::supports_boolean_negation_value_type(&values[0].ty) {
                return Err(invalid_node(
                    "NOT operand is outside its frozen successful domain",
                ));
            }
            driver.complete(
                CompletionKind::Unary(super::super::unary::UnaryIntrinsic::Not),
                &Recipes::default(),
                ControlShape::Eager,
                values,
                result,
                work,
            )?
        }
        ConstantNode::Call(call) => {
            let shape = call_shape(call)?;
            exact_call(call, &values, result, work)?;
            driver.complete(
                CompletionKind::Call,
                &Recipes {
                    call: Some(call),
                    ..Default::default()
                },
                shape,
                values,
                result,
                work,
            )?
        }
    };
    if !value
        .ty
        .same_physical_binding_ignoring_root_nullability_observed(result, || work.step())?
        || (value.ty.nullable() && !result.nullable())
    {
        return Err(invalid_node(
            "constant result differs from its frozen value domain",
        ));
    }
    let value = value.value.into_value(Selection::all(1), work)?;
    let output = finish_root_value::<KernelFailure>(value, result, Selection::all(1), None, work)?;
    if let Some(error) = output.errors().first() {
        Ok(ConstantNodeOutcome::RowError(error.clone()))
    } else {
        Ok(ConstantNodeOutcome::Value(output.values().clone()))
    }
}

#[cfg(test)]
#[path = "constant_driver_failure_tests.rs"]
mod failure_tests;

#[cfg(test)]
#[path = "constant_recipe_boundary_tests.rs"]
mod recipe_boundary_tests;
