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

//! Frontend projection of SQL's frozen fold facts to the shared compiled core.
//! SQL owns traversal and fold policy; Execution owns every calculated value.
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array};
use arrow::datatypes::{DataType, Decimal128Type, Decimal256Type, DecimalType};
use novarocks_expression::constant_node::{
    ConstantCarrierStep, ConstantConversion, ConstantNode, ConstantNodeFailure,
    ConstantNodeOutcome, evaluate_constant_node,
};
use novarocks_functions::{
    ConstantPool, ConstantValue, KernelDiagnostic, KernelFailure,
    validate_function_value_type_observed,
};
use novarocks_sql::compiler::{
    CarrierStep, ConversionPlan, FoldArg, FoldRequest, FrozenFoldIntrinsic, FrozenFoldNode,
    SqlConstantEvaluationError, SqlConstantEvaluator,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, ValueKind,
    ValueLogicalType, ValueTypeRef,
};
use std::sync::Arc;

#[derive(Debug)]
struct ExecutionConstantEvaluator;
static EXECUTION_CONSTANT_EVALUATOR: ExecutionConstantEvaluator = ExecutionConstantEvaluator;
pub(crate) fn constant_evaluator() -> &'static dyn SqlConstantEvaluator {
    &EXECUTION_CONSTANT_EVALUATOR
}

impl SqlConstantEvaluator for ExecutionConstantEvaluator {
    #[cfg(debug_assertions)]
    fn is_legacy_fold_reference(&self) -> bool {
        true
    }

    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let evaluated = (|| {
            let mut supported = fold_value_type_supported(&request.result_type, &mut work)?;
            for arg in &request.args {
                supported &= validate_argument(arg, &mut work)?;
            }
            let (arguments, result_for_entry) = match &request.kind {
                FrozenFoldNode::Call(call) => (call.arguments(), call.prepared_result_type()),
                _ => (request.args.as_slice(), &request.result_type),
            };
            if let FrozenFoldNode::Call(_) = &request.kind {
                supported &= fold_value_type_supported(result_for_entry, &mut work)?;
                for arg in arguments {
                    supported &= validate_argument(arg, &mut work)?;
                }
            }
            if !supported {
                return Ok(None);
            }
            // Preserve the established optional-fold boundary for two bare
            // NULL carriers. Typed NULL comparisons still use the shared core.
            if matches!(
                request.kind,
                FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::NullSafeEq { .. })
            ) && arguments.len() == 2
                && arguments
                    .iter()
                    .all(|arg| arg.value_type.data_type() == &DataType::Null)
            {
                return Ok(None);
            }
            if let FrozenFoldNode::Call(call) = &request.kind {
                let effects = call.prepared().call_contract().effects();
                if effects.value_stability.is_volatile()
                    || effects.instance_state
                        != novarocks_type_contract::FunctionInstanceState::None
                    || !effects.observable_effects.is_empty()
                {
                    return Ok(None);
                }
            }
            let Some(node) = project_node(&request.kind) else {
                return Ok(None);
            };
            let mut children = Vec::with_capacity(arguments.len());
            for arg in arguments {
                children.push(arg.value.clone());
                work.step()?;
            }
            work.flush()?;
            let outcome = evaluate_constant_node(
                node,
                &children,
                result_for_entry,
                control,
                CompilePhase::FunctionSpecialization,
            )
            .map_err(|error| match error {
                ConstantNodeFailure::Control(error) => SqlConstantEvaluationError::Control(error),
                ConstantNodeFailure::InvalidFrozenNode(message) => {
                    SqlConstantEvaluationError::outer_domain(KernelFailure::InvalidProgram(
                        KernelDiagnostic::new(message),
                    ))
                }
                ConstantNodeFailure::Kernel(error) => {
                    SqlConstantEvaluationError::outer_domain(error)
                }
                ConstantNodeFailure::Primitive(error) => {
                    SqlConstantEvaluationError::outer_domain(error)
                }
            })?;
            let output = match outcome {
                ConstantNodeOutcome::Value(output) => output,
                ConstantNodeOutcome::RowError(error) => {
                    work.flush()?;
                    return Err(SqlConstantEvaluationError::from_domain(error));
                }
                ConstantNodeOutcome::Declined(_) => return Ok(None),
            };
            work.flush()?;
            if output.len() != 1 {
                return Err(SqlConstantEvaluationError::outer_domain(
                    KernelFailure::InvalidProgram(KernelDiagnostic::new(&format!(
                        "constant folding produced {} rows, expected exactly 1",
                        output.len()
                    ))),
                ));
            }
            if !novarocks_type_contract::arrow_data_types_exact_observed::<
                SqlConstantEvaluationError,
            >(output.data_type(), request.result_type.data_type(), || {
                work.step()?;
                Ok(())
            })? {
                return Err(SqlConstantEvaluationError::outer_domain(
                    KernelFailure::InvalidProgram(KernelDiagnostic::new(
                        "constant folding output differs from its frozen carrier",
                    )),
                ));
            }
            if output.is_null(0) && !request.result_type.nullable() {
                return Ok(None);
            }
            if !utf8_output_fits(&output, &mut work)? {
                return Ok(None);
            }
            work.flush()?;
            let fits = decimal_output_fits(&output)?;
            work.flush()?;
            if !fits {
                return Ok(None);
            }
            let field = Arc::new(arrow::datatypes::Field::new(
                "literal",
                request.result_type.data_type().clone(),
                request.result_type.nullable(),
            ));
            let data = output.to_data();
            work.flush()?;
            let pool = ConstantPool::try_new(
                field,
                request.result_type.clone(),
                data,
                request.constant_policy,
                CompilePhase::FunctionSpecialization,
                control,
            )?;
            work.step()?;
            Ok(Some(pool.value(0)?))
        })();
        work.finish_result(evaluated, SqlConstantEvaluationError::from)
    }
}
fn validate_argument(
    arg: &FoldArg,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlConstantEvaluationError> {
    let supported = fold_value_type_supported(&arg.value_type, work)?;
    if !arg
        .value_type
        .exactly_equals_observed(arg.value.value_type(), || {
            work.step().map_err(SqlConstantEvaluationError::from)
        })?
    {
        return Err(novarocks_functions::ConstantError::Invalid(
            "fold constant source differs from its exact frozen value type",
        )
        .into());
    }
    work.step()?;
    Ok(supported)
}
fn project_carrier(step: &CarrierStep) -> ConstantCarrierStep<'_> {
    ConstantCarrierStep {
        target: step.target(),
        decimal_overflow_policy: step.decimal_overflow_policy(),
        allow_throw_exception: step.allow_throw_exception(),
    }
}
fn project_conversion(plan: &ConversionPlan) -> ConstantConversion<'_> {
    match plan {
        ConversionPlan::Identity => ConstantConversion::Identity,
        ConversionPlan::Carrier(step) => ConstantConversion::Carrier(project_carrier(step)),
        ConversionPlan::ValueConversion { call, tail } => ConstantConversion::ValueConversion {
            call: call.prepared().prepared(),
            call_result: call.prepared_result_type(),
            tail: tail.as_ref().map(project_carrier),
        },
    }
}
fn project_node(node: &FrozenFoldNode) -> Option<ConstantNode<'_>> {
    Some(match node {
        FrozenFoldNode::Call(call) => ConstantNode::Call(call.prepared().prepared()),
        FrozenFoldNode::Intrinsic(intrinsic) => match intrinsic {
            FrozenFoldIntrinsic::Arithmetic {
                operator,
                decimal_overflow_policy,
                allow_throw_exception,
            } => ConstantNode::Arithmetic {
                operator: *operator,
                decimal_overflow_policy: *decimal_overflow_policy,
                allow_throw_exception: *allow_throw_exception,
            },
            FrozenFoldIntrinsic::Comparison {
                operator,
                left,
                right,
            } => ConstantNode::Comparison {
                operator: *operator,
                left: project_conversion(left),
                right: project_conversion(right),
            },
            FrozenFoldIntrinsic::NullSafeEq { left, right } => ConstantNode::NullSafeEquality {
                left: project_conversion(left),
                right: project_conversion(right),
            },
            FrozenFoldIntrinsic::Conjunction => ConstantNode::Conjunction,
            FrozenFoldIntrinsic::Disjunction => ConstantNode::Disjunction,
            FrozenFoldIntrinsic::Not => ConstantNode::Not,
            FrozenFoldIntrinsic::Cast { plan } => {
                ConstantNode::Conversion(project_conversion(plan))
            }
            FrozenFoldIntrinsic::Negate | FrozenFoldIntrinsic::BitNot => return None,
        },
    })
}

// Inspect bytes before any StringArray::value call can form an invalid &str.
// Only malformed UTF8 preserves the legacy optional fold decline; invalid
// layout remains an ordinary evaluation error, not an Arrow-error fallback.
fn utf8_output_fits(
    output: &ArrayRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlConstantEvaluationError> {
    if output.is_null(0) {
        return Ok(true);
    }
    macro_rules! inspect {
        ($array:ty) => {{
            let array = output.as_any().downcast_ref::<$array>().ok_or_else(|| {
                SqlConstantEvaluationError::Evaluation("invalid UTF8 output class".into())
            })?;
            let offsets = array.value_offsets();
            let start = usize::try_from(offsets[0]).map_err(|_| {
                SqlConstantEvaluationError::Evaluation("invalid UTF8 output offset".into())
            })?;
            let end = usize::try_from(offsets[1]).map_err(|_| {
                SqlConstantEvaluationError::Evaluation("invalid UTF8 output offset".into())
            })?;
            let bytes = array.value_data().get(start..end).ok_or_else(|| {
                SqlConstantEvaluationError::Evaluation("invalid UTF8 output range".into())
            })?;
            // Account real bounded visits before the opaque UTF8 validator.
            for byte in bytes {
                std::hint::black_box(byte);
                work.step()?;
            }
            work.flush()?;
            let valid = std::str::from_utf8(bytes).is_ok();
            work.flush()?;
            Ok(valid)
        }};
    }
    match output.data_type() {
        DataType::Utf8 => inspect!(arrow::array::StringArray),
        DataType::LargeUtf8 => inspect!(arrow::array::LargeStringArray),
        _ => Ok(true),
    }
}

fn decimal_output_fits(output: &ArrayRef) -> Result<bool, String> {
    if output.is_null(0) {
        return Ok(true);
    }
    match output.data_type() {
        DataType::Decimal128(precision, _) => {
            let array = output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    "constant folding has an invalid Decimal128 result class".to_owned()
                })?;
            Ok(Decimal128Type::is_valid_decimal_precision(
                array.value(0),
                *precision,
            ))
        }
        DataType::Decimal256(precision, _) => {
            let array = output
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    "constant folding has an invalid Decimal256 result class".to_owned()
                })?;
            Ok(Decimal256Type::is_valid_decimal_precision(
                array.value(0),
                *precision,
            ))
        }
        _ => Ok(true),
    }
}

/// Preserve the existing fold coverage: only root LARGEINT may be nominal;
/// nested nominal values and physical FixedSizeBinary remain outside this port.
fn fold_value_type_supported(
    value_type: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlConstantEvaluationError> {
    work.step()?;
    value_type
        .logical_type()
        .validate_carrier(value_type.data_type())?;
    // The function owner supplies the actual type/metadata resource limits.
    // Its borrowed walk observes entries, not opaque cloning or kernel work.
    validate_function_value_type_observed(value_type, work)?;
    fn supported(
        value: &ValueTypeRef,
        root: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, SqlConstantEvaluationError> {
        work.step()?;
        let mut allowed = match value.kind() {
            ValueKind::Nominal(ValueLogicalType::LargeInt) => root,
            ValueKind::Nominal(_) => false,
            ValueKind::Scalar(DataType::FixedSizeBinary(_)) => false,
            _ => true,
        };
        for ordinal in 0..value.child_count() {
            work.step()?;
            let child = value
                .child_type_at(ordinal)
                .expect("immutable value node exposes every declared child");
            allowed &= supported(child, false, work)?;
        }
        Ok(allowed)
    }
    supported(value_type.value_type(), true, work)
}
