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

use super::*;
use crate::{
    FunctionArgument, FunctionSpecializationFailure, FunctionValueType, ScopedExpressionEffects,
    specialize_scalar,
};
use arrow_schema::{DataType, Field, TimeUnit};
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters, ValueLogicalType,
};
fn owner(name: &str) -> ExtremaOwner {
    // greatest/least are actual dynamic declarations, with no SQL fixed signatures.
    let (declaration, resolver) = super::super::catalogue::dynamic_definition_parts(name).unwrap();
    ExtremaOwner::new(name, declaration, resolver).unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    }
}

fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
        arguments,
        logical_argument_count: 2,
    }
}

fn input<'a>(
    owner: &'a ExtremaOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: (context()).into(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected,
        request: request(arguments),
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    }
}

fn argument(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}

pub(super) fn prepared_for_test(
    name: &str,
    sources: &[FunctionValueType; 2],
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    if !operation(name) {
        return Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction,
        ));
    }
    let owner = owner(name);
    let arguments = (*sources).clone().map(argument);
    let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [
        Some(ExpressionUseId::new(42)),
        Some(ExpressionUseId::new(43)),
    ];
    specialize_scalar(
        &owner,
        input(&owner, &selected, &arguments, &parameters, &uses),
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        crate::binding_test_control(),
    )
    .map(|call| call.into_prepared())
}

#[test]
fn extrema_owner_all_supported_exact_profiles_are_nullable_eager() {
    let profiles = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Date32,
        DataType::Decimal128(9, 0),
        DataType::Decimal128(38, -46),
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Second, Some("+02:00".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("+02:00".into())),
    ];
    for name in ["greatest", "least"] {
        for ty in &profiles {
            for nullable in [false, true] {
                let source =
                    FunctionValueType::from_legacy_declared_projection(ty.clone(), nullable);
                let prepared = prepared_for_test(name, &[source.clone(), source]).unwrap();
                let result = prepared.contract().result_type();
                let expected = if *ty == DataType::Date32 {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                } else {
                    ty.clone()
                };
                assert_eq!(result.data_type().clone(), expected);
                assert!(result.nullable());
                assert_eq!(
                    prepared.contract().effects().argument_control,
                    ArgumentControl::Eager
                );
                assert_eq!(
                    prepared.contract().effects().own_row_error,
                    FunctionIntrinsicRowError::NoRowError
                );
            }
        }
        let prepared = prepared_for_test(
            name,
            &[
                FunctionValueType::new(DataType::Decimal128(38, 0), true),
                FunctionValueType::new(DataType::Decimal128(38, 1), true),
            ],
        )
        .unwrap();
        assert_eq!(
            prepared.contract().result_type().data_type().clone(),
            DataType::Decimal256(39, 1)
        );
    }
}
#[test]
fn extrema_owner_rejects_actual_unsupported_numeric_and_json_result_domain() {
    for name in ["greatest", "least"] {
        for ty in [
            DataType::Boolean,
            DataType::Binary,
            DataType::UInt64,
            DataType::Decimal256(38, 2),
            DataType::FixedSizeBinary(16),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            DataType::Struct(vec![Field::new("item", DataType::Int64, true)].into()),
        ] {
            let source = FunctionValueType::from_legacy_declared_projection(ty, true);
            assert!(prepared_for_test(name, &[source.clone(), source]).is_err());
        }
        let json =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        // Existing v1 formats these legal JSON numeric texts as unquoted dates.
        // Preserving a Json result receipt would therefore assert a false domain.
        assert!(prepared_for_test(name, &[json.clone(), json]).is_err());
        let invalid = FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("invalid/zone".into())),
            true,
        );
        assert!(prepared_for_test(name, &[invalid.clone(), invalid]).is_err());
    }
}

#[test]
fn extrema_owner_json_source_with_declared_physical_utf8_result_is_supported() {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let physical = FunctionValueType::new(DataType::Utf8, true);
    for name in ["greatest", "least"] {
        for sources in [
            [json.clone(), physical.clone()],
            [physical.clone(), json.clone()],
        ] {
            let prepared = prepared_for_test(name, &sources).unwrap();
            assert_eq!(
                prepared.contract().result_type().logical_type(),
                ValueLogicalType::Physical
            );
            assert_eq!(
                prepared.contract().result_type().data_type().clone(),
                DataType::Utf8
            );
        }
    }
}

#[test]
fn extrema_owner_null_result_refusal_preserves_the_original_full_cast_error() {
    use arrow_array::{ArrayRef, NullArray};
    let expected =
        "math: failed to cast output: Cast error: Casting from Timestamp(µs) to Null not supported";
    for name in ["greatest", "least"] {
        let source = FunctionValueType::new(DataType::Null, true);
        let failure = prepared_for_test(name, &[source.clone(), source]).unwrap_err();
        assert!(
            failure
                .to_string()
                .contains("greatest/least has no v1 value result for an exact Null target")
        );
        for rows in [0, 5] {
            let arrays = [Arc::new(NullArray::new(rows)) as ArrayRef];
            let operation = if name == "greatest" {
                super::super::scalar_extrema::ExtremaOperation::Greatest
            } else {
                super::super::scalar_extrema::ExtremaOperation::Least
            };
            assert_eq!(
                super::super::scalar_extrema::evaluate_legacy(
                    operation,
                    &arrays,
                    rows,
                    Some(&DataType::Null)
                )
                .unwrap_err()
                .to_string(),
                expected
            );
        }
    }
}
