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
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters, ValueLogicalType,
};
fn owner(name: &str) -> Md5sumNumericOwner {
    let (declaration, resolver) = super::super::catalogue::dynamic_definition_parts(name).unwrap();
    Md5sumNumericOwner::new(name, declaration, resolver).unwrap()
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
        logical_argument_count: arguments.len(),
    }
}

fn input<'a>(
    owner: &'a Md5sumNumericOwner,
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

pub(crate) fn prepared_for_test(
    name: &str,
    sources: &[FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    if !operation(name) {
        return Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction,
        ));
    }
    let owner = owner(name);
    let arguments = sources.iter().cloned().map(argument).collect::<Vec<_>>();
    let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = sources
        .iter()
        .enumerate()
        .map(|(i, _)| Some(ExpressionUseId::new(i as u32 + 42)))
        .collect::<Vec<_>>();
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
fn md5sum_numeric_owner_exact_flat_profiles_and_called_on_null_contract() {
    let profiles = [
        DataType::Null,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
    ];
    for ty in profiles {
        let source = FunctionValueType::from_legacy_declared_projection(ty, true);
        let prepared = prepared_for_test("md5sum_numeric", std::slice::from_ref(&source)).unwrap();
        assert_eq!(
            prepared.contract().selected().argument_types.as_ref(),
            &[crate::FunctionArgumentType::Value(source)]
        );
        assert_eq!(
            prepared.contract().result_type().data_type().clone(),
            DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            prepared.contract().result_type().logical_type(),
            ValueLogicalType::LargeInt
        );
        assert!(prepared.contract().result_type().nullable());
        assert_eq!(
            prepared.contract().effects().argument_control,
            ArgumentControl::Eager
        );
        assert_eq!(
            prepared.contract().effects().null_behavior,
            FunctionNullBehavior::CalledOnNull
        );
        assert_eq!(
            prepared.contract().effects().own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert!(prepared.contract().effects().environment.is_empty());
    }
    let mut nominal = vec![
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    ];
    nominal.push(
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Variant,
        )
        .unwrap(),
    );
    for identity in [
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
    ] {
        nominal.push(
            FunctionValueType::try_with_logical_type(DataType::Binary, true, identity).unwrap(),
        );
    }
    for source in nominal {
        let prepared = prepared_for_test("md5sum_numeric", std::slice::from_ref(&source)).unwrap();
        assert_eq!(
            prepared.contract().selected().argument_types.as_ref(),
            &[crate::FunctionArgumentType::Value(source)]
        );
    }
    // The original byte concatenation permits independent heterogeneous profiles.
    let prepared = prepared_for_test(
        "md5sum_numeric",
        &[
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::new(DataType::Null, true),
        ],
    )
    .unwrap();
    assert_eq!(prepared.contract().selected().argument_types.len(), 3);
}

#[test]
fn md5sum_numeric_owner_rejects_unproven_display_shapes_and_preserves_public_arity() {
    for ty in [
        DataType::FixedSizeBinary(16),
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
    ] {
        assert!(
            prepared_for_test(
                "md5sum_numeric",
                &[FunctionValueType::from_legacy_declared_projection(ty, true)]
            )
            .is_err()
        );
    }
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    assert!(prepared_for_test("md5sum_numeric", &[largeint]).is_err());
    assert!(prepared_for_test("md5sum_numeric", &[]).is_err());
    assert!(
        prepared_for_test(
            "uninstalled_md5_alias",
            &[FunctionValueType::new(DataType::Utf8, true)]
        )
        .is_err()
    );
}
