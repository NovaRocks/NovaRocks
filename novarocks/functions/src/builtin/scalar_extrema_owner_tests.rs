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
    let arguments = selected
        .argument_types
        .iter()
        .map(|argument_type| {
            let crate::FunctionArgumentType::Value(value) = argument_type else {
                panic!("extrema requires values");
            };
            argument(value.clone())
        })
        .collect::<Vec<_>>();
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
fn extrema_owner_exact_profiles_keep_source_nullability_and_eager_demand() {
    let profiles = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::Date32,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        DataType::Struct(vec![Field::new("item", DataType::Int64, true)].into()),
        DataType::Decimal128(9, 0),
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
                assert_eq!(result.data_type(), ty);
                assert_eq!(result.nullable(), nullable);
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
            DataType::Decimal128(38, 1)
        );
    }
}
#[test]
fn extrema_owner_rejects_unordered_domains_and_invalid_zones() {
    let entries = DataType::Struct(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into(),
    );
    let map = FunctionValueType::new(
        DataType::Map(Arc::new(Field::new("entries", entries, false)), false),
        true,
    );
    for name in ["greatest", "least"] {
        assert!(prepared_for_test(name, &[map.clone(), map.clone()]).is_err());
        let json = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Json,
        )
        .unwrap();
        assert!(prepared_for_test(name, &[json.clone(), json.clone()]).is_err());
        let text = FunctionValueType::new(DataType::Utf8, true);
        for mixed in [[json.clone(), text.clone()], [text, json]] {
            assert!(prepared_for_test(name, &mixed).is_err());
        }
        let invalid = FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("invalid/zone".into())),
            true,
        );
        assert!(prepared_for_test(name, &[invalid.clone(), invalid]).is_err());
    }
}

#[test]
fn extrema_owner_retains_unknown_result_and_applies_common_precision_without_rounding() {
    for name in ["greatest", "least"] {
        let unknown = FunctionValueType::new(DataType::Null, true);
        let prepared = prepared_for_test(name, &[unknown.clone(), unknown.clone()]).unwrap();
        assert_eq!(prepared.contract().result_type(), &unknown);
        let wide = FunctionValueType::new(DataType::Int64, false);
        let narrow = FunctionValueType::new(DataType::Int8, true);
        let prepared = prepared_for_test(name, &[narrow, wide.clone()]).unwrap();
        assert_eq!(
            prepared.contract().result_type(),
            &wide.with_nullable(true).unwrap()
        );
        assert_eq!(
            prepared.contract().selected().argument_types.as_ref(),
            [
                crate::FunctionArgumentType::Value(wide.with_nullable(true).unwrap()),
                crate::FunctionArgumentType::Value(wide),
            ]
        );
    }
}
