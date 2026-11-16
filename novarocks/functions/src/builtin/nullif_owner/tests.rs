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
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters, ValueLogicalType,
};
fn owner(name: &str) -> NullifOwner {
    let (_, signatures) = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .expect("the actual nullif registry entry");
    let (declaration, resolver) =
        super::super::catalogue::scalar_definition_parts(name, &signatures, FunctionKind::Scalar)
            .unwrap();
    NullifOwner::new(name, declaration, resolver).unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
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
    owner: &'a NullifOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
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
fn nullif_owner_exact_flat_profiles_and_called_on_null_effects() {
    let profiles = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Utf8,
        DataType::Date32,
        DataType::Decimal128(9, 0),
        DataType::Decimal128(18, 2),
        DataType::Decimal128(38, -3),
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ];
    for ty in profiles {
        for nullable in [false, true] {
            let source = FunctionValueType::from_legacy_declared_projection(ty.clone(), nullable);
            let prepared = prepared_for_test("nullif", &[source.clone(), source]).unwrap();
            assert_eq!(prepared.contract().result_type().data_type().clone(), ty);
            assert!(prepared.contract().result_type().nullable());
            let effects = prepared.contract().effects();
            assert_eq!(effects.argument_control, ArgumentControl::Eager);
            assert_eq!(effects.null_behavior, FunctionNullBehavior::CalledOnNull);
            assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
        }
    }
}
#[test]
fn nullif_owner_rejects_unsupported_carriers_timezone_and_logical_identities() {
    let types = vec![
        DataType::Null,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::UInt8,
        DataType::UInt64,
        DataType::Date64,
        DataType::Decimal256(38, 2),
        DataType::FixedSizeBinary(16),
        DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        DataType::Struct(vec![Arc::new(Field::new("item", DataType::Int64, true))].into()),
    ];
    for ty in types {
        let source = FunctionValueType::from_legacy_declared_projection(ty, true);
        assert!(prepared_for_test("nullif", &[source.clone(), source]).is_err());
    }
    for identity in [
        ValueLogicalType::Json,
        ValueLogicalType::Variant,
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Uuid,
    ] {
        let carrier = match identity {
            ValueLogicalType::Json => DataType::Utf8,
            ValueLogicalType::Variant => DataType::LargeBinary,
            ValueLogicalType::LargeInt | ValueLogicalType::Uuid => DataType::FixedSizeBinary(16),
            ValueLogicalType::Hll
            | ValueLogicalType::Bitmap
            | ValueLogicalType::Object
            | ValueLogicalType::Percentile => DataType::Binary,
            ValueLogicalType::Physical => unreachable!(),
        };
        let source = FunctionValueType::try_with_logical_type(carrier, true, identity).unwrap();
        assert!(prepared_for_test("nullif", &[source.clone(), source]).is_err());
    }
}
