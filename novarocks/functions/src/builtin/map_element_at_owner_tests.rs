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
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters, ValueLogicalType,
};
fn owner(name: &str) -> MapElementAtOwner {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .unwrap()
        .1;
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        name,
        &signatures,
        novarocks_type_contract::FunctionKind::Scalar,
    )
    .unwrap();
    MapElementAtOwner::new(name, declaration, resolver).unwrap()
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
        logical_argument_count: arguments.len(),
    }
}

fn input<'a>(
    owner: &'a MapElementAtOwner,
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

pub(crate) fn prepared_for_test(
    name: &str,
    sources: &[FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    prepared_with_control(name, sources, crate::binding_test_control())
}
pub(crate) fn prepared_with_control(
    name: &str,
    sources: &[FunctionValueType],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    if !operation(name) {
        return Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction,
        ));
    }
    let owner = owner(name);
    let arguments = sources.iter().cloned().map(argument).collect::<Vec<_>>();
    let selected = Arc::new(owner.resolve(request(&arguments), control)?);
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
        control,
    )
    .map(|call| call.into_prepared())
}

#[test]
fn map_lookup_real_public_arity_and_nullable_result_match_original_binding() {
    let fields = vec![
        Arc::new(Field::new("key", DataType::Int32, false)),
        Arc::new(Field::new("value", DataType::Utf8, true)),
    ];
    let map = FunctionValueType::from_legacy_declared_projection(
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(fields.into()),
                false,
            )),
            false,
        ),
        true,
    );
    let key = FunctionValueType::new(DataType::Int32, true);
    let p = prepared_for_test("__map_element_at", &[map.clone(), key.clone()]).unwrap();
    assert_eq!(
        p.contract().result_type(),
        &FunctionValueType::new(DataType::Utf8, true)
    );
    assert_eq!(
        p.contract().effects().null_behavior,
        FunctionNullBehavior::CalledOnNull
    );
    assert_eq!(
        p.contract().effects().own_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    assert!(
        prepared_for_test(
            "__map_element_at",
            &[map, key, FunctionValueType::new(DataType::Boolean, true)]
        )
        .is_err()
    );
}

#[test]
fn map_lookup_null_result_profile_preserves_its_exact_type_for_both_source_nullabilities() {
    for nullable in [false, true] {
        let fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Null, true)),
        ];
        let map = FunctionValueType::from_legacy_declared_projection(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(fields.into()),
                    false,
                )),
                false,
            ),
            nullable,
        );
        let prepared = prepared_for_test(
            "__map_element_at",
            &[map, FunctionValueType::new(DataType::Int32, nullable)],
        )
        .unwrap();
        assert_eq!(
            prepared.contract().result_type(),
            &FunctionValueType::new(DataType::Null, true)
        );
    }
}

pub(crate) fn uninhabited_map(value: &FunctionValueType, sorted: bool) -> FunctionValueType {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let key = FunctionValueType::new(DataType::Null, false);
    let fields = vec![
        Field::new("declared-key", DataType::Null, false),
        Field::new(
            "declared-value",
            value.data_type().clone(),
            value.nullable(),
        )
        .with_metadata([("opaque-value".into(), "preserve".into())].into()),
    ];
    let layout = DataType::Map(
        Arc::new(
            Field::new("declared-entries", DataType::Struct(fields.into()), false)
                .with_metadata([("opaque-entries".into(), "preserve".into())].into()),
        ),
        sorted,
    );
    FunctionValueType::try_from_value(
        ValueTypeRef::try_new(
            ValueKind::Map {
                key: key.value_type().clone(),
                value: value.value_type().clone(),
            },
            true,
            storage.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(&layout, storage).unwrap(),
    )
    .unwrap()
}

#[test]
fn uninhabited_key_profile_preserves_complete_map_value_and_independent_probe() {
    use crate::{FunctionArgumentType, FunctionResultType};
    let values = [
        FunctionValueType::new(DataType::Null, true),
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::from_legacy_declared_projection(
            DataType::List(Arc::new(Field::new("nested-item", DataType::Int32, false))),
            false,
        ),
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    ];
    let probes = [
        FunctionValueType::new(DataType::Utf8, false),
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Null, true),
        FunctionValueType::from_legacy_declared_projection(
            DataType::List(Arc::new(Field::new("probe-item", DataType::Int32, true))),
            true,
        ),
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    ];
    let owner = owner("__map_element_at");
    for value in values {
        for sorted in [false, true] {
            let map = uninhabited_map(&value, sorted);
            assert!(super::super::map_lookup_core::has_uninhabited_map_key(&map));
            for probe in &probes {
                let arguments = [argument(map.clone()), argument(probe.clone())];
                let selected = owner
                    .resolve(request(&arguments), crate::binding_test_control())
                    .unwrap();
                assert_eq!(
                    selected.argument_types[0],
                    FunctionArgumentType::Value(map.clone())
                );
                assert_eq!(
                    selected.argument_types[1],
                    FunctionArgumentType::Value(probe.clone())
                );
                assert_eq!(
                    selected.result_type,
                    FunctionResultType::Scalar(value.with_nullable(true).unwrap())
                );
                if value.logical_type() != ValueLogicalType::Physical {
                    let mut wrong_domain = selected.clone();
                    wrong_domain.result_type = FunctionResultType::Scalar(FunctionValueType::new(
                        value.data_type().clone(),
                        true,
                    ));
                    assert!(
                        owner
                            .validate_selected(
                                &wrong_domain,
                                request(&arguments),
                                crate::binding_test_control()
                            )
                            .is_err()
                    );
                }

                assert_eq!(
                    owner
                        .select_at_overload_observed(
                            &selected.overload,
                            request(&arguments),
                            crate::binding_test_control()
                        )
                        .unwrap(),
                    selected
                );
                owner
                    .validate_selected(
                        &selected,
                        request(&arguments),
                        crate::binding_test_control(),
                    )
                    .unwrap();
                let prepared =
                    prepared_for_test("__map_element_at", &[map.clone(), probe.clone()]).unwrap();
                assert_eq!(
                    prepared.contract().effects().argument_control,
                    ArgumentControl::Eager
                );
                assert_eq!(
                    prepared.contract().effects().failure_behavior,
                    FunctionFailureBehavior::Propagate
                );
            }
        }
    }
}

#[test]
fn uninhabited_key_profile_cannot_validate_forged_probe_value_or_inhabited_key() {
    use crate::{FunctionArgumentType, FunctionResultType};
    let owner = owner("__map_element_at");
    let map = uninhabited_map(&FunctionValueType::new(DataType::Int32, false), false);
    let arguments = [
        argument(map.clone()),
        argument(FunctionValueType::new(DataType::Utf8, false)),
    ];
    let selected = owner
        .resolve(request(&arguments), crate::binding_test_control())
        .unwrap();
    let mut wrong_probe = selected.clone();
    let mut revised = wrong_probe.argument_types.to_vec();
    revised[1] = FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, false));
    wrong_probe.argument_types =
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(revised.into_boxed_slice());
    let mut wrong_value = selected.clone();
    wrong_value.result_type =
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, true));
    for wrong in [wrong_probe, wrong_value] {
        assert!(
            owner
                .validate_selected(&wrong, request(&arguments), crate::binding_test_control())
                .is_err()
        );
    }
    let inhabited = FunctionValueType::from_legacy_declared_projection(
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        true,
    );
    assert!(!super::super::map_lookup_core::has_uninhabited_map_key(
        &inhabited
    ));
    let invalid = [argument(inhabited.clone()), arguments[1].clone()];
    assert!(
        owner
            .select_at_overload_observed(
                &selected.overload,
                request(&invalid),
                crate::binding_test_control()
            )
            .is_err()
    );
    assert!(
        owner
            .resolve(request(&invalid), crate::binding_test_control())
            .is_err()
    );
    let ordinary = [
        argument(inhabited),
        argument(FunctionValueType::new(DataType::Int32, true)),
    ];
    let ordinary_selected = owner
        .resolve(request(&ordinary), crate::binding_test_control())
        .unwrap();
    assert_ne!(ordinary_selected.overload, selected.overload);
    owner
        .validate_selected(
            &ordinary_selected,
            request(&ordinary),
            crate::binding_test_control(),
        )
        .unwrap();
    // The scalar NULL type is not a Map with an uninhabited key.
    let scalar = [
        argument(FunctionValueType::new(DataType::Null, true)),
        arguments[1].clone(),
    ];
    assert!(
        owner
            .select_at_overload_observed(
                &selected.overload,
                request(&scalar),
                crate::binding_test_control()
            )
            .is_err()
    );
}

#[test]
fn uninhabited_map_key_carrier_constraint_refuses_nullable_key_and_entries() {
    for (key_nullable, entries_nullable) in [(true, false), (false, true), (true, true)] {
        let carrier = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Null, key_nullable),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                entries_nullable,
            )),
            false,
        );
        assert!(!super::super::map_lookup_core::has_uninhabited_map_key_carrier(&carrier));
    }
    assert!(!super::super::map_lookup_core::has_uninhabited_map_key_carrier(&DataType::Null));
}
