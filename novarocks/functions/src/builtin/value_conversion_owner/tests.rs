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
    FunctionArgument, FunctionArgumentType, FunctionResultType, FunctionSpecializationFailure,
    FunctionValueType, ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
    specialize_frozen_scalar, specialize_scalar,
};
use arrow_array::{ArrayRef, NullArray};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionEffectContext, ExpressionUseId, NR_LOGICAL_TYPE_KEY, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameters, ValueLogicalType,
};
use std::{collections::HashMap, sync::Mutex};

fn owner() -> ValueConversionOwner {
    let (declaration, resolver) = value_conversion::definition_parts().unwrap();
    ValueConversionOwner::new(declaration, resolver).unwrap()
}
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(data_type, nullable)
}
fn nominal(data_type: DataType, nullable: bool, logical: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(data_type, nullable, logical).unwrap()
}
fn large(nullable: bool) -> FunctionValueType {
    nominal(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    }
}
fn arguments(source: &FunctionValueType) -> [FunctionArgument; 1] {
    [FunctionArgument::Value {
        value_type: source.clone(),
        constant: None,
    }]
}
fn request<'a>(
    args: &'a [FunctionArgument],
    target: &'a FunctionValueType,
) -> FunctionBindingRequest<'a> {
    FunctionBindingRequest {
        arguments: args,
        logical_argument_count: 1,
        expected_result_type: Some(target),
    }
}
fn input<'a>(
    owner: &'a ValueConversionOwner,
    selected: &'a FunctionBindingSelection,
    args: &'a [FunctionArgument],
    target: &'a FunctionValueType,
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
    policy: DecimalOverflowPolicy,
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected,
        request: request(args, target),
        environment: &[],
        parameters,
        decimal_overflow_policy: policy,
        proof_scope: CallProofScope::Domain(context().domain),
    }
}
pub(super) fn prepare(
    source: &FunctionValueType,
    target: &FunctionValueType,
    policy: DecimalOverflowPolicy,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    let owner = owner();
    let args = arguments(source);
    let selected = Arc::new(owner.resolve(request(&args, target), crate::binding_test_control())?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(42))];
    specialize_scalar(
        &owner,
        input(&owner, &selected, &args, target, &parameters, &uses, policy),
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        crate::binding_test_control(),
    )
    .map(|result| result.into_prepared())
}
fn pairs(nullable: bool) -> [(FunctionValueType, FunctionValueType, &'static str); 7] {
    [
        (
            nominal(DataType::Utf8, nullable, ValueLogicalType::Json),
            ty(DataType::Utf8, nullable),
            value_conversion::JSON_TEXT,
        ),
        (
            ty(DataType::Int64, nullable),
            large(nullable),
            value_conversion::SIGNED_LARGEINT,
        ),
        (
            large(nullable),
            ty(DataType::Int8, true),
            value_conversion::LARGEINT_SIGNED,
        ),
        (
            large(nullable),
            ty(DataType::Decimal256(41, 2), true),
            value_conversion::LARGEINT_DECIMAL,
        ),
        (
            large(nullable),
            ty(DataType::Float32, nullable),
            value_conversion::LARGEINT_FLOAT,
        ),
        (
            ty(DataType::Utf8, nullable),
            large(true),
            value_conversion::UTF8_LARGEINT,
        ),
        (
            ty(DataType::Null, true),
            nominal(DataType::Utf8, true, ValueLogicalType::Json),
            value_conversion::NULL_LIFT,
        ),
    ]
}

#[test]
fn whole_builtin_catalogue_attaches_one_hidden_owner_and_exact_nine_implementation_records() {
    let owner = owner();
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let actual = catalog
        .definition_by_id(owner.declaration.function_id())
        .unwrap();
    assert_eq!(actual.visibility(), FunctionVisibility::Hidden);
    assert!(actual.binding.as_ref().unwrap().pure.is_some());
    assert_eq!(
        actual.binding_declaration().unwrap(),
        owner.binding_declaration()
    );
    let expected = [
        (
            value_conversion::JSON_TEXT,
            "builtin.scalar/value_domain_conversion/json_text_same_structure/selected-v1",
        ),
        (
            value_conversion::OPAQUE_BINARY,
            "builtin.scalar/value_domain_conversion/opaque_to_binary_same_structure/selected-v1",
        ),
        (
            value_conversion::SIGNED_LARGEINT,
            "builtin.scalar/value_domain_conversion/signed_to_largeint/selected-v1",
        ),
        (
            value_conversion::LARGEINT_SIGNED,
            "builtin.scalar/value_domain_conversion/largeint_to_signed_null_overflow/selected-v1",
        ),
        (
            value_conversion::LARGEINT_DECIMAL,
            "builtin.scalar/value_domain_conversion/largeint_to_decimal_exact/selected-v1",
        ),
        (
            value_conversion::LARGEINT_FLOAT,
            "builtin.scalar/value_domain_conversion/largeint_to_float_round/selected-v1",
        ),
        (
            value_conversion::LARGEINT_TEXT,
            "builtin.scalar/value_domain_conversion/largeint_to_utf8_text/selected-v1",
        ),
        (
            value_conversion::UTF8_LARGEINT,
            "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/selected-v1",
        ),
        (
            value_conversion::NULL_LIFT,
            "builtin.scalar/value_domain_conversion/null_to_typed_nullable/selected-v1",
        ),
    ];
    assert_eq!(owner.implementations.len(), expected.len());
    for record in &owner.implementations {
        let (_, implementation) = expected
            .iter()
            .find(|(overload, _)| record.overload.as_str() == *overload)
            .unwrap();
        assert_eq!(record.implementation.as_str(), *implementation);
        assert_eq!(record.abi, PureKernelAbi::ScalarV1);
        assert_eq!(
            owner
                .declaration
                .effect_declaration(&record.overload)
                .unwrap(),
            &value_conversion::overload_effects(record.overload.as_str())
        );
    }
    // An actual owner attachment is not a whole Server installed-manifest seal.
}

#[test]
fn every_conversion_fresh_frozen_keeps_full_source_target_policy_scope_and_canonical_arc() {
    let owner = owner();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(42))];
    for nullable in [false, true] {
        for (source, target, overload) in pairs(nullable) {
            let args = arguments(&source);
            let selected = Arc::new(
                owner
                    .resolve(request(&args, &target), crate::binding_test_control())
                    .unwrap(),
            );
            assert_eq!(selected.overload.as_str(), overload);
            assert_eq!(
                selected.argument_types.as_ref(),
                &[FunctionArgumentType::Value(source)]
            );
            assert_eq!(
                selected.result_type,
                FunctionResultType::Scalar(target.clone())
            );
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let exact = input(
                    &owner,
                    &selected,
                    &args,
                    &target,
                    &parameters,
                    &uses,
                    policy,
                );
                let fresh = specialize_scalar(
                    &owner,
                    exact,
                    selected.clone(),
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control(),
                )
                .unwrap();
                let canonical = fresh.prepared().contract().clone();
                let direct = owner
                    .prepare_scalar(exact, canonical.clone(), crate::binding_test_control())
                    .unwrap();
                assert!(Arc::ptr_eq(direct.contract(), &canonical));
                assert!(std::ptr::eq(canonical.selected(), selected.as_ref()));
                assert_eq!(canonical.decimal_overflow_policy(), policy);
                let facts = canonical.effects().clone();
                assert_eq!(facts, owner.call_effects(exact));
                let frozen = specialize_frozen_scalar(
                    &owner,
                    exact,
                    selected.clone(),
                    &facts,
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control(),
                )
                .unwrap();
                assert!(std::ptr::eq(
                    frozen.prepared().contract().selected(),
                    selected.as_ref()
                ));
                assert_eq!(frozen.prepared().contract().effects(), &facts);
                assert_eq!(
                    direct.instance_retained_upper_bound(),
                    std::mem::size_of::<ConversionInstance>()
                );
                assert_eq!(
                    direct.create_instance().unwrap().retained_bytes(),
                    std::mem::size_of::<ConversionInstance>()
                );
            }
        }
    }
}

#[test]
fn nested_json_recipe_uses_the_exact_source_owned_fields_and_only_removes_json_identity() {
    let source_field = Arc::new(
        Field::new("actual_child", DataType::Utf8, false).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
            ("provider_id".to_owned(), "173".to_owned()),
        ])),
    );
    let mut metadata = source_field.metadata().clone();
    metadata.remove(NR_LOGICAL_TYPE_KEY);
    let target_field = Arc::new(source_field.as_ref().clone().with_metadata(metadata));
    let key = Arc::new(Field::new("actual_key", DataType::Utf8, false));
    let source_entries = Arc::new(
        Field::new(
            "actual_entries",
            DataType::Struct(vec![key.clone(), source_field.clone()].into()),
            false,
        )
        .with_metadata(HashMap::from([("provider_entries".into(), "17".into())])),
    );
    let target_entries = Arc::new(
        source_entries
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(vec![key, target_field.clone()].into())),
    );
    for (source, target) in [
        (
            DataType::List(source_field.clone()),
            DataType::List(target_field.clone()),
        ),
        (
            DataType::LargeList(source_field.clone()),
            DataType::LargeList(target_field.clone()),
        ),
        (
            DataType::FixedSizeList(source_field.clone(), 2),
            DataType::FixedSizeList(target_field.clone(), 2),
        ),
        (
            DataType::Struct(vec![source_field.clone()].into()),
            DataType::Struct(vec![target_field.clone()].into()),
        ),
        (
            DataType::Map(source_entries, true),
            DataType::Map(target_entries, true),
        ),
    ] {
        let source = ty(source, true);
        let target = ty(target, true);
        let prepared = prepare(&source, &target, DecimalOverflowPolicy::ReportError).unwrap();
        assert_eq!(
            prepared.contract().value_argument_types().next(),
            Some(&source)
        );
        assert_eq!(prepared.contract().result_type(), &target);
        assert_eq!(
            prepared.contract().selected().overload.as_str(),
            value_conversion::JSON_TEXT
        );
    }
    let target = ty(
        DataType::List(Arc::new(target_field.as_ref().clone().with_name("foreign"))),
        true,
    );
    assert!(prepared_for_test(&ty(DataType::List(source_field), true), &target).is_ok());
}

#[test]
fn source_carriers_cannot_author_nominal_domains_or_bypass_the_explicit_target() {
    let owner = owner();
    for (source, target) in [
        (
            ty(DataType::FixedSizeBinary(16), false),
            ty(DataType::Float64, false),
        ),
        (
            nominal(DataType::FixedSizeBinary(16), false, ValueLogicalType::Uuid),
            ty(DataType::Int64, true),
        ),
        (ty(DataType::UInt32, false), large(false)),
        (large(false), ty(DataType::Int8, false)),
        (
            nominal(DataType::Utf8, false, ValueLogicalType::Json),
            large(false),
        ),
        (ty(DataType::Null, true), ty(DataType::Int64, false)),
    ] {
        let args = arguments(&source);
        assert!(
            owner
                .resolve(request(&args, &target), crate::binding_test_control())
                .is_err()
        );
    }
    let args = arguments(&ty(DataType::Int64, false));
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    assert!(matches!(
        owner.resolve(request, crate::binding_test_control()),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
}

#[test]
fn frozen_facts_and_prepare_cannot_replace_overload_owner_pointer_scope_context_or_policy() {
    let owner = owner();
    let source = ty(DataType::Int64, false);
    let target = large(false);
    let args = arguments(&source);
    let selected = Arc::new(
        owner
            .resolve(request(&args, &target), crate::binding_test_control())
            .unwrap(),
    );
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(42))];
    let exact = input(
        &owner,
        &selected,
        &args,
        &target,
        &parameters,
        &uses,
        DecimalOverflowPolicy::ReportError,
    );
    let fresh = specialize_scalar(
        &owner,
        exact,
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        crate::binding_test_control(),
    )
    .unwrap();
    let canonical = fresh.prepared().contract().clone();
    for field in 0..4 {
        let mut wrong = exact;
        match field {
            0 => wrong.context.use_id = ExpressionUseId::new(99),
            1 => wrong.decimal_overflow_policy = DecimalOverflowPolicy::OutputNull,
            2 => wrong.proof_scope = CallProofScope::Unconditional,
            _ => wrong.kind = FunctionKind::Aggregate,
        }
        assert!(matches!(
            owner.prepare_scalar(wrong, canonical.clone(), crate::binding_test_control()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let same_value_foreign_pointer = selected.as_ref().clone();
    let mut wrong = exact;
    wrong.selected = &same_value_foreign_pointer;
    assert!(matches!(
        owner.prepare_scalar(wrong, canonical.clone(), crate::binding_test_control()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let mut forged = selected.as_ref().clone();
    forged.overload = crate::FunctionOverloadId::try_new(value_conversion::LARGEINT_FLOAT).unwrap();
    assert!(
        owner
            .validate_selected(&forged, exact.request, crate::binding_test_control())
            .is_err()
    );
    let environment = [SemanticParameterRef {
        id: SemanticParameterId::new(0),
        expected_key: SemanticParameterKey::StatementStartUtc,
    }];
    wrong = exact;
    wrong.environment = &environment;
    assert!(matches!(
        owner.validate_and_refine(wrong, crate::binding_test_control()),
        Err(FunctionEffectOwnerError::Owner(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
    wrong = exact;
    wrong.proof_scope = CallProofScope::Domain(EvaluationDomainId::new(99));
    assert!(
        owner
            .validate_and_refine(wrong, crate::binding_test_control())
            .is_err()
    );
    let mut facts = canonical.effects().clone();
    facts.observable_effects.rng_sampling = true;
    assert!(
        specialize_frozen_scalar(
            &owner,
            exact,
            selected.clone(),
            &facts,
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control()
        )
        .is_err()
    );
}

struct CompileTrace {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    failure: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileTrace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        match self.failure.clone() {
            Some((at, cause)) if trace.len() - 1 == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn failure(error: FunctionSpecializationFailure) -> CompileControlError {
    match error {
        FunctionSpecializationFailure::Control(cause) => cause,
        FunctionSpecializationFailure::Kernel(KernelFailure::CompileControl(cause)) => cause,
        other => panic!("original typed refusal: {other:?}"),
    }
}

#[test]
fn actual_owner_specialization_preserves_every_three_cause_callback_prefix_without_retry() {
    let owner = owner();
    let source = ty(DataType::Int64, true);
    let target = large(true);
    let args = arguments(&source);
    let selected = Arc::new(
        owner
            .resolve(request(&args, &target), crate::binding_test_control())
            .unwrap(),
    );
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(42))];
    let exact = input(
        &owner,
        &selected,
        &args,
        &target,
        &parameters,
        &uses,
        DecimalOverflowPolicy::OutputNull,
    );
    let baseline = CompileTrace {
        trace: Mutex::new(Vec::new()),
        failure: None,
    };
    specialize_scalar(
        &owner,
        exact,
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        &baseline,
    )
    .unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first().unwrap().1, 0);
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CompileTrace {
                trace: Mutex::new(Vec::new()),
                failure: Some((at, cause.clone())),
            };
            let error = specialize_scalar(
                &owner,
                exact,
                selected.clone(),
                ScopedExpressionEffects::pure_value(context()),
                &control,
            )
            .unwrap_err();
            assert_eq!(failure(error), cause);
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

struct EvaluationControl;
impl KernelEvaluationControl for EvaluationControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("conversion does not wait")
    }
}

#[test]
fn null_lift_prepares_complete_target_but_only_an_empty_strict_active_mask_is_valid() {
    let source = ty(DataType::Null, true);
    let json = nominal(DataType::Utf8, true, ValueLogicalType::Json);
    let nested = ty(
        DataType::List(Arc::new(
            Field::new("json_child", DataType::Utf8, true).with_metadata(HashMap::from([(
                NR_LOGICAL_TYPE_KEY.to_owned(),
                "json".to_owned(),
            )])),
        )),
        true,
    );
    for target in [json, nested] {
        let prepared = prepared_for_test(&source, &target).unwrap();
        assert_eq!(
            prepared.contract().selected().overload.as_str(),
            value_conversion::NULL_LIFT
        );
        let array: ArrayRef = Arc::new(NullArray::new(2));
        let args = [crate::EvaluatedArgument::Column(&array)];
        let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
        let output = instance
            .evaluate(
                Selection::try_sparse(2, &[]).unwrap(),
                &args,
                &EvaluationControl,
            )
            .unwrap();
        assert!(novarocks_type_contract::arrow_data_types_exact(
            output.values().data_type(),
            target.data_type()
        ));
        assert_eq!(output.values().len(), 0);
        assert!(output.errors().is_empty());
        assert!(matches!(
            instance.evaluate(Selection::all(2), &args, &EvaluationControl),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn largeint_decimal_effects_refine_exact_range_and_freeze_overflow_policy() {
    let owner = owner();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(42))];
    for (target, can_overflow) in [
        (ty(DataType::Decimal128(38, 0), true), true),
        (ty(DataType::Decimal128(38, -1), true), false),
        (ty(DataType::Decimal256(41, 2), true), false),
        (ty(DataType::Decimal256(38, 2), true), true),
        (ty(DataType::Decimal256(76, 76), true), true),
    ] {
        let source = large(true);
        let args = arguments(&source);
        let selected = Arc::new(
            owner
                .resolve(request(&args, &target), crate::binding_test_control())
                .unwrap(),
        );
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let exact = input(
                &owner,
                &selected,
                &args,
                &target,
                &parameters,
                &uses,
                policy,
            );
            let fresh = specialize_scalar(
                &owner,
                exact,
                selected.clone(),
                ScopedExpressionEffects::pure_value(context()),
                crate::binding_test_control(),
            )
            .unwrap();
            let contract = fresh.prepared().contract();
            let expected = if can_overflow && policy == DecimalOverflowPolicy::ReportError {
                crate::FunctionIntrinsicRowError::MayRaise
            } else {
                crate::FunctionIntrinsicRowError::NoRowError
            };
            assert_eq!(contract.effects().own_row_error, expected);
            let other = if policy == DecimalOverflowPolicy::OutputNull {
                DecimalOverflowPolicy::ReportError
            } else {
                DecimalOverflowPolicy::OutputNull
            };
            let foreign = input(&owner, &selected, &args, &target, &parameters, &uses, other);
            assert!(
                owner
                    .prepare_scalar(foreign, contract.clone(), crate::binding_test_control())
                    .is_err()
            );
            let mut forged = contract.effects().clone();
            forged.own_row_error = if expected == crate::FunctionIntrinsicRowError::MayRaise {
                crate::FunctionIntrinsicRowError::NoRowError
            } else {
                crate::FunctionIntrinsicRowError::MayRaise
            };
            assert!(
                specialize_frozen_scalar(
                    &owner,
                    exact,
                    selected.clone(),
                    &forged,
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control()
                )
                .is_err()
            );
        }
    }
}

fn conversion_compile_profiles() -> Vec<(FunctionValueType, FunctionValueType)> {
    let json = Arc::new(
        Field::new("actual_json", DataType::Utf8, true).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
            ("provider_id".to_owned(), "731".to_owned()),
        ])),
    );
    let mut metadata = json.metadata().clone();
    metadata.remove(NR_LOGICAL_TYPE_KEY);
    let text = Arc::new(json.as_ref().clone().with_metadata(metadata));
    vec![
        (ty(DataType::Int64, true), large(true)),
        (
            ty(DataType::List(json), true),
            ty(DataType::List(text), true),
        ),
    ]
}
fn with_installed_conversion<T>(
    source: FunctionValueType,
    target: FunctionValueType,
    action: impl FnOnce(&ValueConversionOwner, CallEffectInput<'_>, Arc<ScalarCallContract>) -> T,
) -> T {
    let owner = owner();
    let catalog = super::super::catalogue::builtin_engine_function_catalog();
    let args = arguments(&source);
    let bound = catalog
        .resolve_bound_trusted(
            value_conversion::VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            request(&args, &target),
            crate::binding_test_control(),
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let parameters = SemanticParameters::default();
    let uses = [Some(ExpressionUseId::new(42))];
    let exact = input(
        &owner,
        &selected,
        &args,
        &target,
        &parameters,
        &uses,
        DecimalOverflowPolicy::ReportError,
    );
    assert_eq!(exact.function_id, &bound.function_id);
    let fresh = catalog
        .prepare_fresh_selected(
            exact,
            selected.clone(),
            crate::PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context()),
            },
            crate::binding_test_control(),
        )
        .unwrap();
    let frozen = catalog
        .prepare_frozen_selected(
            exact,
            selected.clone(),
            fresh.call_contract().effects(),
            crate::PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context()),
            },
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(fresh.implementation(), frozen.implementation());
    let crate::PreparedPureKernel::Scalar(prepared) = fresh.into_prepared() else {
        panic!("the actual value-conversion registry must prepare ScalarV1");
    };
    assert!(std::ptr::eq(
        prepared.contract().selected(),
        selected.as_ref()
    ));
    action(&owner, exact, prepared.contract().clone())
}
fn compile_trace() -> CompileTrace {
    CompileTrace {
        trace: Mutex::new(vec![]),
        failure: None,
    }
}
fn compile_causes() -> [CompileControlError; 4] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
        ),
    ]
}

#[test]
fn conversion_owner_captured_contract_and_nested_binding_errors_have_no_later_compile_footer() {
    for (source, target) in conversion_compile_profiles() {
        with_installed_conversion(source, target, |owner, exact, contract| {
            let foreign = FunctionId::try_new("foreign.conversion/compile/v1").unwrap();
            for mismatch in 0..3 {
                let mut wrong = exact;
                match mismatch {
                    0 => wrong.function_id = &foreign,
                    1 => wrong.kind = FunctionKind::Aggregate,
                    2 => wrong.proof_scope = CallProofScope::Domain(EvaluationDomainId::new(999)),
                    _ => unreachable!(),
                }
                let control = compile_trace();
                let error = owner.validate_and_refine(wrong, &control).unwrap_err();
                let expected = if mismatch < 2 {
                    FunctionBindingError::UnknownFunction
                } else {
                    FunctionBindingError::InvalidBinding(
                        "value conversion requires its exact proof scope and no environment".into(),
                    )
                };
                match error {
                    FunctionEffectOwnerError::Owner(actual) => assert_eq!(actual, expected),
                    other => {
                        panic!("the original conversion author error must stay intact: {other:?}")
                    }
                }
                assert_eq!(
                    *control.trace.lock().unwrap(),
                    [(CompilePhase::FunctionSpecialization, 0)]
                );
                let control = compile_trace();
                assert_eq!(
                    owner
                        .prepare_scalar(wrong, contract.clone(), &control)
                        .unwrap_err(),
                    KernelFailure::InvalidProgram(crate::KernelDiagnostic::new(
                        "value conversion differs from its exact checked call"
                    ))
                );
                assert_eq!(
                    *control.trace.lock().unwrap(),
                    [(CompilePhase::FunctionSpecialization, 0)]
                );
            }
            let mut forged = (*exact.selected).clone();
            forged.argument_types =
                vec![FunctionArgumentType::Value(ty(DataType::Int32, true))].into();
            let direct = compile_trace();
            let expected = owner
                .validate_selected(&forged, exact.request, &direct)
                .unwrap_err();
            let mut wrong = exact;
            wrong.selected = &forged;
            let control = compile_trace();
            match owner.validate_and_refine(wrong, &control).unwrap_err() {
                FunctionEffectOwnerError::Owner(actual) => assert_eq!(actual, expected),
                other => {
                    panic!("the original nested binding author error must stay intact: {other:?}")
                }
            }
            let mut expected_trace = vec![
                (CompilePhase::FunctionSpecialization, 0),
                (CompilePhase::FunctionSpecialization, 2),
            ];
            expected_trace.extend_from_slice(&direct.trace.lock().unwrap());
            assert_eq!(
                *control.trace.lock().unwrap(),
                expected_trace,
                "nested binding failure has no enclosing callback footer"
            );
        });
    }
}

#[test]
fn conversion_recipe_and_owner_success_flush_each_original_scope_and_keep_all_refusal_prefixes() {
    for (source, target) in conversion_compile_profiles() {
        with_installed_conversion(source, target, |owner, exact, contract| {
            let binding = compile_trace();
            owner
                .validate_selected(exact.selected, exact.request, &binding)
                .unwrap();
            let recipe_control = compile_trace();
            let recipe = ConversionRecipe::try_new(&contract, &recipe_control).unwrap();
            assert!(recipe.retained_bytes() >= std::mem::size_of::<ConversionRecipe>());
            if matches!(exact.request.arguments[0], FunctionArgument::Value { ref value_type, .. } if matches!(value_type.data_type(), DataType::List(_)))
            {
                assert!(
                    recipe.retained_bytes() > std::mem::size_of::<ConversionRecipe>(),
                    "the real nested recipe owns its allocated graph"
                );
            }
            drop(recipe);
            let recipe_trace = recipe_control.trace.lock().unwrap().clone();
            assert_eq!(
                recipe_trace.first(),
                Some(&(CompilePhase::FunctionSpecialization, 0))
            );
            assert!(
                recipe_trace.last().unwrap().1 > 0,
                "the recipe success path really flushes pending work"
            );
            let mut refine_trace = vec![
                (CompilePhase::FunctionSpecialization, 0),
                (CompilePhase::FunctionSpecialization, 2),
            ];
            refine_trace.extend_from_slice(&binding.trace.lock().unwrap());
            refine_trace.push((CompilePhase::FunctionSpecialization, 0));
            let refined = compile_trace();
            assert_eq!(
                &owner.validate_and_refine(exact, &refined).unwrap(),
                contract.effects()
            );
            assert_eq!(*refined.trace.lock().unwrap(), refine_trace);
            let mut prepare_trace = vec![
                (CompilePhase::FunctionSpecialization, 0),
                (CompilePhase::FunctionSpecialization, 1),
            ];
            prepare_trace.extend_from_slice(&binding.trace.lock().unwrap());
            prepare_trace.push((CompilePhase::FunctionSpecialization, 0));
            let recipe_start = prepare_trace.len();
            prepare_trace.extend_from_slice(&recipe_trace);
            prepare_trace.push((CompilePhase::FunctionSpecialization, 0));
            let prepared_control = compile_trace();
            let prepared = owner
                .prepare_scalar(exact, contract.clone(), &prepared_control)
                .unwrap();
            assert!(Arc::ptr_eq(prepared.contract(), &contract));
            assert_eq!(*prepared_control.trace.lock().unwrap(), prepare_trace);
            drop(prepared);
            for at in 0..recipe_trace.len() {
                for cause in compile_causes() {
                    let control = CompileTrace {
                        trace: Mutex::new(vec![]),
                        failure: Some((at, cause.clone())),
                    };
                    assert_eq!(
                        ConversionRecipe::try_new(&contract, &control).unwrap_err(),
                        KernelFailure::CompileControl(cause.clone())
                    );
                    assert_eq!(*control.trace.lock().unwrap(), recipe_trace[..=at]);
                    let control = CompileTrace {
                        trace: Mutex::new(vec![]),
                        failure: Some((recipe_start + at, cause.clone())),
                    };
                    assert_eq!(
                        owner
                            .prepare_scalar(exact, contract.clone(), &control)
                            .unwrap_err(),
                        KernelFailure::CompileControl(cause)
                    );
                    assert_eq!(
                        *control.trace.lock().unwrap(),
                        prepare_trace[..=recipe_start + at],
                        "an inner refusal cannot append an outer footer"
                    );
                }
            }
            for at in 0..prepare_trace.len() {
                for cause in compile_causes() {
                    let control = CompileTrace {
                        trace: Mutex::new(vec![]),
                        failure: Some((at, cause.clone())),
                    };
                    assert_eq!(
                        owner
                            .prepare_scalar(exact, contract.clone(), &control)
                            .unwrap_err(),
                        KernelFailure::CompileControl(cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), prepare_trace[..=at]);
                }
            }
            for at in 0..refine_trace.len() {
                for cause in compile_causes() {
                    let control = CompileTrace {
                        trace: Mutex::new(vec![]),
                        failure: Some((at, cause.clone())),
                    };
                    match owner.validate_and_refine(exact, &control).unwrap_err() {
                        FunctionEffectOwnerError::Control(actual) => assert_eq!(actual, cause),
                        other => {
                            panic!("the actual refinement refusal must stay original: {other:?}")
                        }
                    }
                    assert_eq!(*control.trace.lock().unwrap(), refine_trace[..=at]);
                }
            }
        });
    }
}

#[test]
fn conversion_prepared_and_real_instance_release_recipe_custody_only_at_actual_drop() {
    for (source, target) in conversion_compile_profiles() {
        with_installed_conversion(source, target, |owner, exact, contract| {
            let selected = contract.call().selected_owner();
            let original_owners = Arc::strong_count(selected);
            let control = Arc::new(compile_trace());
            let weak_control = Arc::downgrade(&control);
            let prepared = owner
                .prepare_scalar(exact, contract.clone(), control.as_ref())
                .unwrap();
            drop(control);
            assert!(
                weak_control.upgrade().is_none(),
                "actual immutable preparation retains no caller control"
            );
            assert_eq!(
                Arc::strong_count(selected),
                original_owners + 1,
                "the real compiled recipe owns the exact selected backing"
            );
            let weak_prepared = Arc::downgrade(&prepared);
            let instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
            drop(prepared);
            assert!(
                weak_prepared.upgrade().is_some(),
                "the actual live instance retains its preparation"
            );
            assert_eq!(Arc::strong_count(selected), original_owners + 1);
            assert!(instance.retained_bytes().unwrap() > 0);
            drop(instance);
            assert!(weak_prepared.upgrade().is_none());
            assert_eq!(
                Arc::strong_count(selected),
                original_owners,
                "real instance and preparation destruction released the recipe's selected owner"
            );
        });
    }
}

#[test]
fn conversion_recipe_refuses_a_real_foreign_canonical_kernel_without_a_later_callback() {
    let catalog = super::super::catalogue::builtin_engine_function_catalog();
    let args = arguments(&ty(DataType::Utf8, true));
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(
            "to_days",
            FunctionKind::Scalar,
            request,
            crate::binding_test_control(),
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let parameters = SemanticParameters::default();
    let uses = [Some(ExpressionUseId::new(42))];
    let input = CallEffectInput {
        context: context(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: &bound.function_id,
        kind: FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Domain(context().domain),
    };
    let foreign = catalog
        .prepare_fresh_selected(
            input,
            selected.clone(),
            crate::PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context()),
            },
            crate::binding_test_control(),
        )
        .unwrap();
    let crate::PreparedPureKernel::Scalar(prepared) = foreign.into_prepared() else {
        panic!("the real foreign author must return its ScalarV1 contract");
    };
    let control = compile_trace();
    assert_eq!(
        ConversionRecipe::try_new(prepared.contract(), &control).unwrap_err(),
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new(
            "conversion has an unknown canonical overload"
        ))
    );
    assert_eq!(
        *control.trace.lock().unwrap(),
        [(CompilePhase::FunctionSpecialization, 0)],
        "the actual private recipe error must not flush a late callback"
    );
}
