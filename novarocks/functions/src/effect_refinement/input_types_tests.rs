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
    FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionOverloadId,
    FunctionResultType, FunctionValueType,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    ArgumentControl, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
    FunctionNullBehavior, FunctionVolatility, MAX_UNOBSERVED_COMPILE_WORK, ObservableEffects,
    ValueLogicalType,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Control {
    checks: Mutex<Vec<u32>>,
    failure: Option<(usize, CompileControlError)>,
}
impl Control {
    fn checks(&self) -> Vec<u32> {
        self.checks.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= MAX_UNOBSERVED_COMPILE_WORK);
        let mut checks = self.checks.lock().unwrap();
        let index = checks.len();
        checks.push(units);
        if let Some((at, error)) = self.failure.clone()
            && at == index
        {
            Err(error)
        } else {
            Ok(())
        }
    }
}

struct Owner {
    declaration: FunctionEffectDeclaration,
    declarations: AtomicUsize,
    refinements: AtomicUsize,
    first_declaration_check_count: AtomicUsize,
    control: Arc<Control>,
}
impl Owner {
    fn new(argument_control: ArgumentControl, control: Arc<Control>) -> Self {
        Self {
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control,
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            declarations: AtomicUsize::new(0),
            refinements: AtomicUsize::new(0),
            first_declaration_check_count: AtomicUsize::new(0),
            control,
        }
    }
    fn untouched(&self) {
        assert_eq!(self.declarations.load(Ordering::Relaxed), 0);
        assert_eq!(self.refinements.load(Ordering::Relaxed), 0);
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        _: &FunctionId,
        _: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if self.declarations.fetch_add(1, Ordering::Relaxed) == 0 {
            self.first_declaration_check_count
                .store(self.control.checks().len(), Ordering::Relaxed);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.refinements.fetch_add(1, Ordering::Relaxed);
        Ok(CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }
}

struct Fixture {
    id: FunctionId,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    selected: FunctionBindingSelection,
    parameters: SemanticParameters,
    expected: Option<FunctionValueType>,
}
impl Fixture {
    fn new(arguments: Vec<FunctionArgument>) -> Self {
        Self {
            id: FunctionId::try_new("fixture/input-types").unwrap(),
            uses: (0..arguments.len())
                .map(|i| Some(ExpressionUseId::new(i as u32 + 1)))
                .collect(),
            selected: FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/input-types/selected").unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                ),
                result_type: FunctionResultType::Scalar(ty(DataType::Int64, false)),
                aggregate: None,
            },
            arguments,
            parameters: SemanticParameters::default(),
            expected: None,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                domain: EvaluationDomainId::new(7),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.id,
            kind: FunctionKind::Scalar,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
                expected_result_type: self.expected.as_ref(),
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Domain(EvaluationDomainId::new(7)),
        }
    }
}
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(data_type, nullable)
}
fn value(value_type: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type,
        constant: None,
    }
}
fn lambda(parameter: FunctionValueType, result: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Lambda {
        parameter_types: vec![parameter].into(),
        result_type: result,
    }
}
fn higher_order() -> ArgumentControl {
    ArgumentControl::HigherOrder {
        body_ordinal: 0,
        body_demand: EvaluationDemand::Value,
    }
}
fn reject(fixture: &Fixture, argument_control: ArgumentControl) {
    let control = Arc::new(Control::default());
    let owner = Owner::new(argument_control, control.clone());
    assert!(matches!(
        refine_call_effects(&owner, fixture.input(), control.as_ref()),
        Err(CallEffectRefinementError::InvalidInput(_))
    ));
    owner.untouched();
}
fn json(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, nullable, ValueLogicalType::Json)
        .unwrap()
}

#[test]
fn value_domains_and_nullable_covariance_are_checked_before_owner() {
    for (actual, selected, valid) in [
        (ty(DataType::Int64, false), ty(DataType::Int64, true), true),
        (ty(DataType::Int64, true), ty(DataType::Int64, false), false),
        (json(false), ty(DataType::Utf8, false), false),
        (ty(DataType::Utf8, false), json(false), false),
        (
            ty(DataType::Int32, false),
            ty(DataType::Int64, false),
            false,
        ),
    ] {
        let mut fixture = Fixture::new(vec![value(actual)]);
        replace_selected_argument_fixture(
            &mut fixture.selected.argument_types,
            0,
            FunctionArgumentType::Value(selected),
        );
        if valid {
            let control = Arc::new(Control::default());
            let owner = Owner::new(ArgumentControl::Eager, control.clone());
            refine_call_effects(&owner, fixture.input(), control.as_ref()).unwrap();
            assert_eq!(owner.refinements.load(Ordering::Relaxed), 1);
        } else {
            reject(&fixture, ArgumentControl::Eager);
        }
    }
}

#[test]
fn value_nested_covariance_preserves_the_existing_semantic_domain() {
    let field = |nullable, annotation: &str| {
        Arc::new(Field::new("item", DataType::Utf8, nullable).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider".into(),
                annotation.into(),
            )]),
        ))
    };
    let mut fixture = Fixture::new(vec![value(ty(
        DataType::List(field(false, "source")),
        false,
    ))]);
    replace_selected_argument_fixture(
        &mut fixture.selected.argument_types,
        0,
        FunctionArgumentType::Value(ty(DataType::List(field(true, "parameter")), true)),
    );
    let control = Arc::new(Control::default());
    let owner = Owner::new(ArgumentControl::Eager, control.clone());
    refine_call_effects(&owner, fixture.input(), control.as_ref()).unwrap();
    fixture.arguments[0] = value(ty(DataType::List(field(true, "source")), false));
    replace_selected_argument_fixture(
        &mut fixture.selected.argument_types,
        0,
        FunctionArgumentType::Value(ty(DataType::List(field(false, "parameter")), true)),
    );
    reject(&fixture, ArgumentControl::Eager);
}

#[test]
fn argument_kind_and_lambda_arity_cannot_reach_owner() {
    let mut value_fixture = Fixture::new(vec![value(ty(DataType::Int64, false))]);
    replace_selected_argument_fixture(
        &mut value_fixture.selected.argument_types,
        0,
        lambda(ty(DataType::Int64, false), ty(DataType::Boolean, true)).argument_type(),
    );
    reject(&value_fixture, ArgumentControl::Eager);
    let mut body_fixture = Fixture::new(vec![lambda(
        ty(DataType::Int64, false),
        ty(DataType::Boolean, true),
    )]);
    replace_selected_argument_fixture(
        &mut body_fixture.selected.argument_types,
        0,
        FunctionArgumentType::Value(ty(DataType::Int64, false)),
    );
    reject(&body_fixture, higher_order());
    replace_selected_argument_fixture(
        &mut body_fixture.selected.argument_types,
        0,
        FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                Box::new([]),
            ),
            result_type: ty(DataType::Boolean, true),
        },
    );
    reject(&body_fixture, higher_order());
}

#[allow(deprecated)]
fn dictionary_parameter(id: i64, annotation: &str, nullable: bool) -> FunctionValueType {
    let field = Field::new_dict(
        "encoded",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        nullable,
        id,
        false,
    )
    .with_metadata(std::collections::HashMap::<String, String>::from([(
        "provider".into(),
        annotation.into(),
    )]));
    ty(DataType::Struct(vec![Arc::new(field)].into()), false)
}

#[test]
fn lambda_types_require_complete_semantics_after_encoding_and_annotation_normalization() {
    let parameter = dictionary_parameter(17, "source", false);
    let result = json(true);
    for normalized in [
        dictionary_parameter(18, "source", false),
        dictionary_parameter(17, "different", false),
    ] {
        assert_eq!(normalized, parameter);
        let mut fixture = Fixture::new(vec![lambda(normalized, result.clone())]);
        replace_selected_argument_fixture(
            &mut fixture.selected.argument_types,
            0,
            lambda(parameter.clone(), result.clone()).argument_type(),
        );
        let control = Arc::new(Control::default());
        let owner = Owner::new(higher_order(), control.clone());
        refine_call_effects(&owner, fixture.input(), control.as_ref()).unwrap();
    }
    for argument in [
        lambda(dictionary_parameter(17, "source", true), result.clone()),
        lambda(parameter.clone(), ty(DataType::Utf8, true)),
        lambda(parameter.clone(), json(false)),
    ] {
        let mut fixture = Fixture::new(vec![argument]);
        replace_selected_argument_fixture(
            &mut fixture.selected.argument_types,
            0,
            lambda(parameter.clone(), result.clone()).argument_type(),
        );
        reject(&fixture, higher_order());
    }
    let mut fixture = Fixture::new(vec![lambda(ty(DataType::Utf8, false), result.clone())]);
    replace_selected_argument_fixture(
        &mut fixture.selected.argument_types,
        0,
        lambda(json(false), result).argument_type(),
    );
    reject(&fixture, higher_order());
}

#[test]
fn exact_lambda_types_remain_valid_on_fresh_and_frozen_refinement() {
    let fixture = Fixture::new(vec![lambda(
        dictionary_parameter(17, "source", false),
        json(true),
    )]);
    let control = Arc::new(Control::default());
    let owner = Owner::new(higher_order(), control.clone());
    let receipt = refine_call_effects(&owner, fixture.input(), control.as_ref()).unwrap();
    let frozen = receipt.facts().clone();
    validate_frozen_call_effects(&owner, fixture.input(), &frozen, control.as_ref()).unwrap();
    assert_eq!(owner.refinements.load(Ordering::Relaxed), 2);
    let mut annotated = Fixture::new(vec![lambda(
        dictionary_parameter(19, "different external annotation", false),
        json(true),
    )]);
    annotated.selected = fixture.selected.clone();
    let control = Arc::new(Control::default());
    let owner = Owner::new(higher_order(), control.clone());
    validate_frozen_call_effects(&owner, annotated.input(), &frozen, control.as_ref()).unwrap();
    assert_eq!(owner.refinements.load(Ordering::Relaxed), 1);
}

fn metadata_type(size: usize) -> FunctionValueType {
    let fields = (0..20)
        .map(|i| {
            Arc::new(
                Field::new(format!("field-{i}"), DataType::Int32, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.payload".into(),
                        "m".repeat(size),
                    )]),
                ),
            )
        })
        .collect::<Vec<_>>();
    ty(DataType::Struct(fields.into()), false)
}

#[test]
fn request_selected_result_and_expected_hold_metadata_free_value_facts() {
    let annotated =
        metadata_type(novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1);
    let plain = metadata_type(0);
    assert_eq!(annotated, plain);
    for channel in 0..4 {
        let mut fixture = Fixture::new(vec![value(plain.clone())]);
        match channel {
            0 => fixture.arguments[0] = value(annotated.clone()),
            1 => replace_selected_argument_fixture(
                &mut fixture.selected.argument_types,
                0,
                FunctionArgumentType::Value(annotated.clone()),
            ),
            2 => fixture.selected.result_type = FunctionResultType::Scalar(annotated.clone()),
            _ => fixture.expected = Some(annotated.clone()),
        }
        let control = Arc::new(Control::default());
        let owner = Owner::new(ArgumentControl::Eager, control.clone());
        refine_call_effects(&owner, fixture.input(), control.as_ref()).unwrap();
        assert_eq!(owner.refinements.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn owner_specific_expected_result_matching_is_not_replaced_by_a_generic_rule() {
    let mut fixture = Fixture::new(vec![value(ty(DataType::Int64, false))]);
    // This owner deliberately has no result-constraint capability. The shared
    // gate validates the supplied type without inventing a matching policy.
    fixture.expected = Some(json(true));
    let control = Arc::new(Control::default());
    let owner = Owner::new(ArgumentControl::Eager, control.clone());
    refine_call_effects(&owner, fixture.input(), control.as_ref()).unwrap();
    assert_eq!(owner.refinements.load(Ordering::Relaxed), 1);
}

#[test]
fn lambda_parameter_resource_failure_is_early_and_typed() {
    let fixture = Fixture::new(vec![FunctionArgument::Lambda {
        parameter_types: vec![ty(DataType::Int64, false); MAX_CALL_EFFECT_ARGUMENTS + 1].into(),
        result_type: ty(DataType::Boolean, false),
    }]);
    let control = Arc::new(Control::default());
    let owner = Owner::new(higher_order(), control.clone());
    assert!(matches!(
        refine_call_effects(&owner, fixture.input(), control.as_ref()),
        Err(CallEffectRefinementError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    owner.untouched();
}

#[test]
fn actual_source_work_entry_256_and_gate_finish_keep_original_control_before_owner() {
    for fixture in [
        Fixture::new(vec![value(ty(DataType::Int64, false)); 320]),
        Fixture::new(vec![lambda(metadata_type(16 * 1024), json(true))]),
    ] {
        let mode = if fixture.arguments.len() == 320 {
            ArgumentControl::Eager
        } else {
            higher_order()
        };
        let good = Arc::new(Control::default());
        let owner = Owner::new(mode, good.clone());
        refine_call_effects(&owner, fixture.input(), good.as_ref()).unwrap();
        let checks = good.checks();
        let declaration_count = owner.first_declaration_check_count.load(Ordering::Relaxed);
        // The source gate's finish is immediately followed by the next scope's
        // entry. Observe the real trace instead of guessing a work total.
        let finish = declaration_count - 2;
        let full = checks[..finish]
            .iter()
            .position(|units| *units == 256)
            .unwrap();
        assert_eq!(checks[0], 0);
        assert!(checks[finish] < 256);
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in [0, full, finish] {
                let control = Arc::new(Control {
                    checks: Mutex::default(),
                    failure: Some((at, error.clone())),
                });
                let owner = Owner::new(mode, control.clone());
                assert!(
                    matches!(refine_call_effects(&owner, fixture.input(), control.as_ref()), Err(CallEffectRefinementError::Control(actual)) if actual == error)
                );
                assert_eq!(control.checks(), checks[..=at]);
                owner.untouched();
            }
        }
    }
}

// Publish a new immutable test declaration after changing one selected fact.
fn replace_selected_argument_fixture(
    arguments: &mut novarocks_type_contract::FunctionArgumentTypes,
    ordinal: usize,
    argument: FunctionArgumentType,
) {
    let mut revised = arguments.as_slice().to_vec();
    revised[ordinal] = argument;
    *arguments =
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(revised.into_boxed_slice());
}
