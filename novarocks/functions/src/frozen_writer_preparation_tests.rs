// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::{FunctionArgument, FunctionBindingRequest, PreparedPureKernel};
use arrow_schema::DataType;
use novarocks_type_contract::{CallProofScope, CompileControlError, EvaluationRegionId};
use std::sync::Mutex;
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
struct Source {
    catalog: EngineFunctionCatalog,
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    ty: FunctionValueType,
    call: RelationEffectContext,
    input: RelationEffectContext,
    parameters: SemanticParameters,
    frozen: CallEffects,
}
impl Source {
    fn new() -> Self {
        Self::with_type(FunctionValueType::new(DataType::Int64, true))
    }
    fn with_type(ty: FunctionValueType) -> Self {
        let catalog = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let arguments = vec![FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        }];
        let call = RelationEffectContext::call(novarocks_type_contract::RelationCallId::new(41));
        let input = RelationEffectContext::materialized_input(
            novarocks_type_contract::RelationCallId::new(41),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let request = FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: 1,
            expected_result_type: None,
        };
        let resolved = catalog
            .resolve_bound_user(
                "count",
                FunctionKind::Aggregate,
                request,
                &Control::default(),
            )
            .unwrap();
        let selected = Arc::new(resolved.selected);
        let function = resolved.function_id;
        // This is the original FE-like installed author. The BE tests below
        // borrow its actual frozen effects, not a declared/default effect set.

        let authored = catalog
            .prepare_fresh_selected(
                CallEffectInput {
                    context: (call).into(),
                    argument_uses: CallArgumentUses::MaterializedInput {
                        context: input,
                        input_type: &ty,
                    },
                    function_id: &function,
                    kind: FunctionKind::Aggregate,
                    selected: &selected,
                    request,
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    proof_scope: CallProofScope::Relation(call.identity),
                },
                selected.clone(),
                PureCallPreparation::Aggregate {
                    arguments: ScopedRelationalEffects::primitive(
                        call,
                        ScopedRelationalEffects::pure_value(input)
                            .for_use(input)
                            .unwrap(),
                    ),
                    options: AggregatePreparationOptions {
                        state_interpretation: None,
                        phase: AggregateKernelPhase::Partial,
                        distinct: false,
                        order_keys: Arc::from([]),
                        state_input_type: None,
                    },
                },
                &Control::default(),
            )
            .unwrap();
        let frozen = authored.call_contract().effects().clone();
        Self {
            catalog,
            function,
            selected,
            arguments,
            ty,
            call,
            input,
            parameters,
            frozen,
        }
    }
    fn loan(&self) -> FrozenWriterAggregatePreparation<'_> {
        FrozenWriterAggregatePreparation {
            function_id: &self.function,
            selected: &self.selected,
            original_request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            call_context: self.call,
            materialized_input_context: self.input,
            frozen_effects: &self.frozen,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            parameters: &self.parameters,
            actual_input_type: &self.ty,
            phase: AggregateKernelPhase::Partial,
        }
    }
}
#[test]
fn frozen_writer_uses_actual_installed_call_and_rejects_context_or_effect_drift() {
    let source = Source::new();
    let prepared =
        prepare_frozen_writer_aggregate(&source.catalog, source.loan(), &Control::default())
            .unwrap();
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("actual aggregate handle");
    };
    assert_eq!(handle.contract().call().context(), source.call.into());
    assert_eq!(handle.contract().call().effects(), &source.frozen);
    assert_eq!(handle.contract().phase(), AggregateKernelPhase::Partial);
    assert_eq!(
        handle.contract().call().decimal_overflow_policy(),
        DecimalOverflowPolicy::OutputNull
    );
    let mut wrong = source.loan();
    wrong.materialized_input_context = source.call;
    assert!(matches!(
        prepare_frozen_writer_aggregate(&source.catalog, wrong, &Control::default()),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    let mut effects = source.frozen.clone();
    effects.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
    let mut wrong = source.loan();
    wrong.frozen_effects = &effects;
    assert!(prepare_frozen_writer_aggregate(&source.catalog, wrong, &Control::default()).is_err());
}
#[test]
fn frozen_writer_preparation_preserves_each_original_control_prefix_without_tail() {
    let source = Source::new();
    let baseline = Control::default();
    prepare_frozen_writer_aggregate(&source.catalog, source.loan(), &baseline).unwrap();
    let count = baseline.trace.into_inner().unwrap().len();
    assert!(count > 1);
    for stop in 0..count {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            let failure = prepare_frozen_writer_aggregate(&source.catalog, source.loan(), &control)
                .unwrap_err();
            match failure {
                FunctionSpecializationFailure::Control(actual) => assert_eq!(actual, cause),
                FunctionSpecializationFailure::Kernel(actual) => {
                    assert_eq!(actual, crate::KernelFailure::CompileControl(cause.clone()))
                }
                other => panic!("originating control changed category: {other:?}"),
            }
            assert_eq!(control.trace.into_inner().unwrap().len(), stop + 1);
        }
    }
}

#[test]
fn frozen_writer_rejects_plain_binary_for_json_and_keeps_independent_complete_equal_input() {
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let source = Source::with_type(json);
    let equal = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let binary = FunctionValueType::new(DataType::LargeBinary, true);
    assert_eq!(equal.data_type(), binary.data_type());
    let mut good = source.loan();
    good.actual_input_type = &equal;
    prepare_frozen_writer_aggregate(&source.catalog, good, &Control::default()).unwrap();
    let mut wrong = source.loan();
    wrong.actual_input_type = &binary;
    assert!(matches!(
        prepare_frozen_writer_aggregate(&source.catalog, wrong, &Control::default()),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
}

// This actual writer recipe borrows Count's immutable declaration. A scope
// that refuses every request proves this route needs no artificial grant.
#[derive(Debug)]
struct RequestRefusal;
struct RefusingScope {
    attempts: usize,
    cause: Arc<RequestRefusal>,
}
impl Default for RefusingScope {
    fn default() -> Self {
        Self {
            attempts: 0,
            cause: Arc::new(RequestRefusal),
        }
    }
}
impl novarocks_type_contract::ValueSchemaCreationScope<Arc<RequestRefusal>> for RefusingScope {
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        panic!("borrow-only Count must not request type storage lifetime")
    }
    fn request_layout(&mut self, _: std::alloc::Layout) -> Result<(), Arc<RequestRefusal>> {
        self.attempts += 1;
        Err(Arc::clone(&self.cause))
    }
    fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), Arc<RequestRefusal>> {
        self.attempts += 1;
        Err(Arc::clone(&self.cause))
    }
}
fn original_preparation_error(
    error: CheckedCallPreparationFailure<Arc<RequestRefusal>>,
) -> FunctionSpecializationFailure {
    match error {
        CheckedCallPreparationFailure::Preparation(crate::CheckedOwnerFailure::Original(error)) => {
            error
        }
        CheckedCallPreparationFailure::Binding(crate::FunctionBindingCreationFailure::Binding(
            error,
        )) => error.into(),
        other => panic!("actual borrowed Count route changed error category: {other:?}"),
    }
}

#[test]
fn frozen_writer_checked_count_borrows_its_original_owner_without_type_requests() {
    let source = Source::new();
    let required_input = FunctionValueType::new(DataType::Int64, false);
    let mut scope = RefusingScope::default();
    let mut good = source.loan();
    good.actual_input_type = &required_input;
    let prepared = prepare_frozen_writer_aggregate_with_creator_in(
        &source.catalog,
        good,
        &mut scope,
        &Control::default(),
    )
    .unwrap();
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("actual checked Count handle");
    };
    assert_eq!(handle.contract().call().context(), source.call.into());
    assert_eq!(handle.contract().call().effects(), &source.frozen);
    assert_eq!(handle.contract().phase(), AggregateKernelPhase::Partial);
    assert_eq!(
        handle.contract().call().decimal_overflow_policy(),
        DecimalOverflowPolicy::OutputNull
    );
    assert_eq!(scope.attempts, 0);
    // Preserve the actual caller-created order owner route too.
    prepare_frozen_writer_aggregate_with_empty_order_keys_and_creator_in(
        &source.catalog,
        source.loan(),
        Arc::from([]),
        &mut scope,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(scope.attempts, 0);
}

#[test]
fn frozen_writer_checked_preserves_phase_nullability_and_foreign_namespace_refusals() {
    let source = Source::new();
    let mut scope = RefusingScope::default();
    for phase in [
        AggregateKernelPhase::Single,
        AggregateKernelPhase::Intermediate,
    ] {
        let mut wrong = source.loan();
        wrong.phase = phase;
        assert!(matches!(
            original_preparation_error(
                prepare_frozen_writer_aggregate_with_creator_in(
                    &source.catalog,
                    wrong,
                    &mut scope,
                    &Control::default()
                )
                .unwrap_err()
            ),
            FunctionSpecializationFailure::InvalidInput(
                "writer preparation changes its original phase, scalar channel or Value scope"
            )
        ));
    }
    let mut wrong = source.loan();
    wrong.materialized_input_context = source.call;
    assert!(matches!(
        original_preparation_error(
            prepare_frozen_writer_aggregate_with_creator_in(
                &source.catalog,
                wrong,
                &mut scope,
                &Control::default()
            )
            .unwrap_err()
        ),
        FunctionSpecializationFailure::InvalidInput(
            "writer preparation changes its original phase, scalar channel or Value scope"
        )
    ));
    let required = Source::with_type(FunctionValueType::new(DataType::Int64, false));
    let nullable_input = FunctionValueType::new(DataType::Int64, true);
    let mut wrong = required.loan();
    wrong.actual_input_type = &nullable_input;
    assert!(matches!(
        original_preparation_error(
            prepare_frozen_writer_aggregate_with_creator_in(
                &required.catalog,
                wrong,
                &mut scope,
                &Control::default()
            )
            .unwrap_err()
        ),
        FunctionSpecializationFailure::InvalidInput(
            "writer materialized input differs from its complete logical channel"
        )
    ));
    let mut foreign = source.frozen.clone();
    foreign.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
    let mut wrong = source.loan();
    wrong.frozen_effects = &foreign;
    assert!(
        prepare_frozen_writer_aggregate_with_creator_in(
            &source.catalog,
            wrong,
            &mut scope,
            &Control::default()
        )
        .is_err()
    );
    assert_eq!(scope.attempts, 0);
}

#[test]
fn frozen_writer_checked_preserves_each_actual_control_prefix_without_tail() {
    let source = Source::new();
    let baseline = Control::default();
    let mut scope = RefusingScope::default();
    prepare_frozen_writer_aggregate_with_creator_in(
        &source.catalog,
        source.loan(),
        &mut scope,
        &baseline,
    )
    .unwrap();
    assert_eq!(scope.attempts, 0);
    let count = baseline.trace.into_inner().unwrap().len();
    assert!(count > 1);
    for stop in 0..count {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            let mut scope = RefusingScope::default();
            let failure = original_preparation_error(
                prepare_frozen_writer_aggregate_with_creator_in(
                    &source.catalog,
                    source.loan(),
                    &mut scope,
                    &control,
                )
                .unwrap_err(),
            );
            match failure {
                FunctionSpecializationFailure::Control(actual) => assert_eq!(actual, cause),
                FunctionSpecializationFailure::Kernel(actual) => {
                    assert_eq!(actual, crate::KernelFailure::CompileControl(cause.clone()))
                }
                other => panic!("originating checked control changed category: {other:?}"),
            }
            assert_eq!(control.trace.into_inner().unwrap().len(), stop + 1);
            assert_eq!(scope.attempts, 0);
        }
    }
}
