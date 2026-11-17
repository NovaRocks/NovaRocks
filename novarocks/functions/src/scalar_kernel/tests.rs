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
use crate::{FunctionOverloadId, KernelDiagnostic, RowDataError};
use arrow_array::types::{Int8Type, Int16Type};
use arrow_array::{Array, DictionaryArray, RunArray};
use arrow_array::{
    ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, NullArray, StringArray,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, EvaluationDemand, EvaluationRegionId, ExpressionUseId,
    FunctionFailureBehavior, FunctionInstanceState, FunctionNullBehavior, FunctionVolatility,
    ObservableEffects,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

fn contract(
    control: ArgumentControl,
    row_error: FunctionIntrinsicRowError,
    nullable: bool,
) -> Arc<ScalarCallContract> {
    contract_with_argument_nullability(control, row_error, nullable, true)
}
fn contract_with_argument_nullability(
    control: ArgumentControl,
    row_error: FunctionIntrinsicRowError,
    nullable: bool,
    argument_nullable: bool,
) -> Arc<ScalarCallContract> {
    use crate::{FunctionArgument, FunctionBindingRequest, FunctionEffectOwnerError};
    use novarocks_type_contract::FunctionEffectDeclaration;
    struct FixtureOwner {
        selected: Arc<FunctionBindingSelection>,
        base: FunctionEffectDeclaration,
    }
    impl FunctionEffectOwner for FixtureOwner {
        type Error = FunctionBindingError;
        fn declaration(
            &self,
            _: &FunctionId,
            _: &FunctionBindingSelection,
        ) -> Result<&FunctionEffectDeclaration, Self::Error> {
            Ok(&self.base)
        }
        fn validate_and_refine(
            &self,
            input: CallEffectInput<'_>,
            _: &dyn PureCompileControl,
        ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
            assert!(std::ptr::eq(input.selected, self.selected.as_ref()));
            assert_eq!(
                input.request.arguments[0].argument_type(),
                self.selected.argument_types[0]
            );
            Ok(CallEffects {
                value_stability: self.base.value_stability,
                own_row_error: self.base.own_row_error,
                failure_behavior: self.base.failure_behavior,
                null_behavior: self.base.null_behavior,
                argument_control: self.base.argument_control,
                instance_state: self.base.instance_state,
                observable_effects: self.base.observable_effects,
                environment: Box::default(),
                proof_scope: input.proof_scope,
            })
        }
    }
    struct CompileControl;
    impl PureCompileControl for CompileControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    let function_id = FunctionId::try_new("fixture/selected-state").unwrap();
    let argument_type = FunctionValueType::new(DataType::Int64, argument_nullable);
    let selected = Arc::new(FunctionBindingSelection {
        overload: FunctionOverloadId::try_new("fixture/i64").unwrap(),
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            vec![FunctionArgumentType::Value(argument_type.clone())].into_boxed_slice(),
        ),
        result_type: FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, nullable)),
        aggregate: None,
    });
    let owner = FixtureOwner {
        selected: selected.clone(),
        base: FunctionEffectDeclaration {
            value_stability: if control == ArgumentControl::TypeOnly {
                FunctionVolatility::Immutable
            } else {
                FunctionVolatility::Volatile
            },
            own_row_error: row_error,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::CalledOnNull,
            argument_control: control,
            instance_state: if control == ArgumentControl::TypeOnly {
                FunctionInstanceState::None
            } else {
                FunctionInstanceState::ScalarInstance
            },
            observable_effects: ObservableEffects {
                rng_sampling: control != ArgumentControl::TypeOnly,
                warnings: false,
                controlled_wait: false,
            },
            environment_dependencies: Box::default(),
        },
    };
    let args = [FunctionArgument::Value {
        value_type: argument_type,
        constant: None,
    }];
    let uses = [if control == ArgumentControl::TypeOnly {
        None
    } else {
        Some(ExpressionUseId::new(10))
    }];
    let parameters = SemanticParameters::default();
    let input = CallEffectInput {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(7),
            region: EvaluationRegionId::new(9),
            demand: EvaluationDemand::Value,
        },
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: &function_id,
        kind: FunctionKind::Scalar,
        selected: selected.as_ref(),
        request: FunctionBindingRequest {
            expected_result_type: None,
            arguments: &args,
            logical_argument_count: 1,
        },
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    };
    let receipt = crate::refine_call_effects(&owner, input, &CompileControl).unwrap();
    Arc::new(
        ScalarCallContract::from_refined(input, &receipt, selected.clone(), &CompileControl)
            .unwrap(),
    )
}
#[derive(Clone, Copy, Debug)]
enum Mode {
    Good,
    EchoConstant,
    RowError,
    Null,
    WrongSelection,
    WrongType,
    OuterFailure,
    Grow,
    GrowAndFail,
    GrowAndCancel,
    GrowAndDeadline,
    GrowAndResource,
}
#[derive(Debug)]
struct Prepared {
    contract: Arc<ScalarCallContract>,
    calls: Arc<AtomicUsize>,
    mode: Mode,
}
struct Instance {
    calls: Arc<AtomicUsize>,
    next: i64,
    mode: Mode,
    extra_bytes: usize,
}
impl PreparedScalarKernel for Prepared {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<Instance>()
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(Instance {
            calls: self.calls.clone(),
            next: 0,
            mode: self.mode,
            extra_bytes: 0,
        }))
    }
}
impl ScalarKernelInstance for Instance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if input.contract().effects().argument_control != ArgumentControl::TypeOnly {
            self.next += 1;
        }
        if matches!(
            self.mode,
            Mode::Grow
                | Mode::GrowAndFail
                | Mode::GrowAndCancel
                | Mode::GrowAndDeadline
                | Mode::GrowAndResource
        ) {
            self.extra_bytes = 1;
        }
        if matches!(self.mode, Mode::OuterFailure | Mode::GrowAndFail) {
            return Err(KernelFailure::Operational(KernelDiagnostic::new(
                "fixture exit",
            )));
        }
        match self.mode {
            Mode::GrowAndCancel => return Err(KernelFailure::Cancelled),
            Mode::GrowAndDeadline => return Err(KernelFailure::DeadlineExceeded),
            Mode::GrowAndResource => return Err(KernelFailure::ResourceExhausted),
            _ => {}
        }
        let rows = input.selection().len();
        let array: ArrayRef = match self.mode {
            Mode::EchoConstant => {
                let argument = input.arguments()[0];
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                Arc::new(Int64Array::from(
                    input
                        .selection()
                        .iter()
                        .enumerate()
                        .map(|(ordinal, row)| {
                            let index = argument.value_row(ordinal, row);
                            (!array.is_null(index)).then(|| array.value(index))
                        })
                        .collect::<Vec<_>>(),
                ))
            }
            Mode::RowError | Mode::Null => Arc::new(Int64Array::from(vec![None; rows])),
            Mode::WrongType => Arc::new(Int32Array::from(vec![1; rows])),
            _ => Arc::new(Int64Array::from(vec![self.next; rows])),
        };
        let selection = if matches!(self.mode, Mode::WrongSelection) {
            Selection::all(rows)
        } else {
            input.selection()
        };
        let errors = if matches!(self.mode, Mode::RowError) {
            (0..rows)
                .map(|ordinal| RowDataError::new(ordinal, "fixture row data"))
                .collect::<Vec<_>>()
                .into_boxed_slice()
        } else {
            Box::default()
        };
        SelectedValues::try_new(selection, array.data_type(), array.clone(), errors)
            .map_err(|_| internal("fixture output shape"))
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.extra_bytes
    }
}
#[derive(Default)]
struct Control {
    work: Mutex<Vec<u32>>,
    fail_positive: bool,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        self.work.lock().unwrap().push(units);
        if self.fail_positive && units > 0 {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        Err(internal("fixture must never wait"))
    }
}
fn preparation(
    control: ArgumentControl,
    mode: Mode,
    row_error: FunctionIntrinsicRowError,
    nullable: bool,
) -> (Arc<dyn PreparedScalarKernel>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Prepared {
            contract: contract(control, row_error, nullable),
            calls: calls.clone(),
            mode,
        }),
        calls,
    )
}

#[test]
fn selected_invocations_keep_instance_state_and_empty_calls_do_not_advance_it() {
    let (prepared, calls) = preparation(
        ArgumentControl::Eager,
        Mode::Good,
        FunctionIntrinsicRowError::NoRowError,
        false,
    );
    let mut first = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
    let mut second = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1; 10]));
    let arguments = [EvaluatedArgument::Column(&input)];
    let selected = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
    let control = Control::default();
    for (first_instance, expected) in [(true, 1), (true, 2), (false, 1)] {
        let instance = if first_instance {
            &mut first
        } else {
            &mut second
        };
        let output = instance.evaluate(selected, &arguments, &control).unwrap();
        assert_eq!(output.selection(), selected);
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[expected; 3]
        );
    }
    first
        .evaluate(
            Selection::try_sparse(10, &[]).unwrap(),
            &arguments,
            &control,
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 3);
    let output = first.evaluate(selected, &arguments, &control).unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[test]
fn type_only_keeps_static_signature_and_has_no_evaluated_arguments() {
    let (prepared, calls) = preparation(
        ArgumentControl::TypeOnly,
        Mode::Good,
        FunctionIntrinsicRowError::NoRowError,
        false,
    );
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    assert_eq!(instance.contract().selected().argument_types.len(), 1);
    assert_eq!(instance.contract().value_argument_types().len(), 0);
    instance
        .evaluate(Selection::all(3), &[], &Control::default())
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn outer_failures_and_owner_contract_violations_latch_without_replaying_effects() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1; 10]));
    let arguments = [EvaluatedArgument::Column(&input)];
    let selected = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
    for mode in [
        Mode::RowError,
        Mode::Null,
        Mode::WrongSelection,
        Mode::WrongType,
        Mode::OuterFailure,
        Mode::Grow,
        Mode::GrowAndFail,
    ] {
        let (prepared, calls) = preparation(
            ArgumentControl::Eager,
            mode,
            FunctionIntrinsicRowError::NoRowError,
            false,
        );
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let _first = instance
            .evaluate(selected, &arguments, &Control::default())
            .unwrap_err();
        assert_eq!(
            instance
                .evaluate(selected, &arguments, &Control::default())
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
    // A declared row data error is a normal result, including its NULL
    // placeholder even when successful results are non-null.
    let (prepared, calls) = preparation(
        ArgumentControl::Eager,
        Mode::RowError,
        FunctionIntrinsicRowError::MayRaise,
        false,
    );
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    for _ in 0..2 {
        assert_eq!(
            instance
                .evaluate(selected, &arguments, &Control::default())
                .unwrap()
                .errors()
                .len(),
            3
        );
    }
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[test]
fn invalid_child_errors_and_interruption_never_reach_the_instance() {
    let selection = Selection::all(1);
    let child = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        Arc::new(Int64Array::from(vec![None])),
        Box::from([RowDataError::new(0, "child")]),
    )
    .unwrap();
    let arguments = [EvaluatedArgument::SelectedColumn(&child)];
    let (prepared, calls) = preparation(
        ArgumentControl::Eager,
        Mode::Good,
        FunctionIntrinsicRowError::NoRowError,
        false,
    );
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    assert!(matches!(
        instance.evaluate(selection, &arguments, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let (prepared, calls) = preparation(
        ArgumentControl::Eager,
        Mode::Good,
        FunctionIntrinsicRowError::NoRowError,
        false,
    );
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let arguments = [EvaluatedArgument::Column(&input)];
    assert!(matches!(
        instance.evaluate(
            selection,
            &arguments,
            &Control {
                work: Mutex::default(),
                fail_positive: true
            }
        ),
        Err(KernelFailure::Cancelled)
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert!(matches!(
        instance.evaluate(selection, &arguments, &Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
}

#[test]
fn null_inspection_handles_encoded_carriers_without_materializing_logical_bitmaps() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
    let dictionary =
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0), Some(1), None]), values)
            .unwrap();
    let run_ends = Int16Array::from(vec![2, 5]);
    let run_values = Int64Array::from(vec![Some(1), None]);
    let runs = RunArray::<Int16Type>::try_new(&run_ends, &run_values).unwrap();
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    assert!(!logical_is_null(&dictionary, 0, 1, &mut work).unwrap());
    assert!(logical_is_null(&dictionary, 1, 1, &mut work).unwrap());
    assert!(logical_is_null(&dictionary, 2, 1, &mut work).unwrap());
    let sliced = runs.slice(1, 3);
    assert!(!logical_is_null(&sliced, 0, 1, &mut work).unwrap());
    assert!(logical_is_null(&sliced, 1, 1, &mut work).unwrap());
    assert!(logical_is_null(&NullArray::new(3), 2, 1, &mut work).unwrap());
    work.finish().unwrap();
    let units = control.work.lock().unwrap();
    assert!(units.iter().sum::<u32>() >= 6);
}

#[test]
fn kernel_diagnostics_are_bounded_and_work_observation_is_not_zero_only() {
    assert_eq!(
        KernelDiagnostic::new(&"界".repeat(512)).message().len(),
        510
    );
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    for _ in 0..1025 {
        work.step().unwrap();
    }
    work.finish().unwrap();
    let units = control.work.lock().unwrap();
    assert_eq!(units.iter().sum::<u32>(), 1025);
    assert!(
        units
            .iter()
            .all(|units| *units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK)
    );
}

#[test]
fn nonnullable_arguments_check_only_selected_logical_values() {
    let frozen = contract_with_argument_nullability(
        ArgumentControl::Eager,
        FunctionIntrinsicRowError::NoRowError,
        false,
        false,
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let prepared: Arc<dyn PreparedScalarKernel> = Arc::new(Prepared {
        contract: frozen,
        calls: calls.clone(),
        mode: Mode::Good,
    });
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    assert_eq!(
        instance.retained_bytes().unwrap(),
        instance.retained_upper_bound()
    );
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let arguments = [EvaluatedArgument::Column(&input)];
    instance
        .evaluate(
            Selection::try_sparse(3, &[0, 2]).unwrap(),
            &arguments,
            &Control::default(),
        )
        .unwrap();
    assert!(matches!(
        instance.evaluate(
            Selection::try_sparse(3, &[1]).unwrap(),
            &arguments,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        instance.retained_bytes().unwrap(),
        instance.retained_upper_bound()
    );
}

#[test]
fn specialization_uses_one_exact_owner_and_preserves_checked_signature_backing() {
    use crate::{FunctionArgument, FunctionBindingRequest, FunctionEffectOwnerError};
    use novarocks_type_contract::FunctionEffectDeclaration;
    struct Owner {
        base: FunctionEffectDeclaration,
        wrong_contract: bool,
        validations: AtomicUsize,
    }
    impl FunctionBindingResolver for Owner {
        fn resolve(
            &self,
            _: FunctionBindingRequest<'_>,
            _control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<FunctionBindingSelection, FunctionBindingError> {
            panic!("specialization must not resolve an overload")
        }
        fn validate_selected(
            &self,
            selected: &FunctionBindingSelection,
            request: FunctionBindingRequest<'_>,
            _control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<(), FunctionBindingError> {
            self.validations.fetch_add(1, Ordering::Relaxed);
            if selected.overload.as_str() != "fixture/i64" || request.arguments.len() != 1 {
                return Err(FunctionBindingError::UnknownFunction);
            }
            Ok(())
        }
    }
    impl FunctionEffectOwner for Owner {
        type Error = FunctionBindingError;
        fn declaration(
            &self,
            function: &FunctionId,
            _: &FunctionBindingSelection,
        ) -> Result<&FunctionEffectDeclaration, Self::Error> {
            if function.as_str() != "fixture/selected-state" {
                return Err(FunctionBindingError::UnknownFunction);
            }
            Ok(&self.base)
        }
        fn validate_and_refine(
            &self,
            input: CallEffectInput<'_>,
            control: &dyn PureCompileControl,
        ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
            control
                .checkpoint(CompilePhase::FunctionSpecialization, 0)
                .map_err(FunctionEffectOwnerError::Control)?;
            self.validate_selected(input.selected, input.request, control)?;
            Ok(CallEffects {
                value_stability: self.base.value_stability,
                own_row_error: self.base.own_row_error,
                failure_behavior: self.base.failure_behavior,
                null_behavior: self.base.null_behavior,
                argument_control: self.base.argument_control,
                instance_state: self.base.instance_state,
                observable_effects: self.base.observable_effects,
                environment: input.environment.into(),
                proof_scope: input.proof_scope,
            })
        }
    }
    impl PureScalarImplementation for Owner {
        fn prepare_scalar(
            &self,
            _: CallEffectInput<'_>,
            contract: Arc<ScalarCallContract>,
            _: &dyn PureCompileControl,
        ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
            let contract = if self.wrong_contract {
                Arc::new((*contract).clone())
            } else {
                contract
            };
            Ok(Arc::new(Prepared {
                contract,
                calls: Arc::new(AtomicUsize::new(0)),
                mode: Mode::Good,
            }))
        }
    }
    struct CompileControl;
    impl PureCompileControl for CompileControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    let frozen = contract(
        ArgumentControl::Eager,
        FunctionIntrinsicRowError::NoRowError,
        false,
    );
    let args = [FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Int64, true),
        constant: None,
    }];
    let argument_uses = [Some(ExpressionUseId::new(10))];
    let input = CallEffectInput {
        context: frozen.context(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: frozen.function_id(),
        kind: FunctionKind::Scalar,
        selected: frozen.selected(),
        request: FunctionBindingRequest {
            expected_result_type: None,
            arguments: &args,
            logical_argument_count: 1,
        },
        environment: &[],
        parameters: frozen.parameters(),
        decimal_overflow_policy: frozen.decimal_overflow_policy(),
        proof_scope: CallProofScope::Unconditional,
    };
    for wrong_contract in [false, true] {
        let owner = Owner {
            base: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Volatile,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Eager,
                instance_state: FunctionInstanceState::ScalarInstance,
                observable_effects: frozen.effects().observable_effects,
                environment_dependencies: Box::default(),
            },
            wrong_contract,
            validations: AtomicUsize::new(0),
        };
        let prepared = specialize_scalar(
            &owner,
            input,
            frozen.call().selected_owner().clone(),
            ScopedExpressionEffects::pure_value(input.context),
            &CompileControl,
        );
        assert_eq!(owner.validations.load(Ordering::Relaxed), 1);
        if wrong_contract {
            assert!(matches!(
                prepared,
                Err(FunctionSpecializationFailure::Kernel(
                    KernelFailure::Internal(_)
                ))
            ));
        } else {
            let prepared = prepared.unwrap().into_prepared();
            assert!(Arc::ptr_eq(
                prepared.contract().call().selected_owner(),
                frozen.call().selected_owner()
            ));
            assert_eq!(prepared.contract().context(), frozen.context());
            assert_eq!(
                prepared.contract().decimal_overflow_policy(),
                frozen.decimal_overflow_policy()
            );
        }
    }
}

#[test]
fn unrepresentable_lifetime_bound_fails_before_state_creation() {
    #[derive(Debug)]
    struct Unrepresentable {
        contract: Arc<ScalarCallContract>,
        creations: Arc<AtomicUsize>,
    }
    impl PreparedScalarKernel for Unrepresentable {
        fn contract(&self) -> &Arc<ScalarCallContract> {
            &self.contract
        }
        fn instance_retained_upper_bound(&self) -> usize {
            usize::MAX
        }
        fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
            self.creations.fetch_add(1, Ordering::Relaxed);
            Err(KernelFailure::Cancelled)
        }
    }
    let creations = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(Unrepresentable {
        contract: contract(
            ArgumentControl::Eager,
            FunctionIntrinsicRowError::NoRowError,
            true,
        ),
        creations: creations.clone(),
    });
    assert!(matches!(
        ScalarEvaluationInstance::instantiate(prepared),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(creations.load(Ordering::Relaxed), 0);
}

#[test]
fn prepared_scalar_reads_only_the_explicit_constant_ordinal_for_every_selected_row() {
    struct ConstantCompileControl;
    impl PureCompileControl for ConstantCompileControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    let policy = crate::ConstantPolicy {
        max_rows: 100,
        max_array_nodes: 100,
        max_logical_elements: 100,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 64 * 1024,
        max_library_validation_work: 1024 * 1024,
        max_library_validation_bytes: 1024 * 1024,
    };
    let ty = FunctionValueType::new(DataType::Int64, true);
    let pool = crate::ConstantPool::try_new(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty,
        Int64Array::from(vec![None, Some(7), Some(91)]).to_data(),
        policy,
        CompilePhase::FunctionSpecialization,
        &ConstantCompileControl,
    )
    .unwrap();
    let (prepared, calls) = preparation(
        ArgumentControl::Eager,
        Mode::EchoConstant,
        FunctionIntrinsicRowError::NoRowError,
        true,
    );
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let selection = Selection::try_sparse(1000, &[0, 399, 999]).unwrap();
    for (ordinal, expected) in [(1, Some(7)), (0, None), (2, Some(91))] {
        let value = pool.value(ordinal).unwrap();
        let argument = EvaluatedArgument::Constant(&value);
        assert!(Arc::ptr_eq(argument.array(), pool.array()));
        let arguments = [argument];
        let result = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![expected; 3]
        );
    }
    assert_eq!(calls.load(Ordering::Relaxed), 3);
    let value = pool.value(1).unwrap();
    let arguments = [EvaluatedArgument::Constant(&value)];
    let result = instance
        .evaluate(Selection::all(0), &arguments, &Control::default())
        .unwrap();
    assert!(result.values().is_empty());
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[test]
fn scalar_instance_drop_keeps_the_last_prepared_owner_alive() {
    #[derive(Debug)]
    struct DropPrepared {
        contract: Arc<ScalarCallContract>,
        owner: std::sync::Weak<DropPrepared>,
        drops: Arc<Mutex<Vec<&'static str>>>,
    }
    struct DropInstance {
        owner: std::sync::Weak<DropPrepared>,
        drops: Arc<Mutex<Vec<&'static str>>>,
    }
    impl Drop for DropPrepared {
        fn drop(&mut self) {
            self.drops.lock().unwrap().push("prepared");
        }
    }
    impl Drop for DropInstance {
        fn drop(&mut self) {
            assert!(
                self.owner.upgrade().is_some(),
                "the prepared owner must remain live throughout typed instance teardown"
            );
            self.drops.lock().unwrap().push("instance");
        }
    }
    impl PreparedScalarKernel for DropPrepared {
        fn contract(&self) -> &Arc<ScalarCallContract> {
            &self.contract
        }
        fn instance_retained_upper_bound(&self) -> usize {
            std::mem::size_of::<DropInstance>()
        }
        fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
            Ok(Box::new(DropInstance {
                owner: self.owner.clone(),
                drops: self.drops.clone(),
            }))
        }
    }
    impl ScalarKernelInstance for DropInstance {
        fn evaluate<'a>(
            &mut self,
            _: ScalarCallInput<'_, 'a>,
            _: &dyn KernelEvaluationControl,
        ) -> Result<SelectedValues<'a>, KernelFailure> {
            Err(internal("drop fixture must never evaluate"))
        }
        fn retained_bytes(&self) -> usize {
            std::mem::size_of::<Self>()
        }
    }

    let drops = Arc::new(Mutex::new(Vec::new()));
    let prepared = Arc::new_cyclic(|owner| DropPrepared {
        contract: contract(
            ArgumentControl::Eager,
            FunctionIntrinsicRowError::NoRowError,
            false,
        ),
        owner: owner.clone(),
        drops: drops.clone(),
    });
    let weak = Arc::downgrade(&prepared);
    let instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    assert_eq!(weak.strong_count(), 1);
    assert!(drops.lock().unwrap().is_empty());
    drop(instance);
    assert!(weak.upgrade().is_none());
    assert_eq!(*drops.lock().unwrap(), vec!["instance", "prepared"]);
}

#[test]
fn primary_scalar_control_survives_retained_growth_and_latches_without_replay() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1; 10]));
    let arguments = [EvaluatedArgument::Column(&input)];
    let selected = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
    for (mode, expected) in [
        (Mode::GrowAndCancel, KernelFailure::Cancelled),
        (Mode::GrowAndDeadline, KernelFailure::DeadlineExceeded),
        (Mode::GrowAndResource, KernelFailure::ResourceExhausted),
        (
            Mode::GrowAndFail,
            KernelFailure::Operational(KernelDiagnostic::new("fixture exit")),
        ),
    ] {
        let (prepared, calls) = preparation(
            ArgumentControl::Eager,
            mode,
            FunctionIntrinsicRowError::NoRowError,
            false,
        );
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        assert_eq!(
            instance
                .evaluate(selected, &arguments, &Control::default())
                .unwrap_err(),
            expected,
        );
        // The real mutable fixture grew beyond its immutable preparation bound.
        assert!(matches!(
            instance.retained_bytes(),
            Err(KernelFailure::Internal(_))
        ));
        assert_eq!(
            instance
                .evaluate(selected, &arguments, &Control::default())
                .unwrap_err(),
            KernelFailure::InstanceFailed,
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
    // A successful implementation still has to obey its retained contract.
    {
        let mode = Mode::Grow;
        let (prepared, calls) = preparation(
            ArgumentControl::Eager,
            mode,
            FunctionIntrinsicRowError::NoRowError,
            false,
        );
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        assert!(matches!(
            instance.evaluate(selected, &arguments, &Control::default()),
            Err(KernelFailure::Internal(_)),
        ));
        assert_eq!(
            instance
                .evaluate(selected, &arguments, &Control::default())
                .unwrap_err(),
            KernelFailure::InstanceFailed,
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn scalar_original_source_survives_real_retained_growth_and_latches_without_replay() {
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    #[derive(Debug)]
    struct OriginalPrepared {
        contract: Arc<ScalarCallContract>,
        source: KernelFailure,
        calls: Arc<AtomicUsize>,
        retained_reads: Arc<AtomicUsize>,
    }
    struct OriginalInstance {
        source: KernelFailure,
        calls: Arc<AtomicUsize>,
        retained_reads: Arc<AtomicUsize>,
        growth: Vec<u8>,
    }
    impl PreparedScalarKernel for OriginalPrepared {
        fn contract(&self) -> &Arc<ScalarCallContract> {
            &self.contract
        }
        fn instance_retained_upper_bound(&self) -> usize {
            std::mem::size_of::<OriginalInstance>()
        }
        fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
            Ok(Box::new(OriginalInstance {
                source: self.source.clone(),
                calls: self.calls.clone(),
                retained_reads: self.retained_reads.clone(),
                growth: Vec::new(),
            }))
        }
    }
    impl ScalarKernelInstance for OriginalInstance {
        fn evaluate<'a>(
            &mut self,
            _: ScalarCallInput<'_, 'a>,
            _: &dyn KernelEvaluationControl,
        ) -> Result<SelectedValues<'a>, KernelFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // A real retained-contract fault after an originating domain
            // failure, not a legal source invoice or allocation grant.
            self.growth.push(1);
            Err(self.source.clone())
        }
        fn retained_bytes(&self) -> usize {
            self.retained_reads.fetch_add(1, Ordering::SeqCst);
            std::mem::size_of::<Self>() + self.growth.capacity()
        }
    }
    let expected = KernelFailure::from_domain(LayoutResourceError::SourceModel);
    let calls = Arc::new(AtomicUsize::new(0));
    let retained_reads = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(OriginalPrepared {
        contract: contract(
            ArgumentControl::Eager,
            FunctionIntrinsicRowError::NoRowError,
            false,
        ),
        source: expected.clone(),
        calls: calls.clone(),
        retained_reads: retained_reads.clone(),
    });
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let arguments = [EvaluatedArgument::Column(&input)];
    let actual = instance
        .evaluate(Selection::all(1), &arguments, &Control::default())
        .unwrap_err();
    // Source equality compares the original shared-cause Arc, not text/facts.
    assert_eq!(actual, expected);
    assert_eq!(retained_reads.load(Ordering::SeqCst), 1);
    // An explicit diagnostic read still observes the real contract violation.
    assert!(matches!(
        instance.retained_bytes(),
        Err(KernelFailure::Internal(_))
    ));
    assert_eq!(
        instance
            .evaluate(Selection::all(1), &arguments, &Control::default())
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
