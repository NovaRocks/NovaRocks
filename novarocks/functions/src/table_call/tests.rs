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
    CallEffectInput, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingSelection, FunctionEffectOwner, FunctionEffectOwnerError, FunctionId,
    FunctionOverloadId, RowDataError, SelectedValues, refine_call_effects,
};
use arrow_array::{FixedSizeBinaryArray, Int32Array, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileControlError, DecimalOverflowPolicy, ExpressionUseId,
    FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameters, ValueLogicalType,
};
use std::{sync::Mutex, time::Duration};

struct CompileControl(Option<CompileControlError>);
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        self.0.clone().map_or(Ok(()), Err)
    }
}
struct RuntimeControl {
    failure: Option<KernelFailure>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl RuntimeControl {
    fn normal() -> Self {
        Self {
            failure: None,
            positive_only: false,
            work: Mutex::default(),
        }
    }
    fn fail(failure: KernelFailure, positive_only: bool) -> Self {
        Self {
            failure: Some(failure),
            positive_only,
            work: Mutex::default(),
        }
    }
    fn assert_quantum_failure(&self) {
        let work = self.work.lock().unwrap();
        let (quantum, entries) = work.split_last().expect("observed control failure");
        assert_eq!(*quantum, crate::MAX_UNOBSERVED_KERNEL_WORK);
        assert!(!entries.is_empty());
        assert!(entries.iter().all(|units| *units == 0));
    }
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        self.work.lock().unwrap().push(units);
        if (!self.positive_only || units > 0)
            && let Some(failure) = &self.failure
        {
            Err(failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("table input/page validation must not wait")
    }
}

struct Fixture {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    declaration: FunctionEffectDeclaration,
    parameters: SemanticParameters,
}
impl Fixture {
    fn new(arguments: &[FunctionValueType], results: &[FunctionValueType]) -> Self {
        let arguments = arguments
            .iter()
            .cloned()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        Self {
            id: FunctionId::try_new("fixture/table/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/table/exact-relation").unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Relation(
                    novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                        results.to_vec().into_boxed_slice(),
                    ),
                ),
                aggregate: None,
            }),
            uses: (0..arguments.len())
                .map(|ordinal| Some(ExpressionUseId::new(ordinal as u32 + 1)))
                .collect(),
            arguments,
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Table,
                instance_state: FunctionInstanceState::TableInstance,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            parameters: SemanticParameters::default(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: (novarocks_type_contract::RelationEffectContext::call(
                novarocks_type_contract::RelationCallId::new(0),
            ))
            .into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.id,
            kind: FunctionKind::Table,
            selected: self.selected.as_ref(),
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    fn call(&self) -> Arc<FunctionCallContract> {
        let input = self.input();
        let receipt = refine_call_effects(self, input, &CompileControl(None)).unwrap();
        Arc::new(
            FunctionCallContract::from_refined(
                input,
                &receipt,
                self.selected.clone(),
                &CompileControl(None),
            )
            .unwrap(),
        )
    }
    fn contract(&self) -> TableCallContract {
        TableCallContract::try_new(self.call(), &CompileControl(None)).unwrap()
    }
}
impl FunctionEffectOwner for Fixture {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || selected != self.selected.as_ref() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != &self.id
            || input.kind != FunctionKind::Table
            || input.selected != self.selected.as_ref()
            || input.request.logical_argument_count != self.arguments.len()
            || input.request.arguments.len() != self.arguments.len()
            || !input.environment.is_empty()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture exact table call differs".into(),
            )
            .into());
        }
        for (argument, expected) in input.request.arguments.iter().zip(&self.arguments) {
            if !argument.equals_observed(expected, CompilePhase::FunctionSpecialization, control)? {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture exact table argument differs".into(),
                )
                .into());
            }
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
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
fn i64_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}
fn values<'a>(selection: Selection<'a>, array: ArrayRef) -> SelectedValues<'a> {
    SelectedValues::try_new(selection, array.data_type(), array.clone(), Box::new([])).unwrap()
}
fn page<'call, 'a>(
    contract: &'call TableCallContract,
    selection: Selection<'a>,
    columns: &'a [ArrayRef],
    parents: &'a [usize],
    complete: &'a [usize],
    capacity: TablePageCapacity,
    control: &dyn KernelEvaluationControl,
) -> Result<TableOutputPage<'call, 'a>, KernelFailure> {
    TableOutputPage::try_new(
        contract, selection, columns, parents, complete, capacity, control,
    )
}
fn invalid_input<T: std::fmt::Debug>(result: Result<T, KernelFailure>) {
    assert!(
        matches!(result, Err(ref cause) if crate::kernel_control::test_contract::input(cause)),
        "{result:?}"
    );
}
fn internal_page<T: std::fmt::Debug>(result: Result<T, KernelFailure>) {
    assert!(
        matches!(result, Err(ref cause) if crate::kernel_control::test_contract::output(cause)),
        "{result:?}"
    );
}

#[test]
fn exact_table_contract_and_input_preserve_selected_arguments_and_nullability() {
    let fixture = Fixture::new(
        &[
            i64_type(false),
            FunctionValueType::new(DataType::Utf8, true),
        ],
        &[i64_type(false)],
    );
    let call = fixture.call();
    let contract = TableCallContract::try_new(call.clone(), &CompileControl(None)).unwrap();
    assert!(Arc::ptr_eq(contract.call(), &call));
    assert!(std::ptr::eq(
        contract.call().selected(),
        fixture.selected.as_ref()
    ));
    assert_eq!(contract.call().kind(), FunctionKind::Table);
    assert_eq!(
        contract.call().effects().argument_control,
        ArgumentControl::Table
    );
    assert_eq!(
        contract.call().effects().instance_state,
        FunctionInstanceState::TableInstance
    );
    assert!(contract.call().selected().aggregate.is_none());
    let rows = [1, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![
        None,
        Some(1),
        None,
        None,
        Some(4),
        None,
    ]));
    let scalar: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
    let arguments = [
        EvaluatedArgument::Column(&array),
        EvaluatedArgument::Scalar(&scalar),
    ];
    let input =
        SelectedTableInput::try_new(&contract, selection, &arguments, &RuntimeControl::normal())
            .unwrap();
    assert!(std::ptr::eq(input.contract(), &contract));
    assert_eq!(input.selection(), selection);
    assert!(std::ptr::eq(input.arguments(), arguments.as_slice()));
    invalid_input(SelectedTableInput::try_new(
        &contract,
        selection,
        &arguments[..1],
        &RuntimeControl::normal(),
    ));
    let selected_null: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(0),
        None,
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    invalid_input(SelectedTableInput::try_new(
        &contract,
        selection,
        &[EvaluatedArgument::Column(&selected_null), arguments[1]],
        &RuntimeControl::normal(),
    ));
    let nullable = Fixture::new(&[i64_type(true)], &[i64_type(false)]).contract();
    SelectedTableInput::try_new(
        &nullable,
        selection,
        &[EvaluatedArgument::Column(&selected_null)],
        &RuntimeControl::normal(),
    )
    .unwrap();
}

#[test]
fn input_rejects_wrong_selection_type_length_and_unresolved_child_row_errors() {
    let contract = Fixture::new(&[i64_type(true)], &[i64_type(false)]).contract();
    let rows = [1, 4];
    let other_rows = [0, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let good = values(selection, Arc::new(Int64Array::from(vec![1, 4])));
    SelectedTableInput::try_new(
        &contract,
        selection,
        &[EvaluatedArgument::SelectedColumn(&good)],
        &RuntimeControl::normal(),
    )
    .unwrap();
    let wrong = values(
        Selection::try_sparse(6, &other_rows).unwrap(),
        Arc::new(Int64Array::from(vec![1, 4])),
    );
    let wrong_type: ArrayRef = Arc::new(Int32Array::from(vec![1; 6]));
    let short: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let error_array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(4)]));
    let errors = SelectedValues::try_new(
        selection,
        error_array.data_type(),
        error_array.clone(),
        vec![RowDataError::new(0, "fixture unresolved child error")].into(),
    )
    .unwrap();
    for argument in [
        EvaluatedArgument::SelectedColumn(&wrong),
        EvaluatedArgument::Column(&wrong_type),
        EvaluatedArgument::Column(&short),
        EvaluatedArgument::SelectedColumn(&errors),
    ] {
        invalid_input(SelectedTableInput::try_new(
            &contract,
            selection,
            &[argument],
            &RuntimeControl::normal(),
        ));
    }
    SelectedTableInput::try_new(
        &contract,
        selection,
        &[EvaluatedArgument::Scalar(&short)],
        &RuntimeControl::normal(),
    )
    .unwrap();
}

#[test]
fn largeint_logical_domain_is_frozen_in_the_contract_not_guessed_from_binary16() {
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    assert!(!largeint.same_value_domain(&physical));
    let logical = Fixture::new(
        std::slice::from_ref(&largeint),
        std::slice::from_ref(&largeint),
    )
    .contract();
    let raw = Fixture::new(
        std::slice::from_ref(&physical),
        std::slice::from_ref(&physical),
    )
    .contract();
    assert_ne!(logical, raw);
    assert_eq!(logical.argument_types().next(), Some(&largeint));
    assert_eq!(logical.result_types(), [largeint]);
    assert_eq!(raw.argument_types().next(), Some(&physical));
    let binary: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter((0..2).map(|_| [0u8; 16])).unwrap());
    let args = [EvaluatedArgument::Column(&binary)];
    // The Arrow carrier deliberately cannot establish this root logical tag.
    SelectedTableInput::try_new(
        &logical,
        Selection::all(2),
        &args,
        &RuntimeControl::normal(),
    )
    .unwrap();
    SelectedTableInput::try_new(&raw, Selection::all(2), &args, &RuntimeControl::normal()).unwrap();
    page(
        &logical,
        Selection::all(2),
        &[binary],
        &[0, 1],
        &[],
        TablePageCapacity {
            rows: 2,
            completions: 0,
        },
        &RuntimeControl::normal(),
    )
    .unwrap();
}

#[test]
fn relation_only_page_maps_repeated_parent_ordinals_to_sparse_batch_rows() {
    let contract = Fixture::new(
        &[],
        &[
            i64_type(false),
            FunctionValueType::new(DataType::Utf8, false),
        ],
    )
    .contract();
    let rows = [1, 4, 9];
    let parents = Selection::try_sparse(10, &rows).unwrap();
    let ordinals = [2, 0, 2, 1];
    let completed = [0, 2];
    let columns: [ArrayRef; 2] = [
        Arc::new(Int64Array::from(vec![9, 1, 19, 4])),
        Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
    ];
    let output = page(
        &contract,
        parents,
        &columns,
        &ordinals,
        &completed,
        TablePageCapacity {
            rows: 4,
            completions: 2,
        },
        &RuntimeControl::normal(),
    )
    .unwrap();
    assert!(std::ptr::eq(output.contract(), &contract));
    assert_eq!(output.parents(), parents);
    assert!(std::ptr::eq(output.columns(), columns.as_slice()));
    assert_eq!(output.parent_ordinals(), ordinals);
    assert_eq!(output.completed_parents(), completed);
    assert_eq!(output.row_count(), 4);
    assert_eq!(
        (0..4)
            .map(|row| output.batch_parent(row).unwrap())
            .collect::<Vec<_>>(),
        [9, 1, 9, 4]
    );
    assert_eq!(output.batch_parent(4), None);
}

#[test]
fn completion_is_explicit_and_independent_of_zero_output_rows() {
    let contract = Fixture::new(&[], &[i64_type(false)]).contract();
    let empty: [ArrayRef; 1] = [Arc::new(Int64Array::from(Vec::<i64>::new()))];
    let parents = Selection::all(3);
    let unfinished = page(
        &contract,
        parents,
        &empty,
        &[],
        &[],
        TablePageCapacity {
            rows: 0,
            completions: 0,
        },
        &RuntimeControl::normal(),
    )
    .unwrap();
    assert_eq!(unfinished.row_count(), 0);
    assert!(unfinished.completed_parents().is_empty());
    let finished = page(
        &contract,
        parents,
        &empty,
        &[],
        &[1],
        TablePageCapacity {
            rows: 0,
            completions: 1,
        },
        &RuntimeControl::normal(),
    )
    .unwrap();
    assert_eq!(finished.row_count(), 0);
    assert_eq!(finished.completed_parents(), [1]);
    let columns: [ArrayRef; 1] = [Arc::new(Int64Array::from(vec![4]))];
    let emitting = page(
        &contract,
        parents,
        &columns,
        &[1],
        &[],
        TablePageCapacity {
            rows: 1,
            completions: 0,
        },
        &RuntimeControl::normal(),
    )
    .unwrap();
    assert_eq!(emitting.row_count(), 1);
    assert!(emitting.completed_parents().is_empty());
}

#[test]
fn page_rejects_bad_grants_parent_ranges_completion_shape_and_relation_carriers() {
    let contract = Fixture::new(&[], &[i64_type(false)]).contract();
    let parents = Selection::all(3);
    let good: [ArrayRef; 1] = [Arc::new(Int64Array::from(vec![1, 2]))];
    let cap = TablePageCapacity {
        rows: 2,
        completions: 2,
    };
    page(
        &contract,
        parents,
        &good,
        &[0, 2],
        &[0, 2],
        cap,
        &RuntimeControl::normal(),
    )
    .unwrap();
    for (ordinals, completed, capacity) in [
        (
            &[0, 2][..],
            &[0, 2][..],
            TablePageCapacity {
                rows: 1,
                completions: 2,
            },
        ),
        (
            &[0, 2],
            &[0, 2],
            TablePageCapacity {
                rows: 2,
                completions: 1,
            },
        ),
        (&[0, 3], &[], cap),
        (&[0, 2], &[3], cap),
        (&[0, 2], &[1, 1], cap),
        (&[0, 2], &[2, 1], cap),
    ] {
        internal_page(page(
            &contract,
            parents,
            &good,
            ordinals,
            completed,
            capacity,
            &RuntimeControl::normal(),
        ));
    }
    let wrong_type: [ArrayRef; 1] = [Arc::new(Int32Array::from(vec![1, 2]))];
    let wrong_length: [ArrayRef; 1] = [Arc::new(Int64Array::from(vec![1]))];
    let wrong_null: [ArrayRef; 1] = [Arc::new(Int64Array::from(vec![Some(1), None]))];
    for columns in [&wrong_type[..], &wrong_length[..], &wrong_null[..], &[]] {
        internal_page(page(
            &contract,
            parents,
            columns,
            &[0, 2],
            &[],
            cap,
            &RuntimeControl::normal(),
        ));
    }
    let with_outer_pass_through = [good[0].clone(), good[0].clone()];
    internal_page(page(
        &contract,
        parents,
        &with_outer_pass_through,
        &[0, 2],
        &[],
        cap,
        &RuntimeControl::normal(),
    ));
    let nullable = Fixture::new(&[], &[i64_type(true)]).contract();
    page(
        &nullable,
        parents,
        &wrong_null,
        &[0, 2],
        &[],
        cap,
        &RuntimeControl::normal(),
    )
    .unwrap();
}

#[test]
fn empty_relation_binding_is_rejected_before_selected_inputs_or_pages() {
    let call = Fixture::new(&[], &[]).call();
    invalid_input(TableCallContract::try_new(call, &CompileControl(None)));
}

#[test]
fn outer_control_types_survive_preparation_input_and_output_validation() {
    let call = Fixture::new(&[i64_type(false)], &[i64_type(false)]).call();
    let contract = TableCallContract::try_new(call.clone(), &CompileControl(None)).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let arguments = [EvaluatedArgument::Column(&array)];
    for (compile_failure, failure) in [
        (CompileControlError::Cancelled, KernelFailure::Cancelled),
        (
            CompileControlError::DeadlineExceeded,
            KernelFailure::DeadlineExceeded,
        ),
        (
            CompileControlError::ResourceExhausted,
            KernelFailure::ResourceExhausted,
        ),
    ] {
        assert_eq!(
            TableCallContract::try_new(
                call.clone(),
                &CompileControl(Some(compile_failure.clone()))
            )
            .unwrap_err(),
            KernelFailure::CompileControl(compile_failure)
        );
        let control = RuntimeControl::fail(failure.clone(), false);
        assert_eq!(
            SelectedTableInput::try_new(&contract, Selection::all(1), &arguments, &control)
                .unwrap_err(),
            failure
        );
        let control = RuntimeControl::fail(failure.clone(), false);
        assert_eq!(
            page(
                &contract,
                Selection::all(1),
                &[],
                &[],
                &[],
                TablePageCapacity {
                    rows: 0,
                    completions: 0
                },
                &control
            )
            .unwrap_err(),
            failure
        );
    }
}

#[test]
fn more_than_one_quantum_of_input_rows_or_sparse_comparison_observes_control() {
    let rows = (0..300).map(|row| row * 2).collect::<Vec<_>>();
    let other_rows = rows.clone();
    assert_ne!(rows.as_ptr(), other_rows.as_ptr());
    let selection = Selection::try_sparse(600, &rows).unwrap();
    let invocation = Selection::try_sparse(600, &other_rows).unwrap();
    let dense: ArrayRef = Arc::new(Int64Array::from(vec![1; 600]));
    let compact = values(selection, Arc::new(Int64Array::from(vec![1; 300])));
    for (nullable, argument) in [
        (false, EvaluatedArgument::Column(&dense)),
        (true, EvaluatedArgument::SelectedColumn(&compact)),
    ] {
        let contract = Fixture::new(&[i64_type(nullable)], &[i64_type(false)]).contract();
        SelectedTableInput::try_new(
            &contract,
            invocation,
            &[argument],
            &RuntimeControl::normal(),
        )
        .unwrap();
        for failure in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let control = RuntimeControl::fail(failure.clone(), true);
            assert_eq!(
                SelectedTableInput::try_new(&contract, invocation, &[argument], &control)
                    .unwrap_err(),
                failure
            );
            control.assert_quantum_failure();
        }
    }
}

#[test]
fn more_than_one_quantum_of_output_parents_or_completions_observes_control() {
    let contract = Fixture::new(&[], &[i64_type(false)]).contract();
    let parents = Selection::all(300);
    let repeated = vec![0; 300];
    let completed = (0..300).collect::<Vec<_>>();
    for (ordinals, completion, capacity) in [
        (
            &repeated[..],
            &[][..],
            TablePageCapacity {
                rows: 300,
                completions: 0,
            },
        ),
        (
            &[][..],
            &completed[..],
            TablePageCapacity {
                rows: 0,
                completions: 300,
            },
        ),
    ] {
        let columns: [ArrayRef; 1] = [Arc::new(Int64Array::from(vec![1; ordinals.len()]))];
        page(
            &contract,
            parents,
            &columns,
            ordinals,
            completion,
            capacity,
            &RuntimeControl::normal(),
        )
        .unwrap();
        for failure in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let control = RuntimeControl::fail(failure.clone(), true);
            assert_eq!(
                page(
                    &contract, parents, &columns, ordinals, completion, capacity, &control
                )
                .unwrap_err(),
                failure
            );
            control.assert_quantum_failure();
        }
    }
}

#[test]
fn parent_row_errors_are_independent_of_output_rows_and_successful_empty_completion() {
    let mut fixture = Fixture::new(&[], &[i64_type(false)]);
    fixture.declaration.own_row_error = FunctionIntrinsicRowError::MayRaise;
    let contract = fixture.contract();
    assert_eq!(
        contract.call().effects().own_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    let rows = [1, 4, 9];
    let parents = Selection::try_sparse(10, &rows).unwrap();
    let input =
        SelectedTableInput::try_new(&contract, parents, &[], &RuntimeControl::normal()).unwrap();
    let errors = [
        RowDataError::new(0, "fixture first parent row error"),
        RowDataError::new(2, "fixture last parent row error"),
    ];
    let classified =
        TableParentErrors::try_new(input, &errors, 2, &RuntimeControl::normal()).unwrap();
    assert_eq!(classified.errors(), errors);
    assert_eq!(classified.input().selection(), parents);
    assert!(std::ptr::eq(classified.input().contract(), &contract));
    assert_eq!(classified.batch_parent(0), Some(1));
    assert_eq!(classified.batch_parent(1), Some(9));
    assert_eq!(classified.batch_parent(2), None);
    let empty: [ArrayRef; 1] = [Arc::new(Int64Array::from(Vec::<i64>::new()))];
    let output = page(
        &contract,
        parents,
        &empty,
        &[],
        &[],
        TablePageCapacity {
            rows: 0,
            completions: 0,
        },
        &RuntimeControl::normal(),
    )
    .unwrap();
    assert_eq!(output.row_count(), 0);
    assert!(output.completed_parents().is_empty());
    assert_eq!(classified.errors().len(), 2);
    // Neither receipt establishes cross-page progress or synthesizes an outer
    // row. The host still owns invocation identity and required-error handling.
}

#[test]
fn parent_error_channel_rejects_bad_capacity_range_order_and_no_row_error_claim() {
    let mut fixture = Fixture::new(&[], &[i64_type(false)]);
    fixture.declaration.own_row_error = FunctionIntrinsicRowError::MayRaise;
    let contract = fixture.contract();
    let input =
        SelectedTableInput::try_new(&contract, Selection::all(3), &[], &RuntimeControl::normal())
            .unwrap();
    let valid = [
        RowDataError::new(0, "fixture row data"),
        RowDataError::new(2, "fixture row data"),
    ];
    internal_page(TableParentErrors::try_new(
        input,
        &valid,
        1,
        &RuntimeControl::normal(),
    ));
    for malformed in [
        vec![RowDataError::new(3, "outside selected parent range")],
        vec![
            RowDataError::new(1, "duplicate"),
            RowDataError::new(1, "duplicate"),
        ],
        vec![
            RowDataError::new(2, "unordered"),
            RowDataError::new(0, "unordered"),
        ],
    ] {
        internal_page(TableParentErrors::try_new(
            input,
            &malformed,
            malformed.len(),
            &RuntimeControl::normal(),
        ));
    }
    let no_error_contract = Fixture::new(&[], &[i64_type(false)]).contract();
    let no_error_input = SelectedTableInput::try_new(
        &no_error_contract,
        Selection::all(3),
        &[],
        &RuntimeControl::normal(),
    )
    .unwrap();
    TableParentErrors::try_new(no_error_input, &[], 0, &RuntimeControl::normal()).unwrap();
    internal_page(TableParentErrors::try_new(
        no_error_input,
        &valid,
        2,
        &RuntimeControl::normal(),
    ));
}

#[test]
fn parent_error_channel_preserves_initial_and_mid_work_outer_control_types() {
    let mut fixture = Fixture::new(&[], &[i64_type(false)]);
    fixture.declaration.own_row_error = FunctionIntrinsicRowError::MayRaise;
    let contract = fixture.contract();
    let rows = (0..300).map(|row| row * 2).collect::<Vec<_>>();
    let selection = Selection::try_sparse(600, &rows).unwrap();
    let input =
        SelectedTableInput::try_new(&contract, selection, &[], &RuntimeControl::normal()).unwrap();
    let errors = (0..300)
        .map(|ordinal| RowDataError::new(ordinal, "fixture selected parent error"))
        .collect::<Vec<_>>();
    TableParentErrors::try_new(input, &errors, 300, &RuntimeControl::normal()).unwrap();
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = RuntimeControl::fail(failure.clone(), positive_only);
            assert_eq!(
                TableParentErrors::try_new(input, &errors, 300, &control).unwrap_err(),
                failure
            );
            if positive_only {
                control.assert_quantum_failure();
            } else {
                assert_eq!(*control.work.lock().unwrap(), vec![0]);
            }
        }
    }
}
