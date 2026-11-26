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

use arrow_schema::DataType;
use novarocks_functions::builtin::catalogue::builtin_engine_function_catalog;
use novarocks_functions::{
    FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionValueType, PureCallLifecycle,
    ResolvedFunctionBinding,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn select(name: &str, types: &[DataType]) -> ResolvedFunctionBinding {
    let arguments = types
        .iter()
        .map(|data_type| FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type.clone(), true),
            constant: None,
        })
        .collect::<Vec<_>>();
    builtin_engine_function_catalog()
        .resolve_bound_user(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: arguments.len(),
                expected_result_type: None,
            },
            &Control,
        )
        .unwrap()
}

#[test]
fn approximate_aggregates_distinct_installed_overloads_share_state_formats() {
    let catalog = builtin_engine_function_catalog();
    for (input, state, default, explicit) in [
        (
            DataType::Null,
            "unknown-v2",
            "unknown-v1",
            "unknown-error-v1",
        ),
        (
            DataType::Boolean,
            "boolean-set-v2",
            "boolean-v1",
            "boolean-error-v1",
        ),
        (
            DataType::Int64,
            "hll-xxhash64-parameterized-v2",
            "default-hll-v1",
            "explicit-error-hll-v1",
        ),
    ] {
        let bound = select("approx_distinct", std::slice::from_ref(&input));
        assert_eq!(
            bound.selected.overload.as_str(),
            format!("builtin.aggregate/approx_distinct/{default}")
        );
        let installed = catalog
            .pure_overload_declaration_observed(
                &bound.function_id,
                bound.kind,
                &bound.selected.overload,
                &Control,
            )
            .unwrap();
        installed
            .admit_selected_lifecycle_observed(
                &bound.selected,
                1,
                PureCallLifecycle::Aggregate,
                &Control,
            )
            .unwrap();
        let state = format!("novarocks/approx_distinct/{state}");
        assert_eq!(
            bound.selected.aggregate.unwrap().state_format.as_str(),
            state
        );
        let epsilon = select("approx_distinct", &[input, DataType::Float64]);
        assert_eq!(
            epsilon.selected.overload.as_str(),
            format!("builtin.aggregate/approx_distinct/{explicit}")
        );
        catalog
            .pure_overload_declaration_observed(
                &epsilon.function_id,
                epsilon.kind,
                &epsilon.selected.overload,
                &Control,
            )
            .unwrap()
            .admit_selected_lifecycle_observed(
                &epsilon.selected,
                2,
                PureCallLifecycle::Aggregate,
                &Control,
            )
            .unwrap();
        assert_eq!(
            epsilon.selected.aggregate.unwrap().state_format.as_str(),
            state
        );
    }
}

use arrow_array::types::Float64Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array, ListArray, MapArray,
    StringArray,
};
use arrow_schema::Field;
use novarocks_functions::{
    AggregateKernelPhase, AggregatePreparationOptions, AggregateStateColumn, CallArgumentUses,
    CallEffectInput, EvaluatedArgument, KernelEvaluationControl, KernelFailure, PreparedPureKernel,
    PureCallPreparation, ScopedRelationalEffects, SelectedAggregateUpdateInput, Selection,
    UnaccountedAggregateStateAllocator,
};
use novarocks_type_contract::{
    CallProofScope, DecimalOverflowPolicy, ExpressionUseId, RelationCallId, RelationEffectContext,
    SemanticParameters,
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("approximate aggregates never wait")
    }
}

fn evaluate(
    name: &str,
    values: &[ArrayRef],
    selected_rows: &[usize],
) -> Result<ArrayRef, KernelFailure> {
    let catalog = builtin_engine_function_catalog();
    let arguments = values
        .iter()
        .map(|value| FunctionArgument::Value {
            value_type: FunctionValueType::new(value.data_type().clone(), true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Aggregate, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = RelationEffectContext::call(RelationCallId::new(17));
    let uses = (0..arguments.len())
        .map(|index| Some(ExpressionUseId::new(index as u32 + 1)))
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                function_id: &bound.function_id,
                kind: FunctionKind::Aggregate,
                selected: &selected,
                request,
                argument_uses: CallArgumentUses::SelectedChannels(&uses),
                context: context.into(),
                parameters: &parameters,
                environment: &[],
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Relation(context.identity),
            },
            selected.clone(),
            PureCallPreparation::Aggregate {
                arguments: ScopedRelationalEffects::pure_value(context),
                options: AggregatePreparationOptions {
                    state_interpretation: None,
                    phase: AggregateKernelPhase::Single,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: None,
                },
            },
            &Control,
        )
        .unwrap();
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("selected approximate aggregate");
    };
    let mut states = AggregateStateColumn::try_new(
        handle.clone(),
        Arc::new(UnaccountedAggregateStateAllocator),
        NonZeroUsize::MIN,
    )?;
    states.push(&Control)?;
    let arguments = values
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let selection = Selection::try_sparse(values[0].len(), selected_rows).unwrap();
    let input = SelectedAggregateUpdateInput::try_new(
        handle.contract(),
        selection,
        &arguments,
        &[],
        &Control,
    )?;
    let mapping = vec![0; selected_rows.len()];
    states
        .prepare_update_batch(&mapping, input, &Control)?
        .run(&Control)?;
    states.emit(&[0], 1, &Control)
}

#[test]
fn approximate_aggregates_distinct_uses_first_selected_contribution_accuracy() {
    use novarocks_failure_contract::FailureProjection;
    use novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;

    let inputs: &[ArrayRef] = &[
        Arc::new(Int64Array::from(vec![
            Some(10),
            None,
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            Some(15),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(-1.0),
            Some(-1.0),
            None,
            Some(0.023),
            Some(f64::NAN),
            Some(-1.0),
            None,
        ])),
    ];
    let value = evaluate("approx_distinct", inputs, &[1, 2, 3, 4, 5, 6]).unwrap();
    let result = value.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(result.value(0), 3);

    // Selecting the invalid row before any eligible contribution is an error
    // with the same installed overload.
    let failure = evaluate("approx_distinct", inputs, &[0, 3, 4]).unwrap_err();
    assert_eq!(failure.failure_fact().reason(), INVALID_FUNCTION_ARGUMENT);
}

#[test]
fn approximate_aggregates_distinct_boolean_ignores_non_null_accuracy_value() {
    let inputs: &[ArrayRef] = &[
        Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(false),
        ])),
        Arc::new(Float64Array::from(vec![
            None,
            Some(f64::NAN),
            Some(-1.0),
            None,
            Some(-1.0),
        ])),
    ];
    for (rows, expected) in [
        (&[0, 2, 3][..], 0),
        (&[0, 1, 2, 3][..], 1),
        (&[0, 1, 2, 3, 4][..], 2),
    ] {
        let value = evaluate("approx_distinct", inputs, rows).unwrap();
        let result = value.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.value(0), expected);
    }
}

#[test]
fn approximate_aggregates_percentile_preserves_types_fractional_weights_and_null_selection() {
    let result = evaluate(
        "approx_percentile",
        &[
            Arc::new(Int64Array::from(vec![
                Some(0),
                Some(10),
                Some(20),
                Some(30),
                None,
                Some(42),
                Some(42),
            ])),
            Arc::new(Float64Array::from(vec![1.1, 1.9, 1.1, 1.9, 0.0, 0.0, 0.0])),
            Arc::new(Float64Array::from(vec![
                Some(0.5),
                Some(0.5),
                Some(0.5),
                Some(0.5),
                Some(5.0),
                None,
                Some(5.0),
            ])),
        ],
        &[0, 1, 2, 3, 4, 5],
    )
    .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        16
    );

    let result = evaluate(
        "approx_percentile",
        &[
            Arc::new(Float32Array::from(vec![1.0, 4.0])),
            Arc::new(Float64Array::from(vec![0.5, 0.5])),
        ],
        &[0, 1],
    )
    .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0),
        4.0
    );

    for sample in [i64::MIN, i64::MAX, 9_007_199_254_740_994] {
        let result = evaluate(
            "approx_percentile",
            &[
                Arc::new(Int64Array::from(vec![sample])),
                Arc::new(Float64Array::from(vec![0.5])),
            ],
            &[0],
        )
        .unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            sample
        );
    }
    let invalid = evaluate(
        "approx_percentile",
        &[
            Arc::new(Int64Array::from(vec![9_007_199_254_740_993])),
            Arc::new(Float64Array::from(vec![0.5])),
        ],
        &[0],
    )
    .unwrap_err();
    assert!(
        invalid
            .to_string()
            .contains("cannot be represented exactly")
    );
}

#[test]
fn approximate_aggregates_percentile_array_has_no_4096_target_limit() {
    let targets = (0..4097)
        .map(|index| Some(f64::from(index % 2)))
        .collect::<Vec<_>>();
    let result = evaluate(
        "approx_percentile",
        &[
            Arc::new(Float64Array::from(vec![2.0, 6.0])),
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
                Some(targets),
                Some(vec![None]),
            ])),
        ],
        &[0, 1],
    )
    .unwrap();
    let result = result
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(result.len(), 4097);
    let (pairs, _) = result.values().as_chunks::<2>();
    assert!(pairs.iter().all(|pair| *pair == [2.0, 6.0]));
    assert_eq!(result.value(4096), 2.0);

    let invalid = evaluate(
        "approx_percentile",
        &[
            Arc::new(Float64Array::from(vec![2.0])),
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
                vec![None],
            )])),
        ],
        &[0],
    )
    .unwrap_err();
    assert!(invalid.to_string().contains("cannot contain NULL"));
}

#[test]
fn approximate_aggregates_frequency_uses_first_options_and_typed_map() {
    let inputs: &[ArrayRef] = &[
        Arc::new(Int64Array::from(vec![0, 5, 0, -1])),
        Arc::new(StringArray::from(vec![
            None,
            Some("x"),
            Some("x"),
            Some("z"),
        ])),
        Arc::new(Int64Array::from(vec![0, 100001, -1, 0])),
    ];
    let result = evaluate("approx_most_frequent", inputs, &[0, 1, 2, 3]).unwrap();
    let result = result.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(
        result
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("x"), Some("z")]
    );
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[2, 1]
    );
    assert!(
        evaluate("approx_most_frequent", inputs, &[0])
            .unwrap()
            .is_null(0)
    );
    assert!(
        evaluate("approx_most_frequent", inputs, &[2])
            .unwrap_err()
            .to_string()
            .contains("bucket count")
    );

    let result = evaluate(
        "approx_most_frequent",
        &[
            Arc::new(Int64Array::from(vec![5, 5, 5])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![1, 1, 1])),
        ],
        &[0, 1, 2],
    )
    .unwrap();
    let result = result.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(
        result
            .keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[2]
    );
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[3]
    );
}

#[test]
fn approximate_aggregates_percentile_installed_matrix_and_accuracy_declarations() {
    let catalog = builtin_engine_function_catalog();
    let array = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
    for data_type in [DataType::Int64, DataType::Float32, DataType::Float64] {
        for target in [DataType::Float64, array.clone()] {
            for weighted in [false, true] {
                let mut args = vec![data_type.clone()];
                if weighted {
                    args.push(DataType::Float64);
                }
                args.push(target.clone());
                let bound = select("approx_percentile", &args);
                let admission = catalog
                    .pure_overload_declaration_observed(
                        &bound.function_id,
                        bound.kind,
                        &bound.selected.overload,
                        &Control,
                    )
                    .unwrap();
                admission
                    .admit_selected_lifecycle_observed(
                        &bound.selected,
                        args.len(),
                        PureCallLifecycle::Aggregate,
                        &Control,
                    )
                    .unwrap();
                let result = if target == array {
                    DataType::List(Arc::new(Field::new("item", data_type.clone(), true)))
                } else {
                    data_type.clone()
                };
                assert_eq!(
                    bound.selected.result_type,
                    novarocks_functions::FunctionResultType::Scalar(FunctionValueType::new(
                        result, true
                    ))
                );
            }
        }
        let accuracy = select(
            "approx_percentile",
            &[
                data_type,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ],
        );
        assert!(
            catalog
                .definition_by_id(&accuracy.function_id)
                .unwrap()
                .is_declared_only_pure_overload(&accuracy.selected.overload)
        );
    }
}
