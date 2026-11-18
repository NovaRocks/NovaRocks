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

//! Driver-local RAND streams, input bindings and interruption at actual roots.

use arrow::{
    array::{BooleanArray, Float64Array, Int64Array, new_empty_array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, CompiledFilterConjunctionInstance,
    EvaluationEnvironment, EvaluationInput, ExprEvaluationFailure, ScopeLayouts,
    testkit::{
        CallbackControl, HarnessControl, PassThroughCallHost, callback_causes,
        compile_fixture_unit, fixture_scope,
        rng_fixture::{POLICY, RuntimeControl, SeedMode, UNIT_LIMITS, bool_source, rng_unit},
    },
};
use novarocks_expression_contract::{BoundExprSetBuilder, ExprPort, RootOrdinal, RootScope};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, SelectedValues, Selection,
    testkit::rng_subset,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

fn batch(unit: &CompiledExprSet, seeds: Vec<Option<i64>>) -> RecordBatch {
    let rows = seeds.len();
    // Borrow the exact immediate input schema, including the unrelated
    // ordinal-zero output. The seed is ordinal one; ValueId 100 is no index.
    let schema = unit.scopes()[0].schema().clone();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![999; rows])),
            Arc::new(Int64Array::from(seeds)),
            Arc::new(BooleanArray::from(vec![true; rows])),
        ],
    )
    .unwrap()
}

fn bits(result: SelectedValues<'_>) -> Vec<u64> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .iter()
        .map(|value| value.to_bits())
        .collect()
}

// Independent PCG32 seed expansion + ChaCha12 / rand 0.8.5 oracles.
const SEED_42_FIRST: u64 = 0x3fe0d98eec6444e4;
const SEED_42_SECOND: u64 = 0x3fe15e014267f5aa;
const SEED_42_THIRD: u64 = 0x3fe45dec0e3bca26;
const SEED_7_FIRST: u64 = 0x3f9f0b83a5aaa3e0;
const SEED_0_FIRST: u64 = 0x3fe76547f659a58d;

fn instance(unit: &Arc<CompiledExprSet>, expression: u32) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(unit.clone(), RootOrdinal::new(expression), &RuntimeControl)
        .unwrap()
}

#[test]
fn actual_value_seed_root_maps_sparse_parent_rows_and_nullable_seed_zero() {
    let unit = rng_unit(SeedMode::Input, false, false);
    let input = batch(&unit, vec![Some(42), Some(7), Some(42), None]);
    let mut evaluator = instance(&unit, 0);
    let rows = [1, 2, 3];
    let selected = Selection::try_sparse(4, &rows).unwrap();
    let result = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selected, selected),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(result.selection(), selected);
    assert_eq!(
        bits(result),
        vec![SEED_7_FIRST, SEED_42_FIRST, SEED_0_FIRST]
    );
    let result = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        bits(result),
        vec![SEED_42_FIRST, SEED_7_FIRST, SEED_42_FIRST, SEED_0_FIRST]
    );
}

#[test]
fn actual_constant_seed_root_continues_across_batches_without_driver_state_sharing() {
    let unit = rng_unit(SeedMode::DirectConstant, false, false);
    let two = batch(&unit, vec![Some(42), Some(42)]);
    let one = batch(&unit, vec![Some(42)]);
    let mut a = instance(&unit, 0);
    let mut b = instance(&unit, 0);
    assert_eq!(
        bits(
            a.evaluate(
                EvaluationInput::Batch {
                    batch: &two,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
        ),
        vec![SEED_42_FIRST, SEED_42_SECOND]
    );
    assert_eq!(
        bits(
            b.evaluate(
                EvaluationInput::Batch {
                    batch: &one,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
        ),
        vec![SEED_42_FIRST]
    );
    assert_eq!(
        bits(
            a.evaluate(
                EvaluationInput::Batch {
                    batch: &one,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
        ),
        vec![SEED_42_THIRD]
    );
    assert_eq!(
        bits(
            b.evaluate(
                EvaluationInput::Batch {
                    batch: &one,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
        ),
        vec![SEED_42_SECOND]
    );
}

#[test]
fn one_definition_with_distinct_actual_root_occurrences_keeps_independent_rng_streams() {
    let unit = rng_unit(SeedMode::DirectConstant, true, false);
    let input = batch(&unit, vec![Some(42), Some(42)]);
    let mut a = instance(&unit, 0);
    let mut b = instance(&unit, 1);
    assert_eq!(
        bits(
            a.evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
        ),
        vec![SEED_42_FIRST, SEED_42_SECOND]
    );
    let row = [1];
    assert_eq!(
        bits(
            b.evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None
                },
                {
                    let selection = Selection::try_sparse(2, &row).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
        ),
        vec![SEED_42_FIRST]
    );
    assert_eq!(
        bits(
            a.evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None
                },
                {
                    let selection = Selection::try_sparse(2, &row).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
        ),
        vec![SEED_42_THIRD]
    );
    assert_eq!(
        bits(
            b.evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None
                },
                {
                    let selection = Selection::try_sparse(2, &row).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
        ),
        vec![SEED_42_SECOND]
    );
}

#[test]
fn empty_selection_does_not_advance_actual_constant_rng_instance() {
    let unit = rng_unit(SeedMode::DirectConstant, false, false);
    let input = batch(&unit, vec![Some(42), Some(42)]);
    let mut evaluator = instance(&unit, 0);
    let none: [usize; 0] = [];
    let empty = Selection::try_sparse(2, &none).unwrap();
    let result = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(empty, empty),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(result.selection(), empty);
    assert!(result.selection().is_empty());
    assert!(result.errors().is_empty());
    assert_eq!(
        bits(
            evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                    ScalarInvocationActivation::Activated,
                    &RuntimeControl,
                    &mut PassThroughCallHost
                )
                .unwrap()
        ),
        vec![SEED_42_FIRST, SEED_42_SECOND]
    );
    let empty_batch = batch(&unit, vec![]);
    assert!(
        evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &empty_batch,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
            .selection()
            .is_empty()
    );
    let first = [0];
    assert_eq!(
        bits(
            evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    {
                        let selection = Selection::try_sparse(2, &first).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &RuntimeControl,
                    &mut PassThroughCallHost
                )
                .unwrap()
        ),
        vec![SEED_42_THIRD]
    );
}

#[test]
fn value_root_materializes_exact_immediate_child_slot_with_sparse_nulls() {
    let unit = rng_unit(SeedMode::Input, false, false);
    let input = batch(&unit, vec![Some(11), Some(42), None, Some(7)]);
    let mut evaluator = instance(&unit, 1);
    let selected_rows = [1, 2, 3];
    let selected = Selection::try_sparse(4, &selected_rows).unwrap();
    let result = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selected, selected),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(result.selection(), selected);
    assert!(result.errors().is_empty());
    assert_eq!(result.values().data_type(), &DataType::Int64);
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // An exact Value root retains source NULL rather than substituting RAND's
    // normalized seed-zero rule. The unrelated ordinal-zero value is 999.
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(42), None, Some(7)]
    );
}

#[test]
fn actual_root_rejects_wrong_schema_and_selection_domain_as_invalid_program() {
    let unit = rng_unit(SeedMode::Input, false, false);
    let input = batch(&unit, vec![Some(42), Some(7)]);
    let mut fields = input.schema().fields().to_vec();
    fields.swap(0, 1);
    let wrong_schema =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), input.columns().to_vec()).unwrap();
    let mut evaluator = instance(&unit, 0);
    assert!(matches!(
        evaluator.evaluate(
            EvaluationInput::Batch {
                batch: &wrong_schema,
                source: None
            },
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
    let mut evaluator = instance(&unit, 0);
    assert!(matches!(
        evaluator.evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
}

#[test]
fn constructor_rejects_absent_actual_root_instead_of_using_a_definition_or_default_site() {
    let unit = rng_unit(SeedMode::Input, false, false);
    assert!(matches!(
        CompiledExpressionInstance::try_new(unit, RootOrdinal::new(u32::MAX), &RuntimeControl,),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

fn runtime_causes() -> [KernelFailure; 7] {
    callback_causes([
        "controller-origin-invalid",
        "controller-origin-internal",
        "controller-origin-operational",
    ])
}

#[test]
fn every_constructor_control_callback_preserves_original_failure_without_retry() {
    let unit = rng_unit(SeedMode::Input, false, false);
    let root = RootOrdinal::new(0);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _instance = CompiledExpressionInstance::try_new(unit.clone(), root, &recorder).unwrap();
    let successful = recorder.trace();
    assert!(!successful.is_empty());
    for index in 1..=successful.len() {
        for cause in runtime_causes() {
            let control = CallbackControl::new(cause.clone(), index);
            let result = CompiledExpressionInstance::try_new(unit.clone(), root, &control);
            assert!(
                matches!(result, Err(actual) if actual == cause),
                "constructor callback {index}"
            );
            assert_eq!(control.trace(), successful[..index]);
        }
    }
}

#[test]
fn every_selected_evaluation_callback_preserves_control_and_poisoned_instance_never_replays() {
    let unit = rng_unit(SeedMode::Input, false, false);
    let input = batch(
        &unit,
        (0..320)
            .map(|row| Some(if row % 2 == 0 { 42 } else { 7 }))
            .collect(),
    );
    let mut evaluator = instance(&unit, 0);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _result = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let successful = recorder.trace();
    assert!(successful.contains(&256));
    assert!(successful.iter().all(|units| *units <= 256));
    for index in 1..=successful.len() {
        for cause in runtime_causes() {
            let mut evaluator = instance(&unit, 0);
            let control = CallbackControl::new(cause.clone(), index);
            let result = evaluator.evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                ScalarInvocationActivation::Activated,
                &control,
                &mut PassThroughCallHost,
            );
            assert!(
                matches!(result, Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause),
                "evaluation callback {index}"
            );
            assert_eq!(control.trace(), successful[..index]);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                    ScalarInvocationActivation::Activated,
                    &RuntimeControl,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
        }
    }
}

#[test]
#[allow(deprecated)] // The actual source descriptors are independent IPC facts.
fn unused_incoming_dictionary_descriptors_are_not_value_identity_but_layout_and_nullability_are() {
    let unit = rng_unit(SeedMode::Input, false, true);
    let schema = unit.scopes()[0].schema().clone();
    let dictionary = schema.field(3);
    let columns = schema
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect::<Vec<_>>();
    let valid = RecordBatch::try_new(schema.clone(), columns.clone()).unwrap();
    let mut evaluator = instance(&unit, 0);
    assert!(
        evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &valid,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            )
            .unwrap()
            .selection()
            .is_empty()
    );
    for (changed_id, changed_order) in [(17, false), (88, true)] {
        let mut fields = schema.fields().to_vec();
        fields[3] = Arc::new(
            Field::new_dict(
                dictionary.name(),
                dictionary.data_type().clone(),
                dictionary.is_nullable(),
                changed_id,
                changed_order,
            )
            .with_metadata(dictionary.metadata().clone()),
        );
        let drifted = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        // Planned layouts describe plain values. Actual source IPC attributes
        // do not change that same value layout, name or nullability.
        assert_eq!(schema.as_ref(), drifted.as_ref());
        let actual_input = RecordBatch::try_new(drifted, columns.clone()).unwrap();
        let mut evaluator = instance(&unit, 0);
        assert!(
            evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &actual_input,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                    ScalarInvocationActivation::Activated,
                    &RuntimeControl,
                    &mut PassThroughCallHost
                )
                .unwrap()
                .selection()
                .is_empty()
        );
    }
    for (data_type, nullable) in [(DataType::Binary, true), (DataType::Utf8, false)] {
        let mut fields = schema.fields().to_vec();
        fields[3] = Arc::new(Field::new(dictionary.name(), data_type.clone(), nullable));
        let mut actual_columns = columns.clone();
        actual_columns[3] = new_empty_array(&data_type);
        let actual_input =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), actual_columns).unwrap();
        let mut evaluator = instance(&unit, 0);
        assert!(matches!(
            evaluator.evaluate(
                EvaluationInput::Batch {
                    batch: &actual_input,
                    source: None
                },
                ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost
            ),
            Err(ExprEvaluationFailure::Kernel(
                KernelFailure::InvalidProgram(_)
            ))
        ));
    }
}

#[test]
fn declared_empty_port_keeps_its_batch_domain_while_zero_port_keeps_one_row() {
    let ty = FunctionValueType::new(DataType::Boolean, false);
    let mut constants = ConstantPools::empty();
    let literal = bool_source(&mut constants, &ty, true);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let catalog = rng_subset();
    let compile = |ports: Vec<ExprPort>, scope, layouts: ScopeLayouts<'_>| {
        let mut draft = BoundExprSetBuilder::new(UNIT_LIMITS);
        let definition = draft.add(None, ty.clone(), literal.clone()).unwrap();
        let value = draft
            .root(definition, EvaluationDemand::Value, scope)
            .unwrap();
        let predicate = draft
            .root(definition, EvaluationDemand::TruthOnly, scope)
            .unwrap();
        let mut work =
            CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
        // This actual unit has no call definitions or claims to manufacture.
        let unit = draft
            .seal(ports, &mut work)
            .unwrap()
            .finish(BTreeMap::new(), &mut work)
            .unwrap();
        work.finish().unwrap();
        let compiled = compile_fixture_unit(
            &unit,
            EvaluationEnvironment {
                constants: &constants,
                parameters: &parameters,
            },
            layouts,
            POLICY,
            &catalog,
        );
        (compiled, value, predicate)
    };
    let schema = Arc::new(Schema::empty());
    let port = ExprPort {
        values: Box::default(),
    };
    let layout = fixture_scope(&port, schema.clone());
    let (declared, value, predicate) = compile(
        vec![port],
        RootScope::Port(0),
        ScopeLayouts {
            ports: std::slice::from_ref(&layout),
            all_ports: None,
        },
    );
    assert_eq!(declared.scopes().len(), 1);
    assert!(declared.scopes()[0].schema().fields().is_empty());
    assert_eq!(declared.root(value).unwrap().scope, RootScope::Port(0));
    assert_eq!(
        declared.root(predicate).unwrap().demand,
        EvaluationDemand::TruthOnly
    );
    let input = RecordBatch::try_new_with_options(
        schema.clone(),
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    let mut scalar =
        CompiledExpressionInstance::try_new(declared.clone(), value, &RuntimeControl).unwrap();
    let selected = scalar
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(selected.selection(), Selection::all(3));
    assert!(selected.errors().is_empty());
    assert_eq!(
        selected
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(true); 3]
    );
    let mut filter = CompiledFilterConjunctionInstance::try_new_with_allocator(
        &[(declared, predicate)],
        &RuntimeControl,
        None,
    )
    .unwrap();
    let filtered = filter
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            Selection::all(3),
            &RuntimeControl,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        filtered
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(true); 3]
    );

    let (zero, value, _) = compile(
        vec![],
        RootScope::AllPorts,
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
    );
    assert!(zero.scopes().is_empty());
    assert_eq!(zero.root(value).unwrap().scope, RootScope::AllPorts);
    let one = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let mut scalar = CompiledExpressionInstance::try_new(zero, value, &RuntimeControl).unwrap();
    let selected = scalar
        .evaluate(
            EvaluationInput::Batch {
                batch: &one,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            ScalarInvocationActivation::Activated,
            &RuntimeControl,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(selected.values().len(), 1);
    assert!(selected.errors().is_empty());
    assert!(
        selected
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(matches!(
        scalar.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), ScalarInvocationActivation::Activated, &RuntimeControl, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(KernelFailure::InvalidProgram(ref detail)))
            if detail.message() == "actual input differs from the root's empty one-row port"
    ));
}
