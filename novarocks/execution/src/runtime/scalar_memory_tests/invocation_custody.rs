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

use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all};
use crate::runtime::{
    mem_tracker::MemTracker,
    runtime_state::RuntimeState,
    scalar_memory::{RuntimeScalarMemoryJournal, RuntimeScalarMemoryScope},
    task::{ExecutionFailure, ExecutionFailureCause},
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int64Array, LargeBinaryArray, StringArray},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_expression::{
    CompiledExpressionInstance, CompiledFilterConjunctionInstance, EvaluationInput, StaticExprKind,
    evaluator_view::{UnitStateTemplate, state_template},
};
use novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT;
use novarocks_functions::{
    DataFailureReason, KernelFailure, RowDataError, ScalarInvocationActivation as Activation,
    ScalarInvocationScope, Selection,
    builtin::json_sql::JsonFunction,
    json_value::{JsonWork, format},
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef, ProgramUnitId};
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source_with_declared_fields};
use novarocks_test_compile_to_execute::compiler_results::compiler_results;
use std::sync::Arc;

const PROJECT_SQL: &str = "SELECT JSON_OBJECT(k VALUE v FORMAT JSON RETURNING JSON) AS j FROM t";

fn runtime_control(tracker: Option<Arc<MemTracker>>) -> RuntimeKernelControl {
    let state = RuntimeState::default();
    let mut control = RuntimeKernelControl::new(state.error_state());
    control.bind_runtime_memory(&state);
    if let Some(tracker) = tracker {
        control.bind_mem_tracker(tracker);
    }
    control
}

fn programs(sql: &str) -> Vec<Arc<LocalProgram>> {
    let fields = [
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ];
    let source = sql_source_with_declared_fields(sql, &fields);
    compiler_results(&source, &installed_builtin_owner_catalogue())
        .into_values()
        .map(|p| Arc::new(p.expect("actual SQL package compiles")))
        .collect()
}

fn project(values: [&str; 3]) -> (Arc<LocalProgram>, ProgramRootRef, RecordBatch) {
    let object = JsonFunction::Object {
        null_on_null: true,
        unique: false,
    }
    .function_id();
    let mut selected = None;
    for program in programs(PROJECT_SQL) {
        for (unit_ordinal, unit) in program.units().iter().enumerate() {
            for &occurrence in unit.calls().keys() {
                let Some(UnitStateTemplate::Scalar { scope, kernel }) =
                    state_template(unit, occurrence)
                else {
                    continue;
                };
                if kernel.contract().function_id() != &object {
                    continue;
                }
                let site = ProgramRootRef {
                    unit: ProgramUnitId::new(unit_ordinal),
                    root: scope.root,
                };
                for node in program.graph().nodes() {
                    let ProgramNodeKind::Project { input, exprs, .. } = node.kind() else {
                        continue;
                    };
                    if !exprs.contains(&site) {
                        continue;
                    }
                    let uses = unit.control_flow().uses();
                    let arguments = &uses[&occurrence].arguments;
                    let input_ordinal = |mut id| {
                        loop {
                            if let Some(binding) = unit.input(id) {
                                break binding.ordinal as usize;
                            }
                            let [child] = uses[&id].arguments.as_ref() else {
                                panic!("fixture argument must lead to a single input slot");
                            };
                            id = *child;
                        }
                    };
                    let key = input_ordinal(arguments[0]);
                    let value = input_ordinal(arguments[1]);
                    let layout = program.graph().nodes()[input.index()].output_layout();
                    let columns = layout
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(ordinal, field)| match field.data_type() {
                            DataType::Utf8 if ordinal == key => {
                                Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]))
                                    as ArrayRef
                            }
                            DataType::Utf8 if ordinal == value => {
                                Arc::new(StringArray::from(values.to_vec())) as ArrayRef
                            }
                            DataType::Utf8 => {
                                Arc::new(StringArray::from(vec!["unused"; 3])) as ArrayRef
                            }
                            DataType::Int64 => {
                                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef
                            }
                            DataType::Boolean => {
                                Arc::new(BooleanArray::from(vec![true; 3])) as ArrayRef
                            }
                            other => panic!("unexpected JSON projection input {other:?}"),
                        })
                        .collect();
                    let batch = RecordBatch::try_new(layout.schema().clone(), columns).unwrap();
                    assert!(
                        selected.replace((program.clone(), site, batch)).is_none(),
                        "one actual JSON project root"
                    );
                }
            }
        }
    }
    selected.expect("actual installed JSON project")
}

fn frame(
    program: &LocalProgram,
    site: ProgramRootRef,
    control: &RuntimeKernelControl,
) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new_with_allocator(
        program.unit(site.unit).unwrap().clone(),
        site.root,
        control,
        control.allocator(),
    )
    .unwrap()
}

fn assert_null_key(error: &RowDataError) {
    assert_eq!(
        error.reason(),
        DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)
    );
    assert_eq!(error.message(), "Null key is not valid in JSON_OBJECT");
}

fn text(bytes: &[u8]) -> String {
    struct RenderWork;
    impl JsonWork for RenderWork {
        type Reservation = ();

        fn step(&mut self) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn reserve(&mut self, _bytes: usize) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn reserve_output(&mut self, _bytes: usize) -> Result<(), KernelFailure> {
            Ok(())
        }
    }
    format(bytes, &mut RenderWork).unwrap()
}

#[test]
fn runtime_json_object_actual_sql_keeps_selected_data_and_required_row() {
    let (program, site, input) = project(["[1,2]", "null", "true"]);
    let tracker = MemTracker::new_root("native-json-selected");
    let control = runtime_control(Some(tracker));
    let mut actual = frame(&program, site, &control);
    let mut journal = RuntimeScalarMemoryJournal::default();
    let all = Selection::all(3);
    let rows = [1, 2];
    let demand = Selection::try_sparse(3, &rows).unwrap();
    let selected = actual
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(all, demand),
            Activation::Activated,
            &control,
            &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        )
        .unwrap();
    let json = selected
        .values()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert!(json.is_null(0));
    assert_eq!(text(json.value(1)), r#"{"b":true}"#);
    let [error] = selected.errors() else {
        panic!("expected one selected JSON row error")
    };
    assert_eq!(error.selected_ordinal(), 0);
    assert_null_key(error);

    let failure = evaluate_all(&mut actual, site, &input, &control).unwrap_err();
    let ExecutionFailureCause::RequiredRow(row) = failure.cause() else {
        panic!("expected original typed row error: {failure:?}");
    };
    assert_eq!(row.batch_row(), 1);
    assert_eq!(row.error().selected_ordinal(), 1);
    assert_null_key(row.error());
}

#[test]
fn runtime_json_object_keeps_original_tracker_through_last_backing_alias() {
    let (program, site, input) = project(["[1,2]", "null", "true"]);
    let tracker = MemTracker::new_root("native-json-original");
    let original = runtime_control(Some(tracker.clone()));
    let substitute_tracker = MemTracker::new_root("native-json-unrelated");
    let substitute = runtime_control(Some(substitute_tracker.clone()));
    let mut actual = frame(&program, site, &original);
    let mut journal = RuntimeScalarMemoryJournal::default();
    let rows = [0, 1, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    // Prepare the actual instance under the original control before changing
    // the evaluation control. The instance and escaped Arrow buffers keep
    // their original allocation host.
    let evaluate = |actual: &mut CompiledExpressionInstance,
                    control: &RuntimeKernelControl,
                    journal: &mut RuntimeScalarMemoryJournal| {
        actual.evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            Activation::Activated,
            control,
            &mut RuntimeScalarMemoryScope::new(control, journal),
        )
    };
    drop(evaluate(&mut actual, &original, &mut journal).unwrap());
    let output = evaluate(&mut actual, &substitute, &mut journal).unwrap();
    let json = output
        .values()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(text(json.value(0)), r#"{"a":[1,2]}"#);
    assert!(json.is_null(1));
    let last = json.values().clone();
    let offsets = json.offsets().inner().inner().clone();
    let validity = json.nulls().unwrap().buffer().clone();

    drop(output);
    drop(actual);
    drop(input);
    drop(program);
    drop(original);
    drop(substitute);
    let retained = tracker.current();
    assert!(retained > 0);
    assert_eq!(substitute_tracker.current(), 0);
    drop(offsets);
    assert_eq!(tracker.current(), retained);
    drop(validity);
    assert_eq!(tracker.current(), retained);
    let shared = last.clone();
    drop(last);
    assert_eq!(tracker.current(), retained);
    std::thread::scope(|scope| scope.spawn(move || drop(shared)).join().unwrap());
    assert_eq!(tracker.current(), 0);
}

#[test]
fn runtime_json_object_missing_host_is_lazy_and_latches_first_demand() {
    let (program, site, input) = project(["[1,2]", "null", "true"]);
    let control = runtime_control(None);
    let mut actual = frame(&program, site, &control);
    let mut journal = RuntimeScalarMemoryJournal::default();
    let all = Selection::all(3);
    let empty = Selection::try_sparse(3, &[]).unwrap();
    let output = actual
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(all, empty),
            Activation::ValidateOnly,
            &control,
            &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        )
        .unwrap();
    assert_eq!(output.values().len(), 0);
    assert!(matches!(
        evaluate_all(&mut actual, site, &input, &control)
            .unwrap_err()
            .cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        evaluate_all(&mut actual, site, &input, &control)
            .unwrap_err()
            .cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    ));
}

#[test]
fn runtime_json_object_rolls_back_capacity_failure_before_publication() {
    let large_value = format!(r#""{}""#, "x".repeat(256 * 1024));
    let (program, site, input) = project([&large_value, "null", "true"]);
    let tracker = MemTracker::new_root("native-json-capacity");
    // The first JSON string alone exceeds this fixture's task tracker limit.
    tracker.install_limit_once(64 * 1024).unwrap();
    let control = runtime_control(Some(tracker.clone()));
    let mut actual = frame(&program, site, &control);
    let failure = evaluate_all(&mut actual, site, &input, &control).unwrap_err();
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Kernel(KernelFailure::Capacity(_))
    ));
    assert!(matches!(
        evaluate_all(&mut actual, site, &input, &control)
            .unwrap_err()
            .cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    ));
    drop(actual);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn runtime_json_object_ordered_filter_suppresses_and_maps_selected_errors() {
    // A LIMIT preserves one ordered Filter above the original row selection.
    let sql = r#"SELECT n FROM (SELECT n, k, flag FROM t LIMIT 3) s
                 WHERE flag AND JSON_OBJECT(k VALUE n) = '{"b":3}'"#;
    let mut selected = None;
    for program in programs(sql) {
        for node in program.graph().nodes() {
            let predicates = match node.kind() {
                ProgramNodeKind::Filter { predicates, .. } => predicates.as_ref(),
                ProgramNodeKind::Scan { residuals, .. } => residuals.as_slice(),
                _ => continue,
            };
            if predicates.len() != 2 {
                continue;
            }
            let units = predicates
                .iter()
                .map(|site| (program.unit(site.unit).unwrap().clone(), site.root))
                .collect::<Vec<_>>();
            let (unit, ordinal) = &units[0];
            let root = unit.root(*ordinal).unwrap();
            let schema = unit.scope(root.scope).unwrap().schema().clone();
            assert!(
                selected.replace((units, schema)).is_none(),
                "one ordered pair"
            );
        }
    }
    let (predicates, schema) = selected.expect("SQL produced an ordered two-predicate Filter/Scan");
    for demanded in [false, true] {
        let memory = demanded.then(super::bound);
        let mut control = super::control(memory.as_ref(), Arc::default());
        if demanded {
            control.bind_mem_tracker(MemTracker::new_root("native-json-filter"));
        }
        let columns = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Boolean => {
                    Arc::new(BooleanArray::from(vec![false, demanded, demanded])) as ArrayRef
                }
                DataType::Int64 => Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                DataType::Utf8 => {
                    Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef
                }
                other => panic!("unexpected filter fixture field {other:?}"),
            })
            .collect();
        let input = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut actual = CompiledFilterConjunctionInstance::try_new_with_allocator(
            &predicates,
            &control,
            control.allocator(),
        )
        .unwrap();
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = actual
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                Selection::all(3),
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
            )
            .map_err(ExecutionFailure::from);
        if demanded {
            let failure = result.unwrap_err();
            let ExecutionFailureCause::RequiredRow(row) = failure.cause() else {
                panic!("expected original typed filter error: {failure:?}");
            };
            assert_eq!(row.batch_row(), 1);
            assert_null_key(row.error());
        } else {
            let output = result.unwrap();
            let values = output.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(false); 3]);
        }
    }
}

#[test]
fn runtime_json_in_uses_constant_proof_and_keeps_later_rhs_errors() {
    let sql = r#"SELECT json_parse(k) IN (JSON '{"a":1,"b":2}', json_parse(v)) FROM t"#;
    let mut fixture = None;
    for program in programs(sql) {
        for node in program.graph().nodes() {
            let ProgramNodeKind::Project { input, exprs, .. } = node.kind() else {
                continue;
            };
            for &site in exprs.iter() {
                let unit = program.unit(site.unit).unwrap();
                let root = unit.root(site.root).unwrap();
                let definitions = unit.definitions();
                let StaticExprKind::PreparedInList { values, .. } =
                    definitions.node(root.definition).unwrap().kind()
                else {
                    continue;
                };
                assert!(
                    unit.recipes()
                        .native_inlist(root.use_id)
                        .unwrap()
                        .uses_json_keys()
                );
                let [constant, dynamic] = values.as_slice() else {
                    panic!("expected one constant and one dynamic JSON operand");
                };
                assert!(matches!(
                    definitions.node(*constant).unwrap().kind(),
                    StaticExprKind::Constant(_)
                ));
                assert!(matches!(
                    definitions.node(*dynamic).unwrap().kind(),
                    StaticExprKind::BoundCall { .. }
                ));
                let uses = unit.control_flow().uses();
                let arguments = &uses[&root.use_id].arguments;
                let input_ordinal =
                    |argument| unit.input(uses[&argument].arguments[0]).unwrap().ordinal as usize;
                let probe = input_ordinal(arguments[0]);
                let rhs = input_ordinal(arguments[2]);
                let schema = program.graph().nodes()[input.index()]
                    .output_layout()
                    .schema();
                let columns = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(ordinal, field)| match field.data_type() {
                        DataType::Utf8 if ordinal == probe => Arc::new(StringArray::from(vec![
                            Some(r#"{"b":2,"a":1}"#),
                            None,
                            Some(r#"{"b":2,"a":1}"#),
                            None,
                            Some("null"),
                            Some("null"),
                        ]))
                            as ArrayRef,
                        DataType::Utf8 if ordinal == rhs => Arc::new(StringArray::from(vec![
                            Some("{]"),
                            Some("{]"),
                            Some("false"),
                            Some("false"),
                            Some("null"),
                            None,
                        ])) as ArrayRef,
                        DataType::Utf8 => {
                            Arc::new(StringArray::from(vec!["unused"; 6])) as ArrayRef
                        }
                        DataType::Int64 => Arc::new(Int64Array::from(vec![0; 6])) as ArrayRef,
                        DataType::Boolean => {
                            Arc::new(BooleanArray::from(vec![true; 6])) as ArrayRef
                        }
                        other => panic!("unexpected IN fixture field {other:?}"),
                    })
                    .collect();
                let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
                assert!(fixture.replace((program.clone(), site, batch)).is_none());
            }
        }
    }
    let (program, site, input) = fixture.expect("actual SQL IN project");
    let control = runtime_control(Some(MemTracker::new_root("native-json-in")));
    let mut instance = frame(&program, site, &control);
    let mut journal = RuntimeScalarMemoryJournal::default();
    let all = Selection::all(6);
    let successful = [2, 3, 4, 5];
    let sparse = Selection::try_sparse(6, &successful).unwrap();
    for (selection, expected_values, expected_errors) in [
        (
            all,
            &[None, None, Some(true), None, Some(true), None][..],
            &[0, 1][..],
        ),
        (sparse, &[Some(true), None, Some(true), None][..], &[][..]),
    ] {
        let output = instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(all, selection),
                Activation::Activated,
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
            )
            .unwrap();
        let values = output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), expected_values);
        assert_eq!(output.errors().len(), expected_errors.len());
        for (error, &ordinal) in output.errors().iter().zip(expected_errors) {
            assert_eq!(error.selected_ordinal(), ordinal);
            assert_eq!(
                error.reason(),
                DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)
            );
            assert_eq!(error.message(), "Invalid JSON input");
        }
    }
}
