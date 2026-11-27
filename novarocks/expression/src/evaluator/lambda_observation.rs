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
use crate::compiler::lambda::LambdaReuseScope;
use crate::testkit::{
    CountingAllocator, HarnessControl, PassThroughCallHost, constant_policy,
    expression_text::{TextColumn, TextContext, type_expression_text},
};
use arrow::{
    array::{Array, ListArray},
    datatypes::{Int32Type, Schema},
    record_batch::RecordBatch,
};
use novarocks_expression_contract::ValueId;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, FunctionValueType, SemanticParameters,
};
use std::cell::RefCell;

pub(super) enum Event {
    Body {
        parents: Vec<usize>,
    },
    ScalarLeaf {
        rows: usize,
        broadcast: Vec<usize>,
    },
    Gather {
        is_collection: bool,
        new_stock: usize,
    },
    Reuse {
        scope: LambdaReuseScope,
        use_id: ExpressionUseId,
        rows: Vec<usize>,
    },
}

thread_local! {
    static EVENTS: RefCell<Option<Vec<Event>>> = const { RefCell::new(None) };
}

pub(super) fn record(event: impl FnOnce() -> Event) {
    EVENTS.with_borrow_mut(|events| {
        if let Some(events) = events {
            events.push(event());
        }
    });
}

fn observe<T>(run: impl FnOnce() -> T) -> (T, Vec<Event>) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            EVENTS.with_borrow_mut(|events| *events = None);
        }
    }
    EVENTS.with_borrow_mut(|events| assert!(events.replace(Vec::new()).is_none()));
    let _reset = Reset;
    (
        run(),
        EVENTS.with_borrow_mut(|events| events.take().unwrap()),
    )
}

fn program(text: &str, columns: &[(&str, ArrayRef, bool)]) -> (Arc<CompiledExprSet>, RecordBatch) {
    let typed_columns = columns
        .iter()
        .enumerate()
        .map(|(i, (name, array, nullable))| TextColumn {
            name: name.to_string(),
            value: ValueId::new(i as u32),
            value_type: FunctionValueType::new(array.data_type().clone(), *nullable),
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(
            typed_columns
                .iter()
                .map(|column| column.value_type.try_to_field(&column.name).unwrap())
                .collect::<Vec<_>>(),
        )),
        columns
            .iter()
            .map(|(_, value, _)| Arc::clone(value))
            .collect(),
    )
    .unwrap();
    let functions = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
    let parameters = SemanticParameters::default();
    let typed = type_expression_text(
        text,
        &typed_columns,
        &TextContext {
            functions: &functions,
            constant_policy: constant_policy(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            parameters: &parameters,
        },
        &HarnessControl,
    )
    .unwrap();
    let layout = typed.layout().unwrap();
    assert_eq!(
        layout.schema(),
        &batch.schema(),
        "fixture demands every original column"
    );
    let mut work =
        CompileCheckpoints::try_new(&HarnessControl, CompilePhase::LowerProgram).unwrap();
    let compiled = crate::compile_expr_set_pure(
        typed.unit(),
        typed.environment(),
        crate::ScopeLayouts {
            ports: std::slice::from_ref(layout),
            all_ports: None,
        },
        constant_policy(),
        &functions,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    (compiled, batch)
}

fn lists(values: impl IntoIterator<Item = Option<Vec<Option<i32>>>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(values))
}

#[test]
fn fallible_body_borrows_wide_guarded_captures_at_each_required_element() {
    let alternating = |a, b| {
        (0..20000)
            .map(|i| Some(if i % 2 == 0 { a } else { b }))
            .collect()
    };
    let wide = |key| {
        std::iter::once(Some(key))
            .chain(std::iter::repeat_n(Some(-1), 19999))
            .collect()
    };
    let inputs = [
        (
            "a",
            lists([Some(alternating(1, 2)), Some(alternating(2, 1))]),
            false,
        ),
        ("b", lists([Some(wide(1)), None]), true),
        ("c", lists([None, Some(wide(2))]), true),
        ("d", lists([Some(wide(2)), Some(wide(1))]), false),
    ];
    let (compiled, batch) = program(
        "filter(a, x -> contains(COALESCE(IF(x = 1, b, c), d), x))",
        &inputs,
    );
    let allocator = Arc::new(CountingAllocator::new(usize::MAX));
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> = allocator.clone();
    let mut evaluator = CompiledExpressionInstance::try_new_with_allocator(
        compiled,
        RootOrdinal::new(0),
        &HarnessControl,
        Some(host),
    )
    .unwrap();
    let selection = Selection::all(2);
    let (output, events) = observe(|| {
        evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                novarocks_functions::ScalarInvocationActivation::Activated,
                &HarnessControl,
                &mut PassThroughCallHost,
            )
            .unwrap()
    });
    assert!(output.errors().is_empty());
    let actual = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let original = inputs[0].1.as_any().downcast_ref::<ListArray>().unwrap();
    for row in 0..2 {
        assert_eq!(actual.value(row).to_data(), original.value(row).to_data());
    }
    let mut body_rows = 0;
    let mut batches = 0;
    let mut leaf_rows = 0;
    let mut capture_copy = 0;
    for event in events {
        match event {
            Event::Body { parents } => {
                body_rows += parents.len();
                batches += 1;
                assert_eq!(parents, [0, 1]);
            }
            Event::ScalarLeaf { rows, broadcast } => {
                assert_eq!(broadcast, [0]);
                leaf_rows += rows;
            }
            Event::Gather {
                is_collection: true,
                new_stock,
            } => capture_copy += new_stock,
            _ => {}
        }
    }
    assert_eq!(body_rows, 40000);
    assert_eq!(batches, 20000);
    assert!(leaf_rows > 0 && leaf_rows <= body_rows);
    assert!(
        capture_copy <= (4 * 20000 * size_of::<i32>()),
        "capture copy was {capture_copy}"
    );
    let alias = actual.value(0);
    drop(output);
    drop(evaluator);
    assert!(allocator.outstanding() > 0);
    drop(alias);
    assert_eq!(allocator.outstanding(), 0);
}

#[test]
fn scalar_call_protocol_borrows_repeated_variable_width_capture() {
    use arrow::array::{ListBuilder, StringBuilder};

    let needles = 64;
    let payload = "a".repeat(32 * 1024);
    let mut left = ListBuilder::new(StringBuilder::new());
    for ordinal in 0..needles {
        left.values().append_value(format!(
            "a distinct needle wider than the grouping metadata threshold {ordinal}"
        ));
    }
    left.append(true);
    let mut right = ListBuilder::new(StringBuilder::new());
    right.values().append_value(&payload);
    right.append(true);
    let inputs = [
        ("a", Arc::new(left.finish()) as ArrayRef, false),
        ("b", Arc::new(right.finish()) as ArrayRef, false),
    ];
    let (compiled, batch) = program("filter(a, x -> contains(b, x))", &inputs);
    let allocator = Arc::new(CountingAllocator::new(usize::MAX));
    let mut evaluator = CompiledExpressionInstance::try_new_with_allocator(
        compiled,
        RootOrdinal::new(0),
        &HarnessControl,
        Some(allocator.clone()),
    )
    .unwrap();
    let selection = Selection::all(1);
    let (output, events) = observe(|| {
        evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                novarocks_functions::ScalarInvocationActivation::Activated,
                &HarnessControl,
                &mut PassThroughCallHost,
            )
            .unwrap()
    });
    assert!(output.errors().is_empty());
    let values = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!values.is_null(0));
    assert_eq!(values.value_length(0), 0);

    let mut body_rows = 0;
    let mut capture_copy = 0;
    for event in events {
        match event {
            Event::Body { parents } => {
                assert_eq!(parents, [0]);
                body_rows += parents.len();
            }
            Event::Gather {
                is_collection: true,
                new_stock,
            } => capture_copy += new_stock,
            _ => {}
        }
    }
    assert_eq!(body_rows, needles);
    assert!(
        capture_copy <= payload.len(),
        "capture copy was {capture_copy} bytes"
    );
    drop(output);
    drop(evaluator);
    assert_eq!(allocator.outstanding(), 0);
}

#[test]
fn array_invocation_frames_reuse_actual_ancestor_rows_across_intermediate_calls() {
    let inputs = [
        (
            "a",
            lists([Some(vec![Some(1), Some(2), Some(3)]), Some(vec![])]),
            false,
        ),
        (
            "bias",
            Arc::new(arrow::array::Int32Array::from(vec![5, 10])),
            false,
        ),
    ];
    let (compiled, batch) = program(
        "transform(a, x -> transform(a, y -> transform(a, z -> bitwise_xor(bitwise_xor(z, bitwise_and(x, bias)), bitwise_and(bias, BIGINT '7')))))",
        &inputs,
    );
    let allocator = Arc::new(CountingAllocator::new(usize::MAX));
    let mut evaluator = CompiledExpressionInstance::try_new_with_allocator(
        compiled,
        RootOrdinal::new(0),
        &HarnessControl,
        Some(allocator.clone()),
    )
    .unwrap();
    let selection = Selection::all(2);
    let (output, events) = observe(|| {
        evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                novarocks_functions::ScalarInvocationActivation::Activated,
                &HarnessControl,
                &mut PassThroughCallHost,
            )
            .unwrap()
    });
    assert!(output.errors().is_empty());
    let actual = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(actual.value_length(0), 3);
    assert_eq!(actual.value_length(1), 0);
    let mut top = BTreeMap::new();
    let mut ancestor = BTreeMap::new();
    for event in events {
        if let Event::Reuse {
            scope,
            use_id,
            rows,
        } = event
        {
            let target = match scope {
                LambdaReuseScope::TopLevel => &mut top,
                LambdaReuseScope::Ancestor(_) => &mut ancestor,
            };
            target.entry(use_id).or_insert_with(Vec::new).extend(rows);
        }
    }
    assert!(top.values().any(|rows| rows == &[0]));
    assert!(
        ancestor.values().any(|rows| rows == &[0, 1, 2]),
        "actual ancestor source positions: {ancestor:?}"
    );
    for rows in top.values().chain(ancestor.values()) {
        let unique = rows.iter().collect::<BTreeSet<_>>();
        assert_eq!(
            unique.len(),
            rows.len(),
            "computed the same logical use more than once: {rows:?}"
        );
    }
    drop(output);
    drop(evaluator);
    assert_eq!(allocator.outstanding(), 0);
}

#[test]
fn scalar_call_protocol_keeps_required_match_refusal_after_an_independent_parent_stops() {
    struct RefuseRequiredBody(bool);
    impl ScalarCallHost for RefuseRequiredBody {
        type Refusal = &'static str;

        fn begin_call(&mut self) {}

        fn run_admitted(
            &mut self,
            phase: ScalarCallPhase,
            _: ScalarCallRequestFacts,
            run: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Self::Refusal>>,
        ) -> Result<(), ScalarCallFailure<Self::Refusal>> {
            let after_first_batch = EVENTS.with_borrow(|slot| {
                slot.as_ref().is_some_and(|events| {
                    events
                        .iter()
                        .filter(|event| matches!(event, Event::Body { .. }))
                        .count()
                        == 2
                })
            });
            if self.0 && phase == ScalarCallPhase::Body && after_first_batch {
                return Err(ScalarCallFailure::Host("required body allocation refused"));
            }
            run()
        }

        fn callback_request_facts(
            &mut self,
            _: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
        ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Self::Refusal>> {
            Ok(ScalarCallRequestFacts::default())
        }
    }

    let inputs = [(
        "a",
        lists([
            Some(vec![Some(2), Some(1), Some(0)]),
            Some(vec![Some(1), Some(0), Some(0)]),
        ]),
        false,
    )];
    let (compiled, batch) = program(
        "TRY(any_match(a, x -> bitwise_and(CAST(x AS BIGINT), BIGINT '1') > BIGINT '0'))",
        &inputs,
    );
    for refuse in [false, true] {
        let allocator = Arc::new(CountingAllocator::new(usize::MAX));
        let mut evaluator = CompiledExpressionInstance::try_new_with_allocator(
            Arc::clone(&compiled),
            RootOrdinal::new(0),
            &HarnessControl,
            Some(allocator.clone()),
        )
        .unwrap();
        let selection = Selection::all(2);
        let (result, events) = observe(|| {
            evaluator.evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                novarocks_functions::ScalarInvocationActivation::Activated,
                &HarnessControl,
                &mut RefuseRequiredBody(refuse),
            )
        });
        match (refuse, result) {
            (true, Err(ExprEvaluationFailure::Host("required body allocation refused"))) => {}
            (false, Ok(result)) => {
                let array = result
                    .values()
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap();
                assert_eq!(array.iter().collect::<Vec<_>>(), [Some(true), Some(true)]);
            }
            (_, other) => panic!("incorrect required body refusal or MATCH result: {other:?}"),
        }
        let parent_batches = events
            .into_iter()
            .filter_map(|event| match event {
                Event::Body { parents } => Some(parents),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(parent_batches, [vec![0, 1], vec![0]]);
        drop(evaluator);
        assert_eq!(allocator.outstanding(), 0);
    }
}

#[test]
fn array_invocation_frames_leave_inherited_row_errors_out_of_body_demand() {
    let inputs = [
        (
            "a",
            lists([Some(vec![Some(1)]), Some(vec![Some(2)])]),
            false,
        ),
        (
            "d",
            Arc::new(arrow::array::Int32Array::from(vec![1, 0])),
            false,
        ),
    ];
    let (compiled, batch) = program("TRY(transform(IF(1 / d > 0, a, a), x -> x + 1))", &inputs);
    let allocator = Arc::new(CountingAllocator::new(usize::MAX));
    let mut evaluator = CompiledExpressionInstance::try_new_with_allocator(
        compiled,
        RootOrdinal::new(0),
        &HarnessControl,
        Some(allocator.clone()),
    )
    .unwrap();
    let selection = Selection::all(2);
    let (result, events) = observe(|| {
        evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                novarocks_functions::ScalarInvocationActivation::Activated,
                &HarnessControl,
                &mut PassThroughCallHost,
            )
            .unwrap()
    });
    assert!(result.errors().is_empty());
    let array = result
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(array.value_length(0), 1);
    assert!(array.is_null(1));
    let positions = events
        .into_iter()
        .filter_map(|event| match event {
            Event::Body { parents } => Some(parents),
            _ => None,
        })
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(positions, [0]);
    drop(result);
    drop(evaluator);
    assert_eq!(allocator.outstanding(), 0);
}
