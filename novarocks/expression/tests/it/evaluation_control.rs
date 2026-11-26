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

//! Constructor and evaluation interruption at the original typed roots.

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPools, ConstantValue};
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ExprEvaluationFailure, ScopeLayouts,
    testkit::{
        CallbackControl, HarnessControl, PassThroughCallHost, callback_causes, checked_constants,
        compile_fixture_unit, fixture_scope,
    },
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection, testkit::installed_builtin_owner_catalogue,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameterValue, SemanticParameters,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

const SQL_LITERAL_POLICY: ConstantPolicy = ConstantPolicy {
    max_rows: 64,
    max_array_nodes: 1024,
    max_logical_elements: 4096,
    max_retained_buffer_bytes: 1 << 20,
    max_type_depth: 64,
    max_type_nodes: 4096,
    max_dictionary_depth: 64,
    max_metadata_bytes: 1 << 20,
    max_library_validation_work: 1 << 20,
    max_library_validation_bytes: 1 << 20,
};

const VALUES_POLICY: ConstantPolicy = ConstantPolicy {
    max_rows: 16,
    max_array_nodes: 128,
    max_logical_elements: 1024,
    ..SQL_LITERAL_POLICY
};

fn comparison_root<F>(
    source_name: &str,
    source_type: DataType,
    parameters: &SemanticParameters,
    expression: F,
) -> (Arc<CompiledExprSet>, RootOrdinal)
where
    F: FnOnce(
        &mut BoundExprSetBuilder,
        &mut ConstantPools,
        novarocks_expression_contract::ExprId,
    ) -> ExprKind,
{
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let mut constants = ConstantPools::empty();
    let ty = FunctionValueType::new(source_type.clone(), true);
    let value = ValueId::new(0);
    let source = draft.add(None, ty.clone(), ExprKind::Value(value)).unwrap();
    let kind = expression(&mut draft, &mut constants, source);
    let comparison = draft
        .add(None, FunctionValueType::new(DataType::Boolean, true), kind)
        .unwrap();
    let root = draft
        .root(comparison, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let port = ExprPort {
        values: Box::new([(value, ty)]),
    };
    let scope = fixture_scope(
        &port,
        Arc::new(Schema::new(vec![Field::new(
            source_name,
            source_type,
            true,
        )])),
    );
    let unit = compile_ownerless_fixture(
        draft,
        vec![port],
        EvaluationEnvironment {
            constants: &constants,
            parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        SQL_LITERAL_POLICY,
    );
    (unit, root)
}

fn compile_ownerless_fixture(
    draft: BoundExprSetBuilder,
    ports: Vec<ExprPort>,
    environment: EvaluationEnvironment<'_>,
    layouts: ScopeLayouts<'_>,
    policy: ConstantPolicy,
) -> Arc<CompiledExprSet> {
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let unit = draft
        .seal(ports, &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    compile_fixture_unit(
        &unit,
        environment,
        layouts,
        policy,
        &installed_builtin_owner_catalogue(),
    )
}

fn signed_in_callback_root() -> (Arc<CompiledExprSet>, RootOrdinal) {
    let allow_throw = SemanticParameterRef {
        id: SemanticParameterId::new(0),
        expected_key: SemanticParameterKey::AllowThrowException,
    };
    let parameters = SemanticParameters::try_new([(
        allow_throw.id,
        SemanticParameterValue::AllowThrowException(false),
    )])
    .unwrap();
    comparison_root("k", DataType::Int64, &parameters, |draft, pools, source| {
        // The original SQL harness selects INTEGER syntax literals, widens
        // their carriers to the probe's BIGINT, and uses a declining fold
        // port. Keep both conversion nodes in the original candidate order.
        let integer = FunctionValueType::new(DataType::Int32, false);
        let widen_to = FunctionValueType::new(DataType::Int64, false);
        let mut list = Vec::new();
        for value in [1, 5] {
            let value = ConstantValue::from_i32(
                Arc::new(Field::new("literal", DataType::Int32, false)),
                integer.clone(),
                value,
                SQL_LITERAL_POLICY,
                CompilePhase::FunctionSpecialization,
                &HarnessControl,
            )
            .unwrap();
            let literal = draft
                .add(None, integer.clone(), checked_constants::kind(pools, value))
                .unwrap();
            let widened = draft
                .add(
                    None,
                    widen_to.clone(),
                    ExprKind::Cast {
                        cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
                        expr: literal,
                        target: DataType::Int64,
                    },
                )
                .unwrap();
            list.push(widened);
        }
        // SQL's typed NULL conversion emits a nullable BIGINT directly.
        let nullable = FunctionValueType::new(DataType::Int64, true);
        let null = ConstantValue::null(
            Arc::new(Field::new("literal", DataType::Int64, true)),
            nullable.clone(),
            SQL_LITERAL_POLICY,
            CompilePhase::FunctionSpecialization,
            &HarnessControl,
        )
        .unwrap();
        list.push(
            draft
                .add(None, nullable, checked_constants::kind(pools, null))
                .unwrap(),
        );
        ExprKind::InList {
            expr: source,
            list: list.into_boxed_slice(),
            negated: false,
        }
    })
}

fn like_callback_root() -> (Arc<CompiledExprSet>, RootOrdinal) {
    let parameters = SemanticParameters::try_new([]).unwrap();
    comparison_root(
        "name",
        DataType::Utf8,
        &parameters,
        |draft, pools, source| {
            let ty = FunctionValueType::new(DataType::Utf8, false);
            let constant = ConstantValue::from_utf8(
                Arc::new(Field::new("literal", DataType::Utf8, false)),
                ty.clone(),
                "a%",
                SQL_LITERAL_POLICY,
                CompilePhase::FunctionSpecialization,
                &HarnessControl,
            )
            .unwrap();
            let pattern = draft
                .add(None, ty, checked_constants::kind(pools, constant))
                .unwrap();
            ExprKind::Like {
                expr: source,
                pattern,
                negated: false,
            }
        },
    )
}

// These are the original (nullable key, nullable payload) Values ports. Keep
// both root sites and their combined layout while exercising only the probe.
fn join_probe_callback_root() -> (Arc<CompiledExprSet>, RootOrdinal) {
    let ty = FunctionValueType::new(DataType::Int64, true);
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let left_key = draft
        .add(None, ty.clone(), ExprKind::Value(ValueId::new(0)))
        .unwrap();
    let right_key = draft
        .add(None, ty.clone(), ExprKind::Value(ValueId::new(2)))
        .unwrap();
    let root = draft
        .root(left_key, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    draft
        .root(right_key, EvaluationDemand::Value, RootScope::Port(1))
        .unwrap();
    let ports = [0_u32, 2].map(|first| ExprPort {
        values: Box::new([
            (ValueId::new(first), ty.clone()),
            (ValueId::new(first + 1), ty.clone()),
        ]),
    });
    let fields = [0, 1].map(|node| {
        [0, 1].map(|ordinal| {
            Field::new(
                format!("local_values_{node}_{ordinal}"),
                DataType::Int64,
                true,
            )
        })
    });
    let layouts = [
        fixture_scope(&ports[0], Arc::new(Schema::new(fields[0].to_vec()))),
        fixture_scope(&ports[1], Arc::new(Schema::new(fields[1].to_vec()))),
    ];
    let combined = fixture_scope(
        &ExprPort {
            values: ports
                .iter()
                .flat_map(|port| port.values.iter().cloned())
                .collect(),
        },
        Arc::new(Schema::new(
            fields.into_iter().flatten().collect::<Vec<_>>(),
        )),
    );
    let unit = compile_ownerless_fixture(
        draft,
        ports.into(),
        EvaluationEnvironment {
            constants: &ConstantPools::empty(),
            parameters: &SemanticParameters::try_new([]).unwrap(),
        },
        ScopeLayouts {
            ports: &layouts,
            all_ports: Some(&combined),
        },
        VALUES_POLICY,
    );
    (unit, root)
}

struct ComparisonBuildControl(&'static str);
impl KernelEvaluationControl for ComparisonBuildControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("{}", self.0)
    }
}

fn assert_comparison_callbacks(
    unit: &Arc<CompiledExprSet>,
    root: RootOrdinal,
    data: &RecordBatch,
    diagnostics: [&str; 3],
    wait_diagnostic: &'static str,
) {
    let recording = CallbackControl::bounded(KernelFailure::Cancelled, usize::MAX, wait_diagnostic);
    CompiledExpressionInstance::try_new(unit.clone(), root, &recording).unwrap();
    let trace = recording.trace();
    for stop in 1..=trace.len() {
        for cause in callback_causes(diagnostics) {
            let control = CallbackControl::bounded(cause.clone(), stop, wait_diagnostic);
            let result = CompiledExpressionInstance::try_new(unit.clone(), root, &control);
            assert!(matches!(result, Err(actual) if actual == cause));
            assert_eq!(control.trace(), trace[..stop]);
        }
    }

    let input = EvaluationInput::Batch {
        batch: data,
        source: None,
    };
    let construction = ComparisonBuildControl(wait_diagnostic);
    for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let invocation = ScalarInvocationScope::new(selection, selection);
        let mut frame =
            CompiledExpressionInstance::try_new(unit.clone(), root, &construction).unwrap();
        // As in the original test, record only the evaluation trace.
        let recording =
            CallbackControl::bounded(KernelFailure::Cancelled, usize::MAX, wait_diagnostic);
        frame
            .evaluate(
                input,
                invocation,
                ScalarInvocationActivation::Activated,
                &recording,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let trace = recording.trace();
        for stop in 1..=trace.len() {
            for cause in callback_causes(diagnostics) {
                let control = CallbackControl::bounded(cause.clone(), stop, wait_diagnostic);
                let mut frame =
                    CompiledExpressionInstance::try_new(unit.clone(), root, &construction).unwrap();
                let result = frame.evaluate(
                    input,
                    invocation,
                    ScalarInvocationActivation::Activated,
                    &control,
                    &mut PassThroughCallHost,
                );
                assert!(
                    matches!(result, Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), trace[..stop]);
                let after =
                    CallbackControl::bounded(KernelFailure::Cancelled, usize::MAX, wait_diagnostic);
                assert!(matches!(
                    frame.evaluate(
                        input,
                        invocation,
                        ScalarInvocationActivation::Activated,
                        &after,
                        &mut PassThroughCallHost,
                    ),
                    Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
                ));
                assert!(after.trace().is_empty());
            }
        }
    }
}

fn original_columns(
    unit: &CompiledExprSet,
    root: RootOrdinal,
    arrays: Vec<ArrayRef>,
) -> RecordBatch {
    let layout = unit.scope(unit.root(root).unwrap().scope).unwrap();
    assert_eq!(layout.schema().fields().len(), arrays.len());
    for (field, array) in layout.schema().fields().iter().zip(&arrays) {
        assert_eq!(field.data_type(), array.data_type());
    }
    RecordBatch::try_new(layout.schema().clone(), arrays).unwrap()
}

#[test]
fn inlist_signed_actual_every_constructor_phase_callback_seven_causes_no_footer_or_replay() {
    let (unit, root) = signed_in_callback_root();
    let batch = original_columns(&unit, root, vec![Arc::new(Int64Array::from(vec![0, 3, 9]))]);
    assert_comparison_callbacks(
        &unit,
        root,
        &batch,
        [
            "in-origin-invalid",
            "in-origin-internal",
            "in-origin-operational",
        ],
        "IN never waits",
    );
}

#[test]
fn like_actual_every_constructor_phase_callback_seven_causes_no_footer_or_replay() {
    let (unit, root) = like_callback_root();
    let batch = original_columns(
        &unit,
        root,
        vec![Arc::new(StringArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
        ]))],
    );
    assert_comparison_callbacks(
        &unit,
        root,
        &batch,
        [
            "like-origin-invalid",
            "like-origin-internal",
            "like-origin-operational",
        ],
        "LIKE never waits",
    );
}

#[test]
fn join_probe_owned_root_frame_every_actual_callback_keeps_seven_causes_and_latch() {
    use novarocks_functions::KernelDiagnostic;

    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            let mut calls = self.calls.lock().unwrap();
            let at = calls.len();
            calls.push(units);
            if let Some((stop, cause)) = &self.refusal
                && at == *stop
            {
                return Err(cause.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("a key root never waits")
        }
    }
    let (unit, root) = join_probe_callback_root();
    let batch = original_columns(
        &unit,
        root,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])),
            Arc::new(Int64Array::from(vec![Some(10), Some(40), Some(20)])),
        ],
    );
    let make = || {
        let construction = Control {
            calls: Mutex::new(vec![]),
            refusal: None,
        };
        CompiledExpressionInstance::try_new(unit.clone(), root, &construction).unwrap()
    };
    let baseline = Control {
        calls: Mutex::new(vec![]),
        refusal: None,
    };
    let input = EvaluationInput::Batch {
        batch: &batch,
        source: None,
    };
    let selection = Selection::all(3);
    let invocation = ScalarInvocationScope::new(selection, selection);
    let mut instance = make();
    instance
        .evaluate(
            input,
            invocation,
            ScalarInvocationActivation::Activated,
            &baseline,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let trace = baseline.calls.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original")),
            KernelFailure::Internal(KernelDiagnostic::new("original")),
            KernelFailure::Operational(KernelDiagnostic::new("original")),
            KernelFailure::InstanceFailed,
        ] {
            let control = Control {
                calls: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let mut instance = make();
            assert_eq!(
                instance
                    .evaluate(
                        input,
                        invocation,
                        ScalarInvocationActivation::Activated,
                        &control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap_err(),
                ExprEvaluationFailure::Kernel(cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), trace[..=at]);
            let before = control.calls.lock().unwrap().clone();
            assert_eq!(
                instance
                    .evaluate(
                        input,
                        invocation,
                        ScalarInvocationActivation::Activated,
                        &control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap_err(),
                ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
            );
            assert_eq!(*control.calls.lock().unwrap(), before);
        }
    }
}

mod callback_cases;
mod filter_conjunction;

fn integer_lists(values: &[Option<i32>]) -> ArrayRef {
    use arrow::array::{ListArray, types::Int32Type};
    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        values.iter().map(|value| Some([*value])),
    ))
}

fn text_lists(values: &[&str]) -> ArrayRef {
    use arrow::array::{ListBuilder, StringBuilder};
    let mut list = ListBuilder::new(StringBuilder::new());
    for value in values {
        list.values().append_value(*value);
        list.append(true);
    }
    Arc::new(list.finish())
}

fn evaluate_sparse_composite_guard(
    inputs: &[(&str, ArrayRef)],
    rows: &[usize],
    result: FunctionValueType,
    author: impl FnOnce(
        &mut BoundExprSetBuilder,
        &mut ConstantPools,
        &[novarocks_expression_contract::ExprId],
        &[FunctionValueType],
    ) -> ExprKind,
) -> (ArrayRef, Box<[novarocks_functions::RowDataError]>) {
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let types = inputs
        .iter()
        .map(|(_, array)| FunctionValueType::new(array.data_type().clone(), false))
        .collect::<Vec<_>>();
    let values = types
        .iter()
        .enumerate()
        .map(|(index, ty)| (ValueId::new(index as u32), ty.clone()))
        .collect::<Vec<_>>();
    let sources = values
        .iter()
        .map(|(value, ty)| {
            draft
                .add(None, ty.clone(), ExprKind::Value(*value))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut constants = ConstantPools::empty();
    let kind = author(&mut draft, &mut constants, &sources, &types);
    let expr = draft.add(None, result, kind).unwrap();
    let root = draft
        .root(expr, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let schema = Arc::new(Schema::new(
        inputs
            .iter()
            .zip(&types)
            .map(|((name, _), ty)| ty.try_to_field(*name).unwrap())
            .collect::<Vec<_>>(),
    ));
    let port = ExprPort {
        values: values.into_boxed_slice(),
    };
    let scope = fixture_scope(&port, Arc::clone(&schema));
    let parameters = SemanticParameters::try_new([]).unwrap();
    let program = compile_ownerless_fixture(
        draft,
        vec![port],
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        SQL_LITERAL_POLICY,
    );
    let input = RecordBatch::try_new(
        schema,
        inputs.iter().map(|(_, array)| array.clone()).collect(),
    )
    .unwrap();
    let selection = Selection::try_sparse(input.num_rows(), rows).unwrap();
    let output = CompiledExpressionInstance::try_new(program, root, &HarnessControl)
        .unwrap()
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.selection(), selection);
    let (_, values, errors) = output.into_parts();
    (values, errors)
}

fn explicit_guard_cast(
    draft: &mut BoundExprSetBuilder,
    source: novarocks_expression_contract::ExprId,
    target: &FunctionValueType,
) -> novarocks_expression_contract::ExprId {
    draft
        .add(
            None,
            target.clone(),
            ExprKind::Cast {
                cast_mode: novarocks_type_contract::TrinoCastMode::Explicit,
                expr: source,
                target: target.data_type().clone(),
            },
        )
        .unwrap()
}

#[test]
fn simple_case_composite_selected_rows_keep_unknown_and_demanded_label_errors_separate() {
    use arrow::array::Int32Array;
    use novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT;
    use novarocks_functions::DataFailureReason;

    let inputs = [
        (
            "probe",
            integer_lists(&[
                Some(99),
                Some(1),
                Some(99),
                None,
                Some(99),
                Some(2),
                Some(2),
                Some(5),
            ]),
        ),
        ("first", integer_lists(&[Some(1); 8])),
        (
            "second_text",
            text_lists(&["bad", "bad", "bad", "2", "bad", "bad", "2", "6"]),
        ),
        (
            "else_text",
            Arc::new(StringArray::from(vec![
                "bad", "bad", "bad", "30", "bad", "777", "bad", "40",
            ])) as ArrayRef,
        ),
    ];
    let rows = [1, 3, 5, 6, 7];
    let result_type = FunctionValueType::new(DataType::Int32, false);
    let (output, errors) = evaluate_sparse_composite_guard(
        &inputs,
        &rows,
        result_type.clone(),
        |draft, pools, sources, types| {
            let mut integer = |draft: &mut BoundExprSetBuilder, value| {
                let value = ConstantValue::from_i32(
                    Arc::new(Field::new("literal", DataType::Int32, false)),
                    result_type.clone(),
                    value,
                    SQL_LITERAL_POLICY,
                    CompilePhase::FunctionSpecialization,
                    &HarnessControl,
                )
                .unwrap();
                draft
                    .add(
                        None,
                        result_type.clone(),
                        checked_constants::kind(pools, value),
                    )
                    .unwrap()
            };
            let first_result = integer(draft, 10);
            let second_result = integer(draft, 20);
            let second_label = explicit_guard_cast(draft, sources[2], &types[0]);
            let fallback = explicit_guard_cast(draft, sources[3], &result_type);
            ExprKind::Case {
                ieee_float_equality: false,
                operand: Some(sources[0]),
                when_then: Box::new([(sources[1], first_result), (second_label, second_result)]),
                else_expr: Some(fallback),
            }
        },
    );
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(10), Some(30), None, Some(20), Some(40)],
    );
    let [error] = errors.as_ref() else {
        panic!("only the demanded invalid label may fail");
    };
    assert_eq!(error.selected_ordinal(), 2);
    assert_eq!(rows[error.selected_ordinal()], 5);
    assert_eq!(
        error.reason(),
        DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
    );
}

#[test]
fn inlist_composite_selected_rows_keep_true_over_unknown_and_report_later_demanded_errors() {
    use arrow::array::BooleanArray;
    use novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT;
    use novarocks_functions::DataFailureReason;

    let inputs = [
        (
            "probe",
            integer_lists(&[
                Some(99),
                Some(1),
                Some(99),
                Some(2),
                Some(99),
                Some(3),
                Some(5),
                Some(5),
            ]),
        ),
        (
            "first",
            integer_lists(&[
                Some(0),
                Some(1),
                Some(0),
                None,
                Some(0),
                None,
                Some(4),
                Some(4),
            ]),
        ),
        (
            "second_text",
            text_lists(&["bad", "bad", "bad", "2", "bad", "4", "bad", "6"]),
        ),
    ];
    let rows = [1, 3, 5, 6, 7];
    for negated in [false, true] {
        let (output, errors) = evaluate_sparse_composite_guard(
            &inputs,
            &rows,
            FunctionValueType::new(DataType::Boolean, true),
            |draft, _, sources, types| ExprKind::InList {
                expr: sources[0],
                list: Box::new([
                    sources[1],
                    explicit_guard_cast(draft, sources[2], &types[0]),
                ]),
                negated,
            },
        );
        let actual = output.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            [None, Some(!negated), None, None, Some(negated)],
        );
        // The original membership control still invokes both candidates.
        // Sparse outer rows are excluded even when their candidate would fail.
        let [after_match, after_miss] = errors.as_ref() else {
            panic!("only the two demanded invalid candidates may fail");
        };
        for (error, ordinal, row) in [(after_match, 0, 1), (after_miss, 3, 6)] {
            assert_eq!(error.selected_ordinal(), ordinal);
            assert_eq!(rows[error.selected_ordinal()], row);
            assert_eq!(
                error.reason(),
                DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
            );
        }
    }
}
