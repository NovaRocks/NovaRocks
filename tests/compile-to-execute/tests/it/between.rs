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

//! Actual SQL author -> native package -> LocalCompiler -> mutable Frame.
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{CompiledExprSet, StaticExprKind};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_expression_contract::RootOrdinal;
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_sql_testkit::sql_source;
use novarocks_test_compile_to_execute::programs::programs_with_catalogue;

use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, PureCompileControl,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("BETWEEN never waits")
    }
}
fn compiled(sql: &str) -> (Arc<CompiledExprSet>, RootOrdinal) {
    let source = sql_source(sql, DataType::Int64);
    let functions = novarocks_sql_testkit::installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue(&source, &functions);
    let mut found = None;
    for unit in programs.values().flat_map(|program| program.units()) {
        for (ordinal, root) in unit.roots().iter().enumerate() {
            let definition = unit.definitions().node(root.definition).unwrap();
            if matches!(definition.kind(), StaticExprKind::PreparedBetween { .. }) {
                assert!(found.is_none(), "one actual predicate root");
                found = Some((
                    unit.clone(),
                    RootOrdinal::new(u32::try_from(ordinal).unwrap()),
                ));
            }
        }
    }
    found.expect("actual SQL BETWEEN root survived lowering")
}
fn batch(unit: &CompiledExprSet, root: RootOrdinal, input: ArrayRef) -> RecordBatch {
    let scope = unit.root(root).unwrap().scope;
    let schema = unit.scope(scope).unwrap().schema().clone();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), input.data_type());
    RecordBatch::try_new(schema, vec![input]).unwrap()
}
fn values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn between_actual_after_four_ordered_uses_original_three_definitions() {
    let (program, site) = compiled("SELECT k BETWEEN 1 AND 5 FROM fixture.source");
    let flow = program.control_flow();
    let use_id = program.root(site).unwrap().use_id;
    let invocation = &flow.uses()[&use_id];
    assert_eq!(invocation.control, ControlShape::Between { negated: false });
    assert_eq!(invocation.arguments.len(), 4);
    assert_ne!(invocation.arguments[0], invocation.arguments[2]);
    assert_eq!(
        flow.uses()[&invocation.arguments[0]].definition,
        flow.uses()[&invocation.arguments[2]].definition
    );
    for ordinal in 1..4 {
        let child = &flow.uses()[&invocation.arguments[ordinal]];
        assert_eq!(
            flow.regions()[&child.context.region].guard.unwrap().kind,
            novarocks_type_contract::GuardKind::BetweenAfterSource {
                ordinal: (ordinal - 1) as u32
            }
        );
    }
    let definition = program.definitions().node(invocation.definition).unwrap();
    let StaticExprKind::PreparedBetween {
        operand, low, high, ..
    } = definition.kind()
    else {
        panic!("exact recipe")
    };
    assert_eq!(
        [*operand, *low, *high],
        [
            flow.uses()[&invocation.arguments[0]].definition,
            flow.uses()[&invocation.arguments[1]].definition,
            flow.uses()[&invocation.arguments[3]].definition
        ]
    );
}
#[test]
fn between_actual_after_nullable_case_sparse_sliced_and_empty_three_valued_output() {
    for (sql, expected) in [
        (
            "SELECT CASE WHEN k = 0 THEN NULL ELSE k END BETWEEN 1 AND 5 FROM fixture.source",
            vec![None, Some(true), Some(true), Some(false)],
        ),
        (
            "SELECT CASE WHEN k = 0 THEN NULL ELSE k END NOT BETWEEN 1 AND 5 FROM fixture.source",
            vec![None, Some(false), Some(false), Some(true)],
        ),
    ] {
        let (program, site) = compiled(sql);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![42, 0, 1, 3, 9, 43]).slice(1, 4));
        let data = batch(&program, site, input);
        for rows in [vec![0, 1, 2, 3], vec![0, 2], Vec::new()] {
            let selection = Selection::try_sparse(4, &rows).unwrap();
            let mut frame =
                CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
            let out = frame
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert!(out.errors().is_empty());
            assert_eq!(
                values(&out),
                rows.iter().map(|r| expected[*r]).collect::<Vec<_>>()
            );
        }
    }
}
// Negating -2^63 requires +2^63, beyond signed INT64's maximum 2^63-1.
// The frozen arithmetic diagnostic spells the typed zero/subtraction and the
// original offending decimal value; no evaluator manufactures this expectation.
const NEGATE_MIN_ERROR: &str =
    "Arithmetic overflow: Overflow happened on: 0 - -9223372036854775808";
#[test]
fn between_actual_after_upper_error_is_required_even_after_false_lower_and_lower_error_is_first() {
    for (sql, expected) in [
        (
            "SELECT k BETWEEN 9223372036854775807 AND -k FROM fixture.source",
            // A false lower comparison never suppresses the required upper
            // NEGATE error at MIN. For 0 and MAX the conjunction is false.
            vec![None, Some(false), Some(false)],
        ),
        (
            "SELECT k BETWEEN -k AND -k FROM fixture.source",
            // At MIN the lower error is retained once. At 0 both comparisons
            // are true; MAX <= -MAX is false for the final row.
            vec![None, Some(true), Some(false)],
        ),
    ] {
        let (program, site) = compiled(sql);
        let data = batch(
            &program,
            site,
            Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
        );
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        let out = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(out.errors().len(), 1);
        assert_eq!(out.errors()[0].selected_ordinal(), 0);
        assert_eq!(out.errors()[0].message(), NEGATE_MIN_ERROR);
        assert_eq!(values(&out), expected);
        assert!(!out.values().is_null(1));
    }
}
#[test]
fn between_actual_after_seeded_rand_keeps_two_observable_uses_of_one_operand_definition() {
    let (program, site) = compiled("SELECT rand(k) BETWEEN -1.0 AND 2.0 FROM fixture.source");
    let flow = program.control_flow();
    let root = &flow.uses()[&program.root(site).unwrap().use_id];
    assert_ne!(root.arguments[0], root.arguments[2]);
    assert_eq!(
        flow.uses()[&root.arguments[0]].definition,
        flow.uses()[&root.arguments[2]].definition
    );
    for use_id in [root.arguments[0], root.arguments[2]] {
        let effects = &program.effects()[&use_id];
        assert!(
            effects
                .for_use(flow.uses()[&use_id].context)
                .unwrap()
                .observable_effects
                .rng_sampling
        );
    }
    let data = batch(&program, site, Arc::new(Int64Array::from(vec![17, 23, 41])));
    let mut frame = CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
    let out = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(values(&out), vec![Some(true); 3]);
    assert!(out.errors().is_empty());
}
struct Callback {
    trace: Mutex<Vec<u32>>,
    stop: usize,
    cause: KernelFailure,
}
impl Callback {
    fn new(stop: usize, cause: KernelFailure) -> Self {
        Self {
            trace: Mutex::new(vec![]),
            stop,
            cause,
        }
    }
}
impl KernelEvaluationControl for Callback {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        assert!(t.len() < self.stop, "no callback after primary refusal");
        t.push(n);
        if t.len() == self.stop {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("BETWEEN never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("between-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("between-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("between-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn between_actual_after_every_constructor_and_phase_callback_seven_causes_no_footer_or_replay() {
    let (program, site) = compiled("SELECT k BETWEEN 1 AND 5 FROM fixture.source");
    let data = batch(&program, site, Arc::new(Int64Array::from(vec![0, 3, 9])));
    let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
    CompiledExpressionInstance::try_new(program.clone(), site, &record).unwrap();
    let trace = record.trace.lock().unwrap().clone();
    for stop in 1..=trace.len() {
        for cause in causes() {
            let c = Callback::new(stop, cause.clone());
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.clone(),site,&c),Err(actual) if actual==cause)
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
        }
    }
    for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &record,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let trace = record.trace.lock().unwrap().clone();
        for stop in 1..=trace.len() {
            for cause in causes() {
                let mut frame =
                    CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
                let c = Callback::new(stop, cause.clone());
                let result = frame.evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &c,
                    &mut PassThroughCallHost,
                );
                assert!(
                    matches!(result, Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
                let after = Callback::new(usize::MAX, KernelFailure::Cancelled);
                assert!(matches!(
                    frame.evaluate(
                        EvaluationInput::Batch {
                            batch: &data,
                            source: None
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &after,
                        &mut PassThroughCallHost
                    ),
                    Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn between_actual_after_original_control_codec_roundtrip_and_invalid_four_use_receipt() {
    for (sql, negated) in [
        ("SELECT k BETWEEN 1 AND 5 FROM fixture.source", false),
        ("SELECT k NOT BETWEEN 1 AND 5 FROM fixture.source", true),
    ] {
        let source = sql_source(sql, DataType::Int64);
        let policy = novarocks_functions::ConstantPolicy {
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
        source.require_statement_constant_policy(policy).unwrap();
        let mut seen = 0;
        for unit in source
            .plan()
            .fragments()
            .values()
            .flat_map(|f| f.nodes().values())
            .filter_map(|node| node.expressions.as_deref())
        {
            let mut encoded = novarocks_plan_codec::physical_control_v2::encode_expression_control(
                unit.control_flow(),
                &Control,
            )
            .unwrap();
            let decoded = novarocks_plan_codec::physical_control_v2::decode_expression_control(
                unit.definitions(),
                &encoded,
                &Control,
            )
            .unwrap();
            assert_eq!(&decoded, unit.control_flow());
            assert_eq!(decoded.root_use_ids(), unit.control_flow().root_use_ids());
            for (root, use_id) in unit.roots().iter().zip(unit.root_use_ids()) {
                assert_eq!(decoded.uses()[use_id].definition, root.definition);
                assert_eq!(decoded.uses()[use_id].context.demand, root.demand);
            }
            let before_count = seen;
            for (&id, invocation) in unit.control_flow().uses() {
                if invocation.control == (ControlShape::Between { negated }) {
                    let emitted = encoded
                        .uses
                        .iter_mut()
                        .find(|entry| entry.id == id.get())
                        .unwrap();
                    assert_eq!(emitted.argument_use_ids.len(), 4);
                    emitted.argument_use_ids.pop();
                    seen += 1;
                }
            }
            if seen != before_count {
                assert!(
                    novarocks_plan_codec::physical_control_v2::decode_expression_control(
                        unit.definitions(),
                        &encoded,
                        &Control,
                    )
                    .is_err()
                );
            }
        }
        assert_eq!(seen, 1);
    }
}
