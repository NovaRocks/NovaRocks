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
    CompileControlError, CompilePhase, ControlShape, ExpressionUseId, OperatorFunction,
    PureCompileControl,
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
            if let Some(conjunction) = range_conjunction(unit, root.use_id)
                && unit.control_flow().uses()[&conjunction].arguments.len() == 2
            {
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
fn range_conjunction(unit: &CompiledExprSet, root: ExpressionUseId) -> Option<ExpressionUseId> {
    let uses = unit.control_flow().uses();
    let use_id = if unit.calls().get(&root).is_some_and(|call| {
        call.call_contract().function_id().as_str()
            == OperatorFunction::BooleanNot.function_id_text()
    }) {
        *uses[&root].arguments.first()?
    } else {
        root
    };
    let use_id = if uses[&use_id].control == ControlShape::Let {
        *uses[&use_id].arguments.get(1)?
    } else {
        use_id
    };
    (uses[&use_id].control == ControlShape::Conjunction).then_some(use_id)
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
fn between_materializes_two_ordered_comparisons_of_the_same_input_port() {
    let (program, site) = compiled("SELECT k BETWEEN 1 AND 5 FROM fixture.source");
    let flow = program.control_flow();
    let root = program.root(site).unwrap().use_id;
    let invocation = &flow.uses()[&root];
    assert_eq!(invocation.control, ControlShape::Conjunction);
    let [lower, upper] = invocation.arguments.as_ref() else {
        panic!("two comparison calls")
    };
    let lower_probe = flow.uses()[lower].arguments[1];
    let upper_probe = flow.uses()[upper].arguments[0];
    assert_ne!(lower_probe, upper_probe);
    let lower_port = program
        .input(lower_probe)
        .expect("first direct probe input");
    let upper_port = program
        .input(upper_probe)
        .expect("second direct probe input");
    assert_eq!(lower_port.scope, upper_port.scope);
    assert_eq!(lower_port.ordinal, upper_port.ordinal);
    for comparison in [lower, upper] {
        assert_eq!(
            program.calls()[comparison]
                .call_contract()
                .function_id()
                .as_str(),
            OperatorFunction::LessThanOrEqual.function_id_text()
        );
    }
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
const NEGATE_MIN_ERROR: &str = "bigint negation overflow: -9223372036854775808";
#[test]
fn between_skips_upper_work_after_false_lower_and_keeps_the_first_error() {
    for (sql, expected, error) in [
        (
            "SELECT k BETWEEN 9223372036854775807 AND -k FROM fixture.source",
            vec![Some(false); 3],
            false,
        ),
        (
            "SELECT k BETWEEN -k AND -k FROM fixture.source",
            // At MIN the lower error is retained once. At 0 both comparisons
            // are true; MAX <= -MAX is false for the final row.
            vec![None, Some(true), Some(false)],
            true,
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
        assert_eq!(out.errors().len(), usize::from(error));
        if error {
            assert_eq!(out.errors()[0].selected_ordinal(), 0);
            assert_eq!(out.errors()[0].message(), NEGATE_MIN_ERROR);
        }
        assert_eq!(values(&out), expected);
        assert!(!out.values().is_null(1));
    }
}
#[test]
fn between_evaluates_the_rand_initializer_once_and_reads_its_local_value_twice() {
    let (program, site) =
        compiled("SELECT rand(k) BETWEEN BIGINT '-1' AND BIGINT '42' FROM fixture.source");
    let flow = program.control_flow();
    let root = &flow.uses()[&program.root(site).unwrap().use_id];
    assert_eq!(root.control, ControlShape::Let);
    let [initializer, body] = root.arguments.as_ref() else {
        panic!("Let initializer and body")
    };
    let effects = &program.effects()[initializer];
    assert!(
        effects
            .for_use(flow.uses()[initializer].context)
            .unwrap()
            .observable_effects
            .rng_sampling
    );
    let [lower, upper] = flow.uses()[body].arguments.as_ref() else {
        panic!("two ordered comparisons")
    };
    for reference in [
        flow.uses()[lower].arguments[1],
        flow.uses()[upper].arguments[0],
    ] {
        assert!(matches!(
            program
                .definitions()
                .node(flow.uses()[&reference].definition)
                .unwrap()
                .kind(),
            StaticExprKind::LocalValue { .. }
        ));
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
fn between_control_roundtrips_and_refuses_a_dangling_comparison() {
    for sql in [
        "SELECT k BETWEEN 1 AND 5 FROM fixture.source",
        "SELECT k NOT BETWEEN 1 AND 5 FROM fixture.source",
    ] {
        let source = sql_source(sql, DataType::Int64);
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
                if invocation.control == ControlShape::Conjunction {
                    let emitted = encoded
                        .uses
                        .iter_mut()
                        .find(|entry| entry.id == id.get())
                        .unwrap();
                    assert_eq!(emitted.argument_use_ids.len(), 2);
                    emitted.argument_use_ids[1] = u32::MAX;
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

mod actual_sql;
