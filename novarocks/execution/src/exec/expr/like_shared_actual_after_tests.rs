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

//! Actual original LIKE Eager2 source and lifecycle probes.
//! Formal host MEM grants remain a separate open gate.
use super::compiled_program::CompiledExpressionInstance;
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue_and_original_inlist_source;
use super::sql_test_source::sql_source_with_single_field_and_core_residuals;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure, StaticExprKind};
use novarocks_expression_contract::RootOrdinal;
use novarocks_functions::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, ScalarInvocationActivation,
    ScalarInvocationScope, Selection,
};
use novarocks_local_program::{LocalProgram, ProgramRootRef, ProgramUnitId};

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
        panic!("LIKE never waits")
    }
}
fn compiled(sql: &str, dtype: DataType) -> (Arc<LocalProgram>, ProgramRootRef) {
    let source =
        sql_source_with_single_field_and_core_residuals(sql, Field::new("name", dtype, true));
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_inlist_source(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        for (unit_ordinal, unit) in program.units().iter().enumerate() {
            for (root_ordinal, root) in unit.roots().iter().enumerate() {
                let definition = unit.definitions().node(root.definition).unwrap();
                if matches!(definition.kind(), StaticExprKind::PreparedLike { .. }) {
                    assert!(found.is_none(), "one actual LIKE root");
                    found = Some((
                        program.clone(),
                        ProgramRootRef {
                            unit: ProgramUnitId::new(unit_ordinal),
                            root: RootOrdinal::new(u32::try_from(root_ordinal).unwrap()),
                        },
                    ));
                }
            }
        }
    }
    found.expect("actual SQL LIKE root survived original lowering")
}
fn batch(program: &LocalProgram, site: ProgramRootRef, input: ArrayRef) -> RecordBatch {
    let unit = program.unit(site.unit).unwrap();
    let root = unit.root(site.root).unwrap();
    let schema = unit.scope(root.scope).unwrap().schema().clone();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), input.data_type());
    RecordBatch::try_new(schema, vec![input]).unwrap()
}
#[test]
fn like_actual_source_original_ordered_definitions_exact_recipe_and_negation() {
    for (sql, negated) in [
        ("SELECT name LIKE name FROM t0", false),
        ("SELECT name NOT LIKE 'a%' FROM t0", true),
    ] {
        let (program, site) = compiled(sql, DataType::Utf8);
        let unit = program.unit(site.unit).unwrap();
        let flow = unit.control_flow();
        let occurrence = &flow.uses()[&unit.root(site.root).unwrap().use_id];
        assert_eq!(occurrence.control, ControlShape::Eager);
        assert_eq!(occurrence.arguments.len(), 2);
        assert_ne!(occurrence.arguments[0], occurrence.arguments[1]);
        let definition = unit.definitions().node(occurrence.definition).unwrap();
        let StaticExprKind::PreparedLike {
            text,
            pattern,
            negated: actual,
        } = definition.kind()
        else {
            panic!("actual original LIKE")
        };
        assert_eq!(*actual, negated);
        assert_eq!(flow.uses()[&occurrence.arguments[0]].definition, *text);
        assert_eq!(flow.uses()[&occurrence.arguments[1]].definition, *pattern);
        let recipe = unit
            .recipes()
            .native_like(unit.root(site.root).unwrap().use_id)
            .unwrap();
        assert_eq!(recipe.text_type().data_type().clone(), DataType::Utf8);
        assert_eq!(recipe.pattern_type().data_type().clone(), DataType::Utf8);
        assert!(recipe.result_type().nullable());
        assert_eq!(recipe.negated(), negated);
    }
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
        panic!("LIKE never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("like-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("like-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("like-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn like_actual_every_constructor_phase_callback_seven_causes_no_footer_or_replay() {
    let (program, site) = compiled("SELECT name LIKE 'a%' FROM t0", DataType::Utf8);
    let data = batch(
        &program,
        site,
        Arc::new(StringArray::from(vec![Some("apple"), None, Some("banana")])),
    );
    let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
    CompiledExpressionInstance::try_new(
        program.unit(site.unit).unwrap().clone(),
        site.root,
        &record,
    )
    .unwrap();
    let trace = record.trace.lock().unwrap().clone();
    for stop in 1..=trace.len() {
        for cause in causes() {
            let c = Callback::new(stop, cause.clone());
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.unit(site.unit).unwrap().clone(), site.root,&c),Err(actual) if actual==cause)
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
        }
    }
    for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(site.unit).unwrap().clone(),
            site.root,
            &Control,
        )
        .unwrap();
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
                let mut frame = CompiledExpressionInstance::try_new(
                    program.unit(site.unit).unwrap().clone(),
                    site.root,
                    &Control,
                )
                .unwrap();
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
                        &mut PassThroughCallHost,
                    ),
                    Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
