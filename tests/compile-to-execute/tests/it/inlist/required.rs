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

//! Actual original SQL source receipts and permanent IN/NOT IN Frame witnesses.
//! Expected truth values are fixed independently of any value evaluator.
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::{CountingAllocator, PassThroughCallHost};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_local_program::ProgramNodeKind;
use novarocks_sql_testkit::{
    inspect_inlist_source, sql_source_with_single_field_and_core_residuals,
};
use novarocks_test_compile_to_execute::programs::programs_with_catalogue_and_original_inlist_source;
use std::{sync::Arc, time::Duration};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN never waits")
    }
}
fn evaluate(sql: &str, field: Field, input: ArrayRef, expected: &[Option<bool>], truth: bool) {
    // Same original SQL's legitimate optimizer variant, not a claim that
    // the native default producer emitted this exact residual site.
    let source = sql_source_with_single_field_and_core_residuals(sql, field);
    assert!(inspect_inlist_source(&source) > 0);
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_inlist_source(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            let site = match node.kind() {
                ProgramNodeKind::Project { exprs, .. }
                    if !truth
                        && exprs.len() == 1
                        && node.output_layout().schema().field(0).data_type()
                            == &DataType::Boolean =>
                {
                    exprs[0]
                }
                ProgramNodeKind::Filter { predicates, .. } if truth => predicates[0],
                ProgramNodeKind::Scan { residuals, .. } if truth && residuals.len() == 1 => {
                    residuals[0]
                }
                _ => continue,
            };

            if program
                .unit(site.unit)
                .and_then(|unit| unit.root(site.root))
                .is_some()
            {
                assert!(found.is_none(), "fixture has one actual membership root");
                found = Some((program.clone(), site));
            }
        }
    }
    let (program, site) = found.expect("actual original membership root");
    let unit = program.unit(site.unit).unwrap();
    let schema = unit
        .scope(unit.root(site.root).unwrap().scope)
        .unwrap()
        .schema()
        .clone();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), input.data_type());
    let data = RecordBatch::try_new(schema, vec![input.clone()]).unwrap();
    assert_eq!(expected.len(), input.len());
    for rows in [
        (0..input.len()).collect::<Vec<_>>(),
        (0..input.len()).filter(|r| r % 2 == 0).collect(),
        Vec::new(),
    ] {
        let selection = Selection::try_sparse(input.len(), &rows).unwrap();
        let mut frame = CompiledExpressionInstance::try_new_with_allocator(
            program.unit(site.unit).unwrap().clone(),
            site.root,
            &Control,
            Some(Arc::new(CountingAllocator::new(usize::MAX))),
        )
        .unwrap();
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
        assert_eq!(out.selection(), selection);
        let wanted = BooleanArray::from(rows.iter().map(|row| expected[*row]).collect::<Vec<_>>());
        assert_eq!(out.values().to_data(), wanted.to_data());
    }
}
fn ints() -> ArrayRef {
    Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        None,
        Some(9),
    ]))
}
fn texts() -> ArrayRef {
    Arc::new(StringArray::from(vec![
        Some("ASIA"),
        Some("AMERICA"),
        Some("AFRICA"),
        None,
        Some("é中"),
    ]))
}
#[test]
fn inlist_actual_compiler_i32_value_nullable_literal_list() {
    // A match dominates the later NULL; nonmatches and NULL source are NULL.
    evaluate(
        "SELECT ship_code IN (1,2,NULL) FROM t0",
        Field::new("ship_code", DataType::Int32, true),
        ints(),
        &[Some(true), Some(true), None, None, None],
        false,
    );
}
#[test]
fn inlist_actual_compiler_utf8_value_nullable_original_region_source() {
    evaluate(
        "SELECT c_region IN ('ASIA','AMERICA') FROM t_filter_customer_metrics",
        Field::new("c_region", DataType::Utf8, true),
        texts(),
        &[Some(true), Some(true), Some(false), None, Some(false)],
        false,
    );
}
#[test]
fn inlist_actual_compiler_i32_notin_truthonly_original_scan_source() {
    // The scalar NOT keeps UNKNOWN for the filter consumer.
    evaluate(
        "SELECT ship_code FROM t0 WHERE ship_code NOT IN (1,2)",
        Field::new("ship_code", DataType::Int32, true),
        ints(),
        &[Some(false), Some(false), Some(true), None, Some(true)],
        true,
    );
}
#[test]
fn inlist_actual_compiler_utf8_truthonly_original_required_predicate() {
    evaluate(
        "SELECT c_region FROM t_filter_customer_metrics WHERE c_region IN ('ASIA','AMERICA')",
        Field::new("c_region", DataType::Utf8, true),
        texts(),
        &[
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
        ],
        true,
    );
}
#[test]
fn inlist_actual_compiler_utf8_case_truthonly_original_required_source() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(100),
        Some(85),
        Some(70),
        None,
        Some(59),
    ]));
    // The ordered thresholds produce A, B, C, E, E. Only A/B are members;
    // SQL NULL does not satisfy a WHEN comparison and therefore selects ELSE.
    evaluate(
        "SELECT ship_code FROM t0 WHERE (CASE WHEN ship_code >= 90 THEN 'A' WHEN ship_code >= 80 THEN 'B' WHEN ship_code >= 70 THEN 'C' WHEN ship_code >= 60 THEN 'D' ELSE 'E' END) IN ('A','B')",
        Field::new("ship_code", DataType::Int32, true),
        input,
        &[
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
        ],
        true,
    );
}
