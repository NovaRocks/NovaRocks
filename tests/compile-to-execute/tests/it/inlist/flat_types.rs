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
use arrow::array::{ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array};
use arrow::record_batch::RecordBatch;
use novarocks_expression::testkit::{CountingAllocator, PassThroughCallHost};
use novarocks_expression::{CompiledExpressionInstance, EvaluationInput};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, signed_inlist_source};
use novarocks_test_compile_to_execute::programs::compiled_signed_inlist_root;
use std::{sync::Arc, time::Duration};

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("IN does not wait")
    }
}

fn evaluate(sql: &str, array: &ArrayRef, expected: &[Option<bool>]) {
    let source = signed_inlist_source(sql, array.data_type().clone());
    let (program, site) =
        compiled_signed_inlist_root(&source, &installed_builtin_owner_catalogue());
    let unit = program.unit(site.unit).unwrap();
    let schema = unit
        .scope(unit.root(site.root).unwrap().scope)
        .unwrap()
        .schema()
        .clone();
    let batch = RecordBatch::try_new(schema, vec![Arc::clone(array)]).unwrap();
    let allocator = Arc::new(CountingAllocator::new(usize::MAX));
    let mut frame = CompiledExpressionInstance::try_new_with_allocator(
        Arc::clone(unit),
        site.root,
        &Control,
        Some(allocator.clone()),
    )
    .unwrap();
    let selection = Selection::all(batch.num_rows());
    let output = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    let actual = output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(actual.iter().collect::<Vec<_>>(), expected, "{sql}");
}

#[test]
fn compiled_flat_in_and_not_in_propagate_null_membership() {
    let cases = [
        (
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            "TRUE, CAST(NULL AS BOOLEAN)",
        ),
        (
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
            "CAST(1.5 AS DOUBLE), CAST(NULL AS DOUBLE)",
        ),
        (
            Arc::new(Date32Array::from(vec![Some(0), Some(1), None])) as ArrayRef,
            "DATE '1970-01-01', CAST(NULL AS DATE)",
        ),
        (
            Arc::new(BinaryArray::from(vec![
                Some(&b"a"[..]),
                Some(&b"b"[..]),
                None,
            ])) as ArrayRef,
            "X'61', CAST(NULL AS VARBINARY)",
        ),
    ];
    for (values, candidates) in cases {
        evaluate(
            &format!("SELECT k IN ({candidates}) FROM t0"),
            &values,
            &[Some(true), None, None],
        );
        evaluate(
            &format!("SELECT k NOT IN ({candidates}) FROM t0"),
            &values,
            &[Some(false), None, None],
        );
    }
}

#[test]
fn compiled_flat_not_in_distinguishes_absent_from_null_source() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None]));
    evaluate(
        "SELECT k NOT IN (CAST(1.5 AS DOUBLE), CAST(3.5 AS DOUBLE)) FROM t0",
        &values,
        &[Some(false), Some(true), None],
    );
}
