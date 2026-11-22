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

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{CompiledExpressionInstance, EvaluationInput, StaticExprKind};
use novarocks_expression_contract::RootOrdinal;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source_with_single_field};
use novarocks_test_compile_to_execute::programs::programs_with_catalogue_and_nullable_source;
use novarocks_type_contract::ControlShape;
use std::sync::Arc;
use std::time::Duration;

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("simple CASE comparisons do not wait")
    }
}

fn evaluate_case(
    sql: &str,
    input: ArrayRef,
    rows: &[usize],
    ieee_float_equality: bool,
    arms: u32,
) -> Vec<Option<i64>> {
    let source =
        sql_source_with_single_field(sql, Field::new("k", input.data_type().clone(), true));
    let programs =
        programs_with_catalogue_and_nullable_source(&source, &installed_builtin_owner_catalogue());
    let mut site = None;
    for unit in programs.values().flat_map(|program| program.units()) {
        for (ordinal, root) in unit.roots().iter().enumerate() {
            let definition = unit.definitions().node(root.definition).unwrap();
            if let StaticExprKind::Case {
                has_case_expr: true,
                ieee_float_equality: actual,
                ..
            } = definition.kind()
            {
                assert_eq!(*actual, ieee_float_equality);
                assert!(site.is_none(), "one actual simple CASE root");
                let invocation = &unit.control_flow().uses()[&root.use_id];
                assert_eq!(
                    invocation.control,
                    ControlShape::Case {
                        simple: true,
                        arms,
                        has_else: true,
                    }
                );
                assert_eq!(invocation.arguments.len(), 2 + arms as usize * 2);
                site = Some((
                    unit.clone(),
                    RootOrdinal::new(u32::try_from(ordinal).unwrap()),
                ));
            }
        }
    }
    let (unit, root) = site.expect("actual compiled simple CASE");
    let scope = unit.root(root).unwrap().scope;
    let schema = unit.scope(scope).unwrap().schema().clone();
    let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
    let selection = Selection::try_sparse(batch.num_rows(), rows).unwrap();
    let mut instance = CompiledExpressionInstance::try_new(unit, root, &Control).unwrap();
    let output = instance
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
    assert_eq!(output.selection(), selection);
    assert!(output.errors().is_empty(), "{:?}", output.errors());
    output
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn simple_case_uses_ieee_equality_on_sparse_rows_with_first_match_and_unknown() {
    for (width, input) in [
        (
            "real",
            Arc::new(Float32Array::from(vec![
                Some(99.0),
                Some(f32::NAN),
                Some(-0.0),
                None,
                Some(1.0),
                Some(0.0),
            ])) as ArrayRef,
        ),
        (
            "double",
            Arc::new(Float64Array::from(vec![
                Some(99.0),
                Some(f64::NAN),
                Some(-0.0),
                None,
                Some(1.0),
                Some(0.0),
            ])) as ArrayRef,
        ),
    ] {
        let sql = format!(
            "SELECT CASE k WHEN CAST(0e0 AS {width}) / CAST(0e0 AS {width}) THEN CAST(10 AS BIGINT) \
               WHEN CAST(0e0 AS {width}) THEN CAST(20 AS BIGINT) \
               WHEN CAST(-0e0 AS {width}) THEN CAST(30 AS BIGINT) \
               WHEN CAST(1e0 AS {width}) THEN CAST(40 AS BIGINT) ELSE CAST(50 AS BIGINT) END FROM fixture"
        );
        assert_eq!(
            evaluate_case(&sql, input, &[1, 2, 3, 4, 5], true, 4),
            [Some(50), Some(20), Some(50), Some(40), Some(20)],
        );
    }
}

#[test]
fn simple_case_leaves_later_labels_unselected_and_uses_exact_decimal_coercion() {
    let input = Arc::new(Float64Array::from(vec![None, Some(1801439850948189.0)])) as ArrayRef;
    assert_eq!(
        evaluate_case(
            "SELECT CASE k WHEN DECIMAL '1801439850948188.9' THEN CAST(1 AS BIGINT) \
               WHEN CAST(CAST(1 AS BIGINT) / CAST(0 AS BIGINT) AS DOUBLE) THEN CAST(2 AS BIGINT) \
               ELSE CAST(3 AS BIGINT) END FROM fixture",
            input,
            &[1],
            true,
            2,
        ),
        [Some(1)],
    );
}

#[test]
fn simple_case_preserves_legacy_wide_equality_after_float_alignment() {
    let input = Arc::new(Float64Array::from(vec![Some(-0.0), Some(0.0)])) as ArrayRef;
    assert_eq!(
        evaluate_case(
            "SELECT CASE k WHEN CAST(1 AS DECIMAL(39,0)) THEN CAST(9 AS BIGINT) \
               WHEN 0e0 THEN CAST(1 AS BIGINT) ELSE CAST(2 AS BIGINT) END FROM fixture",
            input,
            &[0, 1],
            false,
            2,
        ),
        [Some(2), Some(1)],
    );
}
