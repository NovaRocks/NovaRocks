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

//! Actual LocalCompiler/CompiledFixture decimal text selection and empty output.
use super::*;
use crate::EvaluationInput;
use crate::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use arrow::array::{Decimal128Array, StringArray};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

#[test]
fn decimal_to_varchar_cast_preserves_declared_scale_and_selected_nulls() {
    for (scale, five, minus_one) in [
        (0, "5", "-1"),
        (2, "0.05", "-0.01"),
        (
            38,
            "0.00000000000000000000000000000000000005",
            "-0.00000000000000000000000000000000000001",
        ),
    ] {
        let input: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(19), Some(5), None, Some(-1)])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        let input = input.slice(1, 3);
        let expected = [Some(five), None, Some(minus_one)];
        for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
            let program = compiled_cast(
                FunctionValueType::new(input.data_type().clone(), true),
                FunctionValueType::new(DataType::Utf8, true),
                Source::Column,
                Wrap::Bare,
                mode,
                false,
            );
            let batch = RecordBatch::try_new(
                program.input_schema().clone(),
                vec![
                    input.clone(),
                    Arc::new(Int64Array::from(vec![42; 3])),
                    Arc::new(BooleanArray::from(vec![true; 3])),
                ],
            )
            .unwrap();
            let mut evaluator = instance(&program);
            let summary = evaluator.effects[&cast_root_use(&program)];
            assert!(
                !summary
                    .for_use(summary.context())
                    .unwrap()
                    .may_raise_row_error
            );
            for rows in [&[0, 1, 2][..], &[0, 2], &[]] {
                let selection = Selection::try_sparse(3, rows).unwrap();
                let result = evaluator
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
                let expected = StringArray::from_iter(rows.iter().map(|&row| expected[row]));
                assert_eq!(result.selection(), selection);
                assert_eq!(result.values().to_data(), expected.to_data());
                assert!(result.errors().is_empty());
            }
        }
    }
}
