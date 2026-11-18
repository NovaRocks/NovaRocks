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
//! Actual compiler and selected evaluator for the exact Physical FSB16 profile.
use super::*;
use arrow::array::StringArray;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
#[test]
fn largeint_text_actual_compiler_sparse_slice_null_empty_and_effects() {
    let input =
        novarocks_types::largeint::array_from_i128(&[Some(0), Some(i128::MIN), Some(42), None])
            .unwrap()
            .slice(1, 3);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let program = compiled(
                FunctionValueType::new(DataType::FixedSizeBinary(16), true),
                FunctionValueType::new(DataType::Utf8, true),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            let batch = RecordBatch::try_new(
                program.graph().nodes()[1].output_layout().schema().clone(),
                vec![
                    input.clone(),
                    Arc::new(Int64Array::from(vec![42; 3])),
                    Arc::new(BooleanArray::from(vec![true; 3])),
                ],
            )
            .unwrap();
            let mut evaluator = instance(&program);
            let occurrence = cast_root_use(&program);
            let summary = evaluator.effects[&occurrence];
            assert!(
                !summary
                    .for_use(summary.context())
                    .unwrap()
                    .may_raise_row_error
            );
            let rows = [0, 1, 2];
            let result = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(3, &rows).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(
                result.values().to_data(),
                // Signed big-endian physical bytes represent MIN, 42, NULL.
                // Decimal spelling is independent of either cast evaluator.
                StringArray::from(vec![
                    Some("-170141183460469231731687303715884105728"),
                    Some("42"),
                    None
                ])
                .to_data()
            );
            assert!(result.errors().is_empty());
            let output = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(3, &[]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(
                output.values().to_data(),
                StringArray::from(Vec::<Option<&str>>::new()).to_data()
            );
        }
    }
}
