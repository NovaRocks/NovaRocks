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
//! Actual frozen hidden conversion: exact nominal source, typed output and old oracle.
use super::*;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
#[test]
fn nominal_largeint_text_actual_compiler_matches_v1_sparse_min_max_null_slice_empty() {
    let target = FunctionValueType::new(DataType::Utf8, true);
    let program = direct_program(largeint_type(), target.clone());
    assert_eq!(result_type(&program), target);
    let raw = wide(&[Some(0), Some(i128::MIN), Some(42), None, Some(i128::MAX)]).slice(1, 4);
    let batch = input_batch(&program, vec![raw.clone()]);
    let rows = [0, 1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let mut evaluator = instance(&program);
    for _ in 0..2 {
        let output = evaluator
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
        assert_eq!(
            output.values().to_data(),
            // Exact signed base-10 spelling of the declared 128-bit extremes.
            StringArray::from(vec![
                Some("-170141183460469231731687303715884105728"),
                Some("42"),
                None,
                Some("170141183460469231731687303715884105727")
            ])
            .to_data()
        );
    }
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            {
                let selection = Selection::try_sparse(4, &[]).unwrap();
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
