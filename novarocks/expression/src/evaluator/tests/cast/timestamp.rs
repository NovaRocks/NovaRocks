// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::EvaluationInput;
use crate::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use arrow::array::TimestampNanosecondArray;
use arrow::datatypes::TimeUnit;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

#[test]
fn timestamp_narrowing_uses_no_function_instances() {
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        let program = compiled_cast(
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        let batch = RecordBatch::try_new(
            program.input_schema().clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(499),
                    Some(500),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![42; 3])),
                Arc::new(BooleanArray::from(vec![true; 3])),
            ],
        )
        .unwrap();
        let mut evaluator = instance(&program);
        let selection = Selection::all(3);
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
        assert!(evaluator.instances.is_empty());
    }
}
