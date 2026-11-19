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

use crate::{
    EvaluationInput,
    testkit::{
        PassThroughCallHost,
        guarded_fixtures::{
            GUARDED_CONTROL as Control,
            byte::{byte_batch, byte_program, carriers},
        },
    },
};
use arrow::array::Array;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope, Selection};

#[test]
fn four_byte_carriers_empty_selection_skips_required_round_and_keeps_exact_empty_output() {
    for ty in carriers() {
        let program = byte_program(ty.clone(), true);
        let input = byte_batch(
            &program,
            vec![None; 3],
            &[None; 3],
            &[Some(b"fallback".as_slice()); 3],
            vec![Some(10_i128.pow(38) - 1); 3],
        );
        let mut evaluator = program.instance(&Control);
        let rows = [];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(output.values().data_type(), &ty);
        assert_eq!(output.selection(), selection);
        assert!(output.values().is_empty());
        assert!(output.errors().is_empty());
        assert!(evaluator.instances.is_empty());
    }
}
