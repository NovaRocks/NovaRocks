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
use crate::{
    EvaluationInput, ExprEvaluationFailure,
    testkit::{
        PassThroughCallHost,
        filter_conjunction_fixture::{Control, FilterShape, batch, fixture, group},
    },
};
use novarocks_functions::Selection;

#[test]
fn filter_conjunction_actual_pending_error_commits_before_rand_boundary() {
    let f = fixture(FilterShape::DateAndRandom);
    let input = batch(&f, &[0], &[Some(f64::NAN)]);
    let mut instance = group(&f);
    let error = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            Selection::all(1),
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    let ExprEvaluationFailure::RequiredRow(error) = error else {
        panic!("actual required row error");
    };
    assert_eq!(error.root(), f.roots[0]);
    assert_eq!(error.batch_row(), 0);
    assert_eq!(
        instance.constructed_root_call_instances(1),
        0,
        "a terminal row never creates the later actual RAND instance"
    );
}
