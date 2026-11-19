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
use novarocks_expression::{
    EvaluationInput,
    testkit::{
        PassThroughCallHost,
        filter_conjunction_fixture::{Control, FilterShape, batch, bits, fixture, group},
    },
};
use novarocks_functions::Selection;

#[test]
fn filter_conjunction_actual_rand_domains_and_cross_batch_state() {
    let f = fixture(FilterShape::CountAndRandom);
    let mut narrow = group(&f);
    let mut guarded = group(&f);
    let selected = batch(&f, &[0, 0, 0], &[]);
    let rejected = batch(&f, &[0, 1, 0], &[]);
    let a = bits(
        &narrow
            .evaluate(
                EvaluationInput::Batch {
                    batch: &selected,
                    source: None,
                },
                Selection::try_sparse(3, &[0, 2]).unwrap(),
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap(),
    );
    let b = bits(
        &guarded
            .evaluate(
                EvaluationInput::Batch {
                    batch: &rejected,
                    source: None,
                },
                Selection::all(3),
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap(),
    );
    assert_eq!(a, vec![b[0], b[2]]);
    assert_eq!(b[1], Some(false));
    assert_eq!(
        bits(
            &narrow
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &selected,
                        source: None
                    },
                    Selection::all(3),
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
        ),
        bits(
            &guarded
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &selected,
                        source: None
                    },
                    Selection::all(3),
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
        )
    );
}
