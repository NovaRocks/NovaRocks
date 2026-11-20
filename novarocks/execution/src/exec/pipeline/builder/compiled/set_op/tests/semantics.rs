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

use super::{SetOperationKind, execute, oracle};

#[test]
fn compiled_distinct_sets_match_the_reference_across_null_duplicates_and_empty_inputs() {
    let inputs = [
        vec![
            vec![None, Some(1), Some(1), Some(2), Some(3)],
            vec![None, Some(1), Some(1), Some(4)],
            vec![None, None, Some(1), Some(3)],
        ],
        vec![vec![None, None], vec![None], vec![None, None]],
        vec![vec![], vec![None, Some(1)], vec![Some(1)]],
        vec![vec![None, Some(1)], vec![], vec![Some(1)]],
    ];
    for branches in inputs {
        for kind in [SetOperationKind::Intersect, SetOperationKind::Except] {
            assert_eq!(execute(kind, &branches, 1), oracle(kind, &branches));
        }
    }
}
