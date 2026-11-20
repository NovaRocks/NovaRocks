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
fn compiled_tail_handoff_transfers_all_ranges_without_duplicate_keys() {
    let branches = [
        (0..1450).map(Some).collect(),
        (100..1400).map(Some).collect(),
        (200..1350).map(Some).collect(),
    ];
    let kind = SetOperationKind::Intersect;
    assert_eq!(execute(kind, &branches, 3), oracle(kind, &branches));
}
