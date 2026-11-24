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

use std::collections::HashSet;

use arrow_array::FixedSizeBinaryArray;
use novarocks_functions::Selection;

use super::scalar_rows::{Control, prepare};

#[test]
fn uuid_produces_fresh_v4_bytes_across_rows_and_batches() {
    let mut instance = prepare("uuid", &[]);
    let mut seen = HashSet::new();
    for _ in 0..2 {
        let output = instance.evaluate(Selection::all(8), &[], &Control).unwrap();
        let uuids = output
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for uuid in uuids.iter().map(Option::unwrap) {
            let bytes: [u8; 16] = uuid.try_into().unwrap();
            assert_eq!(bytes[6] >> 4, 4);
            assert_eq!(bytes[8] >> 6, 2);
            assert!(seen.insert(bytes));
        }
    }
}
