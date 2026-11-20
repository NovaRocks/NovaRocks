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

use arrow_array::StringArray;
use novarocks_functions::Selection;

use super::scalar_rows::{Control, prepare};

#[test]
fn uuid_produces_fresh_v4_text_across_rows_and_batches() {
    let mut instance = prepare("uuid", &[]);
    let mut seen = HashSet::new();
    for _ in 0..2 {
        let output = instance.evaluate(Selection::all(8), &[], &Control).unwrap();
        let uuids = output
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for uuid in uuids.iter().map(Option::unwrap) {
            let bytes = uuid.as_bytes();
            assert_eq!(bytes.len(), 36);
            for (index, byte) in bytes.iter().copied().enumerate() {
                if matches!(index, 8 | 13 | 18 | 23) {
                    assert_eq!(byte, b'-');
                } else {
                    assert!(byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
                }
            }
            assert_eq!(bytes[14], b'4');
            assert!(matches!(bytes[19], b'8' | b'9' | b'a' | b'b'));
            assert!(seen.insert(uuid.to_owned()));
        }
    }
}
