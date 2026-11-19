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

use super::*;

#[test]
fn only_complete_records_handoff_the_borrowed_fragment_before_end() {
    let first = vec![1, 2, 3, 4];
    let second = vec![5, 6, 7];
    let first_record = fragment_record(first.clone());
    let cut = first_record.len() - 1;
    let mut decoder = new_decoder(1);
    let mut arrived = vec![];
    decoder
        .apply_relay_body_with(&first_record[..cut], |_, target, bytes| {
            arrived.push((target, bytes.to_vec()));
            Ok(())
        })
        .unwrap();
    assert!(arrived.is_empty());
    let tail = [&first_record[cut..], &fragment_record(second.clone())].concat();
    decoder
        .apply_relay_body_with(&tail, |_, target, bytes| {
            arrived.push((target, bytes.to_vec()));
            Ok(())
        })
        .unwrap();
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    assert_eq!(arrived, vec![(target, first), (target, second)]);
    assert!(decoder.fragments.is_empty());
    decoder
        .apply_relay_body_with(&row_count_record(4), |_, _, _| {
            panic!("row count cannot arrive as a fragment")
        })
        .unwrap();
    decoder.check_relay_end(3).unwrap();
    let prepared = finish(decoder).unwrap();
    assert_eq!(prepared.row_count(), 4);
    assert_eq!(prepared.fragment_count, 2);
    assert!(prepared.fragments().is_empty());
}
