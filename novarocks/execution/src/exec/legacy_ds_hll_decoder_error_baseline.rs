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

use super::HllAccumulator;
#[test]
fn legacy_ds_hll_semantic_decoder_full_error_before_owner() {
    let coupon = (1u32 << 26) | 1;
    let mut payload = vec![3, 1, 7, 8, 5, 8, 0, 9];
    payload.extend_from_slice(&2u32.to_le_bytes());
    payload.extend_from_slice(&coupon.to_le_bytes());
    payload.extend_from_slice(&coupon.to_le_bytes());
    let error = HllAccumulator::from_payload_unreserved(&payload)
        .err()
        .expect("original duplicate SET failure");
    assert_eq!(
        (error).to_string(),
        "ds_hll: failed to deserialize HLL payload: InvalidData => SET mode contains duplicate coupons"
    );
}
