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
use std::cmp::Ordering;

use arrow_buffer::i256;

/// Compute 10^exp in i128 with overflow checking.
pub fn pow10_i128(exp: usize) -> Result<i128, String> {
    let mut out: i128 = 1;
    for _ in 0..exp {
        out = out
            .checked_mul(10)
            .ok_or_else(|| "decimal overflow".to_string())?;
    }
    Ok(out)
}

/// Compute 10^exp in i256 with overflow checking.
pub fn pow10_i256(exp: usize) -> Result<i256, String> {
    let mut out = i256::ONE;
    let ten = i256::from_i128(10);
    for _ in 0..exp {
        out = out
            .checked_mul(ten)
            .ok_or_else(|| "decimal overflow".to_string())?;
    }
    Ok(out)
}

/// Integer division with ROUND_HALF_UP, keeping consistent with StarRocks BE:
/// be/src/runtime/decimalv3.h DecimalV3Arithmetics<..., false>::div_round.
pub fn div_round_i128(dividend: i128, divisor: i128) -> i128 {
    debug_assert!(divisor != 0);

    let mut q = dividend / divisor;
    let r = dividend % divisor;

    if r == 0 {
        return q;
    }

    // case 1: |b| is odd. if [|b|/2] < |r|, then add carry; otherwise add 0.
    // case 2: |b| is even. if [|b|/2] <= |r|, then add carry; otherwise add 0.
    // [b/2] == r means round half to up.
    // carry depends on sign of a^b.
    let abs_b = divisor.abs();
    let abs_r = r.abs();
    let threshold = (abs_b >> 1) + (abs_b & 1);

    if abs_r.cmp(&threshold) != Ordering::Less {
        // carry: +1 if dividend and divisor have same sign, -1 otherwise.
        let carry = if (dividend ^ divisor) < 0 { -1 } else { 1 };
        q += carry;
    }

    q
}

/// Integer division with ROUND_HALF_UP for i256.
pub fn div_round_i256(dividend: i256, divisor: i256) -> Result<i256, String> {
    let mut q = dividend
        .checked_div(divisor)
        .ok_or_else(|| "decimal overflow".to_string())?;
    let r = dividend
        .checked_rem(divisor)
        .ok_or_else(|| "decimal overflow".to_string())?;

    if r == i256::ZERO {
        return Ok(q);
    }

    let abs_b = if divisor.is_negative() {
        divisor
            .checked_neg()
            .ok_or_else(|| "decimal overflow".to_string())?
    } else {
        divisor
    };
    let abs_r = if r.is_negative() {
        r.checked_neg()
            .ok_or_else(|| "decimal overflow".to_string())?
    } else {
        r
    };
    let threshold = (abs_b >> 1)
        .checked_add(abs_b & i256::ONE)
        .ok_or_else(|| "decimal overflow".to_string())?;

    if abs_r.cmp(&threshold) != Ordering::Less {
        let carry = if dividend.is_negative() ^ divisor.is_negative() {
            i256::MINUS_ONE
        } else {
            i256::ONE
        };
        q = q
            .checked_add(carry)
            .ok_or_else(|| "decimal overflow".to_string())?;
    }

    Ok(q)
}
