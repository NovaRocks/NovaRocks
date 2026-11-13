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

//! Observed counterpart of Arrow 58.2's safe text-to-Int64 parser.

use crate::{KernelFailure, kernel_input::EvaluationCheckpoints};

/// Accept exactly an optional ASCII sign followed by one or more ASCII
/// decimal digits, with no trimming and no trailing input. Parse failures are
/// Arrow's safe NULL result, not row errors. The caller owns entry and finish
/// checkpoints and handles SQL NULL before invoking this borrowed parser.
///
/// Negative accumulation admits i64::MIN without an unsigned intermediate.
/// Overflow remains latched while subsequent digits are observed, matching
/// atoi's checked parser consumption. Invalid bytes end parsing immediately.
/// Unlike Arrow's last-byte shortcut, this walks the encountered prefix in
/// order; it never delegates an unobserved long string to an opaque parser.
pub(crate) fn parse_i64(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<i64>, KernelFailure> {
    let mut negative = false;
    let mut saw_digit = false;
    let mut value = Some(0_i64);
    for (index, byte) in text.bytes().enumerate() {
        work.step()?;
        if index == 0 && matches!(byte, b'+' | b'-') {
            negative = byte == b'-';
            continue;
        }
        if !byte.is_ascii_digit() {
            return Ok(None);
        }
        saw_digit = true;
        let digit = i64::from(byte - b'0');
        value = value.and_then(|number| {
            let scaled = number.checked_mul(10)?;
            if negative {
                scaled.checked_sub(digit)
            } else {
                scaled.checked_add(digit)
            }
        });
    }
    Ok(if saw_digit { value } else { None })
}

#[cfg(test)]
mod tests;
