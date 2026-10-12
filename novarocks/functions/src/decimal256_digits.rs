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

//! Shared signed 256-bit integer digits, with no heap scratch or scale policy.

use std::convert::Infallible;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixedDigitsEvent {
    DecodeLimb(usize),
    ComplementLimb(usize),
    DivideLimb(usize),
    EmitDigit(usize),
    TestZeroLimb(usize),
}

/// Unsigned ASCII magnitude and its original sign. All storage is inline.
pub struct Decimal256Digits {
    negative: bool,
    digits: [u8; 78],
    start: usize,
}
impl Decimal256Digits {
    pub fn negative(&self) -> bool {
        self.negative
    }
    pub fn digits(&self) -> &[u8] {
        &self.digits[self.start..]
    }
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.digits()).expect("integer digits are ASCII")
    }
}

/// Observe immediately before each original arithmetic action. A refusal is
/// returned unchanged; no later limb, digit or zero-test is visited.
pub fn fixed_digits<E>(
    raw: [u8; 32],
    mut observe: impl FnMut(FixedDigitsEvent) -> Result<(), E>,
) -> Result<Decimal256Digits, E> {
    let mut limbs = [0u64; 4];
    for (i, limb) in limbs.iter_mut().enumerate() {
        observe(FixedDigitsEvent::DecodeLimb(i))?;
        *limb = u64::from_le_bytes([
            raw[i * 8],
            raw[i * 8 + 1],
            raw[i * 8 + 2],
            raw[i * 8 + 3],
            raw[i * 8 + 4],
            raw[i * 8 + 5],
            raw[i * 8 + 6],
            raw[i * 8 + 7],
        ]);
    }
    let negative = raw[31] & 128 != 0;
    if negative {
        let mut carry = true;
        for (i, limb) in limbs.iter_mut().enumerate() {
            observe(FixedDigitsEvent::ComplementLimb(i))?;
            let (n, c) = (!*limb).overflowing_add(u64::from(carry));
            *limb = n;
            carry = c;
        }
    }
    let mut digits = [0u8; 78];
    let mut at = digits.len();
    loop {
        let mut rem = 0u128;
        for (i, limb) in limbs.iter_mut().enumerate().rev() {
            observe(FixedDigitsEvent::DivideLimb(i))?;
            let n = (rem << 64) | u128::from(*limb);
            *limb = (n / 10) as u64;
            rem = n % 10;
        }
        observe(FixedDigitsEvent::EmitDigit(at - 1))?;
        at -= 1;
        digits[at] = b'0' + rem as u8;
        let mut zero = true;
        for (i, limb) in limbs.iter().enumerate() {
            observe(FixedDigitsEvent::TestZeroLimb(i))?;
            if *limb != 0 {
                zero = false;
                break;
            }
        }
        if zero {
            break;
        }
    }
    Ok(Decimal256Digits {
        negative,
        digits,
        start: at,
    })
}

/// Original callers do not introduce an evaluation control or allocation host.
pub fn original_digits(raw: [u8; 32]) -> Decimal256Digits {
    match fixed_digits(raw, |_| Ok::<(), Infallible>(())) {
        Ok(digits) => digits,
        Err(cause) => match cause {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::i256;

    #[test]
    fn decimal256_digits_match_original_extremes_and_limb_patterns() {
        let mut values = vec![i256::MIN, i256::MAX, i256::ZERO, i256::ONE, i256::MINUS_ONE];
        let mut seed = 0x6a09_e667_f3bc_c909u64;
        for _ in 0..128 {
            let mut raw = [0u8; 32];
            for bytes in raw.chunks_exact_mut(8) {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                bytes.copy_from_slice(&seed.to_le_bytes());
            }
            values.push(i256::from_le_bytes(raw));
        }
        for value in values {
            let digits = original_digits(value.to_le_bytes());
            let mut text = String::new();
            if digits.negative() {
                text.push('-');
            }
            text.push_str(digits.as_str());
            assert_eq!(text, value.to_string());
            assert!((1..=77).contains(&digits.digits().len()));
        }
    }

    #[test]
    fn decimal256_digits_every_event_refusal_stops_at_original_prefix() {
        for value in [i256::ZERO, i256::ONE, i256::MINUS_ONE, i256::MIN, i256::MAX] {
            let mut original = Vec::new();
            fixed_digits(value.to_le_bytes(), |event| {
                original.push(event);
                Ok::<_, Infallible>(())
            })
            .unwrap();
            assert!(original.len() <= 701);
            if value == i256::ZERO {
                assert_eq!(original.len(), 13);
            }
            for stop in 0..original.len() {
                let mut seen = Vec::new();
                let result = fixed_digits(value.to_le_bytes(), |event| {
                    seen.push(event);
                    if seen.len() == stop + 1 {
                        Err(stop)
                    } else {
                        Ok(())
                    }
                });
                assert!(matches!(result, Err(cause) if cause == stop));
                assert_eq!(seen, original[..=stop]);
            }
        }
    }
}
