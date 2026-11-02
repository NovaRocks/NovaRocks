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

//! Observed STANDARD decimal grammar with a bounded Arrow float parser input.
//!
//! This normalization is tied to arrow-cast 58.2.0 and lexical-parse-float
//! 1.0.6. It is not a source-length admission limit or a new float algorithm:
//!
//! * Arrow's `parse.rs` delegates Float64Type::parse to lexical_core::parse.
//!   `parse_number` accepts optional sign, decimal digits with at most one
//!   point, and optional e/E followed by optional sign and required digits.
//!   The special tokens are case-insensitive NaN, inf, infinity. Nothing else
//!   is accepted by its STANDARD/default grammar.
//! * `parse_number` saturates the explicit exponent by updating only while
//!   it is below 0x10000000; the FIRST crossing value is retained. We perform
//!   exactly that update before combining it with mantissa position, using
//!   i128 position arithmetic so long leading zeros do not overflow.
//! * `limits::f64_max_digits(10)` is 769. `slow::parse_mantissa` retains these
//!   significant digits and, iff any discarded digit is nonzero, appends ONE
//!   extra digit 1 (`round_up_truncated!`). Adding one to the retained integer
//!   instead would be wrong at a halfway point. This leaf uses the same
//!   prefix/sticky pair and the same scientific exponent. The first 19 digits
//!   and their exponent also agree with lexical's moderate path; whenever it
//!   falls back, its big mantissa and digit count agree exactly. With fewer
//!   digits, normalization only removes leading zeros and relocates the point.
//! * Scientific exponent >=309 always overflows f64; <=-325 is below half
//!   the smallest subnormal. Replacing those exponents by 309/-325 preserves
//!   the original result including sign. All-zero mantissas preserve signed
//!   zero regardless of the explicit exponent. Rounding still belongs to the
//!   public Arrow parser, including subnormal and halfway cases.
//!
//! The opaque backend receives at most 777 bytes: 770 significant digits,
//! one point, one mantissa sign, e, one exponent sign, and three exponent
//! digits. Its slow path is a FIXED bounded primitive, not an observed limb
//! loop. In lexical 1.0.6 `slow::digit_comp`, scientific exponent is within
//! [-324,308] and the <=770 digit mantissa gives radix exponent [-1093,308].
//! Decimal `Bigint::pow` factors radix 10 into 5 and a binary shift. The fixed
//! StackVec is <=6000 bits (<=187 u32 limbs, <=93 u64 limbs), even when the
//! optional radix feature is unified. Noncompact pow5 uses at most eight
//! large multiplications by the fixed 5^135 table (10 u32 / 5 u64 limbs),
//! then at most eleven small multiplications; compact mode uses at most 85
//! small multiplications. Mantissa construction reads <=770 digits and uses
//! at most 87 multiply/add batches (including sticky) on u32 limbs; shifts,
//! normalization, comparisons and rounding traverse only the fixed limb array. There is no
//! source-sized allocation, rescan, or iterative convergence in that backend.
//! These are loop bounds, not a nanosecond SLA or per-256-limb interruption
//! claim. A dependency/parser change must re-audit them. The actual scanner
//! and token emission observe every byte with the caller's original work.
//! Host memory authorization and caller entry/final publication checkpoints
//! remain with the caller; this helper creates no control or budget.

use arrow_array::types::Float64Type;
use arrow_cast::parse::Parser;

use crate::{KernelFailure, kernel_control::internal, kernel_input::EvaluationCheckpoints};

const SIGNIFICANT_DIGITS: usize = 769;
const TOKEN_BYTES: usize = SIGNIFICANT_DIGITS + 1 + 1 + 1 + 1 + 1 + 3;
const EXPONENT_UPDATE_THRESHOLD: i64 = 0x10000000;

fn update_exponent(exponent: &mut i64, digit: u8) {
    if *exponent < EXPONENT_UPDATE_THRESHOLD {
        *exponent = *exponent * 10 + i64::from(digit - b'0');
    }
}

#[derive(Clone, Copy)]
enum State {
    Mantissa,
    ExponentStart,
    ExponentDigits,
}

/// Caller owns entry and final checkpoints and does not publish the result
/// before its final checkpoint. A malformed token is an ordinary safe-parse
/// NULL (`None`), while control refusals retain their original outer category.
pub(super) fn parse_f64(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<f64>, KernelFailure> {
    let bytes = text.as_bytes();
    let sign_len = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    if matches!(bytes.get(sign_len), Some(b'i' | b'I' | b'n' | b'N')) {
        // A complete special token is at most sign + "infinity". This is a
        // grammar refusal, not rejection of a long numeric source.
        if bytes.len() > 9 {
            work.step()?;
            return Ok(None);
        }
        for _ in bytes {
            work.step()?;
        }
        work.step()?;
        // Arrow 58's complete raw special-token parser rejects an unconsumed
        // suffix. Keep that domain before the newer SDK's ASCII trim retry.
        let value = if bytes.last().is_some_and(u8::is_ascii_whitespace) {
            None
        } else {
            Float64Type::parse(text)
        };
        work.step()?;
        return Ok(value);
    }

    let negative = bytes.first() == Some(&b'-');
    let mut state = State::Mantissa;
    let mut point = false;
    let mut mantissa_digits = 0_usize;
    let mut integer_digits = 0_usize;
    let mut first_nonzero = None;
    let mut significant = [0_u8; SIGNIFICANT_DIGITS];
    let mut retained = 0_usize;
    let mut sticky = false;
    let mut exponent = 0_i64;
    let mut negative_exponent = false;
    let mut exponent_digits = false;

    for (index, &byte) in bytes.iter().enumerate() {
        work.step()?;
        if index == 0 && sign_len != 0 {
            continue;
        }
        match state {
            State::Mantissa => match byte {
                b'0'..=b'9' => {
                    if !point {
                        integer_digits += 1;
                    }
                    if first_nonzero.is_none() && byte != b'0' {
                        first_nonzero = Some(mantissa_digits);
                    }
                    mantissa_digits += 1;
                    if first_nonzero.is_some() {
                        if retained < SIGNIFICANT_DIGITS {
                            significant[retained] = byte;
                            retained += 1;
                        } else if byte != b'0' {
                            sticky = true;
                        }
                    }
                }
                b'.' if !point => point = true,
                b'e' | b'E' if mantissa_digits != 0 => state = State::ExponentStart,
                _ => return Ok(None),
            },
            State::ExponentStart => match byte {
                b'+' | b'-' => {
                    negative_exponent = byte == b'-';
                    state = State::ExponentDigits;
                }
                b'0'..=b'9' => {
                    exponent = i64::from(byte - b'0');
                    exponent_digits = true;
                    state = State::ExponentDigits;
                }
                _ => return Ok(None),
            },
            State::ExponentDigits => match byte {
                b'0'..=b'9' => {
                    exponent_digits = true;
                    update_exponent(&mut exponent, byte);
                }
                _ => return Ok(None),
            },
        }
    }
    if mantissa_digits == 0 || (!matches!(state, State::Mantissa) && !exponent_digits) {
        return Ok(None);
    }
    if negative_exponent {
        exponent = -exponent;
    }

    let mut token = Token::new();
    if negative {
        token.push(b'-', work)?;
    }
    if let Some(first) = first_nonzero {
        let scientific = (i128::from(exponent) + integer_digits as i128 - first as i128 - 1)
            .clamp(-325, 309) as i32;
        token.push(significant[0], work)?;
        if retained > 1 || sticky {
            token.push(b'.', work)?;
            for &digit in &significant[1..retained] {
                token.push(digit, work)?;
            }
            if sticky {
                token.push(b'1', work)?;
            }
        }
        token.push(b'e', work)?;
        if scientific < 0 {
            token.push(b'-', work)?;
        }
        let magnitude = scientific.unsigned_abs();
        if magnitude >= 100 {
            token.push(b'0' + (magnitude / 100) as u8, work)?;
        }
        if magnitude >= 10 {
            token.push(b'0' + (magnitude / 10 % 10) as u8, work)?;
        }
        token.push(b'0' + (magnitude % 10) as u8, work)?;
    } else {
        token.push(b'0', work)?;
    }
    work.step()?;
    let normalized = std::str::from_utf8(&token.bytes[..token.len])
        .map_err(|_| internal("normalized decimal token is not ASCII"))?;
    let value = Float64Type::parse(normalized)
        .ok_or_else(|| internal("Arrow refused a proven valid normalized decimal token"))?;
    work.step()?;
    Ok(Some(value))
}

struct Token {
    bytes: [u8; TOKEN_BYTES],
    len: usize,
}
impl Token {
    fn new() -> Self {
        Self {
            bytes: [0; TOKEN_BYTES],
            len: 0,
        }
    }
    fn push(
        &mut self,
        byte: u8,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        work.step()?;
        let slot = self
            .bytes
            .get_mut(self.len)
            .ok_or_else(|| internal("normalized decimal token exceeded its proven bound"))?;
        *slot = byte;
        self.len += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Mutex, time::Duration};

    use crate::KernelEvaluationControl;

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl Control {
        fn refusing(index: usize, failure: KernelFailure) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                refusal: Some((index, failure)),
            }
        }
        fn calls(&self) -> Vec<u32> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let index = calls.len();
            calls.push(units);
            if let Some((at, failure)) = &self.refusal
                && index == *at
            {
                return Err(failure.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("float text parser must not wait");
        }
    }
    fn observed(
        text: &str,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<f64>, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let value = parse_f64(text, &mut work)?;
        work.finish()?;
        Ok(value)
    }
    fn oracle(text: &str) {
        let actual = observed(text, &Control::default()).unwrap();
        let expected = if text.as_bytes().first().is_some_and(u8::is_ascii_whitespace)
            || text.as_bytes().last().is_some_and(u8::is_ascii_whitespace)
        {
            // Original Arrow 58 raw lexical grammar rejects boundary ASCII
            // whitespace. This literal does not depend on the current SDK.
            None
        } else {
            Float64Type::parse(text)
        };
        match (actual, expected) {
            (Some(actual), Some(expected)) if actual.is_nan() && expected.is_nan() => {
                // NaN payload is not a SQL result identity; sign is retained.
                assert_eq!(actual.is_sign_negative(), expected.is_sign_negative());
            }
            (Some(actual), Some(expected)) => {
                assert_eq!(actual.to_bits(), expected.to_bits(), "source={text:?}");
            }
            (None, None) => {}
            (actual, expected) => panic!("source={text:?} actual={actual:?} expected={expected:?}"),
        }
    }

    #[test]
    fn standard_grammar_and_specials_agree_with_actual_arrow_parser() {
        for text in [
            "",
            "+",
            "-",
            ".",
            "+.",
            "1",
            "+1",
            "-0",
            "-0.0",
            "0e9999999999999",
            "-0e-999999999999",
            ".1",
            "1.",
            "1.e2",
            "01.020",
            "1E+2",
            "1e-2",
            "e1",
            "1e",
            "1e+",
            "1e-",
            "1e++1",
            "1e+-1",
            "++1",
            "1..0",
            "0x1",
            "1_000",
            " 1",
            "1 ",
            "1\n",
            "１２",
            "١",
            "1é",
            "NaN",
            "nan",
            "NAN",
            "+NaN",
            "-NaN",
            "inf",
            "+INF",
            "-inF",
            "Infinity",
            "-INFINITY",
            "infinite",
            "nan(1)",
            "NaNe0",
            "inf0",
            "infinity ",
            "-infinity0",
            "1e0.1",
            "1e1e1",
        ] {
            oracle(text);
        }
    }

    #[test]
    fn special_tokens_keep_original_complete_raw_grammar_and_control_tail() {
        // Independent original lexical STANDARD full-consumption expectations.
        // Signed NaNs retain the original sign-only identity assertion.
        for (text, expected) in [
            ("inf", Some(f64::INFINITY)),
            ("+INF", Some(f64::INFINITY)),
            ("-inF", Some(f64::NEG_INFINITY)),
            ("Infinity", Some(f64::INFINITY)),
            ("-INFINITY", Some(f64::NEG_INFINITY)),
            ("NaN", Some(f64::NAN)),
            ("+nan", Some(f64::NAN)),
            ("-nAn", Some(-f64::NAN)),
            ("infinity ", None),
            ("infinity\t", None),
            ("inf     ", None),
            ("+inf\r", None),
            ("NaN ", None),
            ("-NaN\n", None),
            (" inf", None),
            (" NaN", None),
            ("inf\u{a0}", None),
            ("NaN\u{85}", None),
            ("infinite", None),
            ("nan(1)", None),
        ] {
            let actual = observed(text, &Control::default()).unwrap();
            match (actual, expected) {
                (Some(actual), Some(expected)) if expected.is_nan() => {
                    assert!(actual.is_nan(), "source={text:?}");
                    assert_eq!(
                        actual.is_sign_negative(),
                        expected.is_sign_negative(),
                        "source={text:?}"
                    );
                }
                (Some(actual), Some(expected)) => {
                    assert_eq!(actual.to_bits(), expected.to_bits(), "source={text:?}")
                }
                (None, None) => {}
                (actual, expected) => {
                    panic!("source={text:?} actual={actual:?} expected={expected:?}")
                }
            }
        }
        let control = Control::default();
        assert_eq!(observed("infinity ", &control), Ok(None));
        assert_eq!(control.calls(), vec![0, 11]);
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let control = Control::refusing(1, cause.clone());
            assert_eq!(observed("infinity ", &control), Err(cause));
            assert_eq!(control.calls(), vec![0, 11]);
        }
    }

    #[test]
    fn original_exponent_keeps_first_crossing_value_not_a_clamped_limit() {
        for (digits, expected) in [
            ("268435455", 268435455),
            ("268435456", 268435456),
            ("2684354559", 2684354559),
            ("26843545590000000000000000", 2684354559),
            ("9999999999999999999999", 999999999),
            ("0000000000000000000000000000012", 12),
        ] {
            let mut exponent = 0;
            for digit in digits.bytes() {
                update_exponent(&mut exponent, digit);
            }
            assert_eq!(exponent, expected);
            oracle(&format!("1e{digits}"));
            oracle(&format!("-1e-{digits}"));
        }
    }

    #[test]
    fn long_leading_zeros_fraction_positions_and_exponents_have_no_length_gate() {
        for zeros in [0, 19, 769, 4096, 65536] {
            oracle(&format!("+{}12.5", "0".repeat(zeros)));
            oracle(&format!("-0.{}125e{}", "0".repeat(zeros), zeros + 1));
            oracle(&format!("125{}e-{}", "0".repeat(zeros), zeros + 1));
            oracle(&format!("1e+{}2", "0".repeat(zeros)));
            oracle(&format!("-0e-{}999999999999999", "0".repeat(zeros)));
        }
    }

    #[test]
    fn digit_boundaries_and_full_exponent_range_match_original_backend_bits() {
        for length in [1, 16, 17, 18, 19, 20, 64, 768, 769, 770, 2048] {
            let digits = (0..length)
                .map(|index| char::from(b'1' + ((index * 7 + 3) % 9) as u8))
                .collect::<String>();
            for exponent in [-400, -325, -324, -323, -309, -1, 0, 1, 307, 308, 309, 400] {
                for negative in ["", "-"] {
                    oracle(&format!(
                        "{negative}{}.{}e{exponent}",
                        &digits[..1],
                        &digits[1..]
                    ));
                }
            }
        }
        for text in [
            "1.7976931348623157e308",
            "1.7976931348623158e308",
            "1.7976931348623159e308",
            "2.2250738585072014e-308",
            "2.2250738585072011e-308",
            "4.9406564584124654e-324",
            "2.4703282292062327e-324",
            "2.4703282292062328e-324",
            "9.9999999999999999e-325",
            "1.00000000000000011102230246251565404236316680908203125",
            "1.00000000000000033306690738754696212708950042724609375",
        ] {
            oracle(text);
        }
    }

    #[test]
    fn exact_769_digit_tie_and_discarded_sticky_are_not_rounded_to_17_digits() {
        let tie = "1.00000000000000011102230246251565404236316680908203125";
        let digits = tie.bytes().filter(u8::is_ascii_digit).count();
        let padded = format!("{tie}{}", "0".repeat(SIGNIFICANT_DIGITS - digits));
        assert_eq!(
            observed(&padded, &Control::default())
                .unwrap()
                .unwrap()
                .to_bits(),
            1.0_f64.to_bits(),
        );
        let above = format!("{padded}{}1", "0".repeat(8192));
        assert_eq!(
            observed(&above, &Control::default())
                .unwrap()
                .unwrap()
                .to_bits(),
            1.0_f64.to_bits() + 1,
        );
        for text in [&padded, &above, &format!("-{above}")] {
            oracle(text);
        }
    }

    // Exact decimal coefficient of 2^-power: 5^power * 10^-power.
    // This integer-only oracle author does not invoke a float parser.
    fn inverse_power_two(power: usize) -> String {
        let mut decimal = vec![1_u8];
        for _ in 0..power {
            let mut carry = 0;
            for digit in &mut decimal {
                let product = *digit * 5 + carry;
                *digit = product % 10;
                carry = product / 10;
            }
            if carry != 0 {
                decimal.push(carry);
            }
        }
        let coefficient = decimal
            .into_iter()
            .rev()
            .map(|digit| char::from(b'0' + digit))
            .collect::<String>();
        format!(
            "{}.{}e{}",
            &coefficient[..1],
            &coefficient[1..],
            coefficient.len() as i32 - 1 - power as i32
        )
    }

    #[test]
    fn smallest_subnormal_exact_halfway_and_far_sticky_preserve_ties_even() {
        let half = inverse_power_two(1075);
        assert_eq!(
            observed(&half, &Control::default())
                .unwrap()
                .unwrap()
                .to_bits(),
            0
        );
        let (mantissa, exponent) = half.split_once('e').unwrap();
        let above = format!("{mantissa}{}1e{exponent}", "0".repeat(8192));
        assert_eq!(
            observed(&above, &Control::default())
                .unwrap()
                .unwrap()
                .to_bits(),
            1
        );
        let min = inverse_power_two(1074);
        assert_eq!(
            observed(&min, &Control::default())
                .unwrap()
                .unwrap()
                .to_bits(),
            1
        );
        for text in [
            &half,
            &above,
            &min,
            &format!("-{half}"),
            &format!("-{above}"),
        ] {
            oracle(text);
        }
    }

    #[test]
    fn source_byte_and_token_work_use_original_typed_control_at_entry_256_and_tail() {
        let source = format!("1.{}1e-324", "0".repeat(900));
        let accepted = Control::default();
        observed(&source, &accepted).unwrap();
        let trace = accepted.calls();
        assert_eq!(trace[0], 0);
        assert!(trace.iter().filter(|&&units| units == 256).count() >= 6);
        assert!(trace.last().is_some_and(|units| *units > 0 && *units < 256));
        for failure in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            // Every observed position, including token emission and final
            // publication, is refused with the unchanged original category.
            for index in 0..trace.len() {
                let control = Control::refusing(index, failure.clone());
                assert_eq!(observed(&source, &control), Err(failure.clone()));
                assert_eq!(control.calls().len(), index + 1);
            }
        }
    }

    #[test]
    fn malformed_long_source_and_special_declines_still_finish_under_original_control() {
        for source in [
            format!("{}!", "0".repeat(900)),
            "infinity!".into(),
            String::new(),
        ] {
            let accepted = Control::default();
            assert_eq!(observed(&source, &accepted).unwrap(), None);
            let trace = accepted.calls();
            for failure in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                let control = Control::refusing(trace.len() - 1, failure.clone());
                assert_eq!(observed(&source, &control), Err(failure));
            }
        }
    }
}
