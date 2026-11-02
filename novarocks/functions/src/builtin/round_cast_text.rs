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
mod tests {
    use super::*;
    use crate::{KernelEvaluationControl, MAX_UNOBSERVED_KERNEL_WORK};
    use std::{sync::Mutex, time::Duration};

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl Control {
        fn refusing(at: usize, error: KernelFailure) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                refusal: Some((at, error)),
            }
        }
        fn calls(&self) -> Vec<u32> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let at = calls.len();
            calls.push(units);
            if let Some((refused_at, error)) = &self.refusal
                && *refused_at == at
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("integer text parsing must not wait");
        }
    }
    fn observed(text: &str, control: &Control) -> Result<Option<i64>, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let result = parse_i64(text, &mut work)?;
        work.finish()?;
        Ok(result)
    }
    fn assert_arrow(text: &str) {
        assert_eq!(
            observed(text, &Control::default()).unwrap(),
            text.parse::<i64>().ok(),
            "input {text:?}"
        );
    }

    #[test]
    fn signed_boundaries_and_full_consumption_match_arrow_public_parser() {
        for text in [
            "",
            "+",
            "-",
            "0",
            "-0",
            "+0",
            "001",
            "+001",
            "-001",
            "9223372036854775807",
            "+9223372036854775807",
            "-9223372036854775808",
            "9223372036854775808",
            "-9223372036854775809",
            "+9223372036854775808",
            "18446744073709551615",
            " 1",
            "1 ",
            "1\n",
            "\t1",
            "1.0",
            "1e1",
            "0x10",
            "1_000",
            "++1",
            "--1",
            "+-1",
            "-+1",
            "1+1",
            "1-1",
            "1\0",
            "1\x000",
        ] {
            assert_arrow(text);
        }
        assert_eq!(
            observed("-9223372036854775808", &Control::default()),
            Ok(Some(i64::MIN))
        );
        assert_eq!(
            observed("+9223372036854775807", &Control::default()),
            Ok(Some(i64::MAX))
        );
    }

    #[test]
    fn short_mixed_grammar_matches_independent_arrow_parser() {
        // Cover signs in every position and invalid prefixes, interiors and
        // suffixes, including Arrow's fast invalid-last-byte branch.
        let alphabet = *b"+-019 .x";
        for first in alphabet {
            assert_arrow(std::str::from_utf8(&[first]).unwrap());
            for second in alphabet {
                assert_arrow(std::str::from_utf8(&[first, second]).unwrap());
                for third in alphabet {
                    assert_arrow(std::str::from_utf8(&[first, second, third]).unwrap());
                }
            }
        }
    }

    #[test]
    fn long_leading_zero_and_overflow_inputs_remain_observed() {
        let zeros = "0".repeat(4096);
        for suffix in ["0", "1", "9223372036854775807"] {
            let text = format!("+{zeros}{suffix}");
            assert_arrow(&text);
        }
        assert_arrow(&format!("-{zeros}9223372036854775808"));
        let overflow = "9".repeat(4097);
        let control = Control::default();
        assert_eq!(observed(&overflow, &control), Ok(None));
        assert_eq!(overflow.parse::<i64>().ok(), None);
        let calls = control.calls();
        assert_eq!(calls.first(), Some(&0));
        assert_eq!(calls.last(), Some(&1));
        assert_eq!(calls.iter().copied().sum::<u32>(), 4097);
        assert_eq!(calls.iter().filter(|&&units| units == 256).count(), 16);
    }

    #[test]
    fn long_invalid_prefix_and_unicode_are_safe_parse_failures() {
        let text = format!("{}x0", "0".repeat(300));
        let control = Control::default();
        assert_eq!(observed(&text, &control), Ok(None));
        assert_eq!(text.parse::<i64>().ok(), None);
        // The invalid byte is observed; input after it is not evaluated.
        assert_eq!(control.calls(), [0, 256, 45]);
        for text in ["１２", "١", "−1", "＋1", "1é0", "1\u{a0}", "💡0"] {
            assert_arrow(text);
        }
    }

    #[test]
    fn entry_quantum_and_tail_keep_each_typed_control_failure() {
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for (at, expected_calls) in [(0, vec![0]), (1, vec![0, 256]), (2, vec![0, 256, 1])] {
                let control = Control::refusing(at, error.clone());
                assert_eq!(observed(&"0".repeat(257), &control), Err(error.clone()));
                assert_eq!(control.calls(), expected_calls);
            }
            // An ordinary safe parse failure must still observe the tail.
            let control = Control::refusing(1, error.clone());
            assert_eq!(observed("1x0", &control), Err(error));
            assert_eq!(control.calls(), [0, 2]);
        }
    }

    #[test]
    fn empty_input_observes_caller_entry_and_empty_tail() {
        let control = Control::default();
        assert_eq!(observed("", &control), Ok(None));
        assert_eq!(control.calls(), [0, 0]);
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let control = Control::refusing(1, error.clone());
            assert_eq!(observed("", &control), Err(error));
            assert_eq!(control.calls(), [0, 0]);
        }
    }

    #[test]
    fn borrowed_parser_preserves_pending_work_from_its_caller() {
        let control = Control::default();
        control.checkpoint(0).unwrap();
        let mut work = EvaluationCheckpoints::new(&control);
        for _ in 0..250 {
            work.step().unwrap();
        }
        assert_eq!(parse_i64("000001", &mut work).unwrap(), Some(1));
        work.finish().unwrap();
        assert_eq!(control.calls(), [0, 256, 0]);
    }
}
