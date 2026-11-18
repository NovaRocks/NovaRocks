// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::Span;
use novarocks_type_contract::{CompilePhase, MAX_UNOBSERVED_COMPILE_WORK, PureCompileControl};

use super::{QueryServiceError, invalid_procedure_argument};

/// Duration decoded by the original table-procedure argument binder.
/// The endpoint applies the Provider's cutoff domain to its rounded value.
#[derive(Debug, PartialEq)]
pub struct ProcedureRetentionDuration {
    value: f64,
    unit_ms: f64,
}

impl ProcedureRetentionDuration {
    pub fn parse(
        source: &str,
        span: Span,
        text: &str,
        control: &dyn PureCompileControl,
    ) -> Result<Self, QueryServiceError> {
        let checkpoint = |units| {
            control
                .checkpoint(CompilePhase::Validate, units)
                .map_err(QueryServiceError::from_compile_control)
        };
        let invalid = || {
            invalid_procedure_argument(
                source,
                span,
                "Invalid table procedure retention_threshold duration",
            )
        };
        checkpoint(0)?;
        for chunk in text.as_bytes().chunks(MAX_UNOBSERVED_COMPILE_WORK as usize) {
            let ascii = chunk.is_ascii();
            checkpoint(chunk.len() as u32)?;
            if !ascii {
                return Err(invalid());
            }
        }
        let duration = Self::decode(text);
        checkpoint(0)?;
        duration.ok_or_else(invalid)
    }

    fn decode(text: &str) -> Option<Self> {
        let (value, unit_ms) = fast_duration(text).or_else(|| decimal_duration(text))?;
        Some(Self { value, unit_ms })
    }

    pub fn rounded_millis(&self) -> Option<i64> {
        // Airlift rounds after unit conversion. At the representable double
        // equal to Long.MAX_VALUE converted to double, Java's long cast saturates.
        let rounded = (self.value * self.unit_ms + 0.5).floor();
        (rounded <= i64::MAX as f64).then_some(rounded as i64)
    }
}

fn unit_millis(unit: &str) -> Option<f64> {
    match unit {
        "ns" => Some(1.0 / 1_000_000.0),
        "us" => Some(1.0 / 1_000.0),
        "ms" => Some(1.0),
        "s" => Some(1_000.0),
        "m" => Some(60_000.0),
        "h" => Some(3_600_000.0),
        "d" => Some(86_400_000.0),
        _ => None,
    }
}

fn fast_duration(text: &str) -> Option<(f64, f64)> {
    // The fast path only runs when a known unit ends the original input.
    let unit = ["ns", "us", "ms", "s", "m", "h", "d"]
        .into_iter()
        .find(|&unit| text.ends_with(unit))?;
    let number = text.strip_suffix(unit)?.trim_matches(|ch| ch <= '\u{20}');
    let number = number.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(number);
    let unsigned = number.strip_prefix(['+', '-']).unwrap_or(number);
    let value = if let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        let magnitude = hexadecimal_double(hex)?;
        if number.starts_with('-') {
            -magnitude
        } else {
            magnitude
        }
    } else {
        number.parse::<f64>().ok()?
    };
    (value.is_finite() && value >= 0.0).then_some((value, unit_millis(unit)?))
}

fn decimal_duration(text: &str) -> Option<(f64, f64)> {
    // Java's default regex character classes use only ASCII whitespace and
    // digits. The fallback does not admit exponent, sign, or type suffix.
    let whitespace = |ch| matches!(ch, ' ' | '\t'..='\r');
    let text = text.trim_matches(whitespace);
    let unit_start = text
        .trim_end_matches(|ch: char| ch.is_ascii_alphabetic())
        .len();
    let (number, unit) = text.split_at(unit_start);
    let number = number.trim_end_matches(whitespace);
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|byte| byte.is_ascii_digit());
    if !number.split_once('.').map_or_else(
        || digits(number),
        |(whole, fraction)| digits(whole) && digits(fraction),
    ) {
        return None;
    }
    let value: f64 = number.parse().ok()?;
    value.is_finite().then_some((value, unit_millis(unit)?))
}

fn hexadecimal_double(text: &str) -> Option<f64> {
    let (mantissa, exponent) = text.split_once(['p', 'P'])?;
    let exponent_is_negative = exponent.starts_with('-');
    let exponent = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
    if exponent.is_empty() {
        return None;
    }
    let mut adjustment = 0i128;
    for byte in exponent.bytes() {
        if !byte.is_ascii_digit() {
            return None;
        }
        adjustment = adjustment
            .saturating_mul(10)
            .saturating_add(i128::from(byte - b'0'));
    }
    if exponent_is_negative {
        adjustment = -adjustment;
    }

    // Retain the leading 53 significant bits, one rounding bit, and whether
    // the rest was nonzero. The exponent is measured from the leading one.
    let mut digits = 0usize;
    let mut decimal_point = None;
    let mut leading = None;
    let mut prefix = 0u64;
    let mut prefix_bits = 0u32;
    let mut sticky = false;
    for byte in mantissa.bytes() {
        if byte == b'.' {
            if decimal_point.replace(digits).is_some() {
                return None;
            }
            continue;
        }
        let nibble = char::from(byte).to_digit(16)?;
        for bit in (0..4).rev() {
            let set = nibble & (1 << bit) != 0;
            if leading.is_none() {
                if !set {
                    continue;
                }
                leading = Some((digits, bit));
            }
            if prefix_bits < 54 {
                prefix = (prefix << 1) | u64::from(set);
                prefix_bits += 1;
            } else {
                sticky |= set;
            }
        }
        digits += 1;
    }
    if digits == 0 {
        return None;
    }
    let Some((digit, bit)) = leading else {
        return Some(0.0);
    };
    let decimal_point = decimal_point.unwrap_or(digits) as i128;
    let exponent =
        adjustment.saturating_add((decimal_point - digit as i128 - 1) * 4 + i128::from(bit));
    if exponent > 1023 {
        return Some(f64::INFINITY);
    }
    if exponent < -1075 {
        return Some(0.0);
    }
    let precision = 53.min(exponent + 1075) as u32;
    let prefix = prefix << (54 - prefix_bits);
    let shift = 54 - precision;
    let halfway = 1 << (shift - 1);
    let remainder = prefix & ((halfway << 1) - 1);
    let retained = prefix >> shift;
    let rounded = retained
        + u64::from(remainder > halfway || (remainder == halfway && (sticky || retained & 1 != 0)));
    if exponent < -1022 {
        return Some(f64::from_bits(rounded));
    }
    // The leading significand bit advances the encoded exponent by one.
    Some(f64::from_bits((((exponent + 1022) as u64) << 52) + rounded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_validation_preserves_the_original_control_refusal() {
        use novarocks_type_contract::CompileControlError;

        struct RefuseWork;
        impl PureCompileControl for RefuseWork {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::Validate);
                if units == 0 {
                    Ok(())
                } else {
                    Err(CompileControlError::ResourceExhausted)
                }
            }
        }

        let source = "unsupported duration";
        let error = ProcedureRetentionDuration::parse(
            source,
            Span::new(0, source.len()),
            source,
            &RefuseWork,
        )
        .unwrap_err();
        assert_eq!(
            error.compile_control_error(),
            Some(CompileControlError::ResourceExhausted),
        );
    }

    #[test]
    fn duration_units_preserve_reference_numeric_and_whitespace_rules() {
        for (text, millis) in [
            ("7d", Some(604_800_000)),
            (" 1.5 h \t", Some(5_400_000)),
            ("500us", Some(1)),
            ("499999ns", Some(0)),
            ("+1e-3s", Some(1)),
            ("1.e0s", Some(1000)),
            ("0x1.8p2fms", Some(6)),
            ("\0-0x1p-1075s", Some(0)),
            ("\0-0x1.0000000000001p-1075s", None),
            ("+1e0s ", None),
            ("0x1p2ms ", None),
            ("-1d", None),
            ("1h\u{a0}", None),
            ("１s", None),
            ("1H", None),
            ("1e999d", None),
            ("9223372036854775808ms", Some(i64::MAX)),
            ("9223372036854777856ms", None),
        ] {
            assert_eq!(
                ProcedureRetentionDuration::decode(text).and_then(|value| value.rounded_millis()),
                millis,
                "{text:?}"
            );
        }
    }
}
