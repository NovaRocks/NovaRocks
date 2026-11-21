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

//! Duration escrowed by the statement and its selected connector generation.

use std::{cmp::Ordering, fmt};

pub const EXPIRE_SNAPSHOTS_MIN_RETENTION_SESSION_PROPERTY: &str = "expire_snapshots_min_retention";
pub const REMOVE_ORPHAN_FILES_MIN_RETENTION_SESSION_PROPERTY: &str =
    "remove_orphan_files_min_retention";

const DURATION_UNITS: [&str; 7] = ["ns", "us", "ms", "s", "m", "h", "d"];

/// Airlift-style duration retaining the original unit for error details.
#[derive(Clone, Copy, Debug)]
pub struct ConnectorMaintenanceDuration {
    value: f64,
    unit: &'static str,
}

impl ConnectorMaintenanceDuration {
    pub const SEVEN_DAYS: Self = Self {
        value: 7.0,
        unit: "d",
    };

    pub fn parse(text: &str) -> Option<Self> {
        if !text.is_ascii() {
            return None;
        }
        let (value, unit) = fast_duration(text).or_else(|| decimal_duration(text))?;
        Some(Self { value, unit })
    }

    fn milliseconds(self) -> f64 {
        self.value * unit_millis(self.unit).expect("validated duration unit")
    }

    pub fn rounded_millis(self) -> Option<i64> {
        let rounded = (self.milliseconds() + 0.5).floor();
        // At the representable floating-point boundary Java's long cast saturates.
        (rounded <= i64::MAX as f64).then_some(rounded as i64)
    }

    pub(super) fn digest_into(self, hash: &mut sha2::Sha256) {
        use sha2::Digest;
        hash.update(self.value.to_bits().to_be_bytes());
        hash.update([self.unit.len() as u8]);
        hash.update(self.unit.as_bytes());
    }

    pub fn is_shorter_than(self, other: Self) -> bool {
        self.milliseconds().total_cmp(&other.milliseconds()) == Ordering::Less
    }
}

impl PartialEq for ConnectorMaintenanceDuration {
    fn eq(&self, other: &Self) -> bool {
        self.unit == other.unit && self.value.to_bits() == other.value.to_bits()
    }
}

impl Eq for ConnectorMaintenanceDuration {}

impl fmt::Display for ConnectorMaintenanceDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scaled = (self.value * 100.0 + 0.5).floor() as i64;
        write!(f, "{}.{:02}{}", scaled / 100, scaled % 100, self.unit)
    }
}

/// Frozen inputs for the catalog's retention check for an explicit EXECUTE.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorMaintenanceRetention {
    pub requested: ConnectorMaintenanceDuration,
    pub session_minimum: Option<ConnectorMaintenanceDuration>,
}

impl ConnectorMaintenanceRetention {
    pub(super) fn digest_into(self, hash: &mut sha2::Sha256) {
        use sha2::Digest;
        self.requested.digest_into(hash);
        hash.update([u8::from(self.session_minimum.is_some())]);
        if let Some(minimum) = self.session_minimum {
            minimum.digest_into(hash);
        }
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

fn fast_duration(text: &str) -> Option<(f64, &'static str)> {
    // The fast path only runs when a known unit ends the original input.
    let unit = DURATION_UNITS
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
    (value.is_finite() && value >= 0.0).then_some((value, unit))
}

fn decimal_duration(text: &str) -> Option<(f64, &'static str)> {
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
    DURATION_UNITS
        .into_iter()
        .find(|&known| known == unit)
        .filter(|_| value.is_finite())
        .map(|unit| (value, unit))
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
