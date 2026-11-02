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

//! Golden float spelling, independently checked against JDK output.
use std::fmt::Display;

pub fn real(value: f32) -> String {
    if let Some(special) = special(value as f64) {
        return special.into();
    }
    format_decimal(value, value as f64, format!("{value:.1e}"))
}
pub fn double(value: f64) -> String {
    if let Some(special) = special(value) {
        return special.into();
    }
    format_decimal(value, value, format!("{value:.1e}"))
}
fn special(value: f64) -> Option<&'static str> {
    if value.is_nan() {
        Some("NaN")
    } else if value == f64::INFINITY {
        Some("Infinity")
    } else if value == f64::NEG_INFINITY {
        Some("-Infinity")
    } else {
        None
    }
}
fn format_decimal(value: impl Display, exact: f64, two_digits: String) -> String {
    let decimal = value.to_string();
    let negative = decimal.starts_with('-');
    let unsigned = decimal.trim_start_matches('-');
    let dot = unsigned.find('.').unwrap_or(unsigned.len()) as i32;
    let Some(first) = unsigned.bytes().position(|b| b != b'0' && b != b'.') else {
        return if negative {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    };
    let mut exponent = dot - first as i32 - i32::from((first as i32) < dot);
    let digits: String = unsigned[first..].chars().filter(|c| *c != '.').collect();
    let significant = digits.trim_end_matches('0');
    let mut coefficient: u64 = significant.parse().expect("float significand fits u64");
    let decimal_power = exponent - significant.len() as i32 + 1;
    // Rust Display chooses the larger decimal in an exact tie. Java chooses
    // the even significand. Compare the midpoint as an exact binary rational;
    // parsing both candidates back would erase the distinction being tested.
    if coefficient % 2 == 1 && exact_midpoint(exact.abs(), coefficient, decimal_power) {
        coefficient -= 1;
    }
    let significant = coefficient.to_string();
    if significant.len() < digits.trim_end_matches('0').len() {
        exponent -= 1;
    }
    let magnitude = exact.abs();
    if (1e-3..1e7).contains(&magnitude) {
        let mut text = String::new();
        if negative {
            text.push('-');
        }
        let point = exponent + 1;
        if point <= 0 {
            text.push_str("0.");
            text.extend(std::iter::repeat_n('0', (-point) as usize));
            text.push_str(&significant);
        } else if point as usize >= significant.len() {
            text.push_str(&significant);
            text.extend(std::iter::repeat_n('0', point as usize - significant.len()));
            text.push_str(".0");
        } else {
            text.push_str(&significant[..point as usize]);
            text.push('.');
            text.push_str(&significant[point as usize..]);
        }
        return text;
    }
    if significant.len() == 1 {
        return two_digits.replace('e', "E");
    }
    format!(
        "{}{}.{}E{exponent}",
        if negative { "-" } else { "" },
        &significant[..1],
        &significant[1..]
    )
}
fn exact_midpoint(value: f64, upper: u64, decimal_power: i32) -> bool {
    let bits = value.to_bits();
    let encoded_exponent = ((bits >> 52) & 0x7ff) as i32;
    let mut binary = bits & ((1u64 << 52) - 1);
    let mut binary_power = if encoded_exponent == 0 {
        -1074
    } else {
        binary |= 1u64 << 52;
        encoded_exponent - 1023 - 52
    };
    if binary == 0 {
        return false;
    }
    binary_power += binary.trailing_zeros() as i32;
    binary >>= binary.trailing_zeros();
    let mut decimal = u128::from(upper) * 2 - 1;
    if decimal_power >= 0 {
        for _ in 0..decimal_power {
            let Some(next) = decimal.checked_mul(5) else {
                return false;
            };
            decimal = next;
            if decimal > u128::from(binary) {
                return false;
            }
        }
    } else {
        for _ in decimal_power..0 {
            if !decimal.is_multiple_of(5) {
                return false;
            }
            decimal /= 5;
        }
    }
    binary_power == decimal_power - 1 && u128::from(binary) == decimal
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matches_jdk_sample_oracle() {
        for (line, sample) in include_str!("testdata/java_float_samples.tsv")
            .lines()
            .enumerate()
        {
            if sample.starts_with('#') {
                continue;
            }
            let parts: Vec<_> = sample.split('\t').collect();
            let actual = match parts[0] {
                "double" => double(f64::from_bits(u64::from_str_radix(parts[1], 16).unwrap())),
                "real" => real(f32::from_bits(u32::from_str_radix(parts[1], 16).unwrap())),
                _ => panic!("invalid oracle row"),
            };
            assert_eq!(
                actual,
                parts[2],
                "JDK sample line {} bits {}",
                line + 1,
                parts[1]
            );
        }
    }
}
