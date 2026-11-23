// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The numeric text algorithm shared by Trino scalar CAST and JSON number CAST.

use crate::KernelFailure;
use crate::kernel_control::internal;
use crate::kernel_input::EvaluationCheckpoints;
use std::cmp::Ordering;

// Fixed mathematical scratch for finite IEEE binary64 decimal development.
// A value/midpoint needs at most 1076 binary denominator bits; decimal
// normalization contributes at most 324*ceil(log2(10)) bits and the 53-bit
// significand. Nineteen digit iterations need fewer than 64 additional bits.
// 80 words cover that loose sum (2489 < 2560); this is no input text quota.
#[derive(Clone)]
struct Wide {
    words: [u32; 80],
    used: usize,
}
impl Wide {
    fn from_u64(value: u64) -> Self {
        let mut out = Self {
            words: [0; 80],
            used: 0,
        };
        if value != 0 {
            out.words[0] = value as u32;
            out.used = 1;
            if value >> 32 != 0 {
                out.words[1] = (value >> 32) as u32;
                out.used = 2;
            }
        }
        out
    }
    fn compare(
        &self,
        other: &Self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Ordering, KernelFailure> {
        Ok(match self.used.cmp(&other.used) {
            Ordering::Equal => {
                for i in (0..self.used).rev() {
                    work.step()?;
                    let order = self.words[i].cmp(&other.words[i]);
                    if order != Ordering::Equal {
                        return Ok(order);
                    }
                }
                Ordering::Equal
            }
            order => order,
        })
    }
    fn multiply(
        &mut self,
        factor: u32,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let mut carry = 0_u64;
        for word in &mut self.words[..self.used] {
            work.step()?;
            let value = u64::from(*word) * u64::from(factor) + carry;
            *word = value as u32;
            carry = value >> 32;
        }
        if carry != 0 {
            if self.used == self.words.len() {
                return Err(internal(
                    "finite float decimal scratch exceeded its mathematical bound",
                ));
            }
            self.words[self.used] = carry as u32;
            self.used += 1;
        }
        Ok(())
    }
    fn shift(
        &mut self,
        bits: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if self.used == 0 {
            return Ok(());
        }
        let whole = bits / 32;
        let part = bits % 32;
        if self.used + whole + usize::from(part != 0) > self.words.len() {
            return Err(internal(
                "finite float shift exceeded its mathematical bound",
            ));
        }
        if whole != 0 {
            for i in (0..self.used).rev() {
                work.step()?;
                self.words[i + whole] = self.words[i];
            }
            for word in &mut self.words[..whole] {
                work.step()?;
                *word = 0;
            }
            self.used += whole;
        }
        if part != 0 {
            let mut carry = 0_u64;
            for word in &mut self.words[..self.used] {
                work.step()?;
                let value = (u64::from(*word) << part) | carry;
                *word = value as u32;
                carry = value >> 32;
            }
            if carry != 0 {
                self.words[self.used] = carry as u32;
                self.used += 1;
            }
        }
        Ok(())
    }
    fn subtract(
        &mut self,
        other: &Self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let mut borrow = 0_u64;
        for i in 0..self.used {
            work.step()?;
            let sub = u64::from(if i < other.used { other.words[i] } else { 0 }) + borrow;
            let value = u64::from(self.words[i]);
            self.words[i] = value.wrapping_sub(sub) as u32;
            borrow = u64::from(value < sub);
        }
        if borrow != 0 {
            return Err(internal("float decimal subtraction violated its ordering"));
        }
        while self.used != 0 && self.words[self.used - 1] == 0 {
            work.step()?;
            self.used -= 1;
        }
        Ok(())
    }
    fn add(
        &self,
        other: &Self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let mut out = self.clone();
        let count = self.used.max(other.used);
        let mut carry = 0_u64;
        for i in 0..count {
            work.step()?;
            let value = u64::from(out.words[i]) + u64::from(other.words[i]) + carry;
            out.words[i] = value as u32;
            carry = value >> 32;
        }
        out.used = count;
        if carry != 0 {
            if count == out.words.len() {
                return Err(internal(
                    "finite float addition exceeded its mathematical bound",
                ));
            }
            out.words[count] = carry as u32;
            out.used += 1;
        }
        Ok(out)
    }
    fn power10(
        &mut self,
        power: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        for _ in 0..power {
            self.multiply(10, work)?;
        }
        Ok(())
    }
    fn quotient_digit(
        &mut self,
        denominator: &Self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<u8, KernelFailure> {
        let mut digit = 0;
        while self.compare(denominator, work)? != Ordering::Less {
            work.step()?;
            self.subtract(denominator, work)?;
            digit += 1;
            if digit > 9 {
                return Err(internal("float decimal quotient exceeded one digit"));
            }
        }
        Ok(digit)
    }
}

fn five_bits(power: i32) -> i32 {
    if power == 0 {
        0
    } else if power < 27 {
        let value = 5_u128.pow(power as u32);
        128 - value.leading_zeros() as i32
    } else {
        power * 3
    }
}
fn small_decimal_comparison(
    binary_exp: i32,
    fraction_bits: i32,
    significant: i32,
    decimal_exp: i32,
) -> bool {
    let tiny = (fraction_bits - binary_exp - 1).max(0);
    let b5 = (-decimal_exp).max(0);
    let s5 = decimal_exp.max(0);
    let mut b2 = b5 + tiny + binary_exp - fraction_bits + 1;
    let mut s2 = s5 + tiny;
    let mut m2 = b5 + tiny + binary_exp - significant;
    let common = b2.min(s2);
    b2 -= common;
    s2 -= common;
    m2 -= common;
    if fraction_bits == 1 {
        m2 -= 1;
    }
    if m2 < 0 {
        b2 -= m2;
        s2 -= m2;
    }
    fraction_bits + b2 + five_bits(b5) < 64 && s2 + 1 + five_bits(s5 + 1) < 64
}

pub(super) fn format_double(
    value: f64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, KernelFailure> {
    decimal_format(value, None, work)
}

pub(super) fn format_real(
    value: f32,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, KernelFailure> {
    if !value.is_finite() || value == 0.0 {
        return decimal_format(f64::from(value), Some(7), work);
    }
    // Trino's REAL caster first materializes Float.toString and reparses
    // those decimal digits at binary64 width before DecimalFormat.
    work.flush()?;
    let seed = real_decimal(value)
        .parse::<f64>()
        .map_err(|_| internal("finite REAL decimal failed its binary64 projection"))?;
    work.flush()?;
    decimal_format(seed, Some(7), work)
}

fn decimal_format(
    value: f64,
    significant_limit: Option<usize>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, KernelFailure> {
    if !value.is_finite() || value == 0.0 {
        let text = if value.is_nan() {
            "NaN"
        } else if value.is_infinite() {
            if value.is_sign_negative() {
                "-Infinity"
            } else {
                "Infinity"
            }
        } else if value.is_sign_negative() {
            "-0E0"
        } else {
            "0E0"
        };
        let mut output = string_with_capacity(text.len(), work)?;
        for ch in text.chars() {
            work.step()?;
            output.push(ch);
        }
        return Ok(output);
    }
    let bits = value.abs().to_bits();
    let encoded_exp = ((bits >> 52) & 0x7ff) as i32;
    let mut fraction = bits & ((1_u64 << 52) - 1);
    let (binary_exp, significant) = if encoded_exp == 0 {
        let width = 64 - fraction.leading_zeros() as i32;
        let shift = 53 - width;
        fraction <<= shift;
        (-1022 - shift, width)
    } else {
        fraction |= 1_u64 << 52;
        (encoded_exp - 1023, 53)
    };
    let fraction_bits = 53 - fraction.trailing_zeros() as i32;
    let tiny = (fraction_bits - binary_exp - 1).max(0);
    let mut digits = [0_u8; 32];
    let mut count = 0_usize;
    let mut decimal_exp;
    let mut exact = false;
    let mut rounded_up = false;
    if (-21..=62).contains(&binary_exp) && tiny == 0 {
        let integer = if binary_exp >= 52 {
            fraction << (binary_exp - 52)
        } else {
            fraction >> (52 - binary_exp)
        };
        let insignificant = if binary_exp > significant {
            let n = 1_u64 << (binary_exp - significant - 1);
            n.ilog10()
        } else {
            0
        };
        let unit = 10_u64.pow(insignificant);
        let rounded = integer / unit + u64::from(integer % unit >= unit / 2 && unit > 1);
        let text = rounded.to_string();
        decimal_exp = text.len() as i32 - 1 + insignificant as i32;
        for b in text.bytes() {
            work.step()?;
            digits[count] = b - b'0';
            count += 1;
        }
    } else {
        // The legacy converter's upper logarithm estimate affects the two
        // smallest decimal decades: a leading zero can round into the next
        // exponent. Replacing it with exact floor(log10) changes those values.
        #[expect(
            clippy::approx_constant,
            reason = "The legacy converter uses this rounded coefficient for its exponent estimate"
        )]
        const LEGACY_LOG10_2: f64 = 0.301029995663981;
        let normalized = f64::from_bits((1023_u64 << 52) | (fraction & ((1_u64 << 52) - 1)));
        decimal_exp = ((normalized - 1.5) * 0.289529654
            + 0.176091259
            + f64::from(binary_exp) * LEGACY_LOG10_2)
            .floor() as i32;
        // An exact value ratio and the legacy converter's symmetric margin.
        // At powers of two the legacy margin halves again; that behavior and
        // its integral fast path are part of DecimalFormat's observed output.
        let number = fraction >> fraction.trailing_zeros();
        let exponent = binary_exp - fraction_bits + 1;
        let margin_exp = binary_exp - significant - i32::from(fraction_bits == 1);
        let common = (-exponent).max(-margin_exp).max(0);
        let mut numerator = Wide::from_u64(number);
        numerator.shift((exponent + common) as usize, work)?;
        let mut margin = Wide::from_u64(1);
        margin.shift((margin_exp + common) as usize, work)?;
        let mut denominator = Wide::from_u64(1);
        denominator.shift(common as usize, work)?;
        if decimal_exp >= 0 {
            denominator.power10(decimal_exp as usize, work)?;
        } else {
            numerator.power10((-decimal_exp) as usize, work)?;
            margin.power10((-decimal_exp) as usize, work)?;
        }
        let mut ten = denominator.clone();
        ten.multiply(10, work)?;
        let strict = small_decimal_comparison(binary_exp, fraction_bits, significant, decimal_exp);
        let mut first = true;
        loop {
            work.step()?;
            let q = numerator.quotient_digit(&denominator, work)?;
            numerator.multiply(10, work)?;
            margin.multiply(10, work)?;
            let low = numerator.compare(&margin, work)? == Ordering::Less;
            let high = match numerator.add(&margin, work)?.compare(&ten, work)? {
                Ordering::Greater => true,
                Ordering::Equal => !strict,
                Ordering::Less => false,
            };
            if first && q == 0 && !high {
                decimal_exp -= 1;
            } else {
                digits[count] = q;
                count += 1;
            }
            let force_second = first && !(-3..8).contains(&decimal_exp);
            first = false;
            if !force_second && (low || high) {
                let round_up = if high && low {
                    let mut twice = numerator.clone();
                    twice.multiply(2, work)?;
                    match twice.compare(&ten, work)? {
                        Ordering::Greater => true,
                        Ordering::Equal => digits[count - 1] & 1 != 0,
                        Ordering::Less => false,
                    }
                } else {
                    high
                };
                exact = numerator.used == 0;
                rounded_up = round_up;
                if round_up {
                    increment(&mut digits[..count], &mut decimal_exp);
                }
                break;
            }
            if count == digits.len() {
                return Err(internal("finite float decimal digit bound was violated"));
            }
        }
    }
    while count > 1 && digits[count - 1] == 0 {
        count -= 1;
    }
    if let Some(limit) = significant_limit.filter(|limit| *limit < count) {
        let next = digits[limit];
        let terminal_midpoint = next == 5 && limit + 1 == count;
        let increment = next > 5
            || (next == 5
                && (!terminal_midpoint || (!rounded_up && (!exact || digits[limit - 1] % 2 != 0))));
        count = limit;
        if increment {
            self::increment(&mut digits[..count], &mut decimal_exp);
        }
        while count > 1 && digits[count - 1] == 0 {
            count -= 1;
        }
    }
    let exp = decimal_exp.to_string();
    let mut out = string_with_capacity(
        count + exp.len() + usize::from(value.is_sign_negative()) + 3,
        work,
    )?;
    if value.is_sign_negative() {
        out.push('-');
    }
    out.push(char::from(b'0' + digits[0]));
    out.push('.');
    if count == 1 {
        out.push('0');
    } else {
        for &digit in &digits[1..count] {
            work.step()?;
            out.push(char::from(b'0' + digit));
        }
    }
    out.push('E');
    out.push_str(&exp);
    Ok(out)
}

fn increment(digits: &mut [u8], exponent: &mut i32) {
    for digit in digits.iter_mut().rev() {
        if *digit != 9 {
            *digit += 1;
            return;
        }
        *digit = 0;
    }
    digits[0] = 1;
    *exponent += 1;
}

fn string_with_capacity(
    len: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, KernelFailure> {
    work.flush()?;
    let mut out = String::new();
    out.try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(out)
}

/// Java chooses the nearest one- or two-digit decimal when the shortest
/// round-trip has one digit. On a true decimal tie it keeps the even one.
pub(super) fn real_decimal(value: f32) -> String {
    let shortest = format!("{value:E}");
    let Some((coefficient, exponent)) = shortest.split_once('E') else {
        return shortest;
    };
    let magnitude = coefficient.trim_start_matches('-');
    if magnitude.len() == 1 {
        return format!("{value:.1E}");
    }
    let scientific_exponent = exponent.parse::<i32>().expect("finite REAL exponent");
    let (whole, fraction) = magnitude.split_once('.').expect("finite REAL coefficient");
    let coefficient = whole
        .bytes()
        .chain(fraction.bytes())
        .fold(0_u64, |number, digit| number * 10 + u64::from(digit - b'0'));
    let exponent = scientific_exponent - fraction.len() as i32;
    if coefficient % 2 == 0 || !is_real_decimal_midpoint(value, coefficient, exponent) {
        return shortest;
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    format!("{sign}{}E{exponent}", coefficient - 1)
}

fn is_real_decimal_midpoint(value: f32, coefficient: u64, exponent: i32) -> bool {
    let bits = value.abs().to_bits();
    let exponent_bits = ((bits >> 23) & 0xff) as i32;
    let mut fraction = bits & ((1_u32 << 23) - 1);
    let mut binary_power = -149;
    if exponent_bits != 0 {
        fraction |= 1_u32 << 23;
        binary_power = exponent_bits - 150;
    }
    let zeros = fraction.trailing_zeros();
    binary_power += zeros as i32;
    let numerator = u64::from(fraction >> zeros);
    if binary_power != exponent - 1 {
        return false;
    }
    let mut midpoint = 2 * coefficient - 1;
    if exponent >= 0 {
        for _ in 0..exponent {
            // Equality is impossible as soon as the odd numerator exceeds
            // the 24-bit significand, well before u64 overflow.
            midpoint *= 5;
            if midpoint > numerator {
                return false;
            }
        }
    } else {
        for _ in exponent..0 {
            if !midpoint.is_multiple_of(5) {
                return false;
            }
            midpoint /= 5;
        }
    }
    midpoint == numerator
}
