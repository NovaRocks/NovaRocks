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

use super::{JsonError, JsonValueError as Error, JsonWork, Number, Scalar};
use std::fmt::{self, Write};

const PRECISION: usize = 200;
const CAPACITY: usize = 84;

pub(super) fn minimal_signed(mut bytes: &[u8]) -> &[u8] {
    while let [.., next, last] = bytes {
        if (*last == 0 && next & 128 == 0) || (*last == 255 && next & 128 != 0) {
            bytes = &bytes[..bytes.len() - 1];
        } else {
            break;
        }
    }
    bytes
}

pub(super) fn read_i128(source: &[u8]) -> Result<i128, Error> {
    let last = source.last().ok_or(Error::Scalar)?;
    if source.len() > 16 || minimal_signed(source).len() != source.len() {
        return Err(Error::Scalar);
    }
    let extension = if last & 128 == 0 { 0 } else { 255 };
    let mut coefficient = [extension; 16];
    coefficient[..source.len()].copy_from_slice(source);
    Ok(i128::from_le_bytes(coefficient))
}

pub(super) struct Coefficient {
    bytes: [u8; CAPACITY],
    len: usize,
}

impl Coefficient {
    fn unsigned() -> Self {
        Self {
            bytes: [0; CAPACITY],
            len: 1,
        }
    }

    pub fn from_digits(digits: &[u8], negative: bool) -> Result<Self, Error> {
        let mut result = Self::unsigned();
        for &digit in digits {
            let mut carry = u16::from(digit - b'0');
            for byte in &mut result.bytes[..result.len] {
                carry += u16::from(*byte) * 10;
                *byte = carry as u8;
                carry >>= 8;
            }
            if carry != 0 {
                *result.bytes.get_mut(result.len).ok_or(Error::Scalar)? = carry as u8;
                result.len += 1;
            }
        }
        if result.bytes[result.len - 1] & 128 != 0 {
            if result.len == CAPACITY {
                return Err(Error::Scalar);
            }
            result.len += 1;
        }
        if negative {
            twos_complement(&mut result.bytes[..result.len]);
        }
        result.len = minimal_signed(&result.bytes[..result.len]).len();
        Ok(result)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

fn twos_complement(bytes: &mut [u8]) {
    let mut carry = 1;
    for byte in bytes {
        let (value, overflow) = (!*byte).overflowing_add(carry);
        *byte = value;
        carry = u8::from(overflow);
    }
}

struct Digits {
    bytes: [u8; PRECISION],
    start: usize,
    negative: bool,
}

impl Digits {
    fn from_coefficient(source: &[u8]) -> Result<Self, Error> {
        Self::from_coefficient_observed(source, || Ok(())).map_err(|error| match error {
            JsonError::Value(error) => error,
            _ => unreachable!("pure coefficient conversion only returns value errors"),
        })
    }

    fn from_coefficient_observed(
        source: &[u8],
        mut work: impl FnMut() -> Result<(), crate::KernelFailure>,
    ) -> Result<Self, JsonError> {
        let last = source.last().ok_or(Error::Scalar)?;
        if source.len() > CAPACITY || minimal_signed(source).len() != source.len() {
            return Err(Error::Scalar.into());
        }
        let negative = last & 128 != 0;
        let mut magnitude = [0; CAPACITY];
        magnitude[..source.len()].copy_from_slice(source);
        if negative {
            twos_complement(&mut magnitude[..source.len()]);
        }
        let mut remaining = source.len();
        let mut digits = Self {
            bytes: [0; PRECISION],
            start: PRECISION,
            negative,
        };
        loop {
            let mut carry = 0_u16;
            for chunk in magnitude[..remaining].rchunks_mut(16) {
                work()?;
                for byte in chunk.iter_mut().rev() {
                    carry = carry * 256 + u16::from(*byte);
                    *byte = (carry / 10) as u8;
                    carry %= 10;
                }
            }
            digits.start = digits.start.checked_sub(1).ok_or(Error::Scalar)?;
            digits.bytes[digits.start] = b'0' + carry as u8;
            while remaining > 0 && magnitude[remaining - 1] == 0 {
                remaining -= 1;
            }
            if remaining == 0 {
                break;
            }
        }
        Ok(digits)
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[self.start..]).expect("coefficient decimal digits")
    }
}

const fn decimal_capacity() -> [u8; CAPACITY] {
    let mut result = [0; CAPACITY];
    result[0] = 1;
    let mut power = 0;
    while power < PRECISION {
        let mut carry = 0_u16;
        let mut index = 0;
        while index < CAPACITY {
            carry += result[index] as u16 * 10;
            result[index] = carry as u8;
            carry >>= 8;
            index += 1;
        }
        power += 1;
    }
    result
}

pub(super) fn validate_number(scale: i32, coefficient: &[u8]) -> Result<(), Error> {
    if !(-16_384..=16_383).contains(&scale)
        || coefficient.is_empty()
        || coefficient.len() > CAPACITY
        || minimal_signed(coefficient).len() != coefficient.len()
    {
        return Err(Error::Scalar);
    }
    if coefficient == [0] {
        return if scale == 0 {
            Ok(())
        } else {
            Err(Error::Scalar)
        };
    }
    const MAX_MAGNITUDE: [u8; CAPACITY] = decimal_capacity();
    let mut magnitude = [0; CAPACITY];
    let bytes = &mut magnitude[..coefficient.len()];
    bytes.copy_from_slice(coefficient);
    if coefficient[coefficient.len() - 1] & 128 != 0 {
        twos_complement(bytes);
    }
    if magnitude
        .iter()
        .rev()
        .cmp(MAX_MAGNITUDE.iter().rev())
        .is_ge()
    {
        return Err(Error::Scalar);
    }
    let remainder = magnitude.iter().rev().fold(0_u16, |remainder, byte| {
        (remainder * 256 + u16::from(*byte)) % 10
    });
    if remainder == 0 {
        return Err(Error::Scalar);
    }
    Ok(())
}

/// Java's shortest decimal uses one or two significant digits when the
/// shortest round-trip length is one, choosing the nearest admissible value.
pub fn java_double_decimal(value: f64) -> Option<(i64, i32)> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some((0, 0));
    }
    let mut buffer = ryu::Buffer::new();
    let (coefficient, scale) = decimal_components(buffer.format_finite(value));
    if coefficient.unsigned_abs() < 10 {
        let rounded = format_short(value);
        let text = rounded.as_str();
        if text.parse::<f64>().ok() == Some(value) {
            return Some(decimal_components(text));
        }
    }
    Some((coefficient, scale))
}

fn java_real_decimal(value: f32) -> (i64, i32) {
    let mut buffer = ryu::Buffer::new();
    let (coefficient, scale) = decimal_components(buffer.format_finite(value));
    if coefficient != 0 && coefficient.unsigned_abs() < 10 {
        let rounded = format_short(f64::from(value));
        let text = rounded.as_str();
        if text.parse::<f32>().ok() == Some(value) {
            return decimal_components(text);
        }
    }
    (coefficient, scale)
}

fn format_short(value: f64) -> FixedText<32> {
    let mut text = FixedText::new();
    write!(text, "{value:.1e}").expect("bounded scientific float text");
    text
}

fn decimal_components(text: &str) -> (i64, i32) {
    let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
    let (unsigned, negative) = match mantissa.strip_prefix('-') {
        Some(value) => (value, true),
        None => (mantissa, false),
    };
    let mut magnitude = 0_i64;
    let mut fractional = 0_i32;
    let mut point = false;
    for byte in unsigned.bytes() {
        if byte == b'.' {
            point = true;
        } else {
            magnitude = magnitude * 10 + i64::from(byte - b'0');
            fractional += i32::from(point);
        }
    }
    let mut scale = fractional - exponent.parse::<i32>().expect("scientific exponent");
    while magnitude != 0 && magnitude % 10 == 0 {
        magnitude /= 10;
        scale -= 1;
    }
    (if negative { -magnitude } else { magnitude }, scale)
}

#[derive(Clone)]
pub(crate) struct FixedText<const N: usize> {
    data: [u8; N],
    len: usize,
}

impl<const N: usize> FixedText<N> {
    pub fn new() -> Self {
        Self {
            data: [0; N],
            len: 0,
        }
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.data[..self.len]).expect("writes originate from UTF-8 strings")
    }
}

impl<const N: usize> fmt::Write for FixedText<N> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let end = self.len.checked_add(text.len()).ok_or(fmt::Error)?;
        self.data
            .get_mut(self.len..end)
            .ok_or(fmt::Error)?
            .copy_from_slice(text.as_bytes());
        self.len = end;
        Ok(())
    }
}

fn write_decimal(out: &mut impl Write, digits: &str, negative: bool, scale: i32) -> fmt::Result {
    if negative {
        out.write_char('-')?;
    }
    match usize::try_from(scale) {
        Ok(0) => out.write_str(digits),
        Ok(scale) if scale < digits.len() => {
            let (left, right) = digits.split_at(digits.len() - scale);
            write!(out, "{left}.{right}")
        }
        Ok(scale) => {
            out.write_str("0.")?;
            write_zeros(out, scale - digits.len())?;
            out.write_str(digits)
        }
        Err(_) => {
            out.write_str(digits)?;
            write_zeros(out, scale.unsigned_abs() as usize)
        }
    }
}

fn write_zeros(out: &mut impl Write, mut count: usize) -> fmt::Result {
    const ZEROES: &str = "00000000000000000000000000000000";
    while count > 0 {
        let n = count.min(ZEROES.len());
        out.write_str(&ZEROES[..n])?;
        count -= n;
    }
    Ok(())
}

fn write_float(
    out: &mut impl Write,
    value: f64,
    finite: impl FnOnce() -> (i64, i32),
) -> fmt::Result {
    if !value.is_finite() {
        return out.write_str(special_float(value));
    }
    if value == 0.0 {
        return out.write_str(if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        });
    }
    let (coefficient, scale) = finite();
    let mut digits = FixedText::<20>::new();
    write!(digits, "{}", coefficient.unsigned_abs())?;
    let digits = digits.as_str();
    let exponent = digits.len() as i32 - 1 - scale;
    if (-3..7).contains(&exponent) {
        write_decimal(out, digits, coefficient < 0, scale)?;
        if scale <= 0 {
            out.write_str(".0")?;
        }
        return Ok(());
    }
    if coefficient < 0 {
        out.write_char('-')?;
    }
    let fractional = digits.get(1..).filter(|s| !s.is_empty()).unwrap_or("0");
    write!(out, "{}.{fractional}E{exponent}", &digits[..1])
}

fn special_float(value: f64) -> &'static str {
    if value.is_nan() {
        "NaN"
    } else if value.is_sign_negative() {
        "-Infinity"
    } else {
        "Infinity"
    }
}

pub(super) fn write_date(out: &mut impl Write, days: i32) -> fmt::Result {
    let (year, month, day) = crate::datetime_value::date32_to_gregorian(days);
    if year < 0 {
        write!(out, "-{:04}-{month:02}-{day:02}", year.unsigned_abs())
    } else if year > 9999 {
        write!(out, "+{year}-{month:02}-{day:02}")
    } else {
        write!(out, "{year:04}-{month:02}-{day:02}")
    }
}

pub(crate) fn write_scalar_text(out: &mut impl Write, scalar: Scalar<'_>) -> fmt::Result {
    match scalar {
        Scalar::Boolean(value) => write!(out, "{value}"),
        Scalar::String(value) => out.write_str(value),
        Scalar::TinyInt(value) => write!(out, "{value}"),
        Scalar::SmallInt(value) => write!(out, "{value}"),
        Scalar::Integer(value) => write!(out, "{value}"),
        Scalar::BigInt(value) => write!(out, "{value}"),
        Scalar::Real(value) => write_float(out, f64::from(value), || java_real_decimal(value)),
        Scalar::Double(value) => {
            write_float(out, value, || java_double_decimal(value).expect("finite"))
        }
        Scalar::Decimal {
            scale, coefficient, ..
        } => {
            let mut digits = FixedText::<39>::new();
            write!(digits, "{}", coefficient.unsigned_abs())?;
            write_decimal(out, digits.as_str(), coefficient < 0, i32::from(scale))
        }
        Scalar::Number(Number::Finite { scale, coefficient }) => {
            let digits = Digits::from_coefficient(coefficient).map_err(|_| fmt::Error)?;
            write_decimal(out, digits.as_str(), digits.negative, scale)
        }
        Scalar::Number(Number::NaN) => out.write_str("NaN"),
        Scalar::Number(Number::PositiveInfinity) => out.write_str("+Infinity"),
        Scalar::Number(Number::NegativeInfinity) => out.write_str("-Infinity"),
        Scalar::Date(days) => write_date(out, days),
        Scalar::Temporal { ty, value } => crate::temporal_value::format(
            out,
            ty,
            value,
            crate::temporal_value::TemporalTextFormat::Cast,
        )
        .map_err(|_| fmt::Error),
    }
}

/// JSON scalar-to-character presentation. The destination owns its capacity.
pub fn write_scalar_cast(
    out: &mut impl Write,
    scalar: Scalar<'_>,
    work: &mut crate::kernel_input::EvaluationCheckpoints<'_>,
) -> Result<(), super::JsonError> {
    let result = match scalar {
        Scalar::Real(value) => {
            out.write_str(&crate::cast::array_json::cast_float_real(value, work)?)
        }
        Scalar::Double(value) => {
            out.write_str(&crate::cast::array_json::cast_float_double(value, work)?)
        }
        Scalar::Number(Number::Finite { coefficient, scale }) => {
            let digits = Digits::from_coefficient(coefficient)?;
            write_number_cast(out, &digits, scale)
        }
        _ => write_scalar_text(out, scalar),
    };
    result.map_err(|_| Error::Extent.into())
}

fn write_number_cast(out: &mut impl Write, digits: &Digits, scale: i32) -> fmt::Result {
    let text = digits.as_str();
    let exponent = text.len() as i32 - 1 - scale;
    if scale < 0 || exponent < -6 {
        if digits.negative {
            out.write_char('-')?;
        }
        out.write_str(&text[..1])?;
        if text.len() > 1 {
            write!(out, ".{}", &text[1..])?;
        }
        write!(out, "E{exponent:+}")
    } else {
        write_decimal(out, text, digits.negative, scale)
    }
}

pub(super) fn number_digits(
    coefficient: &[u8],
    work: &mut impl JsonWork,
) -> Result<(FixedText<200>, bool), JsonError> {
    let digits = Digits::from_coefficient_observed(coefficient, || work.step())?;
    let mut text = FixedText::new();
    text.write_str(digits.as_str())
        .expect("bounded NUMBER coefficient");
    Ok((text, digits.negative))
}
