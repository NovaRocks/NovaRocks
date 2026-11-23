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

//! Data-size grammar shared by SQL maintenance arguments and Connector properties.

/// The value Airlift DataSize represents as a nonnegative Java long.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorDataSize(i64);

impl ConnectorDataSize {
    pub fn parse<E>(
        text: &str,
        check_active: impl FnMut() -> Result<(), E>,
        invalid: impl Fn(DataSizeParseFailure, &str) -> E,
    ) -> Result<Self, E> {
        parse_airlift_data_size(text, check_active, invalid).map(Self)
    }

    pub const fn bytes(self) -> u64 {
        self.0 as u64
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSizeParseFailure {
    Syntax,
    Unit,
    IntegerOverflow,
    DecimalOverflow,
}

impl DataSizeParseFailure {
    /// Airlift's error detail for the offending token, without caller context.
    pub fn message(self, field: &str) -> String {
        match self {
            Self::Syntax => format!("size is not a valid data size string: {field}"),
            Self::Unit => format!("Unknown unit: {field}"),
            Self::IntegerOverflow if field.parse::<i64>().is_err() => {
                format!("For input string: \"{field}\"")
            }
            Self::IntegerOverflow => "long overflow".to_owned(),
            Self::DecimalOverflow => format!("size is too large: {field}"),
        }
    }
}

/// Parse the Airlift units 1.13 spelling under the caller's original control.
pub fn parse_airlift_data_size<E>(
    text: &str,
    mut check_active: impl FnMut() -> Result<(), E>,
    invalid: impl Fn(DataSizeParseFailure, &str) -> E,
) -> Result<i64, E> {
    use DataSizeParseFailure as P;
    // The 1.13 fast path is selected by Java UTF-16 length and a final B.
    // Its Long.parseLong(char) behavior includes BMP decimal digits and -0.
    // Counting only through 21 decides this original fast-path condition.
    let mut units = 0;
    for ch in text.chars() {
        check_active()?;
        units += ch.len_utf16();
        if units > 20 {
            break;
        }
    }
    if (2..=20).contains(&units)
        && !text.starts_with('+')
        && let Some(number) = text.strip_suffix('B')
        && let Some(bytes) = java_byte_integer(number, &mut check_active)?
    {
        return Ok(bytes);
    }
    // Exact Java regex: ^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$.
    // Neither Unicode whitespace nor exponent/leading-sign syntax is enabled.
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() && regex_space(bytes[i]) {
        check_active()?;
        i += 1;
    }
    let start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        check_active()?;
        i += 1;
    }
    if start == i {
        return Err(invalid(P::Syntax, text));
    }
    let decimal = i < bytes.len() && bytes[i] == b'.';
    if decimal {
        check_active()?;
        i += 1;
        let fraction_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            check_active()?;
            i += 1;
        }
        if i == fraction_start {
            return Err(invalid(P::Syntax, text));
        }
    }
    let number = &text[start..i];
    while i < bytes.len() && regex_space(bytes[i]) {
        check_active()?;
        i += 1;
    }
    let unit_start = i;
    while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
        check_active()?;
        i += 1;
    }
    let unit_end = i;
    while i < bytes.len() && regex_space(bytes[i]) {
        check_active()?;
        i += 1;
    }
    if unit_start == unit_end || i != bytes.len() {
        return Err(invalid(P::Syntax, text));
    }
    let unit = &text[unit_start..unit_end];
    let factor: i64 = match unit {
        "B" => 1,
        "kB" => 1 << 10,
        "MB" => 1 << 20,
        "GB" => 1 << 30,
        "TB" => 1 << 40,
        "PB" => 1 << 50,
        "EB" => 1 << 60,
        _ => return Err(invalid(P::Unit, unit)),
    };
    check_active()?;
    if !decimal {
        let magnitude: i64 = number
            .parse()
            .map_err(|_| invalid(P::IntegerOverflow, number))?;
        return magnitude
            .checked_mul(factor)
            .ok_or_else(|| invalid(P::IntegerOverflow, number));
    }
    // Keep the actual 1.13 IEEE operation sequence. Long.MAX_VALUE compares as
    // 2^63 in this double comparison; Java's narrowing conversion saturates.
    // Rust's saturating float-to-integer conversion has that same boundary.
    let size: f64 = number
        .parse()
        .map_err(|_| invalid(P::DecimalOverflow, text))?;
    check_active()?;
    if !size.is_finite() {
        return Err(invalid(P::DecimalOverflow, text));
    }
    let rounded = (size / (1.0 / factor as f64) + 0.5).floor();
    if rounded > i64::MAX as f64 {
        return Err(invalid(P::DecimalOverflow, text));
    }
    Ok(rounded as i64)
}

fn regex_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | b'\x0c' | b'\r')
}

fn java_byte_integer<E>(
    number: &str,
    check_active: &mut impl FnMut() -> Result<(), E>,
) -> Result<Option<i64>, E> {
    let (negative, digits) = match number.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, number),
    };
    if digits.is_empty() {
        return Ok(None);
    }
    let mut result = 0_i64;
    for ch in digits.chars() {
        check_active()?;
        let Some(digit) = bmp_decimal_digit(ch) else {
            return Ok(None);
        };
        let Some(next) = result
            .checked_mul(10)
            .and_then(|value| value.checked_add(i64::from(digit)))
        else {
            return Ok(None);
        };
        result = next;
    }
    Ok((!negative || result == 0).then_some(result))
}

// Extracted from the exact JDK 25.0.1+8 UnicodeData.txt (Unicode 16.0),
// following Character.digit(char, 10)'s decimal-digit branch. Supplementary
// digits cannot enter Long.parseLong's char-based digit call.
fn bmp_decimal_digit(ch: char) -> Option<u8> {
    const STARTS: [u32; 37] = [
        0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66,
        0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946,
        0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
        0xa9f0, 0xaa50, 0xabf0, 0xff10,
    ];
    let code = u32::from(ch);
    STARTS.into_iter().find_map(|start| {
        code.checked_sub(start)
            .filter(|delta| *delta < 10)
            .map(|delta| delta as u8)
    })
}

#[cfg(test)]
mod tests;
