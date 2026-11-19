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

//! The two declared Iceberg DataSize properties, using Airlift units 1.13.
//!
//! This normalizes a property value; it does not execute a property mutation.

use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, DdlPropertyValue,
};
use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSizeProperty {
    ParquetWriterRowGroupSize,
    TargetMaxFileSize,
}
impl DataSizeProperty {
    pub const fn name(self) -> &'static str {
        match self {
            Self::ParquetWriterRowGroupSize => "parquet_writer_row_group_size",
            Self::TargetMaxFileSize => "target_max_file_size",
        }
    }
    pub const fn raw_key(self) -> &'static str {
        match self {
            Self::ParquetWriterRowGroupSize => "write.parquet.row-group-size-bytes",
            Self::TargetMaxFileSize => "write.target-file-size-bytes",
        }
    }
}

/// A parsed and property-validated count, not a write capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NormalizedDataSize {
    property: DataSizeProperty,
    bytes: i64,
}
impl NormalizedDataSize {
    pub const fn property(self) -> DataSizeProperty {
        self.property
    }
    pub const fn bytes(self) -> i64 {
        self.bytes
    }
    pub const fn raw_key(self) -> &'static str {
        self.property.raw_key()
    }
    pub fn raw_value(self) -> String {
        self.bytes.to_string()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSizeParseFailure {
    Syntax,
    Unit,
    IntegerOverflow,
    DecimalOverflow,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSizeRangeFailure {
    ZeroParquetRowGroup,
    ParquetRowGroupAboveTwoGiB,
    ParquetRowGroupOutsideUsize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSizeFailureReason {
    ExpectedVarchar,
    Parse(DataSizeParseFailure),
    Range(DataSizeRangeFailure),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataSizePropertyFailure {
    property: DataSizeProperty,
    reason: DataSizeFailureReason,
}
impl DataSizePropertyFailure {
    pub const fn property(self) -> DataSizeProperty {
        self.property
    }
    pub const fn reason(self) -> DataSizeFailureReason {
        self.reason
    }
}
impl fmt::Display for DataSizePropertyFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid Iceberg {}: {:?}",
            self.property.name(),
            self.reason
        )
    }
}
impl Error for DataSizePropertyFailure {}
impl FailureProjection for DataSizePropertyFailure {
    fn failure_fact(&self) -> FailureFact {
        ConnectorErrorKind::InvalidRequest.failure_fact()
    }
}
fn invalid(property: DataSizeProperty, reason: DataSizeFailureReason) -> ConnectorError {
    ConnectorError::from_domain(
        ConnectorErrorKind::InvalidRequest,
        DataSizePropertyFailure { property, reason },
    )
}

/// SQL NULL remains NULL. DEFAULT/absence are separate application facts.
/// The original caller supplies its operation control; it is never replaced.
pub fn normalize_data_size_property(
    property: DataSizeProperty,
    value: &DdlPropertyValue,
    control: &dyn ConnectorOperationControl,
) -> Result<Option<NormalizedDataSize>, ConnectorError> {
    control.check_active()?;
    let text = match value {
        DdlPropertyValue::Null => return Ok(None),
        DdlPropertyValue::Varchar(text) => text,
        _ => return Err(invalid(property, DataSizeFailureReason::ExpectedVarchar)),
    };
    let bytes = parse(property, text, control)?;
    if let Some(reason) = data_size_range_failure(property, bytes) {
        return Err(invalid(property, DataSizeFailureReason::Range(reason)));
    }
    control.check_active()?;
    Ok(Some(NormalizedDataSize { property, bytes }))
}

/// Width bounds after parsing the declared data size into nonnegative bytes.
fn data_size_range_failure(property: DataSizeProperty, bytes: i64) -> Option<DataSizeRangeFailure> {
    if property != DataSizeProperty::ParquetWriterRowGroupSize {
        return None;
    }
    if bytes == 0 {
        Some(DataSizeRangeFailure::ZeroParquetRowGroup)
    } else if bytes > 2 * (1_i64 << 30) {
        Some(DataSizeRangeFailure::ParquetRowGroupAboveTwoGiB)
    } else if usize::try_from(bytes).is_err() {
        Some(DataSizeRangeFailure::ParquetRowGroupOutsideUsize)
    } else {
        None
    }
}

fn parse(
    property: DataSizeProperty,
    text: &str,
    control: &dyn ConnectorOperationControl,
) -> Result<i64, ConnectorError> {
    use DataSizeFailureReason::Parse;
    use DataSizeParseFailure as P;
    // The 1.13 fast path is selected by Java UTF-16 length and a final B.
    // Its Long.parseLong(char) behavior includes BMP decimal digits and -0.
    // Counting only through 21 decides this original fast-path condition.
    let mut units = 0;
    for ch in text.chars() {
        control.check_active()?;
        units += ch.len_utf16();
        if units > 20 {
            break;
        }
    }
    if (2..=20).contains(&units) && !text.starts_with('+') {
        if let Some(number) = text.strip_suffix('B') {
            if let Some(bytes) = java_byte_integer(number, control)? {
                return Ok(bytes);
            }
        }
    }
    // Exact Java regex: ^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$.
    // Neither Unicode whitespace nor exponent/leading-sign syntax is enabled.
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() && regex_space(bytes[i]) {
        control.check_active()?;
        i += 1;
    }
    let start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        control.check_active()?;
        i += 1;
    }
    if start == i {
        return Err(invalid(property, Parse(P::Syntax)));
    }
    let decimal = i < bytes.len() && bytes[i] == b'.';
    if decimal {
        control.check_active()?;
        i += 1;
        let fraction_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            control.check_active()?;
            i += 1;
        }
        if i == fraction_start {
            return Err(invalid(property, Parse(P::Syntax)));
        }
    }
    let number = &text[start..i];
    while i < bytes.len() && regex_space(bytes[i]) {
        control.check_active()?;
        i += 1;
    }
    let unit_start = i;
    while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
        control.check_active()?;
        i += 1;
    }
    let unit_end = i;
    while i < bytes.len() && regex_space(bytes[i]) {
        control.check_active()?;
        i += 1;
    }
    if unit_start == unit_end || i != bytes.len() {
        return Err(invalid(property, Parse(P::Syntax)));
    }
    let factor: i64 = match &text[unit_start..unit_end] {
        "B" => 1,
        "kB" => 1 << 10,
        "MB" => 1 << 20,
        "GB" => 1 << 30,
        "TB" => 1 << 40,
        "PB" => 1 << 50,
        "EB" => 1 << 60,
        _ => return Err(invalid(property, Parse(P::Unit))),
    };
    control.check_active()?;
    if !decimal {
        let number: i64 = number
            .parse()
            .map_err(|_| invalid(property, Parse(P::IntegerOverflow)))?;
        return number
            .checked_mul(factor)
            .ok_or_else(|| invalid(property, Parse(P::IntegerOverflow)));
    }
    // Keep the actual 1.13 IEEE operation sequence. Long.MAX_VALUE compares as
    // 2^63 in this double comparison; Java's narrowing conversion saturates.
    // Rust's saturating float-to-integer conversion has that same boundary.
    let size: f64 = number
        .parse()
        .map_err(|_| invalid(property, Parse(P::DecimalOverflow)))?;
    control.check_active()?;
    if !size.is_finite() {
        return Err(invalid(property, Parse(P::DecimalOverflow)));
    }
    let rounded = (size / (1.0 / factor as f64) + 0.5).floor();
    if rounded > i64::MAX as f64 {
        return Err(invalid(property, Parse(P::DecimalOverflow)));
    }
    Ok(rounded as i64)
}

fn regex_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | b'\x0c' | b'\r')
}

fn java_byte_integer(
    number: &str,
    control: &dyn ConnectorOperationControl,
) -> Result<Option<i64>, ConnectorError> {
    let (negative, digits) = match number.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, number),
    };
    if digits.is_empty() {
        return Ok(None);
    }
    let mut result = 0_i64;
    for ch in digits.chars() {
        control.check_active()?;
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
