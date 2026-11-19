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

//! D33 conversion of authored raw JSON text and ordinary VARCHAR values.
//! This is a value author inside the existing prepared CAST recipe. It owns
//! no row selection, Arrow result assembly, inherited errors or allocation grant.
use crate::KernelFailure;
use crate::kernel_control::internal;
use crate::kernel_input::EvaluationCheckpoints;
use arrow_schema::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::{cmp::Ordering, error::Error, fmt, ops::Range};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    JsonToVarchar,
    VarcharToJson,
}

pub(super) fn profile(source: &FunctionValueType, result: &FunctionValueType) -> Option<Operation> {
    if source.data_type() != &DataType::Utf8
        || result.data_type() != &DataType::Utf8
        || (source.nullable() && !result.nullable())
    {
        return None;
    }
    match (source.logical_type(), result.logical_type()) {
        (ValueLogicalType::Json, ValueLogicalType::Physical) if result.nullable() => {
            Some(Operation::JsonToVarchar)
        }
        (ValueLogicalType::Physical, ValueLogicalType::Json) => Some(Operation::VarcharToJson),
        _ => None,
    }
}

#[derive(Debug)]
pub(super) struct InvalidCast;
impl fmt::Display for InvalidCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Cannot cast JSON array element to VARCHAR")
    }
}
impl Error for InvalidCast {}
impl novarocks_failure_contract::FailureProjection for InvalidCast {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT,
        )
    }
}
#[derive(Debug)]
pub(super) enum InvalidJson {
    Syntax,
    NumberRange,
    NestingDepth,
    FloatParser(std::num::ParseFloatError),
}
impl fmt::Display for InvalidJson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax => f.write_str("Invalid raw JSON array element"),
            Self::NumberRange => f.write_str("Raw JSON number is outside the supported range"),
            Self::NestingDepth => f.write_str("Raw JSON nesting depth exceeds 1024"),
            Self::FloatParser(source) => write!(f, "Raw JSON number cannot be parsed: {source}"),
        }
    }
}
impl Error for InvalidJson {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FloatParser(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for InvalidJson {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
        )
    }
}
#[derive(Debug)]
pub(super) enum LeafError {
    Conversion(InvalidCast),
    Parse(InvalidJson),
    Outer(KernelFailure),
}
impl From<KernelFailure> for LeafError {
    fn from(value: KernelFailure) -> Self {
        Self::Outer(value)
    }
}
fn bad<T>() -> Result<T, LeafError> {
    Err(LeafError::Parse(InvalidJson::Syntax))
}
fn bad_number<T>() -> Result<T, LeafError> {
    Err(LeafError::Parse(InvalidJson::NumberRange))
}
fn extent(n: usize) -> Result<usize, LeafError> {
    if n > i32::MAX as usize {
        return Err(KernelFailure::ResourceExhausted.into());
    }
    Ok(n)
}
fn string_with_capacity(
    n: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, LeafError> {
    extent(n)?;
    work.flush()?;
    let mut out = String::new();
    out.try_reserve_exact(n)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(out)
}

/// The caller has already handled the actual source SQL NULL. A successful
/// None here is JSON null, not a conversion error inferred from a NULL value.
pub(super) fn convert_non_null_observed(
    op: Operation,
    raw: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<String>, LeafError> {
    work.step()?;
    match op {
        Operation::VarcharToJson => quote(raw, work).map(Some),
        Operation::JsonToVarchar => {
            let scalar = validate(raw, work)?;
            match scalar {
                Scalar::Null => Ok(None),
                Scalar::Boolean(v) => copy_text(if v { "true" } else { "false" }, work).map(Some),
                Scalar::String {
                    token,
                    decoded_bytes,
                } => decode_string(&raw[token], decoded_bytes, work).map(Some),
                Scalar::Number(range) => number_text(&raw[range], work).map(Some),
                Scalar::Container => Err(LeafError::Conversion(InvalidCast)),
            }
        }
    }
}
fn copy_text(value: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<String, LeafError> {
    let mut out = string_with_capacity(value.len(), work)?;
    for ch in value.chars() {
        work.step()?;
        out.push(ch);
    }
    Ok(out)
}

fn quote(value: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<String, LeafError> {
    // Exact escaped geometry, not a conservative six-times limit that can
    // reject a representable large value. Every original byte is observed.
    let mut len = 2_usize;
    for b in value.bytes() {
        work.step()?;
        len = len
            .checked_add(match b {
                b'"' | b'\\' | b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t' => 2,
                0..=31 => 6,
                _ => 1,
            })
            .ok_or(KernelFailure::ResourceExhausted)?;
    }
    let mut out = string_with_capacity(len, work)?;
    out.push('"');
    for ch in value.chars() {
        work.step()?;
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0'..='\x1f' => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                let b = ch as u8;
                out.push_str("\\u00");
                out.push(char::from(HEX[usize::from(b >> 4)]));
                out.push(char::from(HEX[usize::from(b & 15)]));
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
    Ok(out)
}

#[derive(Debug)]
enum Scalar {
    Null,
    Boolean(bool),
    String {
        token: Range<usize>,
        decoded_bytes: usize,
    },
    Number(Range<usize>),
    Container,
}
struct Reader<'a, 'w, 'c> {
    raw: &'a str,
    at: usize,
    work: &'w mut EvaluationCheckpoints<'c>,
}
impl Reader<'_, '_, '_> {
    fn byte(&self) -> Option<u8> {
        self.raw.as_bytes().get(self.at).copied()
    }
    fn space(&mut self) -> Result<(), LeafError> {
        while matches!(self.byte(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.work.step()?;
            self.at += 1;
        }
        Ok(())
    }
    fn require(&mut self, b: u8) -> Result<(), LeafError> {
        self.work.step()?;
        if self.byte() != Some(b) {
            return bad();
        }
        self.at += 1;
        Ok(())
    }
    fn literal(&mut self, text: &[u8]) -> Result<(), LeafError> {
        for &b in text {
            self.require(b)?;
        }
        Ok(())
    }
    fn hex_unit(&mut self) -> Result<u16, LeafError> {
        let mut value = 0;
        for _ in 0..4 {
            self.work.step()?;
            let Some(digit) = self.byte().and_then(|b| char::from(b).to_digit(16)) else {
                return bad();
            };
            value = value * 16 + digit as u16;
            self.at += 1;
        }
        Ok(value)
    }
    fn string_char(&mut self) -> Result<char, LeafError> {
        self.work.step()?;
        let Some(b) = self.byte() else {
            return bad();
        };
        if b < 32 {
            return bad();
        }
        if b != b'\\' {
            let Some(ch) = self.raw[self.at..].chars().next() else {
                return bad();
            };
            for _ in 1..ch.len_utf8() {
                self.work.step()?;
            }
            self.at += ch.len_utf8();
            return Ok(ch);
        }
        self.at += 1;
        let Some(escape) = self.byte() else {
            return bad();
        };
        self.work.step()?;
        self.at += 1;
        Ok(match escape {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\x08',
            b'f' => '\x0c',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => {
                let high = self.hex_unit()?;
                let code = if (0xd800..=0xdbff).contains(&high) {
                    self.require(b'\\')?;
                    self.require(b'u')?;
                    let low = self.hex_unit()?;
                    if !(0xdc00..=0xdfff).contains(&low) {
                        return bad();
                    }
                    0x10000 + ((u32::from(high) - 0xd800) << 10) + u32::from(low) - 0xdc00
                } else {
                    u32::from(high)
                };
                let Some(ch) = char::from_u32(code) else {
                    return bad();
                };
                ch
            }
            _ => return bad(),
        })
    }
    fn string_token(&mut self) -> Result<(Range<usize>, usize), LeafError> {
        let begin = self.at;
        self.require(b'"')?;
        let mut len = 0_usize;
        while self.byte() != Some(b'"') {
            let ch = self.string_char()?;
            len = len
                .checked_add(ch.len_utf8())
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        self.require(b'"')?;
        Ok((begin..self.at, len))
    }
    fn number_token(&mut self) -> Result<Range<usize>, LeafError> {
        let begin = self.at;
        if self.byte() == Some(b'-') {
            self.work.step()?;
            self.at += 1;
        }
        match self.byte() {
            Some(b'0') => {
                self.work.step()?;
                self.at += 1;
            }
            Some(b'1'..=b'9') => {
                while matches!(self.byte(), Some(b'0'..=b'9')) {
                    self.work.step()?;
                    self.at += 1;
                }
            }
            _ => return bad(),
        }
        if self.byte() == Some(b'.') {
            self.work.step()?;
            self.at += 1;
            let start = self.at;
            while matches!(self.byte(), Some(b'0'..=b'9')) {
                self.work.step()?;
                self.at += 1;
            }
            if self.at == start {
                return bad();
            }
        }
        if matches!(self.byte(), Some(b'e' | b'E')) {
            self.work.step()?;
            self.at += 1;
            if matches!(self.byte(), Some(b'+' | b'-')) {
                self.work.step()?;
                self.at += 1;
            }
            let start = self.at;
            while matches!(self.byte(), Some(b'0'..=b'9')) {
                self.work.step()?;
                self.at += 1;
            }
            if self.at == start {
                return bad();
            }
        }
        Ok(begin..self.at)
    }
}

// An explicit frame stack validates actual bytes and trailing tokens without
// recursive value trees. The depth is the pinned JsonItems raw-entry semantic
// constraint, independent of SQL input quotas and host allocation grants.
const JSON_MAX_NESTING_DEPTH: usize = 1024;
#[derive(Clone, Copy)]
enum Frame {
    ArrayFirst,
    ArrayNext,
    ObjectFirst,
    ObjectKey,
    ObjectColon,
    ObjectValue,
    ObjectNext,
}
fn push_frame(
    stack: &mut Vec<Frame>,
    frame: Frame,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), LeafError> {
    if stack.len() == JSON_MAX_NESTING_DEPTH {
        return Err(LeafError::Parse(InvalidJson::NestingDepth));
    }
    work.flush()?;
    stack
        .try_reserve(1)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    stack.push(frame);
    work.flush()?;
    Ok(())
}
fn validate(raw: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<Scalar, LeafError> {
    let mut reader = Reader { raw, at: 0, work };
    let mut stack = Vec::<Frame>::new();
    let mut root = None;
    let mut want_value = true;
    loop {
        reader.space()?;
        if want_value {
            let is_root = root.is_none();
            let value = match reader.byte() {
                Some(b'n') => {
                    reader.literal(b"null")?;
                    Scalar::Null
                }
                Some(b't') => {
                    reader.literal(b"true")?;
                    Scalar::Boolean(true)
                }
                Some(b'f') => {
                    reader.literal(b"false")?;
                    Scalar::Boolean(false)
                }
                Some(b'"') => {
                    let (token, decoded_bytes) = reader.string_token()?;
                    Scalar::String {
                        token,
                        decoded_bytes,
                    }
                }
                Some(b'-' | b'0'..=b'9') => {
                    let token = reader.number_token()?;
                    // Parsing builds every child before castToVarchar rejects a
                    // container. Child numeric parse errors retain code 7.
                    validate_number(&raw[token.clone()], reader.work)?;
                    Scalar::Number(token)
                }
                Some(b'[') => {
                    reader.require(b'[')?;
                    push_frame(&mut stack, Frame::ArrayFirst, reader.work)?;
                    Scalar::Container
                }
                Some(b'{') => {
                    reader.require(b'{')?;
                    push_frame(&mut stack, Frame::ObjectFirst, reader.work)?;
                    Scalar::Container
                }
                _ => return bad(),
            };
            if is_root {
                root = Some(value);
            }
            want_value = false;
            continue;
        }
        let Some(frame) = stack.last_mut() else {
            break;
        };
        match *frame {
            Frame::ArrayFirst => {
                if reader.byte() == Some(b']') {
                    reader.require(b']')?;
                    stack.pop();
                } else {
                    *frame = Frame::ArrayNext;
                    want_value = true;
                }
            }
            Frame::ArrayNext => match reader.byte() {
                Some(b']') => {
                    reader.require(b']')?;
                    stack.pop();
                }
                Some(b',') => {
                    reader.require(b',')?;
                    want_value = true;
                }
                _ => return bad(),
            },
            Frame::ObjectFirst | Frame::ObjectKey => {
                if matches!(*frame, Frame::ObjectFirst) && reader.byte() == Some(b'}') {
                    reader.require(b'}')?;
                    stack.pop();
                } else {
                    reader.string_token()?;
                    *frame = Frame::ObjectColon;
                }
            }
            Frame::ObjectColon => {
                reader.require(b':')?;
                *frame = Frame::ObjectValue;
            }
            Frame::ObjectValue => {
                *frame = Frame::ObjectNext;
                want_value = true;
            }
            Frame::ObjectNext => match reader.byte() {
                Some(b'}') => {
                    reader.require(b'}')?;
                    stack.pop();
                }
                Some(b',') => {
                    reader.require(b',')?;
                    *frame = Frame::ObjectKey;
                }
                _ => return bad(),
            },
        }
    }
    reader.space()?;
    if reader.at != raw.len() {
        return bad();
    }
    root.ok_or(LeafError::Parse(InvalidJson::Syntax))
}
fn decode_string(
    raw: &str,
    decoded_bytes: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, LeafError> {
    let mut out = string_with_capacity(decoded_bytes, work)?;
    let mut reader = Reader { raw, at: 0, work };
    reader.require(b'"')?;
    while reader.byte() != Some(b'"') {
        out.push(reader.string_char()?);
    }
    reader.require(b'"')?;
    Ok(out)
}

struct NumberFacts<'a> {
    negative: bool,
    coefficient: &'a str,
    exponent: Option<&'a str>,
    fractional: usize,
    significant: usize,
    zero: bool,
}
fn number_facts<'a>(
    raw: &'a str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<NumberFacts<'a>, LeafError> {
    let negative = raw.starts_with('-');
    let unsigned = if negative { &raw[1..] } else { raw };
    let mut end = unsigned.len();
    for (i, b) in unsigned.bytes().enumerate() {
        work.step()?;
        if matches!(b, b'e' | b'E') {
            end = i;
            break;
        }
    }
    let coefficient = &unsigned[..end];
    let exponent = (end < unsigned.len()).then(|| &unsigned[end + 1..]);
    let mut after_point = false;
    let mut fractional = 0_usize;
    let mut significant = 0_usize;
    for b in coefficient.bytes() {
        work.step()?;
        if b == b'.' {
            after_point = true;
            continue;
        }
        if after_point {
            fractional = fractional
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        if significant != 0 || b != b'0' {
            significant = significant
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
    }
    Ok(NumberFacts {
        negative,
        coefficient,
        exponent,
        fractional,
        significant,
        zero: significant == 0,
    })
}
// JsonItems promotes exact values outside DECIMAL(38) to TrinoNumber:
// strip trailing zeros, round HALF_UP to 200 significant digits, strip again,
// then encode the signed 15-bit scale. This fixed decimal scratch is a property
// of the frozen NUMBER representation, not a limit on original input text.
const NUMBER_PRECISION: usize = 200;
struct Number {
    digits: [u8; NUMBER_PRECISION],
    len: usize,
    scale: i128,
    negative: bool,
}
fn promoted_number(
    facts: &NumberFacts<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Number, LeafError> {
    let mut out = Number {
        digits: [0; NUMBER_PRECISION],
        len: 0,
        scale: 0,
        negative: facts.negative && !facts.zero,
    };
    if facts.zero {
        out.digits[0] = b'0';
        out.len = 1;
        return Ok(out);
    }
    let mut trailing = 0_usize;
    let mut seen = 0_usize;
    let mut round_up = false;
    for b in facts.coefficient.bytes() {
        work.step()?;
        if b == b'.' || (seen == 0 && b == b'0') {
            continue;
        }
        trailing = if b == b'0' { trailing + 1 } else { 0 };
        if seen < NUMBER_PRECISION {
            out.digits[seen] = b;
        } else if seen == NUMBER_PRECISION {
            round_up = b >= b'5';
        }
        seen += 1;
    }
    let precision = seen - trailing;
    out.len = precision.min(NUMBER_PRECISION);
    out.scale = i128::try_from(facts.fractional).map_err(|_| KernelFailure::ResourceExhausted)?
        - i128::try_from(trailing).map_err(|_| KernelFailure::ResourceExhausted)?;
    if precision > NUMBER_PRECISION {
        out.scale -= i128::try_from(precision - NUMBER_PRECISION)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        if round_up {
            let mut at = out.len;
            while at > 0 && out.digits[at - 1] == b'9' {
                work.step()?;
                out.digits[at - 1] = b'0';
                at -= 1;
            }
            if at == 0 {
                // Carry 999...9 to 1 followed by 200 zeros, then normalize.
                out.digits[0] = b'1';
                out.len = 1;
                out.scale -= NUMBER_PRECISION as i128;
            } else {
                out.digits[at - 1] += 1;
            }
        }
        while out.len > 1 && out.digits[out.len - 1] == b'0' {
            work.step()?;
            out.len -= 1;
            out.scale -= 1;
        }
    }
    if !(-16384..=16383).contains(&out.scale) {
        return bad_number();
    }
    Ok(out)
}
fn decimal_profile(facts: &NumberFacts<'_>) -> bool {
    facts.significant.max(1) <= 38 && facts.fractional <= 38
}
fn validate_number(raw: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<(), LeafError> {
    let facts = number_facts(raw, work)?;
    if facts.exponent.is_some() {
        parse_double(&facts, work)?;
    } else if !decimal_profile(&facts) {
        promoted_number(&facts, work)?;
    }
    Ok(())
}
fn number_text(raw: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<String, LeafError> {
    let facts = number_facts(raw, work)?;
    if facts.exponent.is_some() {
        return double_text(parse_double(&facts, work)?, work);
    }
    if decimal_profile(&facts) {
        // DECIMAL retains its authored scale and loses only the sign of zero.
        return copy_text(
            if facts.negative && facts.zero {
                &raw[1..]
            } else {
                raw
            },
            work,
        );
    }
    let number = promoted_number(&facts, work)?;
    let adjusted = number.len as i128 - number.scale - 1;
    if number.scale < 0 || adjusted < -6 {
        let exponent = adjusted.to_string();
        let length = number.len
            + usize::from(number.len > 1)
            + 1
            + exponent.len()
            + usize::from(adjusted >= 0)
            + usize::from(number.negative);
        let mut out = string_with_capacity(length, work)?;
        if number.negative {
            out.push('-');
        }
        for (at, b) in number.digits[..number.len].iter().enumerate() {
            work.step()?;
            if at == 1 {
                out.push('.');
            }
            out.push(char::from(*b));
        }
        out.push('E');
        if adjusted >= 0 {
            out.push('+');
        }
        out.push_str(&exponent);
        return Ok(out);
    }
    let scale = usize::try_from(number.scale).map_err(|_| KernelFailure::ResourceExhausted)?;
    let length = number.len.max(scale + 1) + usize::from(scale > 0) + usize::from(number.negative);
    let mut out = string_with_capacity(length, work)?;
    if number.negative {
        out.push('-');
    }
    if scale >= number.len {
        out.push_str("0.");
        for _ in 0..scale - number.len {
            work.step()?;
            out.push('0');
        }
    }
    for (at, b) in number.digits[..number.len].iter().enumerate() {
        work.step()?;
        if scale < number.len && scale > 0 && at == number.len - scale {
            out.push('.');
        }
        out.push(char::from(*b));
    }
    Ok(out)
}

// Every binary64 rounding midpoint has at most 1075 fractional decimal
// places and 309 integer places. Retaining more significant digits than
// their combined bound, plus a sticky digit, preserves ordering against all
// such midpoints. This bounds the opaque standard float parser, not accepted
// user text: every remaining original digit is still observed and classified.
const DOUBLE_PARSE_DIGITS: usize = 1075 + 309 + 2;
fn parse_double(
    facts: &NumberFacts<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<f64, LeafError> {
    if facts.zero {
        return Ok(if facts.negative { -0.0 } else { 0.0 });
    }
    let exponent = facts.exponent.expect("exponent profile");
    let exp_negative = exponent.starts_with('-');
    let digits = exponent.trim_start_matches(['-', '+']);
    let mut magnitude = Some(0_u128);
    for b in digits.bytes() {
        work.step()?;
        magnitude = magnitude
            .and_then(|n| n.checked_mul(10))
            .and_then(|n| n.checked_add(u128::from(b - b'0')));
    }
    let Some(magnitude) = magnitude else {
        return if exp_negative {
            Ok(if facts.negative { -0.0 } else { 0.0 })
        } else {
            bad_number()
        };
    };
    let Ok(exponent) = i128::try_from(magnitude) else {
        return if exp_negative {
            Ok(if facts.negative { -0.0 } else { 0.0 })
        } else {
            bad_number()
        };
    };
    let exponent = if exp_negative { -exponent } else { exponent };
    let fractional =
        i128::try_from(facts.fractional).map_err(|_| KernelFailure::ResourceExhausted)?;
    let significant =
        i128::try_from(facts.significant).map_err(|_| KernelFailure::ResourceExhausted)?;
    let Some(order) = exponent
        .checked_sub(fractional)
        .and_then(|n| n.checked_add(significant - 1))
    else {
        return if exp_negative {
            Ok(if facts.negative { -0.0 } else { 0.0 })
        } else {
            bad_number()
        };
    };
    if order < -324 {
        return Ok(if facts.negative { -0.0 } else { 0.0 });
    }
    if order > 308 {
        return bad_number();
    }
    let retain = facts.significant.min(DOUBLE_PARSE_DIGITS);
    let mut normalized = string_with_capacity(retain + 32, work)?;
    if facts.negative {
        normalized.push('-');
    }
    let mut seen = 0_usize;
    let mut sticky = false;
    for b in facts.coefficient.bytes() {
        work.step()?;
        if b == b'.' || (seen == 0 && b == b'0') {
            continue;
        }
        if seen < retain {
            normalized.push(char::from(b));
        } else {
            sticky |= b != b'0';
        }
        seen += 1;
    }
    let mut power =
        order - i128::try_from(retain).map_err(|_| KernelFailure::ResourceExhausted)? + 1;
    if sticky {
        normalized.push('1');
        power -= 1;
    }
    normalized.push('e');
    normalized.push_str(&power.to_string());
    work.flush()?;
    let result = normalized.parse::<f64>();
    work.flush()?;
    match result {
        Ok(value) if value.is_finite() => Ok(value),
        Ok(_) => bad_number(),
        Err(source) => Err(LeafError::Parse(InvalidJson::FloatParser(source))),
    }
}

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
    ) -> Result<Ordering, LeafError> {
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
    ) -> Result<(), LeafError> {
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
                )
                .into());
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
    ) -> Result<(), LeafError> {
        if self.used == 0 {
            return Ok(());
        }
        let whole = bits / 32;
        let part = bits % 32;
        if self.used + whole + usize::from(part != 0) > self.words.len() {
            return Err(internal("finite float shift exceeded its mathematical bound").into());
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
    ) -> Result<(), LeafError> {
        let mut borrow = 0_u64;
        for i in 0..self.used {
            work.step()?;
            let sub = u64::from(if i < other.used { other.words[i] } else { 0 }) + borrow;
            let value = u64::from(self.words[i]);
            self.words[i] = value.wrapping_sub(sub) as u32;
            borrow = u64::from(value < sub);
        }
        if borrow != 0 {
            return Err(internal("float decimal subtraction violated its ordering").into());
        }
        while self.used != 0 && self.words[self.used - 1] == 0 {
            work.step()?;
            self.used -= 1;
        }
        Ok(())
    }
    fn add(&self, other: &Self, work: &mut EvaluationCheckpoints<'_>) -> Result<Self, LeafError> {
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
                return Err(
                    internal("finite float addition exceeded its mathematical bound").into(),
                );
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
    ) -> Result<(), LeafError> {
        for _ in 0..power {
            self.multiply(10, work)?;
        }
        Ok(())
    }
    fn quotient_digit(
        &mut self,
        denominator: &Self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<u8, LeafError> {
        let mut digit = 0;
        while self.compare(denominator, work)? != Ordering::Less {
            work.step()?;
            self.subtract(denominator, work)?;
            digit += 1;
            if digit > 9 {
                return Err(internal("float decimal quotient exceeded one digit").into());
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

fn double_text(value: f64, work: &mut EvaluationCheckpoints<'_>) -> Result<String, LeafError> {
    if !value.is_finite() {
        return bad_number();
    }
    if value == 0.0 {
        return copy_text(
            if value.is_sign_negative() {
                "-0E0"
            } else {
                "0E0"
            },
            work,
        );
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
                if round_up {
                    let mut at = count;
                    loop {
                        if at == 0 {
                            digits[0] = 1;
                            decimal_exp += 1;
                            break;
                        }
                        at -= 1;
                        if digits[at] != 9 {
                            digits[at] += 1;
                            break;
                        }
                        digits[at] = 0;
                    }
                }
                break;
            }
            if count == digits.len() {
                return Err(internal("finite float decimal digit bound was violated").into());
            }
        }
    }
    while count > 1 && digits[count - 1] == 0 {
        count -= 1;
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

#[cfg(test)]
#[path = "array_json_tests.rs"]
mod tests;
