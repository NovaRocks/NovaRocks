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

//! Shared JSON text lexemes and bounded exact numeric parsing.
use crate::KernelFailure;
use crate::kernel_input::EvaluationCheckpoints;
#[cfg(test)]
use arrow_schema::DataType;
#[cfg(test)]
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::{error::Error, fmt, ops::Range};

#[path = "array_json_structure.rs"]
pub(super) mod structure;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(super) enum Operation {
    JsonToVarchar,
    VarcharToJson,
}

#[derive(Debug)]
#[cfg(test)]
pub(crate) struct InvalidCast;
#[cfg(test)]
impl fmt::Display for InvalidCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Cannot cast JSON array element to VARCHAR")
    }
}
#[cfg(test)]
impl Error for InvalidCast {}
#[cfg(test)]
impl novarocks_failure_contract::FailureProjection for InvalidCast {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT,
        )
    }
}
#[derive(Debug)]
pub(crate) enum InvalidJson {
    Syntax,
    NumberRange,
    #[cfg(test)]
    NestingDepth,
    FloatParser(std::num::ParseFloatError),
}
impl fmt::Display for InvalidJson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax => f.write_str("Invalid raw JSON array element"),
            Self::NumberRange => f.write_str("Raw JSON number is outside the supported range"),
            #[cfg(test)]
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
pub(crate) enum LeafError {
    #[cfg(test)]
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
#[cfg(test)]
fn extent(n: usize) -> Result<usize, LeafError> {
    if n > i32::MAX as usize {
        return Err(KernelFailure::ResourceExhausted.into());
    }
    Ok(n)
}
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(test)]
fn copy_text(value: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<String, LeafError> {
    let mut out = string_with_capacity(value.len(), work)?;
    for ch in value.chars() {
        work.step()?;
        out.push(ch);
    }
    Ok(out)
}

#[cfg(test)]
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
#[cfg(test)]
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
pub(crate) trait LexWork {
    fn step(&mut self) -> Result<(), KernelFailure>;
    fn flush(&mut self) -> Result<(), KernelFailure>;
}

impl LexWork for EvaluationCheckpoints<'_> {
    fn step(&mut self) -> Result<(), KernelFailure> {
        EvaluationCheckpoints::step(self)
    }
    fn flush(&mut self) -> Result<(), KernelFailure> {
        EvaluationCheckpoints::flush(self)
    }
}

pub(crate) struct Reader<'a, 'w> {
    pub(crate) raw: &'a str,
    pub(crate) at: usize,
    pub(crate) work: &'w mut dyn LexWork,
}
impl Reader<'_, '_> {
    pub(crate) fn byte(&self) -> Option<u8> {
        self.raw.as_bytes().get(self.at).copied()
    }
    pub(crate) fn space(&mut self) -> Result<(), LeafError> {
        while matches!(self.byte(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.work.step()?;
            self.at += 1;
        }
        Ok(())
    }
    pub(crate) fn require(&mut self, b: u8) -> Result<(), LeafError> {
        self.work.step()?;
        if self.byte() != Some(b) {
            return bad();
        }
        self.at += 1;
        Ok(())
    }
    pub(crate) fn literal(&mut self, text: &[u8]) -> Result<(), LeafError> {
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
    pub(crate) fn string_char(&mut self) -> Result<char, LeafError> {
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
    pub(crate) fn string_token(&mut self) -> Result<(Range<usize>, usize), LeafError> {
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
    pub(crate) fn number_token(&mut self) -> Result<Range<usize>, LeafError> {
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
#[cfg(test)]
const JSON_MAX_NESTING_DEPTH: usize = 1024;
#[derive(Clone, Copy)]
#[cfg(test)]
enum Frame {
    ArrayFirst,
    ArrayNext,
    ObjectFirst,
    ObjectKey,
    ObjectColon,
    ObjectValue,
    ObjectNext,
}
#[cfg(test)]
fn push_frame(
    stack: &mut Vec<Frame>,
    frame: Frame,
    work: &mut dyn LexWork,
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
#[cfg(test)]
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
#[cfg(test)]
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
fn number_facts<'a>(raw: &'a str, work: &mut dyn LexWork) -> Result<NumberFacts<'a>, LeafError> {
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
fn promoted_number(facts: &NumberFacts<'_>, work: &mut dyn LexWork) -> Result<Number, LeafError> {
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
#[cfg(test)]
fn validate_number(raw: &str, work: &mut dyn LexWork) -> Result<(), LeafError> {
    let facts = number_facts(raw, work)?;
    if facts.exponent.is_some() {
        parse_double(&facts, work)?;
    } else if !decimal_profile(&facts) {
        promoted_number(&facts, work)?;
    }
    Ok(())
}
#[cfg(test)]
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
fn parse_double(facts: &NumberFacts<'_>, work: &mut dyn LexWork) -> Result<f64, LeafError> {
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
    let mut normalized = DoubleLexeme::new();
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
    std::fmt::Write::write_fmt(&mut normalized, format_args!("{power}"))
        .expect("bounded scientific exponent");
    work.flush()?;
    let result = normalized.as_str().parse::<f64>();
    work.flush()?;
    match result {
        Ok(value) if value.is_finite() => Ok(value),
        Ok(_) => bad_number(),
        Err(source) => Err(LeafError::Parse(InvalidJson::FloatParser(source))),
    }
}

struct DoubleLexeme {
    bytes: [u8; DOUBLE_PARSE_DIGITS + 32],
    len: usize,
}

impl DoubleLexeme {
    fn new() -> Self {
        Self {
            bytes: [0; DOUBLE_PARSE_DIGITS + 32],
            len: 0,
        }
    }

    fn push(&mut self, value: char) {
        debug_assert!(value.is_ascii());
        self.bytes[self.len] = value as u8;
        self.len += 1;
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len]).expect("ASCII scientific notation")
    }
}

impl fmt::Write for DoubleLexeme {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        for ch in value.chars() {
            self.push(ch);
        }
        Ok(())
    }
}

pub(crate) enum LexNumber {
    Integer(i32),
    BigInt(i64),
    Double(f64),
    Decimal {
        precision: u8,
        scale: u8,
        coefficient: i128,
    },
    Number {
        scale: i32,
        negative: bool,
        digits: [u8; NUMBER_PRECISION],
        len: usize,
    },
}

pub(crate) fn lex_number(raw: &str, work: &mut dyn LexWork) -> Result<LexNumber, LeafError> {
    let facts = number_facts(raw, work)?;
    if facts.exponent.is_some() {
        return Ok(LexNumber::Double(parse_double(&facts, work)?));
    }
    let integer = !facts.coefficient.contains('.');
    if integer
        && facts.significant <= 19
        && let Ok(value) = raw.parse::<i64>()
    {
        return Ok(match i32::try_from(value) {
            Ok(value) => LexNumber::Integer(value),
            Err(_) => LexNumber::BigInt(value),
        });
    }
    if decimal_profile(&facts) {
        let mut coefficient = 0;
        for byte in facts.coefficient.bytes().filter(u8::is_ascii_digit) {
            work.step()?;
            coefficient = coefficient * 10 + i128::from(byte - b'0');
        }
        return Ok(LexNumber::Decimal {
            precision: facts.significant.max(facts.fractional).max(1) as u8,
            scale: facts.fractional as u8,
            coefficient: if facts.negative {
                -coefficient
            } else {
                coefficient
            },
        });
    }
    let value = promoted_number(&facts, work)?;
    Ok(LexNumber::Number {
        scale: i32::try_from(value.scale).map_err(|_| KernelFailure::ResourceExhausted)?,
        negative: value.negative,
        digits: value.digits,
        len: value.len,
    })
}

#[cfg(test)]
fn double_text(value: f64, work: &mut EvaluationCheckpoints<'_>) -> Result<String, LeafError> {
    if !value.is_finite() {
        return bad_number();
    }
    Ok(super::trino_float_text::format_double(value, work)?)
}

#[cfg(test)]
#[path = "array_json_tests.rs"]
mod tests;

pub(crate) fn cast_float_double(
    value: f64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, KernelFailure> {
    super::trino_float_text::format_double(value, work)
}

pub(crate) fn cast_float_real(
    value: f32,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, KernelFailure> {
    super::trino_float_text::format_real(value, work)
}
