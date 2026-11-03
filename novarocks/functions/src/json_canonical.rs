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
//! Trino-compatible JSON comparison keys. One iterative, host-funded author.
//! Strings are decoded UTF-16 units so isolated surrogates and Java key order
//! survive without being forced through Rust's Unicode-scalar String model.

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::{AggregateStateAllocator, KernelEvaluationControl, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use std::{cmp::Ordering, fmt, num::NonZeroU16, ops::Range, sync::Arc};

pub const FUNCTION_NAME: &str = "__json_canonical_key";
pub const FUNCTION_ID: &str = "builtin.scalar/__json_canonical_key/v1";

#[derive(Debug)]
pub struct JsonCanonicalKey {
    bytes: HostVec<u8, HostAggregateAllocator>,
}
impl JsonCanonicalKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn as_str(&self) -> &str {
        // All original scalar characters are UTF8, and surrogate units are
        // emitted as ASCII escapes by this same writer.
        std::str::from_utf8(&self.bytes).expect("canonical JSON writer emits UTF8")
    }
}
#[derive(Debug)]
pub struct InvalidJsonKey {
    pub offset: usize,
}
impl fmt::Display for InvalidJsonKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid JSON comparison value at byte {}", self.offset)
    }
}
impl std::error::Error for InvalidJsonKey {}
impl novarocks_failure_contract::FailureProjection for InvalidJsonKey {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(419).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for InvalidJsonKey {}
#[derive(Debug)]
pub struct JsonKeyOutputExtent {
    pub bytes: usize,
}
impl fmt::Display for JsonKeyOutputExtent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "canonical JSON output requires {} bytes beyond Utf8's i32 offset representation",
            self.bytes
        )
    }
}
impl std::error::Error for JsonKeyOutputExtent {}
impl novarocks_failure_contract::FailureProjection for JsonKeyOutputExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(420).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for JsonKeyOutputExtent {}
fn invalid(offset: usize) -> KernelFailure {
    KernelFailure::from_domain(InvalidJsonKey { offset })
}

fn push<T>(v: &mut HostVec<T, HostAggregateAllocator>, value: T) -> Result<(), KernelFailure> {
    v.try_reserve(1).map_err(|_| v.allocator().take_failure())?;
    v.push(value);
    Ok(())
}
fn extend(v: &mut HostVec<u8, HostAggregateAllocator>, bytes: &[u8]) -> Result<(), KernelFailure> {
    v.try_reserve(bytes.len())
        .map_err(|_| v.allocator().take_failure())?;
    v.extend_from_slice(bytes);
    Ok(())
}
const NONE: usize = usize::MAX;
#[derive(Clone, Copy)]
enum Kind {
    Null,
    True,
    False,
    String,
    Number,
    Array,
    Object,
}
struct Node {
    kind: Kind,
    text: Range<usize>,
    key: Range<usize>,
    first: usize,
    next: usize,
    ordered: Range<usize>,
}
struct Frame {
    node: usize,
    last: usize,
    state: u8,
    key: Range<usize>,
}
struct Parser<'a> {
    input: &'a str,
    pos: usize,
    nodes: HostVec<Node, HostAggregateAllocator>,
    strings: HostVec<u16, HostAggregateAllocator>,
    frames: HostVec<Frame, HostAggregateAllocator>,
    control: &'a dyn KernelEvaluationControl,
}
impl Parser<'_> {
    fn work(&self) -> Result<(), KernelFailure> {
        self.control.checkpoint(1)
    }
    fn byte(&self) -> Option<u8> {
        self.input.as_bytes().get(self.pos).copied()
    }
    fn space(&mut self) -> Result<(), KernelFailure> {
        while matches!(self.byte(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.work()?;
            self.pos += 1;
        }
        Ok(())
    }
    fn take(&mut self, wanted: u8) -> Result<(), KernelFailure> {
        self.work()?;
        if self.byte() != Some(wanted) {
            return Err(invalid(self.pos));
        }
        self.pos += 1;
        Ok(())
    }
    fn string(&mut self) -> Result<Range<usize>, KernelFailure> {
        self.take(b'"')?;
        let start = self.strings.len();
        loop {
            self.work()?;
            let b = self.byte().ok_or_else(|| invalid(self.pos))?;
            if b == b'"' {
                self.pos += 1;
                return Ok(start..self.strings.len());
            }
            if b < 32 {
                return Err(invalid(self.pos));
            }
            if b == b'\\' {
                self.pos += 1;
                let escape = self.byte().ok_or_else(|| invalid(self.pos))?;
                self.pos += 1;
                let unit = match escape {
                    b'"' => 34,
                    b'\\' => 92,
                    b'/' => 47,
                    b'b' => 8,
                    b'f' => 12,
                    b'n' => 10,
                    b'r' => 13,
                    b't' => 9,
                    b'u' => {
                        let mut value = 0u16;
                        for _ in 0..4 {
                            self.work()?;
                            let digit = self
                                .byte()
                                .and_then(|v| char::from(v).to_digit(16))
                                .ok_or_else(|| invalid(self.pos))?;
                            self.pos += 1;
                            value = value * 16 + digit as u16;
                        }
                        value
                    }
                    _ => return Err(invalid(self.pos - 1)),
                };
                push(&mut self.strings, unit)?;
            } else {
                let ch = self.input[self.pos..]
                    .chars()
                    .next()
                    .ok_or_else(|| invalid(self.pos))?;
                self.pos += ch.len_utf8();
                let mut pair = [0; 2];
                for &unit in ch.encode_utf16(&mut pair).iter() {
                    push(&mut self.strings, unit)?;
                }
            }
        }
    }
    fn number(&mut self) -> Result<Range<usize>, KernelFailure> {
        let start = self.pos;
        if self.byte() == Some(b'-') {
            self.pos += 1;
        }
        match self.byte() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while matches!(self.byte(), Some(b'0'..=b'9')) {
                    self.work()?;
                    self.pos += 1;
                }
            }
            _ => return Err(invalid(self.pos)),
        }
        if self.byte() == Some(b'.') {
            self.pos += 1;
            let begin = self.pos;
            while matches!(self.byte(), Some(b'0'..=b'9')) {
                self.work()?;
                self.pos += 1;
            }
            if begin == self.pos {
                return Err(invalid(self.pos));
            }
        }
        if matches!(self.byte(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.byte(), Some(b'-' | b'+')) {
                self.pos += 1;
            }
            let begin = self.pos;
            while matches!(self.byte(), Some(b'0'..=b'9')) {
                self.work()?;
                self.pos += 1;
            }
            if begin == self.pos {
                return Err(invalid(self.pos));
            }
        }
        // Validate every parsed value before object duplicate-key reduction.
        // A later key cannot hide an earlier unrepresentable BigDecimal.
        number_representation(&self.input[start..self.pos], start, self.control)?;
        Ok(start..self.pos)
    }
    fn value(&mut self, key: Range<usize>) -> Result<usize, KernelFailure> {
        self.space()?;
        self.work()?;
        let mut text = 0..0;
        let kind = match self.byte() {
            Some(b'"') => {
                text = self.string()?;
                Kind::String
            }
            Some(b'[') => {
                self.pos += 1;
                Kind::Array
            }
            Some(b'{') => {
                self.pos += 1;
                Kind::Object
            }
            Some(b'-' | b'0'..=b'9') => {
                text = self.number()?;
                Kind::Number
            }
            Some(b'n') => {
                self.literal(b"null")?;
                Kind::Null
            }
            Some(b't') => {
                self.literal(b"true")?;
                Kind::True
            }
            Some(b'f') => {
                self.literal(b"false")?;
                Kind::False
            }
            _ => return Err(invalid(self.pos)),
        };
        let id = self.nodes.len();
        push(
            &mut self.nodes,
            Node {
                kind,
                text,
                key,
                first: NONE,
                next: NONE,
                ordered: 0..0,
            },
        )?;
        if matches!(kind, Kind::Array | Kind::Object) {
            push(
                &mut self.frames,
                Frame {
                    node: id,
                    last: NONE,
                    state: 0,
                    key: 0..0,
                },
            )?;
        }
        Ok(id)
    }
    fn literal(&mut self, literal: &[u8]) -> Result<(), KernelFailure> {
        for &b in literal {
            self.take(b)?;
        }
        Ok(())
    }
    fn parse(&mut self) -> Result<(), KernelFailure> {
        self.value(0..0)?;
        while !self.frames.is_empty() {
            self.space()?;
            self.work()?;
            let depth = self.frames.len() - 1;
            let node = self.frames[depth].node;
            let object = matches!(self.nodes[node].kind, Kind::Object);
            let close = if object { b'}' } else { b']' };
            match self.frames[depth].state {
                0 | 4 => {
                    // Only the initial state permits an empty container.
                    if self.frames[depth].state == 0 && self.byte() == Some(close) {
                        self.pos += 1;
                        self.frames.pop();
                        continue;
                    }
                    if object {
                        let key = self.string()?;
                        self.space()?;
                        self.take(b':')?;
                        self.frames[depth].key = key;
                    }
                    let key = self.frames[depth].key.clone();
                    self.frames[depth].state = 1;
                    let child = self.value(key)?;
                    let previous = self.frames[depth].last;
                    if previous == NONE {
                        self.nodes[node].first = child;
                    } else {
                        self.nodes[previous].next = child;
                    }
                    self.frames[depth].last = child;
                }
                1 => match self.byte() {
                    Some(b',') => {
                        self.pos += 1;
                        self.frames[depth].state = 4;
                    }
                    Some(b) if b == close => {
                        self.pos += 1;
                        self.frames.pop();
                    }
                    _ => return Err(invalid(self.pos)),
                },
                _ => unreachable!("closed parser states"),
            }
        }
        self.space()?;
        if self.pos != self.input.len() {
            return Err(invalid(self.pos));
        }
        Ok(())
    }
}

fn compare_keys(
    a: usize,
    b: usize,
    nodes: &[Node],
    strings: &[u16],
    control: &dyn KernelEvaluationControl,
) -> Result<Ordering, KernelFailure> {
    let lhs = &strings[nodes[a].key.clone()];
    let rhs = &strings[nodes[b].key.clone()];
    for (&l, &r) in lhs.iter().zip(rhs) {
        control.checkpoint(1)?;
        if l != r {
            return Ok(l.cmp(&r));
        }
    }
    Ok(lhs.len().cmp(&rhs.len()).then_with(|| a.cmp(&b)))
}
fn same_key(
    a: usize,
    b: usize,
    nodes: &[Node],
    strings: &[u16],
    control: &dyn KernelEvaluationControl,
) -> Result<bool, KernelFailure> {
    let lhs = &strings[nodes[a].key.clone()];
    let rhs = &strings[nodes[b].key.clone()];
    if lhs.len() != rhs.len() {
        return Ok(false);
    }
    for (&left, &right) in lhs.iter().zip(rhs) {
        control.checkpoint(1)?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}
fn sort_members(
    values: &mut [usize],
    nodes: &[Node],
    strings: &[u16],
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    // In-place heapsort needs no library scratch and every comparison observes
    // the original cancellation/work owner, including very long object names.
    fn sift(
        values: &mut [usize],
        mut parent: usize,
        end: usize,
        nodes: &[Node],
        strings: &[u16],
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        while let Some(mut child) = parent
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .filter(|&n| n < end)
        {
            control.checkpoint(1)?;
            if child + 1 < end
                && compare_keys(values[child], values[child + 1], nodes, strings, control)?
                    == Ordering::Less
            {
                child += 1;
            }
            if compare_keys(values[parent], values[child], nodes, strings, control)?
                != Ordering::Less
            {
                break;
            }
            values.swap(parent, child);
            parent = child;
        }
        Ok(())
    }
    for start in (0..values.len() / 2).rev() {
        sift(values, start, values.len(), nodes, strings, control)?;
    }
    for end in (1..values.len()).rev() {
        values.swap(0, end);
        sift(values, 0, end, nodes, strings, control)?;
    }
    Ok(())
}
fn write_string(
    out: &mut HostVec<u8, HostAggregateAllocator>,
    units: &[u16],
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    push(out, b'"')?;
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &u in units {
        control.checkpoint(1)?;
        match u {
            34 => extend(out, b"\\\"")?,
            92 => extend(out, b"\\\\")?,
            8 => extend(out, b"\\b")?,
            9 => extend(out, b"\\t")?,
            10 => extend(out, b"\\n")?,
            12 => extend(out, b"\\f")?,
            13 => extend(out, b"\\r")?,
            0..=31 | 0xD800..=0xDFFF => {
                extend(
                    out,
                    &[
                        b'\\',
                        b'u',
                        HEX[(u >> 12) as usize],
                        HEX[((u >> 8) & 15) as usize],
                        HEX[((u >> 4) & 15) as usize],
                        HEX[(u & 15) as usize],
                    ],
                )?;
            }
            _ => {
                let mut bytes = [0; 3];
                let ch = char::from_u32(u32::from(u)).expect("non-surrogate BMP unit");
                extend(out, ch.encode_utf8(&mut bytes).as_bytes())?;
            }
        }
    }
    push(out, b'"')
}

struct NumberRepresentation {
    coefficient_end: usize,
    scale: Option<i64>,
}
/// One numeric representation author shared by parsing and writing. Parsing
/// invokes it even for values later overwritten by a duplicate object key.
fn number_representation(
    raw: &str,
    source_offset: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<NumberRepresentation, KernelFailure> {
    let mut fractional = 0usize;
    let mut after_point = false;
    let mut coefficient_end = raw.len();
    let mut exp_start = raw.len();
    for (index, b) in raw.bytes().enumerate() {
        control.checkpoint(1)?;
        match b {
            b'.' => after_point = true,
            b'e' | b'E' => {
                coefficient_end = index;
                exp_start = index + 1;
                break;
            }
            b'0'..=b'9' if after_point => fractional += 1,
            _ => (),
        }
    }
    if !after_point && exp_start == raw.len() {
        return Ok(NumberRepresentation {
            coefficient_end,
            scale: None,
        });
    }
    let mut exponent = 0i64;
    if exp_start < raw.len() {
        let exp = &raw[exp_start..];
        let neg = exp.starts_with('-');
        for b in exp.bytes().filter(|b| !matches!(b, b'+' | b'-')) {
            control.checkpoint(1)?;
            exponent = exponent
                .checked_mul(10)
                .and_then(|v| v.checked_sub(i64::from(b - b'0')))
                .ok_or_else(|| invalid(source_offset + exp_start))?;
        }
        if !neg {
            exponent = exponent
                .checked_neg()
                .ok_or_else(|| invalid(source_offset + exp_start))?;
        }
    }
    // Jackson's BigDecimal path uses Java's signed i32 exponent and scale.
    // Preserve that actual representation boundary, without a private quota.
    i32::try_from(exponent).map_err(|_| invalid(source_offset + exp_start))?;
    let scale = i64::try_from(fractional)
        .ok()
        .and_then(|v| v.checked_sub(exponent))
        .ok_or_else(|| invalid(source_offset))?;
    i32::try_from(scale).map_err(|_| invalid(source_offset))?;
    Ok(NumberRepresentation {
        coefficient_end,
        scale: Some(scale),
    })
}

fn write_number(
    out: &mut HostVec<u8, HostAggregateAllocator>,
    raw: &str,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let representation = number_representation(raw, 0, control)?;
    let mut digits = HostVec::new_in(out.allocator().clone());
    let negative = raw.starts_with('-');
    for b in raw[..representation.coefficient_end].bytes() {
        control.checkpoint(1)?;
        if b.is_ascii_digit() {
            push(&mut digits, b)?;
        }
    }
    let mut first = digits.len() - 1;
    let mut nonzero = false;
    for (index, &digit) in digits.iter().enumerate() {
        control.checkpoint(1)?;
        if digit != b'0' {
            first = index;
            nonzero = true;
            break;
        }
    }
    let digits = &digits[first..];
    if negative && nonzero {
        push(out, b'-')?;
    }
    let Some(scale) = representation.scale else {
        return extend(out, digits);
    };
    let precision = i64::try_from(digits.len()).map_err(|_| invalid(0))?;
    let adjusted = precision
        .checked_sub(1)
        .and_then(|v| v.checked_sub(scale))
        .ok_or_else(|| invalid(0))?;
    if scale >= 0 && adjusted >= -6 {
        let point = precision - scale;
        if point > 0 {
            extend(out, &digits[..point as usize])?;
            if scale > 0 {
                push(out, b'.')?;
                extend(out, &digits[point as usize..])?;
            }
        } else {
            extend(out, b"0.")?;
            for _ in point..0 {
                control.checkpoint(1)?;
                push(out, b'0')?;
            }
            extend(out, digits)?;
        }
    } else {
        push(out, digits[0])?;
        if digits.len() > 1 {
            push(out, b'.')?;
            extend(out, &digits[1..])?;
        }
        push(out, b'E')?;
        if adjusted >= 0 {
            push(out, b'+')?;
        }
        // Decimal formatting of this signed machine-width exponent uses only
        // a fixed stack buffer; the input coefficient itself is arbitrary size.
        let mut value = adjusted.unsigned_abs();
        let mut buffer = [0u8; 20];
        let mut i = buffer.len();
        loop {
            i -= 1;
            buffer[i] = b'0' + (value % 10) as u8;
            value /= 10;
            if value == 0 {
                break;
            }
        }
        if adjusted < 0 {
            push(out, b'-')?;
        }
        extend(out, &buffer[i..])?;
    }
    Ok(())
}

/// Normalize one logically JSON value on its original allocation/control host.
/// No input-size, node-count or depth quota is introduced. The iterative arena
/// and writer drop without recursive user-depth destructors.
pub fn normalize_json_key(
    input: &str,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<JsonCanonicalKey, KernelFailure> {
    control.checkpoint(0)?;
    let allocator = if host.opaque_allocation_host().is_some() {
        // The original host covers the platform-private journal before its
        // first lock. It stays with the same returned key's actual owner.
        HostAggregateAllocator::try_new_with_journal(host)?
    } else {
        // Explicit non-runtime block-only callers keep their original policy;
        // this branch supplies no opaque capacity or process fallback.
        HostAggregateAllocator::try_new(host)?
    };
    normalize_json_key_in(input, allocator, control)
}

pub(crate) fn normalize_json_key_in(
    input: &str,
    allocator: HostAggregateAllocator,
    control: &dyn KernelEvaluationControl,
) -> Result<JsonCanonicalKey, KernelFailure> {
    control.checkpoint(0)?;
    let mut parser = Parser {
        input,
        pos: 0,
        nodes: HostVec::new_in(allocator.clone()),
        strings: HostVec::new_in(allocator.clone()),
        frames: HostVec::new_in(allocator.clone()),
        control,
    };
    parser.parse()?;
    let mut ordered = HostVec::new_in(allocator.clone());
    for node in 0..parser.nodes.len() {
        control.checkpoint(1)?;
        if !matches!(parser.nodes[node].kind, Kind::Array | Kind::Object) {
            continue;
        }
        let begin = ordered.len();
        let mut next = parser.nodes[node].first;
        while next != NONE {
            control.checkpoint(1)?;
            push(&mut ordered, next)?;
            next = parser.nodes[next].next;
        }
        if matches!(parser.nodes[node].kind, Kind::Object) {
            sort_members(
                &mut ordered[begin..],
                &parser.nodes,
                &parser.strings,
                control,
            )?;
            let end = ordered.len();
            let mut write = begin;
            let mut read = begin;
            while read < end {
                control.checkpoint(1)?;
                let mut last = read;
                while last + 1 < end
                    && same_key(
                        ordered[last],
                        ordered[last + 1],
                        &parser.nodes,
                        &parser.strings,
                        control,
                    )?
                {
                    control.checkpoint(1)?;
                    last += 1;
                }
                ordered[write] = ordered[last];
                write += 1;
                read = last + 1;
            }
            ordered.truncate(write);
        }
        parser.nodes[node].ordered = begin..ordered.len();
    }
    enum Emit {
        Node(usize),
        Byte(u8),
        Key(usize),
    }
    let mut pending = HostVec::new_in(allocator.clone());
    push(&mut pending, Emit::Node(0))?;
    let mut bytes = HostVec::new_in(allocator);
    while let Some(next) = pending.pop() {
        control.checkpoint(1)?;
        match next {
            Emit::Byte(b) => push(&mut bytes, b)?,
            Emit::Key(node) => {
                write_string(
                    &mut bytes,
                    &parser.strings[parser.nodes[node].key.clone()],
                    control,
                )?;
                push(&mut bytes, b':')?;
            }
            Emit::Node(id) => {
                let node = &parser.nodes[id];
                match node.kind {
                    Kind::Null => extend(&mut bytes, b"null")?,
                    Kind::True => extend(&mut bytes, b"true")?,
                    Kind::False => extend(&mut bytes, b"false")?,
                    Kind::String => {
                        write_string(&mut bytes, &parser.strings[node.text.clone()], control)?
                    }
                    Kind::Number => write_number(&mut bytes, &input[node.text.clone()], control)?,
                    Kind::Array | Kind::Object => {
                        let object = matches!(node.kind, Kind::Object);
                        push(&mut bytes, if object { b'{' } else { b'[' })?;
                        push(&mut pending, Emit::Byte(if object { b'}' } else { b']' }))?;
                        for (ordinal, &child) in
                            ordered[node.ordered.clone()].iter().enumerate().rev()
                        {
                            control.checkpoint(1)?;
                            push(&mut pending, Emit::Node(child))?;
                            if object {
                                push(&mut pending, Emit::Key(child))?;
                            }
                            if ordinal > 0 {
                                push(&mut pending, Emit::Byte(b','))?;
                            }
                        }
                    }
                }
            }
        }
    }
    control.checkpoint(0)?;
    Ok(JsonCanonicalKey { bytes })
}

#[cfg(test)]
#[path = "json_canonical_tests.rs"]
mod tests;
